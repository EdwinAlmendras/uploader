from pathlib import Path
from typing import Optional, Callable, Tuple, Set
from uploader.services import StorageService
from uploader.services.repository import MetadataRepository
from uploader.services.resume import blake3_file
import asyncio
import logging

logger = logging.getLogger(__name__)

class FileExistenceChecker:
    """Checks which files already exist in MEGA by path."""

    def __init__(self, storage: StorageService):
        self._storage = storage

    async def check(self, all_files: list[Path], folder_path: Path, dest_path: str,
                   total: int, progress_callback: Optional[Callable]) -> Tuple[list[Tuple[Path, Path]], int]:
        """
        Check which files already exist in MEGA by path.
        
        Returns pending files and skipped count.
        """
        if progress_callback:
            progress_callback("Checking existing files in MEGA...", 0, total)

        pending_files = []
        skipped = 0

        logger.debug(
            "FileExistenceChecker: Starting MEGA path check for %d files (dest_path: '%s')",
            len(all_files), dest_path
        )

        for idx, file_path in enumerate(all_files):
            rel_path = file_path.relative_to(folder_path)
            # Normalize path to use forward slashes (MEGA uses /, not Windows \)
            if rel_path.parent != Path("."):
                parent_str = rel_path.parent.as_posix()  # Always uses / separator
                mega_dest = f"{dest_path}/{parent_str}"
            else:
                mega_dest = dest_path
            target_path = f"{mega_dest}/{file_path.name}"

            logger.debug(
                "FileExistenceChecker: [%d/%d] Checking '%s' in MEGA at: '%s'",
                idx+1, total, file_path.name, target_path
            )

            exists = await self._storage.exists(target_path)
            if exists:
                logger.debug(
                    "FileExistenceChecker: ✓ File already exists in MEGA - '%s' at '%s' - WILL SKIP",
                    file_path.name, target_path
                )
                skipped += 1
            else:
                logger.debug(
                    "FileExistenceChecker:   File NOT in MEGA - '%s' (rel_path: '%s') - WILL CHECK IN DATABASE",
                    file_path.name, rel_path
                )
                pending_files.append((file_path, rel_path))

            if progress_callback and idx % 10 == 0:
                progress_callback(f"Checking MEGA: {file_path.name}", idx + 1, total)

        if skipped > 0:
            logger.info(
                "FileExistenceChecker: Summary - Skipped %d files (already in MEGA), %d files to check in database",
                skipped, len(pending_files)
            )
        else:
            logger.info("FileExistenceChecker: No files found in MEGA, all %d files will be checked in database", total)

        return pending_files, skipped


class Blake3Deduplicator:
    """Checks which files already exist in database by blake3_hash."""

    def __init__(self, repository: MetadataRepository):
        self._repository = repository

    async def check(self, file_paths: list[Path], progress_callback: Optional[Callable] = None) -> Set[Path]:
        """
        Check which files already exist in database by blake3_hash.
        
        Args:
            file_paths: List of file paths to check
            progress_callback: Optional progress callback
            
        Returns:
            Set of file paths that already exist in database (should be skipped)
        """
        if not file_paths:
            logger.debug("Blake3Deduplicator: No files to check")
            return set()

        total = len(file_paths)
        logger.debug("Blake3Deduplicator: Starting deduplication for %d files", total)
        if progress_callback:
            progress_callback("Calculating blake3_hash and checking in database...", 0, total)

        # Calculate blake3_hash for all files
        logger.debug("Blake3Deduplicator: Calculating blake3_hash for %d files...", total)
        hash_tasks = [blake3_file(file_path) for file_path in file_paths]
        blake3_hashes = await asyncio.gather(*hash_tasks)

        # Build mapping: blake3_hash -> list of file_paths (multiple files can have same hash)
        hash_to_paths = {}  # hash -> list of paths
        path_to_hash = {}
        blake3_list = []
        for file_path, blake3_hash in zip(file_paths, blake3_hashes):
            if blake3_hash:
                # Add to hash -> paths mapping (support multiple files with same hash)
                if blake3_hash not in hash_to_paths:
                    hash_to_paths[blake3_hash] = []
                hash_to_paths[blake3_hash].append(file_path)
                
                path_to_hash[file_path] = blake3_hash
                blake3_list.append(blake3_hash)
                logger.debug(
                    "Blake3Deduplicator: Calculated hash for '%s': %s...",
                    file_path.name, blake3_hash[:16]
                )
            else:
                logger.warning("Blake3Deduplicator: Failed to calculate hash for '%s'", file_path)

        if not blake3_list:
            logger.warning("Blake3Deduplicator: No valid blake3 hashes calculated")
            return set()

        # Get unique hashes (files with same content will have same hash)
        unique_hashes = set(blake3_list)
        logger.info(
            "Blake3Deduplicator: Calculated %d hashes (%d unique) from %d files",
            len(blake3_list), len(unique_hashes), total
        )
        
        # Log duplicate hashes (same content, different paths)
        for blake3_hash, paths in hash_to_paths.items():
            if len(paths) > 1:
                logger.debug(
                    "Blake3Deduplicator: Found %d files with SAME hash %s... (duplicate content):",
                    len(paths), blake3_hash[:16]
                )
                for path in paths:
                    logger.debug("Blake3Deduplicator:   - %s", path)

        logger.debug("Blake3Deduplicator: Checking %d unique blake3 hashes in database...", len(unique_hashes))
        # Batch check in database (only unique hashes)
        existing_hashes = await self._repository.check_exists_batch(list(unique_hashes))
        logger.debug("Blake3Deduplicator: Database returned %d existing hashes", len(existing_hashes))

        # Find which files already exist (all files with existing hash should be skipped)
        existing_paths = set()
        for blake3_hash, source_id in existing_hashes.items():
            if blake3_hash in hash_to_paths:
                # Add ALL files with this hash (they're all duplicates)
                paths_with_hash = hash_to_paths[blake3_hash]
                existing_paths.update(paths_with_hash)
                logger.debug(
                    "Blake3Deduplicator: ✓ Hash %s... exists in database (source_id: %s) - Skipping %d file(s):",
                    blake3_hash[:16], source_id, len(paths_with_hash)
                )
                for file_path in paths_with_hash:
                    logger.debug("Blake3Deduplicator:   - '%s' - WILL SKIP", file_path.name)
            else:
                logger.warning(
                    "Blake3Deduplicator: Hash %s... found in database but not in our mapping!",
                    blake3_hash[:16]
                )

        # Log files that don't exist in database
        new_files = set(file_paths) - existing_paths
        if new_files:
            logger.debug("Blake3Deduplicator: %d files are NEW (not in database):", len(new_files))
            for file_path in new_files:
                blake3_hash = path_to_hash.get(file_path, "unknown")
                logger.debug(
                    "Blake3Deduplicator:   - '%s' (hash: %s...) - WILL UPLOAD",
                    file_path.name, blake3_hash[:16] if blake3_hash != "unknown" else "unknown"
                )

        if existing_paths:
            logger.info(
                "Blake3Deduplicator: Summary - Skipped %d files (already in database), %d new files to process",
                len(existing_paths), len(new_files)
            )
        else:
            logger.info("Blake3Deduplicator: All %d files are new (not in database)", total)

        return existing_paths

