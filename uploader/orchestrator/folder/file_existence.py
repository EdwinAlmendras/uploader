from pathlib import Path
from typing import Optional, Callable, Tuple, Set, Dict
from uploader.services import StorageService
from uploader.services.managed_storage import ManagedStorageService
from uploader.services.repository import MetadataRepository
from uploader.services.resume import blake3_file
from uploader.orchestrator.preview_handler import PreviewHandler
from uploader.services.analyzer import AnalyzerService
from mediakit import is_video
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

    def __init__(self, repository: MetadataRepository, storage: StorageService):
        self._repository = repository
        self._storage = storage
        # Get AccountManager if storage is ManagedStorageService
        self._manager = storage.manager if isinstance(storage, ManagedStorageService) else None

    async def check(self, file_paths: list[Path], progress_callback: Optional[Callable] = None) -> Tuple[Set[Path], Dict[Path, str]]:
        """
        Check which files already exist in database by blake3_hash.
        
        Args:
            file_paths: List of file paths to check
            progress_callback: Optional progress callback
            
        Returns:
            Tuple of (set of file paths that exist, dict mapping path -> source_id)
        """
        if not file_paths:
            logger.debug("Blake3Deduplicator: No files to check")
            return set(), {}

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
            return set(), {}

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

        # Find which files already exist (verify they also exist in MEGA)
        existing_paths = set()
        path_to_source_id = {}  # Map path -> source_id for existing files
        for blake3_hash, doc_info in existing_hashes.items():
            if blake3_hash not in hash_to_paths:
                logger.warning(
                    "Blake3Deduplicator: Hash %s... found in database but not in our mapping!",
                    blake3_hash[:16]
                )
                continue
            
            # Handle both old format (just source_id string) and new format (dict with source_id and mega_handle)
            if isinstance(doc_info, dict):
                source_id = doc_info.get("source_id")
                mega_handle = doc_info.get("mega_handle")
            else:
                # Backward compatibility: old format was just source_id string
                source_id = doc_info
                mega_handle = None
            
            # Verify file exists in MEGA by searching for mega_id (attribute 'm')
            # Use AccountManager if available (searches all accounts), otherwise use storage (single account)
            file_exists_in_mega = True
            if source_id:
                # Use manager to search in all accounts if available
                """ if self._manager:
                    file_exists_in_mega = await self._manager.find_by_mega_id(source_id) is not None
                else:
                    # Fallback to storage (single account)
                    file_exists_in_mega = await self._storage.exists_by_mega_id(source_id) """
                
                if not file_exists_in_mega:
                    logger.info(
                        "Blake3Deduplicator: File exists in database but NOT in MEGA (deleted?) - hash: %s..., source_id: %s - WILL RE-UPLOAD",
                        blake3_hash[:16], source_id
                    )
                else:
                    logger.debug(
                        "Blake3Deduplicator: File exists in database AND MEGA - hash: %s..., source_id: %s",
                        blake3_hash[:16], source_id
                    )
            else:
                logger.debug(
                    "Blake3Deduplicator: File in database has no source_id - hash: %s... - WILL RE-UPLOAD",
                    blake3_hash[:16]
                )
            
            # Only skip if file exists in both database AND MEGA
            if file_exists_in_mega:
                paths_with_hash = hash_to_paths[blake3_hash]
                existing_paths.update(paths_with_hash)
                # Map all paths with this hash to the same source_id
                for file_path in paths_with_hash:
                    path_to_source_id[file_path] = source_id
                logger.debug(
                    "Blake3Deduplicator: ✓ Hash %s... exists in database AND MEGA (source_id: %s) - Skipping %d file(s):",
                    blake3_hash[:16], source_id, len(paths_with_hash)
                )
                for file_path in paths_with_hash:
                    logger.debug("Blake3Deduplicator:   - '%s' - WILL SKIP", file_path.name)
            else:
                # File exists in database but not in MEGA - don't skip, allow re-upload
                logger.info(
                    "Blake3Deduplicator: File will be re-uploaded (exists in DB but not in MEGA) - hash: %s..., source_id: %s",
                    blake3_hash[:16], source_id
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

        return existing_paths, path_to_source_id


class PreviewChecker:
    """Checks and regenerates missing previews for existing files."""

    def __init__(
        self,
        storage: StorageService,
        preview_handler: PreviewHandler,
        analyzer: AnalyzerService
    ):
        self._storage = storage
        self._preview_handler = preview_handler
        self._analyzer = analyzer

    async def check_and_regenerate(
        self,
        existing_files: Dict[Path, str],
        dest_path: str,
        relative_to: Path = None,
        progress_callback: Optional[Callable] = None,
    ) -> int:
        """
        Check if previews exist for existing files and regenerate if missing.
        
        Args:
            existing_files: Dict mapping file_path -> source_id for files that exist in database
            progress_callback: Optional progress callback
            
        Returns:
            Number of previews regenerated
        """
        if not existing_files:
            return 0

        # Filter only video files
        video_files = {
            path: source_id 
            for path, source_id in existing_files.items() 
            if is_video(path)
        }

        if not video_files:
            logger.debug("PreviewChecker: No video files to check for previews")
            return 0

        total = len(video_files)
        logger.info("PreviewChecker: Checking previews for %d existing video files", total)
        if progress_callback:
            progress_callback("Checking previews...", 0, total)

        regenerated = 0
        for idx, (file_path, source_id) in enumerate(video_files.items()):
            # For existing files, we don't have dest_path, so use backward compatibility
            # Check preview in old location: /.previews/{source_id}.jpg
            
            relative_path = file_path.relative_to(relative_to)
            
            preview_path = f"{dest_path}/{relative_path.with_suffix('.jpg')}"
            
            print(preview_path)
            
            logger.debug(
                "PreviewChecker: [%d/%d] Checking preview for '%s' (source_id: %s)",
                idx + 1, total, file_path.name, source_id
            )

            # Check if preview exists in MEGA (backward compatibility location)
            preview_exists = await self._storage.exists(preview_path)
            
            if preview_exists:
                logger.debug(
                    "PreviewChecker: ✓ Preview exists for '%s' (source_id: %s) - SKIP",
                    file_path.name, source_id
                )
            else:
                logger.info(
                    "PreviewChecker: ✗ Preview missing for '%s' (source_id: %s) - REGENERATING",
                    file_path.name, source_id
                )
                
                # Get video duration by analyzing the file
                try:
                    tech_data = await self._analyzer.analyze_video_async(file_path)
                    duration = tech_data.get('duration', 0)
                    
                    if duration > 0:
                        # Regenerate and upload preview
                        # Note: For existing files, we don't have dest_path, so use backward compatibility
                        # Preview will be uploaded using source_id (old behavior)
                        preview_handle = await self._preview_handler.upload_preview(
                            file_path, source_id, duration
                            # dest_path and filename not provided - will use backward compatibility
                        )
                        if preview_handle:
                            logger.info(
                                "PreviewChecker: ✓ Preview regenerated and uploaded for '%s' (source_id: %s, handle: %s)",
                                file_path.name, source_id, preview_handle
                            )
                            regenerated += 1
                        else:
                            logger.warning(
                                "PreviewChecker: Failed to upload preview for '%s' (source_id: %s)",
                                file_path.name, source_id
                            )
                    else:
                        logger.warning(
                            "PreviewChecker: Invalid duration (%.2f) for '%s' (source_id: %s) - skipping preview",
                            duration, file_path.name, source_id
                        )
                except Exception as e:
                    logger.error(
                        "PreviewChecker: Error regenerating preview for '%s' (source_id: %s): %s",
                        file_path.name, source_id, e,
                        exc_info=True
                    )

            if progress_callback and idx % 5 == 0:
                progress_callback(f"Checking previews: {file_path.name}", idx + 1, total)

        if regenerated > 0:
            logger.info("PreviewChecker: Regenerated %d missing previews", regenerated)
        else:
            logger.debug("PreviewChecker: All previews exist, nothing to regenerate")

        return regenerated

