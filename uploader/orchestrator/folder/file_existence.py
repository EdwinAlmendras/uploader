from pathlib import Path
from typing import Optional, Callable, Tuple
from uploader.services import StorageService
import logging

logger = logging.getLogger(__name__)

class FileExistenceChecker:
    """Checks which files already exist in MEGA."""

    def __init__(self, storage: StorageService):
        self._storage = storage

    async def check(self, all_files: list[Path], folder_path: Path, dest_path: str,
                   total: int, progress_callback: Optional[Callable]) -> Tuple[list[Tuple[Path, Path]], int]:
        """Return pending files and skipped count."""
        if progress_callback:
            progress_callback("Checking existing files in MEGA...", 0, total)

        pending_files = []
        skipped = 0

        logger.debug(
            "Starting existence check for %d files under '%s' (dest_path: '%s').",
            len(all_files), folder_path, dest_path
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
                "[%d/%d] Checking file: '%s' (rel_path: '%s') in MEGA at: '%s'",
                idx+1, total, file_path, rel_path, target_path
            )

            exists = await self._storage.exists(target_path)
            if exists:
                logger.debug("File already exists in MEGA: '%s' (will skip)", target_path)
                skipped += 1
            else:
                logger.debug("File does not exist in MEGA (will upload): '%s'", target_path)
                pending_files.append((file_path, rel_path))

            if progress_callback and idx % 10 == 0:
                progress_callback(f"Checking: {file_path.name}", idx + 1, total)

        if skipped > 0:
            logger.info("Skipped %s files already in MEGA", skipped)
        else:
            logger.debug("No files were skipped; all files will be uploaded.")

        logger.debug("Existence check complete: %d pending, %d skipped.", len(pending_files), skipped)

        return pending_files, skipped

