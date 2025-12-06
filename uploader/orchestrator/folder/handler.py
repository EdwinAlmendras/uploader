from pathlib import Path
from typing import Optional, Callable
import traceback
from uploader.orchestrator.preview_handler import PreviewHandler
from uploader.orchestrator.file_collector import FileCollector
from uploader.orchestrator.models import FolderUploadResult
from uploader.models import UploadConfig
from .file_processor import FileProcessor
from .file_existence import FileExistenceChecker, Blake3Deduplicator, PreviewChecker
from .parallel_upload import ParallelUploadCoordinator
from .process import FolderUploadProcess
from uploader.services.analyzer import AnalyzerService
from uploader.services.repository import MetadataRepository
from uploader.services.storage import StorageService

import logging
logger = logging.getLogger(__name__)

class FolderUploadHandler:
    """Handles folder uploads with intelligent parallel processing (max 2 concurrent)."""
    
    def __init__(
        self,
        analyzer: AnalyzerService,
        repository: MetadataRepository,
        storage: StorageService,
        preview_handler: PreviewHandler,
        config: UploadConfig
    ):
        """
        Initialize folder upload handler.
        
        Args:
            analyzer: AnalyzerService
            repository: MetadataRepository
            storage: Storage service
            preview_handler: PreviewHandler
            config: UploadConfig
        """
        self._file_collector = FileCollector()
        self._file_processor = FileProcessor(analyzer, repository, storage, preview_handler, config)
        self._existence_checker = FileExistenceChecker(storage)
        self._blake3_deduplicator = Blake3Deduplicator(repository, storage) if repository else None
        self._preview_checker = PreviewChecker(storage, preview_handler, analyzer) if repository else None
        self._upload_coordinator = ParallelUploadCoordinator(self._file_processor, max_parallel=1)
        self._storage = storage
    
    def upload_folder(
        self,
        folder_path: Path,
        dest: Optional[str] = None,
    ) -> 'FolderUploadProcess':
        return FolderUploadProcess(self, folder_path, dest)
    
    async def _upload_folder_internal(
        self,
        folder_path: Path,
        dest: Optional[str],
        progress_callback: Optional[Callable[[str, int, int], None]],
        process: Optional[FolderUploadProcess]
    ) -> FolderUploadResult:
        """Internal method that performs the actual upload."""
        folder_path = Path(folder_path)
        
        if not folder_path.is_dir():
            return FolderUploadResult(
                success=False,
                folder_name=folder_path.name,
                total_files=0,
                uploaded_files=0,
                failed_files=0,
                results=[],
                error=f"Not a directory: {folder_path}"
            )
        
        total = 0
        
        try:
            if process and process.is_cancelled:
                return FolderUploadResult(
                    success=False,
                    folder_name=folder_path.name,
                    total_files=0,
                    uploaded_files=0,
                    failed_files=0,
                    results=[],
                    error="Process was cancelled"
                )
            
            all_files = self._file_collector.collect_files(folder_path)
            total = len(all_files)
            
            if process:
                async with process._stats_lock:
                    process._stats["total_files"] = total
                await process._events.emit("progress", process.stats)
            
            if total == 0:
                return FolderUploadResult(
                    success=True,
                    folder_name=folder_path.name,
                    total_files=0,
                    uploaded_files=0,
                    failed_files=0,
                    results=[],
                    error="No media files found in folder"
                )
            
            dest_path = f"{dest}/{folder_path.name}" if dest else folder_path.name
            
            # Step 1: Check in database by blake3_hash (MORE SECURE - detects duplicates regardless of location/name)
            skipped_hash = 0
            files_to_check_mega = all_files
            existing_files_with_source_id = {}  # path -> source_id for existing files
            if self._blake3_deduplicator:
                logger.info("Checking files in database by blake3_hash (deduplication)")
                existing_paths, path_to_source_id = await self._blake3_deduplicator.check(all_files, progress_callback)
                skipped_hash = len(existing_paths)
                existing_files_with_source_id = path_to_source_id
                
                # Filter out files that exist in database - only check remaining in MEGA
                if existing_paths:
                    ### files_to_check_mega = [f for f in all_files if f not in existing_paths]
                    logger.debug(
                        "After blake3_hash check: %d files skipped (in database), %d files remaining to check in MEGA",
                        skipped_hash, len(files_to_check_mega)
                    )
                else:
                    logger.debug("After blake3_hash check: All %d files are new, checking all in MEGA", len(all_files))
            else:
                logger.warning("Blake3Deduplicator not available, skipping database check")
            
            # Step 2: Check remaining files in MEGA (fast, by path - verifies if already uploaded to expected location)
            skipped_mega = 0
            if files_to_check_mega:
                logger.info("Checking remaining files in MEGA by path")
                pending_files, skipped_mega = await self._existence_checker.check(
                    files_to_check_mega, folder_path, dest_path, len(files_to_check_mega), progress_callback
                )
            else:
                pending_files = []
                logger.debug("No files to check in MEGA (all were skipped by blake3_hash check)")
            
            total_skipped = skipped_mega + skipped_hash
            logger.info(f"Existence check summary: {total} total files, {skipped_hash} skipped by blake3_hash, {skipped_mega} skipped by MEGA path, {total_skipped} total skipped, {len(pending_files)} files to upload")
            
            # Step 3: Check and regenerate missing previews for existing files
            previews_regenerated = 0
            if existing_files_with_source_id and self._preview_checker:
                logger.info("Checking and regenerating missing previews for existing files")
                previews_regenerated = await self._preview_checker.check_and_regenerate(
                    existing_files_with_source_id, progress_callback
                )
                if previews_regenerated > 0:
                    logger.info(f"Regenerated {previews_regenerated} missing previews")
            
            if process:
                async with process._stats_lock:
                    process._stats["skipped"] = total_skipped
                await process._events.emit("progress", process.stats)
            
            if not pending_files:
                return FolderUploadResult(
                    success=True,
                    folder_name=folder_path.name,
                    total_files=total,
                    uploaded_files=total_skipped,
                    failed_files=0,
                    results=[],
                    error=None
                )
            
            # 4. Create root folder first
            root_handle = await self._storage.create_folder(dest_path)
            
            # 5. Pre-create ALL folder structure BEFORE parallel upload starts
            # This prevents race conditions when multiple uploads try to create the same folder
            # Extract ALL intermediate folder levels to ensure proper cache reuse
            logger.info("Pre-creating folder structure...")
            unique_subfolders = set()
            for file_path, rel_path in pending_files:
                if rel_path.parent != Path("."):
                    # Extract ALL intermediate levels (e.g., for "set/set2/set3" 
                    # we need: "dest/set", "dest/set/set2", "dest/set/set2/set3")
                    # Build paths using string concatenation with "/" to ensure correct format
                    parts = rel_path.parent.parts
                    for i in range(1, len(parts) + 1):
                        # Build intermediate path using "/" separator explicitly
                        intermediate_parts = parts[:i]
                        intermediate_str = "/".join(intermediate_parts)
                        full_intermediate = f"{dest_path}/{intermediate_str}"
                        unique_subfolders.add(full_intermediate)
            
            # Create all subfolders sequentially in sorted order (parents before children)
            # This ensures all folders exist before parallel uploads start and cache is properly populated
            if unique_subfolders:
                logger.info(f"Creating {len(unique_subfolders)} subfolder(s) before upload...")
                for subfolder_path in sorted(unique_subfolders):  # Sort ensures parents before children
                    try:
                        await self._storage.create_folder(subfolder_path)
                    except Exception as e:
                        logger.warning(f"Failed to create subfolder {subfolder_path}: {e}")
                        # Continue with other folders - upload will handle missing folders
            
            logger.info("Folder structure ready, starting parallel uploads...")
            
            results = await self._upload_coordinator.upload(
                pending_files, dest_path, total, progress_callback, process
            )
            
            uploaded = sum(1 for r in results if r.success)
            failed = sum(1 for r in results if not r.success)
            
            if process:
                async with process._stats_lock:
                    process._stats["uploaded"] = uploaded
                    process._stats["failed"] = failed
                await process._events.emit("progress", process.stats)
            
            return FolderUploadResult(
                success=failed == 0,
                folder_name=folder_path.name,
                total_files=total,
                uploaded_files=uploaded,
                failed_files=failed,
                results=results,
                mega_folder_handle=root_handle
            )
            
        except Exception as e:
            error_msg = f"{str(e)}"
            logger.error(traceback.format_exc())
            logger.error(f"Error uploading folder {folder_path}: {error_msg}")
            if process:
                await process._events.emit("error", e)
            
            return FolderUploadResult(
                success=False,
                folder_name=folder_path.name,
                total_files=total,
                uploaded_files=0,
                failed_files=total,
                results=[],
                error=error_msg
            )

