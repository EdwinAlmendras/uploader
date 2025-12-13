from pathlib import Path
from typing import Optional, Callable
import traceback
from uploader.orchestrator.preview_handler import PreviewHandler
from uploader.orchestrator.file_collector import FileCollector
from uploader.orchestrator.models import FolderUploadResult
from uploader.models import UploadConfig
from .file_processor import FileProcessor
from .file_existence import FileExistenceChecker, Blake3Deduplicator, PreviewChecker, MegaToDbSynchronizer, MegaFileInfo
from .parallel_upload import ParallelUploadCoordinator
from .process import FolderUploadProcess
from .set_processor import ImageSetProcessor
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
        self._set_processor = ImageSetProcessor(analyzer, repository, storage, config)
        self._existence_checker = FileExistenceChecker(storage)
        self._blake3_deduplicator = Blake3Deduplicator(repository, storage) if repository else None
        self._preview_checker = PreviewChecker(storage, preview_handler, analyzer) if repository else None
        self._mega_to_db_synchronizer = MegaToDbSynchronizer(analyzer, repository, storage) if repository else None
        self._upload_coordinator = ParallelUploadCoordinator(self._file_processor, max_parallel=1)
        self._storage = storage
        self._repository = repository
    
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
            
            dest_path = f"{dest}/{folder_path.name}" if dest else folder_path.name
            
            # Step 0: Detect image sets and individual files
            logger.info("Detecting image sets...")
            sets, individual_files = self._file_collector.detect_sets_and_files(folder_path)
            
            logger.info(f"Found {len(sets)} image set(s) and {len(individual_files)} individual file(s)")
            
            # Calculate total (sets count as 1 each, plus individual files)
            total = len(sets) + len(individual_files)
            
            if process:
                async with process._stats_lock:
                    process._stats["total_files"] = total
                    process._stats["sets_count"] = len(sets)
                await process._events.emit("progress", process.stats)
            
            if total == 0:
                return FolderUploadResult(
                    success=True,
                    folder_name=folder_path.name,
                    total_files=0,
                    uploaded_files=0,
                    failed_files=0,
                    results=[],
                    error="No media files or sets found in folder"
                )
            
            all_results = []
            
            # Step 1: Process sets first (thumbnails, POST, 7z, upload sequentially)
            if sets:
                logger.info(f"Processing {len(sets)} image set(s)...")
                for idx, set_folder in enumerate(sets, 1):
                    if process and process.is_cancelled:
                        logger.info("Upload cancelled by user")
                        break
                    
                    logger.info(f"Processing set {idx}/{len(sets)}: {set_folder.name}")
                    if progress_callback:
                        progress_callback(
                            f"Processing set {idx}/{len(sets)}: {set_folder.name}...",
                            idx - 1,
                            total
                        )
                    
                    try:
                        set_result, image_results = await self._set_processor.process_set(
                            set_folder,
                            dest_path,
                            progress_callback
                        )
                        all_results.append(set_result)
                        all_results.extend(image_results)
                        
                        if process:
                            async with process._stats_lock:
                                process._stats["uploaded"] = sum(1 for r in all_results if r.success)
                                process._stats["failed"] = sum(1 for r in all_results if not r.success)
                            await process._events.emit("progress", process.stats)
                    except Exception as e:
                        logger.error(f"Error processing set {set_folder.name}: {e}", exc_info=True)
                        all_results.append(
                            FolderUploadResult(
                                success=False,
                                folder_name=set_folder.name,
                                total_files=0,
                                uploaded_files=0,
                                failed_files=1,
                                results=[],
                                error=str(e)
                            )
                        )
            
            # Step 2: Process individual files (videos, other files)
            if individual_files and (not process or not process.is_cancelled):
                logger.info(f"Processing {len(individual_files)} individual file(s)...")
                
                all_files = individual_files
            
                # Step 2.1: Check in MEGA by path (obtain nodes and mega_ids)
                logger.info("Checking individual files in MEGA by path")
                pending_after_mega, skipped_mega, mega_files_info = await self._existence_checker.check(
                    all_files, folder_path, dest_path, len(all_files), progress_callback
                )
                
                # Step 2.1.5: Synchronize files from MEGA to DB (if they exist in MEGA but not in DB)
                synced_count = 0
                existing_files_with_source_id = {}  # path -> source_id for existing files
                
                if mega_files_info and self._mega_to_db_synchronizer and self._repository:
                    logger.info("Checking if files found in MEGA exist in database...")
                    for file_path, mega_info in mega_files_info.items():
                        if mega_info.source_id:
                            # Check if exists in DB by source_id
                            exists_in_db = await self._repository.exists_by_source_id(mega_info.source_id)
                            if exists_in_db:
                                # File exists in both MEGA and DB - skip
                                existing_files_with_source_id[file_path] = mega_info.source_id
                                logger.debug(
                                    "File '%s' exists in MEGA and DB (source_id: %s) - SKIP",
                                    file_path.name, mega_info.source_id
                                )
                            else:
                                # File exists in MEGA but NOT in DB - sync to DB
                                logger.info(
                                    "File '%s' exists in MEGA but NOT in DB (source_id: %s) - SYNCING to DB",
                                    file_path.name, mega_info.source_id
                                )
                                synced_source_id = await self._mega_to_db_synchronizer.sync_file(
                                    file_path, mega_info, dest_path
                                )
                                if synced_source_id:
                                    synced_count += 1
                                    existing_files_with_source_id[file_path] = synced_source_id
                                else:
                                    logger.warning(
                                        "Failed to sync file '%s' to DB - will be treated as new",
                                        file_path.name
                                    )
                        else:
                            # File in MEGA has no mega_id - cannot sync, treat as new
                            logger.debug(
                                "File '%s' exists in MEGA but has no mega_id - cannot sync to DB",
                                file_path.name
                            )
                
                if synced_count > 0:
                    logger.info(f"Synchronized {synced_count} file(s) from MEGA to DB")
                
                # Step 2.2: Check remaining files (not in MEGA) in database by blake3_hash
                skipped_hash = 0
                files_to_check_db = [fp for fp, _ in pending_after_mega]
                
                if files_to_check_db and self._blake3_deduplicator:
                    logger.info("Checking remaining individual files in database by blake3_hash (deduplication)")
                    existing_paths, path_to_source_id = await self._blake3_deduplicator.check(
                        files_to_check_db, progress_callback
                    )
                    skipped_hash = len(existing_paths)
                    # Add to existing_files_with_source_id
                    existing_files_with_source_id.update(path_to_source_id)
                    
                    if existing_paths:
                        logger.debug(
                            "After blake3_hash check: %d files skipped (in database), %d files remaining to upload",
                            skipped_hash, len(files_to_check_db) - skipped_hash
                        )
                    else:
                        logger.debug("After blake3_hash check: All %d files are new (not in database)", len(files_to_check_db))
                else:
                    if not files_to_check_db:
                        logger.debug("No files to check in database (all were found in MEGA)")
                    elif not self._blake3_deduplicator:
                        logger.warning("Blake3Deduplicator not available, skipping database check")
                
                # Filter out existing files from pending_files
                existing_paths_set = set(existing_files_with_source_id.keys())
                pending_files = [
                    (file_path, rel_path)
                    for file_path, rel_path in pending_after_mega
                    if file_path not in existing_paths_set
                ]
                
                total_skipped = skipped_mega + skipped_hash
                logger.info(
                    f"Existence check summary: {len(all_files)} individual files, "
                    f"{skipped_mega} skipped by MEGA path, {skipped_hash} skipped by blake3_hash, "
                    f"{synced_count} synced from MEGA to DB, {total_skipped} total skipped, "
                    f"{len(pending_files)} files to upload"
                )
                
                # Step 2.3: Check and regenerate missing previews for existing files
                previews_regenerated = 0
                if existing_files_with_source_id and self._preview_checker:
                    logger.info("Checking and regenerating missing previews for existing files")
                    previews_regenerated = await self._preview_checker.check_and_regenerate(
                        existing_files_with_source_id, dest_path, relative_to=folder_path, progress_callback=progress_callback
                    )
                    if previews_regenerated > 0:
                        logger.info(f"Regenerated {previews_regenerated} missing previews")
                
                if process:
                    async with process._stats_lock:
                        process._stats["skipped"] = total_skipped
                    await process._events.emit("progress", process.stats)
                
                # Step 2.4: Upload individual files if any pending
                if pending_files:
                    # Create root folder first
                    root_handle = await self._storage.create_folder(dest_path)
                    
                    # Pre-create folder structure
                    logger.info("Pre-creating folder structure for individual files...")
                    unique_subfolders = set()
                    for file_path, rel_path in pending_files:
                        if rel_path.parent != Path("."):
                            parts = rel_path.parent.parts
                            for i in range(1, len(parts) + 1):
                                intermediate_parts = parts[:i]
                                intermediate_str = "/".join(intermediate_parts)
                                full_intermediate = f"{dest_path}/{intermediate_str}"
                                unique_subfolders.add(full_intermediate)
                    
                    if unique_subfolders:
                        logger.info(f"Creating {len(unique_subfolders)} subfolder(s) before upload...")
                        for subfolder_path in sorted(unique_subfolders):
                            try:
                                await self._storage.create_folder(subfolder_path)
                            except Exception as e:
                                logger.warning(f"Failed to create subfolder {subfolder_path}: {e}")
                    
                    logger.info("Folder structure ready, starting parallel uploads for individual files...")
                    
                    file_results = await self._upload_coordinator.upload(
                        pending_files, dest_path, total, progress_callback, process
                    )
                    all_results.extend(file_results)
                else:
                    root_handle = await self._storage.create_folder(dest_path) if sets else None
            
            uploaded = sum(1 for r in all_results if r.success)
            failed = sum(1 for r in all_results if not r.success)
            
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
                results=all_results,
                mega_folder_handle=root_handle if 'root_handle' in locals() else None
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

