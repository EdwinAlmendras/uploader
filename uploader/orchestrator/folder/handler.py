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
from .process import FolderUploadProcess, ProcessPhase
from .pipeline_deduplicator import PipelineDeduplicator
from .set_processor import ImageSetProcessor
from uploader.services.analyzer import AnalyzerService
from uploader.services.repository import MetadataRepository
from uploader.services.storage import StorageService
from uploader.services.managed_storage import ManagedStorageService
from uploader.services.hash_cache import HashCache, get_hash_cache

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
        self._preview_handler = preview_handler  # Store preview_handler
        self._preview_checker = PreviewChecker(storage, preview_handler, analyzer) if repository else None
        self._mega_to_db_synchronizer = MegaToDbSynchronizer(analyzer, repository, storage) if repository else None
        self._upload_coordinator = ParallelUploadCoordinator(self._file_processor, max_parallel=1)
        self._storage = storage
        self._repository = repository
        self._hash_cache: Optional[HashCache] = None
        self._analyzer = analyzer
    
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
            
            # Initialize hash cache
            if self._hash_cache is None:
                self._hash_cache = await get_hash_cache()
            
            # Step 0: Detect image sets and individual files
            if process:
                await process.set_phase(ProcessPhase.DETECTING, "Detecting files and sets...")
            
            logger.info("Detecting image sets...")
            sets, individual_files = self._file_collector.detect_sets_and_files(folder_path)
            
            logger.info(f"Found {len(sets)} image set(s) and {len(individual_files)} individual file(s)")
            
            if process:
                await process.complete_phase("detecting", f"Found {len(sets)} sets, {len(individual_files)} files")
            
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
                if process:
                    await process.set_phase(ProcessPhase.CHECKING_MEGA, "Checking files in MEGA...")
                
                logger.info("Checking individual files in MEGA by path")
                pending_after_mega, skipped_mega, mega_files_info = await self._existence_checker.check(
                    all_files, folder_path, dest_path, len(all_files), progress_callback
                )
                
                if process:
                    await process.complete_phase("checking_mega", f"{skipped_mega} found in MEGA, {len(pending_after_mega)} to check")
                
                # Step 2.1.5: Synchronize files from MEGA to DB (if they exist in MEGA but not in DB)
                synced_count = 0
                existing_files_with_source_id = {}  # path -> source_id for existing files
                
                if mega_files_info and self._mega_to_db_synchronizer and self._repository:
                    if process:
                        await process.set_phase(ProcessPhase.SYNCING, "Syncing MEGA files to database...")
                    
                    logger.info("Checking if files found in MEGA exist in database...")
                    sync_total = len(mega_files_info)
                    sync_current = 0
                    
                    for file_path, mega_info in mega_files_info.items():
                        sync_current += 1
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
                                if process:
                                    await process.emit_sync_start(file_path.name)
                                    await process.emit_phase_progress(
                                        "syncing", f"Syncing: {file_path.name}",
                                        sync_current, sync_total, file_path.name
                                    )
                                
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
                                    if process:
                                        await process.emit_sync_complete(file_path.name, True)
                                else:
                                    logger.warning(
                                        "Failed to sync file '%s' to DB - will be treated as new",
                                        file_path.name
                                    )
                                    if process:
                                        await process.emit_sync_complete(file_path.name, False)
                else:
                            # File in MEGA has no mega_id - cannot sync, treat as new
                            logger.debug(
                                "File '%s' exists in MEGA but has no mega_id - cannot sync to DB",
                                file_path.name
                            )
                    
                if process:
                    await process.complete_phase("syncing", f"Synced {synced_count} files")
            
                if synced_count > 0:
                    logger.info(f"Synchronized {synced_count} file(s) from MEGA to DB")
                
                # Step 2.2: Check remaining files (not in MEGA) in database by blake3_hash
                skipped_hash = 0
                files_to_check_db = [fp for fp, _ in pending_after_mega]
                path_to_hash = {}  # Will store calculated hashes for later use
                
                if files_to_check_db and self._blake3_deduplicator:
                    if process:
                        await process.set_phase(ProcessPhase.HASHING, "Calculating hashes...")
                    
                    logger.info("Checking remaining individual files in database by blake3_hash (deduplication)")
                    
                    # Use PipelineDeduplicator with hash cache for better performance and progress
                    pipeline = PipelineDeduplicator(
                        self._repository,
                        self._storage,
                        hash_cache=self._hash_cache,
                        preview_handler=self._preview_handler if self._preview_checker else None,
                        analyzer=self._analyzer if self._preview_checker else None,
                    )
                    
                    # Setup callbacks for hash progress
                    if process:
                        async def on_hash_start(filename):
                            await process.emit_hash_start(filename)
                        
                        hash_count = [0]  # Using list to allow modification in closure
                        async def on_hash_complete(filename, hash_val, from_cache):
                            hash_count[0] += 1
                            await process.emit_hash_complete(
                                filename, hash_count[0], len(files_to_check_db), 
                                from_cache, hash_val
                            )
                            await process.emit_phase_progress(
                                "hashing", 
                                f"{'📦 Cache' if from_cache else '🔢 Calculated'}: {filename}",
                                hash_count[0], len(files_to_check_db), filename, from_cache
                            )
                        
                        async def on_check_complete(filename, exists_in_db, exists_in_mega):
                            """Called when DB/MEGA check completes for a file."""
                            # Emit event through process for progress display
                            await process._events.emit("check_complete", filename, exists_in_db, exists_in_mega)
                        
                        # Set callbacks (wrapping async functions)
                        def sync_hash_start(filename):
                            import asyncio
                            try:
                                loop = asyncio.get_running_loop()
                                loop.create_task(on_hash_start(filename))
                            except RuntimeError:
                                pass
                        
                        def sync_hash_complete(filename, hash_val, from_cache):
                            import asyncio
                            try:
                                loop = asyncio.get_running_loop()
                                loop.create_task(on_hash_complete(filename, hash_val, from_cache))
                            except RuntimeError:
                                pass
                        
                        def sync_check_complete(filename, exists_in_db, exists_in_mega):
                            import asyncio
                            try:
                                loop = asyncio.get_running_loop()
                                loop.create_task(on_check_complete(filename, exists_in_db, exists_in_mega))
                            except RuntimeError:
                                pass
                        
                        pipeline.on_hash_start(sync_hash_start)
                        pipeline.on_hash_complete(sync_hash_complete)
                        pipeline.on_check_complete(sync_check_complete)
                    
                    # Process files through pipeline
                    files_with_rel = [(fp, fp.relative_to(folder_path)) for fp in files_to_check_db]
                    pending_files_result, skipped_paths_set, path_to_source_id, path_to_hash = await pipeline.process(
                        files_with_rel, progress_callback
                    )
                    
                    skipped_hash = len(skipped_paths_set)
                    # Add to existing_files_with_source_id
                    existing_files_with_source_id.update(path_to_source_id)
                    
                    # Use pending files from pipeline result
                    pending_files = pending_files_result
                    
                    # Save hash cache
                    if self._hash_cache:
                        await self._hash_cache.save()
                    
                    if process:
                        await process.complete_phase("hashing", f"{skipped_hash} duplicates found")
                    
                    if skipped_paths_set:
                        logger.debug(
                            "After blake3_hash check: %d files skipped (in database), %d files remaining to upload",
                            skipped_hash, len(files_to_check_db) - skipped_hash
                    )
                else:
                        logger.debug("After blake3_hash check: All %d files are new (not in database)", len(files_to_check_db))
                else:
                    # No deduplicator available, use files from MEGA check
                    pending_files = pending_after_mega
                    
                    if not files_to_check_db:
                        logger.debug("No files to check in database (all were found in MEGA)")
                    elif not self._blake3_deduplicator:
                        logger.warning("Blake3Deduplicator not available, skipping database check")
                
                total_skipped = skipped_mega + skipped_hash
                logger.info(
                    f"Existence check summary: {len(all_files)} individual files, "
                    f"{skipped_mega} skipped by MEGA path, {skipped_hash} skipped by blake3_hash, "
                    f"{synced_count} synced from MEGA to DB, {total_skipped} total skipped, "
                    f"{len(pending_files)} files to upload"
                )
                
                # Step 2.3: Check and regenerate missing previews for existing files
                # NOTE: This is now handled automatically in the pipeline during deduplication
                # (see PipelineDeduplicator._check_and_regenerate_preview)
                # Keeping this code commented for reference in case we need fallback
                # previews_regenerated = 0
                # if existing_files_with_source_id and self._preview_checker:
                #     logger.info("Checking and regenerating missing previews for existing files")
                #     previews_regenerated = await self._preview_checker.check_and_regenerate(
                #         existing_files_with_source_id, dest_path, relative_to=folder_path, progress_callback=progress_callback
                #     )
                #     if previews_regenerated > 0:
                #         logger.info(f"Regenerated {previews_regenerated} missing previews")
                
                if process:
                    async with process._stats_lock:
                        process._stats["skipped"] = total_skipped
                    await process._events.emit("progress", process.stats)
                
                # Step 2.3.5: Check available space for files to upload (only if ManagedStorageService)
                if pending_files and isinstance(self._storage, ManagedStorageService):
                    if process:
                        await process.set_phase(ProcessPhase.CHECKING_SPACE, "Checking storage space...")
                    
                    logger.info("Calculating total size of files to upload...")
                    total_size_to_upload = sum(file_path.stat().st_size for file_path, _ in pending_files)
                    total_size_gb = total_size_to_upload / (1024 ** 3)
                    
                    logger.info(f"Total size to upload: {total_size_gb:.2f} GB ({len(pending_files)} files)")
                    logger.info("Checking total storage availability (across all accounts)...")
                    
                    has_space = await self._storage.check_total_available_space(total_size_to_upload)
                    
                    if not has_space:
                        error_msg = f"No storage space available for {total_size_gb:.2f} GB. Please free up space or add a new MEGA account."
                        logger.error(error_msg)
                        if process:
                            await process.complete_phase("checking_space", "Insufficient space")
                        return FolderUploadResult(
                            success=False,
                            folder_name=folder_path.name,
                            total_files=total,
                            uploaded_files=0,
                            failed_files=len(pending_files),
                            results=all_results,
                            error=error_msg
                        )
                    
                    logger.info("Storage space available")
                    if process:
                        await process.complete_phase("checking_space", f"{total_size_gb:.2f} GB available")
                
                # Step 2.4: Upload individual files if any pending
                if pending_files:
                    if process:
                        await process.set_phase(ProcessPhase.UPLOADING, f"Uploading {len(pending_files)} files...")
                    
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
                await process.set_phase(ProcessPhase.COMPLETED, "Upload complete")
                async with process._stats_lock:
                    process._stats["uploaded"] = uploaded
                    process._stats["failed"] = failed
                await process._events.emit("progress", process.stats)
            
            # Save hash cache on completion
            if self._hash_cache:
                await self._hash_cache.save()
            
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

