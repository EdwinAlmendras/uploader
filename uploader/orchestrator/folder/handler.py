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
        self._preview_handler = preview_handler
        self._file_processor = FileProcessor(analyzer, repository, storage, preview_handler, config)
        self._set_processor = ImageSetProcessor(analyzer, repository, storage, config)
        self._existence_checker = FileExistenceChecker(storage)
        self._blake3_deduplicator = Blake3Deduplicator(repository, storage) if repository else None
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
        root_handle = None  # Initialize root_handle
        
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
            
            # Pre-create all folder structure needed (for both sets and individual files)
            # This avoids creating the same folders multiple times
            logger.info("Pre-creating folder structure...")
            root_handle = await self._storage.create_folder(dest_path)
            
            unique_subfolders = set()
            
            # Collect folder paths needed for sets
            if sets:
                for set_folder in sets:
                    set_rel_path = set_folder.relative_to(folder_path)
                    if set_rel_path.parent != Path("."):
                        parts = set_rel_path.parent.parts
                        for i in range(1, len(parts) + 1):
                            intermediate_parts = parts[:i]
                            intermediate_str = "/".join(intermediate_parts)
                            full_intermediate = f"{dest_path}/{intermediate_str}"
                            unique_subfolders.add(full_intermediate)
            
            # Collect folder paths needed for individual files (if any pending)
            # Note: We don't know pending_files yet, so we'll collect folders for all individual files
            # and the deduplication process will handle which ones actually need upload
            if individual_files:
                for file_path in individual_files:
                    rel_path = file_path.relative_to(folder_path)
                    if rel_path.parent != Path("."):
                        parts = rel_path.parent.parts
                        for i in range(1, len(parts) + 1):
                            intermediate_parts = parts[:i]
                            intermediate_str = "/".join(intermediate_parts)
                            full_intermediate = f"{dest_path}/{intermediate_str}"
                            unique_subfolders.add(full_intermediate)
            
            # Create all unique subfolders once
            if unique_subfolders:
                logger.info(f"Creating {len(unique_subfolders)} unique subfolder(s) before processing...")
                for subfolder_path in sorted(unique_subfolders):
                    try:
                        await self._storage.create_folder(subfolder_path)
                    except Exception as e:
                        logger.debug(f"Folder {subfolder_path} might already exist or error: {e}")
            
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
                        # Calculate relative path from folder_path to set_folder to preserve subfolder structure
                        set_rel_path = set_folder.relative_to(folder_path)
                        
                        # Calculate destination path including subfolders (like we do for individual files)
                        if set_rel_path.parent != Path("."):
                            parent_str = set_rel_path.parent.as_posix()  # Always uses / separator
                            set_dest_path = f"{dest_path}/{parent_str}"
                        else:
                            set_dest_path = dest_path
                        
                        # Folder structure already created above, no need to create again
                        
                        set_result, image_results = await self._set_processor.process_set(
                            set_folder,
                            set_dest_path,  # Use calculated destination path with subfolders
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
                
                # Step 2.1: Check files in database by blake3_hash
                skipped_hash = 0
                files_to_check_db = all_files
                path_to_hash = {}  # Will store calculated hashes for later use
                existing_files_with_source_id = {}  # path -> source_id for existing files
                
                if files_to_check_db and self._blake3_deduplicator:
                    if process:
                        await process.set_phase(ProcessPhase.HASHING, "Calculating hashes...")
                    
                    logger.info("Checking individual files in database by blake3_hash (deduplication)")
                    
                    pipeline = PipelineDeduplicator(
                        self._repository,
                        self._storage,
                        hash_cache=self._hash_cache,
                        preview_handler=self._preview_handler,
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
                    pending_files = [(fp, fp.relative_to(folder_path)) for fp in files_to_check_db]
                    
                    if not self._blake3_deduplicator:
                        logger.warning("Blake3Deduplicator not available, skipping database check")
                
                total_skipped = skipped_hash
                logger.info(
                    f"Existence check summary: {len(all_files)} individual files, "
                    f"{skipped_hash} skipped by blake3_hash, "
                    f"{total_skipped} total skipped, "
                    f"{len(pending_files)} files to upload"
                )
                
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
                    
                    # Root folder and subfolders already created at the beginning, no need to create again
                    logger.info("Folder structure ready, starting parallel uploads for individual files...")
                    
                    file_results = await self._upload_coordinator.upload(
                        pending_files, dest_path, total, progress_callback, process
                    )
                    all_results.extend(file_results)
                # Root folder already created at the beginning
            
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

