"""Folder upload handler - handles folder uploads with intelligent parallel processing."""
import asyncio
import logging
import traceback
from pathlib import Path
from typing import Optional, List, Callable, Tuple, Dict, Any
from dataclasses import dataclass, field
from enum import Enum


from mediakit import is_video, is_image
from uploader.services.analyzer import AnalyzerService
from uploader.services.repository import MetadataRepository
from ..models import UploadResult, UploadConfig
from uploader.services import StorageService
from .models import FolderUploadResult
from .file_collector import FileCollector
from .preview_handler import PreviewHandler

logger = logging.getLogger(__name__)


class ProcessState(Enum):
    """State of upload process."""
    PENDING = "pending"
    RUNNING = "running"
    PAUSED = "paused"
    CANCELLED = "cancelled"
    COMPLETED = "completed"
    FAILED = "failed"


@dataclass
class FileProgress:
    """Progress information for a single file."""
    filename: str
    file_path: Path
    bytes_uploaded: int = 0
    total_bytes: int = 0
    percent: float = 0.0
    status: str = "pending"  # pending, uploading, analyzing, completed, failed


class EventEmitter:
    """Simple event emitter for upload events."""
    
    def __init__(self):
        self._listeners: Dict[str, List[Callable]] = {}
        self._lock = asyncio.Lock()
    
    def on(self, event_name: str, callback: Callable):
        """Subscribe to an event."""
        if event_name not in self._listeners:
            self._listeners[event_name] = []
        if callback not in self._listeners[event_name]:
            self._listeners[event_name].append(callback)
    
    def off(self, event_name: str, callback: Callable):
        """Unsubscribe from an event."""
        if event_name in self._listeners:
            if callback in self._listeners[event_name]:
                self._listeners[event_name].remove(callback)
    
    async def emit(self, event_name: str, *args, **kwargs):
        """Emit an event to all listeners."""
        if event_name not in self._listeners:
            return
        
        async with self._lock:
            for callback in self._listeners[event_name][:]:  # Copy list to avoid modification during iteration
                try:
                    if asyncio.iscoroutinefunction(callback):
                        await callback(*args, **kwargs)
                    else:
                        callback(*args, **kwargs)
                except Exception as e:
                    logger.error(f"Error in event listener for {event_name}: {e}")
    
    def _emit_async(self, event_name: str, *args, **kwargs):
        """Emit an event asynchronously without blocking."""
        try:
            loop = asyncio.get_running_loop()
            asyncio.create_task(self.emit(event_name, *args, **kwargs))
        except RuntimeError:
            # If no loop is running, create a new one
            asyncio.run(self.emit(event_name, *args, **kwargs))


class FolderUploadProcess:
    """
    Process object for folder uploads with event-based progress tracking.
    
    Usage:
        process = handler.upload_folder_async(folder_path, dest)
        
        # Subscribe to events
        process.on_file_start(lambda file: print(f"Starting: {file}"))
        process.on_file_progress(lambda file, progress: print(f"{file}: {progress.percent}%"))
        process.on_file_complete(lambda result: print(f"Done: {result.filename}"))
        process.on_finish(lambda result: print(f"All done!"))
        
        # Start (non-blocking)
        await process.start()
        
        # Or wait for completion
        result = await process.wait()
    """
    
    def __init__(
        self,
        handler: 'FolderUploadHandler',
        folder_path: Path,
        dest: Optional[str] = None
    ):
        self._handler = handler
        self._folder_path = Path(folder_path)
        self._dest = dest
        self._events = EventEmitter()
        self._state = ProcessState.PENDING
        self._task: Optional[asyncio.Task] = None
        self._cancelled = False
        self._result: Optional[FolderUploadResult] = None
        self._error: Optional[Exception] = None
        
        # Stats
        self._stats = {
            "total_files": 0,
            "uploaded": 0,
            "failed": 0,
            "skipped": 0,
            "current_file": None,
            "file_progress": {}  # {filename: FileProgress}
        }
        self._stats_lock = asyncio.Lock()
    
    # Event subscription methods
    def on_start(self, callback: Callable[[], None]):
        """Called when upload process starts."""
        self._events.on("start", callback)
    
    def on_file_start(self, callback: Callable[[Path], None]):
        """Called when a file starts uploading. Receives file_path."""
        self._events.on("file_start", callback)
    
    def on_file_progress(self, callback: Callable[[Path, FileProgress], None]):
        """Called when file upload progress updates. Receives file_path and FileProgress."""
        self._events.on("file_progress", callback)
    
    def on_file_complete(self, callback: Callable[[UploadResult], None]):
        """Called when a file completes successfully. Receives UploadResult."""
        self._events.on("file_complete", callback)
    
    def on_file_fail(self, callback: Callable[[UploadResult], None]):
        """Called when a file fails. Receives UploadResult."""
        self._events.on("file_fail", callback)
    
    def on_progress(self, callback: Callable[[Dict[str, Any]], None]):
        """Called with overall progress stats. Receives stats dict."""
        self._events.on("progress", callback)
    
    def on_finish(self, callback: Callable[[FolderUploadResult], None]):
        """Called when all uploads complete. Receives FolderUploadResult."""
        self._events.on("finish", callback)
    
    def on_error(self, callback: Callable[[Exception], None]):
        """Called when a critical error occurs. Receives Exception."""
        self._events.on("error", callback)
    
    # Control methods
    async def start(self):
        """Start the upload process (non-blocking)."""
        if self._state != ProcessState.PENDING:
            raise RuntimeError(f"Cannot start process in state: {self._state}")
        
        self._state = ProcessState.RUNNING
        self._task = asyncio.create_task(self._run())
        await self._events.emit("start")
    
    async def cancel(self):
        """Cancel the upload process."""
        if self._state in (ProcessState.COMPLETED, ProcessState.CANCELLED, ProcessState.FAILED):
            return
        
        self._cancelled = True
        self._state = ProcessState.CANCELLED
        
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
    
    async def wait(self) -> FolderUploadResult:
        """Wait for the upload process to complete and return result."""
        if self._state == ProcessState.PENDING:
            await self.start()
        
        if self._task:
            await self._task
        
        if self._result is None:
            # Create a failed result if we don't have one
            self._result = FolderUploadResult(
                success=False,
                folder_name=self._folder_path.name,
                total_files=0,
                uploaded_files=0,
                failed_files=0,
                results=[],
                error="Process was cancelled or failed without result"
            )
        
        return self._result
    
    # State properties
    @property
    def state(self) -> ProcessState:
        """Current state of the process."""
        return self._state
    
    @property
    def stats(self) -> Dict[str, Any]:
        """Current statistics (thread-safe copy)."""
        return self._stats.copy()
    
    @property
    def result(self) -> Optional[FolderUploadResult]:
        """Final result (None if not completed yet)."""
        return self._result
    
    @property
    def is_running(self) -> bool:
        """Check if process is currently running."""
        return self._state == ProcessState.RUNNING
    
    @property
    def is_completed(self) -> bool:
        """Check if process is completed."""
        return self._state == ProcessState.COMPLETED
    
    @property
    def is_cancelled(self) -> bool:
        """Check if process was cancelled."""
        return self._state == ProcessState.CANCELLED
    
    # Internal methods
    async def _run(self):
        """Internal method that runs the upload process."""
        try:
            # Create progress callback that emits events (sync wrapper for async operations)
            def progress_callback(message: str, completed: int, total: int):
                if self._cancelled:
                    return
                
                # Schedule async operations without blocking
                async def update_progress():
                    async with self._stats_lock:
                        self._stats["total_files"] = total
                        # Update stats based on message
                        if "Uploaded:" in message or "Completed:" in message:
                            self._stats["uploaded"] = completed
                        elif "Failed:" in message:
                            self._stats["failed"] = completed
                    
                    await self._events.emit("progress", self.stats)
                
                # Schedule the async update
                try:
                    loop = asyncio.get_running_loop()
                    loop.create_task(update_progress())
                except RuntimeError:
                    # If no loop is running, this shouldn't happen but handle it
                    pass
            
            # Run the actual upload
            self._result = await self._handler._upload_folder_internal(
                self._folder_path,
                self._dest,
                progress_callback,
                self
            )
            
            if self._cancelled:
                self._state = ProcessState.CANCELLED
            else:
                self._state = ProcessState.COMPLETED
                await self._events.emit("finish", self._result)
        
        except asyncio.CancelledError:
            self._state = ProcessState.CANCELLED
            raise
        except Exception as e:
            self._state = ProcessState.FAILED
            self._error = e
            logger.error(f"Upload process failed: {e}", exc_info=True)
            await self._events.emit("error", e)
            
            # Create failed result
            self._result = FolderUploadResult(
                success=False,
                folder_name=self._folder_path.name,
                total_files=self._stats.get("total_files", 0),
                uploaded_files=self._stats.get("uploaded", 0),
                failed_files=self._stats.get("total_files", 0),
                results=[],
                error=f"{str(e)}\n\n{traceback.format_exc()}"
            )


class FileProcessor:
    """Processes a single file: upload, analyze, and save metadata."""
    
    def __init__(self, analyzer: AnalyzerService, repository: MetadataRepository, storage: StorageService, preview_handler: PreviewHandler, config: UploadConfig):
        self._analyzer = analyzer
        self._repository = repository
        self._storage = storage
        self._preview_handler = preview_handler
        self._config = config
    
    async def process(self, file_path: Path, rel_path: Path, dest_path: str, progress_callback=None) -> UploadResult:
        """Process file: upload to MEGA, then analyze and save metadata."""
        try:
            mega_dest = f"{dest_path}/{rel_path.parent}" if rel_path.parent != Path(".") else dest_path
            
            # Upload first
            logger.info("Uploading %s to MEGA...", file_path.name)
            try:
                handle = await self._storage.upload_video(file_path, mega_dest, None, progress_callback)
                if not handle:
                    logger.error("Failed to upload %s: upload_video returned None", file_path.name)
                    return UploadResult.fail(file_path.name, "Upload to MEGA failed: upload_video returned None")
            except Exception as upload_error:
                error_msg = str(upload_error) if str(upload_error) else f"{type(upload_error).__name__}: {repr(upload_error)}"
                logger.error("Exception during upload of %s: %s", file_path.name, error_msg, exc_info=True)
                return UploadResult.fail(file_path.name, f"Upload to MEGA failed: {error_msg}")
            
            logger.info("Uploaded %s to MEGA (handle: %s)", file_path.name, handle)
            
            # Analyze and save metadata
            logger.info("Analyzing %s...", file_path.name)
            if is_video(file_path):
                tech_data = self._analyzer.analyze_video(file_path)
                source_id = tech_data["source_id"]
                duration = tech_data.get('duration', 0)
                logger.info("Video analyzed: source_id=%s, duration=%.2fs", source_id, duration)
                
                await self._repository.save_document(tech_data)
                await self._repository.save_video_metadata(source_id, tech_data)
                logger.info("Saved metadata to API for %s", file_path.name)
                
                preview_handle = None
                if self._config.generate_preview:
                    preview_handle = await self._preview_handler.upload_preview(
                        file_path, source_id, tech_data.get("duration", 0)
                    )
                    if preview_handle:
                        logger.info("Preview uploaded (handle: %s)", preview_handle)
                
                logger.info("Successfully completed %s", file_path.name)
                # preview_handle can be None, which is acceptable (method accepts None as default)
                return UploadResult.ok(source_id, file_path.name, handle, preview_handle)  # type: ignore[arg-type]
            
            elif is_image(file_path):
                tech_data = self._analyzer.analyze_photo(file_path)
                source_id = tech_data["source_id"]
                width = tech_data.get('width')
                height = tech_data.get('height')
                logger.info("Image analyzed: source_id=%s, size=%sx%s", source_id, width, height)
                
                await self._repository.save_document(tech_data)
                await self._repository.save_photo_metadata(source_id, tech_data)
                logger.info("Saved metadata to API for %s", file_path.name)
                logger.info("Successfully completed %s", file_path.name)
                # Images don't have preview handles (method accepts None as default)
                return UploadResult.ok(source_id, file_path.name, handle, None)  # type: ignore[arg-type]
            
            logger.warning("Unknown file type for %s", file_path.name)
            return UploadResult.fail(file_path.name, "Unknown file type")
        
        except Exception as e:
            error_msg = str(e) if str(e) else f"{type(e).__name__}: {repr(e)}"
            logger.error("Error processing %s: %s", file_path.name, error_msg, exc_info=True)
            return UploadResult.fail(file_path.name, error_msg)


class FileExistenceChecker:
    """Checks which files already exist in MEGA."""
    
    def __init__(self, storage):
        self._storage = storage
    
    async def check(self, all_files: List[Path], folder_path: Path, dest_path: str, 
                   total: int, progress_callback: Optional[Callable]) -> Tuple[List[Tuple[Path, Path]], int]:
        """Return pending files and skipped count."""
        if progress_callback:
            progress_callback("Checking existing files in MEGA...", 0, total)
        
        pending_files = []
        skipped = 0
        
        for idx, file_path in enumerate(all_files):
            rel_path = file_path.relative_to(folder_path)
            mega_dest = f"{dest_path}/{rel_path.parent}" if rel_path.parent != Path(".") else dest_path
            target_path = f"{mega_dest}/{file_path.name}"
            
            if await self._storage.exists(target_path):
                skipped += 1
            else:
                pending_files.append((file_path, rel_path))
            
            if progress_callback and idx % 10 == 0:
                progress_callback(f"Checking: {file_path.name}", idx + 1, total)
        
        if skipped > 0:
            logger.info("Skipped %s files already in MEGA", skipped)
        
        return pending_files, skipped


class ParallelUploadCoordinator:
    """Coordinates parallel file uploads with max concurrency limit."""
    
    def __init__(self, file_processor: FileProcessor, max_parallel: int = 1):
        self._file_processor = file_processor
        self._max_parallel = max_parallel
    
    async def upload(
        self,
        pending_files: List[Tuple[Path, Path]],
        dest_path: str,
        total: int,
        progress_callback: Optional[Callable] = None,
        process: Optional[FolderUploadProcess] = None
    ) -> List[UploadResult]:
        """Upload files in parallel with max concurrency."""
        logger.info(f"Starting upload: {len(pending_files)} files (max {self._max_parallel} parallel)")
        
        if progress_callback:
            progress_callback(f"Uploading {len(pending_files)} files...", 0, total)
        
        results = []
        semaphore = asyncio.Semaphore(self._max_parallel)
        
        # Create tasks for all files
        tasks = [
            self._upload_single_file(
                file_path=file_path,
                rel_path=rel_path,
                dest_path=dest_path,
                index=idx,
                total_files=len(pending_files),
                semaphore=semaphore,
                process=process
            )
            for idx, (file_path, rel_path) in enumerate(pending_files, 1)
        ]
        
        # Process files as they complete
        completed = 0
        for coro in asyncio.as_completed(tasks):
            if process and process.is_cancelled:
                logger.info("Upload cancelled by user")
                await self._cancel_remaining_tasks(tasks)
                break
            
            result = await coro
            results.append(result)
            completed += 1
            
            if progress_callback:
                status = "✓" if result.success else "✗"
                progress_callback(f"{status} {result.filename}", completed, total)
            
            if process:
                await self._update_process_stats(process, results)
        
        uploaded = sum(1 for r in results if r.success)
        failed = sum(1 for r in results if not r.success)
        logger.info(f"Upload complete: {uploaded} successful, {failed} failed")
        
        return results


    async def _upload_single_file(
        self,
        file_path: Path,
        rel_path: Path,
        dest_path: str,
        index: int,
        total_files: int,
        semaphore: asyncio.Semaphore,
        process: Optional[FolderUploadProcess]
    ) -> UploadResult:
        """Upload a single file with proper logging and error handling."""
        
        if process and process.is_cancelled:
            return UploadResult.fail(file_path.name, "Process cancelled")
        
        async with semaphore:
            file_size_mb = file_path.stat().st_size / (1024 * 1024)
            logger.info(f"[{index}/{total_files}] Uploading: {file_path.name} ({file_size_mb:.2f} MB)")
            
            try:
                # Initialize file tracking
                if process:
                    await self._start_file_tracking(process, file_path)
                
                # Upload the file
                result = await self._file_processor.process(
                    file_path,
                    rel_path,
                    dest_path,
                    progress_callback=self._create_progress_tracker(process, file_path) if process else None
                )
                
                # Finalize file tracking
                if process:
                    await self._complete_file_tracking(process, file_path, result)
                
                status = "✓ Success" if result.success else "✗ Failed"
                logger.info(f"[{index}/{total_files}] {status}: {file_path.name}")
                
                return result
                
            except Exception as e:
                error_msg = str(e) or f"{type(e).__name__}"
                logger.error(f"[{index}/{total_files}] Error uploading {file_path.name}: {error_msg}")
                
                result = UploadResult.fail(file_path.name, error_msg)
                
                if process:
                    await process._events.emit("file_fail", result)
                
                return result


    async def _start_file_tracking(
        self,
        process: FolderUploadProcess,
        file_path: Path
    ) -> None:
        """Initialize tracking for a file upload."""
        file_size = file_path.stat().st_size
        
        await process._events.emit("file_start", file_path)
        
        async with process._stats_lock:
            process._stats["current_file"] = file_path.name
            process._stats["file_progress"][file_path.name] = FileProgress(
                filename=file_path.name,
                file_path=file_path,
                total_bytes=file_size,
                status="uploading"
            )


    async def _complete_file_tracking(
        self,
        process: FolderUploadProcess,
        file_path: Path,
        result: UploadResult
    ) -> None:
        """Finalize tracking for a file upload."""
        async with process._stats_lock:
            if file_path.name in process._stats["file_progress"]:
                file_prog = process._stats["file_progress"][file_path.name]
                file_prog.status = "completed" if result.success else "failed"
                file_prog.percent = 100.0
                file_prog.bytes_uploaded = file_path.stat().st_size
        
        event = "file_complete" if result.success else "file_fail"
        await process._events.emit(event, result)


    def _create_progress_tracker(
        self,
        process: FolderUploadProcess,
        file_path: Path
    ) -> Callable:
        """Create a simple progress tracking callback."""
        
        def track_progress(upload_progress):
            """Update file progress during upload."""
            try:
                loop = asyncio.get_running_loop()
                loop.create_task(self._update_file_progress(process, file_path, upload_progress))
            except Exception as e:
                logger.debug(f"Progress tracking error: {e}")
        
        return track_progress


    async def _update_file_progress(
        self,
        process: FolderUploadProcess,
        file_path: Path,
        upload_progress
    ) -> None:
        """Update file progress statistics."""
        try:
            async with process._stats_lock:
                if file_path.name not in process._stats["file_progress"]:
                    return
                
                file_prog = process._stats["file_progress"][file_path.name]
                file_prog.bytes_uploaded = upload_progress.uploaded_bytes
                file_prog.total_bytes = upload_progress.total_bytes
                
                if upload_progress.total_bytes > 0:
                    file_prog.percent = (upload_progress.uploaded_bytes / upload_progress.total_bytes) * 100
                
                await process._events.emit("file_progress", file_path, file_prog)
        except Exception as e:
            logger.debug(f"Error updating progress: {e}")


    async def _update_process_stats(
        self,
        process: FolderUploadProcess,
        results: List[UploadResult]
    ) -> None:
        """Update overall process statistics."""
        async with process._stats_lock:
            process._stats["uploaded"] = sum(1 for r in results if r.success)
            process._stats["failed"] = sum(1 for r in results if not r.success)
        
        await process._events.emit("progress", process.stats)


    async def _cancel_remaining_tasks(self, tasks: List[asyncio.Task]) -> None:
        """Cancel all remaining tasks gracefully."""
        for task in tasks:
            if not task.done():
                task.cancel()
        
        await asyncio.gather(*tasks, return_exceptions=True)


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
        self._upload_coordinator = ParallelUploadCoordinator(self._file_processor, max_parallel=1)
        self._storage = storage
    
    def upload_folder(
        self,
        folder_path: Path,
        dest: Optional[str] = None,
    ) -> FolderUploadProcess:
        """
        Upload entire folder preserving directory structure with event-based progress tracking.
        
        Returns a FolderUploadProcess that can be started and monitored.
        
        Example:
            process = handler.upload_folder(folder_path, dest)
            process.on_file_start(lambda file: print(f"Starting: {file}"))
            process.on_file_complete(lambda result: print(f"Done: {result.filename}"))
            result = await process.wait()  # wait() starts automatically if needed
        
        Args:
            folder_path: Path to folder to upload
            dest: Optional destination path in MEGA
        
        Returns:
            FolderUploadProcess instance
        """
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
            # Check if cancelled
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
            
            # 1. Analyze folder structure - collect all files
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
            
            # 2. Check if all files already exist in MEGA
            dest_path = f"{dest}/{folder_path.name}" if dest else folder_path.name
            pending_files, skipped = await self._existence_checker.check(
                all_files, folder_path, dest_path, total, progress_callback
            )
            
            if process:
                async with process._stats_lock:
                    process._stats["skipped"] = skipped
                await process._events.emit("progress", process.stats)
            
            # 3. If all files exist, return without creating folders
            if not pending_files:
                return FolderUploadResult(
                    success=True,
                    folder_name=folder_path.name,
                    total_files=total,
                    uploaded_files=skipped,
                    failed_files=0,
                    results=[],
                    error=None
                )
            
            # 4. Create folder structure (only if there are pending files)
            root_handle = await self._storage.create_folder(dest_path)
            
            # 5. Upload files with parallel processing
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
                uploaded_files=uploaded + skipped,
                failed_files=failed,
                results=results,
                mega_folder_handle=root_handle
            )
            
        except Exception as e:
            error_msg = f"{str(e)}\n\n{traceback.format_exc()}"
            
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

