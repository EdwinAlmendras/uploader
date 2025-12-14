from enum import Enum
from pathlib import Path
from typing import Optional, Callable, Dict, Any, TYPE_CHECKING
from uploader.utils.events import EventEmitter, FileProgress, PhaseProgress, HashProgress
from uploader.orchestrator.models import FolderUploadResult, UploadResult
import asyncio
import logging
import traceback
logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    from .handler import FolderUploadHandler


class ProcessPhase(Enum):
    """Phase of upload process."""
    DETECTING = "detecting"
    CHECKING_MEGA = "checking_mega"
    HASHING = "hashing"
    CHECKING_DB = "checking_db"
    SYNCING = "syncing"
    CHECKING_SPACE = "checking_space"
    UPLOADING = "uploading"
    COMPLETED = "completed"


class ProcessState(Enum):
    """State of upload process."""
    PENDING = "pending"
    RUNNING = "running"
    PAUSED = "paused"
    CANCELLED = "cancelled"
    COMPLETED = "completed"
    FAILED = "failed"

class FolderUploadProcess:
    """
    Process object for folder uploads with event-based progress tracking.
    
    Usage:
        process = handler.upload_folder_async(folder_path, dest)
        await process.start()
        
        process.on_file_start(lambda file: print(f"Starting: {file}"))
        process.on_file_progress(lambda file, progress: print(f"{file}: {progress.percent}%"))
        process.on_file_complete(lambda result: print(f"Done: {result.filename}"))
        process.on_finish(lambda result: print(f"All done!"))
        
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
            "synced": 0,
            "current_file": None,
            "current_phase": None,
            "phase_progress": {},  # {phase: {current, total, message}}
            "file_progress": {}  # {filename: FileProgress}
        }
        self._stats_lock = asyncio.Lock()
        self._current_phase: Optional[ProcessPhase] = None
    
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
    
    # Phase event subscription methods
    def on_phase_start(self, callback: Callable[[str, str], None]):
        """Called when a phase starts. Receives (phase_name, message)."""
        self._events.on("phase_start", callback)
    
    def on_phase_progress(self, callback: Callable[[PhaseProgress], None]):
        """Called when phase progress updates. Receives PhaseProgress."""
        self._events.on("phase_progress", callback)
    
    def on_phase_complete(self, callback: Callable[[str, str], None]):
        """Called when a phase completes. Receives (phase_name, message)."""
        self._events.on("phase_complete", callback)
    
    def on_hash_start(self, callback: Callable[[str], None]):
        """Called when hash calculation starts for a file. Receives filename."""
        self._events.on("hash_start", callback)
    
    def on_hash_complete(self, callback: Callable[[HashProgress], None]):
        """Called when hash calculation completes. Receives HashProgress."""
        self._events.on("hash_complete", callback)
    
    def on_sync_start(self, callback: Callable[[str], None]):
        """Called when MEGA→DB sync starts for a file. Receives filename."""
        self._events.on("sync_start", callback)
    
    def on_sync_complete(self, callback: Callable[[str, bool], None]):
        """Called when MEGA→DB sync completes. Receives (filename, success)."""
        self._events.on("sync_complete", callback)
    
    def on_check_complete(self, callback: Callable[[str, bool, bool], None]):
        """Called when DB/MEGA check completes. Receives (filename, exists_in_db, exists_in_mega)."""
        self._events.on("check_complete", callback)
    
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
    
    @property
    def current_phase(self) -> Optional[ProcessPhase]:
        """Current processing phase."""
        return self._current_phase
    
    async def set_phase(self, phase: ProcessPhase, message: str = ""):
        """Set current phase and emit phase_start event."""
        self._current_phase = phase
        async with self._stats_lock:
            self._stats["current_phase"] = phase.value
        await self._events.emit("phase_start", phase.value, message)
    
    async def emit_phase_progress(self, phase: str, message: str, current: int, total: int, filename: str = None, from_cache: bool = False):
        """Emit phase progress event."""
        progress = PhaseProgress(
            phase=phase,
            message=message,
            current=current,
            total=total,
            filename=filename,
            from_cache=from_cache
        )
        async with self._stats_lock:
            self._stats["phase_progress"][phase] = {
                "current": current,
                "total": total,
                "message": message
            }
        await self._events.emit("phase_progress", progress)
    
    async def complete_phase(self, phase: str, message: str = ""):
        """Complete a phase and emit phase_complete event."""
        await self._events.emit("phase_complete", phase, message)
    
    async def emit_hash_start(self, filename: str):
        """Emit hash_start event."""
        await self._events.emit("hash_start", filename)
    
    async def emit_hash_complete(self, filename: str, current: int, total: int, from_cache: bool, hash_value: str = None):
        """Emit hash_complete event."""
        progress = HashProgress(
            filename=filename,
            current=current,
            total=total,
            from_cache=from_cache,
            hash_value=hash_value[:16] if hash_value else None
        )
        await self._events.emit("hash_complete", progress)
    
    async def emit_sync_start(self, filename: str):
        """Emit sync_start event."""
        await self._events.emit("sync_start", filename)
    
    async def emit_sync_complete(self, filename: str, success: bool):
        """Emit sync_complete event."""
        if success:
            async with self._stats_lock:
                self._stats["synced"] = self._stats.get("synced", 0) + 1
        await self._events.emit("sync_complete", filename, success)
    
    # Internal methods
    async def _run(self):
        """Internal method that runs the upload process."""
        try:
            # Create progress callback that emits events (sync wrapper for async operations)
            def progress_callback(message_or_progress, completed: int = None, total: int = None):
                """
                Progress callback that handles both formats:
                1. (message: str, completed: int, total: int) - from folder upload
                2. (progress: object) - from MegaClient upload
                """
                if self._cancelled:
                    return
                
                # Handle MegaClient progress format (single argument with progress object)
                if completed is None and total is None:
                    # Assume it's a progress object from MegaClient
                    try:
                        # Try to extract progress info from the object
                        if hasattr(message_or_progress, 'uploaded_bytes') and hasattr(message_or_progress, 'total_bytes'):
                            completed = message_or_progress.uploaded_bytes
                            total = message_or_progress.total_bytes
                            message = f"Uploading: {completed}/{total} bytes"
                        elif hasattr(message_or_progress, 'percent'):
                            # If it has percent, we can't determine total, so skip
                            return
                        else:
                            # Unknown format, skip
                            return
                    except Exception:
                        # If we can't parse it, skip
                        return
                else:
                    # Standard format: (message, completed, total)
                    message = message_or_progress
                
                # Schedule async operations without blocking
                async def update_progress():
                    async with self._stats_lock:
                        if total is not None:
                        self._stats["total_files"] = total
                        # Update stats based on message
                        if completed is not None:
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


