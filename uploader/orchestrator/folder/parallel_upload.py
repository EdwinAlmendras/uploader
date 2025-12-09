from pathlib import Path
from typing import Optional, Callable, List, Tuple
import asyncio
import logging
from uploader.orchestrator.folder.file_processor import FileProcessor
from uploader.orchestrator.folder.process import FolderUploadProcess
from uploader.orchestrator.models import UploadResult
from uploader.utils.events import FileProgress
logger = logging.getLogger(__name__)

class ParallelUploadCoordinator:
    """
    Coordinates parallel file uploads with adaptive concurrency.
    
    - Small files (< 5MB): up to 5 simultaneous uploads
    - Large files (>= 5MB): 1 upload at a time
    """
    
    # Threshold: 5MB in bytes
    SMALL_FILE_THRESHOLD = 5 * 1024 * 1024  # 5MB
    
    def __init__(self, file_processor: FileProcessor, max_parallel: int = 1):
        self._file_processor = file_processor
        self._max_parallel = max_parallel
        self._preview_tasks: List[asyncio.Task] = []
        import os
        self._small_file_semaphore = asyncio.Semaphore(os.getenv("UPLOADER_MAX_PARALLEL", 3))
        self._large_file_semaphore = asyncio.Semaphore(1)
    
    async def upload(
        self,
        pending_files: List[Tuple[Path, Path]],
        dest_path: str,
        total: int,
        progress_callback: Optional[Callable] = None,
        process: Optional[FolderUploadProcess] = None
    ) -> List[UploadResult]:
        """
        Upload files in parallel with adaptive concurrency.
        
        - Small files (< 5MB): up to 5 simultaneous uploads
        - Large files (>= 5MB): 1 upload at a time
        
        Files are uploaded immediately, previews are generated in background.
        At the end, waits for all previews to complete before returning.
        """
        # Count files by size for logging
        small_count = 0
        large_count = 0
        for file_path, _ in pending_files:
            try:
                file_size = file_path.stat().st_size
                if file_size < self.SMALL_FILE_THRESHOLD:
                    small_count += 1
                else:
                    large_count += 1
            except (OSError, AttributeError):
                # If we can't get size, treat as small
                small_count += 1
        
        logger.info(
            f"Starting upload: {len(pending_files)} files "
            f"({small_count} small < 5MB: max 5 parallel, "
            f"{large_count} large >= 5MB: 1 at a time)"
        )
        
        # Clear preview tasks from previous runs
        self._preview_tasks.clear()
        
        if progress_callback:
            progress_callback(f"Uploading {len(pending_files)} files...", 0, total)
        
        results = []
        
        # Create tasks for all files (semaphore selection happens inside _upload_single_file)
        tasks = [
            asyncio.create_task(
                self._upload_single_file(
                    file_path=file_path,
                    rel_path=rel_path,
                    dest_path=dest_path,
                    index=idx,
                    total_files=len(pending_files),
                    process=process
                )
            )
            for idx, (file_path, rel_path) in enumerate(pending_files, 1)
        ]
        
        # Process files as they complete
        completed = 0
        for task in asyncio.as_completed(tasks):
            if process and process.is_cancelled:
                logger.info("Upload cancelled by user")
                await self._cancel_remaining_tasks(tasks)
                break
            
            result = await task
            results.append(result)
            completed += 1
            
            if progress_callback:
                status = "✓" if result.success else "✗"
                progress_callback(f"{status} {result.filename}", completed, total)
            
            if process:
                await self._update_process_stats(process, results)
        
        uploaded = sum(1 for r in results if r.success)
        failed = sum(1 for r in results if not r.success)
        logger.info(f"File uploads complete: {uploaded} successful, {failed} failed")
        
        # Wait for all previews to complete
        if self._preview_tasks:
            logger.info(f"Waiting for {len(self._preview_tasks)} preview(s) to complete...")
            preview_results = await asyncio.gather(*self._preview_tasks, return_exceptions=True)
            
            successful_previews = sum(1 for r in preview_results if r and not isinstance(r, Exception))
            failed_previews = sum(1 for r in preview_results if isinstance(r, Exception) or r is None)
            
            if failed_previews > 0:
                logger.warning(f"Preview generation: {successful_previews} successful, {failed_previews} failed")
            else:
                logger.info(f"All {successful_previews} preview(s) generated successfully")
        
        logger.info(f"Upload complete: {uploaded} files, {len(self._preview_tasks)} previews")
        
        return results


    async def _upload_single_file(
        self,
        file_path: Path,
        rel_path: Path,
        dest_path: str,
        index: int,
        total_files: int,
        process: Optional[FolderUploadProcess]
    ) -> UploadResult:
        """
        Upload a single file with proper logging and error handling.
        
        Automatically selects the appropriate semaphore based on file size:
        - Small files (< 5MB): use small_file_semaphore (5 concurrent)
        - Large files (>= 5MB): use large_file_semaphore (1 concurrent)
        """
        
        if process and process.is_cancelled:
            return UploadResult.fail(file_path.name, "Process cancelled")
        
        # Determine file size and select appropriate semaphore
        try:
            file_size = file_path.stat().st_size
            is_small = file_size < self.SMALL_FILE_THRESHOLD
            semaphore = self._small_file_semaphore if is_small else self._large_file_semaphore
            size_category = "small" if is_small else "large"
        except (OSError, AttributeError):
            # If we can't get size, treat as small file
            semaphore = self._small_file_semaphore
            size_category = "small (unknown size)"
            file_size = 0
        
        async with semaphore:
            file_size_mb = file_size / (1024 * 1024) if file_size > 0 else file_path.stat().st_size / (1024 * 1024)
            logger.info(f"[{index}/{total_files}] Uploading ({size_category}): {file_path.name} ({file_size_mb:.2f} MB)")
            
            try:
                # Initialize file tracking
                if process:
                    await self._start_file_tracking(process, file_path)
                
                # Upload the file (this returns immediately, preview is handled separately)
                result, duration, source_id = await self._file_processor.process(
                    file_path,
                    rel_path,
                    dest_path,
                    progress_callback=self._create_progress_tracker(process, file_path) if process else None
                )
                
                # Launch preview generation in background for videos (non-blocking)
                if result.success and duration is not None and source_id:
                    try:
                        # Launch preview task in background
                        preview_task = asyncio.create_task(
                            self._file_processor.generate_preview_background(
                                file_path, source_id, duration
                            )
                        )
                        self._preview_tasks.append(preview_task)
                        logger.debug(f"Launched preview generation in background for {file_path.name}")
                    except Exception as e:
                        logger.warning(f"Failed to launch preview for {file_path.name}: {e}")
                
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
