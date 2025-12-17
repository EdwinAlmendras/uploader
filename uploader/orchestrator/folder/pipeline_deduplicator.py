"""
Pipeline Deduplicator - Concurrent hash calculation, DB check, and upload.

Flow:
1. HashWorker: Calculates blake3 hash for one file at a time
2. CheckWorker: Checks if hash exists in DB
3. Files that don't exist go to upload queue
4. All workers run concurrently (pipeline parallelism)

This reduces total time from (hash_time + upload_time) to max(hash_time, upload_time)
"""
import asyncio
import logging
from pathlib import Path
from typing import Optional, Callable, Dict, List, Tuple, Set, Any
from dataclasses import dataclass

from uploader.services.repository import MetadataRepository
from uploader.services.storage import StorageService
from uploader.services.managed_storage import ManagedStorageService
from uploader.services.resume import blake3_file

# Import for video detection
try:
    from mediakit import is_video
except ImportError:
    def is_video(path):
        """Fallback if mediakit not available."""
        video_exts = {'.mp4', '.mkv', '.avi', '.mov', '.webm', '.flv', '.wmv', '.m4v', '.mpg', '.mpeg'}
        return Path(path).suffix.lower() in video_exts

logger = logging.getLogger(__name__)


@dataclass
class FileHashResult:
    """Result of hashing a file."""
    file_path: Path
    rel_path: Path
    blake3_hash: Optional[str]
    file_size: int = 0  # File size in bytes, used to determine if batch check should be used
    error: Optional[str] = None


@dataclass
class FileCheckResult:
    """Result of checking a file in DB."""
    file_path: Path
    rel_path: Path
    blake3_hash: str
    exists_in_db: bool
    exists_in_mega: bool
    source_id: Optional[str] = None


class PipelineDeduplicator:
    """
    Concurrent pipeline for deduplication with progress events.
    
    Processes files through a pipeline:
    1. Hash worker: Calculates blake3 hash (one at a time)
    2. Check worker: Checks DB for existence (batch for small files, individual for large)
    3. Files not found go to pending queue for upload
    
    Emits events for each phase via callbacks.
    """
    
    # Threshold: 5MB in bytes - files smaller than this use batch checks
    SMALL_FILE_THRESHOLD = 5 * 1024 * 1024  # 5MB
    BATCH_CHECK_SIZE = 50  # Number of small file hashes to accumulate before batch check

    def __init__(
        self,
        repository: MetadataRepository,
        storage: StorageService,
        hash_cache: Optional['HashCache'] = None,
        preview_handler=None,
        analyzer=None,
    ):
        self._repository = repository
        self._storage = storage
        self._manager = storage.manager if isinstance(storage, ManagedStorageService) else None
        self._hash_cache = hash_cache
        self._preview_handler = preview_handler
        self._analyzer = analyzer
        
        # Queues for pipeline
        self._hash_queue: asyncio.Queue[Optional[Tuple[Path, Path]]] = asyncio.Queue()
        self._check_queue: asyncio.Queue[Optional[FileHashResult]] = asyncio.Queue()
        
        # Results
        self._pending_files: List[Tuple[Path, Path]] = []
        self._skipped_paths: Set[Path] = set()
        self._path_to_source_id: Dict[Path, str] = {}
        self._path_to_hash: Dict[Path, str] = {}
        
        # Callbacks for progress
        self._on_hash_start: Optional[Callable[[str], None]] = None
        self._on_hash_complete: Optional[Callable[[str, str, bool], None]] = None
        self._on_check_complete: Optional[Callable[[str, bool, bool], None]] = None
        self._on_progress: Optional[Callable[[str, int, int], None]] = None

    def on_hash_start(self, callback: Callable[[str], None]):
        """Set callback for when hash calculation starts for a file."""
        self._on_hash_start = callback

    def on_hash_complete(self, callback: Callable[[str, str, bool], None]):
        """Set callback for when hash calculation completes (filename, hash, from_cache)."""
        self._on_hash_complete = callback

    def on_check_complete(self, callback: Callable[[str, bool, bool], None]):
        """Set callback for when DB check completes (filename, exists_db, exists_mega)."""
        self._on_check_complete = callback

    def on_progress(self, callback: Callable[[str, int, int], None]):
        """Set callback for overall progress (phase, current, total)."""
        self._on_progress = callback

    async def process(
        self,
        files: List[Tuple[Path, Path]],
        progress_callback: Optional[Callable[[str, int, int], None]] = None
    ) -> Tuple[List[Tuple[Path, Path]], Set[Path], Dict[Path, str], Dict[Path, str]]:
        """
        Process files through the deduplication pipeline.
        
        Args:
            files: List of (file_path, rel_path) tuples
            progress_callback: Optional legacy progress callback
            
        Returns:
            Tuple of:
            - pending_files: List of (file_path, rel_path) for files to upload
            - skipped_paths: Set of paths that exist in both DB and MEGA
            - path_to_source_id: Dict mapping path -> source_id for existing files
            - path_to_hash: Dict mapping path -> blake3_hash for all processed files
        """
        if not files:
            return [], set(), {}, {}
        
        # Set legacy callback
        if progress_callback:
            self._on_progress = progress_callback
        
        total = len(files)
        logger.info("Starting pipeline for %d files", total)
        
        # Reset state
        self._pending_files = []
        self._skipped_paths = set()
        self._path_to_source_id = {}
        self._path_to_hash = {}
        
        # Fill hash queue
        for file_path, rel_path in files:
            await self._hash_queue.put((file_path, rel_path))
        
        # Add sentinel to signal end
        await self._hash_queue.put(None)
        
        # Create counters for progress tracking
        progress_state = {"hashed": 0, "checked": 0, "total": total}
        
        # Start workers concurrently
        hash_task = asyncio.create_task(
            self._hash_worker(progress_state)
        )
        check_task = asyncio.create_task(
            self._check_worker(progress_state)
        )
        
        # Wait for all workers to complete
        await asyncio.gather(hash_task, check_task)
        
        logger.info("Completed - %d pending, %d skipped", len(self._pending_files), len(self._skipped_paths))
        
        return (
            self._pending_files,
            self._skipped_paths,
            self._path_to_source_id,
            self._path_to_hash
        )

    async def _hash_worker(self, progress_state: dict):
        """Worker that calculates blake3 hashes one at a time."""
        while True:
            item = await self._hash_queue.get()
            
            if item is None:
                # Signal end to check worker
                await self._check_queue.put(None)
                break
            
            file_path, rel_path = item
            
            # Emit hash start event
            if self._on_hash_start:
                try:
                    self._on_hash_start(file_path.name)
                except Exception as e:
                    logger.debug("Error in hash_start callback: %s", e)
            
            # Check cache first if available
            from_cache = False
            blake3_hash = None
            
            if self._hash_cache:
                cached_hash = await self._hash_cache.get(file_path)
                if cached_hash:
                    blake3_hash = cached_hash
                    from_cache = True
                    logger.debug("Hash from cache for '%s': %s...", file_path.name, blake3_hash[:16])
            
            # Calculate hash if not in cache
            if not blake3_hash:
                try:
                    blake3_hash = await blake3_file(file_path)
                    logger.debug("Calculated hash for '%s': %s...", file_path.name, blake3_hash[:16])
                    
                    # Save to cache if available
                    if self._hash_cache:
                        await self._hash_cache.set(file_path, blake3_hash)
                        
                except Exception as e:
                    logger.error("Hash failed for '%s': %s", file_path.name, e)
                    blake3_hash = None
            
            # Update progress
            progress_state["hashed"] += 1
            
            # Emit hash complete event
            if self._on_hash_complete:
                try:
                    self._on_hash_complete(file_path.name, blake3_hash or "", from_cache)
                except Exception as e:
                    logger.debug("Error in hash_complete callback: %s", e)
            
            # Emit progress event
            if self._on_progress:
                try:
                    self._on_progress(
                        f"Calculating hash {progress_state['hashed']}/{progress_state['total']}: {file_path.name}",
                        progress_state['hashed'],
                        progress_state['total']
                    )
                except Exception as e:
                    logger.debug("Error in progress callback: %s", e)
            
            # Get file size for batch check decision
            try:
                file_size = file_path.stat().st_size
            except (OSError, AttributeError):
                file_size = 0
            
            # Send to check worker
            result = FileHashResult(
                file_path=file_path,
                rel_path=rel_path,
                blake3_hash=blake3_hash,
                file_size=file_size,
                error=None if blake3_hash else "Hash calculation failed"
            )
            await self._check_queue.put(result)

    async def _check_worker(self, progress_state: dict):
        """Worker that checks files in DB and MEGA. Uses batch checks for small files."""
        small_file_buffer: List[FileHashResult] = []
        
        while True:
            # Process buffer if it's full or if we're done receiving files
            if len(small_file_buffer) >= self.BATCH_CHECK_SIZE:
                await self._process_small_file_batch(small_file_buffer, progress_state)
                small_file_buffer.clear()
            
            # Get next result
            result = await self._check_queue.get()
            
            if result is None:
                # End of queue - process remaining buffer
                if small_file_buffer:
                    await self._process_small_file_batch(small_file_buffer, progress_state)
                    small_file_buffer.clear()
                break
            
            if not result.blake3_hash:
                # Hash failed, add to pending (will be uploaded and hashed again)
                self._pending_files.append((result.file_path, result.rel_path))
                continue
            
            # Store hash mapping
            self._path_to_hash[result.file_path] = result.blake3_hash
            
            # Determine if file is small or large
            is_small = result.file_size < self.SMALL_FILE_THRESHOLD
            
            if is_small:
                # Accumulate small files for batch processing
                small_file_buffer.append(result)
            else:
                # Process large files immediately (one by one)
                await self._check_single_file(result, progress_state)
        
        logger.debug("Check worker finished - processed all files")
    
    async def _process_small_file_batch(self, batch: List[FileHashResult], progress_state: dict):
        """Process a batch of small files with a single DB check."""
        if not batch:
            return
        
        logger.debug("Processing batch of %d small files", len(batch))
        
        # Collect all hashes
        batch_hashes = [r.blake3_hash for r in batch if r.blake3_hash]
        
        if not batch_hashes:
            # No valid hashes, add all to pending
            for result in batch:
                self._pending_files.append((result.file_path, result.rel_path))
                progress_state["checked"] += 1
            return
        
        # Batch check in DB
        existing_hashes: Dict[str, Any] = {}
        try:
            existing_hashes = await self._repository.check_exists_batch(batch_hashes)
            logger.debug("Batch check returned %d existing hashes out of %d checked", len(existing_hashes), len(batch_hashes))
        except Exception as e:
            logger.warning("Batch DB check failed: %s", e)
        
        # Process each file in the batch
        for result in batch:
            await self._process_file_with_db_result(result, existing_hashes, progress_state)
    
    async def _check_single_file(self, result: FileHashResult, progress_state: dict):
        """Check a single file (used for large files)."""
        # Check if exists in DB
        existing_hashes: Dict[str, Any] = {}
        try:
            existing_hashes = await self._repository.check_exists_batch([result.blake3_hash])
        except Exception as e:
            logger.warning("DB check failed for '%s': %s", result.file_path.name, e)
        
        await self._process_file_with_db_result(result, existing_hashes, progress_state)
    
    async def _process_file_with_db_result(
        self,
        result: FileHashResult,
        existing_hashes: Dict[str, Any],
        progress_state: dict
    ):
        """Process a file using the DB check results."""
        exists_in_db = False
        source_id = None
        mega_handle = None
        
        if result.blake3_hash in existing_hashes:
            doc_info = existing_hashes[result.blake3_hash]
            exists_in_db = True
            
            # Handle both old and new format
            if isinstance(doc_info, dict):
                source_id = doc_info.get("source_id")
                mega_handle = doc_info.get("mega_handle")
            else:
                source_id = doc_info
            
            logger.debug("'%s' exists in DB (source_id: %s)", result.file_path.name, source_id)
        
        # If exists in DB, verify it also exists in MEGA
        exists_in_mega = False
        if exists_in_db and source_id:
            try:
                if self._manager:
                    exists_in_mega = await self._manager.find_by_mega_id(source_id) is not None
                else:
                    exists_in_mega = await self._storage.exists_by_mega_id(source_id)
            except Exception as e:
                logger.warning("MEGA check failed for '%s': %s", result.file_path.name, e)
        
        # Update progress
        progress_state["checked"] += 1
        
        # Emit check complete event
        if self._on_check_complete:
            try:
                self._on_check_complete(result.file_path.name, exists_in_db, exists_in_mega)
            except Exception as e:
                logger.debug("Error in check_complete callback: %s", e)
        
        # Decide if file should be uploaded
        if exists_in_db and exists_in_mega:
            # Skip - exists in both
            self._skipped_paths.add(result.file_path)
            self._path_to_source_id[result.file_path] = source_id
            logger.debug("✓ '%s' exists in DB and MEGA - SKIP", result.file_path.name)
            
            # Check and regenerate preview if missing (for videos only)
            if is_video(result.file_path) and self._preview_handler and self._analyzer:
                await self._check_and_regenerate_preview(result.file_path, source_id)
            
        elif exists_in_db and not exists_in_mega:
            # Exists in DB but not MEGA - re-upload
            self._pending_files.append((result.file_path, result.rel_path))
            logger.debug("'%s' in DB but not MEGA - WILL RE-UPLOAD", result.file_path.name)
        else:
            # New file - upload
            self._pending_files.append((result.file_path, result.rel_path))
            logger.debug("'%s' is NEW - WILL UPLOAD", result.file_path.name)
    
    async def _check_and_regenerate_preview(self, file_path: Path, source_id: str):
        """
        Check if preview exists for a video and regenerate if missing.
        
        Uses the real MEGA path of the video (obtained via source_id) to determine
        where the preview should be.
        
        Args:
            file_path: Local path to the video file
            source_id: Source ID (mega_id) of the video in MEGA
        """
        try:
            # Get the actual node from MEGA using source_id
            node_info = None
            if self._manager:
                node_info = await self._manager.find_by_mega_id(source_id)
            else:
                # This is a limitation - single storage doesn't have find_by_mega_id
                logger.debug("Cannot check preview for '%s' - single storage mode", file_path.name)
                return
            
            if not node_info:
                logger.warning("Video node not found in MEGA for '%s' (source_id: %s)", file_path.name, source_id)
                return
            
            _, node = node_info
            
            # Get the full path of the video in MEGA
            video_path = node.path
            logger.debug(f"Video path: {video_path}")

        
            # Construct preview path by changing extension to .jpg
            preview_path = video_path.rsplit('.', 1)[0] + '.jpg'
            logger.debug(f"Preview path: {preview_path}")
            preview_exists = await self._storage.exists(preview_path)
            if preview_exists:
                logger.debug("Preview exists for '%s'", file_path.name)
            else:
                logger.debug("✗ Preview missing for '%s' - REGENERATING", file_path.name)
                # Analyze video to get duration
                tech_data = await self._analyzer.analyze_video_async(file_path)
                duration = tech_data.get('duration', 0)
                
                if duration > 0:
                    # Extract dest_path and filename from video_path
                    # video_path example: "Videos/Lisa/video.mp4"
                    path_parts = video_path.rsplit('/', 1)
                    if len(path_parts) == 2:
                        dest_path, video_filename = path_parts
                    else:
                        dest_path = None
                        video_filename = path_parts[0]
                    
                    # Regenerate and upload preview
                    preview_handle = await self._preview_handler.upload_preview(
                        file_path,
                        source_id,
                        duration,
                        dest_path=dest_path,
                        filename=video_filename
                    )
                    
                    if preview_handle:
                        logger.debug("✓ Preview regenerated for '%s'", file_path.name)
                    else:
                        logger.warning("Failed to regenerate preview for '%s'", file_path.name)
                else:
                    logger.warning("Invalid duration for '%s' - skipping preview", file_path.name)
                    
        except Exception as e:
            logger.error("Error checking/regenerating preview for '%s': %s", file_path.name, e, exc_info=True)
