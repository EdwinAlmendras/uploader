"""
Upload Orchestrator - Coordinates upload workflow using services.

Implements Template Method pattern for upload flow.
Uses Dependency Injection for services.

Flow:
1. Analyze video/photo (AnalyzerService)
2. Save to DB (MetadataRepository)
3. Upload to MEGA (StorageService)
4. Optional: Generate and upload preview

Supports:
- Single video upload
- Single photo upload
- Folder upload (preserves directory structure)
- Social video upload (with channel metadata)
"""
from pathlib import Path
from typing import Optional, List, Callable, Tuple
from dataclasses import dataclass, field
import asyncio

from .models import UploadResult, UploadStatus, SocialInfo, UploadConfig, TelegramInfo
from .protocols import IStorageClient
from .services.analyzer import AnalyzerService
from .services.repository import MetadataRepository, HTTPAPIClient
from .services.preview import PreviewService
from .services.storage import StorageService

# Import extensions from mediakit
from mediakit import is_video, is_image


@dataclass
class FolderUploadResult:
    """Result of folder upload."""
    success: bool
    folder_name: str
    total_files: int
    uploaded_files: int
    failed_files: int
    results: List[UploadResult]
    mega_folder_handle: Optional[str] = None
    error: Optional[str] = None
    
    @property
    def all_success(self) -> bool:
        return self.failed_files == 0


@dataclass
class UploadTask:
    """Task for parallel upload."""
    file_path: Path
    source_id: str
    tech_data: dict
    is_video: bool
    rel_path: Path
    mega_dest: str
    file_size: int = 0


def _get_parallel_count(avg_size: float) -> int:
    """
    Get optimal parallel upload count based on average file size.
    
    Small files benefit from more parallelism.
    Large files already use 21 connections each, so limit parallelism.
    """
    MB = 1024 * 1024
    
    if avg_size < 1 * MB:
        return 10  # Small files: high parallelism
    elif avg_size < 10 * MB:
        return 6   # Medium files: moderate parallelism
    else:
        return 3   # Large files: low parallelism (each uses 21 connections)


class UploadOrchestrator:
    """
    Orchestrates video uploads using injected services.
    
    Follows:
    - Dependency Injection (services injected)
    - Single Responsibility (delegates to services)
    - Open/Closed (extend via new services)
    
    Usage:
        # With single MEGA client
        async with UploadOrchestrator(api_url, mega) as uploader:
            result = await uploader.upload(video_path)
        
        # With ManagedStorageService (multi-account)
        managed = await ManagedStorageService().start()
        async with UploadOrchestrator(api_url, storage_service=managed) as uploader:
            result = await uploader.upload(video_path)
    """
    
    def __init__(
        self,
        api_url: str,
        storage_client: IStorageClient = None,
        config: Optional[UploadConfig] = None,
        storage_service = None,
    ):
        """
        Initialize orchestrator with dependencies.
        
        Args:
            api_url: Datastore API URL
            storage_client: Storage client (MEGA) - deprecated, use storage_service
            config: Upload configuration
            storage_service: Pre-built storage service (StorageService or ManagedStorageService)
        """
        self._api_url = api_url
        self._storage_client = storage_client
        self._config = config or UploadConfig()
        self._external_storage = storage_service
        
        # Services (initialized in __aenter__)
        self._api_client: Optional[HTTPAPIClient] = None
        self._analyzer: Optional[AnalyzerService] = None
        self._repository: Optional[MetadataRepository] = None
        self._preview: Optional[PreviewService] = None
        self._storage = None
    
    async def __aenter__(self):
        """Initialize services."""
        # API client
        self._api_client = HTTPAPIClient(self._api_url)
        await self._api_client.__aenter__()
        
        # Services with dependency injection
        self._analyzer = AnalyzerService()
        self._repository = MetadataRepository(self._api_client)
        self._preview = PreviewService(self._config)
        
        # Use external storage service if provided, otherwise create one
        if self._external_storage:
            self._storage = self._external_storage
        elif self._storage_client:
            self._storage = StorageService(self._storage_client, self._config)
        else:
            raise ValueError("Either storage_client or storage_service must be provided")
        
        return self
    
    async def __aexit__(self, *args):
        """Cleanup resources."""
        if self._api_client:
            await self._api_client.__aexit__(*args)
    
    async def upload(
        self,
        path: Path,
        dest: Optional[str] = None,
        progress_callback=None
    ) -> UploadResult:
        """
        Upload video with metadata (Template Method).
        
        Args:
            path: Path to video file
            dest: Destination folder in MEGA
            progress_callback: Optional progress callback
            
        Returns:
            UploadResult
        """
        path = Path(path)
        
        try:
            # 1. Analyze
            tech_data = await self._analyzer.analyze_async(path)
            source_id = tech_data["source_id"]
            duration = tech_data.get("duration", 0)
            
            # 2. Save metadata
            await self._repository.save_document(tech_data)
            await self._repository.save_video_metadata(source_id, tech_data)
            
            # 3. Upload video
            mega_handle = await self._storage.upload_video(
                path, dest, source_id, progress_callback
            )
            
            if not mega_handle:
                return UploadResult.fail(path.name, "Upload to MEGA failed")
            
            # 4. Generate and upload preview
            preview_handle = None
            if self._config.generate_preview:
                preview_handle = await self._upload_preview(path, source_id, duration)
            
            return UploadResult.ok(
                source_id=source_id,
                filename=path.name,
                mega_handle=mega_handle,
                preview_handle=preview_handle
            )
            
        except Exception as e:
            import traceback
            traceback.print_exc()
            return UploadResult.fail(path.name, str(e))
    
    async def upload_social(
        self,
        path: Path,
        social_info: SocialInfo,
        dest: Optional[str] = None,
        progress_callback=None
    ) -> UploadResult:
        """
        Upload social video with channel metadata.
        
        Args:
            path: Path to video file
            social_info: Social metadata from yt-dlp
            dest: Destination folder in MEGA
            progress_callback: Optional progress callback
            
        Returns:
            UploadResult
        """
        path = Path(path)
        
        try:
            # 1. Analyze
            tech_data = await self._analyzer.analyze_async(path)
            source_id = tech_data["source_id"]
            duration = tech_data.get("duration", 0)
            
            # 2. Save metadata (document + video_metadata)
            await self._repository.save_document(tech_data)
            await self._repository.save_video_metadata(source_id, tech_data)
            
            # 3. Save social info (channel + social_video)
            await self._repository.save_social_info(source_id, social_info)
            
            # 4. Upload video
            mega_handle = await self._storage.upload_video(
                path, dest, source_id, progress_callback
            )
            
            if not mega_handle:
                return UploadResult.fail(path.name, "Upload to MEGA failed")
            
            # 5. Generate and upload preview
            preview_handle = None
            if self._config.generate_preview:
                preview_handle = await self._upload_preview(path, source_id, duration)
            
            return UploadResult.ok(
                source_id=source_id,
                filename=path.name,
                mega_handle=mega_handle,
                preview_handle=preview_handle
            )
            
        except Exception as e:
            return UploadResult.fail(path.name, str(e))
    
    async def upload_photo(
        self,
        path: Path,
        dest: Optional[str] = None,
        progress_callback=None
    ) -> UploadResult:
        """
        Upload photo with metadata.
        
        Args:
            path: Path to photo file
            dest: Destination folder in MEGA
            progress_callback: Optional progress callback
            
        Returns:
            UploadResult
        """
        path = Path(path)
        
        try:
            # 1. Analyze
            photo_data = await self._analyzer.analyze_photo_async(path)
            source_id = photo_data["source_id"]
            
            # 2. Save metadata
            await self._repository.save_document(photo_data)
            await self._repository.save_photo_metadata(source_id, photo_data)
            
            # 3. Upload photo
            mega_handle = await self._storage.upload_video(
                path, dest, source_id, progress_callback
            )
            
            if not mega_handle:
                return UploadResult.fail(path.name, "Upload to MEGA failed")
            
            return UploadResult.ok(
                source_id=source_id,
                filename=path.name,
                mega_handle=mega_handle
            )
            
        except Exception as e:
            return UploadResult.fail(path.name, str(e))
    
    async def upload_auto(
        self,
        path: Path,
        dest: Optional[str] = None,
        progress_callback=None
    ) -> UploadResult:
        """
        Upload media file (auto-detect video or photo).
        
        Args:
            path: Path to media file
            dest: Destination folder in MEGA
            progress_callback: Optional progress callback
            
        Returns:
            UploadResult
        """
        path = Path(path)
        
        if self._analyzer.is_video(path):
            return await self.upload(path, dest, progress_callback)
        elif self._analyzer.is_photo(path):
            return await self.upload_photo(path, dest, progress_callback)
        else:
            return UploadResult.fail(path.name, f"Unsupported file type: {path.suffix}")
    
    async def upload_telegram(
        self,
        path: Path,
        telegram_info: Optional[TelegramInfo] = None,
        dest: Optional[str] = None,
        progress_callback=None
    ) -> UploadResult:
        """
        Upload media with optional Telegram metadata.
        
        Args:
            path: Path to media file
            telegram_info: Optional Telegram metadata
            dest: Destination folder in MEGA
            progress_callback: Optional progress callback
            
        Returns:
            UploadResult
        """
        path = Path(path)
        
        try:
            # 1. Detect type and analyze
            if self._analyzer.is_video(path):
                tech_data = await self._analyzer.analyze_async(path)
                source_id = tech_data["source_id"]
                duration = tech_data.get("duration", 0)
                is_video = True
            elif self._analyzer.is_photo(path):
                tech_data = await self._analyzer.analyze_photo_async(path)
                source_id = tech_data["source_id"]
                duration = 0
                is_video = False
            else:
                return UploadResult.fail(path.name, f"Unsupported file type: {path.suffix}")
            
            # 2. Save base metadata
            await self._repository.save_document(tech_data)
            
            if is_video:
                await self._repository.save_video_metadata(source_id, tech_data)
            else:
                await self._repository.save_photo_metadata(source_id, tech_data)
            
            # 3. Save Telegram metadata if provided
            if telegram_info:
                await self._repository.save_telegram(source_id, telegram_info)
            
            # 4. Upload to MEGA
            mega_handle = await self._storage.upload_video(
                path, dest, source_id, progress_callback
            )
            
            if not mega_handle:
                return UploadResult.fail(path.name, "Upload to MEGA failed")
            
            # 5. Generate and upload preview for videos
            preview_handle = None
            if is_video and self._config.generate_preview:
                preview_handle = await self._upload_preview(path, source_id, duration)
            
            return UploadResult.ok(
                source_id=source_id,
                filename=path.name,
                mega_handle=mega_handle,
                preview_handle=preview_handle
            )
            
        except Exception as e:
            import traceback
            traceback.print_exc()
            return UploadResult.fail(path.name, str(e))
    
    async def _upload_preview(
        self,
        path: Path,
        source_id: str,
        duration: float
    ) -> Optional[str]:
        """Generate and upload preview grid to /.previews/{source_id}.jpg"""
        try:
            # Generate grid (3x3, 4x4, or 5x5 based on duration)
            grid_path = await self._preview.generate(path, duration)
            
            if not grid_path or not grid_path.exists():
                print(f"[preview] Failed to generate grid for {path.name}")
                return None
            
            # Upload to /.previews/{source_id}.jpg
            handle = await self._storage.upload_preview(grid_path, source_id)
            
            # Cleanup temp file
            try:
                grid_path.unlink(missing_ok=True)
                # Also cleanup temp dir
                if grid_path.parent.name.startswith("preview_"):
                    import shutil
                    shutil.rmtree(grid_path.parent, ignore_errors=True)
            except:
                pass
            
            if handle:
                print(f"[preview] Uploaded: /.previews/{source_id}.jpg")
            else:
                print(f"[preview] Upload failed for {source_id}")
            
            return handle
            
        except Exception as e:
            print(f"[preview] Error: {e}")
            return None
    
    async def upload_folder(
        self,
        folder_path: Path,
        dest: Optional[str] = None,
        progress_callback: Optional[Callable[[str, int, int], None]] = None,
    ) -> FolderUploadResult:
        """
        Upload entire folder preserving directory structure.
        
        Flow:
        1. Collect all media files
        2. Analyze all files (extract metadata)
        3. Generate previews for videos
        4. Send ALL metadata to API in single batch request
        5. Upload files to MEGA with mega_id
        6. Upload previews to /.previews/
        
        Args:
            folder_path: Path to folder to upload
            dest: Destination folder in MEGA (e.g., "/Sets")
            progress_callback: Called with (stage, current, total)
            
        Returns:
            FolderUploadResult
        """
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
        
        # Track total for error reporting
        total = 0
        
        try:
            # 1. Collect all files
            all_files = self._collect_files(folder_path)
            total = len(all_files)
            
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
            
            # 2. Analyze all files and prepare batch
            if progress_callback:
                progress_callback("Analyzing files...", 0, total)
            
            batch_items = []
            file_data = []  # (path, source_id, tech_data, is_video, rel_path)
            
            for idx, file_path in enumerate(all_files):
                rel_path = file_path.relative_to(folder_path)
                
                if is_video(file_path):
                    tech_data = self._analyzer.analyze_video(file_path)
                    source_id = tech_data["source_id"]
                    
                    batch_items.append({
                        "document": self._repository.prepare_document(tech_data),
                        "video_metadata": self._repository.prepare_video_metadata(source_id, tech_data),
                    })
                    file_data.append((file_path, source_id, tech_data, True, rel_path))
                    
                elif is_image(file_path):
                    tech_data = self._analyzer.analyze_photo(file_path)
                    source_id = tech_data["source_id"]
                    
                    batch_items.append({
                        "document": self._repository.prepare_document(tech_data),
                        "photo_metadata": self._repository.prepare_photo_metadata(source_id, tech_data),
                    })
                    file_data.append((file_path, source_id, tech_data, False, rel_path))
                
                if progress_callback:
                    progress_callback(f"Analyzed: {file_path.name}", idx + 1, total)
            
            # 3. Send batch to API (single request)
            if progress_callback:
                progress_callback("Saving metadata to API...", 0, 1)
            
            await self._repository.save_batch(batch_items)
            
            # 4. Create folder structure and upload files
            dest_path = f"{dest}/{folder_path.name}" if dest else folder_path.name
            root_handle = await self._storage.create_folder(dest_path)
            
            # 4.1 Check existing files for resume support
            existing_files = await self._get_existing_files(dest_path)
            
            # Filter out already uploaded files
            pending_data = []
            skipped = 0
            for file_path, source_id, tech_data, is_vid, rel_path in file_data:
                # Check if file already exists by source_id (filename is source_id.ext in MEGA)
                filename = f"{source_id}{file_path.suffix}"
                if filename in existing_files or file_path.name in existing_files:
                    skipped += 1
                    if progress_callback:
                        progress_callback(f"Skipped (exists): {file_path.name}", skipped, total)
                else:
                    pending_data.append((file_path, source_id, tech_data, is_vid, rel_path))
            
            if skipped > 0:
                print(f"[resume] Skipped {skipped} already uploaded files")
            
            # Calculate average file size for parallel count
            total_size = sum(f.stat().st_size for f, _, _, _, _ in pending_data) if pending_data else 0
            avg_size = total_size / len(pending_data) if pending_data else 0
            parallel_count = _get_parallel_count(avg_size)
            
            if progress_callback:
                progress_callback(f"Uploading with {parallel_count} parallel uploads...", 0, total)
            
            # Prepare upload tasks (only pending files)
            upload_tasks: List[UploadTask] = []
            for file_path, source_id, tech_data, is_vid, rel_path in pending_data:
                mega_dest = f"{dest_path}/{rel_path.parent}" if rel_path.parent != Path(".") else dest_path
                upload_tasks.append(UploadTask(
                    file_path=file_path,
                    source_id=source_id,
                    tech_data=tech_data,
                    is_video=is_vid,
                    rel_path=rel_path,
                    mega_dest=mega_dest,
                    file_size=file_path.stat().st_size
                ))
            
            # Upload with controlled parallelism
            results: List[UploadResult] = []
            uploaded = 0
            failed = 0
            completed = 0
            
            semaphore = asyncio.Semaphore(parallel_count)
            
            async def upload_one(task: UploadTask) -> UploadResult:
                async with semaphore:
                    try:
                        handle = await self._storage.upload_video(
                            task.file_path, task.mega_dest, task.source_id, None
                        )
                        
                        if not handle:
                            return UploadResult.fail(task.file_path.name, "Upload failed")
                        
                        # Generate and upload preview for videos
                        preview_handle = None
                        if task.is_video and self._config.generate_preview:
                            duration = task.tech_data.get("duration", 0)
                            preview_handle = await self._upload_preview(
                                task.file_path, task.source_id, duration
                            )
                        
                        return UploadResult.ok(
                            source_id=task.source_id,
                            filename=task.file_path.name,
                            mega_handle=handle,
                            preview_handle=preview_handle
                        )
                    except Exception as e:
                        return UploadResult.fail(task.file_path.name, str(e))
            
            # Run all uploads with progress tracking
            async def run_with_progress():
                nonlocal completed, uploaded, failed
                
                tasks = [upload_one(t) for t in upload_tasks]
                
                for coro in asyncio.as_completed(tasks):
                    result = await coro
                    results.append(result)
                    completed += 1
                    
                    if result.success:
                        uploaded += 1
                    else:
                        failed += 1
                    
                    if progress_callback:
                        progress_callback(f"Uploaded: {result.filename}", completed, total)
            
            await run_with_progress()
            
            return FolderUploadResult(
                success=failed == 0,
                folder_name=folder_path.name,
                total_files=total,
                uploaded_files=uploaded + skipped,  # Include skipped as "uploaded"
                failed_files=failed,
                results=results,
                mega_folder_handle=root_handle
            )
            
        except Exception as e:
            import traceback
            error_msg = f"{str(e)}\n\n{traceback.format_exc()}"
            print(f"\n❌ UPLOAD ERROR: {error_msg}")
            return FolderUploadResult(
                success=False,
                folder_name=folder_path.name,
                total_files=total,
                uploaded_files=0,
                failed_files=total,
                results=[],
                error=error_msg
            )
    
    def _collect_files(self, folder: Path) -> List[Path]:
        """Collect all media files recursively."""
        files = []
        for item in folder.rglob("*"):
            if item.is_file() and (is_video(item) or is_image(item)):
                files.append(item)
        return sorted(files)
    
    async def _get_existing_files(self, dest_path: str) -> set:
        """Get set of existing filenames in MEGA destination (for resume support)."""
        try:
            if hasattr(self._storage, 'list_files'):
                return await self._storage.list_files(dest_path)
            elif hasattr(self._storage, '_storage') and hasattr(self._storage._storage, 'list_all'):
                # ManagedStorageService
                items = await self._storage._storage.manager.list_all(dest_path)
                return {node.name for _, node in items}
            elif hasattr(self._storage, '_client'):
                # Direct storage client
                node = await self._storage._client.get(dest_path)
                if node and node.is_folder:
                    return {child.name for child in node.children}
        except Exception as e:
            print(f"[resume] Could not check existing files: {e}")
        return set()
