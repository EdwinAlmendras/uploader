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
from typing import Optional, List, Callable
from dataclasses import dataclass

from .models import UploadResult, UploadStatus, SocialInfo, UploadConfig
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


class UploadOrchestrator:
    """
    Orchestrates video uploads using injected services.
    
    Follows:
    - Dependency Injection (services injected)
    - Single Responsibility (delegates to services)
    - Open/Closed (extend via new services)
    
    Usage:
        async with UploadOrchestrator(api_url, mega) as uploader:
            result = await uploader.upload(video_path)
            result = await uploader.upload_social(video_path, social_info)
    """
    
    def __init__(
        self,
        api_url: str,
        storage_client: IStorageClient,
        config: Optional[UploadConfig] = None
    ):
        """
        Initialize orchestrator with dependencies.
        
        Args:
            api_url: Datastore API URL
            storage_client: Storage client (MEGA)
            config: Upload configuration
        """
        self._api_url = api_url
        self._storage_client = storage_client
        self._config = config or UploadConfig()
        
        # Services (initialized in __aenter__)
        self._api_client: Optional[HTTPAPIClient] = None
        self._analyzer: Optional[AnalyzerService] = None
        self._repository: Optional[MetadataRepository] = None
        self._preview: Optional[PreviewService] = None
        self._storage: Optional[StorageService] = None
    
    async def __aenter__(self):
        """Initialize services."""
        # API client
        self._api_client = HTTPAPIClient(self._api_url)
        await self._api_client.__aenter__()
        
        # Services with dependency injection
        self._analyzer = AnalyzerService()
        self._repository = MetadataRepository(self._api_client)
        self._preview = PreviewService(self._config)
        self._storage = StorageService(self._storage_client, self._config)
        
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
    
    async def _upload_preview(
        self,
        path: Path,
        source_id: str,
        duration: float
    ) -> Optional[str]:
        """Generate and upload preview grid."""
        try:
            # Generate grid
            grid_path = await self._preview.generate(path, duration)
            
            # Upload to /.previews/
            handle = await self._storage.upload_preview(grid_path, source_id)
            
            # Cleanup
            grid_path.unlink(missing_ok=True)
            
            return handle
            
        except Exception:
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
                    results=[]
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
            
            results: List[UploadResult] = []
            uploaded = 0
            failed = 0
            
            for idx, (file_path, source_id, tech_data, is_vid, rel_path) in enumerate(file_data):
                if progress_callback:
                    progress_callback(f"Uploading: {file_path.name}", idx + 1, total)
                
                try:
                    # Determine MEGA destination
                    mega_dest = f"{dest_path}/{rel_path.parent}" if rel_path.parent != Path(".") else dest_path
                    
                    # Upload file
                    handle = await self._storage.upload_video(file_path, mega_dest, source_id, None)
                    
                    if not handle:
                        results.append(UploadResult.fail(file_path.name, "Upload failed"))
                        failed += 1
                        continue
                    
                    # Generate and upload preview for videos
                    preview_handle = None
                    if is_vid and self._config.generate_preview:
                        duration = tech_data.get("duration", 0)
                        preview_handle = await self._upload_preview(file_path, source_id, duration)
                    
                    results.append(UploadResult.ok(
                        source_id=source_id,
                        filename=file_path.name,
                        mega_handle=handle,
                        preview_handle=preview_handle
                    ))
                    uploaded += 1
                    
                except Exception as e:
                    results.append(UploadResult.fail(file_path.name, str(e)))
                    failed += 1
            
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
            return FolderUploadResult(
                success=False,
                folder_name=folder_path.name,
                total_files=0,
                uploaded_files=0,
                failed_files=0,
                results=[],
                error=str(e)
            )
    
    def _collect_files(self, folder: Path) -> List[Path]:
        """Collect all media files recursively."""
        files = []
        for item in folder.rglob("*"):
            if item.is_file() and (is_video(item) or is_image(item)):
                files.append(item)
        return sorted(files)
