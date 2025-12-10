"""Single file upload handlers."""
from pathlib import Path
from typing import Optional
from ..models import UploadResult, SocialInfo, TelegramInfo
from .preview_handler import PreviewHandler


class SingleUploadHandler:
    """Handles single file uploads (video, photo, social, telegram)."""
    
    def __init__(
        self,
        analyzer,
        repository,
        storage,
        preview_handler: PreviewHandler,
        config
    ):
        """
        Initialize single upload handler.
        
        Args:
            analyzer: AnalyzerService
            repository: MetadataRepository
            storage: Storage service
            preview_handler: PreviewHandler
            config: UploadConfig
        """
        self._analyzer = analyzer
        self._repository = repository
        self._storage = storage
        self._preview_handler = preview_handler
        self._config = config
    
    async def upload(
        self,
        path: Path,
        dest: Optional[str] = None,
        progress_callback=None
    ) -> UploadResult:
        """Upload video with metadata."""
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
                # Use video filename for preview (VIDEO.mp4 -> VIDEO.jpg)
                preview_filename = path.stem + ".jpg"
                preview_handle = await self._preview_handler.upload_preview(
                    path, source_id, duration, dest_path=dest, filename=preview_filename
                )
            
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
        """Upload social video with channel metadata."""
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
                # Use video filename for preview (VIDEO.mp4 -> VIDEO.jpg)
                preview_filename = path.stem + ".jpg"
                preview_handle = await self._preview_handler.upload_preview(
                    path, source_id, duration, dest_path=dest, filename=preview_filename
                )
            
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
        """Upload photo with metadata."""
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
                mega_handle=mega_handle,
                preview_handle=None
            )
            
        except Exception as e:
            return UploadResult.fail(path.name, str(e))
    
    async def upload_auto(
        self,
        path: Path,
        dest: Optional[str] = None,
        progress_callback=None
    ) -> UploadResult:
        """Auto-detect type and upload."""
        from mediakit import is_video, is_image
        
        if is_video(path):
            return await self.upload(path, dest, progress_callback)
        elif is_image(path):
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
        """Upload media with optional Telegram metadata."""
        path = Path(path)
        
        try:
            from mediakit import is_video, is_image
            
            # 1. Detect type and analyze
            if is_video(path):
                tech_data = await self._analyzer.analyze_async(path)
                source_id = tech_data["source_id"]
                duration = tech_data.get("duration", 0)
                is_vid = True
            elif is_image(path):
                tech_data = await self._analyzer.analyze_photo_async(path)
                source_id = tech_data["source_id"]
                duration = 0
                is_vid = False
            else:
                return UploadResult.fail(path.name, f"Unsupported file type: {path.suffix}")
            
            # 2. Save base metadata
            await self._repository.save_document(tech_data)
            
            if is_vid:
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
            # Wait for preview to complete before returning to ensure file is not deleted
            preview_handle = None
            if is_vid and self._config.generate_preview:
                # Use video filename for preview (VIDEO.mp4 -> VIDEO.jpg)
                preview_filename = path.stem + ".jpg"
                # Generate preview synchronously to ensure file exists
                preview_handle = await self._preview_handler.upload_preview(
                    path, source_id, duration, dest_path=dest, filename=preview_filename
                )
            
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

