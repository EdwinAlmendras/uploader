"""Single file upload handlers."""
import asyncio
from pathlib import Path
from typing import Optional
from mediakit.analyzer import generate_id
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
            # 1. Generate source_id quickly (doesn't require analysis)
            source_id = generate_id()
            
            # 2. Start analysis and upload in parallel
            analyze_task = asyncio.create_task(self._analyzer.analyze_async(path))
            upload_task = asyncio.create_task(
                self._storage.upload_video(path, dest, source_id, progress_callback)
            )
            
            # 3. Wait for both to complete
            tech_data, mega_handle = await asyncio.gather(analyze_task, upload_task)
            
            # Replace source_id from analysis with our pre-generated one
            tech_data["source_id"] = source_id
            duration = tech_data.get("duration", 0)
            
            if not mega_handle:
                return UploadResult.fail(path.name, "Upload to MEGA failed")
            
            # 4. Save metadata only if upload was successful
            await self._repository.save_document(tech_data)
            await self._repository.save_video_metadata(source_id, tech_data)
            
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
            # 1. Generate source_id quickly (doesn't require analysis)
            source_id = generate_id()
            
            # 2. Start analysis and upload in parallel
            analyze_task = asyncio.create_task(self._analyzer.analyze_async(path))
            upload_task = asyncio.create_task(
                self._storage.upload_video(path, dest, source_id, progress_callback)
            )
            
            # 3. Wait for both to complete
            tech_data, mega_handle = await asyncio.gather(analyze_task, upload_task)
            
            # Replace source_id from analysis with our pre-generated one
            tech_data["source_id"] = source_id
            duration = tech_data.get("duration", 0)
            
            if not mega_handle:
                return UploadResult.fail(path.name, "Upload to MEGA failed")
            
            # 4. Save metadata only if upload was successful
            await self._repository.save_document(tech_data)
            await self._repository.save_video_metadata(source_id, tech_data)
            await self._repository.save_social_info(source_id, social_info)
            
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
            # 1. Generate source_id quickly (doesn't require analysis)
            source_id = generate_id()
            
            # 2. Start analysis and upload in parallel
            analyze_task = asyncio.create_task(self._analyzer.analyze_photo_async(path))
            upload_task = asyncio.create_task(
                self._storage.upload_video(path, dest, source_id, progress_callback)
            )
            
            # 3. Wait for both to complete
            photo_data, mega_handle = await asyncio.gather(analyze_task, upload_task)
            
            # Replace source_id from analysis with our pre-generated one
            photo_data["source_id"] = source_id
            
            if not mega_handle:
                return UploadResult.fail(path.name, "Upload to MEGA failed")
            
            # 4. Save metadata only if upload was successful
            await self._repository.save_document(photo_data)
            await self._repository.save_photo_metadata(source_id, photo_data)
            
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
            
            # 1. Detect type
            is_vid = is_video(path)
            is_img = is_image(path)
            
            if not is_vid and not is_img:
                return UploadResult.fail(path.name, f"Unsupported file type: {path.suffix}")
            
            # 2. Generate source_id quickly (doesn't require analysis)
            source_id = generate_id()
            
            # 3. Start analysis and upload in parallel
            if is_vid:
                analyze_task = asyncio.create_task(self._analyzer.analyze_async(path))
            else:
                analyze_task = asyncio.create_task(self._analyzer.analyze_photo_async(path))
            
            upload_task = asyncio.create_task(
                self._storage.upload_video(path, dest, source_id, progress_callback)
            )
            
            # 4. Wait for both to complete
            tech_data, mega_handle = await asyncio.gather(analyze_task, upload_task)
            
            # Replace source_id from analysis with our pre-generated one
            tech_data["source_id"] = source_id
            duration = tech_data.get("duration", 0) if is_vid else 0
            
            if not mega_handle:
                return UploadResult.fail(path.name, "Upload to MEGA failed")
            
            # 5. Save metadata only if upload was successful
            await self._repository.save_document(tech_data)
            
            if is_vid:
                await self._repository.save_video_metadata(source_id, tech_data)
            else:
                await self._repository.save_photo_metadata(source_id, tech_data)
            
            # Save Telegram metadata if provided
            if telegram_info:
                await self._repository.save_telegram(source_id, telegram_info)
            
            # 6. Generate and upload preview for videos
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

