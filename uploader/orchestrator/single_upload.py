"""Single file upload handlers."""
import asyncio
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
        """Upload video with metadata (optimized with parallel operations)."""
        path = Path(path)
        
        try:
            # 1. Analyze first (needed for source_id and duration)
            tech_data = await self._analyzer.analyze_async(path)
            source_id = tech_data["source_id"]
            duration = tech_data.get("duration", 0)
            
            # 2. Execute in parallel:
            #    - Save metadata (document + video_metadata)
            #    - Upload video to MEGA
            #    - Generate and upload preview (if enabled)
            tasks = []
            
            # Task 1: Save metadata (can run in parallel)
            async def save_metadata():
                await self._repository.save_document(tech_data)
                await self._repository.save_video_metadata(source_id, tech_data)
            
            tasks.append(save_metadata())
            
            # Task 2: Upload video (can start immediately after analysis)
            tasks.append(self._storage.upload_video(path, dest, source_id, progress_callback))
            
            # Task 3: Generate and upload preview (if enabled)
            if self._config.generate_preview:
                tasks.append(self._preview_handler.upload_preview(path, source_id, duration))
            
            # Wait for all tasks to complete
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Extract results
            metadata_result = results[0]
            mega_handle = results[1]
            preview_handle = results[2] if self._config.generate_preview else None
            
            # Check for errors
            if isinstance(metadata_result, Exception):
                return UploadResult.fail(path.name, f"Failed to save metadata: {metadata_result}")
            
            if isinstance(mega_handle, Exception):
                return UploadResult.fail(path.name, f"Upload to MEGA failed: {mega_handle}")
            
            if not mega_handle or not isinstance(mega_handle, str):
                return UploadResult.fail(path.name, "Upload to MEGA failed")
            
            # Handle preview result
            final_preview_handle: Optional[str] = None
            if preview_handle is not None:
                if isinstance(preview_handle, Exception):
                    # Preview failure is not critical, log but continue
                    import logging
                    logging.warning(f"Preview generation failed for {path.name}: {preview_handle}")
                elif isinstance(preview_handle, str):
                    final_preview_handle = preview_handle
            
            return UploadResult.ok(
                source_id=source_id,
                filename=path.name,
                mega_handle=mega_handle,
                preview_handle=final_preview_handle  # type: ignore
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
        """Upload social video with channel metadata (optimized with parallel operations)."""
        path = Path(path)
        
        try:
            # 1. Analyze first (needed for source_id and duration)
            tech_data = await self._analyzer.analyze_async(path)
            source_id = tech_data["source_id"]
            duration = tech_data.get("duration", 0)
            
            # 2. Execute in parallel:
            #    - Save metadata (document + video_metadata + social_info)
            #    - Upload video to MEGA
            #    - Generate and upload preview (if enabled)
            tasks = []
            
            # Task 1: Save all metadata (can run in parallel)
            async def save_metadata():
                await self._repository.save_document(tech_data)
                await self._repository.save_video_metadata(source_id, tech_data)
                await self._repository.save_social_info(source_id, social_info)
            
            tasks.append(save_metadata())
            
            # Task 2: Upload video (can start immediately after analysis)
            tasks.append(self._storage.upload_video(path, dest, source_id, progress_callback))
            
            # Task 3: Generate and upload preview (if enabled)
            if self._config.generate_preview:
                tasks.append(self._preview_handler.upload_preview(path, source_id, duration))
            
            # Wait for all tasks to complete
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Extract results
            metadata_result = results[0]
            mega_handle = results[1]
            preview_handle = results[2] if self._config.generate_preview else None
            
            # Check for errors
            if isinstance(metadata_result, Exception):
                return UploadResult.fail(path.name, f"Failed to save metadata: {metadata_result}")
            
            if isinstance(mega_handle, Exception):
                return UploadResult.fail(path.name, f"Upload to MEGA failed: {mega_handle}")
            
            if not mega_handle or not isinstance(mega_handle, str):
                return UploadResult.fail(path.name, "Upload to MEGA failed")
            
            # Handle preview result
            final_preview_handle: Optional[str] = None
            if preview_handle is not None:
                if isinstance(preview_handle, Exception):
                    # Preview failure is not critical, log but continue
                    import logging
                    logging.warning(f"Preview generation failed for {path.name}: {preview_handle}")
                elif isinstance(preview_handle, str):
                    final_preview_handle = preview_handle
            
            return UploadResult.ok(
                source_id=source_id,
                filename=path.name,
                mega_handle=mega_handle,
                preview_handle=final_preview_handle  # type: ignore
            )
            
        except Exception as e:
            return UploadResult.fail(path.name, str(e))
    
    async def upload_photo(
        self,
        path: Path,
        dest: Optional[str] = None,
        progress_callback=None
    ) -> UploadResult:
        """Upload photo with metadata (optimized with parallel operations)."""
        path = Path(path)
        
        try:
            # 1. Analyze first (needed for source_id)
            photo_data = await self._analyzer.analyze_photo_async(path)
            source_id = photo_data["source_id"]
            
            # 2. Execute in parallel:
            #    - Save metadata (document + photo_metadata)
            #    - Upload photo to MEGA
            tasks = []
            
            # Task 1: Save metadata (can run in parallel)
            async def save_metadata():
                await self._repository.save_document(photo_data)
                await self._repository.save_photo_metadata(source_id, photo_data)
            
            tasks.append(save_metadata())
            
            # Task 2: Upload photo (can start immediately after analysis)
            tasks.append(self._storage.upload_video(path, dest, source_id, progress_callback))
            
            # Wait for all tasks to complete
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Extract results
            metadata_result = results[0]
            mega_handle = results[1]
            
            # Check for errors
            if isinstance(metadata_result, Exception):
                return UploadResult.fail(path.name, f"Failed to save metadata: {metadata_result}")
            
            if isinstance(mega_handle, Exception):
                return UploadResult.fail(path.name, f"Upload to MEGA failed: {mega_handle}")
            
            if not mega_handle or not isinstance(mega_handle, str):
                return UploadResult.fail(path.name, "Upload to MEGA failed")
            
            return UploadResult.ok(
                source_id=source_id,
                filename=path.name,
                mega_handle=mega_handle,
                preview_handle=None  # type: ignore
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
        from mediakit import is_video, is_image  # type: ignore
        
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
        """Upload media with optional Telegram metadata (optimized with parallel operations)."""
        path = Path(path)
        
        try:
            from mediakit import is_video, is_image  # type: ignore
            
            # 1. Detect type and analyze first (needed for source_id and duration)
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
            
            # 2. Execute in parallel:
            #    - Save metadata (document + video/photo_metadata + telegram if provided)
            #    - Upload to MEGA
            #    - Generate and upload preview for videos (if enabled)
            tasks = []
            
            # Task 1: Save all metadata (can run in parallel)
            async def save_metadata():
                await self._repository.save_document(tech_data)
                if is_vid:
                    await self._repository.save_video_metadata(source_id, tech_data)
                else:
                    await self._repository.save_photo_metadata(source_id, tech_data)
                if telegram_info:
                    await self._repository.save_telegram(source_id, telegram_info)
            
            tasks.append(save_metadata())
            
            # Task 2: Upload to MEGA (can start immediately after analysis)
            tasks.append(self._storage.upload_video(path, dest, source_id, progress_callback))
            
            # Task 3: Generate and upload preview for videos (if enabled)
            if is_vid and self._config.generate_preview:
                tasks.append(self._preview_handler.upload_preview(path, source_id, duration))
            
            # Wait for all tasks to complete
            results = await asyncio.gather(*tasks, return_exceptions=True)
            
            # Extract results
            metadata_result = results[0]
            mega_handle = results[1]
            preview_handle = results[2] if (is_vid and self._config.generate_preview) else None
            
            # Check for errors
            if isinstance(metadata_result, Exception):
                return UploadResult.fail(path.name, f"Failed to save metadata: {metadata_result}")
            
            if isinstance(mega_handle, Exception):
                return UploadResult.fail(path.name, f"Upload to MEGA failed: {mega_handle}")
            
            if not mega_handle or not isinstance(mega_handle, str):
                return UploadResult.fail(path.name, "Upload to MEGA failed")
            
            # Handle preview result
            final_preview_handle: Optional[str] = None
            if preview_handle is not None:
                if isinstance(preview_handle, Exception):
                    # Preview failure is not critical, log but continue
                    import logging
                    logging.warning(f"Preview generation failed for {path.name}: {preview_handle}")
                elif isinstance(preview_handle, str):
                    final_preview_handle = preview_handle
            
            return UploadResult.ok(
                source_id=source_id,
                filename=path.name,
                mega_handle=mega_handle,
                preview_handle=final_preview_handle  # type: ignore
            )
            
        except Exception as e:
            import traceback
            traceback.print_exc()
            return UploadResult.fail(path.name, str(e))

