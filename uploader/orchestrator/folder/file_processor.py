from pathlib import Path
from typing import Optional, Tuple
from uploader.services import AnalyzerService, MetadataRepository, StorageService
from uploader.orchestrator.models import UploadResult
from mediakit import is_video, is_image
from uploader.orchestrator.preview_handler import PreviewHandler
from uploader.models import UploadConfig
import logging

logger = logging.getLogger(__name__)

class FileProcessor:
    """Processes a single file: upload, analyze, and save metadata."""
    
    def __init__(self, analyzer: AnalyzerService, repository: MetadataRepository, storage: StorageService, preview_handler: PreviewHandler, config: UploadConfig):
        self._analyzer = analyzer
        self._repository = repository
        self._storage = storage
        self._preview_handler = preview_handler
        self._config = config
    
    async def process(self, file_path: Path, rel_path: Path, dest_path: str, progress_callback=None) -> Tuple[UploadResult, Optional[float], Optional[str]]:
        """
        Process file: upload to MEGA, then analyze and save metadata.
        
        Returns immediately after upload and metadata save. Preview generation
        is launched in background and tracked separately.
        
        Returns:
            Tuple of (UploadResult, duration, source_id) for videos that need previews.
            Duration and source_id are None for images or if preview is disabled.
        """
        try:
            # Normalize path to use forward slashes (MEGA uses /, not Windows \)
            if rel_path.parent != Path("."):
                parent_str = rel_path.parent.as_posix()  # Always uses / separator
                mega_dest = f"{dest_path}/{parent_str}"
            else:
                mega_dest = dest_path
            
            # Upload first
            logger.info("Uploading %s to MEGA...", file_path.name)
            try:
                handle = await self._storage.upload_video(file_path, mega_dest, None, progress_callback)
                if not handle:
                    logger.error("Failed to upload %s: upload_video returned None", file_path.name)
                    return (UploadResult.fail(file_path.name, "Upload to MEGA failed: upload_video returned None"), None, None)
            except Exception as upload_error:
                error_msg = str(upload_error) if str(upload_error) else f"{type(upload_error).__name__}: {repr(upload_error)}"
                logger.error("Exception during upload of %s: %s", file_path.name, error_msg, exc_info=True)
                return (UploadResult.fail(file_path.name, f"Upload to MEGA failed: {error_msg}"), None, None)
            
            logger.info("Uploaded %s to MEGA (handle: %s)", file_path.name, handle)
            
            # Analyze and save metadata
            logger.info("Analyzing %s...", file_path.name)
            if is_video(file_path):
                tech_data = await self._analyzer.analyze_video_async(file_path)
                source_id = tech_data["source_id"]
                duration = tech_data.get('duration', 0)
                logger.info("Video analyzed: source_id=%s, duration=%.2fs", source_id, duration)
                
                await self._repository.save_document(tech_data)
                await self._repository.save_video_metadata(source_id, tech_data)
                logger.info("Saved metadata to API for %s", file_path.name)
                
                # Preview will be generated in background - return immediately with duration/source_id
                # The preview task will be tracked and awaited at the end
                logger.info("Successfully completed %s (preview will be generated in background)", file_path.name)
                # Preview handle will be set later when background task completes
                preview_duration = duration if self._config.generate_preview else None
                preview_source_id = source_id if self._config.generate_preview else None
                return (UploadResult.ok(source_id, file_path.name, handle, None), preview_duration, preview_source_id)  # type: ignore[arg-type]
            
            elif is_image(file_path):
                tech_data = await self._analyzer.analyze_photo_async(file_path)
                source_id = tech_data["source_id"]
                width = tech_data.get('width')
                height = tech_data.get('height')
                logger.info("Image analyzed: source_id=%s, size=%sx%s", source_id, width, height)
                
                await self._repository.save_document(tech_data)
                await self._repository.save_photo_metadata(source_id, tech_data)
                logger.info("Saved metadata to API for %s", file_path.name)
                logger.info("Successfully completed %s", file_path.name)
                # Images don't have preview handles (method accepts None as default)
                return (UploadResult.ok(source_id, file_path.name, handle, None), None, None)  # type: ignore[arg-type]
            
            logger.warning("Unknown file type for %s", file_path.name)
            return (UploadResult.fail(file_path.name, "Unknown file type"), None, None)
        
        except Exception as e:
            error_msg = str(e) if str(e) else f"{type(e).__name__}: {repr(e)}"
            logger.error("Error processing %s: %s", file_path.name, error_msg, exc_info=True)
            return (UploadResult.fail(file_path.name, error_msg), None, None)
    
    async def generate_preview_background(
        self,
        file_path: Path,
        source_id: str,
        duration: float
    ) -> Optional[str]:
        """
        Generate and upload preview in background.
        
        This method is called separately and doesn't block the main upload flow.
        """
        if not self._config.generate_preview:
            return None
        
        try:
            preview_handle = await self._preview_handler.upload_preview(
                file_path, source_id, duration
            )
            if preview_handle:
                logger.info("Preview uploaded in background (handle: %s) for %s", preview_handle, file_path.name)
            return preview_handle
        except Exception as e:
            logger.error("Error generating preview for %s: %s", file_path.name, e, exc_info=True)
            return None


