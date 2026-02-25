from pathlib import Path
from typing import Optional, Tuple
from uploader.services import AnalyzerService, MetadataRepository, StorageService
from uploader.orchestrator.models import UploadResult
from mediakit.analyzer import generate_id
from uploader.orchestrator.preview_handler import PreviewHandler
from uploader.models import UploadConfig
from uploader.use_cases.file_processing import (
    AnalyzeFileUseCase,
    PersistFileMetadataUseCase,
    ResolveMegaDestinationUseCase,
    ResolvePreviewRequestUseCase,
    UploadFileUseCase,
    UploadPreviewUseCase,
)
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
        self._resolve_dest = ResolveMegaDestinationUseCase()
        self._upload_file = UploadFileUseCase()
        self._analyze_file = AnalyzeFileUseCase()
        self._persist_metadata = PersistFileMetadataUseCase()
        self._resolve_preview = ResolvePreviewRequestUseCase()
        self._upload_preview = UploadPreviewUseCase()
    
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
            mega_dest = self._resolve_dest.execute(rel_path, dest_path)
            
            # Generate source_id BEFORE upload (needed for mega_id attribute)
            source_id = generate_id()
            logger.debug("Generated source_id=%s for %s", source_id, file_path.name)
            
            # Upload with source_id (stored as mega_id attribute 'm' in MEGA)
            logger.info("Uploading %s to MEGA...", file_path.name)
            try:
                handle = await self._upload_file.execute(
                    self._storage,
                    file_path,
                    mega_dest,
                    source_id,
                    progress_callback,
                )
                if not handle:
                    logger.error("Failed to upload %s: upload_video returned None", file_path.name)
                    return (UploadResult.fail(file_path.name, "Upload to MEGA failed: upload_video returned None"), None, None)
            except Exception as upload_error:
                error_msg = str(upload_error) if str(upload_error) else f"{type(upload_error).__name__}: {repr(upload_error)}"
                logger.error("Exception during upload of %s: %s", file_path.name, error_msg, exc_info=True)
                return (UploadResult.fail(file_path.name, f"Upload to MEGA failed: {error_msg}"), None, None)
            
            logger.info("Uploaded %s to MEGA (handle: %s, source_id: %s)", file_path.name, handle, source_id)
            
            logger.info("Analyzing %s...", file_path.name)
            analysis = await self._analyze_file.execute(self._analyzer, file_path, source_id)

            if analysis.kind == "video":
                logger.info(
                    "Video analyzed: source_id=%s, duration=%.2fs",
                    source_id,
                    analysis.duration or 0,
                )
            elif analysis.kind == "image":
                logger.info(
                    "Image analyzed: source_id=%s, size=%sx%s",
                    source_id,
                    analysis.tech_data.get("width"),
                    analysis.tech_data.get("height"),
                )
            else:
                logger.info(
                    "Processed other file type: %s (hash: %s...)",
                    file_path.name,
                    str(analysis.tech_data.get("blake3_hash", ""))[:16],
                )

            await self._persist_metadata.execute(self._repository, source_id, analysis)
            logger.info("Saved metadata to API for %s", file_path.name)

            preview_duration, preview_source_id = self._resolve_preview.execute(
                analysis,
                source_id,
                self._config.generate_preview,
            )
            if preview_duration is not None:
                logger.info(
                    "Successfully completed %s (preview will be generated in background)",
                    file_path.name,
                )
            else:
                logger.info("Successfully completed %s", file_path.name)
            return (
                UploadResult.ok(source_id, file_path.name, handle, None),
                preview_duration,
                preview_source_id,
            )
        
        except Exception as e:
            error_msg = str(e) if str(e) else f"{type(e).__name__}: {repr(e)}"
            logger.error("Error processing %s: %s", file_path.name, error_msg, exc_info=True)
            return (UploadResult.fail(file_path.name, error_msg), None, None)
    
    async def generate_preview_background(
        self,
        file_path: Path,
        source_id: str,
        duration: float,
        dest_path: Optional[str] = None
    ) -> Optional[str]:
        """
        Generate and upload preview in background.
        
        This method is called separately and doesn't block the main upload flow.
        
        Args:
            file_path: Path to video file
            source_id: Source ID (for backward compatibility)
            duration: Video duration
            dest_path: Destination path where video was uploaded (for preview location)
        """
        if not self._config.generate_preview:
            return None
        
        try:
            preview_handle = await self._upload_preview.execute(
                self._preview_handler,
                file_path,
                source_id,
                duration,
                dest_path=dest_path,
            )
            if preview_handle:
                preview_filename = f"{file_path.stem}.jpg"
                preview_location = f"{dest_path}/{preview_filename}" if dest_path else f"/.previews/{source_id}.jpg"
                logger.info("Preview uploaded in background (handle: %s) for %s at %s", preview_handle, file_path.name, preview_location)
            return preview_handle
        except Exception as e:
            logger.error("Error generating preview for %s: %s", file_path.name, e, exc_info=True)
            return None


