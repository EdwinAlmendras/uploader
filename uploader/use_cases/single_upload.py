"""Use cases for single upload flows (video, photo, social, telegram)."""
from __future__ import annotations

import asyncio
import logging
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict, Literal, Optional, Tuple

from mediakit import is_image, is_video
from mediakit.analyzer import generate_id

from uploader.models import UploadConfig, UploadResult
from uploader.use_cases.file_processing import (
    FileAnalysisResult,
    PersistFileMetadataUseCase,
    UploadFileUseCase,
    UploadPreviewUseCase,
)

logger = logging.getLogger(__name__)

MediaKind = Literal["video", "photo"]
ExtraPersistHook = Callable[[str, Dict[str, Any]], Awaitable[None]]


def _describe_exception(exc: Exception) -> str:
    message = str(exc).strip()
    if message:
        return message
    return f"{type(exc).__name__}: {repr(exc)}"


class DetectMediaKindUseCase:
    """Detect media kind for auto-routing workflows."""

    @staticmethod
    def execute(path: Path) -> Optional[MediaKind]:
        file_path = Path(path)
        if is_video(file_path):
            return "video"
        if is_image(file_path):
            return "photo"
        return None


class AnalyzeMediaUseCase:
    """Analyze media according to requested kind and normalize metadata."""

    async def execute(
        self,
        analyzer: Any,
        file_path: Path,
        source_id: str,
        media_kind: MediaKind,
    ) -> FileAnalysisResult:
        if media_kind == "video":
            tech_data = await analyzer.analyze_async(file_path)
            tech_data["source_id"] = source_id
            return FileAnalysisResult(
                kind="video",
                tech_data=tech_data,
                duration=tech_data.get("duration", 0),
            )

        tech_data = await analyzer.analyze_photo_async(file_path)
        tech_data["source_id"] = source_id
        return FileAnalysisResult(kind="image", tech_data=tech_data, duration=None)


class ParallelAnalyzeAndUploadUseCase:
    """Run analysis and upload concurrently with proper task cleanup."""

    def __init__(
        self,
        analyze_media: Optional[AnalyzeMediaUseCase] = None,
        upload_file: Optional[UploadFileUseCase] = None,
    ):
        self._analyze_media = analyze_media or AnalyzeMediaUseCase()
        self._upload_file = upload_file or UploadFileUseCase()

    async def execute(
        self,
        analyzer: Any,
        storage: Any,
        file_path: Path,
        dest: Optional[str],
        source_id: str,
        media_kind: MediaKind,
        progress_callback=None,
    ) -> Tuple[FileAnalysisResult, Optional[str]]:
        analyze_task = asyncio.create_task(
            self._analyze_media.execute(analyzer, file_path, source_id, media_kind)
        )
        upload_task = asyncio.create_task(
            self._upload_file.execute(storage, file_path, dest, source_id, progress_callback)
        )

        try:
            analysis_result, mega_handle = await asyncio.gather(analyze_task, upload_task)
            return analysis_result, mega_handle
        except Exception:
            for task in (analyze_task, upload_task):
                if not task.done():
                    task.cancel()
            await asyncio.gather(analyze_task, upload_task, return_exceptions=True)
            raise


class SingleFileUploadUseCase:
    """Execute the base single-file upload workflow."""

    def __init__(
        self,
        parallel_analyze_upload: Optional[ParallelAnalyzeAndUploadUseCase] = None,
        persist_metadata: Optional[PersistFileMetadataUseCase] = None,
        upload_preview: Optional[UploadPreviewUseCase] = None,
    ):
        self._parallel_analyze_upload = (
            parallel_analyze_upload or ParallelAnalyzeAndUploadUseCase()
        )
        self._persist_metadata = persist_metadata or PersistFileMetadataUseCase()
        self._upload_preview = upload_preview or UploadPreviewUseCase()

    async def execute(
        self,
        analyzer: Any,
        repository: Any,
        storage: Any,
        preview_handler: Any,
        config: UploadConfig,
        path: Path,
        media_kind: MediaKind,
        dest: Optional[str] = None,
        progress_callback=None,
        enable_preview: bool = False,
        extra_persist: Optional[ExtraPersistHook] = None,
    ) -> UploadResult:
        file_path = Path(path)
        source_id = generate_id()

        logger.debug(
            "Single upload started: file=%s media_kind=%s source_id=%s",
            file_path.name,
            media_kind,
            source_id,
        )

        try:
            analysis_result, mega_handle = await self._parallel_analyze_upload.execute(
                analyzer,
                storage,
                file_path,
                dest,
                source_id,
                media_kind,
                progress_callback,
            )
        except Exception as exc:
            error_msg = _describe_exception(exc)
            logger.error(
                "Failed parallel analyze/upload for %s: %s",
                file_path.name,
                error_msg,
                exc_info=True,
            )
            return UploadResult.fail(file_path.name, error_msg)

        if not mega_handle:
            return UploadResult.fail(file_path.name, "Upload to MEGA failed")

        try:
            await self._persist_metadata.execute(repository, source_id, analysis_result)
            if extra_persist:
                await extra_persist(source_id, analysis_result.tech_data)
        except Exception as exc:
            error_msg = _describe_exception(exc)
            logger.error(
                "Failed metadata persistence for %s (source_id=%s): %s",
                file_path.name,
                source_id,
                error_msg,
                exc_info=True,
            )
            return UploadResult.fail(file_path.name, error_msg)

        preview_handle = None
        if enable_preview and media_kind == "video" and config.generate_preview:
            preview_dest = dest if dest is not None else ""
            preview_duration = analysis_result.duration or 0
            preview_handle = await self._upload_preview.execute(
                preview_handler,
                file_path,
                source_id,
                preview_duration,
                dest_path=preview_dest,
            )

        return UploadResult.ok(
            source_id=source_id,
            filename=file_path.name,
            mega_handle=mega_handle,
            preview_handle=preview_handle,
        )
