"""Use cases for single-file processing workflow."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, Optional, Tuple
import mimetypes

from mediakit import is_image, is_video

from uploader.services.resume import blake3_file


@dataclass(frozen=True)
class FileAnalysisResult:
    """Normalized result after analyzing a file."""

    kind: str  # video | image | other
    tech_data: Dict[str, Any]
    duration: Optional[float] = None


class ResolveMegaDestinationUseCase:
    """Build MEGA destination preserving relative folder structure."""

    @staticmethod
    def execute(rel_path: Path, dest_path: str) -> str:
        if rel_path.parent != Path("."):
            return f"{dest_path}/{rel_path.parent.as_posix()}"
        return dest_path


class UploadFileUseCase:
    """Upload file to storage with source_id metadata."""

    async def execute(
        self,
        storage: Any,
        file_path: Path,
        mega_dest: str,
        source_id: str,
        progress_callback=None,
    ) -> Optional[str]:
        return await storage.upload_video(file_path, mega_dest, source_id, progress_callback)


class AnalyzeFileUseCase:
    """Analyze file and produce normalized metadata payload."""

    _FALLBACK_MIMES = {
        ".pdf": "application/pdf",
        ".html": "text/html",
        ".htm": "text/html",
        ".xls": "application/vnd.ms-excel",
        ".xlsx": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        ".doc": "application/msword",
        ".docx": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        ".txt": "text/plain",
        ".srt": "text/plain",
    }

    async def execute(self, analyzer: Any, file_path: Path, source_id: str) -> FileAnalysisResult:
        if is_video(file_path):
            tech_data = await analyzer.analyze_video_async(file_path)
            tech_data["source_id"] = source_id
            return FileAnalysisResult(kind="video", tech_data=tech_data, duration=tech_data.get("duration", 0))

        if is_image(file_path):
            tech_data = await analyzer.analyze_photo_async(file_path)
            tech_data["source_id"] = source_id
            return FileAnalysisResult(kind="image", tech_data=tech_data, duration=None)

        stat = file_path.stat()
        mimetype, _ = mimetypes.guess_type(str(file_path))
        if not mimetype:
            mimetype = self._FALLBACK_MIMES.get(file_path.suffix.lower(), "application/octet-stream")
        file_hash = await blake3_file(file_path)
        tech_data = {
            "source_id": source_id,
            "filename": file_path.name,
            "mimetype": mimetype,
            "mtime": datetime.fromtimestamp(stat.st_mtime),
            "ctime": datetime.fromtimestamp(stat.st_ctime),
            "blake3_hash": file_hash,
        }
        return FileAnalysisResult(kind="other", tech_data=tech_data, duration=None)


class PersistFileMetadataUseCase:
    """Persist metadata according to analyzed file kind."""

    async def execute(self, repository: Any, source_id: str, analysis: FileAnalysisResult) -> None:
        await repository.save_document(analysis.tech_data)
        if analysis.kind == "video":
            await repository.save_video_metadata(source_id, analysis.tech_data)
        elif analysis.kind == "image":
            await repository.save_photo_metadata(source_id, analysis.tech_data)


class ResolvePreviewRequestUseCase:
    """Resolve preview generation intent from analysis/config."""

    @staticmethod
    def execute(
        analysis: FileAnalysisResult, source_id: str, generate_preview: bool
    ) -> Tuple[Optional[float], Optional[str]]:
        if analysis.kind != "video" or not generate_preview:
            return (None, None)
        return (analysis.duration, source_id)


class UploadPreviewUseCase:
    """Upload generated preview with filename conventions."""

    async def execute(
        self,
        preview_handler: Any,
        file_path: Path,
        source_id: str,
        duration: float,
        dest_path: Optional[str] = None,
    ) -> Optional[str]:
        preview_filename = f"{file_path.stem}.jpg"
        return await preview_handler.upload_preview(
            file_path,
            source_id,
            duration,
            dest_path=dest_path,
            filename=preview_filename,
        )
