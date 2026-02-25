"""Single file upload handlers."""
from pathlib import Path
from typing import Any, Dict, Optional

from ..models import UploadResult, SocialInfo, TelegramInfo
from ..use_cases.single_upload import DetectMediaKindUseCase, SingleFileUploadUseCase
from .preview_handler import PreviewHandler


class SingleUploadHandler:
    """Handles single file uploads (video, photo, social, telegram)."""

    def __init__(
        self,
        analyzer,
        repository,
        storage,
        preview_handler: PreviewHandler,
        config,
    ):
        self._analyzer = analyzer
        self._repository = repository
        self._storage = storage
        self._preview_handler = preview_handler
        self._config = config
        self._single_upload = SingleFileUploadUseCase()
        self._detect_media_kind = DetectMediaKindUseCase()

    async def upload(
        self,
        path: Path,
        dest: Optional[str] = None,
        progress_callback=None,
    ) -> UploadResult:
        """Upload video with metadata."""
        return await self._single_upload.execute(
            analyzer=self._analyzer,
            repository=self._repository,
            storage=self._storage,
            preview_handler=self._preview_handler,
            config=self._config,
            path=Path(path),
            media_kind="video",
            dest=dest,
            progress_callback=progress_callback,
            enable_preview=True,
        )

    async def upload_social(
        self,
        path: Path,
        social_info: SocialInfo,
        dest: Optional[str] = None,
        progress_callback=None,
    ) -> UploadResult:
        """Upload social video with channel metadata."""

        async def persist_social(source_id: str, _tech_data: Dict[str, Any]) -> None:
            await self._repository.save_social_info(source_id, social_info)

        return await self._single_upload.execute(
            analyzer=self._analyzer,
            repository=self._repository,
            storage=self._storage,
            preview_handler=self._preview_handler,
            config=self._config,
            path=Path(path),
            media_kind="video",
            dest=dest,
            progress_callback=progress_callback,
            enable_preview=True,
            extra_persist=persist_social,
        )

    async def upload_photo(
        self,
        path: Path,
        dest: Optional[str] = None,
        progress_callback=None,
    ) -> UploadResult:
        """Upload photo with metadata."""
        return await self._single_upload.execute(
            analyzer=self._analyzer,
            repository=self._repository,
            storage=self._storage,
            preview_handler=self._preview_handler,
            config=self._config,
            path=Path(path),
            media_kind="photo",
            dest=dest,
            progress_callback=progress_callback,
            enable_preview=False,
        )

    async def upload_auto(
        self,
        path: Path,
        dest: Optional[str] = None,
        progress_callback=None,
    ) -> UploadResult:
        """Auto-detect type and upload."""
        file_path = Path(path)
        media_kind = self._detect_media_kind.execute(file_path)

        if media_kind == "video":
            return await self.upload(file_path, dest, progress_callback)
        if media_kind == "photo":
            return await self.upload_photo(file_path, dest, progress_callback)
        return UploadResult.fail(file_path.name, f"Unsupported file type: {file_path.suffix}")

    async def upload_telegram(
        self,
        path: Path,
        telegram_info: Optional[TelegramInfo] = None,
        dest: Optional[str] = None,
        progress_callback=None,
    ) -> UploadResult:
        """Upload media with optional Telegram metadata."""
        file_path = Path(path)
        media_kind = self._detect_media_kind.execute(file_path)
        if media_kind is None:
            return UploadResult.fail(file_path.name, f"Unsupported file type: {file_path.suffix}")

        extra_persist = None
        if telegram_info:

            async def persist_telegram(source_id: str, _tech_data: Dict[str, Any]) -> None:
                await self._repository.save_telegram(source_id, telegram_info)

            extra_persist = persist_telegram

        return await self._single_upload.execute(
            analyzer=self._analyzer,
            repository=self._repository,
            storage=self._storage,
            preview_handler=self._preview_handler,
            config=self._config,
            path=file_path,
            media_kind=media_kind,
            dest=dest,
            progress_callback=progress_callback,
            enable_preview=media_kind == "video",
            extra_persist=extra_persist,
        )

