"""Tests for single upload handler and base single upload workflow."""
from pathlib import Path
from unittest.mock import AsyncMock

import pytest

from uploader.models import SocialInfo, TelegramInfo, UploadConfig, UploadStatus
from uploader.orchestrator.single_upload import SingleUploadHandler


def _build_handler(config: UploadConfig):
    analyzer = AsyncMock()
    repository = AsyncMock()
    storage = AsyncMock()
    preview_handler = AsyncMock()
    handler = SingleUploadHandler(analyzer, repository, storage, preview_handler, config)
    return handler, analyzer, repository, storage, preview_handler


@pytest.mark.asyncio
async def test_upload_video_success_with_preview(monkeypatch):
    monkeypatch.setattr("uploader.use_cases.single_upload.generate_id", lambda: "source-video-1")

    config = UploadConfig(generate_preview=True)
    handler, analyzer, repository, storage, preview_handler = _build_handler(config)

    analyzer.analyze_async.return_value = {"duration": 120.5}
    storage.upload_video.return_value = "mega-handle-1"
    preview_handler.upload_preview.return_value = "preview-handle-1"

    result = await handler.upload(Path("movie.mp4"), dest="/Uploads")

    assert result.status == UploadStatus.SUCCESS
    assert result.source_id == "source-video-1"
    assert result.mega_handle == "mega-handle-1"
    assert result.preview_handle == "preview-handle-1"

    repository.save_document.assert_awaited_once()
    repository.save_video_metadata.assert_awaited_once()
    repository.save_photo_metadata.assert_not_awaited()
    preview_handler.upload_preview.assert_awaited_once_with(
        Path("movie.mp4"),
        "source-video-1",
        120.5,
        dest_path="/Uploads",
        filename="movie.jpg",
    )


@pytest.mark.asyncio
async def test_upload_social_persists_social_metadata(monkeypatch):
    monkeypatch.setattr("uploader.use_cases.single_upload.generate_id", lambda: "source-social-1")

    config = UploadConfig(generate_preview=False)
    handler, analyzer, repository, storage, preview_handler = _build_handler(config)

    analyzer.analyze_async.return_value = {"duration": 10}
    storage.upload_video.return_value = "mega-social-1"
    social_info = SocialInfo(
        platform="youtube",
        video_id="abc123",
        video_url="https://youtube.com/watch?v=abc123",
        channel_id="UC1",
    )

    result = await handler.upload_social(Path("clip.mp4"), social_info, dest="/Social")

    assert result.status == UploadStatus.SUCCESS
    repository.save_social_info.assert_awaited_once_with("source-social-1", social_info)
    preview_handler.upload_preview.assert_not_awaited()


@pytest.mark.asyncio
async def test_upload_photo_uses_photo_analysis(monkeypatch):
    monkeypatch.setattr("uploader.use_cases.single_upload.generate_id", lambda: "source-photo-1")

    config = UploadConfig(generate_preview=True)
    handler, analyzer, repository, storage, preview_handler = _build_handler(config)

    analyzer.analyze_photo_async.return_value = {"width": 1920, "height": 1080}
    storage.upload_video.return_value = "mega-photo-1"

    result = await handler.upload_photo(Path("image.jpg"), dest="/Photos")

    assert result.status == UploadStatus.SUCCESS
    analyzer.analyze_photo_async.assert_awaited_once_with(Path("image.jpg"))
    repository.save_photo_metadata.assert_awaited_once()
    repository.save_video_metadata.assert_not_awaited()
    preview_handler.upload_preview.assert_not_awaited()


@pytest.mark.asyncio
async def test_upload_auto_rejects_unsupported_file():
    config = UploadConfig()
    handler, _, _, _, _ = _build_handler(config)

    result = await handler.upload_auto(Path("notes.txt"))

    assert result.status == UploadStatus.FAILED
    assert "Unsupported file type" in (result.error or "")


@pytest.mark.asyncio
async def test_upload_telegram_video_persists_telegram_and_preview(monkeypatch):
    monkeypatch.setattr("uploader.use_cases.single_upload.generate_id", lambda: "source-tg-1")

    config = UploadConfig(generate_preview=True)
    handler, analyzer, repository, storage, preview_handler = _build_handler(config)

    analyzer.analyze_async.return_value = {"duration": 42}
    storage.upload_video.return_value = "mega-tg-1"
    preview_handler.upload_preview.return_value = "preview-tg-1"

    telegram_info = TelegramInfo(message_id=10, chat_id=20)
    result = await handler.upload_telegram(Path("telegram_video.mp4"), telegram_info, dest="/Telegram")

    assert result.status == UploadStatus.SUCCESS
    repository.save_telegram.assert_awaited_once_with("source-tg-1", telegram_info)
    preview_handler.upload_preview.assert_awaited_once()


@pytest.mark.asyncio
async def test_upload_fails_when_storage_returns_none(monkeypatch):
    monkeypatch.setattr("uploader.use_cases.single_upload.generate_id", lambda: "source-fail-1")

    config = UploadConfig(generate_preview=True)
    handler, analyzer, repository, storage, preview_handler = _build_handler(config)

    analyzer.analyze_async.return_value = {"duration": 8}
    storage.upload_video.return_value = None

    result = await handler.upload(Path("broken.mp4"))

    assert result.status == UploadStatus.FAILED
    assert result.error == "Upload to MEGA failed"
    repository.save_document.assert_not_awaited()
    preview_handler.upload_preview.assert_not_awaited()
