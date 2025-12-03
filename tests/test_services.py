"""Tests for uploader services."""
import pytest
from pathlib import Path
from unittest.mock import Mock, AsyncMock, patch

from uploader.services.analyzer import AnalyzerService
from uploader.services.preview import PreviewService
from uploader.services.storage import StorageService
from uploader.models import UploadConfig


class TestAnalyzerService:
    def test_is_video(self):
        assert AnalyzerService.is_video(Path("video.mp4")) is True
        assert AnalyzerService.is_video(Path("video.mkv")) is True
        assert AnalyzerService.is_video(Path("photo.jpg")) is False
    
    def test_is_photo(self):
        assert AnalyzerService.is_photo(Path("photo.jpg")) is True
        assert AnalyzerService.is_photo(Path("photo.png")) is True
        assert AnalyzerService.is_photo(Path("video.mp4")) is False
    
    def test_analyze_unsupported(self):
        service = AnalyzerService()
        with pytest.raises(ValueError, match="Unsupported file type"):
            service.analyze(Path("file.txt"))


class TestPreviewService:
    def test_get_preview_name(self):
        service = PreviewService()
        assert service.get_preview_name("abc123") == "abc123.jpg"
    
    def test_config_grid_size(self):
        config = UploadConfig(grid_size_short=3, grid_size_long=6)
        service = PreviewService(config)
        
        # Access config through service
        assert service._config.get_grid_size(100) == 3
        assert service._config.get_grid_size(2000) == 6


class TestStorageService:
    @pytest.fixture
    def mock_client(self):
        client = Mock()
        client.upload = AsyncMock(return_value=Mock(handle="uploaded_handle"))
        client.get = AsyncMock(return_value=None)
        client.get_root = AsyncMock(return_value=Mock(handle="root_handle"))
        client.mkdir = AsyncMock(return_value=Mock(handle="folder_handle"))
        return client
    
    @pytest.mark.asyncio
    async def test_upload_preview_creates_folder(self, mock_client):
        service = StorageService(mock_client)
        
        # First call - folder doesn't exist
        mock_client.get.return_value = None
        
        handle = await service.upload_preview(Path("preview.jpg"), "abc123")
        
        # Should create folder
        mock_client.mkdir.assert_called_once()
        assert handle == "uploaded_handle"
    
    @pytest.mark.asyncio
    async def test_upload_preview_reuses_folder(self, mock_client):
        service = StorageService(mock_client)
        
        # Folder exists
        mock_client.get.return_value = Mock(handle="existing_handle")
        
        await service.upload_preview(Path("preview1.jpg"), "abc1")
        await service.upload_preview(Path("preview2.jpg"), "abc2")
        
        # Should only call get once, then reuse cached handle
        mock_client.mkdir.assert_not_called()
