"""Tests for uploader services."""
import pytest
from pathlib import Path
from unittest.mock import Mock, AsyncMock, patch, MagicMock

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
        client.create_folder = AsyncMock(return_value=Mock(handle="folder_handle"))
        return client
    
    @pytest.mark.asyncio
    async def test_upload_preview_creates_folder(self, mock_client):
        service = StorageService(mock_client)
        
        # First call - folder doesn't exist
        mock_client.get.return_value = None
        
        handle = await service.upload_preview(Path("preview.jpg"), "abc123")
        
        # Should create folder using create_folder
        mock_client.create_folder.assert_called_once()
        assert handle == "uploaded_handle"
    
    @pytest.mark.asyncio
    async def test_upload_preview_reuses_folder(self, mock_client):
        service = StorageService(mock_client)
        
        # Folder exists
        mock_client.get.return_value = Mock(handle="existing_handle")
        
        await service.upload_preview(Path("preview1.jpg"), "abc1")
        await service.upload_preview(Path("preview2.jpg"), "abc2")
        
        # Should only call get once, then reuse cached handle
        mock_client.create_folder.assert_not_called()


class TestManagedStorageService:
    """Tests for ManagedStorageService with multi-account cache."""
    
    @pytest.fixture
    def mock_account_manager(self):
        """Create a mock AccountManager."""
        manager = Mock()
        manager._current_account = "account1"
        # Create account mocks with numeric attributes
        account1 = Mock(name="account1")
        account1.space_used = 0
        account1.space_free = 1000000000  # 1GB
        
        account2 = Mock(name="account2")
        account2.space_used = 0
        account2.space_free = 1000000000  # 1GB
        
        manager._accounts = {
            "account1": account1,
            "account2": account2
        }
        return manager
    
    @pytest.fixture
    def mock_client1(self):
        """Mock client for account1."""
        client = Mock()
        client.upload = AsyncMock(return_value=Mock(handle="handle1"))
        client.get = AsyncMock(return_value=None)
        client.get_root = AsyncMock(return_value=Mock(handle="root1"))
        client.create_folder = AsyncMock(return_value=Mock(handle="folder1"))
        return client
    
    @pytest.fixture
    def mock_client2(self):
        """Mock client for account2."""
        client = Mock()
        client.upload = AsyncMock(return_value=Mock(handle="handle2"))
        client.get = AsyncMock(return_value=None)
        client.get_root = AsyncMock(return_value=Mock(handle="root2"))
        client.create_folder = AsyncMock(return_value=Mock(handle="folder2"))
        return client
    
    @pytest.mark.asyncio
    async def test_folder_cache_per_account(self, mock_account_manager, mock_client1, mock_client2):
        """Test that each account has its own folder cache."""
        from uploader.services.managed_storage import ManagedStorageService
        
        service = ManagedStorageService()
        service._manager = mock_account_manager
        
        # Mock get_client_for to return different clients for different accounts
        async def get_client_for(file_size):
            account = mock_account_manager._current_account
            if account == "account1":
                return mock_client1
            else:
                return mock_client2
        
        mock_account_manager.get_client_for = AsyncMock(side_effect=get_client_for)
        
        # Upload to account1
        mock_account_manager._current_account = "account1"
        mock_client1.get.return_value = Mock(handle="telegram_folder_1")
        
        handle1 = await service._get_or_create_folder(mock_client1, "/Telegram/channel1", "account1")
        
        # Verify cache was set for account1
        # Note: path is normalized (leading "/" is stripped)
        assert "account1" in service._folder_cache
        assert "Telegram/channel1" in service._folder_cache["account1"]
        assert service._folder_cache["account1"]["Telegram/channel1"] == "telegram_folder_1"
        
        # Upload to account2
        mock_account_manager._current_account = "account2"
        mock_client2.get.return_value = Mock(handle="telegram_folder_2")
        
        handle2 = await service._get_or_create_folder(mock_client2, "/Telegram/channel1", "account2")
        
        # Verify cache was set for account2 separately
        assert "account2" in service._folder_cache
        assert "Telegram/channel1" in service._folder_cache["account2"]
        assert service._folder_cache["account2"]["Telegram/channel1"] == "telegram_folder_2"
        
        # Verify accounts have different handles
        assert handle1 == "telegram_folder_1"
        assert handle2 == "telegram_folder_2"
        assert handle1 != handle2
    
    @pytest.mark.asyncio
    async def test_preview_folder_per_account(self, mock_account_manager, mock_client1, mock_client2):
        """Test that preview folder cache is per account."""
        from uploader.services.managed_storage import ManagedStorageService
        
        service = ManagedStorageService()
        service._manager = mock_account_manager
        
        # Account1 - create preview folder
        mock_account_manager._current_account = "account1"
        mock_client1.get.return_value = Mock(handle="previews_1")
        
        handle1 = await service._ensure_preview_folder(mock_client1, "account1")
        
        # Verify cache attribute exists for account1
        assert hasattr(service, "_preview_account1")
        assert getattr(service, "_preview_account1") == "previews_1"
        
        # Account2 - create preview folder
        mock_account_manager._current_account = "account2"
        mock_client2.get.return_value = Mock(handle="previews_2")
        
        handle2 = await service._ensure_preview_folder(mock_client2, "account2")
        
        # Verify cache attribute exists for account2
        assert hasattr(service, "_preview_account2")
        assert getattr(service, "_preview_account2") == "previews_2"
        
        # Verify accounts have different handles
        assert handle1 == "previews_1"
        assert handle2 == "previews_2"
        assert handle1 != handle2
    
    @pytest.mark.asyncio
    async def test_upload_video_tracks_account_change(self, mock_account_manager, mock_client1, mock_client2):
        """Test that upload_video correctly tracks account changes."""
        from uploader.services.managed_storage import ManagedStorageService
        
        service = ManagedStorageService()
        service._manager = mock_account_manager
        
        # Mock get_client_for
        async def get_client_for(file_size):
            account = mock_account_manager._current_account
            if account == "account1":
                return mock_client1
            else:
                return mock_client2
        
        mock_account_manager.get_client_for = AsyncMock(side_effect=get_client_for)
        
        # Create a temporary file for testing
        import tempfile
        with tempfile.NamedTemporaryFile(delete=False, suffix=".mp4") as tmp:
            tmp.write(b"fake video content")
            tmp_path = Path(tmp.name)
        
        try:
            # Upload with account1
            mock_account_manager._current_account = "account1"
            mock_client1.get.return_value = Mock(handle="dest_folder_1")
            
            # Mock the account to have proper numeric attributes
            account1 = mock_account_manager._accounts["account1"]
            account1.space_used = 0
            account1.space_free = 1000000000
            
            handle1 = await service.upload_video(tmp_path, "/Telegram/channel1", "source1")
            
            assert service._last_account == "account1"
            assert "account1" in service._folder_cache
            
            # Upload with account2
            mock_account_manager._current_account = "account2"
            mock_client2.get.return_value = Mock(handle="dest_folder_2")
            
            # Mock the account to have proper numeric attributes
            account2 = mock_account_manager._accounts["account2"]
            account2.space_used = 0
            account2.space_free = 1000000000
            
            handle2 = await service.upload_video(tmp_path, "/Telegram/channel1", "source2")
            
            assert service._last_account == "account2"
            assert "account2" in service._folder_cache
            
            # Verify both accounts have their own cache entries
            assert "account1" in service._folder_cache
            assert "account2" in service._folder_cache
            
        finally:
            # Cleanup
            if tmp_path.exists():
                tmp_path.unlink()
