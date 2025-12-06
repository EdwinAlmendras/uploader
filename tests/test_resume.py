"""Simple tests for ResumeService."""
import pytest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch
from uploader.services.resume import ResumeService, blake3_file


class TestBlake3File:
    """Test blake3 calculation."""
    
    @pytest.mark.asyncio
    async def test_blake3_file(self, tmp_path):
        """Test blake3 calculation on a file."""
        test_file = tmp_path / "test.txt"
        test_file.write_text("hello world")
        
        result = await blake3_file(test_file)
        
        assert len(result) == 64  # BLAKE3 hex length (same as SHA256)
        # BLAKE3 hash of "hello world"
        assert result == "d74981efa70a0c880b8d8c1985d075dbcbf679b99a5f9914e5aaf96b831a9e24"


class TestResumeService:
    """Test ResumeService."""
    
    @pytest.fixture
    def mock_storage(self):
        """Mock storage with exists method."""
        storage = MagicMock()
        storage.exists = AsyncMock(return_value=False)
        return storage
    
    @pytest.fixture
    def service(self, mock_storage):
        """Create ResumeService instance."""
        return ResumeService("http://test-api", mock_storage)
    
    @pytest.mark.asyncio
    async def test_exists_in_mega_uses_storage(self, service, mock_storage):
        """Test _exists_in_mega calls storage.exists."""
        mock_storage.exists = AsyncMock(return_value=True)
        
        result = await service._exists_in_mega("/Folder", "file.mp4")
        
        assert result is True
        mock_storage.exists.assert_called_once_with("/Folder/file.mp4")
    
    @pytest.mark.asyncio
    async def test_exists_in_mega_returns_false_on_error(self, service, mock_storage):
        """Test _exists_in_mega returns False on error."""
        mock_storage.exists = AsyncMock(side_effect=Exception("Network error"))
        
        result = await service._exists_in_mega("/Folder", "file.mp4")
        
        assert result is False
    
    @pytest.mark.asyncio
    async def test_filter_pending_skips_existing(self, service, mock_storage, tmp_path):
        """Test filter_pending skips files that exist in MEGA."""
        # Create test files
        file1 = tmp_path / "exists.mp4"
        file2 = tmp_path / "new.mp4"
        file1.write_bytes(b"existing")
        file2.write_bytes(b"new file")
        
        # Mock: first file exists, second doesn't
        mock_storage.exists = AsyncMock(side_effect=[True, False])
        
        with patch.object(service, '_lookup_source_ids', return_value={}):
            pending, recovered = await service.filter_pending(
                [file1, file2], "/Dest"
            )
        
        assert len(pending) == 1
        assert pending[0] == file2
    
    @pytest.mark.asyncio
    async def test_filter_pending_recovers_source_ids(self, service, mock_storage, tmp_path):
        """Test filter_pending recovers source_ids from API."""
        file1 = tmp_path / "file1.mp4"
        file1.write_bytes(b"content")
        blake3_hash = await blake3_file(file1)
        
        mock_storage.exists = AsyncMock(return_value=False)
        
        with patch.object(service, '_lookup_source_ids', return_value={blake3_hash: "abc123"}):
            pending, recovered = await service.filter_pending([file1], "/Dest")
        
        assert len(pending) == 1
        assert recovered.get(blake3_hash) == "abc123"
    
    @pytest.mark.asyncio
    async def test_lookup_source_ids_empty_list(self, service):
        """Test _lookup_source_ids with empty list."""
        result = await service._lookup_source_ids([])
        assert result == {}
    
    @pytest.mark.asyncio
    async def test_lookup_source_ids_no_api_url(self, mock_storage):
        """Test _lookup_source_ids without API URL."""
        service = ResumeService(None, mock_storage)
        result = await service._lookup_source_ids(["blake3_hash"])
        assert result == {}

