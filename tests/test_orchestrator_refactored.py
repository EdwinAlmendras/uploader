"""Tests for refactored orchestrator package."""
import pytest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock
from uploader.orchestrator import UploadOrchestrator, FolderUploadResult, UploadTask
from uploader.models import UploadConfig, TelegramInfo


class TestOrchestratorRefactored:
    """Test refactored orchestrator structure."""
    
    @pytest.mark.asyncio
    async def test_orchestrator_imports(self):
        """Test that orchestrator imports correctly."""
        from uploader.orchestrator import UploadOrchestrator
        from uploader.orchestrator.models import FolderUploadResult, UploadTask
        from uploader.orchestrator.parallel import get_parallel_count
        from uploader.orchestrator.file_collector import FileCollector
        from uploader.orchestrator.preview_handler import PreviewHandler
        from uploader.orchestrator.single_upload import SingleUploadHandler
        from uploader.orchestrator.folder_upload import FolderUploadHandler
        
        assert UploadOrchestrator is not None
        assert FolderUploadResult is not None
        assert UploadTask is not None
    
    @pytest.mark.asyncio
    async def test_parallel_count_calculation(self):
        """Test parallel count calculation."""
        from uploader.orchestrator.parallel import get_parallel_count
        
        MB = 1024 * 1024
        
        # Small files
        assert get_parallel_count(0.5 * MB) == 10
        
        # Medium files
        assert get_parallel_count(5 * MB) == 6
        
        # Large files
        assert get_parallel_count(50 * MB) == 3
    
    @pytest.mark.asyncio
    async def test_file_collector(self, tmp_path):
        """Test file collection."""
        from uploader.orchestrator.file_collector import FileCollector
        
        # Create test files
        (tmp_path / "video.mp4").write_bytes(b"fake video")
        (tmp_path / "photo.jpg").write_bytes(b"fake photo")
        (tmp_path / "text.txt").write_text("not media")
        (tmp_path / "subdir").mkdir()
        (tmp_path / "subdir" / "video2.mp4").write_bytes(b"fake video 2")
        
        collector = FileCollector()
        files = collector.collect_files(tmp_path)
        
        # Should find 3 media files (2 videos, 1 photo)
        assert len(files) == 3
        assert any("video.mp4" in str(f) for f in files)
        assert any("photo.jpg" in str(f) for f in files)
        assert any("video2.mp4" in str(f) for f in files)
        assert not any("text.txt" in str(f) for f in files)
    
    def test_folder_upload_result(self):
        """Test FolderUploadResult model."""
        from uploader.models import UploadResult
        
        result = FolderUploadResult(
            success=True,
            folder_name="test",
            total_files=10,
            uploaded_files=8,
            failed_files=2,
            results=[UploadResult.ok("abc123", "test.mp4", "handle123")]
        )
        
        assert result.folder_name == "test"
        assert result.total_files == 10
        assert result.all_success is False  # Has 2 failed files
    
    def test_upload_task(self):
        """Test UploadTask model."""
        task = UploadTask(
            file_path=Path("test.mp4"),
            source_id="abc123",
            tech_data={"duration": 100},
            is_video=True,
            rel_path=Path("test.mp4"),
            mega_dest="/test",
            file_size=1024
        )
        
        assert task.source_id == "abc123"
        assert task.is_video is True
        assert task.file_size == 1024

