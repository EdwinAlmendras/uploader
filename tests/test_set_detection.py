"""Tests for image set detection and processing."""
import pytest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, Mock, patch
import tempfile
import shutil

from uploader.orchestrator.folder.set_detector import SetDetector
from uploader.orchestrator.folder.set_processor import ImageSetProcessor
from uploader.orchestrator.file_collector import FileCollector
from uploader.orchestrator.folder.handler import FolderUploadHandler
from uploader.services.analyzer import AnalyzerService
from uploader.services.repository import MetadataRepository
from uploader.services.storage import StorageService
from uploader.models import UploadConfig


class TestSetDetector:
    """Test set detection logic."""
    
    @pytest.fixture
    def detector(self):
        return SetDetector()
    
    def test_detect_image_set_valid(self, detector, tmp_path):
        """Test detection of a valid image set (only images, no videos/subfolders)."""
        # Create a folder with only images
        set_folder = tmp_path / "test_set"
        set_folder.mkdir()
        
        (set_folder / "image1.jpg").write_bytes(b"fake image")
        (set_folder / "image2.png").write_bytes(b"fake image")
        (set_folder / "image3.jpg").write_bytes(b"fake image")
        
        assert detector._is_image_set(set_folder) is True
    
    def test_detect_image_set_with_video(self, detector, tmp_path):
        """Test that folder with video moves images to subfolder."""
        # Create a folder with images and a video
        set_folder = tmp_path / "mixed_folder"
        set_folder.mkdir()
        
        (set_folder / "image1.jpg").write_bytes(b"fake image")
        (set_folder / "video.mp4").write_bytes(b"fake video")
        
        # Should move images to subfolder
        detector._move_images_to_subfolder(set_folder)
        
        # Check that images were moved
        subfolder = set_folder / set_folder.name
        assert subfolder.exists()
        assert (subfolder / "image1.jpg").exists()
        # Original folder should still have video
        assert (set_folder / "video.mp4").exists()
    
    def test_detect_image_set_with_subfolder(self, detector, tmp_path):
        """Test that folder with subfolder moves images to subfolder."""
        # Create a folder with images and a subfolder
        set_folder = tmp_path / "mixed_folder"
        set_folder.mkdir()
        
        (set_folder / "image1.jpg").write_bytes(b"fake image")
        (set_folder / "subfolder").mkdir()
        (set_folder / "subfolder" / "file.txt").write_text("test")
        
        # Should move images to subfolder
        detector._move_images_to_subfolder(set_folder)
        
        # Check that images were moved
        subfolder = set_folder / set_folder.name
        assert subfolder.exists()
        assert (subfolder / "image1.jpg").exists()
        # Original subfolder should still exist
        assert (set_folder / "subfolder").exists()
    
    def test_detect_sets_recursive(self, detector, tmp_path):
        """Test recursive set detection."""
        # Create structure:
        # root/
        #   set1/ (valid set)
        #   set2/ (valid set)
        #   mixed/ (has video, should create subfolder)
        #   video.mp4 (individual file)
        
        set1 = tmp_path / "set1"
        set1.mkdir()
        (set1 / "img1.jpg").write_bytes(b"fake")
        (set1 / "img2.jpg").write_bytes(b"fake")
        
        set2 = tmp_path / "set2"
        set2.mkdir()
        (set2 / "img1.png").write_bytes(b"fake")
        
        mixed = tmp_path / "mixed"
        mixed.mkdir()
        (mixed / "img1.jpg").write_bytes(b"fake")
        (mixed / "video.mp4").write_bytes(b"fake")
        
        (tmp_path / "video.mp4").write_bytes(b"fake")
        
        sets, individual_files = detector.detect_sets(tmp_path)
        
        # Should find at least set1 and set2
        set_names = {s.name for s in sets}
        assert "set1" in set_names or any("set1" in str(s) for s in sets)
        assert "set2" in set_names or any("set2" in str(s) for s in sets)
        
        # Should find the video file
        assert any("video.mp4" in str(f) for f in individual_files)


class TestFileCollector:
    """Test file collector with set detection."""
    
    @pytest.fixture
    def collector(self):
        return FileCollector()
    
    def test_detect_sets_and_files(self, collector, tmp_path):
        """Test detection of sets and individual files."""
        # Create a set
        set_folder = tmp_path / "my_set"
        set_folder.mkdir()
        (set_folder / "img1.jpg").write_bytes(b"fake")
        (set_folder / "img2.jpg").write_bytes(b"fake")
        
        # Create individual files
        (tmp_path / "video.mp4").write_bytes(b"fake")
        (tmp_path / "photo.jpg").write_bytes(b"fake")
        
        sets, individual_files = collector.detect_sets_and_files(tmp_path)
        
        assert len(sets) >= 1
        assert len(individual_files) >= 2


class TestImageSetProcessor:
    """Test image set processing."""
    
    @pytest.fixture
    def mock_analyzer(self):
        analyzer = MagicMock(spec=AnalyzerService)
        analyzer.analyze_photo_async = AsyncMock(return_value={
            "source_id": "test_id",
            "filename": "test.jpg",
            "mimetype": "image/jpeg",
            "width": 1920,
            "height": 1080,
            "blake3_hash": "test_hash"
        })
        return analyzer
    
    @pytest.fixture
    def mock_repository(self):
        repo = MagicMock(spec=MetadataRepository)
        repo.save_document = AsyncMock()
        repo.save_photo_metadata = AsyncMock()
        repo.save_set_document = AsyncMock()
        return repo
    
    @pytest.fixture
    def mock_storage(self):
        storage = MagicMock(spec=StorageService)
        storage.upload_video = AsyncMock(return_value="mega_handle_123")
        storage.upload_preview = AsyncMock(return_value="preview_handle_123")
        storage.create_folder = AsyncMock(return_value="folder_handle")
        return storage
    
    @pytest.fixture
    def config(self):
        return UploadConfig()
    
    @pytest.fixture
    def processor(self, mock_analyzer, mock_repository, mock_storage, config):
        return ImageSetProcessor(mock_analyzer, mock_repository, mock_storage, config)
    
    @pytest.mark.asyncio
    async def test_process_set_generates_thumbnails(self, processor, tmp_path):
        """Test that processing a set generates thumbnails."""
        # Create a set with real images (copy from tests_files if available)
        set_folder = tmp_path / "test_set"
        set_folder.mkdir()
        
        # Create a simple test image using PIL if available
        try:
            from PIL import Image
            test_img = Image.new('RGB', (2000, 1500), color='red')
            test_img.save(set_folder / "test1.jpg")
            test_img.save(set_folder / "test2.jpg")
        except ImportError:
            # If PIL not available, skip thumbnail generation test
            pytest.skip("PIL not available for thumbnail generation test")
        
        # Mock the archiver to avoid needing 7z
        with patch.object(processor._archiver, 'create', return_value=[tmp_path / "test.7z"]):
            result, image_results = await processor.process_set(
                set_folder,
                "/test_dest",
                None
            )
        
        # Check that thumbnail folders were created
        thumb_320 = set_folder / "m"
        thumb_1024 = set_folder / "x"
        
        # Thumbnails should be generated (if PIL available)
        # Note: This might fail if images are too small, but structure should exist
        assert thumb_320.exists() or thumb_1024.exists()
    
    @pytest.mark.asyncio
    async def test_process_set_saves_images_with_set_doc_id(
        self, processor, mock_repository, tmp_path
    ):
        """Test that images are saved with set_doc_id reference."""
        set_folder = tmp_path / "test_set"
        set_folder.mkdir()
        
        # Create test images
        (set_folder / "img1.jpg").write_bytes(b"fake image 1")
        (set_folder / "img2.jpg").write_bytes(b"fake image 2")
        
        # Mock archiver
        with patch.object(processor._archiver, 'create', return_value=[tmp_path / "test.7z"]):
            result, image_results = await processor.process_set(
                set_folder,
                "/test_dest",
                None
            )
        
        # Verify that save_document was called with set_doc_id
        calls = mock_repository.save_document.call_args_list
        
        # Should have calls for images (with set_doc_id) and set document
        image_calls = [c for c in calls if 'set_doc_id' in str(c)]
        assert len(image_calls) >= 2  # At least 2 images
        
        # Verify set_doc_id is present in image document calls
        for call in image_calls:
            call_kwargs = call[1] if call[0] == () else {}
            call_args = call[0] if call[0] != () else []
            # Check if set_doc_id is in the data
            if call_args:
                data = call_args[0]
                assert 'set_doc_id' in data
    
    @pytest.mark.asyncio
    async def test_process_set_creates_and_uploads_7z(
        self, processor, mock_storage, tmp_path
    ):
        """Test that 7z archive is created and uploaded, along with cover and grid."""
        set_folder = tmp_path / "test_set"
        set_folder.mkdir()
        (set_folder / "img1.jpg").write_bytes(b"fake image")
        (set_folder / "img2.jpg").write_bytes(b"fake image")
        
        # Create a fake 7z file
        archive_file = tmp_path / "test_set.7z"
        archive_file.write_bytes(b"fake 7z content")
        
        # Mock archiver to return our fake archive
        # Mock grid preview generator to return a temp file
        with patch.object(processor._archiver, 'create', return_value=[archive_file]), \
             patch.object(processor, '_generate_grid_preview', new_callable=AsyncMock) as mock_grid:
            
            # Create a temp grid preview file
            temp_grid = tmp_path / "grid_temp.jpg"
            temp_grid.write_bytes(b"fake grid")
            mock_grid.return_value = temp_grid
            
            result, image_results = await processor.process_set(
                set_folder,
                "/test_dest",
                None
            )
        
        # Verify 7z upload was called
        mock_storage.upload_video.assert_called()
        
        # Verify cover was uploaded (upload_preview should be called for cover)
        mock_storage.upload_preview.assert_called()
        
        # Verify the archive file was passed to upload
        upload_calls = mock_storage.upload_video.call_args_list
        assert len(upload_calls) > 0
        
        # Verify cover and grid were uploaded via upload_preview
        preview_calls = mock_storage.upload_preview.call_args_list
        assert len(preview_calls) >= 2, f"Expected at least 2 preview uploads (cover and grid), got {len(preview_calls)}"
        
        # Extract call arguments to verify filenames
        cover_found = False
        grid_found = False
        
        for call in preview_calls:
            # Get keyword arguments
            kwargs = call.kwargs if hasattr(call, 'kwargs') else {}
            # Get positional arguments (if any)
            args = call.args if hasattr(call, 'args') else []
            
            # Check filename parameter
            filename = kwargs.get('filename', None)
            if filename:
                if filename == "test_set_cover.jpg":
                    cover_found = True
                    assert kwargs.get('dest_path') == "/test_dest", "Cover should be uploaded to same dest_path as 7z"
                elif filename == "test_set.jpg":
                    grid_found = True
                    assert kwargs.get('dest_path') == "/test_dest", "Grid should be uploaded to same dest_path as 7z"
        
        assert cover_found, "Cover should be uploaded with filename 'test_set_cover.jpg'"
        assert grid_found, "Grid should be uploaded with filename 'test_set.jpg'"


class TestFolderUploadHandlerWithSets:
    """Test folder upload handler with set detection and processing."""
    
    @pytest.fixture
    def mock_analyzer(self):
        analyzer = MagicMock(spec=AnalyzerService)
        analyzer.analyze_photo_async = AsyncMock(return_value={
            "source_id": "test_id",
            "filename": "test.jpg",
            "mimetype": "image/jpeg",
            "width": 1920,
            "height": 1080,
            "blake3_hash": "test_hash"
        })
        analyzer.analyze_video_async = AsyncMock(return_value={
            "source_id": "test_video_id",
            "filename": "test.mp4",
            "mimetype": "video/mp4",
            "duration": 100.0,
            "blake3_hash": "test_hash_video"
        })
        return analyzer
    
    @pytest.fixture
    def mock_repository(self):
        repo = MagicMock(spec=MetadataRepository)
        repo.save_document = AsyncMock()
        repo.save_photo_metadata = AsyncMock()
        repo.save_video_metadata = AsyncMock()
        repo.save_set_document = AsyncMock()
        repo.check_exists_batch = AsyncMock(return_value={})
        return repo
    
    @pytest.fixture
    def mock_storage(self):
        storage = MagicMock(spec=StorageService)
        storage.upload_video = AsyncMock(return_value="mega_handle_123")
        storage.upload_preview = AsyncMock(return_value="preview_handle_123")
        storage.create_folder = AsyncMock(return_value="folder_handle")
        storage.exists = AsyncMock(return_value=False)
        return storage
    
    @pytest.fixture
    def mock_preview_handler(self):
        from uploader.orchestrator.preview_handler import PreviewHandler
        handler = MagicMock(spec=PreviewHandler)
        handler.upload_preview = AsyncMock(return_value="preview_handle")
        return handler
    
    @pytest.fixture
    def config(self):
        return UploadConfig()
    
    @pytest.fixture
    def handler(
        self, mock_analyzer, mock_repository, mock_storage, 
        mock_preview_handler, config
    ):
        return FolderUploadHandler(
            mock_analyzer,
            mock_repository,
            mock_storage,
            mock_preview_handler,
            config
        )
    
    @pytest.mark.asyncio
    async def test_upload_folder_detects_sets_first(
        self, handler, tmp_path, mock_repository, mock_storage
    ):
        """Test that folder upload detects and processes sets before individual files."""
        # Create structure:
        # root/
        #   set1/ (image set)
        #   video.mp4 (individual file)
        
        set1 = tmp_path / "set1"
        set1.mkdir()
        (set1 / "img1.jpg").write_bytes(b"fake image")
        (set1 / "img2.jpg").write_bytes(b"fake image")
        
        (tmp_path / "video.mp4").write_bytes(b"fake video")
        
        # Mock the set processor to avoid actual processing
        mock_process_set = AsyncMock(return_value=(
            MagicMock(success=True, filename="set1.7z"),
            [MagicMock(success=True), MagicMock(success=True)]
        ))
        handler._set_processor.process_set = mock_process_set
        
        # Mock file existence checker to return empty (no existing files)
        handler._existence_checker.check = AsyncMock(return_value=([], 0))
        handler._blake3_deduplicator = None  # Disable deduplication for simplicity
        
        # Mock archiver
        with patch.object(handler._set_processor._archiver, 'create', 
                        return_value=[tmp_path / "set1.7z"]):
            
            process = handler.upload_folder(tmp_path, "/test_dest")
            result = await process.wait()
        
        # Verify set processor was called
        assert mock_process_set.called
        
        # Verify repository was called for set document (through the set processor)
        # Note: This is called inside process_set, so we verify the mock was called
        assert mock_process_set.call_count > 0
    
    @pytest.mark.asyncio
    async def test_upload_folder_with_real_set(self, handler, tmp_path):
        """Test upload folder with a real set from tests_files."""
        # Try to use the real set from tests_files if it exists
        real_set_path = Path(__file__).parent.parent / "tests_files" / "set"
        
        if not real_set_path.exists():
            pytest.skip("tests_files/set not found")
        
        # Copy set to tmp_path for testing
        test_set = tmp_path / "test_set"
        shutil.copytree(real_set_path, test_set)
        
        # Mock services to avoid actual uploads
        handler._storage.upload_video = AsyncMock(return_value="mega_handle")
        handler._storage.create_folder = AsyncMock(return_value="folder_handle")
        handler._storage.exists = AsyncMock(return_value=False)
        
        # Mock repository (access through file_processor or set_processor)
        handler._file_processor._repository.check_exists_batch = AsyncMock(return_value={})
        handler._set_processor._repository.check_exists_batch = AsyncMock(return_value={})
        handler._blake3_deduplicator = None  # Disable for simplicity
        
        # Mock archiver to return a fake archive
        fake_archive = tmp_path / "test_set.7z"
        fake_archive.write_bytes(b"fake archive")
        
        with patch.object(handler._set_processor._archiver, 'create', 
                         return_value=[fake_archive]):
            
            process = handler.upload_folder(test_set, "/test_dest")
            result = await process.wait()
        
        # Verify that processing was attempted
        assert result is not None
        # The result should indicate processing was done
        # (exact assertions depend on implementation)

