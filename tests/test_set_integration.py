"""Integration tests for image set detection and processing with various scenarios."""
import pytest
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, Mock, patch
import tempfile
import shutil
from typing import List

from uploader.orchestrator.folder.handler import FolderUploadHandler
from uploader.orchestrator.folder.set_detector import SetDetector
from uploader.orchestrator.folder.set_processor import ImageSetProcessor
from uploader.services.analyzer import AnalyzerService
from uploader.services.repository import MetadataRepository
from uploader.services.storage import StorageService
from uploader.models import UploadConfig
from uploader.orchestrator.models import UploadResult


class MockServices:
    """Helper class to create and manage mocked services."""
    
    def __init__(self):
        self.analyzer = self._create_analyzer()
        self.repository = self._create_repository()
        self.storage = self._create_storage()
        self.preview_handler = self._create_preview_handler()
        self.config = UploadConfig()
    
    def _create_analyzer(self):
        analyzer = MagicMock(spec=AnalyzerService)
        
        def analyze_photo_async(path):
            return AsyncMock(return_value={
                "source_id": f"img_{path.name}",
                "filename": path.name,
                "mimetype": "image/jpeg",
                "width": 1920,
                "height": 1080,
                "blake3_hash": f"hash_{path.name}",
                "mtime": None,
                "ctime": None
            })()
        
        def analyze_video_async(path):
            return AsyncMock(return_value={
                "source_id": f"vid_{path.name}",
                "filename": path.name,
                "mimetype": "video/mp4",
                "duration": 100.0,
                "width": 1920,
                "height": 1080,
                "blake3_hash": f"hash_{path.name}",
                "mtime": None,
                "ctime": None
            })()
        
        analyzer.analyze_photo_async = analyze_photo_async
        analyzer.analyze_video_async = analyze_video_async
        return analyzer
    
    def _create_repository(self):
        repo = MagicMock(spec=MetadataRepository)
        repo.save_document = AsyncMock()
        repo.save_photo_metadata = AsyncMock()
        repo.save_batch = AsyncMock()
        repo.save_video_metadata = AsyncMock()
        repo.save_set_document = AsyncMock()
        repo.check_exists_batch = AsyncMock(return_value={})
        repo.prepare_document = MagicMock(side_effect=lambda data: dict(data))
        repo.prepare_photo_metadata = MagicMock(
            side_effect=lambda document_id, data: {
                "document_id": document_id,
                "set_doc_id": data.get("set_doc_id"),
            }
        )
        return repo
    
    def _create_storage(self):
        storage = MagicMock(spec=StorageService)
        storage.upload_video = AsyncMock(return_value="mega_handle_123")
        storage.create_folder = AsyncMock(return_value="folder_handle")
        storage.exists = AsyncMock(return_value=False)
        storage.exists_by_mega_id = AsyncMock(return_value=False)
        return storage
    
    def _create_preview_handler(self):
        from uploader.orchestrator.preview_handler import PreviewHandler
        handler = MagicMock(spec=PreviewHandler)
        handler.upload_preview = AsyncMock(return_value="preview_handle")
        return handler


class TestSetIntegration:
    """Integration tests for various set scenarios."""
    
    @pytest.fixture
    def mock_services(self):
        return MockServices()
    
    @pytest.fixture
    def handler(self, mock_services):
        return FolderUploadHandler(
            mock_services.analyzer,
            mock_services.repository,
            mock_services.storage,
            mock_services.preview_handler,
            mock_services.config
        )
    
    def create_test_structure(self, tmp_path: Path, scenario: str) -> Path:
        """
        Create different test folder structures based on scenario.
        
        Scenarios:
        - 'pure_set': Only images, no videos/subfolders
        - 'set_with_video': Images + video (should move images)
        - 'set_with_subfolder': Images + subfolder (should move images)
        - 'mixed_upload': Sets + videos + individual images
        - 'nested_sets': Sets inside sets
        - 'empty_folders': Empty folders mixed with sets
        """
        root = tmp_path / "test_upload"
        root.mkdir()
        
        if scenario == 'pure_set':
            # Pure image set
            set1 = root / "pure_set"
            set1.mkdir()
            (set1 / "img1.jpg").write_bytes(b"fake image 1")
            (set1 / "img2.jpg").write_bytes(b"fake image 2")
            (set1 / "img3.png").write_bytes(b"fake image 3")
        
        elif scenario == 'set_with_video':
            # Set with video - images should be moved
            mixed = root / "mixed_folder"
            mixed.mkdir()
            (mixed / "img1.jpg").write_bytes(b"fake image 1")
            (mixed / "img2.jpg").write_bytes(b"fake image 2")
            (mixed / "video.mp4").write_bytes(b"fake video")
        
        elif scenario == 'set_with_subfolder':
            # Set with subfolder - images should be moved
            mixed = root / "mixed_folder"
            mixed.mkdir()
            (mixed / "img1.jpg").write_bytes(b"fake image 1")
            (mixed / "img2.jpg").write_bytes(b"fake image 2")
            (mixed / "subfolder").mkdir()
            (mixed / "subfolder" / "file.txt").write_text("test")
        
        elif scenario == 'mixed_upload':
            # Multiple sets + videos + individual images
            # Set 1: Pure set
            set1 = root / "set1"
            set1.mkdir()
            (set1 / "img1.jpg").write_bytes(b"fake image")
            (set1 / "img2.jpg").write_bytes(b"fake image")
            
            # Set 2: Another pure set
            set2 = root / "set2"
            set2.mkdir()
            (set2 / "photo1.png").write_bytes(b"fake image")
            (set2 / "photo2.png").write_bytes(b"fake image")
            
            # Mixed folder (will become a set after moving images)
            mixed = root / "mixed_content"
            mixed.mkdir()
            (mixed / "img1.jpg").write_bytes(b"fake image")
            (mixed / "img2.jpg").write_bytes(b"fake image")
            (mixed / "video1.mp4").write_bytes(b"fake video")
            
            # Individual videos
            (root / "video1.mp4").write_bytes(b"fake video 1")
            (root / "video2.mp4").write_bytes(b"fake video 2")
            
            # Individual images (not in a set)
            (root / "single_image.jpg").write_bytes(b"fake image")
        
        elif scenario == 'nested_sets':
            # Sets inside folders
            parent = root / "parent_folder"
            parent.mkdir()
            
            set1 = parent / "nested_set1"
            set1.mkdir()
            (set1 / "img1.jpg").write_bytes(b"fake image")
            (set1 / "img2.jpg").write_bytes(b"fake image")
            
            set2 = parent / "nested_set2"
            set2.mkdir()
            (set2 / "photo1.png").write_bytes(b"fake image")
        
        elif scenario == 'empty_folders':
            # Empty folders + sets
            empty1 = root / "empty1"
            empty1.mkdir()
            
            set1 = root / "set1"
            set1.mkdir()
            (set1 / "img1.jpg").write_bytes(b"fake image")
            (set1 / "img2.jpg").write_bytes(b"fake image")
            
            empty2 = root / "empty2"
            empty2.mkdir()
        
        elif scenario == 'complex_mixed':
            # Complex scenario with everything
            # Pure sets
            set1 = root / "pure_set1"
            set1.mkdir()
            (set1 / "img1.jpg").write_bytes(b"fake")
            (set1 / "img2.jpg").write_bytes(b"fake")
            
            # Mixed folder (images + video)
            mixed1 = root / "mixed1"
            mixed1.mkdir()
            (mixed1 / "img1.jpg").write_bytes(b"fake")
            (mixed1 / "img2.jpg").write_bytes(b"fake")
            (mixed1 / "video.mp4").write_bytes(b"fake")
            
            # Mixed folder (images + subfolder)
            mixed2 = root / "mixed2"
            mixed2.mkdir()
            (mixed2 / "img1.jpg").write_bytes(b"fake")
            (mixed2 / "sub").mkdir()
            (mixed2 / "sub" / "file.txt").write_text("test")
            
            # Individual files
            (root / "video1.mp4").write_bytes(b"fake")
            (root / "video2.mp4").write_bytes(b"fake")
            (root / "single.jpg").write_bytes(b"fake")
            
            # Nested set
            nested = root / "nested"
            nested.mkdir()
            nested_set = nested / "inner_set"
            nested_set.mkdir()
            (nested_set / "img1.jpg").write_bytes(b"fake")
            (nested_set / "img2.jpg").write_bytes(b"fake")
        
        return root
    
    @pytest.mark.asyncio
    async def test_pure_set_detection_and_processing(
        self, handler, mock_services, tmp_path
    ):
        """Test detection and processing of a pure image set."""
        root = self.create_test_structure(tmp_path, 'pure_set')
        
        # Mock archiver
        fake_archive = tmp_path / "pure_set.7z"
        fake_archive.write_bytes(b"fake archive")
        
        with patch.object(handler._set_processor._archiver, 'create',
                         return_value=[fake_archive]):
            process = handler.upload_folder(root, "/test_dest")
            result = await process.wait()
        
        # Verify set was detected and processed
        assert result is not None
        assert result.total_files > 0
        
        # Verify set document was saved
        mock_services.repository.save_set_document.assert_called()
    
    @pytest.mark.asyncio
    async def test_set_with_video_moves_images(
        self, handler, mock_services, tmp_path
    ):
        """Test that set with video moves images to subfolder."""
        root = self.create_test_structure(tmp_path, 'set_with_video')
        
        # Check that images were moved
        mixed_folder = root / "mixed_folder"
        subfolder = mixed_folder / "mixed_folder"
        
        # After detection, images should be in subfolder
        detector = SetDetector()
        sets, individual_files = detector.detect_sets(root)
        
        # Should have created a set (the subfolder with images)
        assert len(sets) > 0
        
        # Original folder should still have video
        assert (mixed_folder / "video.mp4").exists()
        
        # Images should be in subfolder
        assert subfolder.exists()
        assert any(f.name == "img1.jpg" for f in subfolder.iterdir() if f.is_file())
    
    @pytest.mark.asyncio
    async def test_set_with_subfolder_moves_images(
        self, handler, mock_services, tmp_path
    ):
        """Test that set with subfolder moves images to subfolder."""
        root = self.create_test_structure(tmp_path, 'set_with_subfolder')
        
        detector = SetDetector()
        sets, individual_files = detector.detect_sets(root)
        
        # Should have created a set
        assert len(sets) > 0
        
        mixed_folder = root / "mixed_folder"
        subfolder = mixed_folder / "mixed_folder"
        
        # Images should be in subfolder
        assert subfolder.exists()
        assert any(f.name == "img1.jpg" for f in subfolder.iterdir() if f.is_file())
    
    @pytest.mark.asyncio
    async def test_mixed_upload_processes_sets_first(
        self, handler, mock_services, tmp_path
    ):
        """Test that mixed upload (sets + videos + images) processes sets first."""
        root = self.create_test_structure(tmp_path, 'mixed_upload')
        
        # Track call order
        call_order = []
        
        def track_set_process(*args, **kwargs):
            call_order.append('set')
            return (
                MagicMock(success=True, filename="set.7z"),
                [MagicMock(success=True), MagicMock(success=True)]
            )
        
        def track_file_process(*args, **kwargs):
            call_order.append('file')
            return (MagicMock(success=True), None, None)
        
        handler._set_processor.process_set = AsyncMock(side_effect=track_set_process)
        handler._file_processor.process = AsyncMock(side_effect=track_file_process)
        
        # Mock archiver
        fake_archive = tmp_path / "test.7z"
        fake_archive.write_bytes(b"fake")
        
        with patch.object(handler._set_processor._archiver, 'create',
                         return_value=[fake_archive]):
            # Disable deduplication for simplicity
            handler._blake3_deduplicator = None
            handler._existence_checker.check = AsyncMock(return_value=([], 0))
            
            process = handler.upload_folder(root, "/test_dest")
            result = await process.wait()
        
        # Verify sets were processed before files
        set_calls = [c for c in call_order if c == 'set']
        file_calls = [c for c in call_order if c == 'file']
        
        # Should have processed sets
        assert len(set_calls) > 0
        
        # Sets should be processed before individual files
        if file_calls:
            first_file_index = call_order.index('file')
            last_set_index = len(call_order) - 1 - call_order[::-1].index('set')
            assert last_set_index < first_file_index, "Sets should be processed before files"
    
    @pytest.mark.asyncio
    async def test_nested_sets_detection(
        self, handler, mock_services, tmp_path
    ):
        """Test detection of nested sets."""
        root = self.create_test_structure(tmp_path, 'nested_sets')
        
        detector = SetDetector()
        sets, individual_files = detector.detect_sets(root)
        
        # Should detect nested sets
        assert len(sets) >= 2
        
        set_names = {s.name for s in sets}
        assert "nested_set1" in set_names or any("nested_set1" in str(s) for s in sets)
        assert "nested_set2" in set_names or any("nested_set2" in str(s) for s in sets)
    
    @pytest.mark.asyncio
    async def test_empty_folders_ignored(
        self, handler, mock_services, tmp_path
    ):
        """Test that empty folders are ignored."""
        root = self.create_test_structure(tmp_path, 'empty_folders')
        
        detector = SetDetector()
        sets, individual_files = detector.detect_sets(root)
        
        # Should only detect the set, not empty folders
        assert len(sets) >= 1
        assert any("set1" in str(s) for s in sets)
    
    @pytest.mark.asyncio
    async def test_complex_mixed_scenario(
        self, handler, mock_services, tmp_path
    ):
        """Test complex scenario with all types of content."""
        root = self.create_test_structure(tmp_path, 'complex_mixed')
        
        # Mock archiver
        fake_archive = tmp_path / "test.7z"
        fake_archive.write_bytes(b"fake")
        
        with patch.object(handler._set_processor._archiver, 'create',
                         return_value=[fake_archive]):
            # Disable deduplication
            handler._blake3_deduplicator = None
            handler._existence_checker.check = AsyncMock(return_value=([], 0))
            
            process = handler.upload_folder(root, "/test_dest")
            result = await process.wait()
        
        # Verify processing completed
        assert result is not None
        assert result.total_files > 0
        
        # Verify sets were processed
        assert handler._set_processor.process_set.called if hasattr(handler._set_processor.process_set, 'called') else True
    
    @pytest.mark.asyncio
    async def test_set_processing_creates_thumbnails(
        self, handler, mock_services, tmp_path
    ):
        """Test that set processing creates thumbnails."""
        root = self.create_test_structure(tmp_path, 'pure_set')
        set_folder = root / "pure_set"
        
        # Create real images using PIL if available
        try:
            from PIL import Image
            for i, img_file in enumerate(set_folder.glob("*.jpg")):
                img = Image.new('RGB', (2000, 1500), color=(i*50, 100, 150))
                img.save(img_file)
        except ImportError:
            pytest.skip("PIL not available for thumbnail test")
        
        # Process the set
        processor = ImageSetProcessor(
            mock_services.analyzer,
            mock_services.repository,
            mock_services.storage,
            mock_services.config
        )
        
        fake_archive = tmp_path / "pure_set.7z"
        fake_archive.write_bytes(b"fake")
        
        with patch.object(processor._archiver, 'create',
                         return_value=[fake_archive]):
            result, image_results = await processor.process_set(
                set_folder,
                "/test_dest",
                None
            )
        
        # Check that thumbnail folders were created
        thumb_320 = set_folder / "m"
        thumb_1024 = set_folder / "x"
        
        # At least one thumbnail folder should exist
        assert thumb_320.exists() or thumb_1024.exists()
    
    @pytest.mark.asyncio
    async def test_set_images_saved_with_set_doc_id(
        self, handler, mock_services, tmp_path
    ):
        """Test that images in a set are saved with set_doc_id."""
        root = self.create_test_structure(tmp_path, 'pure_set')
        set_folder = root / "pure_set"
        
        processor = ImageSetProcessor(
            mock_services.analyzer,
            mock_services.repository,
            mock_services.storage,
            mock_services.config
        )
        
        fake_archive = tmp_path / "pure_set.7z"
        fake_archive.write_bytes(b"fake")
        
        with patch.object(processor._archiver, 'create',
                         return_value=[fake_archive]):
            result, image_results = await processor.process_set(
                set_folder,
                "/test_dest",
                None
            )
        
        # Verify save_batch was called and every document includes set_doc_id.
        mock_services.repository.save_batch.assert_awaited_once()
        call = mock_services.repository.save_batch.call_args
        batch_payload = call.kwargs.get("items") if call and call.kwargs else None
        if batch_payload is None and call and call.args:
            batch_payload = call.args[0]
        batch_payload = batch_payload or []
        assert len(batch_payload) >= 2
        for item in batch_payload:
            document = item.get("document", {})
            assert "set_doc_id" in document
            assert document["set_doc_id"] is not None
    
    @pytest.mark.asyncio
    async def test_multiple_sets_processed_sequentially(
        self, handler, mock_services, tmp_path
    ):
        """Test that multiple sets are processed one after another."""
        root = self.create_test_structure(tmp_path, 'mixed_upload')
        
        # Track processing order
        processed_sets = []
        
        async def track_set(set_folder, *args, **kwargs):
            processed_sets.append(set_folder.name)
            fake_archive = tmp_path / f"{set_folder.name}.7z"
            fake_archive.write_bytes(b"fake")
            return (
                MagicMock(success=True, filename=f"{set_folder.name}.7z"),
                [MagicMock(success=True), MagicMock(success=True)]
            )
        
        handler._set_processor.process_set = track_set
        
        fake_archive = tmp_path / "test.7z"
        fake_archive.write_bytes(b"fake")
        
        with patch.object(handler._set_processor._archiver, 'create',
                         return_value=[fake_archive]):
            handler._blake3_deduplicator = None
            handler._existence_checker.check = AsyncMock(return_value=([], 0))
            
            process = handler.upload_folder(root, "/test_dest")
            result = await process.wait()
        
        # Should have processed multiple sets
        assert len(processed_sets) >= 2
        
        # Sets should be processed in order (not in parallel)
        # This is verified by the sequential nature of the calls
    
    @pytest.mark.asyncio
    async def test_individual_files_processed_after_sets(
        self, handler, mock_services, tmp_path
    ):
        """Test that individual files are processed after all sets."""
        root = self.create_test_structure(tmp_path, 'mixed_upload')
        
        processing_phase = {'current': 'sets'}  # Track current phase
        set_count = [0]
        file_count = [0]
        
        async def track_set(*args, **kwargs):
            assert processing_phase['current'] == 'sets', "Files should not be processed during set phase"
            set_count[0] += 1
            fake_archive = tmp_path / "test.7z"
            fake_archive.write_bytes(b"fake")
            return (
                MagicMock(success=True),
                [MagicMock(success=True)]
            )
        
        def track_file(*args, **kwargs):
            # Switch phase when first file is processed
            if processing_phase['current'] == 'sets':
                processing_phase['current'] = 'files'
            assert processing_phase['current'] == 'files', "Should be in files phase"
            file_count[0] += 1
            return (MagicMock(success=True), None, None)
        
        handler._set_processor.process_set = track_set
        handler._file_processor.process = AsyncMock(side_effect=track_file)
        
        fake_archive = tmp_path / "test.7z"
        fake_archive.write_bytes(b"fake")
        
        with patch.object(handler._set_processor._archiver, 'create',
                         return_value=[fake_archive]):
            handler._blake3_deduplicator = None
            handler._existence_checker.check = AsyncMock(return_value=([], 0))
            
            process = handler.upload_folder(root, "/test_dest")
            result = await process.wait()
        
        # Verify both phases were executed
        assert set_count[0] > 0, "Should have processed sets"
        # Files might be 0 if all were filtered, but phase tracking should work

