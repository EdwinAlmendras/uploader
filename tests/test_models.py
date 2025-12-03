"""Tests for uploader models."""
import pytest
from uploader.models import (
    UploadResult, 
    UploadStatus, 
    SocialInfo, 
    UploadConfig
)


class TestUploadResult:
    def test_ok_result(self):
        result = UploadResult.ok(
            source_id="abc123",
            filename="video.mp4",
            mega_handle="handle1",
            preview_handle="preview1"
        )
        assert result.success is True
        assert result.status == UploadStatus.SUCCESS
        assert result.source_id == "abc123"
        assert result.mega_handle == "handle1"
    
    def test_fail_result(self):
        result = UploadResult.fail(
            filename="video.mp4",
            error="Upload failed"
        )
        assert result.success is False
        assert result.status == UploadStatus.FAILED
        assert result.error == "Upload failed"
        assert result.source_id == ""
    
    def test_partial_result(self):
        result = UploadResult.partial(
            source_id="abc123",
            filename="video.mp4",
            mega_handle="handle1",
            error="Preview failed"
        )
        assert result.success is False
        assert result.status == UploadStatus.PARTIAL
        assert result.mega_handle == "handle1"
        assert result.error == "Preview failed"
    
    def test_immutable(self):
        result = UploadResult.ok("abc", "file.mp4", "h1")
        with pytest.raises(Exception):
            result.source_id = "xyz"


class TestSocialInfo:
    def test_create_social_info(self):
        info = SocialInfo(
            platform="youtube",
            video_id="abc123",
            video_url="https://youtube.com/watch?v=abc123"
        )
        assert info.platform == "youtube"
        assert info.video_id == "abc123"
    
    def test_upload_date_iso(self):
        info = SocialInfo(
            platform="youtube",
            video_id="abc",
            video_url="url",
            upload_date="20241015"
        )
        assert info.upload_date_iso == "2024-10-15T00:00:00"
    
    def test_upload_date_iso_none(self):
        info = SocialInfo(
            platform="youtube",
            video_id="abc",
            video_url="url"
        )
        assert info.upload_date_iso is None
    
    def test_has_channel(self):
        info_with = SocialInfo(
            platform="youtube",
            video_id="abc",
            video_url="url",
            channel_id="UCxxx"
        )
        info_without = SocialInfo(
            platform="youtube",
            video_id="abc",
            video_url="url"
        )
        assert info_with.has_channel is True
        assert info_without.has_channel is False


class TestUploadConfig:
    def test_default_config(self):
        config = UploadConfig()
        assert config.dest_folder == "/Videos"
        assert config.preview_folder == "/.previews"
        assert config.generate_preview is True
    
    def test_grid_size_tiny_video(self):
        """< 1 min = 3x3"""
        config = UploadConfig()
        assert config.get_grid_size(30) == 3   # 30 sec
        assert config.get_grid_size(59) == 3   # 59 sec
    
    def test_grid_size_short_video(self):
        """1-15 min = 4x4"""
        config = UploadConfig()
        assert config.get_grid_size(60) == 4   # 1 min
        assert config.get_grid_size(600) == 4  # 10 min
        assert config.get_grid_size(899) == 4  # 14:59
    
    def test_grid_size_long_video(self):
        """> 15 min = 5x5"""
        config = UploadConfig()
        assert config.get_grid_size(900) == 5  # 15 min
        assert config.get_grid_size(1500) == 5 # 25 min
        assert config.get_grid_size(3600) == 5 # 1 hour
