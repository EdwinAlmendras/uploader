"""
Models for uploader module.

Immutable dataclasses following Single Responsibility Principle.
"""
from dataclasses import dataclass, field
from typing import Optional, Dict, Any
from pathlib import Path
from enum import Enum


class UploadStatus(Enum):
    """Upload operation status."""
    SUCCESS = "success"
    FAILED = "failed"
    PARTIAL = "partial"  # Upload ok but preview failed


@dataclass(frozen=True)
class UploadResult:
    """Immutable result of an upload operation."""
    source_id: str
    filename: str
    status: UploadStatus = UploadStatus.SUCCESS
    mega_handle: Optional[str] = None
    preview_handle: Optional[str] = None
    error: Optional[str] = None
    
    @property
    def success(self) -> bool:
        return self.status == UploadStatus.SUCCESS
    
    @classmethod
    def ok(cls, source_id: str, filename: str, mega_handle: str, preview_handle: str = None):
        return cls(
            source_id=source_id,
            filename=filename,
            status=UploadStatus.SUCCESS,
            mega_handle=mega_handle,
            preview_handle=preview_handle
        )
    
    @classmethod
    def fail(cls, filename: str, error: str):
        return cls(
            source_id="",
            filename=filename,
            status=UploadStatus.FAILED,
            error=error
        )
    
    @classmethod
    def partial(cls, source_id: str, filename: str, mega_handle: str, error: str):
        return cls(
            source_id=source_id,
            filename=filename,
            status=UploadStatus.PARTIAL,
            mega_handle=mega_handle,
            error=error
        )


@dataclass(frozen=True)
class SocialInfo:
    """Immutable social media metadata from yt-dlp."""
    platform: str
    video_id: str
    video_url: str
    title: Optional[str] = None
    description: Optional[str] = None
    # Stats
    view_count: Optional[int] = None
    like_count: Optional[int] = None
    comment_count: Optional[int] = None
    upload_date: Optional[str] = None  # YYYYMMDD
    # Channel
    channel_id: Optional[str] = None
    channel_name: Optional[str] = None
    channel_url: Optional[str] = None
    channel_username: Optional[str] = None
    channel_follower_count: Optional[int] = None
    
    @property
    def upload_date_iso(self) -> Optional[str]:
        """Convert YYYYMMDD to ISO format."""
        if self.upload_date and len(self.upload_date) == 8:
            d = self.upload_date
            return f"{d[:4]}-{d[4:6]}-{d[6:8]}T00:00:00"
        return None
    
    @property
    def has_channel(self) -> bool:
        return self.channel_id is not None


@dataclass(frozen=True)
class TelegramInfo:
    """Immutable Telegram media metadata."""
    message_id: int
    chat_id: int
    upload_date: Optional[str] = None  # ISO format or datetime
    telegram_document_id: Optional[int] = None


@dataclass(frozen=True)
class UploadConfig:
    """Immutable configuration for upload operations."""
    dest_folder: str = "/Videos"
    preview_folder: str = "/.previews"
    generate_preview: bool = True
    # Grid sizes based on duration
    grid_size_tiny: int = 3    # < 1 min
    grid_size_short: int = 4   # 1-15 min
    grid_size_long: int = 5    # > 15 min
    # Thresholds in seconds
    tiny_threshold: int = 60       # 1 minute

    long_threshold: int = 900      # 15 minutes
    
    # Flags
    skip_space_check: bool = False  # Skip storage space verification
    
    def get_grid_size(self, duration: float) -> int:
        """
        Get grid size based on video duration.
        
        - < 1 min: 3x3
        - 1-15 min: 4x4
        - > 15 min: 5x5
        """
        if duration < self.tiny_threshold:
            return self.grid_size_tiny
        if duration < self.long_threshold:
            return self.grid_size_short
        return self.grid_size_long

