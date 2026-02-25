"""
Uploader - Centralized upload orchestration for social and kmp.

Follows SOLID principles:
- Single Responsibility: Each service handles one concern
- Open/Closed: Extend via new services
- Liskov Substitution: Services implement protocols
- Interface Segregation: Small focused interfaces
- Dependency Injection: Services injected into orchestrator

Usage:
    from uploader import UploadOrchestrator, SocialInfo, UploadConfig
    
    # Basic video upload
    async with UploadOrchestrator(api_url, mega) as uploader:
        result = await uploader.upload(video_path)
    
    # Photo upload
    result = await uploader.upload_photo(photo_path)
    
    # Auto-detect (video or photo)
    result = await uploader.upload_auto(media_path)
    
    # Social upload (with channel/social metadata)
    social_info = SocialInfo(
        platform="youtube",
        video_id="abc123",
        video_url="https://youtube.com/watch?v=abc123",
        channel_id="UCxxx",
        channel_name="Channel Name"
    )
    result = await uploader.upload_social(video_path, social_info)
"""
from .orchestrator import UploadOrchestrator, FolderUploadResult
from .models import UploadResult, UploadStatus, SocialInfo, UploadConfig, TelegramInfo
from .services import (
    AnalyzerService,
    MetadataRepository,
    PreviewService,
    StorageService,
)

# Optional: requires mega-account package
try:
    from .services import ManagedStorageService
    _has_managed = True
except ImportError:
    _has_managed = False

__version__ = "0.2.5"
__all__ = [
    # Main
    "UploadOrchestrator",
    "FolderUploadResult",
    # Models
    "UploadResult",
    "UploadStatus",
    "SocialInfo",
    "UploadConfig",
    "TelegramInfo",
    # Services
    "AnalyzerService",
    "MetadataRepository",
    "PreviewService",
    "StorageService",
]

if _has_managed:
    __all__.append("ManagedStorageService")
