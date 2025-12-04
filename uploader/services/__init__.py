"""Services for uploader module."""
from .analyzer import AnalyzerService
from .repository import MetadataRepository
from .preview import PreviewService
from .storage import StorageService

# Optional: requires mega-account package
try:
    from .managed_storage import ManagedStorageService
    __all__ = [
        "AnalyzerService",
        "MetadataRepository", 
        "PreviewService",
        "StorageService",
        "ManagedStorageService",
    ]
except ImportError:
    __all__ = [
        "AnalyzerService",
        "MetadataRepository", 
        "PreviewService",
        "StorageService",
    ]
