
"""Services for uploader module."""
from .analyzer import AnalyzerService
from .repository import MetadataRepository
from .preview import PreviewService
from .storage import StorageService
from .hash_cache import HashCache, get_hash_cache

# Optional: requires mega-account package
try:
    from .managed_storage import ManagedStorageService
    __all__ = [
        "AnalyzerService",
        "MetadataRepository", 
        "PreviewService",
        "StorageService",
        "ManagedStorageService",
        "HashCache",
        "get_hash_cache",
    ]
except ImportError:
    __all__ = [
        "AnalyzerService",
        "MetadataRepository", 
        "PreviewService",
        "StorageService",
        "HashCache",
        "get_hash_cache",
    ]
