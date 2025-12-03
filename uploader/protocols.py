"""
Protocols (Interfaces) for Dependency Inversion.

Following Interface Segregation Principle - small, focused interfaces.
"""
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Optional, Dict, Any, Protocol, runtime_checkable


@runtime_checkable
class IAnalyzer(Protocol):
    """Interface for media analysis."""
    
    def analyze(self, path: Path) -> Dict[str, Any]:
        """Analyze media file and return metadata."""
        ...


@runtime_checkable
class IStorageClient(Protocol):
    """Interface for cloud storage operations."""
    
    async def upload(
        self,
        path: Path,
        dest_folder: Optional[str] = None,
        name: Optional[str] = None,
        progress_callback=None,
        custom: Optional[Dict] = None
    ) -> Any:
        """Upload file to storage."""
        ...
    
    async def get(self, path: str) -> Optional[Any]:
        """Get node by path."""
        ...
    
    async def create_folder(self, name: str, parent: Optional[str] = None) -> Any:
        """Create folder."""
        ...
    
    async def get_root(self) -> Any:
        """Get root folder."""
        ...


@runtime_checkable  
class IAPIClient(Protocol):
    """Interface for API operations."""
    
    async def post(self, endpoint: str, json: Dict) -> Any:
        """POST request to API."""
        ...
    
    async def get(self, endpoint: str) -> Any:
        """GET request to API."""
        ...


class IPreviewGenerator(ABC):
    """Interface for preview generation."""
    
    @abstractmethod
    async def generate(self, path: Path, duration: float) -> Path:
        """Generate preview image."""
        pass


class IMetadataRepository(ABC):
    """Interface for metadata storage (Repository Pattern)."""
    
    @abstractmethod
    async def save_document(self, data: Dict[str, Any]) -> None:
        """Save base document."""
        pass
    
    @abstractmethod
    async def save_video_metadata(self, document_id: str, data: Dict[str, Any]) -> None:
        """Save video technical metadata."""
        pass
    
    @abstractmethod
    async def save_channel(self, data: Dict[str, Any]) -> None:
        """Save/update channel."""
        pass
    
    @abstractmethod
    async def save_social_video(self, data: Dict[str, Any]) -> None:
        """Save social video metadata."""
        pass
