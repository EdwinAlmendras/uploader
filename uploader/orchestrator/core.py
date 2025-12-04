"""Core orchestrator - coordinates all upload workflows."""
from pathlib import Path
from typing import Optional

from ..models import UploadResult, UploadConfig, SocialInfo, TelegramInfo
from ..protocols import IStorageClient
from ..services.analyzer import AnalyzerService
from ..services.repository import MetadataRepository, HTTPAPIClient
from ..services.preview import PreviewService
from ..services.storage import StorageService

from .single_upload import SingleUploadHandler
from .folder_upload import FolderUploadHandler
from .preview_handler import PreviewHandler
from .models import FolderUploadResult


class UploadOrchestrator:
    """
    Orchestrates video uploads using injected services.
    
    Follows:
    - Dependency Injection (services injected)
    - Single Responsibility (delegates to handlers)
    - Open/Closed (extend via new handlers)
    
    Usage:
        # With single MEGA client
        async with UploadOrchestrator(api_url, mega) as uploader:
            result = await uploader.upload(video_path)
        
        # With ManagedStorageService (multi-account)
        managed = await ManagedStorageService().start()
        async with UploadOrchestrator(api_url, storage_service=managed) as uploader:
            result = await uploader.upload(video_path)
    """
    
    def __init__(
        self,
        api_url: str,
        storage_client: IStorageClient = None,
        config: Optional[UploadConfig] = None,
        storage_service = None,
    ):
        """
        Initialize orchestrator with dependencies.
        
        Args:
            api_url: Datastore API URL
            storage_client: Storage client (MEGA) - deprecated, use storage_service
            config: Upload configuration
            storage_service: Pre-built storage service (StorageService or ManagedStorageService)
        """
        self._api_url = api_url
        self._storage_client = storage_client
        self._config = config or UploadConfig()
        self._external_storage = storage_service
        
        # Services (initialized in __aenter__)
        self._api_client: Optional[HTTPAPIClient] = None
        self._analyzer: Optional[AnalyzerService] = None
        self._repository: Optional[MetadataRepository] = None
        self._preview: Optional[PreviewService] = None
        self._storage = None
        
        # Handlers (initialized in __aenter__)
        self._single_handler: Optional[SingleUploadHandler] = None
        self._folder_handler: Optional[FolderUploadHandler] = None
    
    async def __aenter__(self):
        """Initialize services and handlers."""
        # API client
        self._api_client = HTTPAPIClient(self._api_url)
        await self._api_client.__aenter__()
        
        # Services with dependency injection
        self._analyzer = AnalyzerService()
        self._repository = MetadataRepository(self._api_client)
        self._preview = PreviewService(self._config)
        
        # Use external storage service if provided, otherwise create one
        if self._external_storage:
            self._storage = self._external_storage
        elif self._storage_client:
            self._storage = StorageService(self._storage_client, self._config)
        else:
            raise ValueError("Either storage_client or storage_service must be provided")
        
        # Initialize handlers
        preview_handler = PreviewHandler(self._preview, self._storage)
        self._single_handler = SingleUploadHandler(
            self._analyzer,
            self._repository,
            self._storage,
            preview_handler,
            self._config
        )
        self._folder_handler = FolderUploadHandler(
            self._analyzer,
            self._repository,
            self._storage,
            preview_handler,
            self._config
        )
        
        return self
    
    async def __aexit__(self, *args):
        """Cleanup resources."""
        if self._api_client:
            await self._api_client.__aexit__(*args)
    
    async def upload(
        self,
        path: Path,
        dest: Optional[str] = None,
        progress_callback=None
    ) -> UploadResult:
        """Upload video with metadata."""
        return await self._single_handler.upload(path, dest, progress_callback)
    
    async def upload_social(
        self,
        path: Path,
        social_info: SocialInfo,
        dest: Optional[str] = None,
        progress_callback=None
    ) -> UploadResult:
        """Upload social video with channel metadata."""
        return await self._single_handler.upload_social(path, social_info, dest, progress_callback)
    
    async def upload_photo(
        self,
        path: Path,
        dest: Optional[str] = None,
        progress_callback=None
    ) -> UploadResult:
        """Upload photo with metadata."""
        return await self._single_handler.upload_photo(path, dest, progress_callback)
    
    async def upload_auto(
        self,
        path: Path,
        dest: Optional[str] = None,
        progress_callback=None
    ) -> UploadResult:
        """Auto-detect type and upload."""
        return await self._single_handler.upload_auto(path, dest, progress_callback)
    
    async def upload_telegram(
        self,
        path: Path,
        telegram_info: Optional[TelegramInfo] = None,
        dest: Optional[str] = None,
        progress_callback=None
    ) -> UploadResult:
        """Upload media with optional Telegram metadata."""
        return await self._single_handler.upload_telegram(path, telegram_info, dest, progress_callback)
    
    async def upload_folder(
        self,
        folder_path: Path,
        dest: Optional[str] = None,
        progress_callback=None
    ) -> FolderUploadResult:
        """Upload entire folder preserving directory structure."""
        return await self._folder_handler.upload_folder(folder_path, dest, progress_callback)

