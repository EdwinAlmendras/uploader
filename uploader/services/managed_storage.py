"""
Managed Storage Service - Uses AccountManager for multi-account uploads.

Automatically selects the best account based on available space.
Creates new sessions when all accounts are full.

Requires: pip install mega-account (or from local)
"""
from pathlib import Path
from typing import Optional, Dict, Any

from ..models import UploadConfig

try:
    from mega_account import AccountManager, NoSpaceError
except ImportError:
    raise ImportError(
        "mega-account is required for ManagedStorageService. "
        "Install it with: pip install mega-account"
    )


class ManagedStorageService:
    """
    Storage service with automatic multi-account management.
    
    Uses AccountManager to:
    - Auto-select best account for each upload
    - Track space usage
    - Create new sessions when needed
    
    Usage:
        >>> service = ManagedStorageService()
        >>> await service.start()
        >>> handle = await service.upload_video(path, dest, source_id)
        >>> await service.close()
    """
    
    def __init__(
        self,
        sessions_dir: Optional[Path] = None,
        config: Optional[UploadConfig] = None,
        buffer_mb: int = 100,
        auto_create: bool = True
    ):
        """
        Initialize managed storage service.
        
        Args:
            sessions_dir: Directory for session files (default: ~/.config/mega/sessions/)
            config: Upload configuration
            buffer_mb: Buffer space to keep free per account (MB)
            auto_create: Prompt for new account when all are full
        """
        self._config = config or UploadConfig()
        self._manager = AccountManager(
            sessions_dir=sessions_dir,
            buffer_mb=buffer_mb,
            auto_create=auto_create
        )
        self._preview_folder_handle: Optional[str] = None
        self._folder_cache: Dict[str, str] = {}  # path -> handle cache
        self._started = False
    
    @property
    def manager(self) -> AccountManager:
        """Get the underlying AccountManager."""
        return self._manager
    
    async def start(self) -> 'ManagedStorageService':
        """Start the service and load accounts."""
        await self._manager.load_accounts()
        self._started = True
        return self
    
    async def close(self) -> None:
        """Close all connections."""
        await self._manager.close()
        self._started = False
    
    async def __aenter__(self) -> 'ManagedStorageService':
        return await self.start()
    
    async def __aexit__(self, *args) -> None:
        await self.close()
    
    async def upload_video(
        self,
        path: Path,
        dest: Optional[str] = None,
        source_id: Optional[str] = None,
        progress_callback=None
    ) -> Optional[str]:
        """
        Upload video/file to storage.
        
        Automatically selects account with enough space.
        
        Args:
            path: Path to file
            dest: Destination folder path
            source_id: Source ID (stored as 'm' attribute)
            progress_callback: Optional progress callback
            
        Returns:
            Handle of uploaded file or None on failure
        """
        path = Path(path)
        file_size = path.stat().st_size
        dest = dest or self._config.dest_folder
        
        # Get client with enough space
        client = await self._manager.get_client_for(file_size)
        
        # Get or create destination folder
        dest_handle = await self._get_or_create_folder(client, dest)
        
        # Upload with mega_id (flat 'm' attribute)
        node = await client.upload(
            path,
            dest_folder=dest_handle,
            progress_callback=progress_callback,
            mega_id=source_id
        )
        
        # Update space tracking in manager
        if node and self._manager._current_account:
            account = self._manager._accounts.get(self._manager._current_account)
            if account:
                account.space_used += file_size
                account.space_free -= file_size
        
        return node.handle if node else None
    
    async def create_folder(self, path: str) -> Optional[str]:
        """
        Create folder path in MEGA (creates parents if needed).
        
        Uses first available account.
        
        Args:
            path: Folder path (e.g., "Sets/my_folder/subfolder")
            
        Returns:
            Handle of created/existing folder
        """
        # Use any available client
        client = await self._manager.get_client_for(0)
        return await self._get_or_create_folder(client, path)
    
    async def _get_or_create_folder(self, client, path: str) -> Optional[str]:
        """Get or create folder, creating parents as needed. Uses cache."""
        if not path:
            root = await client.get_root()
            return root.handle if root else None
        
        # Normalize path
        path = path.strip("/")
        
        # Check cache first
        if path in self._folder_cache:
            return self._folder_cache[path]
        
        # Check if exists in MEGA
        node = await client.get(f"/{path}")
        if node:
            self._folder_cache[path] = node.handle
            return node.handle
        
        # Create path recursively
        parts = path.split("/")
        current_handle = None
        current_path = ""
        
        for part in parts:
            current_path = f"{current_path}/{part}" if current_path else part
            
            # Check cache for intermediate paths
            if current_path in self._folder_cache:
                current_handle = self._folder_cache[current_path]
                continue
            
            node = await client.get(f"/{current_path}")
            
            if node:
                current_handle = node.handle
            else:
                # Create folder
                if current_handle is None:
                    root = await client.get_root()
                    current_handle = root.handle
                
                new_folder = await client.create_folder(part, current_handle)
                current_handle = new_folder.handle
            
            # Cache intermediate path
            self._folder_cache[current_path] = current_handle
        
        return current_handle
    
    async def upload_preview(
        self,
        path: Path,
        source_id: str
    ) -> Optional[str]:
        """
        Upload preview to /.previews/{source_id}.jpg
        
        Args:
            path: Path to preview image
            source_id: Source ID for naming
            
        Returns:
            Handle of uploaded preview or None on failure
        """
        try:
            path = Path(path)
            file_size = path.stat().st_size
            
            # Get client with enough space
            client = await self._manager.get_client_for(file_size)
            
            # Get or create .previews folder
            folder_handle = await self._ensure_preview_folder(client)
            
            if not folder_handle:
                print(f"[storage] Failed to get/create .previews folder")
                return None
            
            # Upload with source_id as name
            preview_name = f"{source_id}.jpg"
            node = await client.upload(
                path,
                dest_folder=folder_handle,
                name=preview_name
            )
            
            return node.handle if node else None
            
        except Exception as e:
            print(f"[storage] Upload preview error: {e}")
            return None
    
    async def _ensure_preview_folder(self, client) -> Optional[str]:
        """
        Ensure /.previews/ folder exists in MEGA root.
        
        Returns:
            Handle of preview folder
        """
        if self._preview_folder_handle:
            return self._preview_folder_handle
        
        try:
            # Try to get existing folder
            folder = await client.get("/.previews")
            
            if folder:
                self._preview_folder_handle = folder.handle
            else:
                # Create folder in root
                root = await client.get_root()
                if root:
                    folder = await client.create_folder(".previews", root.handle)
                    self._preview_folder_handle = folder.handle
                    print(f"[storage] Created /.previews folder")
            
            return self._preview_folder_handle
        except Exception as e:
            print(f"[storage] Error creating .previews folder: {e}")
            return None
    
    def get_status(self) -> str:
        """Get status string for all accounts."""
        return str(self._manager)
