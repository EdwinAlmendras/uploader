"""
Managed Storage Service - Uses AccountManager for multi-account uploads.

Automatically selects the best account based on available space.
Creates new sessions when all accounts are full.

Requires: pip install mega-account (or from local)
"""
from pathlib import Path
from typing import Optional, Dict, Any

from ..models import UploadConfig
from mega_account import AccountManager, NoSpaceError
import logging

logger = logging.getLogger("uploader.services.managed_storage")


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
        if not sessions_dir:
            sessions_dir = Path.home() / ".config" / "mega" / "sessions"
        if not sessions_dir.exists():
            sessions_dir.mkdir(parents=True, exist_ok=True)
            
        self._manager = AccountManager(
            sessions_dir=sessions_dir,
            buffer_mb=buffer_mb,
            auto_create=auto_create
        )
        self._folder_cache: Dict[str, Dict[str, str]] = {}  # account_name -> {path -> handle} cache
        self._last_account: Optional[str] = None
        self._started = False
        self._locked_account: Optional[str] = None  # Account locked for folder upload
        self._locked_client = None  # Client for locked account
    
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
    
    async def lock_account_for_size(self, total_size: int) -> bool:
        """
        Lock an account for folder upload to ensure all files go to same account.
        
        Args:
            total_size: Total size of all files in folder (bytes)
            
        Returns:
            True if account locked successfully, False if no account has enough space
        """
        try:
            # Get client with enough space for entire folder
            client = await self._manager.get_client_for(total_size)
            self._locked_account = self._manager._current_account
            self._locked_client = client
            return True
        except Exception:
            return False
    
    def unlock_account(self):
        """Unlock account after folder upload completes."""
        self._locked_account = None
        self._locked_client = None
        
    
    async def check_accounts_space(self):
        if not self._started:
            await self.start()
            
        MIN_FREE_SPACE_GB = 1
        MIN_FREE_SPACE_BYTES = MIN_FREE_SPACE_GB * 1024 * 1024 * 1024
        
        manager = self._manager
        active_accounts = manager.active_accounts
        
        if active_accounts:
            all_accounts_low_space = all(
                account.space_free < MIN_FREE_SPACE_BYTES 
                for account in active_accounts
            )
            if all_accounts_low_space:
                logger.info(f"All accounts have less than {MIN_FREE_SPACE_GB}GB free space. Creating a new account...")
                new_account = await manager()
                return new_account
        else:
            logger.info("No accounts found. Creating a new account...")
            new_account = await manager.create_new_session()
            return new_account
    
    
    async def upload_video(
        self,
        path: Path,
        dest: Optional[str] = None,
        source_id: Optional[str] = None,
        progress_callback=None
    ) -> Optional[str]:
        """
        Upload video/file to storage.
        
        Automatically selects account with enough space, or uses locked account.
        
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
        
        # Use locked account if available, otherwise select automatically
        if self._locked_client and self._locked_account:
            client = self._locked_client
            current_account = self._locked_account
        else:
            # Get client with enough space
            client = await self._manager.get_client_for(file_size)
            current_account = self._manager._current_account
        
        # Track account changes and clear cache if needed
        if current_account != self._last_account:
            # Account changed - cache is still valid per account, but we track it
            self._last_account = current_account
        
        # Get or create destination folder (uses account-specific cache)
        dest_handle = await self._get_or_create_folder(client, dest, current_account)
        
        # Upload with mega_id (flat 'm' attribute)
        node = await client.upload(
            path,
            dest_folder=dest_handle,
            progress_callback=progress_callback,
            mega_id=source_id
        )
        
        # Update space tracking in manager
        if node and current_account:
            account = self._manager._accounts.get(current_account)
            if account:
                account.space_used += file_size
                account.space_free -= file_size
        
        return node.handle if node else None
    
    async def exists(self, path: str) -> bool:
        """
        Check if file/folder exists in MEGA across ALL accounts.
        
        Args:
            path: Full path to check (e.g., "/Folder/file.mp4")
            
        Returns:
            True if exists in any account, False otherwise
        """
        try:
            # Normalize path - ensure it starts with /
            if not path.startswith("/"):
                path = f"/{path}"
            # Use AccountManager.exists() which searches all accounts
            exists = await self._manager.exists(path)
            if exists:
                logger.debug(f"File exists in MEGA: {path}")
            return exists
        except Exception as e:
            logger.debug(f"Error checking if file exists {path}: {e}")
            return False
    
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
        current_account = self._manager._current_account or "default"
        return await self._get_or_create_folder(client, path, current_account)
    
    async def _get_or_create_folder(self, client, path: str, account_name: Optional[str] = None) -> Optional[str]:
        """
        Get or create folder, creating parents as needed. Uses account-specific cache.
        
        Args:
            client: MegaClient instance
            path: Folder path
            account_name: Account name for cache (required for multi-account)
        """
        if not path:
            root = await client.get_root()
            return root.handle if root else None
        
        # Normalize path
        path = path.strip("/")
        
        # Get account-specific cache
        if not account_name:
            account_name = self._manager._current_account or "default"
        
        if account_name not in self._folder_cache:
            self._folder_cache[account_name] = {}
        
        account_cache = self._folder_cache[account_name]
        
        # Check cache first
        if path in account_cache:
            return account_cache[path]
        
        # Check if exists in MEGA
        node = await client.get(f"/{path}")
        if node:
            account_cache[path] = node.handle
            return node.handle
        
        # Create path recursively
        parts = path.split("/")
        current_handle = None
        current_path = ""
        
        for part in parts:
            current_path = f"{current_path}/{part}" if current_path else part
            
            # Check cache for intermediate paths
            if current_path in account_cache:
                current_handle = account_cache[current_path]
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
            account_cache[current_path] = current_handle
        
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
            current_account = self._manager._current_account or "default"
            
            # Get or create .previews folder
            folder_handle = await self._ensure_preview_folder(client, current_account)
            
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
    
    async def _ensure_preview_folder(self, client, account_name: Optional[str] = None) -> Optional[str]:
        """
        Ensure /.previews/ folder exists in MEGA root.
        
        Uses account-specific cache for preview folder handle.
        
        Args:
            client: MegaClient instance
            account_name: Account name for cache (required for multi-account)
        
        Returns:
            Handle of preview folder
        """
        if not account_name:
            account_name = self._manager._current_account or "default"
        
        # Use account-specific cache key
        cache_key = f"_preview_{account_name}"
        
        # Check if we have cached handle for this account
        if hasattr(self, cache_key):
            return getattr(self, cache_key)
        
        try:
            # Try to get existing folder
            folder = await client.get("/.previews")
            
            if folder:
                handle = folder.handle
            else:
                # Create folder in root
                root = await client.get_root()
                if root:
                    folder = await client.create_folder(".previews", root.handle)
                    handle = folder.handle
                    print(f"[storage] Created /.previews folder in account {account_name}")
                else:
                    return None
            
            # Cache handle for this account
            setattr(self, cache_key, handle)
            return handle
        except Exception as e:
            print(f"[storage] Error creating .previews folder: {e}")
            return None
    
    def get_status(self) -> str:
        """Get status string for all accounts."""
        return str(self._manager)
