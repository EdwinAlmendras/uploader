"""
Managed Storage Service - Uses AccountManager for multi-account uploads.

Automatically selects the best account based on available space.
Creates new sessions when all accounts are full.

Requires: pip install mega-account (or from local)
"""
from pathlib import Path
from typing import Optional, Dict, Any, List
import hashlib
import os

from ..models import UploadConfig
from mega_account import AccountManager, NoSpaceError
from mega_account.api_client import AccountAPIClient
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
        auto_create: bool = True,
        collection_name: Optional[str] = None,
        api_url: Optional[str] = None
    ):
        """
        Initialize managed storage service.
        
        Args:
            sessions_dir: Directory for session files (default: ~/.config/mega/sessions/)
            config: Upload configuration
            buffer_mb: Buffer space to keep free per account (MB)
            auto_create: Prompt for new account when all are full
            collection_name: Optional collection name to load only sessions from that collection
            api_url: API URL for mega-account-api (default: http://127.0.0.1:8000)
        """
        self._config = config or UploadConfig()
        self._collection_name = collection_name
        self._api_url = api_url or os.getenv("MEGA_ACCOUNT_API_URL", "http://127.0.0.1:8000")
        self._started = False
        if not sessions_dir:
            sessions_dir = os.getenv("MEGA_SESSIONS_DIR")
            if not sessions_dir:
                sessions_dir = Path.home() / ".config" / "mega" / "sessions"
                if not sessions_dir.exists():
                    sessions_dir.mkdir(parents=True, exist_ok=True)
        else:
            if not sessions_dir.exists():
                sessions_dir.mkdir(parents=True, exist_ok=True)
        
        self._sessions_dir = Path(sessions_dir)
        self._buffer_mb = buffer_mb
        self._auto_create = auto_create
        self._manager = None  # Will be initialized in start() if collection_name is provided
    
    @property
    def manager(self) -> AccountManager:
        """Get the underlying AccountManager."""
        if self._manager is None:
            raise RuntimeError("Manager not initialized. Call start() first.")
        return self._manager
    
    async def start(self) -> 'ManagedStorageService':
        """Start the service and load accounts."""
        # If collection_name is provided, load only sessions from that collection
        if self._collection_name:
            await self._load_manager_from_collection()
        else:
            # Default behavior: load all sessions from directory
            if self._manager is None:
                self._manager = AccountManager(
                    sessions_dir=self._sessions_dir,
                    buffer_mb=self._buffer_mb,
                    auto_create=self._auto_create
                )
            await self._manager.load_accounts()
        
        self._started = True
        return self
    
    async def _load_manager_from_collection(self) -> None:
        """
        Load AccountManager with only sessions from the specified collection.
        
        This method:
        1. Calls mega-account-api to get emails from the collection
        2. Builds session paths from emails (md5(email).session) in sessions_dir
        3. Creates AccountManager.from_session_paths() with those specific paths
        """
        try:
            logger.info(f"Loading collection: {self._collection_name}")
            
            # Get emails from collection via API
            async with AccountAPIClient(api_url=self._api_url) as api:
                emails = await api.get_collection_emails(collection_name=self._collection_name)
            
            if not emails:
                logger.warning(f"No emails found in collection: {self._collection_name}")
                raise ValueError(f"No emails found in collection: {self._collection_name}")
            
            logger.info(f"Found {len(emails)} account(s) in collection: {self._collection_name}")
            
            # Build session paths from emails (md5(email).session)
            session_paths: List[Path] = []
            for email in emails:
                email_hash = hashlib.md5(email.lower().encode()).hexdigest()
                session_path = self._sessions_dir / f"{email_hash}.session"
                if session_path.exists():
                    session_paths.append(session_path)
                    logger.debug(f"Found session for {email}: {session_path}")
                else:
                    logger.warning(f"Session not found for {email}: {session_path}")
            
            if not session_paths:
                logger.warning(f"No session files found for collection: {self._collection_name}")
                # Fall back to loading all sessions
                self._manager = AccountManager(
                    sessions_dir=self._sessions_dir,
                    buffer_mb=self._buffer_mb,
                    auto_create=self._auto_create
                )
                await self._manager.load_accounts()
                return
            
            logger.info(f"Loading {len(session_paths)} session(s) from collection")
            
            # Create AccountManager with only these session paths
            self._manager = AccountManager.from_session_paths(
                session_paths=session_paths,
                buffer_mb=self._buffer_mb,
                auto_create=self._auto_create
            )
            
            # Load the accounts
            await self._manager.load_accounts()
            
            loaded_count = len(self._manager.accounts)
            logger.info(f"Successfully loaded {loaded_count} session(s) from collection")
            
        except Exception as e:
            logger.error(f"Failed to load collection {self._collection_name}: {e}")
            logger.info("Falling back to loading all sessions from directory")
            # Fall back to loading all sessions
            self._manager = AccountManager(
                sessions_dir=self._sessions_dir,
                buffer_mb=self._buffer_mb,
                auto_create=self._auto_create
            )
            await self._manager.load_accounts()
    
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
        if not self._started or self._manager is None:
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
                new_account = await manager.create_new_session()
                return new_account
        else:
            logger.info("No accounts found. Creating a new account...")
            new_account = await manager.create_new_session()
            return new_account
    
    async def check_available_for_size(self, file_size: int) -> bool:
        """
        Check if there's enough space available for a file of the given size.
        
        This method checks if a SINGLE account has enough space for the file.
        Use check_total_available_space() for folders that can be split across accounts.
        
        If no account has enough space, it will attempt to create a new account
        (if auto_create is enabled).
        
        Args:
            file_size: Size of the file in bytes
            
        Returns:
            True if space is available (or a new account was created), False otherwise
        """
        if not self._started:
            await self.start()
        
        try:
            # Try to get a client for this file size
            # This will automatically create a new account if needed and auto_create is True
            client = await self._manager.get_client_for(file_size, prompt_new=True)
            if client:
                # Space is available
                return True
            return False
        except NoSpaceError as e:
            logger.warning(f"No space available for {file_size / (1024**3):.2f} GB file: {e}")
            return False
        except Exception as e:
            logger.error(f"Error checking space availability: {e}")
            return False
    
    async def check_total_available_space(self, total_size: int) -> bool:
        """
        Check if there's enough TOTAL space available across ALL accounts for a folder.
        
        This method sums the free space from all active accounts, which is appropriate
        for folders that can be split across multiple accounts.
        
        Args:
            total_size: Total size in bytes (e.g., sum of all files in a folder)
            
        Returns:
            True if total available space >= total_size, False otherwise
        """
        if not self._started:
            await self.start()
        
        try:
            manager = self._manager
            active_accounts = manager.active_accounts
            
            if not active_accounts:
                # No accounts available, try to create one
                if manager.auto_create:
                    logger.info("No accounts found. Attempting to create a new account...")
                    new_account = await manager.create_new_session()
                    if new_account:
                        active_accounts = manager.active_accounts
            
            if not active_accounts:
                logger.warning("No accounts available")
                return False
            
            # Sum free space from all active accounts
            total_free_space = sum(account.space_free for account in active_accounts)
            
            logger.debug(
                f"Total free space: {total_free_space / (1024**3):.2f} GB, "
                f"Required: {total_size / (1024**3):.2f} GB"
            )
            
            if total_free_space >= total_size:
                return True
            
            # Not enough space, try to create a new account if auto_create is enabled
            if manager.auto_create:
                logger.info(
                    f"Insufficient total space ({total_free_space / (1024**3):.2f} GB). "
                    f"Attempting to create a new account..."
                )
                new_account = await manager.create_new_session(prompt_new=True)
                if new_account:
                    # Recalculate with new account
                    active_accounts = manager.active_accounts
                    total_free_space = sum(account.space_free for account in active_accounts)
                    return total_free_space >= total_size
            
            return False
            
        except Exception as e:
            logger.error(f"Error checking total space availability: {e}")
            return False
    
    
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
    
    async def exists_by_mega_id(self, mega_id: str) -> bool:
        """
        Check if file exists in MEGA by mega_id (attribute 'm') across ALL accounts.
        
        Uses AccountManager to search in all accounts.
        
        Args:
            mega_id: Source ID (mega_id stored as 'm' attribute)
            
        Returns:
            True if exists in any account, False otherwise
        """
        try:
            result = await self._manager.find_by_mega_id(mega_id)
            return result is not None
        except Exception as e:
            logger.debug(f"Error searching for mega_id {mega_id}: {e}")
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
        source_id: Optional[str] = None,
        dest_path: Optional[str] = None,
        filename: Optional[str] = None
    ) -> Optional[str]:
        """
        Upload preview to the same path as the video, or to .previews folder.
        
        Args:
            path: Path to preview image
            source_id: Source ID for naming (backward compatibility, used if dest_path/filename not provided)
            dest_path: Destination folder path (same as video)
            filename: Preview filename (e.g., "VIDEO.jpg")
            
        Returns:
            Handle of uploaded preview or None on failure
        """
        try:
            path = Path(path)
            file_size = path.stat().st_size
            
            # Get client with enough space
            client = await self._manager.get_client_for(file_size)
            current_account = self._manager._current_account or "default"
            
            if dest_path and filename:
                # New behavior: upload to same path as video
                # Get or create destination folder (same as video)
                folder_handle = await self._get_or_create_folder(client, dest_path, current_account)
                
                if not folder_handle:
                    logger.error(f"[storage] Failed to get/create folder: {dest_path}")
                    return None
                
                # Upload with the specified filename
                node = await client.upload(
                    path,
                    dest_folder=folder_handle,
                    name=filename
                )
                
                logger.info(f"[storage] Uploaded preview to {dest_path}/{filename}")
                return node.handle if node else None
            else:
                # Backward compatibility: upload to .previews folder
                if not source_id:
                    logger.error("[storage] source_id required for backward compatibility")
                    return None
                
                # Get or create .previews folder
                folder_handle = await self._ensure_preview_folder(client, current_account)
                
                if not folder_handle:
                    logger.error("[storage] Failed to get/create .previews folder")
                    return None
                
                # Upload with source_id as name
                preview_name = f"{source_id}.jpg"
                node = await client.upload(
                    path,
                    dest_folder=folder_handle,
                    name=preview_name
                )
                
                logger.info(f"[storage] Uploaded preview to /.previews/{preview_name}")
                return node.handle if node else None
            
        except Exception as e:
            logger.error(f"[storage] Upload preview error: {e}")
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
