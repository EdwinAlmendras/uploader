"""
Storage Service - Single Responsibility: upload files to cloud storage.

Wraps MEGA client with additional logic for previews.
"""
from pathlib import Path
from typing import Optional, Dict, Any

from ..models import UploadConfig
from megapy import MegaClient
import logging
logger = logging.getLogger(__name__)


class StorageService:
    """
    Service for uploading files to cloud storage (MEGA).
    
    Handles both video uploads and preview uploads.
    """
    
    def __init__(
        self,
        client: MegaClient,
        config: Optional[UploadConfig] = None
    ):
        """
        Initialize storage service.
        
        Args:
            client: Storage client (MEGA)
            config: Upload configuration
        """
        self._client = client
        self._config = config or UploadConfig()
        self._preview_folder_handle: Optional[str] = None
        self._folder_cache: Dict[str, str] = {}  # path -> handle cache
    
    async def upload_video(
        self,
        path: Path,
        dest: Optional[str] = None,
        source_id: Optional[str] = None,
        progress_callback=None
    ) -> Optional[str]:
        """
        Upload video/file to storage.
        
        Args:
            path: Path to file
            dest: Destination folder path
            source_id: Source ID (stored as 'm' attribute)
            progress_callback: Optional progress callback
            
        Returns:
            Handle of uploaded file or None on failure
        """
        dest = dest or self._config.dest_folder
        
        # Get or create destination folder
        dest_handle = await self._get_or_create_folder(dest)
        
        # Upload with mega_id (flat 'm' attribute)
        node = await self._client.upload(
            path,
            dest_folder=dest_handle,
            progress_callback=progress_callback,
            mega_id=source_id
        )
        
        return node.handle if node else None
    
    async def get_node(self, path: str):
        """
        Get node from MEGA by path.
        
        Args:
            path: Full path to check (e.g., "/Folder/file.mp4")
            
        Returns:
            Node if exists, None otherwise
        """
        try:
            path = path if path.startswith("/") else f"/{path}"
            return await self._client.get(path)
        except Exception:
            return None
    
    async def exists(self, path: str) -> bool:
        """
        Check if file/folder exists in MEGA.
        
        Args:
            path: Full path to check (e.g., "/Folder/file.mp4")
            
        Returns:
            True if exists, False otherwise
        """
        node = await self.get_node(path)
            return node is not None
    
    async def exists_by_mega_id(self, mega_id: str) -> bool:
        """
        Check if file exists in MEGA by mega_id (attribute 'm').
        
        Searches through all files to find one with mega_id.
        
        Args:
            mega_id: Source ID (mega_id stored as 'm' attribute)
            
        Returns:
            True if exists, False otherwise
        """
        try:
            # Ensure nodes are loaded
            if self._client._node_service is None:
                await self._client._load_nodes()
            
            # Search recursively through all files
            def search_nodes(node):
                """Recursively search for node with mega_id."""
                if not node:
                    return False
                
                # Check if this node has the mega_id
                if node.attributes and node.attributes.mega_id == mega_id:
                    return True
                
                # If it's a folder, search children
                if node.is_folder:
                    for child in node.children:
                        if search_nodes(child):
                            return True
                
                return False
            
            # Start from root
            root = await self._client.get_root()
            return search_nodes(root)
        except Exception as e:
            logger.debug(f"Error searching for mega_id {mega_id}: {e}")
            return False
    
    async def create_folder(self, path: str) -> Optional[str]:
        """
        Create folder path in MEGA (creates parents if needed).
        
        Args:
            path: Folder path (e.g., "Sets/my_folder/subfolder")
            
        Returns:
            Handle of created/existing folder
        """
        return await self._get_or_create_folder(path)
    
    async def _get_or_create_folder(self, path: str) -> Optional[str]:
        """Get or create folder, creating parents as needed. Uses cache."""
        
        
        if not path:
            root = await self._client.get_root()
            return root.handle if root else None
        
        # Normalize path
        path = path.strip("/")
        
        # Check cache first
        if path in self._folder_cache:
            logger.debug(f"Folder found in cache: /{path}")
            return self._folder_cache[path]
        
        # Check if exists in MEGA
        logger.debug(f"Checking if folder exists in MEGA: /{path}")
        node = await self._client.get(f"/{path}")
        if node:
            logger.debug(f"Folder already exists in MEGA: /{path} (handle: {node.handle})")
            self._folder_cache[path] = node.handle
            return node.handle
        
        # Create path recursively
        logger.info(f"Creating folder structure: /{path}")
        parts = path.split("/")
        current_handle = None
        current_path = ""
        
        for part in parts:
            current_path = f"{current_path}/{part}" if current_path else part
            
            # Check cache for intermediate paths
            if current_path in self._folder_cache:
                logger.debug(f"Intermediate path found in cache: /{current_path}")
                current_handle = self._folder_cache[current_path]
                continue
            
            logger.debug(f"Checking intermediate path: /{current_path}")
            node = await self._client.get(f"/{current_path}")
            
            if node:
                logger.debug(f"Intermediate folder exists: /{current_path} (handle: {node.handle})")
                current_handle = node.handle
            else:
                # Create folder
                if current_handle is None:
                    root = await self._client.get_root()
                    current_handle = root.handle
                
                logger.info(f"Creating folder: {part} in parent (handle: {current_handle})")
                new_folder = await self._client.create_folder(part, current_handle)
                if new_folder:
                    current_handle = new_folder.handle
                    logger.info(f"Folder created successfully: {part} (handle: {current_handle})")
                else:
                    logger.error(f"Failed to create folder: {part}")
                    return None
            
            # Cache intermediate path
            self._folder_cache[current_path] = current_handle
        
        logger.info(f"Folder structure created/verified: /{path} (handle: {current_handle})")
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
            if dest_path and filename:
                # New behavior: upload to same path as video
                # Get or create destination folder (same as video)
                folder_handle = await self._get_or_create_folder(dest_path)
                
                if not folder_handle:
                    logger.error(f"[storage] Failed to get/create folder: {dest_path}")
                    return None
                
                # Upload with the specified filename
                node = await self._client.upload(
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
                folder_handle = await self._ensure_preview_folder()
                
                if not folder_handle:
                    logger.error("[storage] Failed to get/create .previews folder")
                    return None
                
                # Upload with source_id as name
                preview_name = f"{source_id}.jpg"
                node = await self._client.upload(
                    path,
                    dest_folder=folder_handle,
                    name=preview_name
                )
                
                logger.info(f"[storage] Uploaded preview to /.previews/{preview_name}")
                return node.handle if node else None
            
        except Exception as e:
            logger.error(f"[storage] Upload preview error: {e}")
            return None
    
    async def _ensure_preview_folder(self) -> Optional[str]:
        """
        Ensure /.previews/ folder exists in MEGA root.
        
        Returns:
            Handle of preview folder
        """
        if self._preview_folder_handle:
            return self._preview_folder_handle
        
        try:
            # Try to get existing folder
            folder = await self._client.get("/.previews")
            
            if folder:
                self._preview_folder_handle = folder.handle
            else:
                # Create folder in root using create_folder
                root = await self._client.get_root()
                if root:
                    folder = await self._client.create_folder(".previews", root.handle)
                    self._preview_folder_handle = folder.handle
                    print(f"[storage] Created /.previews folder")
            
            return self._preview_folder_handle
        except Exception as e:
            print(f"[storage] Error creating .previews folder: {e}")
            return None
