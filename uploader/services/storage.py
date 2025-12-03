"""
Storage Service - Single Responsibility: upload files to cloud storage.

Wraps MEGA client with additional logic for previews.
"""
from pathlib import Path
from typing import Optional, Dict, Any

from ..protocols import IStorageClient
from ..models import UploadConfig


class StorageService:
    """
    Service for uploading files to cloud storage (MEGA).
    
    Handles both video uploads and preview uploads.
    """
    
    def __init__(
        self,
        client: IStorageClient,
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
        """Get or create folder, creating parents as needed."""
        if not path:
            root = await self._client.get_root()
            return root.handle if root else None
        
        # Normalize path
        path = path.strip("/")
        
        # Check if exists
        node = await self._client.get(f"/{path}")
        if node:
            return node.handle
        
        # Create path recursively
        parts = path.split("/")
        current_handle = None
        current_path = ""
        
        for part in parts:
            current_path = f"{current_path}/{part}" if current_path else part
            node = await self._client.get(f"/{current_path}")
            
            if node:
                current_handle = node.handle
            else:
                # Create folder
                if current_handle is None:
                    root = await self._client.get_root()
                    current_handle = root.handle
                
                new_folder = await self._client.mkdir(part, parent=current_handle)
                current_handle = new_folder.handle
        
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
            # Get or create .previews folder
            folder_handle = await self._ensure_preview_folder()
            
            if not folder_handle:
                print(f"[storage] Failed to get/create .previews folder")
                return None
            
            # Upload with source_id as name
            preview_name = f"{source_id}.jpg"
            node = await self._client.upload(
                path,
                dest_folder=folder_handle,
                name=preview_name
            )
            
            return node.handle if node else None
            
        except Exception as e:
            print(f"[storage] Upload preview error: {e}")
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
                # Create folder in root
                root = await self._client.get_root()
                if root:
                    folder = await self._client.mkdir(".previews", parent=root.handle)
                    self._preview_folder_handle = folder.handle
                    print(f"[storage] Created /.previews folder")
            
            return self._preview_folder_handle
        except Exception as e:
            print(f"[storage] Error creating .previews folder: {e}")
            return None
