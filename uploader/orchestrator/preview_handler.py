"""Preview generation and upload handler."""
import asyncio
from pathlib import Path
from typing import Optional
import shutil
import logging

logger = logging.getLogger(__name__)


class PreviewHandler:
    """Handles preview generation and upload."""
    
    def __init__(self, preview_service, storage_service):
        """
        Initialize preview handler.
        
        Args:
            preview_service: PreviewService instance
            storage_service: Storage service for uploading previews
        """
        self._preview = preview_service
        self._storage = storage_service
    
    async def upload_preview(
        self,
        path: Path,
        source_id: str,
        duration: float,
        dest_path: Optional[str] = None,
        filename: Optional[str] = None
    ) -> Optional[str]:
        """
        Generate and upload preview grid to the same path as the video.
        
        If dest_path and filename are provided, uploads to {dest_path}/{filename}.jpg
        Otherwise, falls back to old behavior (/.previews/{source_id}.jpg) for backward compatibility.
        """
        try:
            # Verify file exists before generating preview
            path = Path(path)
            if not path.exists():
                print(f"[preview] Error: Video file does not exist: {path}")
                return None
            
            # Generate grid (3x3, 4x4, or 5x5 based on duration)
            grid_path = await self._preview.generate(path, duration)
            
            if not grid_path or not grid_path.exists():
                print(f"[preview] Failed to generate grid for {path.name}")
                return None
            
            # Determine preview filename: use video filename with .jpg extension
            if filename:
                # Remove extension from video filename and add .jpg
                video_stem = Path(filename).stem
                preview_filename = f"{video_stem}.jpg"
            else:
                # Fallback to source_id for backward compatibility
                preview_filename = f"{source_id}.jpg"
            
            # Upload to same path as video, or fallback to .previews
            if dest_path:
                handle = await self._storage.upload_preview(
                    grid_path, 
                    dest_path=dest_path,
                    filename=preview_filename
                )
                preview_path = f"{dest_path}/{preview_filename}"
            else:
                # Backward compatibility: use old .previews path
                handle = await self._storage.upload_preview(
                    grid_path,
                    source_id=source_id
                )
                preview_path = f"/.previews/{source_id}.jpg"
            
            # Cleanup temp file
            try:
                grid_path.unlink(missing_ok=True)
                # Also cleanup temp dir
                if grid_path.parent.name.startswith("preview_"):
                    shutil.rmtree(grid_path.parent, ignore_errors=True)
            except:
                pass
            
            if handle:
                print(f"[preview] Uploaded: {preview_path}")
            else:
                print(f"[preview] Upload failed for {preview_path}")
            
            return handle
            
        except FileNotFoundError as e:
            print(f"[preview] Error: Video file does not exist: {path}")
            return None
        except Exception as e:
            print(f"[preview] Error: {e}")
            return None
    
    async def upload_preview_background(
        self,
        path: Path,
        source_id: str,
        duration: float
    ):
        """Generate and upload preview in background (non-blocking)."""
        try:
            await self.upload_preview(path, source_id, duration)
        except Exception as e:
            print(f"[preview] Background error for {source_id}: {e}")

