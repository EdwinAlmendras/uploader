"""
Preview Service - Single Responsibility: generate video previews.

Uses mediakit for grid generation.
"""
from pathlib import Path
from typing import Optional

from ..protocols import IPreviewGenerator
from ..models import UploadConfig


class PreviewService(IPreviewGenerator):
    """
    Service for generating video preview grids.
    
    Uses Strategy Pattern via config for grid size determination.
    """
    
    def __init__(self, config: Optional[UploadConfig] = None):
        """
        Initialize preview service.
        
        Args:
            config: Upload configuration with grid settings
        """
        self._config = config or UploadConfig()
    
    async def generate(self, path: Path, duration: float) -> Path:
        """
        Generate grid preview for video.
        
        Grid size is determined by duration:
        - < 20 min: 4x4 grid
        - >= 20 min: 5x5 grid
        
        Args:
            path: Path to video file
            duration: Video duration in seconds
            
        Returns:
            Path to generated grid image
        """
        from mediakit.video import generate_video_grid
        
        grid_size = self._config.get_grid_size(duration)
        return await generate_video_grid(path, grid_size=grid_size)
    
    def get_preview_name(self, source_id: str) -> str:
        """
        Get preview filename for source_id.
        
        Args:
            source_id: Document source ID
            
        Returns:
            Preview filename (e.g., "abc123.jpg")
        """
        return f"{source_id}.jpg"
