"""
Preview Service - Single Responsibility: generate video previews.

Uses mediakit generate_video_grid function.
Grid sizes:
- < 1 min: 3x3
- 1-15 min: 4x4
- > 15 min: 5x5
"""
from pathlib import Path
from typing import Optional

from ..models import UploadConfig


class PreviewService:
    """
    Service for generating video preview grids.
    
    Uses mediakit.video.generate_video_grid().
    """
    
    def __init__(self, config: Optional[UploadConfig] = None):
        self._config = config or UploadConfig()
    
    async def generate(self, video_path: Path, duration: float) -> Path:
        """
        Generate grid preview for video.
        
        Args:
            video_path: Path to video file
            duration: Video duration in seconds
            
        Returns:
            Path to generated grid image
        """
        from mediakit.video import generate_video_grid
        
        grid_size = self._config.get_grid_size(duration)
        
        # generate_video_grid is async
        return await generate_video_grid(
            video_path=video_path,
            grid_size=grid_size,
            max_size=360,
            quality=90
        )
    
    def get_preview_name(self, source_id: str) -> str:
        """Get preview filename: {source_id}.jpg"""
        return f"{source_id}.jpg"
