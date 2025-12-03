"""
Preview Service - Single Responsibility: generate video previews.

Uses mediakit VideoGridGenerator for grid generation.
Grid sizes:
- < 1 min: 3x3
- 1-15 min: 4x4
- > 15 min: 5x5
"""
from pathlib import Path
from typing import Optional
import tempfile
import asyncio

from ..models import UploadConfig


class PreviewService:
    """
    Service for generating video preview grids.
    
    Uses mediakit VideoGridGenerator.
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
            Path to generated grid image (temp file)
        """
        grid_size = self._config.get_grid_size(duration)
        
        # Run in executor (mediakit is sync)
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            None, self._generate_sync, video_path, grid_size
        )
    
    def _generate_sync(self, video_path: Path, grid_size: int) -> Path:
        """Generate grid synchronously."""
        from mediakit.video import VideoGridGenerator
        
        # Create temp output path
        temp_dir = tempfile.mkdtemp(prefix="preview_")
        output_path = Path(temp_dir) / f"{video_path.stem}_grid.jpg"
        
        # Generate grid
        generator = VideoGridGenerator()
        generator.generate(
            video_path=video_path,
            output_path=output_path,
            cols=grid_size,
            rows=grid_size
        )
        
        return output_path
    
    def get_preview_name(self, source_id: str) -> str:
        """Get preview filename: {source_id}.jpg"""
        return f"{source_id}.jpg"
