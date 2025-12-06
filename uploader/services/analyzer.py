"""
Analyzer Service - Single Responsibility: analyze media files.

Uses mediakit for video/photo analysis.
"""
from pathlib import Path
from typing import Dict, Any
import asyncio

from ..protocols import IAnalyzer
from .resume import blake3_file

# Try to import from mediakit, fallback to local definitions
try:
    from mediakit import VIDEO_EXTENSIONS, IMAGE_EXTENSIONS, is_video, is_image
except ImportError:
    VIDEO_EXTENSIONS = {
        '.mp4', '.avi', '.mkv', '.mov', '.wmv', '.flv', '.webm', '.m4v',
        '.3gp', '.ogv', '.mts', '.m2ts', '.ts', '.mpeg', '.mpg',
    }
    IMAGE_EXTENSIONS = {
        '.jpg', '.jpeg', '.png', '.webp', '.gif', '.bmp', '.tiff', '.tif',
        '.heic', '.heif',
    }
    
    def is_video(path) -> bool:
        return Path(path).suffix.lower() in VIDEO_EXTENSIONS
    
    def is_image(path) -> bool:
        return Path(path).suffix.lower() in IMAGE_EXTENSIONS


class AnalyzerService(IAnalyzer):
    """
    Service for analyzing media files (videos and photos).
    
    Wraps mediakit.analyze_video and analyze_photo with async support.
    """
    
    def analyze(self, path: Path) -> Dict[str, Any]:
        """
        Analyze media file synchronously (auto-detect type).
        
        Args:
            path: Path to media file
            
        Returns:
            Dict with metadata
        """
        path = Path(path)
        
        if is_video(path):
            return self.analyze_video(path)
        elif is_image(path):
            return self.analyze_photo(path)
        else:
            raise ValueError(f"Unsupported file type: {path.suffix}")
    
    def analyze_video(self, path: Path) -> Dict[str, Any]:
        """Analyze video file."""
        from mediakit import analyze_video
        return analyze_video(path)
    
    def analyze_photo(self, path: Path) -> Dict[str, Any]:
        """Analyze photo file."""
        from mediakit import analyze_photo
        return analyze_photo(path)
    
    async def analyze_async(self, path: Path) -> Dict[str, Any]:
        """Analyze media file asynchronously."""
        loop = asyncio.get_event_loop()
        tech_data = await loop.run_in_executor(None, self.analyze, path)
        # Add blake3_hash
        tech_data["blake3_hash"] = await blake3_file(path)
        return tech_data
    
    async def analyze_video_async(self, path: Path) -> Dict[str, Any]:
        """Analyze video file asynchronously."""
        loop = asyncio.get_event_loop()
        tech_data = await loop.run_in_executor(None, self.analyze_video, path)
        # Add blake3_hash
        tech_data["blake3_hash"] = await blake3_file(path)
        return tech_data
    
    async def analyze_photo_async(self, path: Path) -> Dict[str, Any]:
        """Analyze photo file asynchronously."""
        loop = asyncio.get_event_loop()
        tech_data = await loop.run_in_executor(None, self.analyze_photo, path)
        # Add blake3_hash
        tech_data["blake3_hash"] = await blake3_file(path)
        return tech_data
    
    @staticmethod
    def is_video(path: Path) -> bool:
        return is_video(path)
    
    @staticmethod
    def is_photo(path: Path) -> bool:
        return is_image(path)
