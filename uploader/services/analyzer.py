"""
Analyzer Service - Single Responsibility: analyze media files.

Uses mediakit for video/photo analysis.
"""
from pathlib import Path
from typing import Dict, Any
import asyncio

from ..protocols import IAnalyzer

# Import from mediakit (single source of truth)
from mediakit import VIDEO_EXTENSIONS, IMAGE_EXTENSIONS, is_video, is_image


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
        return await loop.run_in_executor(None, self.analyze, path)
    
    async def analyze_video_async(self, path: Path) -> Dict[str, Any]:
        """Analyze video file asynchronously."""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self.analyze_video, path)
    
    async def analyze_photo_async(self, path: Path) -> Dict[str, Any]:
        """Analyze photo file asynchronously."""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, self.analyze_photo, path)
    
    @staticmethod
    def is_video(path: Path) -> bool:
        return is_video(path)
    
    @staticmethod
    def is_photo(path: Path) -> bool:
        return is_image(path)
