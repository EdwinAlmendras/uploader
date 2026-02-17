"""
Analyzer Service - Single Responsibility: analyze media files.

Uses mediakit for video/photo analysis.
"""
from pathlib import Path
from typing import Dict, Any, Optional, List
import asyncio
import os

from ..protocols import IAnalyzer
from .resume import blake3_file
from mediakit import is_video, is_image, analyze_video, analyze_photo


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
        return analyze_video(path)
    
    def analyze_photo(
        self, 
        path: Path, 
        phash: Optional[str] = None,
        avg_color_lab: Optional[List[float]] = None
    ) -> Dict[str, Any]:
        """
        Analyze photo file.
        
        Args:
            path: Path to image file
            phash: Pre-calculated pHash (optional)
            avg_color_lab: Pre-calculated avg_color_lab (optional)
        """
        return analyze_photo(path, include_embedding=False, phash=phash, avg_color_lab=avg_color_lab)
    
    async def analyze_async(self, path: Path) -> Dict[str, Any]:
        """Analyze media file asynchronously."""
        loop = asyncio.get_event_loop()
        tech_data = await loop.run_in_executor(None, self.analyze, path)
        if not self._skip_hash_check():
            tech_data["blake3_hash"] = await blake3_file(path)
        return tech_data
    
    async def analyze_video_async(self, path: Path) -> Dict[str, Any]:
        """Analyze video file asynchronously."""
        loop = asyncio.get_event_loop()
        tech_data = await loop.run_in_executor(None, self.analyze_video, path)
        if not self._skip_hash_check():
            tech_data["blake3_hash"] = await blake3_file(path)
        return tech_data
    
    async def analyze_photo_async(
        self, 
        path: Path,
        phash: Optional[str] = None,
        avg_color_lab: Optional[List[float]] = None
    ) -> Dict[str, Any]:
        """
        Analyze photo file asynchronously.
        
        Args:
            path: Path to image file
            phash: Pre-calculated pHash (optional)
            avg_color_lab: Pre-calculated avg_color_lab (optional)
        """
        loop = asyncio.get_event_loop()
        tech_data = await loop.run_in_executor(
            None, 
            self.analyze_photo, 
            path, 
            phash, 
            avg_color_lab
        )
        if not self._skip_hash_check():
            tech_data["blake3_hash"] = await blake3_file(path)
        return tech_data
    
    @staticmethod
    def is_video(path: Path) -> bool:
        return is_video(path)
    
    @staticmethod
    def is_photo(path: Path) -> bool:
        return is_image(path)
    @staticmethod
    def _skip_hash_check() -> bool:
        raw = os.getenv("UPLOADER_SKIP_HASH_CHECK", "").strip().lower()
        return raw in {"1", "true", "yes", "on"}
