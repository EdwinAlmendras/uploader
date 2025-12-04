"""File collection utilities for folder uploads."""
from pathlib import Path
from typing import List
from mediakit import is_video, is_image


class FileCollector:
    """Collects media files from folders."""
    
    @staticmethod
    def collect_files(folder: Path) -> List[Path]:
        """
        Collect all media files recursively.
        
        Args:
            folder: Root folder to scan
            
        Returns:
            List of media file paths
        """
        files = []
        for item in folder.rglob("*"):
            if item.is_file() and (is_video(item) or is_image(item)):
                files.append(item)
        return sorted(files)

