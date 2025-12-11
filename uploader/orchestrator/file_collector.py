"""File collection utilities for folder uploads."""
from pathlib import Path
from typing import List, Tuple
from mediakit import is_video, is_image
from .folder.set_detector import SetDetector


class FileCollector:
    """Collects media files from folders and detects image sets."""
    
    def __init__(self):
        self._set_detector = SetDetector()
    
    def collect_files(self, folder: Path) -> List[Path]:
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
    
    def detect_sets_and_files(self, folder: Path) -> Tuple[List[Path], List[Path]]:
        """
        Detect image sets and collect individual files.
        
        Args:
            folder: Root folder to scan
            
        Returns:
            Tuple of (list of set folders, list of individual file paths)
        """
        sets, individual_files = self._set_detector.detect_sets(folder)
        return sets, individual_files

