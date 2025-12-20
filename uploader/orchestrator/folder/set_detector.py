"""
Set Detector - Detects image sets in folders.

An image set is a folder that contains ONLY images (no videos, no subfolders).
If a folder has videos or subfolders mixed with images, the images are moved
to a subfolder with the parent's name to create a proper set.
"""
from pathlib import Path
from typing import List, Tuple, Set
import shutil
import logging
from mediakit import is_video, is_image

logger = logging.getLogger(__name__)

# Supported file extensions for individual files (in addition to videos and images)
SUPPORTED_EXTENSIONS = {'.pdf', '.html', '.htm', '.xls', '.xlsx', '.doc', '.docx', '.txt', '.srt'}

def is_supported_file(path: Path) -> bool:
    """Check if file is a supported type (video, image, or other supported document)."""
    return is_video(path) or is_image(path) or path.suffix.lower() in SUPPORTED_EXTENSIONS


class SetDetector:
    """Detects and prepares image sets for processing."""
    
    def __init__(self):
        pass
    
    def detect_sets(
        self, 
        folder_path: Path
    ) -> Tuple[List[Path], List[Path]]:
        """
        Detect image sets in a folder.
        
        Args:
            folder_path: Root folder to scan
            
        Returns:
            Tuple of (list of set folders, list of individual files)
        """
        sets = []
        individual_files = []
        
        # Folders to ignore (thumbnail folders that shouldn't be scanned)
        ignored_thumbnail_folders = {'m', 'x', 'xl'}
        
        # Scan all subdirectories
        for item in folder_path.iterdir():
            if item.is_dir():
                # Skip thumbnail folders (m, x, xl) to avoid unnecessary recursion
                if item.name in ignored_thumbnail_folders:
                    logger.debug(f"Skipping thumbnail folder: {item.name}")
                    continue
                
                if self._is_image_set(item):
                    sets.append(item)
                    logger.debug(f"Detected image set: {item.name}")
                else:
                    sub_sets, sub_files = self.detect_sets(item)
                    sets.extend(sub_sets)
                    individual_files.extend(sub_files)
            elif item.is_file():
                # Check if it's a supported file (video, image, or other supported document)
                if is_supported_file(item):
                    individual_files.append(item)
        
        return sets, individual_files
    
    def _is_image_set(self, folder: Path) -> bool:
        """
        Check if a folder is a valid image set.
        
        A valid set:
        - Contains only image files (no videos)
        - Has no subdirectories (except ignored folders like 'm', 'x', 'xl', '.previews')
        - Has at least one image
        """
        ignored_folders = {'m', 'x', 'xl', '.previews', '.covers', 'files'}
        has_images = False
        has_videos = False
        has_subfolders = False
        count_images = 0
        for item in folder.iterdir():
            if item.is_dir():
                # Ignore thumbnail folders and other system folders
                if item.name not in ignored_folders:
                    has_subfolders = True
                    logger.debug(f"Folder {folder.name} has subfolder: {item.name}")
            elif item.is_file():
                if is_image(item):
                    has_images = True
                    count_images += 1
                elif is_video(item):
                    has_videos = True
                    logger.debug(f"Folder {folder.name} has video: {item.name}")
        
        # Valid set: has images, no videos, no subfolders
        if count_images < 5:
            return False
        
        if has_images and not has_videos and not has_subfolders:
            return True
        
        # If folder has videos/subfolders but also images, we need to move images
        if has_images and (has_videos or has_subfolders):
            logger.info(
                f"Folder {folder.name} has mixed content. "
                f"Moving images to subfolder to create set..."
            )
            self._move_images_to_subfolder(folder)
            # After moving, check again
            return self._is_image_set(folder)
        
        return False
    
    def _move_images_to_subfolder(self, folder: Path) -> None:
        """
        Move all images from folder to a subfolder with the same name.
        
        This creates a proper image set structure.
        """
        images = [item for item in folder.iterdir() if item.is_file() and is_image(item)]
        
        if not images:
            return
        
        # Create subfolder with parent's name
        subfolder = folder / folder.name
        subfolder.mkdir(exist_ok=True)
        
        logger.info(f"Moving {len(images)} images to {subfolder.name}/")
        
        for image in images:
            try:
                dest = subfolder / image.name
                # Handle name conflicts
                if dest.exists():
                    counter = 1
                    while dest.exists():
                        stem = image.stem
                        suffix = image.suffix
                        dest = subfolder / f"{stem}_{counter}{suffix}"
                        counter += 1
                
                shutil.move(str(image), str(dest))
                logger.debug(f"Moved {image.name} -> {subfolder.name}/{dest.name}")
            except Exception as e:
                logger.error(f"Error moving {image.name}: {e}")

