"""
Set Processor - Processes image sets: thumbnails, 7z creation, and upload.

Uses mediakit for image processing and archive creation.
"""
from pathlib import Path
from typing import List, Optional, Tuple, Dict, Any, Callable
import asyncio
import logging
import tempfile
import shutil
from enum import IntEnum

from mediakit import is_image
from mediakit.set_processor import SetProcessor as MediakitSetProcessor, SetMetadata
from mediakit.image.resizer import SetResizer, ResizeConfig
from mediakit.image.selector import ImageSelector
from mediakit.archive.sevenzip import SevenZipArchiver, ArchiveConfig
from mediakit.core.interfaces import ResizeQuality
from mediakit.preview.image_preview import ImagePreviewGenerator, GridConfig

from uploader.services import AnalyzerService, MetadataRepository, StorageService
from uploader.orchestrator.models import UploadResult
from uploader.models import UploadConfig
from mediakit.analyzer import generate_id

logger = logging.getLogger(__name__)


class CustomResizeQuality(IntEnum):
    """Custom resize quality for set thumbnails."""
    THUMB_320 = 320
    THUMB_1024 = 1024


class ImageSetProcessor:
    """Processes image sets: generates thumbnails, creates 7z, uploads to MEGA."""
    
    def __init__(
        self,
        analyzer: AnalyzerService,
        repository: MetadataRepository,
        storage: StorageService,
        config: UploadConfig
    ):
        self._analyzer = analyzer
        self._repository = repository
        self._storage = storage
        self._config = config
        
        # Initialize mediakit components
        self._set_processor = MediakitSetProcessor()
        self._selector = ImageSelector()
        self._preview_generator = ImagePreviewGenerator(cell_size=400)
        
        # Custom resize config for 320px and 1024px thumbnails
        # We'll use SetResizer directly with custom sizes (though we'll do custom resize)
        self._resizer = SetResizer(ResizeConfig(qualities=[ResizeQuality.SMALL]))
        self._archiver = SevenZipArchiver(ArchiveConfig(compression_level=0, output_dir=None))
    
    async def process_set(
        self,
        set_folder: Path,
        dest_path: str,
        progress_callback: Optional[Callable] = None
    ) -> Tuple[UploadResult, List[UploadResult]]:
        """
        Process a complete image set.
        
        Steps:
        1. Generate thumbnails (320px and 1024px)
        2. Analyze all images and save to API with set_doc_id
        3. Create 7z archive
        4. Upload 7z to MEGA
        5. Save set document to API
        
        Args:
            set_folder: Path to the image set folder
            dest_path: Destination path in MEGA
            progress_callback: Optional progress callback
            
        Returns:
            Tuple of (set_upload_result, list of image_upload_results)
        """
        set_name = set_folder.name
        logger.info(f"Processing image set: {set_name}")
        
        if progress_callback:
            progress_callback(f"Processing set: {set_name}...", 0, 100)
        
        try:
            # Step 1: Get images and generate thumbnails
            images = self._selector.get_images(set_folder)
            if not images:
                return (
                    UploadResult.fail(set_name, "No images found in set"),
                    []
                )
            
            logger.info(f"Found {len(images)} images in set {set_name}")
            
            # Step 1.1: Select cover (first vertical image)
            cover = self._selector.select_cover(images)
            logger.info(f"Selected cover: {cover.name} (first vertical image)")
            
            # Generate thumbnails (320px and 1024px)
            await self._generate_thumbnails(set_folder, images, progress_callback)
            
            # Step 1.2: Generate grid preview for the set
            logger.info("Generating grid preview for set...")
            if progress_callback:
                progress_callback(f"Generating grid preview...", 25, 100)
            
            grid_preview_path = await self._generate_grid_preview(set_folder)
            
            # Step 2: Generate set document ID (for the 7z)
            set_source_id = generate_id()
            logger.info(f"Generated set source_id: {set_source_id}")
            
            # Step 3: Process all images (analyze and save to API with set_doc_id)
            image_results = await self._process_set_images(
                set_folder,
                images,
                set_source_id,
                progress_callback
            )
            
            # Step 4: Create 7z archive
            archive_name = f"{set_name}.7z"
            logger.info(f"Creating 7z archive: {archive_name}")
            if progress_callback:
                progress_callback(f"Creating archive: {archive_name}...", 60, 100)
            
            archive_files = self._archiver.create(set_folder, archive_name)
            
            if not archive_files:
                # Cleanup grid preview if exists
                if grid_preview_path and grid_preview_path.exists():
                    try:
                        grid_preview_path.unlink(missing_ok=True)
                    except:
                        pass
                return (
                    UploadResult.fail(set_name, "Failed to create 7z archive"),
                    image_results
                )
            
            logger.info(f"Created {len(archive_files)} archive file(s)")
            
            # Step 5: Upload 7z to MEGA (sequentially, one at a time)
            mega_handles = []
            for idx, archive_file in enumerate(archive_files, 1):
                logger.info(f"Uploading archive part {idx}/{len(archive_files)}: {archive_file.name}")
                if progress_callback:
                    progress_callback(
                        f"Uploading archive {idx}/{len(archive_files)}...",
                        70 + (idx * 10 // len(archive_files)),
                        100
                    )
                
                try:
                    # Upload archive file
                    handle = await self._storage.upload_video(
                        archive_file,
                        dest_path,
                        set_source_id,
                        progress_callback
                    )
                    if handle:
                        mega_handles.append(handle)
                        logger.info(f"Uploaded archive part {idx}: {handle}")
                    else:
                        logger.error(f"Failed to upload archive part {idx}")
                except Exception as e:
                    logger.error(f"Error uploading archive part {idx}: {e}")
            
            if not mega_handles:
                # Cleanup grid preview if exists
                if grid_preview_path and grid_preview_path.exists():
                    try:
                        grid_preview_path.unlink(missing_ok=True)
                    except:
                        pass
                return (
                    UploadResult.fail(set_name, "Failed to upload 7z archive"),
                    image_results
                )
            
            # Step 6: Upload cover image (first vertical image) to same path
            cover_handle = None
            if cover and cover.exists():
                try:
                    logger.info(f"Uploading cover image: {cover.name}")
                    if progress_callback:
                        progress_callback(f"Uploading cover: {cover.name}...", 95, 100)
                    
                    # Upload cover to same path as 7z with name {set_name}_cover.jpg
                    cover_filename = f"{set_name}_cover.jpg"
                    
                    # Create a temp copy with correct name for upload
                    temp_cover = tempfile.NamedTemporaryFile(suffix='.jpg', delete=False)
                    temp_cover_path = Path(temp_cover.name)
                    temp_cover.close()
                    
                    # Copy cover to temp file
                    shutil.copy2(cover, temp_cover_path)
                    
                    try:
                        cover_handle = await self._storage.upload_preview(
                            temp_cover_path,
                            dest_path=dest_path,
                            filename=cover_filename
                        )
                        
                        if cover_handle:
                            logger.info(f"Cover uploaded: {dest_path}/{cover_filename}")
                        else:
                            logger.warning(f"Failed to upload cover image")
                    finally:
                        # Cleanup temp file
                        try:
                            temp_cover_path.unlink(missing_ok=True)
                        except:
                            pass
                        
                except Exception as e:
                    logger.warning(f"Error uploading cover: {e}")
            
            # Step 7: Upload grid preview to same path as 7z (same name, .jpg extension)
            grid_handle = None
            if grid_preview_path and grid_preview_path.exists():
                try:
                    logger.info(f"Uploading grid preview: {set_name}.jpg")
                    if progress_callback:
                        progress_callback(f"Uploading grid preview...", 96, 100)
                    
                    # Grid preview name: same as 7z but with .jpg extension
                    grid_filename = f"{set_name}.jpg"
                    
                    grid_handle = await self._storage.upload_preview(
                        grid_preview_path,
                        dest_path=dest_path,
                        filename=grid_filename
                    )
                    
                    if grid_handle:
                        logger.info(f"Grid preview uploaded: {dest_path}/{grid_filename}")
                    else:
                        logger.warning(f"Failed to upload grid preview")
                        
                except Exception as e:
                    logger.warning(f"Error uploading grid preview: {e}")
                finally:
                    # Cleanup grid preview temp file
                    if grid_preview_path and grid_preview_path.exists():
                        try:
                            grid_preview_path.unlink(missing_ok=True)
                            # Also cleanup temp dir if it's in a temp directory
                            if grid_preview_path.parent.name.startswith("tmp") or "temp" in str(grid_preview_path.parent):
                                try:
                                    shutil.rmtree(grid_preview_path.parent, ignore_errors=True)
                                except:
                                    pass
                        except:
                            pass
            
            # Step 8: Save set document to API
            await self._save_set_document(set_source_id, set_name, archive_name, len(images))
            
            logger.info(f"Successfully processed set: {set_name}")
            
            return (
                UploadResult.ok(set_source_id, set_name, mega_handles[0], None),
                image_results
            )
            
        except Exception as e:
            error_msg = str(e) or f"{type(e).__name__}"
            logger.error(f"Error processing set {set_name}: {error_msg}", exc_info=True)
            return (
                UploadResult.fail(set_name, error_msg),
                []
            )
    
    async def _generate_thumbnails(
        self,
        set_folder: Path,
        images: List[Path],
        progress_callback: Optional[Callable]
    ) -> None:
        """Generate 320px and 1024px thumbnails for all images."""
        logger.info("Generating thumbnails (320px and 1024px)...")
        
        # Create thumbnail folders
        thumb_320_dir = set_folder / "m"
        thumb_1024_dir = set_folder / "x"
        thumb_320_dir.mkdir(exist_ok=True)
        thumb_1024_dir.mkdir(exist_ok=True)
        
        # Process images in parallel batches
        loop = asyncio.get_event_loop()
        batch_size = 10
        
        for i in range(0, len(images), batch_size):
            batch = images[i:i+batch_size]
            tasks = [
                loop.run_in_executor(
                    None,
                    self._resize_image_thumbnails,
                    img,
                    thumb_320_dir,
                    thumb_1024_dir
                )
                for img in batch
            ]
            
            await asyncio.gather(*tasks, return_exceptions=True)
            
            if progress_callback:
                progress_callback(
                    f"Generated thumbnails: {min(i+batch_size, len(images))}/{len(images)}",
                    (i+batch_size) * 30 // len(images),
                    100
                )
        
        logger.info("Thumbnail generation complete")
    
    async def _generate_grid_preview(
        self,
        set_folder: Path
    ) -> Optional[Path]:
        """
        Generate grid preview for the set using mediakit.
        
        Returns:
            Path to generated grid preview image
        """
        try:
            loop = asyncio.get_event_loop()
            
            # Generate grid preview using mediakit (synchronous operation)
            grid_path = await loop.run_in_executor(
                None,
                self._preview_generator.generate,
                set_folder,
                None,  # output_path=None means it will create a temp file
                None   # config=None means use defaults
            )
            
            if grid_path and grid_path.exists():
                logger.info(f"Grid preview generated: {grid_path}")
                return grid_path
            else:
                logger.warning("Failed to generate grid preview")
                return None
                
        except Exception as e:
            logger.error(f"Error generating grid preview: {e}", exc_info=True)
            return None
    
    @staticmethod
    def _resize_image_thumbnails(
        image_path: Path,
        thumb_320_dir: Path,
        thumb_1024_dir: Path
    ) -> None:
        """Resize a single image to 320px and 1024px thumbnails."""
        from PIL import Image
        from mediakit.image.orientation import OrientationFixer
        
        try:
            with Image.open(image_path) as img:
                fixed_img = OrientationFixer.fix_pil_image(img)
                
                # Determine output name (convert to jpg)
                output_name = image_path.stem + ".jpg"
                
                # Resize to 320px
                thumb_320_path = thumb_320_dir / output_name
                ImageSetProcessor._resize_to_max_dimension(fixed_img, thumb_320_path, 320)
                
                # Resize to 1024px
                thumb_1024_path = thumb_1024_dir / output_name
                ImageSetProcessor._resize_to_max_dimension(fixed_img, thumb_1024_path, 1024)
                
        except Exception as e:
            logger.warning(f"Error generating thumbnails for {image_path.name}: {e}")
    
    @staticmethod
    def _resize_to_max_dimension(img, output_path: Path, max_size: int) -> None:
        """Resize image so the longest side is max_size."""
        from PIL import Image
        
        w, h = img.size
        
        if w > max_size or h > max_size:
            if w > h:
                new_w = max_size
                new_h = int(h * max_size / w)
            else:
                new_h = max_size
                new_w = int(w * max_size / h)
            resized = img.resize((new_w, new_h), Image.Resampling.LANCZOS)
        else:
            resized = img
        
        # Convert to RGB if needed
        if resized.mode in ("RGBA", "LA"):
            background = Image.new("RGB", resized.size, (255, 255, 255))
            alpha = resized.split()[-1]
            background.paste(resized.convert("RGB"), mask=alpha)
            resized = background
        elif resized.mode not in ("RGB", "L"):
            resized = resized.convert("RGB")
        
        resized.save(output_path, format="JPEG", quality=90, optimize=True, progressive=True)
    
    async def _process_set_images(
        self,
        set_folder: Path,
        images: List[Path],
        set_source_id: str,
        progress_callback: Optional[Callable]
    ) -> List[UploadResult]:
        """Process all images in the set: analyze and save to API with set_doc_id."""
        results = []
        
        logger.info(f"Processing {len(images)} images for set...")
        
        for idx, image_path in enumerate(images, 1):
            try:
                if progress_callback:
                    progress_callback(
                        f"Analyzing image {idx}/{len(images)}: {image_path.name}",
                        30 + (idx * 30 // len(images)),
                        100
                    )
                
                # Analyze image
                tech_data = await self._analyzer.analyze_photo_async(image_path)
                
                # Generate source_id for this image
                image_source_id = generate_id()
                tech_data["source_id"] = image_source_id
                
                # Add set_doc_id reference
                tech_data["set_doc_id"] = set_source_id
                
                # Save document with set_doc_id
                await self._repository.save_document(tech_data)
                
                # Save photo metadata
                await self._repository.save_photo_metadata(image_source_id, tech_data)
                
                logger.debug(f"Saved image {image_path.name} with set_doc_id={set_source_id}")
                
                results.append(
                    UploadResult.ok(image_source_id, image_path.name, None, None)
                )
                
            except Exception as e:
                logger.error(f"Error processing image {image_path.name}: {e}")
                results.append(
                    UploadResult.fail(image_path.name, str(e))
                )
        
        logger.info(f"Processed {len(results)} images for set")
        return results
    
    async def _save_set_document(
        self,
        set_source_id: str,
        set_name: str,
        archive_name: str,
        image_count: int
    ) -> None:
        """Save set document to API (the 7z archive document)."""
        set_doc = {
            "source_id": set_source_id,
            "filename": archive_name,
            "mimetype": "application/x-7z-compressed",
            "set_image_count": image_count,
            "set_name": set_name
        }
        
        await self._repository.save_set_document(set_doc)
        logger.info(f"Saved set document: {set_source_id}")

