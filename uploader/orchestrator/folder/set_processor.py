"""
Set Processor - Processes image sets: thumbnails, 7z creation, and upload.

Uses mediakit for image processing and archive creation.
"""
from pathlib import Path
from typing import List, Optional, Tuple, Callable, Union, Dict
import asyncio
import logging
import tempfile
import shutil
from enum import IntEnum

from PIL import Image

from mediakit.set_processor import SetProcessor as MediakitSetProcessor
from mediakit.image.selector import ImageSelector
from mediakit.archive.sevenzip import SevenZipArchiver, ArchiveConfig
from mediakit.preview.image_preview import ImagePreviewGenerator

from uploader.services import AnalyzerService, MetadataRepository, StorageService
from uploader.services.managed_storage import ManagedStorageService
from uploader.services.resume import blake3_file
from uploader.orchestrator.models import UploadResult
from uploader.models import UploadConfig
from mediakit.analyzer import generate_id
from mediakit.image.processor import ImageProcessor
from typing import Any

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
        self._image_processor = ImageProcessor()
        # Initialize mediakit components
        self._set_processor = MediakitSetProcessor()
        self._selector = ImageSelector()
        self._preview_generator = ImagePreviewGenerator(cell_size=400)
        
        self._archiver = SevenZipArchiver(ArchiveConfig(compression_level=0, output_dir=None))
    
    async def process_set(
        self,
        set_folder: Path,
        dest_path: str,
        progress_callback: Optional[Callable] = None,
        pre_check_info: Optional[Dict[str, Any]] = None
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
        logger.debug(f"Processing image set: {set_name}")
        
        if progress_callback:
            progress_callback(f"Processing set: {set_name}...", 0, 100)
        
        try:
            # Step 1: Get images (needed for basic validation)
            images = self._selector.get_images(set_folder)
            if not images:
                return (
                    UploadResult.fail(set_name, "No images found in set"),
                    []
                )
            
            # Use pre-check info if provided (from batch check), otherwise check individually
            existing_source_id = None
            existing_mega_handle = None
            archive_exists_in_both = False
            archive_hash = None
            
            if pre_check_info and pre_check_info.get('exists_in_both'):
                # Use batch check results
                existing_source_id = pre_check_info.get('source_id')
                existing_mega_handle = pre_check_info.get('mega_handle')
                archive_hash = pre_check_info.get('hash')
                archive_exists_in_both = True
            else:
                # Check individually (legacy behavior or if pre_check_info not provided)
                archive_name = f"{set_name}.7z"
                existing_archive_path = set_folder.parent / "files" / archive_name
                archive_file_for_hash = None
                
                if existing_archive_path.exists():
                    archive_file_for_hash = existing_archive_path
                
                # If we have an archive file, check if it already exists
                if archive_file_for_hash and archive_file_for_hash.exists():
                    try:
                        # Calculate blake3 hash
                        logger.debug(f"Calculating hash for archive: {archive_file_for_hash.name}")
                        if progress_callback:
                            progress_callback(f"Calculating archive hash...", 10, 100)
                        
                        archive_hash = await blake3_file(archive_file_for_hash)
                        
                        logger.debug(f"Archive hash: {archive_hash}")
                        
                        if self._repository:
                            existing_hashes = await self._repository.check_exists_batch([archive_hash])
                            logger.debug(f"Existing hashes: {existing_hashes}")
                            if archive_hash in existing_hashes:
                                doc_info = existing_hashes[archive_hash]
                                logger.debug(f"Doc info: {doc_info}")
                                # Handle both old and new format
                                if isinstance(doc_info, dict):
                                    existing_source_id = doc_info.get("source_id")
                                    logger.debug(f"Existing source ID: {existing_source_id}")
                                    existing_mega_handle = doc_info.get("mega_handle")
                                    logger.debug(f"Existing mega handle: {existing_mega_handle}")
                                else:
                                    existing_source_id = doc_info
                                    logger.debug(f"Existing source ID: {existing_source_id}")
                                
                                
                                if existing_source_id:
                                    exists_in_mega = False
                                    try:
                                        if isinstance(self._storage, ManagedStorageService):
                                            exists_in_mega = await self._storage.manager.find_by_mega_id(existing_source_id) is not None
                                        else:
                                            exists_in_mega = await self._storage.exists_by_mega_id(existing_source_id)
                                    except Exception as e:
                                        logger.warning(f"MEGA check failed for archive: {e}")
                                    
                                    if exists_in_mega:
                                        archive_exists_in_both = True
                        else:
                            logger.warning(f"Repository not initialized, skipping DB check")
                    except Exception as e:
                        logger.warning(f"Error checking archive existence: {e}", exc_info=True)
                elif pre_check_info:
                    # Use hash from batch check even if archive doesn't exist locally
                    archive_hash = pre_check_info.get('hash')
                    existing_source_id = pre_check_info.get('source_id')
                    existing_mega_handle = pre_check_info.get('mega_handle')
            
            # If archive exists in both DB and MEGA, skip processing (handler.py handles grid check)
            if archive_exists_in_both and existing_source_id:
                return (
                    UploadResult.ok(existing_source_id, set_name, existing_mega_handle, None),
                    []
                )
            cover = self._selector.select_cover(images)
            perceptual_features = await self._generate_thumbnails(set_folder, images, progress_callback)
            if progress_callback:
                progress_callback(f"Generating grid preview...", 25, 100)
            grid_preview_path = await self._generate_grid_preview(set_folder, len(images))
            
            if existing_source_id and not archive_exists_in_both:
                set_source_id = existing_source_id
                logger.debug(f"Using existing set source_id for re-upload: {set_source_id}")
            else:
                set_source_id = generate_id()
                logger.debug(f"Generated new set source_id: {set_source_id}")
            skip_image_processing = (existing_archive_path.exists() and existing_source_id and not archive_exists_in_both)
            
            image_results = []
            if not skip_image_processing:
                image_results = await self._process_set_images(
                    set_folder,
                    images,
                    set_source_id,
                    progress_callback,
                    perceptual_features=perceptual_features
                )
            else:
                logger.debug(f"Skipping image processing - using existing 7z file from files/ subfolder")
            
            # Step 4: Use existing 7z or create new one
            if existing_archive_path.exists() and existing_source_id and not archive_exists_in_both:
                # Use existing 7z file for upload (re-upload scenario)
                logger.debug(f"Using existing 7z archive from files/ subfolder: {existing_archive_path}")
                archive_files = [existing_archive_path]
            else:
                # Create 7z archive (normal flow when no existing archive)
                logger.info(f"Creating 7z archive: {archive_name}")
                if progress_callback:
                    progress_callback(f"Creating archive: {archive_name}...", 60, 100)
                
                archive_files = self._archiver.create(set_folder, archive_name)
                
                if not archive_files:
                    return (
                        UploadResult.fail(set_name, "Failed to create 7z archive"),
                        image_results
                    )
                
                logger.debug(f"Created {len(archive_files)} archive file(s)")
            
            if not archive_hash and archive_files and archive_files[0].exists():
                try:
                    logger.debug(f"Calculating hash for archive before saving: {archive_files[0].name}")
                    archive_hash = await blake3_file(archive_files[0])
                    logger.debug(f"Archive hash calculated: {archive_hash}")
                except Exception as e:
                    logger.warning(f"Failed to calculate archive hash: {e}")
            
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
                return (
                    UploadResult.fail(set_name, "Failed to upload 7z archive"),
                    image_results
                )
            
            # Step 6: Upload cover image (first vertical image) to same path
            cover_handle = None
            if cover and cover.exists():
                try:
                    logger.debug(f"Uploading cover image: {cover.name}")
                    if progress_callback:
                        progress_callback(f"Uploading cover: {cover.name}...", 95, 100)
                    
                    # Upload cover to same path as 7z with name {set_name}_cover.jpg
                    cover_filename = f"{set_name}.cover.jpg"
                    
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
                            logger.debug(f"Cover uploaded: {dest_path}/{cover_filename}")
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
                    logger.debug(f"Uploading grid preview: {set_name}.jpg")
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
                        logger.debug(f"Grid preview uploaded: {dest_path}/{grid_filename}")
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
            
            # Step 8: Save set document to API (with blake3_hash if available)
            await self._save_set_document(set_source_id, set_name, archive_name, len(images), archive_hash)
            
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
    
    @staticmethod
    def _process_single_image_static(
        image_path: Path,
        thumb_320_dir: Path,
        thumb_1024_dir: Path
    ) -> tuple:
        """
        Static function to process a single image (pickleable for ProcessPoolExecutor).
        
        Creates its own ImageProcessor instance to avoid serialization issues.
        Returns (success: bool, phash: str|None, avg_color_lab: list|None)
        """
        try:
            from mediakit.image.processor import ImageProcessor
            from mediakit.image.perceptual import calculate_phash, calculate_avg_color_lab
            from PIL import Image
            
            processor = ImageProcessor()
            output_name = image_path.stem + ".jpg"
            thumb_320_path = thumb_320_dir / output_name
            
            # Generate 320px thumbnail
            processor.thumb(image_path, thumb_320_path, 320)
            
            # Generate 1024px preview
            thumb_1024_path = thumb_1024_dir / output_name
            processor.resize(image_path, thumb_1024_path, 1024, resample=Image.Resampling.BICUBIC)
            
            # Calculate pHash and avg_color_lab using the 320px thumbnail (much faster!)
            phash_result = None
            avg_color_result = None
            
            try:
                # Use the thumbnail we just created for faster calculation
                with Image.open(thumb_320_path) as thumb_img:
                    # Calculate pHash using thumbnail
                    phash_result = calculate_phash(image=thumb_img)
                    # Calculate avg_color_lab using thumbnail
                    avg_color_result = calculate_avg_color_lab(image=thumb_img)
            except Exception as e:
                logger.debug(f"Could not calculate perceptual features from thumbnail for {image_path.name}: {e}")
                # Fallback: calculate from original image
                try:
                    phash_result = calculate_phash(image_path)
                    avg_color_result = calculate_avg_color_lab(image_path)
                except Exception:
                    pass
            
            return (True, phash_result, avg_color_result)
        except Exception as e:
            logger.warning(f"Error generating thumbnails for {image_path.name}: {e}")
            return (False, None, None)
    
    async def _generate_thumbnails(
        self,
        set_folder: Path,
        images: List[Path],
        progress_callback: Optional[Callable]
    ) -> Dict[Path, Tuple[Optional[str], Optional[List[float]]]]:
        """
        Generate 320px and 1024px thumbnails for all images using ProcessPoolExecutor.
        
        Also calculates pHash and avg_color_lab using the 320px thumbnails for efficiency.
        
        Returns:
            Dict mapping image_path -> (phash, avg_color_lab)
        """
        logger.debug(f"Generating thumbnails (320px and 1024px) for {len(images)} images...")
        
        # Create thumbnail folders
        thumb_320_dir = set_folder / "m"
        thumb_1024_dir = set_folder / "x"
        thumb_320_dir.mkdir(exist_ok=True)
        thumb_1024_dir.mkdir(exist_ok=True)
        
        # Process images in parallel using ProcessPoolExecutor to utilize all CPU cores
        import os
        from concurrent.futures import ProcessPoolExecutor
        
        total_images = len(images)
        
        max_workers = os.cpu_count() or os.getenv("IMAGE_RESIZE_MAX_WORKERS")
        logger.debug(f"Using ProcessPoolExecutor with {max_workers} workers for {total_images} images")
        perceptual_features = {}
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(
                    ImageSetProcessor._process_single_image_static,
                    img,
                    thumb_320_dir,
                    thumb_1024_dir
                ): img
                for img in images
            }
            
            loop = asyncio.get_event_loop()
            async_futures = {asyncio.wrap_future(f, loop=loop): img for f, img in futures.items()}
            
            batch_size = max(1, max_workers // 2)  # Update progress every N completions
            completed = 0
            
            for future, image_path in async_futures.items():
                try:
                    success, phash_result, avg_color_result = await future
                    completed += 1
                    
                    # Store perceptual features if calculated
                    if success and (phash_result or avg_color_result):
                        perceptual_features[image_path] = (phash_result, avg_color_result)
                    
                    # Update progress periodically
                    if progress_callback and (completed % batch_size == 0 or completed == total_images):
                        progress_callback(
                                    f"Generated thumbnails: {completed}/{total_images}",
                                    (completed * 30 // total_images) if total_images > 0 else 0,
                            100
                        )
                    
                except Exception as e:
                    logger.error(f"Error in thumbnail generation task: {e}")
                    completed += 1
        
        logger.info(f"Thumbnail generation complete: {completed}/{total_images} images processed")
        
        # Return perceptual features for use in _process_set_images
        return perceptual_features
    
    async def _generate_grid_preview(
        self,
        set_folder: Path,
        image_count: int
    ) -> Optional[Path]:
        """
        Generate grid preview for the set using mediakit.
        
        Args:
            set_folder: Path to the image set folder
            image_count: Total number of images (used for dynamic grid sizing)
        
        Returns:
            Path to generated grid preview image
        """
        try:
            loop = asyncio.get_event_loop()
            
            # Calculate dynamic rows/cols based on image count thresholds
            if image_count < 100:
                grid_dim = 3
            elif image_count < 300:
                grid_dim = 4
            elif image_count < 600:
                grid_dim = 5
            elif image_count < 1000:
                grid_dim = 6
            elif image_count < 2000:
                grid_dim = 7
            else:
                grid_dim = 8
            
            from mediakit.preview.image_preview import GridConfig
            config = GridConfig(rows=grid_dim, cols=grid_dim, cell_size=400)
            logger.debug(f"Using dynamic grid {grid_dim}x{grid_dim} for {image_count} images")
            
            # Generate grid preview using mediakit (synchronous operation)
            grid_path = await loop.run_in_executor(
                None,
                self._preview_generator.generate,
                set_folder,
                None,  # output_path=None means it will create a temp file
                config
            )
            
            if grid_path and grid_path.exists():
                logger.debug(f"Grid preview generated: {grid_path}")
                return grid_path
            else:
                logger.warning("Failed to generate grid preview")
                return None
                
        except Exception as e:
            logger.error(f"Error generating grid preview: {e}", exc_info=True)
            return None
    
    async def _process_set_images(
        self,
        set_folder: Path,
        images: List[Path],
        set_source_id: str,
        progress_callback: Optional[Callable],
        perceptual_features: Optional[Dict[Path, Tuple[Optional[str], Optional[List[float]]]]] = None
    ) -> List[UploadResult]:
        """
        Process all images in the set: analyze all first, then save to API in batch.
        
        Optimized to analyze all images first, then make a single batch POST instead
        of one POST per image. This significantly reduces HTTP requests.
        
        Args:
            perceptual_features: Optional dict mapping image_path -> (phash, avg_color_lab)
                If provided, these values will be used instead of recalculating them.
        """
        results = []
        batch_items = []  # Accumulate data for batch POST
        
        logger.debug(f"Processing {len(images)} images for set...")
        
        # Step 1: Analyze all images first (can be done in parallel if needed)
        analyzed_images = []
        for idx, image_path in enumerate(images, 1):
            try:
                if progress_callback:
                    progress_callback(
                        f"Analyzing image {idx}/{len(images)}: {image_path.name}",
                        30 + (idx * 30 // len(images)),
                        100
                    )
                
                # Get pre-calculated perceptual features if available (from thumbnail generation)
                phash_val = None
                avg_color_val = None
                if perceptual_features and image_path in perceptual_features:
                    phash_val, avg_color_val = perceptual_features[image_path]
                    logger.debug(f"Using pre-calculated perceptual features for {image_path.name}")
                
                # Analyze image (pass pre-calculated values to avoid recalculation)
                tech_data = await self._analyzer.analyze_photo_async(
                    image_path,
                    phash=phash_val,
                    avg_color_lab=avg_color_val
                )
                
                # Generate source_id for this image
                image_source_id = generate_id(12)
                tech_data["source_id"] = image_source_id
                
                # Add set_doc_id reference
                tech_data["set_doc_id"] = set_source_id
                
                analyzed_images.append((image_path, image_source_id, tech_data))
                
            except Exception as e:
                logger.error(f"Error analyzing image {image_path.name}: {e}")
                results.append(
                    UploadResult.fail(image_path.name, str(e))
                )
        
        # Step 2: Prepare batch data for all successfully analyzed images
        successful_images = []  # Track images that will be in batch
        for image_path, image_source_id, tech_data in analyzed_images:
            try:
                # Prepare document data
                document_data = self._repository.prepare_document(tech_data)
                
                # Prepare photo metadata
                photo_metadata = self._repository.prepare_photo_metadata(image_source_id, tech_data)
                
                # Add to batch
                batch_items.append({
                    "document": document_data,
                    "photo_metadata": photo_metadata
                })
                successful_images.append((image_path, image_source_id))
                
            except Exception as e:
                logger.error(f"Error preparing batch data for {image_path.name}: {e}")
                results.append(
                    UploadResult.fail(image_path.name, str(e))
                )
        
        # Step 3: Save all images in a single batch POST
        if batch_items:
            try:
                if progress_callback:
                    progress_callback(
                        f"Saving {len(batch_items)} images to API (batch)...",
                        60,
                        100
                    )
                
                logger.info(f"Saving {len(batch_items)} images to API in batch...")
                await self._repository.save_batch(batch_items)
                
                # Create success results for all successfully saved images
                for image_path, image_source_id in successful_images:
                    results.append(
                        UploadResult.ok(image_source_id, image_path.name, None, None)
                    )
                    logger.debug(f"Saved image {image_path.name} with set_doc_id={set_source_id}")
                
                logger.info(f"Successfully saved {len(batch_items)} images in batch")
                
            except Exception as e:
                logger.error(f"Error saving batch to API: {e}", exc_info=True)
                # If batch fails, mark all batch items as failed
                for image_path, image_source_id in successful_images:
                    results.append(
                        UploadResult.fail(image_path.name, f"Batch save failed: {str(e)}")
                    )
        
        logger.debug(f"Processed {len(results)} images for set ({len([r for r in results if r.success])} successful)")
        return results
    
    async def _save_set_document(
        self,
        set_source_id: str,
        set_name: str,
        archive_name: str,
        image_count: int,
        blake3_hash: Optional[str] = None
    ) -> None:
        """Save set document to API (the 7z archive document)."""
        set_doc = {
            "source_id": set_source_id,
            "filename": archive_name,
            "mimetype": "application/x-7z-compressed",
            "set_image_count": image_count,
            "set_name": set_name
        }
        
        # Include blake3_hash if available
        if blake3_hash:
            set_doc["blake3_hash"] = blake3_hash
            logger.debug(f"Including blake3_hash in set document: {blake3_hash[:16]}...")
            
        await self._repository.save_set_document(set_doc)
        logger.debug(f"Saved set document: {set_source_id}")

