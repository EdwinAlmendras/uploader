"""
Set Processor - Processes image sets: thumbnails, 7z creation, and upload.

Uses mediakit for image processing and archive creation.
"""
from pathlib import Path
from typing import List, Optional, Tuple, Callable, Union, Dict
import asyncio
import logging
import tempfile
import os
from enum import IntEnum

from PIL import Image

from mediakit.set_processor import SetProcessor as MediakitSetProcessor
from mediakit.image.selector import ImageSelector
from mediakit.archive.sevenzip import SevenZipArchiver, ArchiveConfig
from mediakit.preview.image_preview import ImagePreviewGenerator

from uploader.services import AnalyzerService, MetadataRepository, StorageService
from uploader.orchestrator.models import UploadResult
from uploader.models import UploadConfig
from uploader.use_cases.deduplication import exists_in_mega_by_source_id
from mediakit.analyzer import generate_id
from mediakit.image.processor import ImageProcessor
from typing import Any

logger = logging.getLogger(__name__)
SET_IMAGE_SOURCE_ID_LENGTH = 16


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
        self._set_batch_retry_max = self._resolve_set_batch_retry_max()
    
    async def process_set(
        self,
        set_folder: Path,
        dest_path: str,
        progress_callback: Optional[Callable] = None,
        pre_check_info: Optional[Dict[str, Any]] = None
    ) -> Tuple[UploadResult, List[UploadResult]]:
        from uploader.use_cases.process_image_set import ProcessImageSetUseCase

        use_case = ProcessImageSetUseCase(self)
        return await use_case.execute(set_folder, dest_path, progress_callback, pre_check_info)

    async def _exists_in_mega(self, source_id: str) -> bool:
        """Check source existence in MEGA across storage implementations."""
        try:
            return await exists_in_mega_by_source_id(self._storage, source_id)
        except Exception as exc:
            logger.warning("MEGA check failed for archive source_id=%s: %s", source_id, exc)
        return False
    
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
        
        # Process images in parallel using ProcessPoolExecutor to utilize CPU cores.
        # If the runtime blocks process creation (common in restricted environments),
        # fall back to in-process execution with the same output contract.
        from concurrent.futures import ProcessPoolExecutor

        total_images = len(images)
        max_workers = self._resolve_thumbnail_workers()
        logger.debug(
            "Using ProcessPoolExecutor with %d workers for %d images",
            max_workers,
            total_images,
        )
        perceptual_features = {}
        completed = 0

        try:
            with ProcessPoolExecutor(max_workers=max_workers) as executor:
                futures = {
                    executor.submit(
                        ImageSetProcessor._process_single_image_static,
                        img,
                        thumb_320_dir,
                        thumb_1024_dir,
                    ): img
                    for img in images
                }

                loop = asyncio.get_event_loop()
                async_futures = {
                    asyncio.wrap_future(future, loop=loop): image_path
                    for future, image_path in futures.items()
                }

                batch_size = max(1, max_workers // 2)
                for future, image_path in async_futures.items():
                    try:
                        success, phash_result, avg_color_result = await future
                    except Exception as task_error:
                        logger.error("Thumbnail task failed for %s: %s", image_path.name, task_error)
                        success, phash_result, avg_color_result = False, None, None

                    completed += 1
                    if success and (phash_result or avg_color_result):
                        perceptual_features[image_path] = (phash_result, avg_color_result)
                    if progress_callback and (completed % batch_size == 0 or completed == total_images):
                        progress_callback(
                            f"Generated thumbnails: {completed}/{total_images}",
                            (completed * 30 // total_images) if total_images > 0 else 0,
                            100,
                        )
        except (PermissionError, OSError) as pool_error:
            logger.warning(
                "ProcessPoolExecutor unavailable (%s). Falling back to sequential thumbnail generation.",
                pool_error,
            )
            return await self._generate_thumbnails_sequential(
                images,
                thumb_320_dir,
                thumb_1024_dir,
                progress_callback,
            )
        
        logger.info(f"Thumbnail generation complete: {completed}/{total_images} images processed")
        
        # Return perceptual features for use in _process_set_images
        return perceptual_features

    def _resolve_thumbnail_workers(self) -> int:
        """Resolve worker count from env/cpu with safe bounds."""
        env_workers = os.getenv("IMAGE_RESIZE_MAX_WORKERS")
        if env_workers:
            try:
                parsed = int(env_workers)
                if parsed > 0:
                    return parsed
            except ValueError:
                logger.warning(
                    "Invalid IMAGE_RESIZE_MAX_WORKERS=%r. Falling back to cpu_count.",
                    env_workers,
                )
        cpu_count = os.cpu_count() or 1
        return max(1, cpu_count)

    async def _generate_thumbnails_sequential(
        self,
        images: List[Path],
        thumb_320_dir: Path,
        thumb_1024_dir: Path,
        progress_callback: Optional[Callable],
    ) -> Dict[Path, Tuple[Optional[str], Optional[List[float]]]]:
        """Fallback thumbnail generator used when process pools are unavailable."""
        total_images = len(images)
        completed = 0
        perceptual_features: Dict[Path, Tuple[Optional[str], Optional[List[float]]]] = {}
        loop = asyncio.get_event_loop()

        for image_path in images:
            try:
                success, phash_result, avg_color_result = await loop.run_in_executor(
                    None,
                    ImageSetProcessor._process_single_image_static,
                    image_path,
                    thumb_320_dir,
                    thumb_1024_dir,
                )
            except Exception as task_error:
                logger.error(
                    "Sequential thumbnail task failed for %s: %s",
                    image_path.name,
                    task_error,
                )
                success, phash_result, avg_color_result = False, None, None

            completed += 1
            if success and (phash_result or avg_color_result):
                perceptual_features[image_path] = (phash_result, avg_color_result)

            if progress_callback:
                progress_callback(
                    f"Generated thumbnails: {completed}/{total_images}",
                    (completed * 30 // total_images) if total_images > 0 else 0,
                    100,
                )

        logger.info(
            "Sequential thumbnail generation complete: %d/%d images processed",
            completed,
            total_images,
        )
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

            temp_dir = Path(tempfile.mkdtemp(prefix="preview_"))
            output_path = temp_dir / "grid.jpg"

            # Generate grid preview using mediakit (synchronous operation)
            grid_path = await loop.run_in_executor(
                None,
                self._preview_generator.generate,
                set_folder,
                output_path,
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
        Process all images in the set.

        Analysis runs first, then persistence happens via batch API with retry.
        On retry, only image source IDs are regenerated; set_source_id is fixed.
        
        Args:
            perceptual_features: Optional dict mapping image_path -> (phash, avg_color_lab)
                If provided, these values will be used instead of recalculating them.
        """
        results = []
        
        logger.debug(f"Processing {len(images)} images for set...")
        
        # Step 1: Analyze all images first (can be done in parallel if needed)
        analyzed_images: List[Tuple[Path, Dict[str, Any]]] = []
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
                
                # Analyze image (pass pre-calculated values when analyzer supports them)
                tech_data = await self._analyze_photo_with_optional_features(
                    image_path,
                    phash=phash_val,
                    avg_color_lab=avg_color_val,
                )
                
                # Generate source_id for this image
                image_source_id = generate_id(SET_IMAGE_SOURCE_ID_LENGTH)
                tech_data["source_id"] = image_source_id
                
                # Add set_doc_id reference
                tech_data["set_doc_id"] = set_source_id
                
                analyzed_images.append((image_path, tech_data))
                
            except Exception as e:
                logger.error(f"Error analyzing image {image_path.name}: {e}")
                results.append(
                    UploadResult.fail(image_path.name, str(e))
                )

        if analyzed_images:
            batch_results = await self._save_images_batch_with_retry(
                analyzed_images,
                set_source_id,
                progress_callback,
            )
            results.extend(batch_results)
        
        logger.debug(f"Processed {len(results)} images for set ({len([r for r in results if r.success])} successful)")
        return results

    def _resolve_set_batch_retry_max(self) -> int:
        raw = os.getenv("UPLOADER_SET_BATCH_RETRY_MAX", "3").strip()
        try:
            return max(1, int(raw))
        except ValueError:
            logger.warning(
                "Invalid UPLOADER_SET_BATCH_RETRY_MAX=%r. Using default=3.",
                raw,
            )
            return 3

    @staticmethod
    def _regenerate_image_ids(
        analyzed_images: List[Tuple[Path, Dict[str, Any]]],
        set_source_id: str,
    ) -> None:
        seen: set[str] = set()
        for _image_path, tech_data in analyzed_images:
            next_id = generate_id(SET_IMAGE_SOURCE_ID_LENGTH)
            while next_id in seen:
                next_id = generate_id(SET_IMAGE_SOURCE_ID_LENGTH)
            seen.add(next_id)
            tech_data["source_id"] = next_id
            tech_data["set_doc_id"] = set_source_id

    def _build_batch_items(
        self,
        analyzed_images: List[Tuple[Path, Dict[str, Any]]],
        set_source_id: str,
    ) -> Tuple[List[Dict[str, Any]], List[Tuple[Path, str]]]:
        batch_items: List[Dict[str, Any]] = []
        image_refs: List[Tuple[Path, str]] = []

        for image_path, tech_data in analyzed_images:
            image_source_id = str(tech_data.get("source_id") or "")
            if not image_source_id:
                image_source_id = generate_id(SET_IMAGE_SOURCE_ID_LENGTH)
                tech_data["source_id"] = image_source_id
            tech_data["set_doc_id"] = set_source_id
            document_data = self._repository.prepare_document(tech_data)
            photo_metadata = self._repository.prepare_photo_metadata(image_source_id, tech_data)
            batch_items.append(
                {
                    "document": document_data,
                    "photo_metadata": photo_metadata,
                }
            )
            image_refs.append((image_path, image_source_id))

        return batch_items, image_refs

    async def _save_images_batch_with_retry(
        self,
        analyzed_images: List[Tuple[Path, Dict[str, Any]]],
        set_source_id: str,
        progress_callback: Optional[Callable],
    ) -> List[UploadResult]:
        attempts = self._set_batch_retry_max
        last_error: Optional[Exception] = None

        for attempt in range(1, attempts + 1):
            batch_items, image_refs = self._build_batch_items(analyzed_images, set_source_id)
            if progress_callback:
                progress_callback(
                    f"Saving {len(batch_items)} images to API (batch attempt {attempt}/{attempts})...",
                    60,
                    100,
                )
            logger.info(
                "Saving %d images to API in batch (attempt %d/%d)",
                len(batch_items),
                attempt,
                attempts,
            )
            try:
                await self._repository.save_batch(batch_items)
                logger.info(
                    "Successfully saved %d images in batch on attempt %d/%d",
                    len(batch_items),
                    attempt,
                    attempts,
                )
                return [
                    UploadResult.ok(image_source_id, image_path.name, None, None)
                    for image_path, image_source_id in image_refs
                ]
            except Exception as exc:
                last_error = exc
                logger.error(
                    "Batch save failed on attempt %d/%d for set_doc_id=%s: %s",
                    attempt,
                    attempts,
                    set_source_id,
                    exc,
                    exc_info=True,
                )
                if attempt >= attempts:
                    break
                self._regenerate_image_ids(analyzed_images, set_source_id)
                logger.warning(
                    "Retrying batch save with regenerated image IDs for set_doc_id=%s",
                    set_source_id,
                )

        message = str(last_error) if last_error else "unknown batch error"
        return [
            UploadResult.fail(
                image_path.name,
                f"Batch save failed after {attempts} attempts: {message}",
            )
            for image_path, _tech_data in analyzed_images
        ]

    async def _analyze_photo_with_optional_features(
        self,
        image_path: Path,
        phash: Optional[str],
        avg_color_lab: Optional[List[float]],
    ) -> Dict[str, Any]:
        """
        Call analyzer with optional perceptual features and gracefully degrade
        to legacy analyzer signatures.
        """
        analyze_photo = self._analyzer.analyze_photo_async
        try:
            return await analyze_photo(
                image_path,
                phash=phash,
                avg_color_lab=avg_color_lab,
            )
        except TypeError as exc:
            # Legacy analyzers may not accept phash/avg_color_lab kwargs.
            if "unexpected keyword argument" not in str(exc):
                raise
            logger.debug(
                "Analyzer does not accept perceptual kwargs for %s; retrying legacy call.",
                image_path.name,
            )
            tech_data = await analyze_photo(image_path)
            if not isinstance(tech_data, dict):
                return tech_data
            if phash and "phash" not in tech_data:
                tech_data["phash"] = phash
            if avg_color_lab and "avg_color_lab" not in tech_data:
                tech_data["avg_color_lab"] = avg_color_lab
            return tech_data
    
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

