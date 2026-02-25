"""Use case: process one image set end-to-end."""
from __future__ import annotations

from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

import logging
import shutil
import tempfile

from mediakit.analyzer import generate_id

from uploader.orchestrator.models import UploadResult
from uploader.services.resume import blake3_file
from uploader.use_cases.deduplication import parse_repository_doc_info

logger = logging.getLogger(__name__)


class ProcessImageSetUseCase:
    """Runs the full set pipeline using an ImageSetProcessor instance."""

    def __init__(self, processor: Any):
        self._processor = processor

    async def execute(
        self,
        set_folder: Path,
        dest_path: str,
        progress_callback: Optional[Callable] = None,
        pre_check_info: Optional[Dict[str, Any]] = None,
    ) -> Tuple[UploadResult, List[UploadResult]]:
        set_name = set_folder.name
        logger.debug("Processing image set: %s", set_name)

        if progress_callback:
            progress_callback(f"Processing set: {set_name}...", 0, 100)

        try:
            images = self._processor._selector.get_images(set_folder)
            if not images:
                return (UploadResult.fail(set_name, "No images found in set"), [])

            existing_source_id = None
            existing_mega_handle = None
            archive_exists_in_both = False
            archive_hash = None
            archive_name = f"{set_name}.7z"
            existing_archive_path = set_folder.parent / "files" / archive_name

            if pre_check_info:
                existing_source_id = pre_check_info.get("source_id")
                existing_mega_handle = pre_check_info.get("mega_handle")
                archive_hash = pre_check_info.get("hash")
                archive_exists_in_both = bool(pre_check_info.get("exists_in_both"))

            if archive_exists_in_both and existing_source_id:
                return (UploadResult.ok(existing_source_id, set_name, existing_mega_handle, None), [])

            if existing_source_id and not archive_exists_in_both:
                set_source_id = existing_source_id
                logger.debug("Using existing set source_id for re-upload: %s", set_source_id)
            else:
                set_source_id = generate_id()
                logger.debug("Generated new set source_id: %s", set_source_id)

            # Ensure archive exists first, so hash dedup can happen before expensive image processing.
            if existing_archive_path.exists():
                archive_files = [existing_archive_path]
            else:
                logger.info("Creating 7z archive: %s", archive_name)
                if progress_callback:
                    progress_callback(f"Creating archive: {archive_name}...", 10, 100)
                archive_files = self._processor._archiver.create(set_folder, archive_name)
                if not archive_files:
                    return (UploadResult.fail(set_name, "Failed to create 7z archive"), [])
                logger.debug("Created %d archive file(s)", len(archive_files))

            if not archive_hash and archive_files and archive_files[0].exists():
                try:
                    logger.debug("Calculating hash for archive before saving: %s", archive_files[0].name)
                    if progress_callback:
                        progress_callback("Calculating archive hash...", 20, 100)
                    archive_hash = await blake3_file(archive_files[0])
                    logger.debug("Archive hash calculated: %s", archive_hash)
                except Exception as exc:
                    logger.warning("Failed to calculate archive hash: %s", exc)

            if (
                not archive_exists_in_both
                and archive_hash
                and self._processor._repository
            ):
                try:
                    existing_hashes = await self._processor._repository.check_exists_batch([archive_hash])
                    doc_info = existing_hashes.get(archive_hash)
                    if doc_info:
                        detected_source_id, detected_mega_handle = parse_repository_doc_info(doc_info)
                        if detected_source_id:
                            existing_source_id = detected_source_id
                            existing_mega_handle = detected_mega_handle or existing_mega_handle
                            exists_in_mega = await self._processor._exists_in_mega(existing_source_id)
                            if exists_in_mega:
                                logger.info(
                                    "Set archive already exists in DB and MEGA (source_id=%s), skipping set %s",
                                    existing_source_id,
                                    set_name,
                                )
                                return (
                                    UploadResult.ok(existing_source_id, set_name, existing_mega_handle, None),
                                    [],
                                )
                            # Reuse existing set source_id if DB has the archive but MEGA does not.
                            set_source_id = existing_source_id
                except Exception as exc:
                    logger.warning("Archive hash dedup check failed for %s: %s", set_name, exc, exc_info=True)

            skip_image_processing = bool(existing_source_id)
            image_results: List[UploadResult] = []
            cover = self._processor._selector.select_cover(images)
            grid_preview_path: Optional[Path] = None
            if not skip_image_processing:
                perceptual_features = await self._processor._generate_thumbnails(
                    set_folder,
                    images,
                    progress_callback,
                )
                if progress_callback:
                    progress_callback("Generating grid preview...", 40, 100)
                grid_preview_path = await self._processor._generate_grid_preview(set_folder, len(images))
                image_results = await self._processor._process_set_images(
                    set_folder,
                    images,
                    set_source_id,
                    progress_callback,
                    perceptual_features=perceptual_features,
                )
            else:
                logger.debug(
                    "Skipping image metadata processing for set %s (existing source_id=%s)",
                    set_name,
                    existing_source_id,
                )
                if progress_callback:
                    progress_callback("Generating grid preview...", 40, 100)
                grid_preview_path = await self._processor._generate_grid_preview(set_folder, len(images))

            mega_handles = []
            for idx, archive_file in enumerate(archive_files, 1):
                logger.info(
                    "Uploading archive part %d/%d: %s",
                    idx,
                    len(archive_files),
                    archive_file.name,
                )
                if progress_callback:
                    progress_callback(
                        f"Uploading archive {idx}/{len(archive_files)}...",
                        70 + (idx * 10 // len(archive_files)),
                        100,
                    )
                try:
                    handle = await self._processor._storage.upload_video(
                        archive_file,
                        dest_path,
                        set_source_id,
                        progress_callback,
                    )
                    if handle:
                        mega_handles.append(handle)
                        logger.info("Uploaded archive part %d: %s", idx, handle)
                    else:
                        logger.error("Failed to upload archive part %d", idx)
                except Exception as exc:
                    logger.error("Error uploading archive part %d: %s", idx, exc)

            if not mega_handles:
                return (UploadResult.fail(set_name, "Failed to upload 7z archive"), image_results)

            if cover and cover.exists():
                try:
                    logger.debug("Uploading cover image: %s", cover.name)
                    if progress_callback:
                        progress_callback(f"Uploading cover: {cover.name}...", 95, 100)
                    cover_filename = f"{set_name}_cover.jpg"
                    temp_cover_dir = Path(tempfile.mkdtemp(prefix="cover_"))
                    temp_cover_path = temp_cover_dir / "cover.jpg"
                    shutil.copy2(cover, temp_cover_path)
                    try:
                        cover_handle = await self._processor._storage.upload_preview(
                            temp_cover_path,
                            dest_path=dest_path,
                            filename=cover_filename,
                        )
                        if cover_handle:
                            logger.debug("Cover uploaded: %s/%s", dest_path, cover_filename)
                        else:
                            logger.warning("Failed to upload cover image")
                    finally:
                        try:
                            temp_cover_path.unlink(missing_ok=True)
                        except OSError as cleanup_error:
                            logger.debug(
                                "Could not remove temporary cover %s: %s",
                                temp_cover_path,
                                cleanup_error,
                            )
                        try:
                            temp_cover_dir.rmdir()
                        except OSError:
                            pass
                except Exception as exc:
                    logger.warning("Error uploading cover: %s", exc)

            if grid_preview_path and grid_preview_path.exists():
                try:
                    logger.debug("Uploading grid preview: %s.jpg", set_name)
                    if progress_callback:
                        progress_callback("Uploading grid preview...", 96, 100)
                    grid_filename = f"{set_name}.jpg"
                    grid_handle = await self._processor._storage.upload_preview(
                        grid_preview_path,
                        dest_path=dest_path,
                        filename=grid_filename,
                    )
                    if grid_handle:
                        logger.debug("Grid preview uploaded: %s/%s", dest_path, grid_filename)
                    else:
                        logger.warning("Failed to upload grid preview")
                except Exception as exc:
                    logger.warning("Error uploading grid preview: %s", exc)
                finally:
                    if grid_preview_path and grid_preview_path.exists():
                        try:
                            grid_preview_path.unlink(missing_ok=True)
                            parent_name = grid_preview_path.parent.name
                            if (
                                parent_name.startswith("tmp")
                                or parent_name.startswith("preview_")
                                or "temp" in str(grid_preview_path.parent)
                            ):
                                try:
                                    shutil.rmtree(grid_preview_path.parent, ignore_errors=True)
                                except OSError as cleanup_error:
                                    logger.debug(
                                        "Could not remove temporary grid folder %s: %s",
                                        grid_preview_path.parent,
                                        cleanup_error,
                                    )
                        except OSError as cleanup_error:
                            logger.debug(
                                "Could not remove temporary grid file %s: %s",
                                grid_preview_path,
                                cleanup_error,
                            )

            await self._processor._save_set_document(
                set_source_id,
                set_name,
                archive_name,
                len(images),
                archive_hash,
            )
            logger.info("Successfully processed set: %s", set_name)

            return (UploadResult.ok(set_source_id, set_name, mega_handles[0], None), image_results)
        except Exception as exc:
            error_msg = str(exc) or f"{type(exc).__name__}"
            logger.error("Error processing set %s: %s", set_name, error_msg, exc_info=True)
            return (UploadResult.fail(set_name, error_msg), [])
