"""Folder upload workflow orchestration."""
from __future__ import annotations

from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, TYPE_CHECKING
import traceback
import logging

from uploader.models import UploadConfig, UploadResult
from uploader.orchestrator.models import FolderUploadResult
from uploader.services.hash_cache import HashCache

from .pipeline_deduplicator import PipelineDeduplicator
from .process import ProcessPhase
from .set_deduplication import SetBatchDeduplicator

if TYPE_CHECKING:
    from uploader.orchestrator.file_collector import FileCollector
    from uploader.orchestrator.preview_handler import PreviewHandler
    from uploader.services.analyzer import AnalyzerService
    from uploader.services.repository import MetadataRepository
    from uploader.services.storage import StorageService
    from .file_existence import Blake3Deduplicator, PreviewChecker
    from .parallel_upload import ParallelUploadCoordinator
    from .set_processor import ImageSetProcessor
    from .process import FolderUploadProcess

logger = logging.getLogger(__name__)


class FolderUploadWorkflow:
    """Executes the folder upload workflow using injected collaborators."""

    def __init__(
        self,
        file_collector: "FileCollector",
        set_processor: "ImageSetProcessor",
        upload_coordinator: "ParallelUploadCoordinator",
        storage: "StorageService",
        repository: Optional["MetadataRepository"],
        preview_handler: "PreviewHandler",
        analyzer: "AnalyzerService",
        blake3_deduplicator: Optional["Blake3Deduplicator"],
        preview_checker: Optional["PreviewChecker"],
        config: UploadConfig,
        hash_cache: Optional[HashCache],
        skip_hash_check: Callable[[], bool],
    ):
        self._file_collector = file_collector
        self._set_processor = set_processor
        self._upload_coordinator = upload_coordinator
        self._storage = storage
        self._repository = repository
        self._preview_handler = preview_handler
        self._analyzer = analyzer
        self._blake3_deduplicator = blake3_deduplicator
        self._preview_checker = preview_checker
        self._config = config
        self._hash_cache = hash_cache
        self._skip_hash_check = skip_hash_check

    async def execute(
        self,
        folder_path: Path,
        dest: Optional[str],
        progress_callback: Optional[Callable[[str, int, int], None]],
        process: Optional["FolderUploadProcess"],
    ) -> FolderUploadResult:
        folder_path = Path(folder_path)
        if not folder_path.is_dir():
            return FolderUploadResult(
                success=False,
                folder_name=folder_path.name,
                total_files=0,
                uploaded_files=0,
                failed_files=0,
                results=[],
                error=f"Not a directory: {folder_path}",
            )

        total = 0
        root_handle: Optional[str] = None

        try:
            if process and process.is_cancelled:
                return FolderUploadResult(
                    success=False,
                    folder_name=folder_path.name,
                    total_files=0,
                    uploaded_files=0,
                    failed_files=0,
                    results=[],
                    error="Process was cancelled",
                )

            dest_path = f"{dest}/{folder_path.name}" if dest else folder_path.name
            if process:
                await process.set_phase(ProcessPhase.DETECTING, "Detecting files and sets...")

            sets, individual_files = self._file_collector.detect_sets_and_files(folder_path)
            logger.info(
                "Found %d image set(s) and %d individual file(s)",
                len(sets),
                len(individual_files),
            )

            if process:
                await process.complete_phase(
                    "detecting",
                    f"Found {len(sets)} sets, {len(individual_files)} files",
                )

            total = len(sets) + len(individual_files)
            if process:
                async with process._stats_lock:
                    process._stats["total_files"] = total
                    process._stats["sets_count"] = len(sets)
                await process._events.emit("progress", process.stats)

            if total == 0:
                return FolderUploadResult(
                    success=True,
                    folder_name=folder_path.name,
                    total_files=0,
                    uploaded_files=0,
                    failed_files=0,
                    results=[],
                    error="No media files or sets found in folder",
                )

            all_results: List[UploadResult] = []

            logger.info("Pre-creating folder structure...")
            root_handle = await self._storage.create_folder(dest_path)
            required_paths = self._collect_required_subfolders(
                folder_path,
                dest_path,
                sets,
                individual_files,
            )
            if required_paths:
                await self._create_subfolders(required_paths)

            if sets:
                await self._process_sets_phase(
                    sets=sets,
                    folder_path=folder_path,
                    dest_path=dest_path,
                    total=total,
                    all_results=all_results,
                    progress_callback=progress_callback,
                    process=process,
                )

            if individual_files and (not process or not process.is_cancelled):
                await self._process_individual_files_phase(
                    individual_files=individual_files,
                    folder_path=folder_path,
                    dest_path=dest_path,
                    total=total,
                    all_results=all_results,
                    progress_callback=progress_callback,
                    process=process,
                )

            uploaded = sum(1 for result in all_results if result.success)
            failed = sum(1 for result in all_results if not result.success)

            if process:
                await process.set_phase(ProcessPhase.COMPLETED, "Upload complete")
                async with process._stats_lock:
                    process._stats["uploaded"] = uploaded
                    process._stats["failed"] = failed
                await process._events.emit("progress", process.stats)

            if self._hash_cache:
                await self._hash_cache.save()

            return FolderUploadResult(
                success=failed == 0,
                folder_name=folder_path.name,
                total_files=total,
                uploaded_files=uploaded,
                failed_files=failed,
                results=all_results,
                mega_folder_handle=root_handle,
            )
        except Exception as exc:
            error_msg = str(exc)
            logger.error(traceback.format_exc())
            logger.error("Error uploading folder %s: %s", folder_path, error_msg)
            if process:
                await process._events.emit("error", exc)
            return FolderUploadResult(
                success=False,
                folder_name=folder_path.name,
                total_files=total,
                uploaded_files=0,
                failed_files=total,
                results=[],
                error=error_msg,
            )

    def _collect_required_subfolders(
        self,
        folder_path: Path,
        dest_path: str,
        sets: List[Path],
        individual_files: List[Path],
    ) -> Set[str]:
        unique_subfolders: Set[str] = set()

        for set_folder in sets:
            set_rel_path = set_folder.relative_to(folder_path)
            if set_rel_path.parent != Path("."):
                parts = set_rel_path.parent.parts
                for i in range(1, len(parts) + 1):
                    unique_subfolders.add(f"{dest_path}/{'/'.join(parts[:i])}")

        for file_path in individual_files:
            rel_path = file_path.relative_to(folder_path)
            if rel_path.parent != Path("."):
                parts = rel_path.parent.parts
                for i in range(1, len(parts) + 1):
                    unique_subfolders.add(f"{dest_path}/{'/'.join(parts[:i])}")

        return unique_subfolders

    async def _create_subfolders(self, unique_subfolders: Set[str]) -> None:
        logger.info(
            "Creating %d unique subfolder(s) before processing...",
            len(unique_subfolders),
        )
        for subfolder_path in sorted(unique_subfolders):
            try:
                await self._storage.create_folder(subfolder_path)
            except Exception as exc:
                logger.debug("Folder %s might already exist or failed: %s", subfolder_path, exc)

    async def _process_sets_phase(
        self,
        sets: List[Path],
        folder_path: Path,
        dest_path: str,
        total: int,
        all_results: List[UploadResult],
        progress_callback: Optional[Callable[[str, int, int], None]],
        process: Optional["FolderUploadProcess"],
    ) -> None:
        logger.info("Processing %d image set(s)...", len(sets))
        if process:
            await process.set_phase(
                ProcessPhase.CHECKING_MEGA,
                f"Checking {len(sets)} set(s) in DB/MEGA...",
            )
        set_check_results = await self._batch_check_sets(sets)
        if process:
            await process.complete_phase("checking_mega", f"Checked {len(sets)} set(s)")
            await process.set_phase(
                ProcessPhase.UPLOADING,
                f"Processing {len(sets)} set(s)...",
            )

        for idx, set_folder in enumerate(sets, 1):
            if process and process.is_cancelled:
                logger.info("Upload cancelled by user")
                break

            logger.info("Processing set %d/%d: %s", idx, len(sets), set_folder.name)
            if progress_callback:
                progress_callback(
                    f"Processing set {idx}/{len(sets)}: {set_folder.name}...",
                    idx - 1,
                    total,
                )
            if process:
                await process.emit_phase_progress(
                    "uploading",
                    f"Set {idx}/{len(sets)}: {set_folder.name}",
                    idx - 1,
                    len(sets),
                    set_folder.name,
                )

            set_rel_path = set_folder.relative_to(folder_path)
            if set_rel_path.parent != Path("."):
                set_dest_path = f"{dest_path}/{set_rel_path.parent.as_posix()}"
            else:
                set_dest_path = dest_path

            set_check_info = set_check_results.get(set_folder, {})
            if set_check_info.get("exists_in_both"):
                existing_source_id = set_check_info.get("source_id")
                existing_mega_handle = set_check_info.get("mega_handle")
                if existing_source_id:
                    await self._ensure_grid_preview_for_existing_set(set_folder, existing_source_id)
                all_results.append(
                    UploadResult.ok(
                        existing_source_id or "",
                        set_folder.name,
                        existing_mega_handle or "",
                        None,
                    )
                )
                if process:
                    await process.emit_phase_progress(
                        "uploading",
                        f"Set {idx}/{len(sets)} done (already exists): {set_folder.name}",
                        idx,
                        len(sets),
                        set_folder.name,
                    )
                continue

            try:
                set_result, image_results = await self._set_processor.process_set(
                    set_folder,
                    set_dest_path,
                    progress_callback,
                    pre_check_info=set_check_info if set_check_info else None,
                )
                all_results.append(set_result)
                all_results.extend(image_results)
                if process:
                    async with process._stats_lock:
                        process._stats["uploaded"] = sum(1 for item in all_results if item.success)
                        process._stats["failed"] = sum(1 for item in all_results if not item.success)
                    await process._events.emit("progress", process.stats)
                    await process.emit_phase_progress(
                        "uploading",
                        f"Set {idx}/{len(sets)} done: {set_folder.name}",
                        idx,
                        len(sets),
                        set_folder.name,
                    )
            except Exception as exc:
                logger.error("Error processing set %s: %s", set_folder.name, exc, exc_info=True)
                all_results.append(UploadResult.fail(set_folder.name, str(exc)))
                if process:
                    await process.emit_phase_progress(
                        "uploading",
                        f"Set {idx}/{len(sets)} failed: {set_folder.name}",
                        idx,
                        len(sets),
                        set_folder.name,
                    )

    async def _process_individual_files_phase(
        self,
        individual_files: List[Path],
        folder_path: Path,
        dest_path: str,
        total: int,
        all_results: List[UploadResult],
        progress_callback: Optional[Callable[[str, int, int], None]],
        process: Optional["FolderUploadProcess"],
    ) -> None:
        logger.info("Processing %d individual file(s)...", len(individual_files))

        skipped_hash = 0
        files_to_check_db = individual_files
        if files_to_check_db and self._blake3_deduplicator and not self._skip_hash_check():
            if process:
                await process.set_phase(ProcessPhase.HASHING, "Calculating hashes...")
            pipeline = PipelineDeduplicator(
                self._repository,
                self._storage,
                hash_cache=self._hash_cache,
                preview_handler=self._preview_handler,
                analyzer=self._analyzer if self._preview_checker else None,
            )
            if process:
                self._wire_pipeline_callbacks(pipeline, process, len(files_to_check_db))

            files_with_rel = [(file_path, file_path.relative_to(folder_path)) for file_path in files_to_check_db]
            pending_files, skipped_paths_set, _, _ = await pipeline.process(files_with_rel, progress_callback)
            skipped_hash = len(skipped_paths_set)

            if self._hash_cache:
                await self._hash_cache.save()

            if process:
                await process.complete_phase("hashing", f"{skipped_hash} duplicates found")
        elif files_to_check_db and self._skip_hash_check():
            logger.warning(
                "Skipping blake3 deduplication for folder files (UPLOADER_SKIP_HASH_CHECK is enabled)."
            )
            pending_files = [(file_path, file_path.relative_to(folder_path)) for file_path in files_to_check_db]
        else:
            pending_files = [(file_path, file_path.relative_to(folder_path)) for file_path in files_to_check_db]
            if not self._blake3_deduplicator:
                logger.warning("Blake3Deduplicator not available, skipping database check")

        total_skipped = skipped_hash
        logger.info(
            "Existence check summary: %d individual files, %d skipped by blake3_hash, "
            "%d total skipped, %d files to upload",
            len(individual_files),
            skipped_hash,
            total_skipped,
            len(pending_files),
        )
        if process:
            async with process._stats_lock:
                process._stats["skipped"] = total_skipped
            await process._events.emit("progress", process.stats)

        if pending_files and self._supports_total_space_check() and not self._config.skip_space_check:
            if process:
                await process.set_phase(ProcessPhase.CHECKING_SPACE, "Checking storage space...")
            total_size_to_upload = sum(file_path.stat().st_size for file_path, _ in pending_files)
            total_size_gb = total_size_to_upload / (1024 ** 3)
            logger.info(
                "Total size to upload: %.2f GB (%d files)",
                total_size_gb,
                len(pending_files),
            )
            has_space = await self._storage.check_total_available_space(total_size_to_upload)
            if not has_space:
                error_msg = (
                    f"No storage space available for {total_size_gb:.2f} GB. "
                    "Please free up space or add a new MEGA account."
                )
                logger.error(error_msg)
                if process:
                    await process.complete_phase("checking_space", "Insufficient space")
                raise RuntimeError(error_msg)
            if process:
                await process.complete_phase("checking_space", f"{total_size_gb:.2f} GB available")

        if pending_files:
            if process:
                await process.set_phase(ProcessPhase.UPLOADING, f"Uploading {len(pending_files)} files...")
            file_results = await self._upload_coordinator.upload(
                pending_files,
                dest_path,
                total,
                progress_callback,
                process,
            )
            all_results.extend(file_results)

    def _wire_pipeline_callbacks(
        self,
        pipeline: PipelineDeduplicator,
        process: "FolderUploadProcess",
        total_files: int,
    ) -> None:
        async def on_hash_start(filename):
            await process.emit_hash_start(filename)

        hash_count = [0]

        async def on_hash_complete(filename, hash_val, from_cache):
            hash_count[0] += 1
            await process.emit_hash_complete(
                filename,
                hash_count[0],
                total_files,
                from_cache,
                hash_val,
            )
            await process.emit_phase_progress(
                "hashing",
                f"{'Cache' if from_cache else 'Calculated'}: {filename}",
                hash_count[0],
                total_files,
                filename,
                from_cache,
            )

        async def on_check_complete(filename, exists_in_db, exists_in_mega):
            await process._events.emit("check_complete", filename, exists_in_db, exists_in_mega)

        def schedule(coro):
            import asyncio

            try:
                loop = asyncio.get_running_loop()
                loop.create_task(coro)
            except RuntimeError:
                pass

        pipeline.on_hash_start(lambda filename: schedule(on_hash_start(filename)))
        pipeline.on_hash_complete(
            lambda filename, hash_val, from_cache: schedule(
                on_hash_complete(filename, hash_val, from_cache)
            )
        )
        pipeline.on_check_complete(
            lambda filename, exists_in_db, exists_in_mega: schedule(
                on_check_complete(filename, exists_in_db, exists_in_mega)
            )
        )

    def _supports_total_space_check(self) -> bool:
        return callable(getattr(self._storage, "check_total_available_space", None))

    async def _resolve_archive_mega_path(self, source_id: str) -> Optional[str]:
        manager = getattr(self._storage, "manager", None)
        if not manager or not hasattr(manager, "find_by_mega_id"):
            return None
        try:
            archive_node_info = await manager.find_by_mega_id(source_id)
        except Exception as exc:
            logger.debug("Failed to resolve archive by source_id=%s: %s", source_id, exc)
            return None
        if not archive_node_info:
            return None
        _, archive_node = archive_node_info
        return getattr(archive_node, "path", None)

    async def _ensure_grid_preview_for_existing_set(self, set_folder: Path, source_id: str) -> None:
        archive_mega_path = await self._resolve_archive_mega_path(source_id)
        if not archive_mega_path:
            return
        grid_mega_path = archive_mega_path.rsplit(".", 1)[0] + ".jpg"
        try:
            grid_exists = await self._storage.exists(grid_mega_path)
        except Exception as exc:
            logger.debug("Failed checking grid preview existence for %s: %s", set_folder.name, exc)
            return
        if grid_exists:
            return
        images = self._set_processor._selector.get_images(set_folder)
        grid_preview_path = await self._set_processor._generate_grid_preview(set_folder, len(images))
        if not grid_preview_path or not grid_preview_path.exists():
            return
        path_parts = archive_mega_path.rsplit("/", 1)
        archive_dest_path = path_parts[0] if len(path_parts) == 2 else None
        grid_filename = f"{set_folder.name}.jpg"
        try:
            await self._storage.upload_preview(
                grid_preview_path,
                dest_path=archive_dest_path,
                filename=grid_filename,
            )
            logger.info("Grid preview regenerated for '%s'", set_folder.name)
        except Exception as exc:
            logger.warning("Error uploading grid preview for %s: %s", set_folder.name, exc)
        finally:
            try:
                grid_preview_path.unlink(missing_ok=True)
            except OSError as cleanup_exc:
                logger.debug(
                    "Failed cleanup for temporary grid preview %s: %s",
                    grid_preview_path,
                    cleanup_exc,
                )

    async def _batch_check_sets(self, sets: List[Path]) -> Dict[Path, Dict[str, Any]]:
        deduplicator = SetBatchDeduplicator(
            repository=self._repository,
            storage=self._storage,
            hash_cache=self._hash_cache,
            skip_hash_check=self._skip_hash_check,
        )
        return await deduplicator.check_sets(sets)
