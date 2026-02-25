"""Folder upload handler - thin coordinator around workflow/use-cases."""
from __future__ import annotations

from pathlib import Path
from typing import Callable, Optional
import os
import logging

from uploader.models import UploadConfig
from uploader.orchestrator.file_collector import FileCollector
from uploader.orchestrator.models import FolderUploadResult
from uploader.orchestrator.preview_handler import PreviewHandler
from uploader.services.analyzer import AnalyzerService
from uploader.services.hash_cache import HashCache, get_hash_cache
from uploader.services.repository import MetadataRepository
from uploader.services.storage import StorageService

from .file_existence import Blake3Deduplicator, FileExistenceChecker, MegaToDbSynchronizer, PreviewChecker
from .file_processor import FileProcessor
from .parallel_upload import ParallelUploadCoordinator
from .process import FolderUploadProcess
from .set_processor import ImageSetProcessor
from .upload_workflow import FolderUploadWorkflow

logger = logging.getLogger(__name__)


class FolderUploadHandler:
    """Coordinates folder uploads while delegating business flow to a workflow."""

    def __init__(
        self,
        analyzer: AnalyzerService,
        repository: MetadataRepository,
        storage: StorageService,
        preview_handler: PreviewHandler,
        config: UploadConfig,
    ):
        self._file_collector = FileCollector()
        self._preview_handler = preview_handler
        self._file_processor = FileProcessor(analyzer, repository, storage, preview_handler, config)
        self._set_processor = ImageSetProcessor(analyzer, repository, storage, config)
        self._existence_checker = FileExistenceChecker(storage)
        self._blake3_deduplicator = Blake3Deduplicator(repository, storage) if repository else None
        self._preview_checker = PreviewChecker(storage, preview_handler, analyzer) if repository else None
        self._mega_to_db_synchronizer = MegaToDbSynchronizer(analyzer, repository, storage) if repository else None
        self._upload_coordinator = ParallelUploadCoordinator(self._file_processor, max_parallel=1)
        self._storage = storage
        self._repository = repository
        self._hash_cache: Optional[HashCache] = None
        self._analyzer = analyzer
        self._config = config

    @staticmethod
    def _skip_hash_check() -> bool:
        raw = os.getenv("UPLOADER_SKIP_HASH_CHECK", "").strip().lower()
        return raw in {"1", "true", "yes", "on"}

    def upload_folder(self, folder_path: Path, dest: Optional[str] = None) -> FolderUploadProcess:
        return FolderUploadProcess(self, folder_path, dest)

    async def _upload_folder_internal(
        self,
        folder_path: Path,
        dest: Optional[str],
        progress_callback: Optional[Callable[[str, int, int], None]],
        process: Optional[FolderUploadProcess],
    ) -> FolderUploadResult:
        if self._hash_cache is None:
            self._hash_cache = await get_hash_cache()

        workflow = FolderUploadWorkflow(
            file_collector=self._file_collector,
            set_processor=self._set_processor,
            upload_coordinator=self._upload_coordinator,
            storage=self._storage,
            repository=self._repository,
            preview_handler=self._preview_handler,
            analyzer=self._analyzer,
            blake3_deduplicator=self._blake3_deduplicator,
            preview_checker=self._preview_checker,
            config=self._config,
            hash_cache=self._hash_cache,
            skip_hash_check=self._skip_hash_check,
        )
        result = await workflow.execute(folder_path, dest, progress_callback, process)
        if self._hash_cache:
            await self._hash_cache.save()
        return result
