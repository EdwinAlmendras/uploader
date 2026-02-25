"""Application use cases for uploader workflows."""

from .file_processing import (
    AnalyzeFileUseCase,
    PersistFileMetadataUseCase,
    ResolveMegaDestinationUseCase,
    ResolvePreviewRequestUseCase,
    UploadFileUseCase,
    UploadPreviewUseCase,
)
from .process_image_set import ProcessImageSetUseCase
from .deduplication import (
    DedupDecision,
    ResolveDedupActionUseCase,
    ResolveFileHashUseCase,
    exists_in_mega_by_source_id,
    parse_repository_doc_info,
)
from .single_upload import (
    AnalyzeMediaUseCase,
    DetectMediaKindUseCase,
    ParallelAnalyzeAndUploadUseCase,
    SingleFileUploadUseCase,
)

__all__ = [
    "AnalyzeFileUseCase",
    "PersistFileMetadataUseCase",
    "ResolveMegaDestinationUseCase",
    "ResolvePreviewRequestUseCase",
    "UploadFileUseCase",
    "UploadPreviewUseCase",
    "ProcessImageSetUseCase",
    "DedupDecision",
    "ResolveDedupActionUseCase",
    "ResolveFileHashUseCase",
    "exists_in_mega_by_source_id",
    "parse_repository_doc_info",
    "AnalyzeMediaUseCase",
    "DetectMediaKindUseCase",
    "ParallelAnalyzeAndUploadUseCase",
    "SingleFileUploadUseCase",
]
