"""Orchestrator data models."""
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, List
from ..models import UploadResult


@dataclass
class FolderUploadResult:
    """Result of folder upload."""
    success: bool
    folder_name: str
    total_files: int
    uploaded_files: int
    failed_files: int
    results: List[UploadResult]
    mega_folder_handle: Optional[str] = None
    error: Optional[str] = None
    
    @property
    def all_success(self) -> bool:
        return self.failed_files == 0


@dataclass
class UploadTask:
    """Task for parallel upload."""
    file_path: Path
    source_id: str
    tech_data: dict
    is_video: bool
    rel_path: Path
    mega_dest: str
    file_size: int = 0

