"""Folder upload handler - handles batch folder uploads."""
import asyncio
from pathlib import Path
from typing import Optional, List, Callable, Tuple, Dict
from mediakit import is_video, is_image

from ..models import UploadResult
from .models import FolderUploadResult, UploadTask
from .parallel import get_parallel_count
from .file_collector import FileCollector
from .preview_handler import PreviewHandler
from ..services.resume import sha256_file


class FolderUploadHandler:
    """Handles folder uploads with resume support."""
    
    def __init__(
        self,
        analyzer,
        repository,
        storage,
        preview_handler: PreviewHandler,
        config
    ):
        """
        Initialize folder upload handler.
        
        Args:
            analyzer: AnalyzerService
            repository: MetadataRepository
            storage: Storage service
            preview_handler: PreviewHandler
            config: UploadConfig
        """
        self._analyzer = analyzer
        self._repository = repository
        self._storage = storage
        self._preview_handler = preview_handler
        self._config = config
        self._file_collector = FileCollector()
    
    async def upload_folder(
        self,
        folder_path: Path,
        dest: Optional[str] = None,
        progress_callback: Optional[Callable[[str, int, int], None]] = None,
    ) -> FolderUploadResult:
        """Upload entire folder preserving directory structure."""
        folder_path = Path(folder_path)
        
        if not folder_path.is_dir():
            return FolderUploadResult(
                success=False,
                folder_name=folder_path.name,
                total_files=0,
                uploaded_files=0,
                failed_files=0,
                results=[],
                error=f"Not a directory: {folder_path}"
            )
        
        total = 0
        
        try:
            # 1. Collect all files
            all_files = self._file_collector.collect_files(folder_path)
            total = len(all_files)
            
            if total == 0:
                return FolderUploadResult(
                    success=True,
                    folder_name=folder_path.name,
                    total_files=0,
                    uploaded_files=0,
                    failed_files=0,
                    results=[],
                    error="No media files found in folder"
                )
            
            # 2. Create folder structure FIRST
            dest_path = f"{dest}/{folder_path.name}" if dest else folder_path.name
            root_handle = await self._storage.create_folder(dest_path)
            
            # 3. Check existing files
            pending_files, skipped = await self._check_existing_files(
                all_files, folder_path, dest_path, total, progress_callback
            )
            
            if not pending_files:
                return FolderUploadResult(
                    success=True,
                    folder_name=folder_path.name,
                    total_files=total,
                    uploaded_files=skipped,
                    failed_files=0,
                    results=[],
                    error=None
                )
            
            # 4. Calculate SHA256 and lookup existing source_ids
            sha256_map, existing_ids = await self._prepare_resume(
                pending_files, folder_path, progress_callback
            )
            
            # 5. Analyze files
            batch_items, pending_data = await self._analyze_files(
                sha256_map, existing_ids, folder_path, progress_callback
            )
            
            # 6. Save batch to API
            if batch_items:
                if progress_callback:
                    progress_callback(f"Saving {len(batch_items)} new files to API...", 0, 1)
                await self._repository.save_batch(batch_items)
            
            # 7. Lock account and upload
            results = await self._upload_files(
                pending_data, dest_path, total, progress_callback
            )
            
            # Unlock account
            if hasattr(self._storage, 'unlock_account'):
                self._storage.unlock_account()
            
            uploaded = sum(1 for r in results if r.success)
            failed = sum(1 for r in results if not r.success)
            
            return FolderUploadResult(
                success=failed == 0,
                folder_name=folder_path.name,
                total_files=total,
                uploaded_files=uploaded + skipped,
                failed_files=failed,
                results=results,
                mega_folder_handle=root_handle
            )
            
        except Exception as e:
            import traceback
            error_msg = f"{str(e)}\n\n{traceback.format_exc()}"
            print(f"\n❌ UPLOAD ERROR: {error_msg}")
            # Unlock account on error
            if hasattr(self._storage, 'unlock_account'):
                self._storage.unlock_account()
            return FolderUploadResult(
                success=False,
                folder_name=folder_path.name,
                total_files=total,
                uploaded_files=0,
                failed_files=total,
                results=[],
                error=error_msg
            )
    
    async def _check_existing_files(
        self,
        all_files: List[Path],
        folder_path: Path,
        dest_path: str,
        total: int,
        progress_callback: Optional[Callable]
    ) -> Tuple[List[Tuple[Path, Path]], int]:
        """Check which files already exist in MEGA."""
        if progress_callback:
            progress_callback("Checking existing files in MEGA...", 0, total)
        
        pending_files = []
        skipped = 0
        
        for idx, file_path in enumerate(all_files):
            rel_path = file_path.relative_to(folder_path)
            mega_dest = f"{dest_path}/{rel_path.parent}" if rel_path.parent != Path(".") else dest_path
            target_path = f"{mega_dest}/{file_path.name}"
            
            exists = await self._storage.exists(target_path)
            if exists:
                skipped += 1
            else:
                pending_files.append((file_path, rel_path))
            
            if progress_callback and idx % 10 == 0:
                progress_callback(f"Checking: {file_path.name}", idx + 1, total)
        
        if skipped > 0:
            print(f"[resume] Skipped {skipped} files already in MEGA")
        
        return pending_files, skipped
    
    async def _prepare_resume(
        self,
        pending_files: List[Tuple[Path, Path]],
        folder_path: Path,
        progress_callback: Optional[Callable]
    ) -> Tuple[Dict[str, Tuple[Path, Path]], Dict[str, str]]:
        """Calculate SHA256 and lookup existing source_ids."""
        if progress_callback:
            progress_callback(f"Calculating hashes for {len(pending_files)} files...", 0, len(pending_files))
        
        sha256_map = {}
        sha256_list = []
        
        for idx, (file_path, rel_path) in enumerate(pending_files):
            sha = sha256_file(file_path)
            sha256_map[sha] = (file_path, rel_path)
            sha256_list.append(sha)
            if progress_callback and idx % 10 == 0:
                progress_callback(f"Hashing: {file_path.name}", idx + 1, len(pending_files))
        
        # Lookup existing source_ids
        existing_ids = {}
        if sha256_list:
            try:
                existing_ids = await self._repository.lookup_by_sha256(sha256_list)
                if existing_ids:
                    print(f"[resume] Found {len(existing_ids)} files already in API (will reuse source_ids)")
            except Exception as e:
                print(f"[resume] API lookup failed: {e}")
        
        return sha256_map, existing_ids
    
    async def _analyze_files(
        self,
        sha256_map: Dict[str, Tuple[Path, Path]],
        existing_ids: Dict[str, str],
        folder_path: Path,
        progress_callback: Optional[Callable]
    ) -> Tuple[List[dict], List[Tuple]]:
        """Analyze files and prepare batch items."""
        if progress_callback:
            progress_callback(f"Analyzing {len(sha256_map)} files...", 0, len(sha256_map))
        
        batch_items = []
        pending_data = []
        
        for idx, (sha, (file_path, rel_path)) in enumerate(sha256_map.items()):
            existing_source_id = existing_ids.get(sha)
            
            if is_video(file_path):
                tech_data = self._analyzer.analyze_video(file_path)
                
                if existing_source_id:
                    source_id = existing_source_id
                else:
                    source_id = tech_data["source_id"]
                    batch_items.append({
                        "document": self._repository.prepare_document(tech_data),
                        "video_metadata": self._repository.prepare_video_metadata(source_id, tech_data),
                    })
                
                pending_data.append((file_path, source_id, tech_data, True, rel_path))
                
            elif is_image(file_path):
                tech_data = self._analyzer.analyze_photo(file_path)
                
                if existing_source_id:
                    source_id = existing_source_id
                else:
                    source_id = tech_data["source_id"]
                    batch_items.append({
                        "document": self._repository.prepare_document(tech_data),
                        "photo_metadata": self._repository.prepare_photo_metadata(source_id, tech_data),
                    })
                
                pending_data.append((file_path, source_id, tech_data, False, rel_path))
            
            if progress_callback:
                progress_callback(f"Analyzed: {file_path.name}", idx + 1, len(sha256_map))
        
        return batch_items, pending_data
    
    async def _upload_files(
        self,
        pending_data: List[Tuple],
        dest_path: str,
        total: int,
        progress_callback: Optional[Callable]
    ) -> List[UploadResult]:
        """Upload files with controlled parallelism."""
        # Calculate total size and lock account
        total_size = sum(f.stat().st_size for f, _, _, _, _ in pending_data) if pending_data else 0
        avg_size = total_size / len(pending_data) if pending_data else 0
        parallel_count = get_parallel_count(avg_size)
        
        # Lock account for entire folder
        if hasattr(self._storage, 'lock_account_for_size') and total_size > 0:
            if progress_callback:
                progress_callback(f"Selecting account for {total_size / (1024*1024):.1f} MB...", 0, 1)
            locked = await self._storage.lock_account_for_size(total_size)
            if not locked:
                raise RuntimeError(f"No account has enough space for {total_size / (1024*1024):.1f} MB")
        
        if progress_callback:
            progress_callback(f"Uploading with {parallel_count} parallel uploads...", 0, total)
        
        # Prepare upload tasks
        upload_tasks = []
        for file_path, source_id, tech_data, is_vid, rel_path in pending_data:
            mega_dest = f"{dest_path}/{rel_path.parent}" if rel_path.parent != Path(".") else dest_path
            upload_tasks.append(UploadTask(
                file_path=file_path,
                source_id=source_id,
                tech_data=tech_data,
                is_video=is_vid,
                rel_path=rel_path,
                mega_dest=mega_dest,
                file_size=file_path.stat().st_size
            ))
        
        # Upload with parallelism
        results = []
        completed = 0
        semaphore = asyncio.Semaphore(parallel_count)
        
        async def upload_one(task: UploadTask) -> UploadResult:
            async with semaphore:
                try:
                    handle = await self._storage.upload_video(
                        task.file_path, task.mega_dest, task.source_id, None
                    )
                    
                    if not handle:
                        return UploadResult.fail(task.file_path.name, "Upload failed")
                    
                    # Generate and upload preview for videos
                    preview_handle = None
                    if task.is_video and self._config.generate_preview:
                        duration = task.tech_data.get("duration", 0)
                        preview_handle = await self._preview_handler.upload_preview(
                            task.file_path, task.source_id, duration
                        )
                    
                    return UploadResult.ok(
                        source_id=task.source_id,
                        filename=task.file_path.name,
                        mega_handle=handle,
                        preview_handle=preview_handle
                    )
                except Exception as e:
                    return UploadResult.fail(task.file_path.name, str(e))
        
        # Run all uploads
        tasks = [upload_one(t) for t in upload_tasks]
        
        for coro in asyncio.as_completed(tasks):
            result = await coro
            results.append(result)
            completed += 1
            
            if progress_callback:
                progress_callback(f"Uploaded: {result.filename}", completed, total)
        
        return results

