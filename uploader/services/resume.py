"""
Resume Service - Handles interrupted upload recovery.

Flow:
1. Check MEGA paths → skip files that exist
2. For missing files: calculate sha256
3. Batch API lookup: sha256 → source_id
4. Upload with recovered source_id (or generate new)
"""
import logging
import hashlib
from pathlib import Path
from typing import Dict, List, Set, Tuple
import httpx

log = logging.getLogger(__name__)


def sha256_file(path: Path) -> str:
    """Calculate SHA256 of file."""
    h = hashlib.sha256()
    with open(path, 'rb') as f:
        for chunk in iter(lambda: f.read(8192), b''):
            h.update(chunk)
    return h.hexdigest()


class ResumeService:
    """
    Resume interrupted uploads.
    
    Usage:
        resume = ResumeService(api_url, mega_manager)
        pending, recovered_ids = await resume.filter_pending(files, dest_folder)
        # pending = files not in MEGA
        # recovered_ids = {sha256: source_id} from API
    """
    
    def __init__(self, api_url: str, storage):
        self._api_url = api_url
        self._storage = storage  # AccountManager or MegaClient
    
    async def filter_pending(
        self, 
        file_paths: List[Path], 
        dest_folder: str
    ) -> Tuple[List[Path], Dict[str, str]]:
        """
        Filter files and recover source_ids.
        
        Args:
            file_paths: Local files to check
            dest_folder: MEGA destination
            
        Returns:
            (pending_files, sha256_to_source_id)
            - pending_files: files not in MEGA (need upload)
            - sha256_to_source_id: recovered source_ids from API
        """
        # Step 1: Check which files exist in MEGA
        pending = []
        skipped = 0
        
        for path in file_paths:
            exists = await self._exists_in_mega(dest_folder, path.name)
            if exists:
                skipped += 1
            else:
                pending.append(path)
        
        if skipped:
            log.info(f"[resume] Skipped {skipped} files (exist in MEGA)")
        
        if not pending:
            return [], {}
        
        # Step 2: Calculate sha256 for pending files
        sha256_map = {}  # path_str → sha256
        sha256_list = []
        
        for path in pending:
            try:
                sha = sha256_file(path)
                sha256_map[str(path)] = sha
                sha256_list.append(sha)
            except Exception as e:
                log.warning(f"[resume] Hash failed for {path}: {e}")
        
        # Step 3: Batch lookup to API → get existing source_ids
        recovered_ids = await self._lookup_source_ids(sha256_list)
        
        if recovered_ids:
            log.info(f"[resume] Recovered {len(recovered_ids)} source_ids from API")
        
        return pending, recovered_ids
    
    async def _exists_in_mega(self, dest_folder: str, filename: str) -> bool:
        """Check if file exists in MEGA by path."""
        full_path = f"{dest_folder}/{filename}"
        
        try:
            if hasattr(self._storage, 'exists'):
                return await self._storage.exists(full_path)
            elif hasattr(self._storage, 'manager'):
                return await self._storage.manager.exists(full_path)
            elif hasattr(self._storage, 'get'):
                node = await self._storage.get(full_path)
                return node is not None
        except:
            pass
        return False
    
    async def _lookup_source_ids(self, sha256_list: List[str]) -> Dict[str, str]:
        """Batch lookup: sha256 → source_id from API."""
        if not sha256_list or not self._api_url:
            return {}
        
        try:
            async with httpx.AsyncClient(timeout=30) as client:
                resp = await client.post(
                    f"{self._api_url}/documents/lookup",
                    json={"sha256_list": sha256_list}
                )
                resp.raise_for_status()
                return resp.json().get("found", {})
        except Exception as e:
            log.warning(f"[resume] API lookup failed: {e}")
            return {}
