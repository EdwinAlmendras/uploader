"""
Resume Service - Handles interrupted upload recovery.

Flow:
1. Check MEGA paths → skip files that exist
2. For missing files: calculate blake3_hash
3. Batch API lookup: blake3_hash → source_id
4. Upload with recovered source_id (or generate new)
"""
import logging
import asyncio
from pathlib import Path
from typing import Dict, List, Set, Tuple
import httpx
from blake3 import blake3

log = logging.getLogger(__name__)


async def blake3_file(path: Path) -> str:
    """Calculate BLAKE3 hash of file asynchronously (non-blocking)."""
    def _hash_file():
        """Synchronous hash calculation to run in thread pool."""
        hasher = blake3()
        with open(path, 'rb') as f:
            # Use larger chunks for better performance
            while True:
                chunk = f.read(65536)  # 64KB chunks
                if not chunk:
                    break
                hasher.update(chunk)
        return hasher.hexdigest()
    
    # Run in thread pool to avoid blocking the event loop
    return await asyncio.to_thread(_hash_file)


class ResumeService:
    """
    Resume interrupted uploads.
    
    Usage:
        resume = ResumeService(api_url, mega_manager)
        pending, recovered_ids = await resume.filter_pending(files, dest_folder)
        # pending = files not in MEGA
        # recovered_ids = {blake3_hash: source_id} from API
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
            (pending_files, blake3_hash_to_source_id)
            - pending_files: files not in MEGA (need upload)
            - blake3_hash_to_source_id: recovered source_ids from API
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
        
        # Step 2: Calculate blake3_hash for pending files (async, non-blocking)
        blake3_map = {}  # path_str → blake3_hash
        blake3_list = []
        
        # Calculate hashes concurrently for better performance
        hash_tasks = []
        for path in pending:
            hash_tasks.append(blake3_file(path))
        
        hash_results = await asyncio.gather(*hash_tasks, return_exceptions=True)
        
        for path, hash_result in zip(pending, hash_results):
            if isinstance(hash_result, Exception):
                log.warning(f"[resume] Hash failed for {path}: {hash_result}")
            else:
                blake3_map[str(path)] = hash_result
                blake3_list.append(hash_result)
        
        # Step 3: Batch lookup to API → get existing source_ids
        recovered_ids = await self._lookup_source_ids(blake3_list)
        
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
    
    async def _lookup_source_ids(self, blake3_list: List[str]) -> Dict[str, str]:
        """Batch lookup: blake3_hash → source_id from API."""
        if not blake3_list or not self._api_url:
            return {}
        
        try:
            async with httpx.AsyncClient(timeout=30) as client:
                resp = await client.post(
                    f"{self._api_url}/documents/lookup",
                    json={"blake3_list": blake3_list}
                )
                resp.raise_for_status()
                return resp.json().get("found", {})
        except Exception as e:
            log.warning(f"[resume] API lookup failed: {e}")
            return {}
