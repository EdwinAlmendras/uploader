"""Hash-based set deduplication helpers.

This module isolates the "does this set archive already exist" flow from the
folder handler so the orchestration layer stays readable and easier to test.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict, Iterable, List, Optional

import logging

from uploader.services.hash_cache import HashCache
from uploader.services.repository import MetadataRepository
from uploader.use_cases.deduplication import (
    ResolveDedupActionUseCase,
    ResolveFileHashUseCase,
    exists_in_mega_by_source_id,
    parse_repository_doc_info,
)

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class SetCheckInfo:
    """Deduplication result for one set."""

    hash: Optional[str] = None
    source_id: Optional[str] = None
    mega_handle: Optional[str] = None
    exists_in_both: bool = False
    from_cache: bool = False

    def as_dict(self) -> Dict[str, Any]:
        """Compatibility shim for existing handler call sites."""
        payload: Dict[str, Any] = {
            "exists_in_both": self.exists_in_both,
        }
        if self.hash:
            payload["hash"] = self.hash
        if self.source_id:
            payload["source_id"] = self.source_id
        if self.mega_handle:
            payload["mega_handle"] = self.mega_handle
        if self.from_cache:
            payload["from_cache"] = True
        return payload


class SetBatchDeduplicator:
    """Batch hash checker for set archives with graceful fallbacks."""

    def __init__(
        self,
        repository: Optional[MetadataRepository],
        storage: Any,
        hash_cache: Optional[HashCache],
        skip_hash_check: Callable[[], bool],
        batch_size: int = 50,
    ):
        self._repository = repository
        self._storage = storage
        self._hash_cache = hash_cache
        self._skip_hash_check = skip_hash_check
        self._batch_size = max(1, batch_size)
        self._resolve_hash = ResolveFileHashUseCase()
        self._resolve_action = ResolveDedupActionUseCase()

    async def check_sets(self, sets: List[Path]) -> Dict[Path, Dict[str, Any]]:
        """
        Check set archives in batch.

        For each set it looks for `set_folder.parent/files/{set_name}.7z`,
        resolves/caches its hash, checks DB, and verifies source presence in MEGA.
        """
        if self._skip_hash_check():
            logger.warning(
                "Skipping set deduplication hash checks (UPLOADER_SKIP_HASH_CHECK is enabled)."
            )
            return {set_folder: {} for set_folder in sets}

        results: Dict[Path, SetCheckInfo] = {}
        hashes_to_sets: Dict[str, Path] = {}
        needs_hash: List[tuple[Path, Path]] = []
        cached_hashes = 0
        calculated_hashes = 0
        hash_failures = 0

        for set_folder in sets:
            archive_path = self._archive_path(set_folder)
            if not archive_path.exists():
                results[set_folder] = SetCheckInfo()
                continue

            archive_hash = await self._get_cached_hash(archive_path)
            if archive_hash:
                cached_hashes += 1
                hashes_to_sets[archive_hash] = set_folder
                results[set_folder] = SetCheckInfo(hash=archive_hash, from_cache=True)
                continue

            needs_hash.append((set_folder, archive_path))

        for set_folder, archive_path in needs_hash:
            archive_hash = await self._calculate_hash_safe(archive_path)
            if not archive_hash:
                hash_failures += 1
                results[set_folder] = SetCheckInfo()
                continue
            calculated_hashes += 1
            hashes_to_sets[archive_hash] = set_folder
            results[set_folder] = SetCheckInfo(hash=archive_hash, from_cache=False)

        if hashes_to_sets and self._repository:
            merged = await self._fetch_and_merge_results(hashes_to_sets, results)
            results.update(merged)

        total_existing = sum(1 for info in results.values() if info.exists_in_both)
        logger.info(
            "Set hash check finished: sets=%d cached_hash=%d calculated_hash=%d "
            "hash_failures=%d existing_in_db_and_mega=%d",
            len(sets),
            cached_hashes,
            calculated_hashes,
            hash_failures,
            total_existing,
        )

        return {set_folder: info.as_dict() for set_folder, info in results.items()}

    @staticmethod
    def _archive_path(set_folder: Path) -> Path:
        archive_name = f"{set_folder.name}.7z"
        return set_folder.parent / "files" / archive_name

    async def _get_cached_hash(self, archive_path: Path) -> Optional[str]:
        if not self._hash_cache:
            return None
        try:
            return await self._hash_cache.get(archive_path)
        except Exception as exc:
            logger.debug("Hash cache read failed for %s: %s", archive_path.name, exc)
            return None

    async def _calculate_hash_safe(self, archive_path: Path) -> Optional[str]:
        archive_hash, _ = await self._resolve_hash.execute(archive_path, self._hash_cache)
        return archive_hash

    async def _fetch_and_merge_results(
        self,
        hashes_to_sets: Dict[str, Path],
        base_results: Dict[Path, SetCheckInfo],
    ) -> Dict[Path, SetCheckInfo]:
        merged = dict(base_results)
        all_hashes = list(hashes_to_sets.keys())
        for batch in self._chunked(all_hashes, self._batch_size):
            existing_hashes: Dict[str, Dict[str, Any]]
            try:
                existing_hashes = await self._repository.check_exists_batch(batch)  # type: ignore[arg-type]
            except Exception as exc:
                logger.warning("Set batch check failed for %d hashes: %s", len(batch), exc)
                continue

            for archive_hash in batch:
                set_folder = hashes_to_sets[archive_hash]
                previous = merged.get(set_folder, SetCheckInfo(hash=archive_hash))
                doc_info = existing_hashes.get(archive_hash)
                if not doc_info:
                    merged[set_folder] = SetCheckInfo(
                        hash=previous.hash,
                        from_cache=previous.from_cache,
                    )
                    continue

                source_id, mega_handle = parse_repository_doc_info(doc_info)
                exists_in_mega = False
                if source_id:
                    exists_in_mega = await exists_in_mega_by_source_id(self._storage, source_id)
                decision = self._resolve_action.execute(
                    exists_in_db=bool(doc_info),
                    exists_in_mega=exists_in_mega,
                )

                merged[set_folder] = SetCheckInfo(
                    hash=previous.hash,
                    source_id=source_id,
                    mega_handle=mega_handle,
                    exists_in_both=decision.action == "skip",
                    from_cache=previous.from_cache,
                )

        return merged

    @staticmethod
    def _chunked(values: List[str], size: int) -> Iterable[List[str]]:
        for i in range(0, len(values), size):
            yield values[i : i + size]
