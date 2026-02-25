"""Shared deduplication helpers for hash and existence workflows."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, Optional, Tuple

import logging

from uploader.services.resume import blake3_file

logger = logging.getLogger(__name__)


DedupAction = Literal["skip", "reupload", "upload"]


@dataclass(frozen=True)
class DedupDecision:
    """Result of deciding what to do with a file after dedup checks."""

    action: DedupAction
    reason: str


def parse_repository_doc_info(doc_info: Any) -> Tuple[Optional[str], Optional[str]]:
    """
    Normalize repository lookup payload into (source_id, mega_handle).

    Supports both legacy payloads where doc_info is a raw source_id string and
    newer payloads where doc_info is a dict with source fields.
    """
    if isinstance(doc_info, dict):
        return doc_info.get("source_id"), doc_info.get("mega_handle")
    return doc_info, None


async def exists_in_mega_by_source_id(storage: Any, source_id: str) -> bool:
    """
    Verify if a source_id exists in MEGA.

    Namespace-only behavior: requires storage.exists_by_mega_id(source_id).
    """
    if not source_id:
        return False
    try:
        return await storage.exists_by_mega_id(source_id)
    except Exception as storage_exc:
        logger.debug(
            "Storage mega_id lookup failed for source_id=%s: %s",
            source_id,
            storage_exc,
        )
    return False


class ResolveDedupActionUseCase:
    """Resolve final action from DB/MEGA existence state."""

    @staticmethod
    def execute(exists_in_db: bool, exists_in_mega: bool) -> DedupDecision:
        if exists_in_db and exists_in_mega:
            return DedupDecision(action="skip", reason="exists_in_db_and_mega")
        if exists_in_db and not exists_in_mega:
            return DedupDecision(action="reupload", reason="exists_in_db_not_in_mega")
        return DedupDecision(action="upload", reason="not_found_in_db")


class ResolveFileHashUseCase:
    """Resolve file hash with optional cache read/write."""

    async def execute(self, file_path: Path, hash_cache: Any = None) -> tuple[Optional[str], bool]:
        if hash_cache:
            try:
                cached_hash = await hash_cache.get(file_path)
                if cached_hash:
                    return cached_hash, True
            except Exception as cache_read_exc:
                logger.debug("Hash cache read failed for %s: %s", file_path.name, cache_read_exc)

        try:
            file_hash = await blake3_file(file_path)
        except Exception as hash_exc:
            logger.warning("Hash calculation failed for %s: %s", file_path, hash_exc)
            return None, False

        if hash_cache and file_hash:
            try:
                await hash_cache.set(file_path, file_hash)
            except Exception as cache_write_exc:
                logger.debug("Hash cache write failed for %s: %s", file_path.name, cache_write_exc)

        return file_hash, False
