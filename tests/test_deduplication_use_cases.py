"""Tests for shared deduplication use cases."""
from pathlib import Path
from unittest.mock import AsyncMock, Mock

import pytest

from uploader.use_cases.deduplication import (
    ResolveDedupActionUseCase,
    ResolveFileHashUseCase,
    exists_in_mega_by_source_id,
    parse_repository_doc_info,
)


def test_parse_repository_doc_info_with_dict():
    source_id, mega_handle = parse_repository_doc_info(
        {"source_id": "src-1", "mega_handle": "mh-1"}
    )
    assert source_id == "src-1"
    assert mega_handle == "mh-1"


def test_parse_repository_doc_info_with_legacy_string():
    source_id, mega_handle = parse_repository_doc_info("legacy-source")
    assert source_id == "legacy-source"
    assert mega_handle is None


@pytest.mark.asyncio
async def test_exists_in_mega_prefers_manager():
    manager = Mock()
    manager.find_by_mega_id = AsyncMock(return_value=("acc", object()))
    storage = Mock()
    storage.manager = manager
    storage.exists_by_mega_id = AsyncMock(return_value=False)

    exists = await exists_in_mega_by_source_id(storage, "src-1")

    assert exists is True
    manager.find_by_mega_id.assert_awaited_once_with("src-1")
    storage.exists_by_mega_id.assert_not_awaited()


@pytest.mark.asyncio
async def test_exists_in_mega_falls_back_to_storage():
    storage = Mock()
    storage.manager = None
    storage.exists_by_mega_id = AsyncMock(return_value=True)

    exists = await exists_in_mega_by_source_id(storage, "src-2")

    assert exists is True
    storage.exists_by_mega_id.assert_awaited_once_with("src-2")


@pytest.mark.asyncio
async def test_resolve_file_hash_uses_cache_hit():
    resolver = ResolveFileHashUseCase()
    cache = AsyncMock()
    cache.get = AsyncMock(return_value="cached-hash")
    cache.set = AsyncMock()

    file_hash, from_cache = await resolver.execute(Path("video.mp4"), cache)

    assert file_hash == "cached-hash"
    assert from_cache is True
    cache.set.assert_not_awaited()


@pytest.mark.asyncio
async def test_resolve_file_hash_calculates_and_stores(monkeypatch):
    monkeypatch.setattr(
        "uploader.use_cases.deduplication.blake3_file",
        AsyncMock(return_value="fresh-hash"),
    )
    resolver = ResolveFileHashUseCase()
    cache = AsyncMock()
    cache.get = AsyncMock(return_value=None)
    cache.set = AsyncMock()

    file_hash, from_cache = await resolver.execute(Path("video.mp4"), cache)

    assert file_hash == "fresh-hash"
    assert from_cache is False
    cache.set.assert_awaited_once_with(Path("video.mp4"), "fresh-hash")


def test_resolve_dedup_action_skip():
    use_case = ResolveDedupActionUseCase()
    decision = use_case.execute(exists_in_db=True, exists_in_mega=True)
    assert decision.action == "skip"
    assert decision.reason == "exists_in_db_and_mega"


def test_resolve_dedup_action_reupload():
    use_case = ResolveDedupActionUseCase()
    decision = use_case.execute(exists_in_db=True, exists_in_mega=False)
    assert decision.action == "reupload"
    assert decision.reason == "exists_in_db_not_in_mega"


def test_resolve_dedup_action_upload():
    use_case = ResolveDedupActionUseCase()
    decision = use_case.execute(exists_in_db=False, exists_in_mega=False)
    assert decision.action == "upload"
    assert decision.reason == "not_found_in_db"
