"""Tests for uploader CLI helpers."""
import logging
import os
from pathlib import Path

from uploader.cli import _build_managed_storage, _load_env_file, _normalize_dest, _setup_logging


def test_normalize_dest():
    assert _normalize_dest(None) is None
    assert _normalize_dest("") is None
    assert _normalize_dest(" / ") is None
    assert _normalize_dest("/Sets/") == "Sets"
    assert _normalize_dest("Videos/2026") == "Videos/2026"


def test_load_env_file(tmp_path, monkeypatch):
    env_path = tmp_path / ".env"
    env_path.write_text(
        "\n".join(
            [
                "DATASTORE_API_URL=http://localhost:3312",
                "MEGA_ACCOUNT_API_URL='http://127.0.0.1:9932'",
                "export MEGA_SESSIONS_DIR=C:/sessions",
            ]
        ),
        encoding="utf-8",
    )

    monkeypatch.delenv("DATASTORE_API_URL", raising=False)
    monkeypatch.delenv("MEGA_ACCOUNT_API_URL", raising=False)
    monkeypatch.delenv("MEGA_SESSIONS_DIR", raising=False)

    _load_env_file(env_path)

    assert "DATASTORE_API_URL" in os.environ
    assert os.environ["DATASTORE_API_URL"] == "http://localhost:3312"
    assert os.environ["MEGA_ACCOUNT_API_URL"] == "http://127.0.0.1:9932"
    assert os.environ["MEGA_SESSIONS_DIR"] == "C:/sessions"


def test_build_managed_storage_uses_available_kwargs():
    class FakeManagedStorage:
        def __init__(self, sessions_dir=None, api_url=None, collection_name=None):
            self.sessions_dir = sessions_dir
            self.api_url = api_url
            self.collection_name = collection_name

    sessions_dir = Path("C:/tmp/sessions")
    storage = _build_managed_storage(
        FakeManagedStorage,
        sessions_dir=sessions_dir,
        collection="my_collection",
        mega_account_api_url="http://127.0.0.1:9932",
    )

    assert storage.sessions_dir == sessions_dir
    assert storage.api_url == "http://127.0.0.1:9932"
    assert storage.collection_name == "my_collection"


def test_setup_logging_defaults_to_silent():
    mode = _setup_logging(debug=False, silent=False, log_level=None)
    assert mode == "silent"
    assert logging.getLogger().isEnabledFor(logging.CRITICAL) is False
    logging.disable(logging.NOTSET)


def test_setup_logging_debug_mode():
    mode = _setup_logging(debug=True, silent=False, log_level=None)
    assert mode == "DEBUG"
    assert logging.getLogger().isEnabledFor(logging.DEBUG) is True
    logging.disable(logging.NOTSET)
