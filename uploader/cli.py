"""Command line interface for uploader package."""
from __future__ import annotations

import argparse
import asyncio
import inspect
import logging
import os
import sys
from pathlib import Path
from typing import Optional, Sequence

from .cli_progress import (
    FolderUploadProgressDisplay,
    SingleFileUploadProgress,
    render_configuration_summary,
)


DEFAULT_MEGA_ACCOUNT_API_URL = "http://127.0.0.1:9932"


class CLIError(RuntimeError):
    """Raised when CLI validation/execution fails."""


def _setup_logging(debug: bool, silent: bool, log_level: Optional[str]) -> str:
    """
    Configure logging.

    Default behavior is silent unless --debug or --log-level is provided.
    Returns a string describing effective mode.
    """
    root_logger = logging.getLogger()
    for handler in list(root_logger.handlers):
        root_logger.removeHandler(handler)

    logging.disable(logging.NOTSET)

    if silent or (not debug and not log_level):
        logging.disable(logging.CRITICAL)
        root_logger.setLevel(logging.CRITICAL + 1)
        return "silent"

    if debug:
        level = logging.DEBUG
    elif log_level:
        level = getattr(logging, log_level.upper(), logging.INFO)
    else:
        env_level = os.getenv("LOG_LEVEL")
        level = getattr(logging, (env_level or "INFO").upper(), logging.INFO)

    try:
        from rich.logging import RichHandler  # type: ignore

        handler = RichHandler(
            rich_tracebacks=True,
            markup=True,
            show_time=False,
            show_path=False,
        )
        formatter = logging.Formatter("%(message)s")
    except Exception:
        handler = logging.StreamHandler()
        formatter = logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s")

    handler.setFormatter(formatter)
    root_logger.addHandler(handler)
    root_logger.setLevel(level)
    return logging.getLevelName(level)


def _normalize_dest(dest: Optional[str]) -> Optional[str]:
    if dest is None:
        return None
    value = dest.strip()
    if value in {"", "/"}:
        return None
    return value.lstrip("/").rstrip("/")


def _strip_optional_quotes(value: str) -> str:
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
        return value[1:-1]
    return value


def _load_env_file(path: Path, override: bool = False) -> None:
    if not path.exists():
        raise CLIError(f"env file not found: {path}")
    if not path.is_file():
        raise CLIError(f"env path is not a file: {path}")

    try:
        content = path.read_text(encoding="utf-8")
    except OSError as exc:
        raise CLIError(f"could not read env file {path}: {exc}") from exc

    for raw_line in content.splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export ") :].strip()
        if "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        if not key:
            continue

        value = _strip_optional_quotes(value.strip())
        if override or key not in os.environ:
            os.environ[key] = value


def _resolve_default_env_file() -> Optional[Path]:
    default_env = Path(".env")
    return default_env if default_env.exists() and default_env.is_file() else None


def _build_managed_storage(
    managed_cls,
    sessions_dir: Optional[Path],
    collection: Optional[str],
    mega_account_api_url: str,
):
    """Build ManagedStorageService across minor signature differences."""
    signature = inspect.signature(managed_cls.__init__)
    params = signature.parameters
    kwargs = {}

    if "sessions_dir" in params:
        kwargs["sessions_dir"] = sessions_dir
    if "api_url" in params:
        kwargs["api_url"] = mega_account_api_url
    if "collection_name" in params and collection is not None:
        kwargs["collection_name"] = collection
    elif "collection" in params and collection is not None:
        kwargs["collection"] = collection
    elif collection is not None:
        print(
            "WARNING: this uploader version ignores --collection on ManagedStorageService",
            file=sys.stderr,
        )

    return managed_cls(**kwargs)


async def _initialize_storage(storage, skip_space_check: bool) -> None:
    if skip_space_check:
        start = getattr(storage, "start", None)
        if callable(start):
            await start()
        return

    check_accounts_space = getattr(storage, "check_accounts_space", None)
    if callable(check_accounts_space):
        await check_accounts_space()
        return

    start = getattr(storage, "start", None)
    if callable(start):
        await start()


async def _close_storage_safely(storage) -> None:
    close = getattr(storage, "close", None)
    if callable(close):
        try:
            await close()
        except Exception:
            pass


async def _run_upload(
    source: Path,
    dest: Optional[str],
    collection: Optional[str],
    skip_space_check: bool,
    mega_account_api_url: str,
    sessions_dir: Optional[Path],
) -> int:
    datastore_api_url = os.getenv("DATASTORE_API_URL")
    if not datastore_api_url:
        raise CLIError("DATASTORE_API_URL environment variable is not set")

    dest_norm = _normalize_dest(dest)

    try:
        from uploader import UploadOrchestrator
        from uploader.models import UploadConfig
    except Exception as exc:
        raise CLIError(f"cannot import uploader package: {exc}") from exc

    try:
        from uploader.services.managed_storage import ManagedStorageService
    except Exception:
        try:
            from uploader import ManagedStorageService
        except Exception as exc:
            raise CLIError(
                f"ManagedStorageService is not available (install managed dependencies): {exc}"
            ) from exc

    storage = _build_managed_storage(
        ManagedStorageService,
        sessions_dir=sessions_dir,
        collection=collection,
        mega_account_api_url=mega_account_api_url,
    )

    await _initialize_storage(storage, skip_space_check=skip_space_check)

    try:
        config = UploadConfig(skip_space_check=skip_space_check)
        async with UploadOrchestrator(
            datastore_api_url,
            storage_service=storage,
            config=config,
        ) as orchestrator:
            if source.is_file():
                if not skip_space_check:
                    check_available_for_size = getattr(storage, "check_available_for_size", None)
                    if callable(check_available_for_size):
                        file_size = source.stat().st_size
                        has_space = await check_available_for_size(file_size)
                        if not has_space:
                            size_gb = file_size / (1024**3)
                            raise CLIError(
                                f"no storage space available for file size {size_gb:.2f} GB"
                            )

                progress = SingleFileUploadProgress(source)
                progress.start()
                result = await orchestrator.upload_auto(source, dest_norm, progress.get_callback())
                success = bool(getattr(result, "success", True))
                error = getattr(result, "error", None)
                progress.complete(success=success, error=error)
                return 0 if success else 1

            if source.is_dir():
                print("Uploading folder...")
                display = FolderUploadProgressDisplay()
                process = orchestrator.upload_folder(source, dest_norm)
                process.on_file_start(display.on_file_start)
                process.on_file_progress(display.on_file_progress)
                process.on_file_complete(display.on_file_complete)
                process.on_file_fail(display.on_file_fail)
                process.on_phase_start(display.on_phase_start)
                process.on_phase_progress(display.on_phase_progress)
                process.on_phase_complete(display.on_phase_complete)
                process.on_error(display.on_error)

                folder_result = await process.wait()
                display.on_finish(folder_result)
                if bool(getattr(folder_result, "success", False)):
                    return 0

                folder_error = getattr(folder_result, "error", None)
                if folder_error:
                    print(f"ERROR: {folder_error}", file=sys.stderr)
                return 1

            raise CLIError(f"source is neither file nor directory: {source}")
    finally:
        await _close_storage_safely(storage)


def _build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="mega-up",
        description="Upload a file or folder to MEGA using uploader orchestrator.",
    )
    parser.add_argument("source", nargs="?", type=Path, help="Source file or folder path")
    parser.add_argument(
        "-g",
        "--dest",
        default=None,
        help="Destination folder path in MEGA (example: /Sets or /Videos/2026)",
    )
    parser.add_argument(
        "-c",
        "--collection",
        default=None,
        help="Load only accounts from this collection in mega-account-api",
    )
    parser.add_argument(
        "-k",
        "--skip-space-check",
        action="store_true",
        help="Skip storage free-space checks",
    )
    parser.add_argument(
        "--skip-hash-check",
        action="store_true",
        help="Skip BLAKE3 dedup checks in uploader flow",
    )
    parser.add_argument(
        "--mega-account-api-url",
        default=None,
        help=(
            "mega-account-api URL (default from MEGA_ACCOUNT_API_URL "
            f"or {DEFAULT_MEGA_ACCOUNT_API_URL})"
        ),
    )
    parser.add_argument(
        "--sessions-dir",
        type=Path,
        default=None,
        help="Sessions directory (default from MEGA_SESSIONS_DIR or service defaults)",
    )
    parser.add_argument(
        "--env-file",
        type=Path,
        default=None,
        help="Load environment variables from this .env file",
    )
    parser.add_argument("--debug", action="store_true", help="Enable debug logs")
    parser.add_argument("--silent", action="store_true", help="Only print errors")
    parser.add_argument(
        "--log-level",
        default=None,
        help="Explicit log level (DEBUG/INFO/WARNING/ERROR)",
    )
    parser.add_argument(
        "--version",
        action="version",
        version="mega-up (from uploader)",
    )
    return parser


def run_cli(argv: Optional[Sequence[str]] = None) -> int:
    parser = _build_parser()
    args = parser.parse_args(argv)

    used_env_file = args.env_file or _resolve_default_env_file()
    if used_env_file is not None:
        try:
            _load_env_file(Path(used_env_file))
        except CLIError as exc:
            print(f"ERROR: {exc}", file=sys.stderr)
            return 1

    effective_log_mode = _setup_logging(
        debug=args.debug,
        silent=args.silent,
        log_level=args.log_level,
    )

    if args.source is None:
        parser.print_help()
        return 0

    source = Path(args.source).expanduser()
    if not source.exists():
        print(f"ERROR: source does not exist: {source}", file=sys.stderr)
        return 1

    if args.skip_hash_check:
        os.environ["UPLOADER_SKIP_HASH_CHECK"] = "1"
        print("WARNING: skipping hash checking (--skip-hash-check).", file=sys.stderr)

    sessions_dir = args.sessions_dir
    if sessions_dir is None:
        env_sessions_dir = os.getenv("MEGA_SESSIONS_DIR")
        if env_sessions_dir:
            sessions_dir = Path(env_sessions_dir)

    mega_account_api_url = (
        args.mega_account_api_url
        or os.getenv("MEGA_ACCOUNT_API_URL")
        or DEFAULT_MEGA_ACCOUNT_API_URL
    )

    datastore_api_url = os.getenv("DATASTORE_API_URL")
    rendered_dest = _normalize_dest(args.dest) or "(default)"
    source_kind = "file" if source.is_file() else "folder" if source.is_dir() else "unknown"
    render_configuration_summary(
        {
            "Source": str(source),
            "Source Type": source_kind,
            "Dest": rendered_dest,
            "Collection": args.collection or "-",
            "Datastore API": datastore_api_url or "(missing)",
            "Account API": mega_account_api_url,
            "Sessions Dir": str(sessions_dir) if sessions_dir else "(service default)",
            "Skip Space Check": "yes" if args.skip_space_check else "no",
            "Skip Hash Check": "yes" if args.skip_hash_check else "no",
            "Env File": str(used_env_file) if used_env_file else "-",
            "Logging": effective_log_mode,
        }
    )

    try:
        return asyncio.run(
            _run_upload(
                source=source,
                dest=args.dest,
                collection=args.collection,
                skip_space_check=args.skip_space_check,
                mega_account_api_url=mega_account_api_url,
                sessions_dir=sessions_dir,
            )
        )
    except CLIError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 1
    except KeyboardInterrupt:
        print("Cancelled.", file=sys.stderr)
        return 130


def main() -> None:
    raise SystemExit(run_cli())


if __name__ == "__main__":
    main()
