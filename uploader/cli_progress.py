"""Console rendering and progress helpers for uploader CLI."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional
import time

try:
    from rich.console import Console, Group
    from rich.live import Live
    from rich.panel import Panel
    from rich.progress import (
        BarColumn,
        DownloadColumn,
        Progress,
        SpinnerColumn,
        TaskID,
        TextColumn,
        TimeRemainingColumn,
        TransferSpeedColumn,
    )
    from rich.table import Table

    RICH_AVAILABLE = True
except Exception:  # pragma: no cover - fallback if rich is unavailable
    Console = None
    Group = None
    Live = None
    Panel = None
    Progress = None
    SpinnerColumn = None
    TextColumn = None
    BarColumn = None
    DownloadColumn = None
    TransferSpeedColumn = None
    TimeRemainingColumn = None
    Table = None
    TaskID = int
    RICH_AVAILABLE = False


LARGE_FILE_THRESHOLD = 50 * 1024 * 1024
SINGLE_FILE_PERCENT_STEP = 5
FOLDER_FILE_PERCENT_STEP = 10

if RICH_AVAILABLE:
    console = Console()
else:
    console = None


def _echo(message: str) -> None:
    if console:
        console.print(message)
        return
    print(message)


def _extract_uploaded_total(progress_obj: Any) -> tuple[int, int]:
    """Best effort progress extraction from uploader/megapy callbacks."""
    if progress_obj is None:
        return 0, 0

    if hasattr(progress_obj, "uploaded_bytes") and hasattr(progress_obj, "total_bytes"):
        uploaded = int(getattr(progress_obj, "uploaded_bytes") or 0)
        total = int(getattr(progress_obj, "total_bytes") or 0)
        return uploaded, total

    if isinstance(progress_obj, (tuple, list)) and len(progress_obj) >= 2:
        try:
            return int(progress_obj[0] or 0), int(progress_obj[1] or 0)
        except Exception:
            return 0, 0

    return 0, 0


def _human_size(value: int) -> str:
    size = float(max(value, 0))
    units = ["B", "KB", "MB", "GB", "TB"]
    unit_idx = 0
    while size >= 1024.0 and unit_idx < len(units) - 1:
        size /= 1024.0
        unit_idx += 1
    if unit_idx == 0:
        return f"{int(size)} {units[unit_idx]}"
    return f"{size:.2f} {units[unit_idx]}"


def render_configuration_summary(config: Dict[str, Any]) -> None:
    """Render startup configuration summary."""
    if RICH_AVAILABLE and Panel and Table and console:
        table = Table.grid(padding=(0, 2))
        table.add_column(style="bold cyan", justify="right")
        table.add_column(style="white")

        for key, value in config.items():
            rendered = "-" if value is None else str(value)
            table.add_row(key, rendered)

        panel = Panel(
            table,
            title="[bold green]mega-up[/bold green]",
            subtitle="[dim]uploader CLI[/dim]",
            border_style="blue",
        )
        console.print(panel)
        return

    _echo("mega-up configuration:")
    for key, value in config.items():
        _echo(f"  {key}: {value}")


class SingleFileUploadProgress:
    """Single-file upload progress renderer."""

    def __init__(self, file_path: Path):
        self.file_path = Path(file_path)
        self.filename = self.file_path.name
        try:
            self.file_size = self.file_path.stat().st_size
        except OSError:
            self.file_size = 0
        self._started = False
        self._last_printed_percent = -1
        self._last_print_time = 0.0

        self._progress = None
        self._live = None
        self._task_id = None
        if RICH_AVAILABLE and Progress and Live and console:
            self._progress = Progress(
                SpinnerColumn(),
                TextColumn("[bold cyan]{task.fields[filename]}", justify="left"),
                BarColumn(bar_width=42),
                TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
                DownloadColumn(),
                TransferSpeedColumn(),
                TimeRemainingColumn(),
                expand=False,
                console=console,
            )

    def start(self) -> None:
        if self._started:
            return

        if self._progress is not None and self.file_size > LARGE_FILE_THRESHOLD:
            self._live = Live(
                self._progress,
                console=console,
                refresh_per_second=5,
                vertical_overflow="visible",
            )
            self._live.start()
            self._task_id = self._progress.add_task(
                "upload",
                filename=self.filename[:60],
                total=self.file_size,
            )
        else:
            _echo(f"[cyan]Uploading:[/cyan] {self.filename}" if RICH_AVAILABLE else f"Uploading: {self.filename}")

        self._started = True

    def update(self, progress_obj: Any) -> None:
        if not self._started:
            self.start()

        uploaded, total = _extract_uploaded_total(progress_obj)
        if total <= 0:
            total = self.file_size
        if total <= 0:
            return

        if self._progress is not None and self._task_id is not None:
            try:
                self._progress.update(self._task_id, completed=uploaded, total=total)
            except Exception:
                pass
            return

        percent = int((uploaded / total) * 100)
        now = time.monotonic()
        should_print = (
            self.file_size <= LARGE_FILE_THRESHOLD
            or percent >= 100
            or percent - self._last_printed_percent >= SINGLE_FILE_PERCENT_STEP
            or now - self._last_print_time >= 2.0
        )
        if should_print and percent != self._last_printed_percent:
            _echo(f"  {percent:3d}% ({_human_size(uploaded)}/{_human_size(total)})")
            self._last_printed_percent = percent
            self._last_print_time = now

    def complete(self, success: bool = True, error: Optional[str] = None) -> None:
        if self._live is not None:
            self._live.stop()
            self._live = None

        if success:
            _echo(
                f"[green]Uploaded:[/green] {self.filename}"
                if RICH_AVAILABLE
                else f"Uploaded: {self.filename}"
            )
            return

        suffix = f" - {error}" if error else ""
        _echo(
            f"[red]Failed:[/red] {self.filename}{suffix}"
            if RICH_AVAILABLE
            else f"Failed: {self.filename}{suffix}"
        )

    def get_callback(self):
        def callback(progress_obj: Any) -> None:
            self.update(progress_obj)

        return callback


@dataclass
class _Phase:
    name: str
    message: str
    current: int = 0
    total: int = 0


class FolderUploadProgressDisplay:
    """Event-based console display for folder upload process."""

    def __init__(self):
        self._phase: Optional[_Phase] = None
        self._file_percent_cache: Dict[str, int] = {}
        self._file_size_bytes: Dict[str, int] = {}
        self._timeline_seen: set[str] = set()
        self._active_tasks: Dict[str, TaskID] = {}
        self._stats: Dict[str, int] = {
            "total_files": 0,
            "uploaded": 0,
            "failed": 0,
            "skipped": 0,
        }
        self._phase_task_id: Optional[TaskID] = None
        self._overall_task_id: Optional[TaskID] = None
        self._meta_progress = None
        self._file_progress = None
        self._live = None

        if RICH_AVAILABLE and Progress and Live and console:
            self._meta_progress = Progress(
                SpinnerColumn(),
                TextColumn("[bold cyan]{task.fields[label]}", justify="left"),
                BarColumn(bar_width=28),
                TextColumn("{task.completed}/{task.total}"),
                TextColumn("[dim]{task.fields[detail]}", justify="left"),
                expand=False,
                console=console,
            )
            self._file_progress = Progress(
                SpinnerColumn(),
                TextColumn("[bold green]{task.fields[label]}", justify="left"),
                BarColumn(bar_width=42),
                TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
                DownloadColumn(),
                TransferSpeedColumn(),
                TimeRemainingColumn(),
                expand=False,
                console=console,
            )

    def _emit_timeline(
        self,
        status: str,
        kind: str,
        name: str,
        size_bytes: Optional[int] = None,
        error: Optional[str] = None,
    ) -> None:
        stamp = time.strftime("%H:%M:%S")
        size_label = f" {_human_size(size_bytes)}" if size_bytes and size_bytes > 0 else ""
        error_label = f" cause={error}" if error else ""
        if RICH_AVAILABLE:
            palette = {
                "DONE": "green",
                "FAIL": "red",
                "SYNC": "cyan",
                "INFO": "blue",
            }
            color = palette.get(status, "white")
            _echo(
                f"[dim]{stamp}[/dim] [{color}]{status:<4}[/{color}] "
                f"{kind}: {name}{size_label}{error_label}"
            )
            return
        _echo(f"{stamp} {status:<4} {kind}: {name}{size_label}{error_label}")

    def _start_live(self) -> None:
        if self._meta_progress is None or self._live is not None:
            return

        renderable = self._meta_progress
        if self._file_progress is not None and Group is not None:
            renderable = Group(self._meta_progress, self._file_progress)

        self._live = Live(
            renderable,
            console=console,
            refresh_per_second=8,
            vertical_overflow="visible",
        )
        self._live.start()
        self._phase_task_id = self._meta_progress.add_task(
            "phase",
            label="Phase",
            total=1,
            completed=0,
            detail="waiting...",
        )
        self._overall_task_id = self._meta_progress.add_task(
            "overall",
            label="Overall",
            total=1,
            completed=0,
            detail="uploaded=0 failed=0 skipped=0",
        )

    def _stop_live(self) -> None:
        if self._live is None:
            return
        self._live.stop()
        self._live = None

    def _update_overall_task(self) -> None:
        if self._meta_progress is None or self._overall_task_id is None:
            return
        uploaded = int(self._stats.get("uploaded", 0) or 0)
        failed = int(self._stats.get("failed", 0) or 0)
        skipped = int(self._stats.get("skipped", 0) or 0)
        total_from_stats = int(self._stats.get("total_files", 0) or 0)
        completed = max(uploaded + failed + skipped, 0)
        total = max(total_from_stats, completed, 1)
        self._meta_progress.update(
            self._overall_task_id,
            label="Overall",
            completed=min(completed, total),
            total=total,
            detail=f"uploaded={uploaded} failed={failed} skipped={skipped}",
        )

    def _update_phase_task(
        self,
        phase_name: str,
        message: str,
        current: int = 0,
        total: int = 0,
    ) -> None:
        self._start_live()
        if self._meta_progress is None or self._phase_task_id is None:
            return
        safe_total = max(total, 1)
        safe_current = min(max(current, 0), safe_total)
        detail = (message or "").strip() or "working..."
        self._meta_progress.update(
            self._phase_task_id,
            label=f"Phase {phase_name}",
            completed=safe_current,
            total=safe_total,
            detail=detail[:120],
        )

    def on_file_start(self, file_path: Path) -> None:
        name = Path(file_path).name
        try:
            file_size = Path(file_path).stat().st_size
        except OSError:
            file_size = 0
        self._file_size_bytes[name] = file_size

        if self._file_progress is not None:
            self._start_live()
            task_id = self._file_progress.add_task(
                "upload",
                label=name[:60],
                total=max(file_size, 1),
            )
            self._active_tasks[name] = task_id
            return

        _echo(f"[cyan]Starting:[/cyan] {name}" if RICH_AVAILABLE else f"Starting: {name}")

    def on_file_progress(self, file_path: Path, file_progress: Any) -> None:
        name = Path(file_path).name
        uploaded = int(getattr(file_progress, "bytes_uploaded", 0) or 0)
        total = int(getattr(file_progress, "total_bytes", 0) or 0)
        if total <= 0:
            return

        task_id = self._active_tasks.get(name)
        if self._file_progress is not None and task_id is not None:
            try:
                self._file_progress.update(task_id, completed=uploaded, total=total)
            except Exception:
                pass
            return

        percent = int((uploaded / total) * 100)
        prev = self._file_percent_cache.get(name, -1)
        if percent >= 100 or percent - prev >= FOLDER_FILE_PERCENT_STEP:
            _echo(
                f"  {name}: {percent:3d}% ({_human_size(uploaded)}/{_human_size(total)})"
            )
            self._file_percent_cache[name] = percent

    def on_file_complete(self, result: Any) -> None:
        name = getattr(result, "filename", "file")
        task_id = self._active_tasks.pop(name, None)
        size_bytes = self._file_size_bytes.pop(name, None)
        if self._file_progress is not None and task_id is not None:
            try:
                self._file_progress.remove_task(task_id)
            except Exception:
                pass

        self._emit_timeline("DONE", "file", name, size_bytes=size_bytes)

    def on_file_fail(self, result: Any) -> None:
        name = getattr(result, "filename", "file")
        error = getattr(result, "error", None)
        task_id = self._active_tasks.pop(name, None)
        size_bytes = self._file_size_bytes.pop(name, None)
        if self._file_progress is not None and task_id is not None:
            try:
                self._file_progress.remove_task(task_id)
            except Exception:
                pass

        self._emit_timeline("FAIL", "file", name, size_bytes=size_bytes, error=error)

    def on_phase_start(self, phase_name: str, message: str) -> None:
        self._phase = _Phase(phase_name, message)
        self._update_phase_task(phase_name, message, 0, 1)
        if not RICH_AVAILABLE:
            _echo(f"[{phase_name}] {message}")

    def on_phase_progress(self, phase_progress: Any) -> None:
        try:
            phase_name = getattr(phase_progress, "phase", "phase")
            message = getattr(phase_progress, "message", "")
            current = int(getattr(phase_progress, "current", 0) or 0)
            total = int(getattr(phase_progress, "total", 0) or 0)
            self._phase = _Phase(
                name=phase_name,
                message=message,
                current=current,
                total=total,
            )
        except Exception:
            return
        self._update_phase_task(phase_name, message, current, total)
        if phase_name == "uploading" and message.startswith("Set "):
            normalized = message.lower()
            if " done" in normalized or " failed" in normalized:
                key = f"set::{message}"
                if key not in self._timeline_seen:
                    self._timeline_seen.add(key)
                    set_name = message.split(":", 1)[1].strip() if ":" in message else message
                    status = "FAIL" if " failed" in normalized else "DONE"
                    self._emit_timeline(status, "set", set_name)
        if not RICH_AVAILABLE and total > 0:
            percent = int((current / total) * 100) if total else 0
            cache_key = f"phase::{phase_name}"
            prev = self._file_percent_cache.get(cache_key, -1)
            if percent >= 100 or percent - prev >= FOLDER_FILE_PERCENT_STEP:
                self._file_percent_cache[cache_key] = percent
                _echo(f"[{phase_name}] {percent:3d}% - {message}")

    def on_phase_complete(self, phase_name: str, message: str) -> None:
        self._update_phase_task(phase_name, f"done: {message}", 1, 1)
        if not RICH_AVAILABLE:
            _echo(f"[{phase_name}] done - {message}")

    def on_progress(self, stats: Dict[str, Any]) -> None:
        if not isinstance(stats, dict):
            return
        for key in ("total_files", "uploaded", "failed", "skipped"):
            if key in stats:
                try:
                    self._stats[key] = int(stats.get(key, 0) or 0)
                except Exception:
                    pass
        self._update_overall_task()

    def on_hash_start(self, filename: str) -> None:
        self._update_phase_task("hashing", f"Hashing: {filename}", 0, 1)

    def on_hash_complete(self, hash_progress: Any) -> None:
        filename = getattr(hash_progress, "filename", "")
        current = int(getattr(hash_progress, "current", 0) or 0)
        total = int(getattr(hash_progress, "total", 0) or 0)
        from_cache = bool(getattr(hash_progress, "from_cache", False))
        mode = "cache" if from_cache else "calc"
        self._update_phase_task("hashing", f"{mode}: {filename}", current, total)

    def on_sync_start(self, filename: str) -> None:
        self._update_phase_task("syncing", f"Syncing: {filename}", 0, 1)

    def on_sync_complete(self, filename: str, success: bool) -> None:
        state = "ok" if success else "failed"
        self._update_phase_task("syncing", f"{state}: {filename}", 1, 1)
        if success:
            self._emit_timeline("SYNC", "db", filename)
        else:
            self._emit_timeline("FAIL", "sync", filename)

    def on_check_complete(self, filename: str, exists_in_db: bool, exists_in_mega: bool) -> None:
        state = "db+mega" if exists_in_db and exists_in_mega else "new"
        self._update_phase_task("checking_db", f"{state}: {filename}", 0, 1)

    def on_error(self, error: Exception) -> None:
        self._stop_live()
        self._emit_timeline("FAIL", "process", "folder upload", error=str(error))
        _echo(f"[red]Error:[/red] {error}" if RICH_AVAILABLE else f"Error: {error}")

    def on_finish(self, folder_result: Any) -> None:
        self._stop_live()
        uploaded = getattr(folder_result, "uploaded_files", None)
        total = getattr(folder_result, "total_files", None)
        failed = getattr(folder_result, "failed_files", None)
        skipped = self._stats.get("skipped", 0)
        _echo(
            f"[bold]Finished[/bold] uploaded={uploaded} total={total} failed={failed} skipped={skipped}"
            if RICH_AVAILABLE
            else f"Finished: uploaded={uploaded} total={total} failed={failed} skipped={skipped}"
        )
