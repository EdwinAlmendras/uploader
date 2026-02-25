"""Console rendering and progress helpers for uploader CLI."""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, Optional
import time

try:
    from rich.console import Console
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
            _echo(f"  {percent:3d}% ({uploaded}/{total} bytes)")
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
        self._active_tasks: Dict[str, TaskID] = {}
        self._progress = None
        self._live = None

        if RICH_AVAILABLE and Progress and Live and console:
            self._progress = Progress(
                SpinnerColumn(),
                TextColumn("[bold cyan]{task.fields[label]}", justify="left"),
                BarColumn(bar_width=42),
                TextColumn("[progress.percentage]{task.percentage:>3.0f}%"),
                DownloadColumn(),
                TransferSpeedColumn(),
                TimeRemainingColumn(),
                expand=False,
                console=console,
            )

    def _start_live(self) -> None:
        if self._progress is None or self._live is not None:
            return
        self._live = Live(
            self._progress,
            console=console,
            refresh_per_second=5,
            vertical_overflow="visible",
        )
        self._live.start()

    def _stop_live(self) -> None:
        if self._live is None:
            return
        self._live.stop()
        self._live = None

    def on_file_start(self, file_path: Path) -> None:
        name = Path(file_path).name
        try:
            file_size = Path(file_path).stat().st_size
        except OSError:
            file_size = 0

        if self._progress is not None and file_size > LARGE_FILE_THRESHOLD:
            self._start_live()
            task_id = self._progress.add_task("upload", label=name[:60], total=file_size)
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
        if self._progress is not None and task_id is not None:
            try:
                self._progress.update(task_id, completed=uploaded, total=total)
            except Exception:
                pass
            return

        percent = int((uploaded / total) * 100)
        prev = self._file_percent_cache.get(name, -1)
        if percent >= 100 or percent - prev >= FOLDER_FILE_PERCENT_STEP:
            _echo(f"  {name}: {percent:3d}% ({uploaded}/{total} bytes)")
            self._file_percent_cache[name] = percent

    def on_file_complete(self, result: Any) -> None:
        name = getattr(result, "filename", "file")
        task_id = self._active_tasks.pop(name, None)
        if self._progress is not None and task_id is not None:
            try:
                self._progress.remove_task(task_id)
            except Exception:
                pass
            if not self._active_tasks:
                self._stop_live()

        _echo(f"[green]Uploaded:[/green] {name}" if RICH_AVAILABLE else f"Uploaded: {name}")

    def on_file_fail(self, result: Any) -> None:
        name = getattr(result, "filename", "file")
        error = getattr(result, "error", None)
        task_id = self._active_tasks.pop(name, None)
        if self._progress is not None and task_id is not None:
            try:
                self._progress.remove_task(task_id)
            except Exception:
                pass
            if not self._active_tasks:
                self._stop_live()

        suffix = f" - {error}" if error else ""
        _echo(
            f"[red]Failed:[/red] {name}{suffix}"
            if RICH_AVAILABLE
            else f"Failed: {name}{suffix}"
        )

    def on_phase_start(self, phase_name: str, message: str) -> None:
        self._phase = _Phase(phase_name, message)
        _echo(
            f"[bold blue]Phase:[/bold blue] {phase_name} - {message}"
            if RICH_AVAILABLE
            else f"[{phase_name}] {message}"
        )

    def on_phase_progress(self, phase_progress: Any) -> None:
        try:
            self._phase = _Phase(
                name=getattr(phase_progress, "phase", "phase"),
                message=getattr(phase_progress, "message", ""),
                current=int(getattr(phase_progress, "current", 0) or 0),
                total=int(getattr(phase_progress, "total", 0) or 0),
            )
        except Exception:
            return

    def on_phase_complete(self, phase_name: str, message: str) -> None:
        _echo(
            f"[dim]Done:[/dim] {phase_name} - {message}"
            if RICH_AVAILABLE
            else f"[{phase_name}] done - {message}"
        )

    def on_error(self, error: Exception) -> None:
        _echo(f"[red]Error:[/red] {error}" if RICH_AVAILABLE else f"Error: {error}")

    def on_finish(self, folder_result: Any) -> None:
        self._stop_live()
        uploaded = getattr(folder_result, "uploaded_files", None)
        total = getattr(folder_result, "total_files", None)
        failed = getattr(folder_result, "failed_files", None)
        _echo(
            f"[bold]Finished[/bold] uploaded={uploaded} total={total} failed={failed}"
            if RICH_AVAILABLE
            else f"Finished: uploaded={uploaded} total={total} failed={failed}"
        )
