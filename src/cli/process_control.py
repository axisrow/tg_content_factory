from __future__ import annotations

import os
import signal
import subprocess
import sys
import time
from pathlib import Path

from src.config import AppConfig


class ProcessControlError(RuntimeError):
    """Raised when server process control cannot proceed safely."""


def pid_file_path(config: AppConfig) -> Path:
    db_path = Path(config.database.path)
    return db_path.with_suffix(".pid")


def is_process_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def read_pid(path: Path) -> int | None:
    try:
        raw = path.read_text(encoding="utf-8").strip()
    except FileNotFoundError:
        return None

    if not raw:
        return None

    try:
        return int(raw)
    except ValueError:
        raise ProcessControlError(f"Invalid PID file contents: {path}")


def _process_command(pid: int) -> str:
    if sys.platform.startswith("linux"):
        cmdline_path = Path("/proc") / str(pid) / "cmdline"
        try:
            raw = cmdline_path.read_bytes()
        except FileNotFoundError:
            return ""
        return raw.replace(b"\x00", b" ").decode("utf-8", errors="ignore").strip()

    try:
        result = subprocess.run(
            ["ps", "-o", "command=", "-p", str(pid)],
            capture_output=True,
            check=False,
            text=True,
        )
    except OSError:
        return ""

    if result.returncode != 0:
        return ""
    return result.stdout.strip()


def is_expected_server_process(pid: int) -> bool:
    if pid <= 0 or not is_process_alive(pid):
        return False

    command = _process_command(pid)
    if not command:
        return False

    return "src.main" in command and "serve" in command


def remove_pid_file(path: Path) -> None:
    try:
        path.unlink()
    except FileNotFoundError:
        pass


def ensure_server_not_running(path: Path) -> None:
    pid = read_pid(path)
    if pid is None:
        return

    if is_expected_server_process(pid):
        raise ProcessControlError(f"Server already running with PID {pid}")

    remove_pid_file(path)


def register_current_process(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    ensure_server_not_running(path)
    path.write_text(f"{os.getpid()}\n", encoding="utf-8")


def unregister_current_process(path: Path) -> None:
    pid = read_pid(path)
    if pid == os.getpid():
        remove_pid_file(path)


def stop_server(path: Path, timeout_sec: float = 10.0) -> tuple[bool, str]:
    pid = read_pid(path)
    if pid is None:
        return False, f"Server is not running (no PID file: {path})."

    if not is_process_alive(pid):
        remove_pid_file(path)
        return False, f"Removed stale PID file: {path}."

    if not is_expected_server_process(pid):
        return False, f"PID {pid} is not a managed src.main serve process."

    os.kill(pid, signal.SIGTERM)

    deadline = time.monotonic() + timeout_sec
    while time.monotonic() < deadline:
        if not is_process_alive(pid):
            remove_pid_file(path)
            return True, f"Server stopped (PID {pid})."
        time.sleep(0.1)

    return False, f"Timed out waiting for server PID {pid} to stop."
