"""Auto-restart of the managed daemon when the checked-out src/ is newer.

The CLI edits src/ but forgets to restart the worker daemon, which then runs
stale code for hours (found live 2026-10-02: daemon from 01:00 kept crashing
in the notification-snapshot path on code fixed later that morning). The
decision logic lives in src/cli/daemon_autorestart.py; the PostToolUse hook
calls the scripts/ shim after every Bash tool call.
"""

from __future__ import annotations

import os
import time
from pathlib import Path

from src.cli.daemon_autorestart import is_stale


def _touch(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("x = 1\n", encoding="utf-8")
    # Ensure a strictly monotonic mtime even on coarse-grained filesystems.
    st = os.stat(path)
    os.utime(path, (st.st_atime, st.st_mtime + 1))


def _write_pid_file(tmp_path: Path) -> Path:
    pid_file = tmp_path / "data" / "tg_search.pid"
    pid_file.parent.mkdir(parents=True, exist_ok=True)
    pid_file.write_text("12345\n", encoding="utf-8")
    return pid_file


def test_is_stale_when_src_newer_than_pid_file(tmp_path):
    pid_file = _write_pid_file(tmp_path)
    _touch(tmp_path / "src" / "cli" / "commands" / "scheduler.py")

    assert is_stale(pid_file, tmp_path / "src") is True


def test_not_stale_when_daemon_fresh(tmp_path):
    pid_file = _write_pid_file(tmp_path)
    _touch(tmp_path / "src" / "main.py")
    # Daemon started after the last src edit.
    st = os.stat(pid_file)
    os.utime(pid_file, (st.st_atime, st.st_mtime + 10))

    assert is_stale(pid_file, tmp_path / "src") is False


def test_not_stale_without_pid_file(tmp_path):
    _touch(tmp_path / "src" / "main.py")

    assert is_stale(tmp_path / "data" / "tg_search.pid", tmp_path / "src") is False


def test_not_stale_when_mtime_equal(tmp_path):
    pid_file = _write_pid_file(tmp_path)
    src_file = tmp_path / "src" / "main.py"
    _touch(src_file)
    src_st = os.stat(src_file)
    os.utime(pid_file, (src_st.st_atime, src_st.st_mtime))

    assert is_stale(pid_file, tmp_path / "src") is False


def test_is_stale_ignores_non_python_files(tmp_path):
    pid_file = _write_pid_file(tmp_path)
    _touch(tmp_path / "src" / "web" / "templates" / "index.html")

    assert is_stale(pid_file, tmp_path / "src") is False


def test_pid_file_mtime_is_the_daemon_start_proxy(tmp_path):
    """The staleness contract: the PID file is (re)written fresh at worker
    startup, so its mtime is the daemon build time."""
    from src.cli.process_control import register_current_process

    pid_file = tmp_path / "tg_search.pid"
    register_current_process(pid_file)

    assert int(pid_file.read_text()) == os.getpid()
    assert abs(pid_file.stat().st_mtime - time.time()) < 60
