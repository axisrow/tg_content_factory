"""Auto-restart the managed worker daemon when the checked-out code is newer.

The CLI works on src/ but the running daemon keeps the old build until someone
remembers ``python -m src.main restart``. This module holds the restart-decision
logic; ``scripts/restart_daemon_if_stale.py`` is the entry point wired as a
PostToolUse (Bash) hook in the project ``.claude/settings.local.json``:

```json
{
  "hooks": {
    "PostToolUse": [
      {
        "matcher": "Bash",
        "hooks": [{"type": "command", "command": ".venv/bin/python scripts/restart_daemon_if_stale.py"}]
      }
    ]
  }
}
```

Gates (any miss → silent no-op): daemon is a live managed ``src.main`` process,
the working tree is clean (a deliberate checkpoint, not mid-edit — a restart on
a half-applied edit could crash the daemon on broken imports), and some
``src/**/*.py`` is newer than the PID file. The PID file is (re)written at
worker startup (``process_control.register_current_process``), so its mtime is
the daemon's build time. After a successful restart the PID file is the newest
timestamp again, so the hook is self-quiescent.

# ponytail: no lock against two sessions restarting concurrently — restart is
# idempotent (stop+start), the window is narrow, and a double restart is benign.
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]


def is_stale(pid_file: Path, src_root: Path) -> bool:
    """True when a ``src/**/*.py`` file is strictly newer than the PID file."""
    try:
        started_at = pid_file.stat().st_mtime
    except OSError:
        return False

    newest = 0.0
    for path in src_root.rglob("*.py"):
        newest = max(newest, path.stat().st_mtime)
    return newest > started_at


def _is_managed_daemon(pid: int) -> bool:
    """True when the PID belongs to a live ``python -m src.main`` daemon.

    Mirrors ``process_control.is_expected_server_process`` — guards against a
    recycled PID before we send signals / restart over an unrelated process.
    """
    try:
        result = subprocess.run(
            ["ps", "-o", "command=", "-p", str(pid)],
            capture_output=True,
            check=False,
            text=True,
            timeout=5,
        )
    except (OSError, subprocess.TimeoutExpired):
        return False
    if result.returncode != 0:
        return False
    tokens = result.stdout.split()
    for index, token in enumerate(tokens[:-2]):
        if token == "-m" and tokens[index + 1] == "src.main" and tokens[index + 2] in {
            "restart",
            "worker",
            "serve",
        }:
            return True
    return False


def working_tree_clean(repo_root: Path) -> bool:
    result = subprocess.run(
        ["git", "status", "--porcelain"],
        cwd=repo_root,
        capture_output=True,
        check=False,
        text=True,
        timeout=10,
    )
    return result.returncode == 0 and not result.stdout.strip()


def daemon_pid(pid_file: Path) -> int | None:
    """Live managed-daemon PID from the PID file, or None."""
    try:
        pid = int(pid_file.read_text(encoding="utf-8").strip())
    except (OSError, ValueError):
        return None
    if not _is_managed_daemon(pid):
        return None
    return pid


def restart(repo_root: Path) -> None:
    """Detach a ``src.main restart`` so the hook process can exit immediately."""
    log = open(repo_root / "data" / "daemon_auto_restart.log", "ab")  # noqa: SIM115
    subprocess.Popen(
        [sys.executable, "-m", "src.main", "restart"],
        cwd=repo_root,
        stdout=log,
        stderr=subprocess.STDOUT,
        stdin=subprocess.DEVNULL,
        start_new_session=True,
    )


def main() -> None:
    pid_file = REPO_ROOT / "data" / "tg_search.pid"
    if daemon_pid(pid_file) is None:
        return
    if not working_tree_clean(REPO_ROOT):
        return
    if not is_stale(pid_file, REPO_ROOT / "src"):
        return
    restart(REPO_ROOT)
    print("daemon stale -> restarting (src.main restart detached, log: data/daemon_auto_restart.log)")
