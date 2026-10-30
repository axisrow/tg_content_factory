from __future__ import annotations

import json
import os
import re
import shlex
import signal
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path

from tg_agent.config import GLOBAL_DB_NAME, AppConfig, load_config, read_global_store_marker
from tg_agent.telegram.session_lock import SessionLock, SessionLockError
from tg_agent.utils.process_identity import process_executable, process_start_identity


class ProcessControlError(RuntimeError):
    """Raised when server process control cannot proceed safely."""


class StopResult(Enum):
    STOPPED = "stopped"
    NOT_RUNNING = "not_running"
    STALE_PID = "stale_pid"
    UNMANAGED = "unmanaged"
    TIMEOUT = "timeout"


# Module and console-script invocations that own the sessions and are managed
# via the PID file. `restart` is itself a daemon: after stopping the previous
# process it runs the worker in-process, so its cmdline stays `... restart`
# (#automation-first default — the managed daemon has no web panel).
_DAEMON_COMMANDS = {"serve", "worker", "restart"}
_WORKER_LOCKS: dict[Path, SessionLock] = {}


@dataclass(frozen=True)
class StopOutcome:
    result: StopResult
    message: str


def pid_file_path(config: AppConfig) -> Path:
    db_path = Path(config.database.path)
    return db_path.with_suffix(".pid")


def pid_file_candidates(config_path: str) -> list[Path]:
    """Every PID file a daemon of this project may be registered at (#1698).

    The path resolves from the *current* config, but a running daemon keeps
    the path it was launched with: a store switch (global enable/migrate/
    disable, legacy markers — #1724) moves ``database.path`` underneath it,
    and the documented stop/restart flow then looks in the wrong place.
    Candidates, in check order:

    * the current config (marker applied),
    * the same config without the marker (the pre-switch database path),
    * the legacy plain-path marker's store (a pre-#1724 daemon keeps its PID
      file there until someone stops it).

    Duplicates collapse; the current store comes first.
    """
    candidates = [
        pid_file_path(load_config(config_path)),
        pid_file_path(load_config(config_path, apply_marker=False)),
    ]
    marker_root = read_global_store_marker()
    if marker_root is not None:
        candidates.append((marker_root / GLOBAL_DB_NAME).with_suffix(".pid"))
    unique: list[Path] = []
    for candidate in candidates:
        if candidate not in unique:
            unique.append(candidate)
    return unique


def ensure_no_sibling_daemon(pid_path: Path, config_path: str) -> None:
    """Refuse to start when a live project daemon is registered elsewhere (#1698).

    ``restart`` stops every candidate first, so a fresh ``worker``/``serve``
    start that still sees a live daemon on another candidate path is a
    double-spawn that ends degraded (the second worker loses every session
    lock and loops "Pending-task ingest throttled" forever).
    """
    for candidate in pid_file_candidates(config_path):
        if candidate.resolve() == Path(pid_path).resolve():
            continue  # ensure_server_not_running owns the registration path itself
        pid = read_pid(candidate)
        if pid is not None and pid != os.getpid() and is_expected_server_process(pid):
            owner = read_process_owner(candidate)
            provenance = ""
            stop_hint = "its owning project"
            if owner is not None:
                provenance = (
                    f" (project {owner.get('project_root')}, store {owner.get('store_root')}, "
                    f"started {owner.get('started_at')})"
                )
                stop_hint = str(owner.get("project_root", stop_hint))
            raise ProcessControlError(
                f"Refusing to start: this project's daemon is already running on another "
                f"store path — PID {pid}, PID file {candidate}{provenance}. Stop it first "
                f"(tg-agent stop from {stop_hint}) or inspect with tg-agent daemon status."
            )


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
    except ValueError as exc:
        raise ProcessControlError(f"Invalid PID file contents: {path}") from exc


def owner_file_path(path: Path) -> Path:
    """Keep provenance beside the numeric PID file for older readers."""
    return path.with_name(path.name + ".owner.json")


def read_process_owner(path: Path) -> dict | None:
    """Read provenance, refusing malformed records rather than losing ownership."""
    try:
        raw = owner_file_path(path).read_text(encoding="utf-8")
    except FileNotFoundError:
        return None
    try:
        owner = json.loads(raw)
        if (
            not isinstance(owner, dict)
            or type(owner.get("pid")) is not int
            or not isinstance(owner.get("project_root"), str)
            or not owner["project_root"]
        ):
            raise ValueError("Missing process ownership")
    except (ValueError, TypeError) as exc:
        raise ProcessControlError(f"Invalid process owner record: {owner_file_path(path)}") from exc
    return owner


def _process_command(pid: int) -> str:
    if sys.platform.startswith("linux"):
        cmdline_path = Path("/proc") / str(pid) / "cmdline"
        try:
            raw = cmdline_path.read_bytes()
        except FileNotFoundError:
            return ""
        return shlex.join(part.decode("utf-8", errors="replace") for part in raw.split(b"\x00") if part)

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
    command = result.stdout.strip()
    # ps renders argv without quoting. Recover the executable boundary from
    # comm rather than splitting a venv path containing spaces into words.
    try:
        executable_result = subprocess.run(
            ["ps", "-o", "comm=", "-p", str(pid)],
            capture_output=True,
            check=False,
            text=True,
        )
    except OSError:
        return command
    executable = executable_result.stdout.strip()
    if executable_result.returncode == 0 and executable and command.startswith(executable + " "):
        arguments = command[len(executable) + 1:]
        # A pip-installed console script normally shares the interpreter's bin
        # directory. Its path also needs quoting when that directory has spaces.
        console_script = str(Path(executable).with_name("tg-agent"))
        if arguments.startswith(console_script + " "):
            arguments = shlex.quote(console_script) + arguments[len(console_script):]
        return shlex.quote(executable) + " " + arguments
    return command


def is_expected_server_process(pid: int) -> bool:
    if pid <= 0 or not is_process_alive(pid):
        return False

    command = _process_command(pid)
    if not command:
        return False

    try:
        tokens = shlex.split(command)
    except ValueError:
        return False
    if not tokens:
        return False

    executable = Path(tokens.pop(0)).name
    # Case-insensitive: macOS reports the python.org framework binary's comm as
    # "Python" (the .app bundle name), and the hand-off must recognize it too.
    if re.fullmatch(r"(?:python|pypy)(?:\d+(?:\.\d+)*)?", executable, re.IGNORECASE):
        # Only inspect Python's actual module/script, never arguments to -c or
        # another program (e.g. a test mentioning "tg-agent worker").
        while tokens and tokens[0].startswith("-"):
            option = tokens.pop(0)
            if option == "-m":
                if not tokens or tokens.pop(0) != "tg_agent.main":
                    return False
                break
            if option in {"-W", "-X"}:
                if not tokens:
                    return False
                tokens.pop(0)
            elif option.startswith(("-W", "-X")):
                continue
            elif len(option) < 2 or any(flag not in "uBOEIsSPqb" for flag in option[1:]):
                return False
        else:
            if not tokens or Path(tokens.pop(0)).name != "tg-agent":
                return False
    elif executable != "tg-agent":
        return False

    # --config consumes its value even if that value happens to be "worker".
    # Fail closed on unknown root options; eager help/version never run a daemon.
    while tokens:
        option = tokens.pop(0)
        if option == "--config":
            if not tokens:
                return False
            tokens.pop(0)
        elif option.startswith("--config="):
            continue
        elif option == "--":
            return bool(tokens) and tokens[0] in _DAEMON_COMMANDS
        else:
            return option in _DAEMON_COMMANDS
    return False


def remove_pid_file(path: Path) -> None:
    try:
        path.unlink()
    except FileNotFoundError:
        pass
    owner_file_path(path).unlink(missing_ok=True)


def ensure_server_not_running(path: Path) -> None:
    pid = read_pid(path)
    if pid is None:
        return

    if pid == os.getpid():
        remove_pid_file(path)
        return

    if is_expected_server_process(pid):
        raise ProcessControlError(f"Server already running with PID {pid}")

    remove_pid_file(path)


def register_current_process(path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    canonical = path.resolve()
    lock = _WORKER_LOCKS.get(canonical)
    if lock is None:
        lock = SessionLock("project-worker", str(canonical), lock_dir=path.parent / ".worker-locks")
        try:
            lock.acquire(grace_seconds=0)
        except SessionLockError as exc:
            raise ProcessControlError(f"Another worker owns this project database: {canonical}") from exc
        try:
            ensure_server_not_running(path)
        except BaseException:
            lock.release()
            raise
        _WORKER_LOCKS[canonical] = lock
    owner = {
        "pid": os.getpid(),
        "role": "worker",
        "project_root": str(Path.cwd().resolve()),
        "store_root": str(path.resolve().parent),
        "executable": str(Path(sys.executable).resolve()),
        # macOS may report the Python.app bundle binary for `ps comm` even when
        # sys.executable is bin/python3.13; record the observable self-view too.
        "executable_observed": process_executable(os.getpid()),
        "started_at": datetime.now(timezone.utc).isoformat(),
        "command": _process_command(os.getpid()),
        "start_identity": process_start_identity(os.getpid()),
    }
    target = owner_file_path(path)
    temporary = target.with_name(f"{target.name}.{os.getpid()}.tmp")
    try:
        with temporary.open("x", encoding="utf-8") as handle:
            os.chmod(temporary, 0o600)
            json.dump(owner, handle)
        temporary.replace(target)
    finally:
        temporary.unlink(missing_ok=True)
    path.write_text(f"{os.getpid()}\n", encoding="utf-8")


def unregister_current_process(path: Path) -> None:
    try:
        pid = read_pid(path)
        if pid == os.getpid():
            remove_pid_file(path)
    except Exception:
        # PID cleanup must never block shutdown; stale/unreadable files are safe to ignore.
        pass
    finally:
        lock = _WORKER_LOCKS.pop(path.resolve(), None)
        if lock is not None:
            lock.release()


def stop_server(
    path: Path,
    timeout_sec: float = 150.0,
    kill_timeout_sec: float = 5.0,
    force: bool = False,
) -> StopOutcome:
    pid = read_pid(path)
    if pid is None:
        return StopOutcome(
            StopResult.NOT_RUNNING,
            f"Server is not running (no PID file: {path}).",
        )

    if not is_process_alive(pid):
        remove_pid_file(path)
        return StopOutcome(
            StopResult.STALE_PID,
            f"Removed stale PID file: {path}.",
        )

    # The managed-process check stays in force under --force: an unknown PID
    # is never signalled, escape hatch or not (#1714).
    if not is_expected_server_process(pid):
        return StopOutcome(
            StopResult.UNMANAGED,
            f"PID {pid} is not a managed tg-agent daemon process.",
        )

    owner = None
    if not force:
        # Provenance verification (#1714): refuse unless the caller's project
        # owns this daemon. stop --force is the explicit operator escape hatch.
        owner = read_process_owner(path)
        if owner is None:
            return StopOutcome(StopResult.UNMANAGED,
                f"Cannot verify provenance for legacy PID {pid}; refusing to signal it. "
                "Stop the original worker from its owning installation, verify it "
                "manually, or pass --force.")
        observed_executable = process_executable(pid)
        if (owner.get("role") != "worker" or owner.get("store_root") != str(path.resolve().parent)
                or not owner.get("start_identity") or not owner.get("executable")
                or (owner["executable"] != observed_executable
                    and owner.get("executable_observed") != observed_executable)):
            return StopOutcome(StopResult.UNMANAGED,
                               f"Cannot verify role, storage, executable and start identity for PID {pid}; "
                               f"recorded executable {owner.get('executable')!r}, observed {observed_executable!r}.")
        if owner is not None:
            if owner["pid"] != pid or owner["project_root"] != str(Path.cwd().resolve()):
                return StopOutcome(
                    StopResult.UNMANAGED,
                    f"Refusing to stop PID {pid}: daemon belongs to project {owner['project_root']} "
                    f"(store {owner.get('store_root', path.parent)}). Run process control from that "
                    f"project, or pass --force from this one.",
                )
            recorded_command = owner.get("command")
            if recorded_command and recorded_command != _process_command(pid):
                return StopOutcome(StopResult.UNMANAGED, f"PID {pid} no longer matches its process owner record.")
            if owner.get("start_identity") and owner["start_identity"] != process_start_identity(pid):
                return StopOutcome(StopResult.UNMANAGED, f"PID {pid} has been reused; refusing to signal it.")

    try:
        os.kill(pid, signal.SIGTERM)
    except ProcessLookupError:
        remove_pid_file(path)
        return StopOutcome(
            StopResult.STALE_PID,
            f"Removed stale PID file: {path}.",
        )
    except PermissionError as exc:
        raise ProcessControlError(f"Permission denied sending signal to PID {pid}") from exc

    deadline = time.monotonic() + timeout_sec
    while time.monotonic() < deadline:
        if not is_process_alive(pid):
            remove_pid_file(path)
            return StopOutcome(
                StopResult.STOPPED,
                f"Server stopped (PID {pid}).",
            )
        time.sleep(0.1)

    identity_now = process_start_identity(pid)
    if identity_now is not None and identity_now != (owner or {}).get("start_identity"):
        # A vanished process reports no identity: exiting is the desired outcome.
        return StopOutcome(StopResult.UNMANAGED, f"PID {pid} changed during shutdown; refusing force kill.")
    try:
        os.kill(pid, signal.SIGKILL)
    except ProcessLookupError:
        remove_pid_file(path)
        return StopOutcome(
            StopResult.STOPPED,
            f"Server stopped (PID {pid}).",
        )
    except PermissionError as exc:
        raise ProcessControlError(f"Permission denied sending signal to PID {pid}") from exc

    if not is_process_alive(pid):
        remove_pid_file(path)
        return StopOutcome(
            StopResult.STOPPED,
            f"Server stopped after force kill (PID {pid}).",
        )

    kill_deadline = time.monotonic() + kill_timeout_sec
    while time.monotonic() < kill_deadline:
        if not is_process_alive(pid):
            remove_pid_file(path)
            return StopOutcome(
                StopResult.STOPPED,
                f"Server stopped after force kill (PID {pid}).",
            )
        time.sleep(0.05)

    return StopOutcome(
        StopResult.TIMEOUT,
        f"Timed out waiting for server PID {pid} to stop.",
    )
