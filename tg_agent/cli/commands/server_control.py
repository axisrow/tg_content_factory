from __future__ import annotations

import argparse
import sys

from tg_agent.cli.commands import worker
from tg_agent.cli.process_control import (
    ProcessControlError,
    StopResult,
    pid_file_candidates,
    stop_server,
)

_GRACEFUL_STOP_NOTICE = (
    "Останавливаю сервер gracefully. Если сейчас идёт сбор канала, "
    "подожду завершения активной задачи; остальные останутся pending в БД."
)
_GRACEFUL_RESTART_NOTICE = (
    "Перезапускаю воркер gracefully. Если сейчас идёт сбор канала, "
    "подожду завершения активной задачи; остальные останутся pending в БД."
)


def _stop_managed_server(config_path: str, notice: str, *, force: bool = False) -> None:
    """Stop every daemon this project may have registered; exit(1) on refusal.

    PID-file candidates (#1698): a daemon keeps its launch-time store path
    across a store switch, so stop/restart probe all of them and terminate
    whichever is alive. UNMANAGED/TIMEOUT exit non-zero (#1714: a daemon
    owned by another project is never signalled — provenance is printed;
    ``stop --force`` is the explicit operator escape hatch).
    """
    candidates = pid_file_candidates(config_path)
    try:
        print(notice)
        outcomes = [stop_server(path, force=force) for path in candidates]
    except ProcessControlError as exc:
        print(str(exc))
        sys.exit(1)
    if all(outcome.result is StopResult.NOT_RUNNING for outcome in outcomes):
        paths = ", ".join(str(path) for path in candidates)
        print(f"Server is not running (no PID file: {paths}).")
        return
    for outcome in outcomes:
        print(outcome.message)
    if any(outcome.result in (StopResult.UNMANAGED, StopResult.TIMEOUT) for outcome in outcomes):
        sys.exit(1)


def stop_web(config_path: str, *, force: bool = False) -> None:
    """Stop the managed daemon(s) started by this app (graceful)."""
    _stop_managed_server(config_path, _GRACEFUL_STOP_NOTICE, force=force)


def _restart_failed_banner() -> None:
    print(
        "RESTART FAILED: the previous daemon was stopped, but the new worker did "
        "not start — NO daemon is running now. Read the startup error above, fix "
        "it, then run `tg-agent restart` (or `worker`) again; `tg-agent daemon "
        "status` reports the current state.",
        file=sys.stderr,
    )


def restart_web(config_path: str) -> None:
    """Restart the managed daemon: stop gracefully, then run the worker.

    The automation-first default daemon is the worker runtime WITHOUT the web
    panel — no web server, no WEB_PASS. To host the (deprecated) web panel
    use ``serve`` explicitly. A failed start phase is loud (#1714): the stop
    phase already terminated the old daemon, so silence would hide a
    zero-daemon machine.
    """
    _stop_managed_server(config_path, _GRACEFUL_RESTART_NOTICE)
    try:
        worker.serve_worker(config_path)
    except SystemExit as exc:
        if exc.code in (None, 0):
            raise
        _restart_failed_banner()
        raise
    except Exception:
        _restart_failed_banner()
        raise


def run_stop(args: argparse.Namespace) -> None:
    stop_web(args.config)


def run_restart(args: argparse.Namespace) -> None:
    restart_web(args.config)
