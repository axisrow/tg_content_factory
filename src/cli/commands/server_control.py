from __future__ import annotations

import argparse
import sys

from src.cli.commands import serve
from src.cli.process_control import ProcessControlError, StopResult, pid_file_path, stop_server
from src.config import load_config

_GRACEFUL_STOP_NOTICE = (
    "Останавливаю сервер gracefully. Если сейчас идёт сбор канала, "
    "подожду завершения активной задачи; остальные останутся pending в БД."
)
_GRACEFUL_RESTART_NOTICE = (
    "Перезапускаю сервер gracefully. Если сейчас идёт сбор канала, "
    "подожду завершения активной задачи; остальные останутся pending в БД."
)


def _stop_managed_server(config_path: str, notice: str) -> None:
    """Stop the managed server, printing *notice* first; exit(1) on failure.

    Shared by ``stop_web`` and ``restart_web`` — both gracefully terminate the
    running server and exit non-zero if it is unmanaged or never stops.
    """
    config = load_config(config_path)
    try:
        print(notice)
        outcome = stop_server(pid_file_path(config))
    except ProcessControlError as exc:
        print(str(exc))
        sys.exit(1)
    print(outcome.message)
    if outcome.result in (StopResult.UNMANAGED, StopResult.TIMEOUT):
        sys.exit(1)


def stop_web(config_path: str) -> None:
    """Stop the web server started by this app (graceful)."""
    _stop_managed_server(config_path, _GRACEFUL_STOP_NOTICE)


def restart_web(config_path: str, *, web_pass: str | None = None) -> None:
    """Restart the web server: stop gracefully, then start a fresh ``serve``."""
    _stop_managed_server(config_path, _GRACEFUL_RESTART_NOTICE)
    serve.serve_web(config_path, web_pass=web_pass)


def run_stop(args: argparse.Namespace) -> None:
    stop_web(args.config)


def run_restart(args: argparse.Namespace) -> None:
    restart_web(args.config, web_pass=getattr(args, "web_pass", None))
