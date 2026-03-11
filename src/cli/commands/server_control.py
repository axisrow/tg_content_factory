from __future__ import annotations

import argparse
import sys

from src.cli.commands import serve
from src.cli.process_control import ProcessControlError, pid_file_path, stop_server
from src.config import load_config


def run_stop(args: argparse.Namespace) -> None:
    config = load_config(args.config)
    try:
        stopped, message = stop_server(pid_file_path(config))
    except ProcessControlError as exc:
        print(str(exc))
        sys.exit(1)
    print(message)
    can_continue = (
        stopped
        or message.startswith("Removed stale PID file")
        or "not running" in message
    )
    if not can_continue:
        sys.exit(1)


def run_restart(args: argparse.Namespace) -> None:
    config = load_config(args.config)
    try:
        stopped, message = stop_server(pid_file_path(config))
    except ProcessControlError as exc:
        print(str(exc))
        sys.exit(1)
    print(message)
    can_continue = (
        stopped
        or message.startswith("Removed stale PID file")
        or "not running" in message
    )
    if not can_continue:
        sys.exit(1)
    serve.run(args)
