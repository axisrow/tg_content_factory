from __future__ import annotations

import argparse
import logging
import sys

from src.cli.process_control import (
    pid_file_path,
    register_current_process,
    unregister_current_process,
)
from src.config import load_config
from src.runtime.worker import run_worker


def serve_worker(config_path: str) -> None:
    """Start the standalone Telegram worker runtime as the managed daemon.

    Shared body for every automation-first daemon entry: the Typer ``worker``
    command, the argparse ``run`` wrapper below, and ``restart`` (which stops
    the previous daemon, then calls this) all land here. Registers the same
    PID file ``serve`` uses, so ``stop``/``restart`` and the CLI worker
    hand-off see it; ``run_worker`` owns its own event loop, so this stays a
    plain ``def``. No web panel, no WEB_PASS — the automation contract has no
    human-facing surface.
    """
    config = load_config(config_path)
    pid_path = pid_file_path(config)
    try:
        register_current_process(pid_path)
    except RuntimeError as exc:
        logging.error(str(exc))
        sys.exit(1)

    try:
        run_worker(config)
    except KeyboardInterrupt:
        pass
    finally:
        unregister_current_process(pid_path)


def run(args: argparse.Namespace) -> None:
    serve_worker(args.config)
