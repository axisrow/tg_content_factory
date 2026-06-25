from __future__ import annotations

import argparse

from src.config import load_config
from src.runtime.worker import run_worker


def serve_worker(config_path: str) -> None:
    """Start the standalone Telegram worker runtime.

    Shared body for both CLI entry points: the argparse ``run`` wrapper below
    and the Typer ``worker`` command (``src/cli/typer_commands.py``) call this
    with the resolved ``--config`` path. ``run_worker`` owns its own event loop,
    so this stays a plain ``def`` (no async-bridge needed).
    """
    config = load_config(config_path)
    run_worker(config)


def run(args: argparse.Namespace) -> None:
    serve_worker(args.config)
