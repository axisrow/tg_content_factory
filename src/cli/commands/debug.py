"""Shared async bodies for the ``debug`` CLI group (epic #959, Wave 2 — #1122).

Migrated off the argparse dispatcher onto the Typer ``app`` (see
``src/cli/typer_commands.py``). Each leaf sub-command is a plain ``async def
*_impl`` here — no local ``asyncio.run`` (the Typer commands funnel through the
single ``run_async`` bridge) and no ``argparse.Namespace`` (Typer passes the
resolved flags as keyword arguments).
"""

from __future__ import annotations

import argparse
import asyncio
import os
import resource
from collections import deque

from src.cli import runtime
from src.cli.runtime import APP_LOG_PATH


async def logs_impl(config_path: str, *, limit: int = 50) -> None:
    """Print the last *limit* lines of the app log."""
    _, db = await runtime.init_db(config_path)
    try:
        if APP_LOG_PATH.exists():
            with open(APP_LOG_PATH, encoding="utf-8", errors="replace") as f:
                tail = deque(f, maxlen=limit)
            for line in tail:
                print(line, end="")
        else:
            print(f"No log file found at {APP_LOG_PATH}")
            print("Tip: start the server first — logs are written to data/app.log.")
    finally:
        await db.close()


async def memory_impl(config_path: str) -> None:
    """Print process RSS, DB stats and DB file size."""
    _, db = await runtime.init_db(config_path)
    try:
        usage = resource.getrusage(resource.RUSAGE_SELF)
        print(f"Max RSS: {usage.ru_maxrss / 1024:.1f} MB")

        stats = await db.get_stats()
        print("\nDB stats:")
        for key, value in stats.items():
            print(f"  {key}: {value}")

        db_path = db._path if hasattr(db, "_path") else "unknown"
        if db_path and db_path != ":memory:" and os.path.exists(db_path):
            size_mb = os.path.getsize(db_path) / (1024 * 1024)
            print(f"  DB file size: {size_mb:.1f} MB")
    finally:
        await db.close()


async def timing_impl(config_path: str) -> None:
    """Print operation timing stats (no persistent data collected yet)."""
    _, db = await runtime.init_db(config_path)
    try:
        print("Operation timing stats:")
        print("  (no persistent timing data collected)")
        print("  Tip: use 'test benchmark' for pytest serial vs parallel benchmarks")
    finally:
        await db.close()


def run(args: argparse.Namespace) -> None:
    """Thin argparse adapter over the ``*_impl`` bodies (legacy dispatch path).

    The production CLI routes ``debug`` through the Typer ``app`` (#1122); this
    wrapper is kept so the argparse ``build_parser()`` leaf audit and the existing
    command-level tests still exercise the shared bodies.
    """
    action = args.debug_action
    if action == "logs":
        asyncio.run(logs_impl(args.config, limit=args.limit))
    elif action == "memory":
        asyncio.run(memory_impl(args.config))
    elif action == "timing":
        asyncio.run(timing_impl(args.config))
