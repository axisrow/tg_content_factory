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

import typer

from src.cli import runtime
from src.cli.commands.common import (
    apply_startup,
    run_async,
)
from src.cli.runtime import APP_LOG_PATH


def _db_file_size_mb(db_path: str) -> float | None:
    if db_path and db_path != ":memory:" and os.path.exists(db_path):
        return os.path.getsize(db_path) / (1024 * 1024)
    return None


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
        size_mb = await asyncio.to_thread(_db_file_size_mb, db_path)
        if size_mb is not None:
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


async def errors_impl(config_path: str, *, as_json: bool = False) -> None:
    """Print aggregated provider error-recovery stats (#1055).

    Folds the error histories of every live ``ErrorRecoveryService`` instance
    (the per-service LLM/embedding recovery wrappers) into one process-wide
    view. In a short-lived CLI invocation no provider call has run, so the
    aggregate is usually empty — the stats are meaningful in a long-running
    ``serve``/``worker`` process (or the Web debug page) where provider calls
    have actually happened.
    """
    from src.services.error_recovery_service import ErrorRecoveryService

    _, db = await runtime.init_db(config_path)
    try:
        stats = ErrorRecoveryService.aggregate_error_stats()
        if as_json:
            import json

            print(json.dumps(stats, indent=2, ensure_ascii=False))
            return

        print("Provider error-recovery stats (aggregated across live instances):")
        print(f"  Live recovery instances: {stats['instances']}")
        print(f"  Total errors recorded:   {stats['total_errors']}")
        print(f"  Open circuit breakers:   {stats['open_circuits']}")

        by_category = stats.get("by_category") or {}
        if by_category:
            print("  By category:")
            for cat, count in sorted(by_category.items()):
                print(f"    {cat}: {count}")

        recent = stats.get("recent") or []
        if recent:
            print("  Recent errors (newest first):")
            for r in recent:
                print(f"    [{r.get('category')}] {r.get('type')}: {r.get('message')}")

        if stats["total_errors"] == 0:
            print("  (no provider errors recorded in this process)")
    finally:
        await db.close()


def run(args: argparse.Namespace) -> None:
    """Thin argparse-Namespace adapter over the ``*_impl`` bodies.

    The production CLI routes ``debug`` through the Typer ``app`` (#1122). This
    wrapper is kept so the existing command-level tests that drive the legacy
    ``commands.debug.run(Namespace)`` path still exercise the shared bodies.
    """
    action = args.debug_action
    if action == "logs":
        asyncio.run(logs_impl(args.config, limit=args.limit))
    elif action == "memory":
        asyncio.run(memory_impl(args.config))
    elif action == "timing":
        asyncio.run(timing_impl(args.config))
    elif action == "errors":
        asyncio.run(errors_impl(args.config, as_json=getattr(args, "json", False)))


# --------------------------------------------------------------------------- #
# debug → logs / memory / timing
# --------------------------------------------------------------------------- #

debug_app = typer.Typer(no_args_is_help=True, help="Diagnostic tools")


@debug_app.command("logs")
def debug_logs(
    ctx: typer.Context,
    limit: int = typer.Option(50, "--limit", help="Number of log lines (default: 50)"),
) -> None:
    """Show recent log entries."""
    apply_startup(ctx)
    run_async(logs_impl(ctx.obj.config, limit=limit))


@debug_app.command("memory")
def debug_memory(ctx: typer.Context) -> None:
    """Show memory usage statistics."""
    apply_startup(ctx)
    run_async(memory_impl(ctx.obj.config))


@debug_app.command("timing")
def debug_timing(ctx: typer.Context) -> None:
    """Show operation timing stats."""
    apply_startup(ctx)
    run_async(timing_impl(ctx.obj.config))


@debug_app.command("errors")
def debug_errors(
    ctx: typer.Context,
    as_json: bool = typer.Option(False, "--json", help="Emit raw JSON instead of text"),
) -> None:
    """Show aggregated provider error-recovery stats (#1055)."""
    apply_startup(ctx)
    run_async(errors_impl(ctx.obj.config, as_json=as_json))
