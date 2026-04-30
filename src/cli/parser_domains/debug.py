from __future__ import annotations

import argparse


def register(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser | None:
    # ── debug ──
    debug_parser = subparsers.add_parser("debug", help="Diagnostic tools")
    debug_sub = debug_parser.add_subparsers(dest="debug_action")
    debug_logs = debug_sub.add_parser("logs", help="Show recent log entries")
    debug_logs.add_argument("--limit", type=int, default=50, help="Number of log lines (default: 50)")
    debug_sub.add_parser("memory", help="Show memory usage statistics")
    debug_sub.add_parser("timing", help="Show operation timing stats")
