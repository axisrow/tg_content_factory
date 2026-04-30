from __future__ import annotations

import argparse


def register(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser | None:
    # ── export ──
    export_parser = subparsers.add_parser("export", help="Export collected messages")
    export_sub = export_parser.add_subparsers(dest="export_action")
    for fmt_name in ("json", "csv", "rss"):
        exp = export_sub.add_parser(fmt_name, help=f"Export as {fmt_name.upper()}")
        exp.add_argument("--channel-id", type=int, default=None, dest="channel_id",
                         help="Filter by channel ID")
        exp.add_argument("--limit", type=int, default=200, help="Max messages (default: 200)")
        exp.add_argument("--output", "-o", default=None, help="Output file (default: stdout)")
