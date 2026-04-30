from __future__ import annotations

import argparse


def register(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser | None:
    collect_parser = subparsers.add_parser("collect", help="Run one-shot collection")
    collect_parser.add_argument(
        "--channel-id",
        type=int,
        default=None,
        help="Collect single channel by channel_id (full mode)",
    )
    collect_sub = collect_parser.add_subparsers(dest="collect_action")
    collect_sample = collect_sub.add_parser(
        "sample",
        help="Preview last N messages without saving to DB",
    )
    collect_sample.add_argument("channel_id", type=int, help="Channel ID (numeric)")
    collect_sample.add_argument(
        "--limit",
        type=int,
        default=10,
        help="Number of messages to preview (default: 10)",
    )
