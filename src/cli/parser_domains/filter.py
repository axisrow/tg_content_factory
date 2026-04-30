from __future__ import annotations

import argparse


def register(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser | None:
    flt_parser = subparsers.add_parser("filter", help="Channel content filter")
    flt_sub = flt_parser.add_subparsers(dest="filter_action")
    flt_sub.add_parser("analyze", help="Analyze channels and show report")
    flt_sub.add_parser("apply", help="Analyze and mark filtered channels")
    flt_sub.add_parser("reset", help="Reset all channel filters")
    flt_sub.add_parser("precheck", help="Apply pre-filter by subscriber ratio (no Telegram needed)")
    flt_toggle = flt_sub.add_parser("toggle", help="Toggle filter for a single channel")
    flt_toggle.add_argument("pk", type=int, help="Channel primary key")
    flt_purge = flt_sub.add_parser("purge", help="Purge messages from filtered channels")
    flt_purge.add_argument("--pks", default=None, help="Comma-separated PKs (default: all)")
    flt_purge_msgs = flt_sub.add_parser(
        "purge-messages",
        help="Delete messages for a specific channel from DB",
    )
    flt_purge_msgs.add_argument("--channel-id", type=int, required=True, dest="channel_id",
                                help="Channel ID whose messages to delete")
    flt_purge_msgs.add_argument("--yes", "-y", action="store_true", help="Skip confirmation prompt")

    flt_hard = flt_sub.add_parser(
        "hard-delete",
        help="Hard-delete filtered channels from DB (dev/testing)",
    )
    flt_hard.add_argument("--pks", default=None, help="Comma-separated PKs (default: all)")
    flt_hard.add_argument("--yes", action="store_true", help="Skip confirmation prompt")
