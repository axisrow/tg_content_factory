from __future__ import annotations

import argparse


def register(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser | None:
    # ── messages ──
    msg_parser = subparsers.add_parser("messages", help="Read messages from DB or live Telegram")
    msg_sub = msg_parser.add_subparsers(dest="messages_action")
    msg_read = msg_sub.add_parser("read", help="Read messages from a channel/dialog")
    msg_read.add_argument("identifier", help="Channel pk, channel_id, @username, or dialog ID")
    msg_read.add_argument("--limit", type=int, default=50, help="Max messages (default: 50)")
    msg_read.add_argument("--live", action="store_true", help="Read from Telegram instead of DB")
    msg_read.add_argument("--phone", default=None, help="Account phone (for --live)")
    msg_read.add_argument("--query", default="", help="Text filter (DB only)")
    msg_read.add_argument("--date-from", dest="date_from", default=None, help="Start date YYYY-MM-DD (DB only)")
    msg_read.add_argument("--date-to", dest="date_to", default=None, help="End date YYYY-MM-DD (DB only)")
    msg_read.add_argument("--topic-id", type=int, default=None, dest="topic_id", help="Forum topic ID")
    msg_read.add_argument("--offset-id", type=int, default=None, dest="offset_id",
                          help="Read messages before this message ID (--live)")
    msg_read.add_argument(
        "--format", choices=["text", "json", "csv"], default="text", dest="output_format",
        help="Output format (default: text)",
    )
