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

    # ── export telegram (Telegram-Desktop-compatible JSON/HTML tree) ──
    tg = export_sub.add_parser("telegram", help="Export as Telegram-Desktop JSON/HTML")
    tg.add_argument("--channel-id", type=int, default=None, dest="channel_id",
                    help="Telegram channel ID to export (required)")
    tg.add_argument("--format", choices=["json", "html", "both"], default="json",
                    dest="export_format", help="Output format (default: json)")
    tg.add_argument("--with-media", action="store_true", default=False, dest="with_media",
                    help="Download media artifacts (enqueues a worker task)")
    tg.add_argument("--wait", action="store_true", default=False,
                    help="With --with-media: poll the enqueued task until it finishes")
    tg.add_argument("--max-file-size", type=int, default=None, dest="max_file_size",
                    help="Skip files larger than N MB (default: from settings or 3)")
    tg.add_argument("--date-from", default=None, dest="date_from", help="Start date YYYY-MM-DD")
    tg.add_argument("--date-to", default=None, dest="date_to", help="End date YYYY-MM-DD")
    tg.add_argument("--limit", type=int, default=5000, help="Max messages (default: 5000)")
    tg.add_argument("--output", "-o", default=None,
                    help="Output directory (default: data/exports/ChatExport_<date>_<channel>)")
