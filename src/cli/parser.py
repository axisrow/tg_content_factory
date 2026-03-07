from __future__ import annotations

import argparse


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="TG Post Search")
    parser.add_argument("--config", default="config.yaml", help="Path to config file")
    sub = parser.add_subparsers(dest="command")

    serve_parser = sub.add_parser("serve", help="Start web server")
    serve_parser.add_argument("--web-pass", help="Web panel password (overrides config)")

    collect_parser = sub.add_parser("collect", help="Run one-shot collection")
    collect_parser.add_argument(
        "--channel-id", type=int, default=None,
        help="Collect single channel by channel_id (full mode)",
    )

    search_parser = sub.add_parser("search", help="Search messages")
    search_parser.add_argument("query", help="Search query")
    search_parser.add_argument("--limit", type=int, default=20, help="Max results")
    search_parser.add_argument(
        "--mode",
        choices=["local", "telegram", "my_chats", "channel"],
        default="local",
        help="Search mode: local, telegram, my_chats, channel",
    )
    search_parser.add_argument(
        "--channel-id", type=int, default=None,
        help="Channel ID for --mode=channel",
    )

    ch_parser = sub.add_parser("channel", help="Channel management")
    ch_sub = ch_parser.add_subparsers(dest="channel_action")

    ch_sub.add_parser("list", help="List channels with message counts")
    ch_add = ch_sub.add_parser("add", help="Add channel by identifier")
    ch_add.add_argument("identifier", help="Username, link, or numeric ID")

    ch_del = ch_sub.add_parser("delete", help="Delete channel")
    ch_del.add_argument("identifier", help="Channel pk, channel_id, or @username")

    ch_toggle = ch_sub.add_parser("toggle", help="Toggle channel active state")
    ch_toggle.add_argument("identifier", help="Channel pk, channel_id, or @username")

    ch_collect = ch_sub.add_parser("collect", help="Collect single channel (full)")
    ch_collect.add_argument("identifier", help="Channel pk, channel_id, or @username")

    ch_stats = ch_sub.add_parser("stats", help="Collect channel statistics")
    ch_stats.add_argument(
        "identifier", nargs="?", default=None,
        help="Channel pk, channel_id, or @username",
    )
    ch_stats.add_argument(
        "--all", action="store_true",
        help="Collect stats for all active channels",
    )

    ch_sub.add_parser("refresh-types", help="Fill missing channel_type for existing channels")

    ch_import = ch_sub.add_parser("import", help="Bulk import from file or text")
    ch_import.add_argument("source", help="Path to .txt/.csv file, or comma-separated identifiers")

    flt_parser = sub.add_parser("filter", help="Channel content filter")
    flt_sub = flt_parser.add_subparsers(dest="filter_action")
    flt_sub.add_parser("analyze", help="Analyze channels and show report")
    flt_sub.add_parser("apply", help="Analyze and mark filtered channels")
    flt_sub.add_parser("reset", help="Reset all channel filters")
    flt_sub.add_parser("precheck", help="Apply pre-filter by subscriber ratio (no Telegram needed)")

    kw_parser = sub.add_parser("keyword", help="Keyword management")
    kw_sub = kw_parser.add_subparsers(dest="keyword_action")
    kw_sub.add_parser("list", help="List keywords")

    kw_add = kw_sub.add_parser("add", help="Add keyword")
    kw_add.add_argument("pattern", help="Keyword pattern")
    kw_add.add_argument("--regex", action="store_true", help="Treat pattern as regex")

    kw_del = kw_sub.add_parser("delete", help="Delete keyword")
    kw_del.add_argument("id", type=int, help="Keyword id")

    kw_toggle = kw_sub.add_parser("toggle", help="Toggle keyword active state")
    kw_toggle.add_argument("id", type=int, help="Keyword id")

    acc_parser = sub.add_parser("account", help="Account management")
    acc_sub = acc_parser.add_subparsers(dest="account_action")
    acc_sub.add_parser("list", help="List accounts")

    acc_toggle = acc_sub.add_parser("toggle", help="Toggle account active state")
    acc_toggle.add_argument("id", type=int, help="Account id")

    acc_del = acc_sub.add_parser("delete", help="Delete account")
    acc_del.add_argument("id", type=int, help="Account id")

    sched_parser = sub.add_parser("scheduler", help="Scheduler control")
    sched_sub = sched_parser.add_subparsers(dest="scheduler_action")
    sched_sub.add_parser("start", help="Start scheduler (foreground)")
    sched_sub.add_parser("trigger", help="Trigger one-shot collection")
    sched_sub.add_parser("search", help="Run keyword search now")

    my_tg_parser = sub.add_parser("my-telegram", help="View account dialogs")
    my_tg_sub = my_tg_parser.add_subparsers(dest="my_telegram_action")
    my_tg_list = my_tg_sub.add_parser("list", help="List all dialogs for an account")
    my_tg_list.add_argument("--phone", default=None, help="Account phone (default: first connected)")  # noqa: E501

    notif_parser = sub.add_parser("notification", help="Personal notification bot management")
    notif_sub = notif_parser.add_subparsers(dest="notification_action")
    notif_sub.add_parser("setup", help="Create personal notification bot via BotFather")
    notif_sub.add_parser("status", help="Show notification bot status")
    notif_sub.add_parser("delete", help="Delete notification bot via BotFather")

    return parser
