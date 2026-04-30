from __future__ import annotations

import argparse


def register(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser | None:
    ch_parser = subparsers.add_parser("channel", help="Channel management")
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
        "identifier",
        nargs="?",
        default=None,
        help="Channel pk, channel_id, or @username",
    )
    ch_stats.add_argument(
        "--all",
        action="store_true",
        help="Collect stats for all active channels",
    )

    ch_sub.add_parser("refresh-types", help="Fill missing channel_type for existing channels")

    ch_meta = ch_sub.add_parser("refresh-meta", help="Refresh about/linked_chat_id for channels")
    ch_meta.add_argument(
        "identifier",
        nargs="?",
        default=None,
        help="Channel pk, channel_id, or @username (omit for all)",
    )
    ch_meta.add_argument(
        "--all",
        action="store_true",
        help="Refresh metadata for all active channels",
    )

    ch_import = ch_sub.add_parser("import", help="Bulk import from file or text")
    ch_import.add_argument("source", help="Path to .txt/.csv file, or comma-separated identifiers")

    ch_add_bulk = ch_sub.add_parser("add-bulk", help="Add channels from account dialogs")
    ch_add_bulk.add_argument("--phone", required=True, help="Account phone")
    ch_add_bulk.add_argument(
        "--dialog-ids", required=True, dest="dialog_ids",
        help="Comma-separated dialog IDs to add as channels",
    )

    # ── channel tag ──
    ch_tag_parser = ch_sub.add_parser("tag", help="Manage channel tags")
    ch_tag_sub = ch_tag_parser.add_subparsers(dest="tag_action")
    ch_tag_sub.add_parser("list", help="List all tags")
    ch_tag_add = ch_tag_sub.add_parser("add", help="Create a tag")
    ch_tag_add.add_argument("name", help="Tag name")
    ch_tag_del = ch_tag_sub.add_parser("delete", help="Delete a tag")
    ch_tag_del.add_argument("name", help="Tag name")
    ch_tag_set = ch_tag_sub.add_parser("set", help="Set tags for a channel")
    ch_tag_set.add_argument("pk", type=int, help="Channel primary key")
    ch_tag_set.add_argument("tags", help="Comma-separated tag names")
    ch_tag_get = ch_tag_sub.add_parser("get", help="Get tags for a channel")
    ch_tag_get.add_argument("pk", type=int, help="Channel primary key")
