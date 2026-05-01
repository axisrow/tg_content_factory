from __future__ import annotations

import argparse


def register(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser | None:
    sq_parser = subparsers.add_parser("search-query", help="Search query management")
    sq_sub = sq_parser.add_subparsers(dest="search_query_action")
    sq_sub.add_parser("list", help="List search queries")

    sq_get = sq_sub.add_parser("get", help="Show search query details")
    sq_get.add_argument("id", type=int, help="Search query id")

    sq_add = sq_sub.add_parser("add", help="Add search query")
    sq_add.add_argument("query", help="FTS5 search query text")
    sq_add.add_argument("--interval", type=int, default=60, help="Run interval in minutes")
    sq_add.add_argument("--regex", action="store_true", help="Use regex matching")
    sq_add.add_argument("--fts", action="store_true", help="Use FTS5 boolean syntax (no quoting)")
    sq_add.add_argument("--notify", action="store_true", help="Notify on collect")
    sq_add.add_argument("--no-track-stats", dest="track_stats", action="store_false", default=True)
    sq_add.add_argument(
        "--exclude-patterns", default="", help="Exclude patterns, one per line (use \\n)"
    )
    sq_add.add_argument("--max-length", type=int, default=None, help="Max message text length")

    sq_edit = sq_sub.add_parser("edit", help="Edit search query")
    sq_edit.add_argument("id", type=int, help="Search query id")
    sq_edit.add_argument("--query", help="New query text")
    sq_edit.add_argument("--interval", type=int, help="New interval in minutes")
    sq_edit.add_argument("--regex", action="store_true", default=None)
    sq_edit.add_argument("--no-regex", dest="regex", action="store_false")
    sq_edit.add_argument("--fts", action="store_true", default=None)
    sq_edit.add_argument("--no-fts", dest="fts", action="store_false")
    sq_edit.add_argument("--notify", action="store_true", default=None)
    sq_edit.add_argument("--no-notify", dest="notify", action="store_false")
    sq_edit.add_argument("--track-stats", action="store_true", default=None)
    sq_edit.add_argument("--no-track-stats", dest="track_stats", action="store_false")
    sq_edit.add_argument("--exclude-patterns", default=None, help="Exclude patterns (use \\n)")
    sq_edit.add_argument("--max-length", type=int, default=None, help="Max message text length")
    sq_edit.add_argument("--no-max-length", dest="max_length", action="store_const", const=-1)

    sq_del = sq_sub.add_parser("delete", help="Delete search query")
    sq_del.add_argument("id", type=int, help="Search query id")

    sq_toggle = sq_sub.add_parser("toggle", help="Toggle search query active state")
    sq_toggle.add_argument("id", type=int, help="Search query id")

    sq_run = sq_sub.add_parser("run", help="Run a search query once and show matches")
    sq_run.add_argument("id", type=int, help="Search query id")

    sq_stats = sq_sub.add_parser("stats", help="Show daily stats for a search query")
    sq_stats.add_argument("id", type=int, help="Search query id")
    sq_stats.add_argument("--days", type=int, default=30, help="Number of days")
