from __future__ import annotations

import argparse


def register(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser | None:
    search_parser = subparsers.add_parser("search", help="Search messages")
    search_parser.add_argument("query", nargs="?", default="", help="Search query")
    search_parser.add_argument("--limit", type=int, default=20, help="Max results")
    search_parser.add_argument(
        "--mode",
        choices=["local", "semantic", "hybrid", "telegram", "my_chats", "channel"],
        default="local",
        help="Search mode: local, semantic, hybrid, telegram, my_chats, channel",
    )
    search_parser.add_argument(
        "--channel-id",
        type=int,
        default=None,
        help="Channel ID for --mode=channel",
    )
    search_parser.add_argument("--min-length", type=int, default=None, help="Min message length")
    search_parser.add_argument("--max-length", type=int, default=None, help="Max message length")
    search_parser.add_argument(
        "--fts", action="store_true", default=False, help="Use FTS5 boolean syntax"
    )
    search_parser.add_argument(
        "--index-now",
        action="store_true",
        default=False,
        help="Run semantic embeddings indexing/backfill before exiting",
    )
    search_parser.add_argument(
        "--reset-index",
        action="store_true",
        default=False,
        help="Drop semantic vector index before --index-now",
    )
