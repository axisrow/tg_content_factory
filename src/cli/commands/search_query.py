"""Shared async bodies for the ``search-query`` CLI group (epic #959, Wave 3 — #1123).

Migrated off the argparse dispatcher onto the Typer ``app`` (see
``src/cli/typer_commands.py``). Each leaf sub-command is a plain ``async def
*_impl`` here — no local ``asyncio.run`` and no ``argparse.Namespace``. A thin
``run(args)`` adapter is kept for the argparse leaf audit and the existing
command-level tests.
"""

from __future__ import annotations

import argparse
import asyncio
import inspect
import logging

from pydantic import ValidationError

from src.cli import runtime
from src.database.bundles import SearchQueryBundle
from src.services.search_query_service import SearchQueryService

logger = logging.getLogger(__name__)


async def _chat_filter_warning(svc: SearchQueryService, chat_filter: str) -> str:
    validator = getattr(svc, "validate_chat_filter", None)
    if validator is None:
        return ""
    try:
        result = validator(chat_filter)
        validation = await result if inspect.isawaitable(result) else result
        warning_text = getattr(validation, "warning_text", None)
        text = warning_text() if callable(warning_text) else ""
        return text if isinstance(text, str) else ""
    except Exception:
        # Returning "" silently hides a validation failure as "no warning". Log so the
        # operator can tell a clean filter from a broken validator (#676).
        logger.warning("Failed to validate chat filter %r", chat_filter, exc_info=True)
        return ""


async def list_impl(config_path: str) -> None:
    """List all search queries with 30-day match counts and last-run times."""
    _, db = await runtime.init_db(config_path)
    try:
        svc = SearchQueryService(SearchQueryBundle.from_database(db))
        items = await svc.get_with_stats()
        if not items:
            print("No search queries found.")
            return
        fmt = "{:<5} {:<40} {:<10} {:<10} {:<20}"
        print(fmt.format("ID", "Query", "Interval", "Total30d", "Last run"))
        print("-" * 90)
        for item in items:
            sq = item["query"]
            print(
                fmt.format(
                    sq.id or 0,
                    sq.query[:40],
                    f"{sq.interval_minutes}m",
                    item["total_30d"],
                    (item["last_run"] or "—")[:20],
                )
            )
            chat_filter = getattr(sq, "chat_filter", "")
            if chat_filter:
                print(f"      chats: {chat_filter}")
                if item.get("chat_filter_warnings"):
                    print(f"      warning: {item['chat_filter_warnings']}")
    finally:
        await db.close()


async def get_impl(config_path: str, *, query_id: int) -> None:
    """Show full details for one search query by id."""
    _, db = await runtime.init_db(config_path)
    try:
        svc = SearchQueryService(SearchQueryBundle.from_database(db))
        sq = await svc.get(query_id)
        if not sq:
            print(f"Search query id={query_id} not found")
            return
        print(f"ID: {sq.id}")
        print(f"Query: {sq.query}")
        print(f"Interval: {sq.interval_minutes}m")
        print(f"Active: {sq.is_active}")
        print(f"Regex: {sq.is_regex}")
        print(f"FTS: {sq.is_fts}")
        print(f"Notify: {sq.notify_on_collect}")
        print(f"Track stats: {sq.track_stats}")
        print(f"Max length: {sq.max_length if sq.max_length is not None else '—'}")
        print(f"Exclude patterns: {sq.exclude_patterns or '—'}")
        chat_filter = getattr(sq, "chat_filter", "")
        print(f"Chats: {chat_filter or 'all'}")
        warning = await _chat_filter_warning(svc, chat_filter)
        if warning:
            print(f"Warning: {warning}")
    finally:
        await db.close()


async def add_impl(
    config_path: str,
    *,
    query: str,
    interval: int = 60,
    is_regex: bool = False,
    is_fts: bool = False,
    notify: bool = False,
    track_stats: bool = True,
    exclude_patterns: str = "",
    max_length: int | None = None,
    chats: str = "",
) -> None:
    """Add a new search query."""
    _, db = await runtime.init_db(config_path)
    try:
        svc = SearchQueryService(SearchQueryBundle.from_database(db))
        exclude = exclude_patterns.replace("\\n", "\n") if exclude_patterns else ""
        try:
            sq_id = await svc.add(
                query,
                interval,
                is_regex=is_regex,
                is_fts=is_fts,
                notify_on_collect=notify,
                track_stats=track_stats,
                exclude_patterns=exclude,
                max_length=max_length,
                chat_filter=chats,
            )
        except ValidationError as e:
            print(f"Error: {e.errors()[0]['msg']}")
            return
        print(f"Added search query id={sq_id}: {query}")
        warning = await _chat_filter_warning(svc, chats)
        if warning:
            print(f"Warning: {warning}")
    finally:
        await db.close()


async def edit_impl(
    config_path: str,
    *,
    query_id: int,
    query: str | None = None,
    interval: int | None = None,
    is_regex: bool | None = None,
    is_fts: bool | None = None,
    notify: bool | None = None,
    track_stats: bool | None = None,
    exclude_patterns: str | None = None,
    max_length: int | None = None,
    chats: str | None = None,
) -> None:
    """Edit an existing search query; unset flags keep their current value."""
    _, db = await runtime.init_db(config_path)
    try:
        svc = SearchQueryService(SearchQueryBundle.from_database(db))
        sq = await svc.get(query_id)
        if not sq:
            print(f"Search query id={query_id} not found")
            return
        notify_val = notify if notify is not None else sq.notify_on_collect
        tstats = track_stats if track_stats is not None else sq.track_stats
        is_fts_val = is_fts if is_fts is not None else sq.is_fts
        exclude = (
            exclude_patterns.replace("\\n", "\n")
            if exclude_patterns is not None
            else sq.exclude_patterns
        )
        max_len = (
            None
            if max_length == -1
            else max_length if max_length is not None else sq.max_length
        )
        try:
            await svc.update(
                query_id,
                query if query else sq.query,
                interval if interval is not None else sq.interval_minutes,
                is_regex=is_regex if is_regex is not None else sq.is_regex,
                is_fts=is_fts_val,
                notify_on_collect=notify_val,
                track_stats=tstats,
                exclude_patterns=exclude,
                max_length=max_len,
                chat_filter=(
                    chats if chats is not None else getattr(sq, "chat_filter", "")
                ),
            )
        except ValidationError as e:
            print(f"Error: {e.errors()[0]['msg']}")
            return
        print(f"Updated search query id={query_id}")
        warning = await _chat_filter_warning(
            svc,
            chats if chats is not None else getattr(sq, "chat_filter", ""),
        )
        if warning:
            print(f"Warning: {warning}")
    finally:
        await db.close()


async def delete_impl(config_path: str, *, query_id: int) -> None:
    """Delete a search query by id."""
    _, db = await runtime.init_db(config_path)
    try:
        svc = SearchQueryService(SearchQueryBundle.from_database(db))
        await svc.delete(query_id)
        print(f"Deleted search query id={query_id}")
    finally:
        await db.close()


async def toggle_impl(config_path: str, *, query_id: int) -> None:
    """Toggle a search query's active state."""
    _, db = await runtime.init_db(config_path)
    try:
        svc = SearchQueryService(SearchQueryBundle.from_database(db))
        await svc.toggle(query_id)
        print(f"Toggled search query id={query_id}")
    finally:
        await db.close()


async def run_impl(config_path: str, *, query_id: int) -> None:
    """Run a search query once and report how many matches were found."""
    _, db = await runtime.init_db(config_path)
    try:
        svc = SearchQueryService(SearchQueryBundle.from_database(db))
        count = await svc.run_once(query_id)
        print(f"Search query id={query_id} executed: {count} matches found.")
    finally:
        await db.close()


async def stats_impl(config_path: str, *, query_id: int, days: int = 30) -> None:
    """Show a daily match-count histogram for a search query."""
    _, db = await runtime.init_db(config_path)
    try:
        svc = SearchQueryService(SearchQueryBundle.from_database(db))
        stats = await svc.get_daily_stats(query_id, days)
        if not stats:
            print("No stats found.")
            return
        max_count = max(s.count for s in stats)
        for s in stats:
            bar_len = int(s.count / max_count * 40) if max_count else 0
            bar = "#" * bar_len
            print(f"{s.day}  {bar:<40} {s.count}")
    finally:
        await db.close()


def run(args: argparse.Namespace) -> None:
    """Thin argparse adapter over the ``*_impl`` bodies (legacy dispatch path).

    The production CLI routes ``search-query`` through the Typer ``app`` (#1123);
    this wrapper keeps the argparse leaf audit and command-level tests working.
    All args are read via ``getattr`` defaults so partial test Namespaces stay
    usable (see #1117).
    """
    action = getattr(args, "search_query_action", None)
    if action == "list":
        asyncio.run(list_impl(args.config))
    elif action == "get":
        asyncio.run(get_impl(args.config, query_id=args.id))
    elif action == "add":
        asyncio.run(
            add_impl(
                args.config,
                query=args.query,
                interval=getattr(args, "interval", 60),
                is_regex=getattr(args, "regex", False),
                is_fts=getattr(args, "fts", False),
                notify=getattr(args, "notify", False),
                track_stats=getattr(args, "track_stats", True),
                exclude_patterns=getattr(args, "exclude_patterns", ""),
                max_length=getattr(args, "max_length", None),
                chats=getattr(args, "chats", ""),
            )
        )
    elif action == "edit":
        asyncio.run(
            edit_impl(
                args.config,
                query_id=args.id,
                query=getattr(args, "query", None),
                interval=getattr(args, "interval", None),
                is_regex=getattr(args, "regex", None),
                is_fts=getattr(args, "fts", None),
                notify=getattr(args, "notify", None),
                track_stats=getattr(args, "track_stats", None),
                exclude_patterns=getattr(args, "exclude_patterns", None),
                max_length=getattr(args, "max_length", None),
                chats=getattr(args, "chats", None),
            )
        )
    elif action == "delete":
        asyncio.run(delete_impl(args.config, query_id=args.id))
    elif action == "toggle":
        asyncio.run(toggle_impl(args.config, query_id=args.id))
    elif action == "run":
        asyncio.run(run_impl(args.config, query_id=args.id))
    elif action == "stats":
        asyncio.run(stats_impl(args.config, query_id=args.id, days=getattr(args, "days", 30)))
