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
from typing import Any, cast

import typer
from pydantic import ValidationError

from src.cli import runtime
from src.cli.commands.common import (
    _NEG_ID_POSITIONAL,
    apply_startup,
    run_async,
)
from src.database.bundles import SearchQueryBundle
from src.models import SearchQueryDailyStat
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
        items = cast(list[dict[str, Any]], await svc.get_with_stats())
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
        stats = cast(list[SearchQueryDailyStat], await svc.get_daily_stats(query_id, days))
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


# --------------------------------------------------------------------------- #
# search-query → list / get / add / edit / delete / toggle / run / stats
# --------------------------------------------------------------------------- #

search_query_app = typer.Typer(no_args_is_help=True, help="Search query management")


@search_query_app.command("list")
def search_query_list(ctx: typer.Context) -> None:
    """List search queries."""
    apply_startup(ctx)
    run_async(list_impl(ctx.obj.config))


@search_query_app.command("get")
def search_query_get(
    ctx: typer.Context,
    query_id: int = typer.Argument(..., metavar="id", help="Search query id"),
) -> None:
    """Show search query details."""
    apply_startup(ctx)
    run_async(get_impl(ctx.obj.config, query_id=query_id))


@search_query_app.command("add", context_settings=_NEG_ID_POSITIONAL)
def search_query_add(
    ctx: typer.Context,
    query: str = typer.Argument(..., help="FTS5 search query text"),
    interval: int = typer.Option(60, "--interval", help="Run interval in minutes"),
    regex: bool = typer.Option(False, "--regex", help="Use regex matching"),
    fts: bool = typer.Option(False, "--fts", help="Use FTS5 boolean syntax (no quoting)"),
    notify: bool = typer.Option(False, "--notify", help="Notify on collect"),
    no_track_stats: bool = typer.Option(
        False, "--no-track-stats", help="Disable stat tracking (default: tracking on)"
    ),
    exclude_patterns: str = typer.Option(
        "", "--exclude-patterns", help="Exclude patterns, one per line (use \\n)"
    ),
    max_length: int | None = typer.Option(None, "--max-length", help="Max message text length"),
    chats: str = typer.Option("", "--chats", help="Chat filter: IDs, usernames or t.me links"),
) -> None:
    """Add search query."""
    apply_startup(ctx)
    # argparse declares ONLY ``--no-track-stats`` (store_false, default True) on
    # ``add`` — no ``--track-stats`` flag. Mirror that exactly so the Typer surface
    # is not one flag wider than argparse (#1123 review).
    run_async(
        add_impl(
            ctx.obj.config,
            query=query,
            interval=interval,
            is_regex=regex,
            is_fts=fts,
            notify=notify,
            track_stats=not no_track_stats,
            exclude_patterns=exclude_patterns,
            max_length=max_length,
            chats=chats,
        )
    )


@search_query_app.command("edit")
def search_query_edit(
    ctx: typer.Context,
    query_id: int = typer.Argument(..., metavar="id", help="Search query id"),
    query: str | None = typer.Option(None, "--query", help="New query text"),
    interval: int | None = typer.Option(None, "--interval", help="New interval in minutes"),
    regex: bool | None = typer.Option(None, "--regex/--no-regex", help="Toggle regex matching"),
    fts: bool | None = typer.Option(None, "--fts/--no-fts", help="Toggle FTS5 syntax"),
    notify: bool | None = typer.Option(None, "--notify/--no-notify", help="Toggle notify on collect"),
    track_stats: bool | None = typer.Option(
        None, "--track-stats/--no-track-stats", help="Toggle stat tracking"
    ),
    exclude_patterns: str | None = typer.Option(
        None, "--exclude-patterns", help="Exclude patterns (use \\n)"
    ),
    max_length: int | None = typer.Option(None, "--max-length", help="Max message text length"),
    clear_max_length: bool = typer.Option(
        False, "--no-max-length", help="Clear the max-length limit"
    ),
    chats: str | None = typer.Option(
        None, "--chats", help="Chat filter: IDs, usernames or t.me links"
    ),
    clear_chats: bool = typer.Option(False, "--clear-chats", help="Clear the chat filter"),
) -> None:
    """Edit search query; unset flags keep their current value."""
    apply_startup(ctx)
    # ``--no-max-length`` maps to the sentinel -1 the impl treats as "clear";
    # ``--clear-chats`` maps to "" — mirrors the argparse store_const declarations.
    resolved_max_length = -1 if clear_max_length else max_length
    resolved_chats = "" if clear_chats else chats
    run_async(
        edit_impl(
            ctx.obj.config,
            query_id=query_id,
            query=query,
            interval=interval,
            is_regex=regex,
            is_fts=fts,
            notify=notify,
            track_stats=track_stats,
            exclude_patterns=exclude_patterns,
            max_length=resolved_max_length,
            chats=resolved_chats,
        )
    )


@search_query_app.command("delete")
def search_query_delete(
    ctx: typer.Context,
    query_id: int = typer.Argument(..., metavar="id", help="Search query id"),
) -> None:
    """Delete search query."""
    apply_startup(ctx)
    run_async(delete_impl(ctx.obj.config, query_id=query_id))


@search_query_app.command("toggle")
def search_query_toggle(
    ctx: typer.Context,
    query_id: int = typer.Argument(..., metavar="id", help="Search query id"),
) -> None:
    """Toggle search query active state."""
    apply_startup(ctx)
    run_async(toggle_impl(ctx.obj.config, query_id=query_id))


@search_query_app.command("run")
def search_query_run(
    ctx: typer.Context,
    query_id: int = typer.Argument(..., metavar="id", help="Search query id"),
) -> None:
    """Run a search query once and show matches."""
    apply_startup(ctx)
    run_async(run_impl(ctx.obj.config, query_id=query_id))


@search_query_app.command("stats")
def search_query_stats(
    ctx: typer.Context,
    query_id: int = typer.Argument(..., metavar="id", help="Search query id"),
    days: int = typer.Option(30, "--days", help="Number of days"),
) -> None:
    """Show daily stats for a search query."""
    apply_startup(ctx)
    run_async(stats_impl(ctx.obj.config, query_id=query_id, days=days))
