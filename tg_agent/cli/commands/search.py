from __future__ import annotations

import argparse
import asyncio
import csv
import io
import logging

from tg_agent.cli import runtime
from tg_agent.search.engine import DB_SEARCH_MODES, TELEGRAM_SEARCH_MODES, SearchEngine
from tg_agent.services.embedding_service import EmbeddingService
from tg_agent.utils.json import safe_json_dumps
from tg_agent.utils.text_safety import csv_safe_cell


def _format_search_result_line(msg) -> str:
    """One rendered search hit; prefer @username/title over the bare numeric id.

    Premium global search returns messages whose channels are usually NOT in the
    accounts' dialogs — a bare numeric id cannot be re-resolved by `channel add`,
    while a username/title can.
    """
    if msg.channel_username:
        channel_label = f"@{msg.channel_username} ({msg.channel_id})"
    elif msg.channel_title:
        channel_label = f"{msg.channel_title} ({msg.channel_id})"
    else:
        channel_label = f"Channel {msg.channel_id}"
    return f"[{msg.date}] {channel_label}: {(msg.text or '')[:200]}"


def _print_search_result(result, fmt: str) -> None:
    """Render a SearchResult as text (default), json, or csv.

    Same field set as ``messages read`` plus the channel labels the search hit
    already carries (``@username``/title) — the digest workflow needs the source
    attribution without a second lookup.
    """
    if fmt == "json":
        items = [
            {
                "id": msg.id,
                "channel_id": msg.channel_id,
                "message_id": msg.message_id,
                "date": msg.date,
                "text": msg.text,
                "channel_username": msg.channel_username,
                "channel_title": msg.channel_title,
                "views": msg.views,
                "forwards": msg.forwards,
            }
            for msg in result.messages
        ]
        # Wrap in the full result object: a consumer with --limit N must see
        # has_more/total (silent truncation = #1350 class) and error, not a
        # bare [] that reads as "nothing found".
        print(
            safe_json_dumps(
                {
                    "query": result.query,
                    "total": result.total,
                    "has_more": result.has_more,
                    "error": result.error,
                    "messages": items,
                },
                indent=2,
            )
        )
        return
    if fmt == "csv":
        buf = io.StringIO()
        writer = csv.writer(buf)
        writer.writerow(
            ["id", "channel_id", "message_id", "date", "text",
             "channel_username", "channel_title", "views", "forwards"]
        )
        for msg in result.messages:
            writer.writerow([
                msg.id, msg.channel_id, msg.message_id,
                str(msg.date) if msg.date else "",
                csv_safe_cell((msg.text or "")[:500]),
                csv_safe_cell(msg.channel_username or ""),
                csv_safe_cell(msg.channel_title or ""),
                msg.views, msg.forwards,
            ])
        print(buf.getvalue(), end="")
        return
    total_display = f"{result.total}+" if result.has_more else str(result.total)
    print(f"Found {total_display} results for '{result.query}':\n")
    for msg in result.messages:
        print(_format_search_result_line(msg))
        print("---")


async def search_impl(
    config_path: str,
    *,
    query: str = "",
    limit: int = 20,
    mode: str = "local",
    channel_id: int | None = None,
    min_length: int | None = None,
    max_length: int | None = None,
    fts: bool = False,
    include_filtered: bool = False,
    date_from: str | None = None,
    date_to: str | None = None,
    output_format: str = "text",
    index_now: bool = False,
    reset_index: bool = False,
    purge_cache: bool = False,
) -> None:
    """Search messages across the configured *mode*, or run an index/purge side task.

    Shared async body for both CLI entry points — the argparse ``run`` wrapper
    below and the Typer ``search`` command (``tg_agent/cli/typer_commands.py``).
    Driven through the single async-bridge ``run_async`` by its callers, so
    there is no local ``asyncio.run`` in the migrated path.
    """
    config, db = await runtime.init_db(config_path)

    if purge_cache:
        try:
            if not query:
                logging.error("--purge-cache requires a query (the Premium search text to purge).")
                return
            deleted = await db.repos.messages.delete_premium_search_results(query)
            print(f"Purged {deleted} cached message(s) from Premium search for '{query}'.")
        finally:
            await db.close()
        return

    # Date windows are a DB-mode feature — SearchEngine's live methods take no
    # date bounds, so fail loudly instead of silently ignoring them (#1350 class).
    if (date_from or date_to) and mode not in DB_SEARCH_MODES:
        logging.error(
            "--date-from/--date-to are only supported with modes: %s (got '%s').",
            ", ".join(sorted(DB_SEARCH_MODES)),
            mode,
        )
        await db.close()
        return

    pool = None
    if mode in TELEGRAM_SEARCH_MODES:
        _, pool = await runtime.init_pool(config, db)
        if not pool.clients:
            logging.error("No connected accounts. Run 'serve' and add accounts via web UI.")
            await db.close()
            return

    try:
        if index_now:
            if reset_index:
                await db.repos.messages.reset_embeddings_index()
            indexed = await EmbeddingService(db, config).index_pending_messages()
            print(f"Indexed {indexed} messages for semantic search.")
            return

        if not query:
            logging.error("Search query is required unless --index-now is used.")
            return

        engine = SearchEngine(db, pool, config=config)

        if mode == "telegram":
            result = await engine.search_telegram(query, limit=limit)
        elif mode == "my_chats":
            result = await engine.search_my_chats(query, limit=limit)
        elif mode == "channel":
            result = await engine.search_in_channel(channel_id, query, limit=limit)
        elif mode == "semantic":
            result = await engine.search_semantic(
                query,
                limit=limit,
                date_from=date_from,
                date_to=date_to,
                min_length=min_length,
                max_length=max_length,
                include_filtered=include_filtered,
            )
        elif mode == "hybrid":
            result = await engine.search_hybrid(
                query,
                limit=limit,
                date_from=date_from,
                date_to=date_to,
                min_length=min_length,
                max_length=max_length,
                is_fts=fts,
                include_filtered=include_filtered,
            )
        else:
            result = await engine.search_local(
                query,
                limit=limit,
                date_from=date_from,
                date_to=date_to,
                min_length=min_length,
                max_length=max_length,
                is_fts=fts,
                include_filtered=include_filtered,
            )

        _print_search_result(result, output_format)
    finally:
        if pool:
            await pool.disconnect_all()
        await db.close()


def run(args: argparse.Namespace) -> None:
    asyncio.run(
        search_impl(
            args.config,
            query=getattr(args, "query", "") or "",
            limit=getattr(args, "limit", 20),
            mode=getattr(args, "mode", "local"),
            channel_id=getattr(args, "channel_id", None),
            min_length=getattr(args, "min_length", None),
            max_length=getattr(args, "max_length", None),
            fts=getattr(args, "fts", False),
            include_filtered=getattr(args, "all", False),
            date_from=getattr(args, "date_from", None),
            date_to=getattr(args, "date_to", None),
            output_format=getattr(args, "output_format", "text"),
            index_now=getattr(args, "index_now", False),
            reset_index=getattr(args, "reset_index", False),
            purge_cache=getattr(args, "purge_cache", False),
        )
    )
