from __future__ import annotations

import argparse
import asyncio
import logging

from src.cli import runtime
from src.search.engine import SearchEngine
from src.services.embedding_service import EmbeddingService


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
    index_now: bool = False,
    reset_index: bool = False,
    purge_cache: bool = False,
) -> None:
    """Search messages across the configured *mode*, or run an index/purge side task.

    Shared async body for both CLI entry points — the argparse ``run`` wrapper
    below and the Typer ``search`` command (``src/cli/typer_commands.py``).
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

    pool = None
    if mode in ("telegram", "my_chats", "channel"):
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
                min_length=min_length,
                max_length=max_length,
                include_filtered=include_filtered,
            )
        elif mode == "hybrid":
            result = await engine.search_hybrid(
                query,
                limit=limit,
                min_length=min_length,
                max_length=max_length,
                is_fts=fts,
                include_filtered=include_filtered,
            )
        else:
            result = await engine.search_local(
                query,
                limit=limit,
                min_length=min_length,
                max_length=max_length,
                is_fts=fts,
                include_filtered=include_filtered,
            )

        total_display = f"{result.total}+" if result.has_more else str(result.total)
        print(f"Found {total_display} results for '{result.query}':\n")
        for msg in result.messages:
            text_preview = (msg.text or "")[:200]
            print(f"[{msg.date}] Channel {msg.channel_id}: {text_preview}")
            print("---")
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
            index_now=getattr(args, "index_now", False),
            reset_index=getattr(args, "reset_index", False),
            purge_cache=getattr(args, "purge_cache", False),
        )
    )
