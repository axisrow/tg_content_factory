from __future__ import annotations

import argparse
import asyncio
import logging

from src.cli import runtime
from src.search.engine import SearchEngine
from src.services.embedding_service import EmbeddingService


def run(args: argparse.Namespace) -> None:
    async def _run() -> None:
        config, db = await runtime.init_db(args.config)

        pool = None
        if args.mode in ("telegram", "my_chats", "channel"):
            _, pool = await runtime.init_pool(config, db)
            if not pool.clients:
                logging.error("No connected accounts. Run 'serve' and add accounts via web UI.")
                await db.close()
                return

        try:
            if getattr(args, "index_now", False):
                if getattr(args, "reset_index", False):
                    await db.repos.messages.reset_embeddings_index()
                indexed = await EmbeddingService(db, config).index_pending_messages()
                print(f"Indexed {indexed} messages for semantic search.")
                return

            if not args.query:
                logging.error("Search query is required unless --index-now is used.")
                return

            engine = SearchEngine(db, pool, config=config)

            if args.mode == "telegram":
                result = await engine.search_telegram(args.query, limit=args.limit)
            elif args.mode == "my_chats":
                result = await engine.search_my_chats(args.query, limit=args.limit)
            elif args.mode == "channel":
                result = await engine.search_in_channel(
                    args.channel_id, args.query, limit=args.limit
                )
            elif args.mode == "semantic":
                result = await engine.search_semantic(
                    args.query,
                    limit=args.limit,
                    min_length=args.min_length,
                    max_length=args.max_length,
                )
            elif args.mode == "hybrid":
                result = await engine.search_hybrid(
                    args.query,
                    limit=args.limit,
                    min_length=args.min_length,
                    max_length=args.max_length,
                    is_fts=args.fts,
                )
            else:
                result = await engine.search_local(
                    args.query,
                    limit=args.limit,
                    min_length=args.min_length,
                    max_length=args.max_length,
                    is_fts=args.fts,
                )

            print(f"Found {result.total} results for '{result.query}':\n")
            for msg in result.messages:
                text_preview = (msg.text or "")[:200]
                print(f"[{msg.date}] Channel {msg.channel_id}: {text_preview}")
                print("---")
        finally:
            if pool:
                await pool.disconnect_all()
            await db.close()

    asyncio.run(_run())
