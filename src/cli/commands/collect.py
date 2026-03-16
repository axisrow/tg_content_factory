from __future__ import annotations

import argparse
import asyncio
import logging

from src.cli import runtime
from src.database.bundles import ChannelBundle
from src.services.collection_service import CollectionService
from src.services.task_enqueuer import TaskEnqueuer
from src.telegram.collector import Collector


def run(args: argparse.Namespace) -> None:
    async def _run() -> None:
        collect_action = getattr(args, "collect_action", None)

        if collect_action == "sample":
            await _run_sample(args)
            return

        config, db = await runtime.init_db(args.config)
        _, pool = await runtime.init_pool(config, db)
        try:
            if not pool.clients:
                logging.error("No connected accounts. Run 'serve' and add accounts via web UI.")
                return

            collector = Collector(pool, db, config.scheduler)

            if args.channel_id:
                channels = await db.get_channels()
                channel = next((ch for ch in channels if ch.channel_id == args.channel_id), None)
                if not channel:
                    print(f"Channel {args.channel_id} not found in DB")
                    return
                if channel.is_filtered:
                    print(
                        f"Channel {args.channel_id} is filtered and excluded from collection"
                    )
                    return
                count = await collector.collect_single_channel(channel, full=True)
                print(f"Collected {count} messages from channel {args.channel_id}")
            else:
                channel_bundle = ChannelBundle.from_database(db)
                collection_service = CollectionService(
                    channel_bundle, collector, collection_queue=None
                )
                task_enqueuer = TaskEnqueuer(db, collection_service)
                result = await task_enqueuer.enqueue_all_channels()
                print(
                    f"Enqueued {result.queued_count} channels "
                    f"(skipped {result.skipped_existing_count}, "
                    f"total {result.total_candidates}). "
                    f"Run 'serve' to execute tasks."
                )
        finally:
            await pool.disconnect_all()
            await db.close()

    async def _run_sample(args: argparse.Namespace) -> None:
        config, db = await runtime.init_db(args.config)
        _, pool = await runtime.init_pool(config, db)
        try:
            if not pool.clients:
                logging.error("No connected accounts. Run 'serve' and add accounts via web UI.")
                return

            channel_id = args.channel_id
            limit = getattr(args, "limit", 10)
            collector = Collector(pool, db, config.scheduler)

            print(f"Sampling last {limit} messages from channel {channel_id}...\n")
            previews = await collector.sample_channel(channel_id, limit=limit)

            if not previews:
                print("No messages found.")
                return

            for msg in previews:
                date_str = msg["date"].strftime("%Y-%m-%d %H:%M") if msg["date"] else "no date"
                media = msg["media_type"] or "-"
                text = msg["text_preview"] or ""
                preview = repr(text) if text else "(no text)"
                print(f"#{msg['message_id']:<8} {date_str}  {media:<12} {preview}")
        finally:
            await pool.disconnect_all()
            await db.close()

    asyncio.run(_run())
