from __future__ import annotations

import argparse
import asyncio
import logging

from src.cli import runtime
from src.collection_queue import CollectionQueue
from src.database.bundles import ChannelBundle
from src.services.collection_service import CollectionService
from src.services.task_enqueuer import TaskEnqueuer
from src.telegram.collector import Collector


def run(args: argparse.Namespace) -> None:
    async def _run() -> None:
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
                collection_queue = CollectionQueue(collector, channel_bundle)
                collection_service = CollectionService(
                    channel_bundle, collector, collection_queue
                )
                task_enqueuer = TaskEnqueuer(db, collection_service)
                result = await task_enqueuer.enqueue_all_channels()
                print(
                    f"Enqueued {result.queued_count} channels "
                    f"(skipped {result.skipped_existing_count}, "
                    f"total {result.total_candidates})"
                )
        finally:
            await pool.disconnect_all()
            await db.close()

    asyncio.run(_run())
