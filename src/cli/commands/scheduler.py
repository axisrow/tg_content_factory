from __future__ import annotations

import argparse
import asyncio
import logging

from src.cli import runtime
from src.database.bundles import ChannelBundle
from src.scheduler.manager import SchedulerManager
from src.services.collection_service import CollectionService
from src.services.task_enqueuer import TaskEnqueuer
from src.telegram.collector import Collector


def run(args: argparse.Namespace) -> None:
    async def _run() -> None:
        config, db = await runtime.init_db(args.config)
        _, pool = await runtime.init_pool(config, db)

        try:
            if not pool.clients:
                logging.error("No connected accounts.")
                return

            collector = Collector(pool, db, config.scheduler)
            channel_bundle = ChannelBundle.from_database(db)
            collection_service = CollectionService(
                channel_bundle, collector, collection_queue=None
            )
            task_enqueuer = TaskEnqueuer(db, collection_service)

            if args.scheduler_action == "start":
                manager = SchedulerManager(
                    config.scheduler,
                    task_enqueuer=task_enqueuer,
                )
                await manager.start()
                print(
                    f"Scheduler started (every {config.scheduler.collect_interval_minutes} min). "
                    "Press Ctrl+C to stop."
                )
                try:
                    while True:
                        await asyncio.sleep(1)
                except (KeyboardInterrupt, asyncio.CancelledError):
                    await manager.stop()
                    print("\nScheduler stopped.")
            elif args.scheduler_action == "trigger":
                result = await collection_service.enqueue_all_channels()
                print(
                    f"Enqueued {result.queued_count} channels "
                    f"(skipped {result.skipped_existing_count}, "
                    f"total {result.total_candidates}). "
                    f"Run 'serve' to execute tasks."
                )
        finally:
            await pool.disconnect_all()
            await db.close()

    asyncio.run(_run())
