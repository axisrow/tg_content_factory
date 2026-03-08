from __future__ import annotations

import argparse
import asyncio
import logging

from src.cli import runtime
from src.scheduler.manager import SchedulerManager
from src.search.engine import SearchEngine
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
            search_engine = SearchEngine(db, pool)

            if args.scheduler_action == "start":
                manager = SchedulerManager(
                    collector, config.scheduler, search_engine=search_engine, db=db
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
                stats = await collector.collect_all_channels()
                print(f"Collection complete: {stats}")
            elif args.scheduler_action == "search":
                manager = SchedulerManager(
                    collector, config.scheduler, search_engine=search_engine, db=db
                )
                stats = await manager.trigger_search_now()
                print(f"Notification query search complete: {stats}")
        finally:
            await pool.disconnect_all()
            await db.close()

    asyncio.run(_run())
