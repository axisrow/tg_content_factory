from __future__ import annotations

import argparse
import asyncio
import logging

from src.cli import runtime
from src.database.bundles import ChannelBundle
from src.scheduler.service import SchedulerManager
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
            collection_service = CollectionService(channel_bundle, collector, collection_queue=None)
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

            elif args.scheduler_action == "status":
                interval = config.scheduler.collect_interval_minutes
                autostart = await db.get_setting("scheduler_autostart") or "0"
                print("Scheduler config:")
                print(f"  Interval: {interval} min")
                print(f"  Autostart: {'yes' if autostart == '1' else 'no'}")
                settings = await db.repos.settings.list_all()
                disabled_jobs = [
                    (k.removeprefix("scheduler_job_disabled:"), v)
                    for k, v in settings if k.startswith("scheduler_job_disabled:")
                ]
                if disabled_jobs:
                    print("  Disabled jobs:")
                    for job_id, val in disabled_jobs:
                        if val == "1":
                            print(f"    - {job_id}")

            elif args.scheduler_action == "stop":
                await db.set_setting("scheduler_autostart", "0")
                print("Scheduler autostart disabled. Running scheduler will stop on next restart.")

            elif args.scheduler_action == "job-toggle":
                job_id = args.job_id
                key = f"scheduler_job_disabled:{job_id}"
                current = await db.repos.settings.get_setting(key)
                new_disabled = current != "1"
                await db.repos.settings.set_setting(key, "1" if new_disabled else "0")
                status = "disabled" if new_disabled else "enabled"
                print(f"Job '{job_id}' {status}.")

            elif args.scheduler_action == "set-interval":
                job_id = args.job_id
                minutes = max(1, min(args.minutes, 1440))
                if job_id == "collect_all":
                    await db.repos.settings.set_setting("collect_interval_minutes", str(minutes))
                else:
                    await db.repos.settings.set_setting(
                        f"scheduler_job_{job_id}_interval", str(minutes)
                    )
                print(f"Interval for '{job_id}' set to {minutes} min.")

            elif args.scheduler_action == "task-cancel":
                ok = await db.repos.tasks.cancel_collection_task(args.task_id)
                if ok:
                    print(f"Task {args.task_id} cancelled.")
                else:
                    print(f"Task {args.task_id} not found or already completed.")

            elif args.scheduler_action == "clear-pending":
                deleted = await db.repos.tasks.delete_pending_channel_tasks()
                print(f"Cleared {deleted} pending collection tasks.")
        finally:
            await pool.disconnect_all()
            await db.close()

    asyncio.run(_run())
