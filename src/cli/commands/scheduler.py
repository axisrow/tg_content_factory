"""Shared async bodies for the ``scheduler`` CLI group (epic #959, Wave 3 — #1123).

Migrated off the argparse dispatcher onto the Typer ``app`` (see
``src/cli/typer_commands.py``). Each leaf sub-command is a plain ``async def
*_impl`` here — no local ``asyncio.run`` and no ``argparse.Namespace``. A thin
``run(args)`` adapter is kept for the argparse leaf audit and existing tests.

Only ``start`` / ``trigger`` talk to Telegram; every other action is a pure DB
operation and must not pay (or risk) a full multi-account pool connect.
"""

from __future__ import annotations

import argparse
import asyncio
import logging

import typer

from src.cli import runtime
from src.cli.commands.common import (
    apply_startup,
    run_async,
)
from src.database.bundles import ChannelBundle
from src.scheduler.service import SchedulerManager
from src.services.collection_service import CollectionService
from src.services.task_enqueuer import TaskEnqueuer
from src.telegram.collector import Collector


async def start_impl(config_path: str) -> None:
    """Start the scheduler in the foreground (needs a connected account)."""
    config, db = await runtime.init_db(config_path)
    pool = None
    try:
        _, pool = await runtime.init_pool(config, db)
        if not pool.clients:
            logging.error("No connected accounts.")
            return
        collector = Collector(pool, db, config.scheduler)
        channel_bundle = ChannelBundle.from_database(db)
        collection_service = CollectionService(channel_bundle, collector, collection_queue=None)
        task_enqueuer = TaskEnqueuer(db, collection_service)

        manager = SchedulerManager(config.scheduler, task_enqueuer=task_enqueuer)
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
    finally:
        if pool is not None:
            await pool.disconnect_all()
        await db.close()


async def trigger_impl(config_path: str) -> None:
    """Trigger a one-shot enqueue of all channels (needs a connected account)."""
    config, db = await runtime.init_db(config_path)
    pool = None
    try:
        _, pool = await runtime.init_pool(config, db)
        if not pool.clients:
            logging.error("No connected accounts.")
            return
        collector = Collector(pool, db, config.scheduler)
        channel_bundle = ChannelBundle.from_database(db)
        collection_service = CollectionService(channel_bundle, collector, collection_queue=None)
        result = await collection_service.enqueue_all_channels(
            resolve_backoff_remaining_sec=pool.get_resolve_username_backoff_remaining_sec(),
        )
        print(
            f"Enqueued {result.queued_count} channels "
            f"(skipped {result.skipped_existing_count}, "
            f"total {result.total_candidates}). "
            f"Run 'serve' to execute tasks."
        )
    finally:
        if pool is not None:
            await pool.disconnect_all()
        await db.close()


async def status_impl(config_path: str) -> None:
    """Show scheduler configuration and disabled-job status."""
    config, db = await runtime.init_db(config_path)
    try:
        interval = config.scheduler.collect_interval_minutes
        autostart = await db.get_setting("scheduler_autostart") or "0"
        queue_paused = await db.get_setting("collection_queue_paused") or "0"
        print("Scheduler config:")
        print(f"  Interval: {interval} min")
        print(f"  Autostart: {'yes' if autostart == '1' else 'no'}")
        print(f"  Queue paused: {'yes' if queue_paused == '1' else 'no'}")
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
    finally:
        await db.close()


async def stop_impl(config_path: str) -> None:
    """Disable scheduler autostart."""
    _, db = await runtime.init_db(config_path)
    try:
        await db.set_setting("scheduler_autostart", "0")
        print("Scheduler autostart disabled. Running scheduler will stop on next restart.")
    finally:
        await db.close()


async def job_toggle_impl(config_path: str, *, job_id: str) -> None:
    """Toggle a scheduler job enabled/disabled."""
    _, db = await runtime.init_db(config_path)
    try:
        # pipeline_run_ is no longer a periodic job (#835/2) — canonicalize to the live
        # content_generate_ id so toggling it disables the real job, not a dead key.
        if job_id.startswith("pipeline_run_"):
            job_id = "content_generate_" + job_id.removeprefix("pipeline_run_")
        key = f"scheduler_job_disabled:{job_id}"
        current = await db.repos.settings.get_setting(key)
        new_disabled = current != "1"
        await db.repos.settings.set_setting(key, "1" if new_disabled else "0")
        status = "disabled" if new_disabled else "enabled"
        print(f"Job '{job_id}' {status}.")
    finally:
        await db.close()


async def set_interval_impl(config_path: str, *, job_id: str, minutes: int) -> None:
    """Set a scheduler job interval (clamped to 1–1440 minutes)."""
    _, db = await runtime.init_db(config_path)
    try:
        minutes = max(1, min(minutes, 1440))
        # Mirror the web set_job_interval handler — sq_/pipeline intervals live on
        # their own records, not in a settings key nobody reads (the old
        # scheduler_job_{id}_interval write was a silent no-op, audit #835/9).
        if job_id == "collect_all":
            await db.repos.settings.set_setting("collect_interval_minutes", str(minutes))
        elif job_id == "warm_all_dialogs":
            await db.repos.settings.set_setting("warm_dialogs_interval_minutes", str(minutes))
        elif job_id.startswith("sq_"):
            sq_id = int(job_id.removeprefix("sq_"))
            sq = await db.repos.search_queries.get_by_id(sq_id)
            if not sq:
                print(f"Search query {sq_id} not found.")
                return
            await db.repos.search_queries.update(
                sq_id, sq.model_copy(update={"interval_minutes": minutes})
            )
        elif job_id.startswith(("pipeline_run_", "content_generate_")):
            pid = int(job_id.removeprefix("pipeline_run_").removeprefix("content_generate_"))
            pipeline = await db.repos.content_pipelines.get_by_id(pid)
            if not pipeline:
                print(f"Pipeline {pid} not found.")
                return
            await db.repos.content_pipelines.update_generate_interval(pid, minutes)
        else:
            print(f"Unknown job_id '{job_id}'.")
            return
        print(f"Interval for '{job_id}' set to {minutes} min.")
    finally:
        await db.close()


async def task_cancel_impl(config_path: str, *, task_id: int) -> None:
    """Cancel a pending collection task by id."""
    _, db = await runtime.init_db(config_path)
    try:
        ok = await db.repos.tasks.cancel_collection_task(task_id)
        if ok:
            print(f"Task {task_id} cancelled.")
        else:
            print(f"Task {task_id} not found or already completed.")
    finally:
        await db.close()


async def clear_pending_impl(config_path: str) -> None:
    """Clear all pending collection tasks."""
    _, db = await runtime.init_db(config_path)
    try:
        deleted = await db.repos.tasks.delete_pending_channel_tasks()
        print(f"Cleared {deleted} pending collection tasks.")
    finally:
        await db.close()


async def queue_pause_impl(config_path: str) -> None:
    """Pause the collection queue (queued tasks stay pending)."""
    _, db = await runtime.init_db(config_path)
    try:
        await db.set_setting("collection_queue_paused", "1")
        # Setting alone is read only at worker startup; signal a running worker too
        # so the pause actually takes effect now (audit #835/5).
        from src.services.telegram_command_service import TelegramCommandService

        await TelegramCommandService(db).enqueue(
            "collection.pause", payload={}, requested_by="cli:scheduler.queue-pause"
        )
        print(
            "Collection queue paused. The running worker stops pulling new tasks; "
            "queued tasks remain pending."
        )
    finally:
        await db.close()


async def queue_resume_impl(config_path: str) -> None:
    """Resume the collection queue."""
    _, db = await runtime.init_db(config_path)
    try:
        await db.set_setting("collection_queue_paused", "0")
        from src.services.telegram_command_service import TelegramCommandService

        await TelegramCommandService(db).enqueue(
            "collection.resume", payload={}, requested_by="cli:scheduler.queue-resume"
        )
        print("Collection queue resumed.")
    finally:
        await db.close()


def run(args: argparse.Namespace) -> None:
    """Thin argparse adapter over the ``*_impl`` bodies (legacy dispatch path).

    The production CLI routes ``scheduler`` through the Typer ``app`` (#1123); this
    wrapper keeps the argparse leaf audit and command-level tests working. Args are
    read via ``getattr`` defaults so partial test Namespaces stay usable (#1117).
    """
    action = getattr(args, "scheduler_action", None)
    if action == "start":
        asyncio.run(start_impl(args.config))
    elif action == "trigger":
        asyncio.run(trigger_impl(args.config))
    elif action == "status":
        asyncio.run(status_impl(args.config))
    elif action == "stop":
        asyncio.run(stop_impl(args.config))
    elif action == "job-toggle":
        asyncio.run(job_toggle_impl(args.config, job_id=args.job_id))
    elif action == "set-interval":
        asyncio.run(set_interval_impl(args.config, job_id=args.job_id, minutes=args.minutes))
    elif action == "task-cancel":
        asyncio.run(task_cancel_impl(args.config, task_id=args.task_id))
    elif action == "clear-pending":
        asyncio.run(clear_pending_impl(args.config))
    elif action == "queue-pause":
        asyncio.run(queue_pause_impl(args.config))
    elif action == "queue-resume":
        asyncio.run(queue_resume_impl(args.config))


# --------------------------------------------------------------------------- #
# scheduler → start / trigger / status / stop / job-toggle / set-interval
#             / task-cancel / clear-pending / queue-pause / queue-resume
# --------------------------------------------------------------------------- #

scheduler_app = typer.Typer(no_args_is_help=True, help="Scheduler control")


@scheduler_app.command("start")
def scheduler_start(ctx: typer.Context) -> None:
    """Start scheduler (foreground)."""
    apply_startup(ctx)
    run_async(start_impl(ctx.obj.config))


@scheduler_app.command("trigger")
def scheduler_trigger(ctx: typer.Context) -> None:
    """Trigger one-shot collection."""
    apply_startup(ctx)
    run_async(trigger_impl(ctx.obj.config))


@scheduler_app.command("status")
def scheduler_status(ctx: typer.Context) -> None:
    """Show scheduler configuration and status."""
    apply_startup(ctx)
    run_async(status_impl(ctx.obj.config))


@scheduler_app.command("stop")
def scheduler_stop(ctx: typer.Context) -> None:
    """Disable scheduler autostart."""
    apply_startup(ctx)
    run_async(stop_impl(ctx.obj.config))


@scheduler_app.command("job-toggle")
def scheduler_job_toggle(
    ctx: typer.Context,
    job_id: str = typer.Argument(..., help="Job identifier (e.g. collect_all, sq_1)"),
) -> None:
    """Toggle scheduler job enabled/disabled."""
    apply_startup(ctx)
    run_async(job_toggle_impl(ctx.obj.config, job_id=job_id))


@scheduler_app.command("set-interval")
def scheduler_set_interval(
    ctx: typer.Context,
    job_id: str = typer.Argument(..., help="Job identifier"),
    minutes: int = typer.Argument(..., help="Interval in minutes (1-1440)"),
) -> None:
    """Set scheduler job interval."""
    apply_startup(ctx)
    run_async(set_interval_impl(ctx.obj.config, job_id=job_id, minutes=minutes))


@scheduler_app.command("task-cancel")
def scheduler_task_cancel(
    ctx: typer.Context,
    task_id: int = typer.Argument(..., help="Task ID to cancel"),
) -> None:
    """Cancel a collection task."""
    apply_startup(ctx)
    run_async(task_cancel_impl(ctx.obj.config, task_id=task_id))


@scheduler_app.command("clear-pending")
def scheduler_clear_pending(ctx: typer.Context) -> None:
    """Clear all pending collection tasks."""
    apply_startup(ctx)
    run_async(clear_pending_impl(ctx.obj.config))


@scheduler_app.command("queue-pause")
def scheduler_queue_pause(ctx: typer.Context) -> None:
    """Pause the collection queue (queued tasks stay pending)."""
    apply_startup(ctx)
    run_async(queue_pause_impl(ctx.obj.config))


@scheduler_app.command("queue-resume")
def scheduler_queue_resume(ctx: typer.Context) -> None:
    """Resume the collection queue."""
    apply_startup(ctx)
    run_async(queue_resume_impl(ctx.obj.config))
