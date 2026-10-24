"""Shared async bodies for the ``scheduler`` CLI group (epic #959, Wave 3 — #1123).

Migrated off the argparse dispatcher onto the Typer ``app`` (see
``tg_agent/cli/typer_commands.py``). Each leaf sub-command is a plain ``async def
*_impl`` here — no local ``asyncio.run`` and no ``argparse.Namespace``. A thin
``run(args)`` adapter is kept for the argparse leaf audit and existing tests.

No action opens a Telegram pool: each is a pure DB operation or a
telegram-command hand-off to the running worker (#1568, #1569).
"""

from __future__ import annotations

import argparse
import asyncio
import json
import math
from datetime import datetime, timezone

import typer

from tg_agent.cli import runtime
from tg_agent.cli.commands.common import (
    apply_startup,
    cli_error,
    run_async,
)
from tg_agent.database import Database
from tg_agent.models import CollectionTaskType
from tg_agent.services.runtime_diagnostics import evaluate_worker_heartbeat
from tg_agent.services.telegram_command_service import TelegramCommandService
from tg_agent.utils.datetime import parse_datetime


async def _enqueue_reconcile(db: Database, command: str) -> None:
    """Tell the live worker to re-read scheduler settings (worker has no re-sync).

    The CLI writes settings that the running APScheduler only reads at job
    registration — without this command the live scheduler keeps the old state
    until a worker restart. Mirrors the web routes, which enqueue
    ``scheduler.reconcile`` after every mutation (audit #835/5 added the same
    for queue-pause/resume).
    """
    await TelegramCommandService(db).enqueue(
        "scheduler.reconcile",
        payload={},
        requested_by=f"cli:scheduler.{command}",
    )


async def start_impl(config_path: str) -> None:
    """Enable scheduler autostart; the running worker applies it via reconcile.

    The old behaviour ran a foreground SchedulerManager on its own pool — a
    second collector racing the daemon for the same flood-wait budget (#1569).
    Writing the setting + enqueueing ``scheduler.reconcile`` is the same live
    path the web routes use.
    """
    _, db = await runtime.init_db(config_path)
    try:
        await db.set_setting("scheduler_autostart", "1")
        await _enqueue_reconcile(db, "start")
        print(
            "Scheduler autostart enabled. The running worker starts its scheduler "
            "on reconcile; the next daemon start picks it up via autostart."
        )
    finally:
        await db.close()


async def trigger_impl(config_path: str) -> None:
    """Hand a one-shot collection run to the live worker (no local pool, #1568).

    The worker's ``scheduler.trigger_collection`` handler runs
    ``SchedulerManager.trigger_background()`` — the same enqueue-all path as the
    periodic job, with its own throttle/backoff handling. The CLI previously
    wrote the PENDING rows itself and stayed silent when the live worker's
    ingest was throttled or the in-memory queue was full.
    """
    _, db = await runtime.init_db(config_path)
    try:
        command_id = await TelegramCommandService(db).enqueue(
            "scheduler.trigger_collection",
            payload={},
            requested_by="cli:scheduler.trigger",
        )
        print(
            f"Command #{command_id} enqueued: scheduler.trigger_collection. "
            "A running worker starts the collection; check 'scheduler queue-status'."
        )
    finally:
        await db.close()


async def _print_live_worker_status(db: Database, config_interval: int) -> None:
    """Append the live worker section read from ``runtime_snapshots`` (#1570).

    The config block describes what will happen on the next worker start; this
    is what the running worker actually does right now — the same snapshots the
    web side renders (worker_heartbeat / scheduler_status / scheduler_jobs /
    collection_queue_status). Heartbeat freshness is classified by the shared
    ``evaluate_worker_heartbeat`` so CLI cannot drift from the web banner.
    """
    heartbeat = await db.repos.runtime_snapshots.get_snapshot("worker_heartbeat")
    health = evaluate_worker_heartbeat(heartbeat)
    if not health.alive:
        detail = "stale heartbeat" if health.stale else (health.status or "no heartbeat")
        age = f", last {health.age_sec:.0f}s ago" if health.age_sec is not None else ""
        print(f"Worker: down ({detail}{age}) — live state unavailable, showing config only")
        return
    age = f"{health.age_sec:.0f}s ago" if health.age_sec is not None else "just now"
    print(f"Worker: alive (heartbeat {age})")

    sched = await db.repos.runtime_snapshots.get_snapshot("scheduler_status")
    payload = sched.payload if sched is not None else {}
    print("Live scheduler:")
    print(f"  Running: {'yes' if payload.get('is_running') else 'no'}")
    print(f"  Interval: {payload.get('interval_minutes', config_interval)} min")
    queue = await db.repos.runtime_snapshots.get_snapshot("collection_queue_status")
    qpayload = queue.payload if queue is not None else {}
    if qpayload:
        print(
            f"  Queue: paused={'yes' if qpayload.get('paused') else 'no'}"
            f", running={qpayload.get('running_count', 0)}"
            f"/{qpayload.get('target_worker_count', 0)}"
        )
    jobs_snap = await db.repos.runtime_snapshots.get_snapshot("scheduler_jobs")
    jobs = jobs_snap.payload.get("jobs", []) if jobs_snap is not None else []
    if isinstance(jobs, list) and jobs:
        print("  Jobs:")
        for job in jobs:
            if not isinstance(job, dict):
                continue
            print(f"    - {job.get('job_id')}: next run {job.get('next_run') or 'n/a'}")


async def status_impl(config_path: str) -> None:
    """Show scheduler configuration, disabled jobs, and live worker state."""
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
        await _print_live_worker_status(db, interval)
    finally:
        await db.close()


async def stop_impl(config_path: str) -> None:
    """Disable scheduler autostart."""
    _, db = await runtime.init_db(config_path)
    try:
        await db.set_setting("scheduler_autostart", "0")
        await _enqueue_reconcile(db, "stop")
        print("Scheduler autostart disabled. The running worker stops its scheduler on reconcile.")
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
        # Journaled as the operator's decision so startup canonicalization never
        # mistakes it for legacy state and re-enables the job (#1315).
        await db.repos.settings.record_setting_decision(
            key, "1" if new_disabled else "0", origin="human", actor="cli"
        )
        await _enqueue_reconcile(db, "job-toggle")
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
                cli_error(f"Search query {sq_id} not found.")
            await db.repos.search_queries.update(
                sq_id, sq.model_copy(update={"interval_minutes": minutes})
            )
        elif job_id.startswith(("pipeline_run_", "content_generate_")):
            pid = int(job_id.removeprefix("pipeline_run_").removeprefix("content_generate_"))
            pipeline = await db.repos.content_pipelines.get_by_id(pid)
            if not pipeline:
                cli_error(f"Pipeline {pid} not found.")
            await db.repos.content_pipelines.update_generate_interval(pid, minutes)
        else:
            cli_error(f"Unknown job_id '{job_id}'.")
        await _enqueue_reconcile(db, "set-interval")
        print(f"Interval for '{job_id}' set to {minutes} min.")
    finally:
        await db.close()


async def task_cancel_impl(config_path: str, *, task_id: int) -> None:
    """Cancel a collection task and interrupt a live run of it (#1568).

    The DB flip covers the PENDING case; the ``collection.cancel_task`` command
    makes the worker set the in-flight cancel event, so a RUNNING task stops at
    the collector's next check instead of finishing the whole channel.
    """
    _, db = await runtime.init_db(config_path)
    try:
        ok = await db.repos.tasks.cancel_collection_task(task_id)
        if not ok:
            print(f"Task {task_id} not found or already completed.")
            return
        command_id = await TelegramCommandService(db).enqueue(
            "collection.cancel_task",
            payload={"task_id": task_id},
            requested_by="cli:scheduler.task-cancel",
        )
        print(
            f"Task {task_id} cancelled (command #{command_id} interrupts a live run)."
        )
    finally:
        await db.close()


async def clear_pending_impl(config_path: str) -> None:
    """Clear pending collection tasks and drain the live worker queue (#1568).

    The DB delete covers PENDING rows immediately; the ``collection.clear_pending``
    command makes the worker drop its in-memory queue, flood-wait requeue timers
    and known-task ids — previously those survived until a worker restart.
    """
    _, db = await runtime.init_db(config_path)
    try:
        deleted = await db.repos.tasks.delete_pending_channel_tasks()
        command_id = await TelegramCommandService(db).enqueue(
            "collection.clear_pending",
            payload={},
            requested_by="cli:scheduler.clear-pending",
        )
        print(
            f"Cleared {deleted} pending collection tasks "
            f"(command #{command_id} drains the live worker queue)."
        )
    finally:
        await db.close()


async def queue_pause_impl(config_path: str) -> None:
    """Pause the collection queue (queued tasks stay pending)."""
    _, db = await runtime.init_db(config_path)
    try:
        await db.set_setting("collection_queue_paused", "1")
        # Setting alone is read only at worker startup; signal a running worker too
        # so the pause actually takes effect now (audit #835/5).
        from tg_agent.services.telegram_command_service import TelegramCommandService

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
        from tg_agent.services.telegram_command_service import TelegramCommandService

        await TelegramCommandService(db).enqueue(
            "collection.resume", payload={}, requested_by="cli:scheduler.queue-resume"
        )
        print("Collection queue resumed.")
    finally:
        await db.close()


async def queue_status_impl(config_path: str, *, output_format: str = "text") -> None:
    """Show live collection-queue progress with an ETA from measured throughput.

    Число — только из данных: rate = завершения за последнее окно (по умолчанию
    5 мин); без завершений ETA не выдумываем («warming up or stalled»).
    """
    _, db = await runtime.init_db(config_path)
    try:
        task_type = CollectionTaskType.CHANNEL_COLLECT.value
        window_min = 5
        snap = await db.repos.tasks.queue_snapshot(task_type, window_min=window_min)
        paused = (await db.get_setting("collection_queue_paused") or "0") == "1"
        finished_window = snap.completed_window + snap.failed_window
        # Rate по фактической длительности наблюдения: делить на всё окно,
        # когда сбор идёт первую минуту, занижает скорость в разы (ETA ~×5).
        observed_min = window_min
        first_dt = parse_datetime(snap.first_completion_at) if snap.first_completion_at else None
        if first_dt is not None:
            observed_min = min(
                window_min,
                max((datetime.now(timezone.utc) - first_dt).total_seconds() / 60, 0.5),
            )
        rate = finished_window / observed_min if finished_window else 0.0
        eta_min = math.ceil(snap.remaining / rate) if rate > 0 and snap.remaining else None

        if output_format == "json":
            print(
                json.dumps(
                    {
                        "task_type": task_type,
                        "window_min": window_min,
                        "observed_min": round(observed_min, 2),
                        "pending": snap.pending,
                        "running": snap.running,
                        "remaining": snap.remaining,
                        "completed_window": snap.completed_window,
                        "failed_window": snap.failed_window,
                        "rate_per_min": round(rate, 2),
                        "eta_min": eta_min,
                        "paused": paused,
                    },
                    ensure_ascii=False,
                )
            )
            return

        paused_mark = " [PAUSED]" if paused else ""
        if snap.remaining == 0:
            print(f"Collection queue ({task_type}): empty.{paused_mark}")
            return
        print(
            f"Collection queue ({task_type}): {snap.remaining} remaining "
            f"({snap.pending} pending, {snap.running} running){paused_mark}"
        )
        window_label = (
            f"{observed_min:.1f} min" if observed_min < window_min else f"{window_min} min"
        )
        print(
            f"Last {window_label}: {snap.completed_window} completed, "
            f"{snap.failed_window} failed — rate ~{rate:.1f}/min"
        )
        if eta_min is None:
            print(
                "ETA: unknown — no completions in the last "
                f"{window_min} min (warming up or stalled; re-check in 2-3 min)"
            )
        else:
            print(f"ETA: ~{eta_min} min")
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
    elif action == "queue-status":
        asyncio.run(
            queue_status_impl(args.config, output_format=getattr(args, "scheduler_format", "text"))
        )


# --------------------------------------------------------------------------- #
# scheduler → start / trigger / status / stop / job-toggle / set-interval
#             / task-cancel / clear-pending / queue-pause / queue-resume
#             / queue-status
# --------------------------------------------------------------------------- #

scheduler_app = typer.Typer(no_args_is_help=True, help="Scheduler control")


@scheduler_app.command("start")
def scheduler_start(ctx: typer.Context) -> None:
    """Enable scheduler autostart (the worker applies it via reconcile)."""
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


@scheduler_app.command("queue-status")
def scheduler_queue_status(
    ctx: typer.Context,
    format: str = typer.Option("text", "--format", help="Output format: text or json"),
) -> None:
    """Show collection queue progress and an ETA from measured throughput."""
    apply_startup(ctx)
    run_async(queue_status_impl(ctx.obj.config, output_format=format))
