"""Application orchestration for the scheduler web domain (#654).

Each handler returns a lightweight DTO from ``responses`` (never a FastAPI
response). Heavy context construction lives in ``context``; form/query parsing
lives in ``forms``. Route functions stay thin and just map DTO -> response.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import timezone

from fastapi import Request

from src.web import deps
from src.web.routes.channel_collection import bulk_enqueue_msg
from src.web.scheduler import forms
from src.web.scheduler.context import (
    _build_collector_health_context,
    _build_jobs_context,
    _load_pipeline_run_result_meta,
    _notification_snapshot_payload,
)
from src.web.scheduler.responses import (
    SchedulerPage,
    SchedulerRedirect,
    SchedulerTemplate,
)

logger = logging.getLogger(__name__)


def _shutting_down(request: Request) -> bool:
    return bool(getattr(request.app.state, "shutting_down", False))


async def enqueue_scheduler_command(
    request: Request,
    command_type: str,
    *,
    payload: dict[str, object] | None = None,
    redirect_code: str,
) -> SchedulerRedirect:
    command_id = await deps.telegram_command_service(request).enqueue(
        command_type,
        payload=payload or {},
        requested_by=f"web:scheduler.{command_type}",
    )
    return SchedulerRedirect(msg=redirect_code, extra={"command_id": command_id})


async def cancel_task(request: Request, task_id: int) -> SchedulerRedirect:
    if _shutting_down(request):
        return SchedulerRedirect(error="shutting_down")
    # Go through CollectionService so web-mode (where collection_queue is None)
    # falls back to a DB-only cancellation instead of raising AttributeError.
    service = deps.collection_service(request)
    await service.cancel_task(task_id)
    return SchedulerRedirect(msg="task_cancelled")


async def clear_pending_collect_tasks(request: Request) -> SchedulerRedirect:
    if _shutting_down(request):
        return SchedulerRedirect(error="shutting_down")
    service = deps.collection_service(request)
    deleted = await service.clear_pending_collect_tasks()
    msg = "pending_collect_tasks_deleted" if deleted > 0 else "pending_collect_tasks_empty"
    return SchedulerRedirect(msg=msg)


async def render_scheduler_page(
    request: Request, page: int, status: str, limit: int
) -> SchedulerPage:
    """Skeleton — only the lightweight controls. Health/jobs/tasks load lazily (#756)."""
    sched = deps.get_scheduler(request)
    db = deps.get_db(request)

    # Filter/page/limit are echoed into the controls' forms and the lazy fragment URLs.
    page = forms.normalize_page(page)
    limit = forms.normalize_limit(limit)
    status_filter = forms.normalize_status(status)

    notification_snapshot = await _notification_snapshot_payload(request)
    bot_payload = notification_snapshot.get("bot")
    bot_configured = bool(isinstance(bot_payload, dict) and bot_payload.get("configured"))
    queue_paused = (await db.repos.settings.get_setting("collection_queue_paused")) == "1"

    context = {
        "is_running": sched.is_running,
        "queue_paused": queue_paused,
        "interval_minutes": sched.interval_minutes,
        "msg": request.query_params.get("msg"),
        "bot_configured": bot_configured,
        "status_filter": status_filter,
        "page": page,
        "limit": limit,
    }
    return SchedulerPage(context)


async def render_scheduler_health_fragment(request: Request) -> SchedulerTemplate:
    collector_health = await _build_collector_health_context(request)
    return SchedulerTemplate("scheduler/_health.html", {"collector_health": collector_health})


async def render_scheduler_jobs_fragment(
    request: Request, page: int, status: str, limit: int
) -> SchedulerTemplate:
    sched = deps.get_scheduler(request)
    db = deps.get_db(request)
    scheduler_jobs, search_log = await asyncio.gather(
        _build_jobs_context(sched, db),
        db.get_recent_searches(),
    )
    return SchedulerTemplate(
        "scheduler/_jobs.html",
        {
            "scheduler_jobs": scheduler_jobs,
            "search_log": search_log,
            "is_running": sched.is_running,
            # Echoed into the job toggle/set-interval forms' filter_qs.
            "status_filter": forms.normalize_status(status),
            "page": forms.normalize_page(page),
            "limit": forms.normalize_limit(limit),
        },
    )


async def render_scheduler_tasks_fragment(
    request: Request, page: int, status: str, limit: int
) -> SchedulerTemplate:
    db = deps.get_db(request)
    page = forms.normalize_page(page)
    limit = forms.normalize_limit(limit)
    status_filter = forms.normalize_status(status)
    offset = (page - 1) * limit

    (tasks_page, all_count, active_count, pending_collect) = await asyncio.gather(
        db.get_collection_tasks_paginated(limit=limit, offset=offset, status_filter=status_filter),
        db.count_collection_tasks(),
        db.count_collection_tasks("active"),
        db.get_pending_channel_tasks(),
    )
    tasks, filtered_count = tasks_page
    pipeline_result_meta = await _load_pipeline_run_result_meta(db, tasks)
    result_column_title = "Результат"
    visible_pipeline_labels = {
        str(meta["label"])
        for meta in pipeline_result_meta.values()
        if isinstance(meta.get("label"), str)
    }
    if tasks and all(task.task_type.value == "pipeline_run" for task in tasks) and len(visible_pipeline_labels) == 1:
        result_column_title = next(iter(visible_pipeline_labels))

    total_pages = max(1, (filtered_count + limit - 1) // limit)
    if page > total_pages:
        page = total_pages
        offset = (page - 1) * limit
        tasks, filtered_count = await db.get_collection_tasks_paginated(
            limit=limit, offset=offset, status_filter=status_filter
        )

    return SchedulerTemplate(
        "scheduler/_tasks.html",
        {
            "tasks": tasks,
            "has_active_tasks": active_count > 0,
            "page": page,
            "total_pages": total_pages,
            "all_count": all_count,
            "active_count": active_count,
            "completed_count": all_count - active_count,
            "status_filter": status_filter,
            "limit": limit,
            "pending_collect_count": len(pending_collect),
            "pipeline_result_meta": pipeline_result_meta,
            "result_column_title": result_column_title,
        },
    )


async def toggle_scheduler_job(request: Request, job_id: str) -> SchedulerRedirect:
    if _shutting_down(request):
        return SchedulerRedirect(error="shutting_down")
    if not forms.is_valid_job_id(job_id):
        return SchedulerRedirect(error="invalid_job")
    db = deps.get_db(request)
    # pipeline_run_ is no longer a periodic job (#835/2) — content_generate_ is the live one.
    # Normalize so a stale UI row / external caller toggling pipeline_run_<id> disables the
    # real content_generate_<id> job, not a dead scheduler_job_disabled:pipeline_run_<id> key.
    job_id = forms.canonical_job_id(job_id)
    key = f"scheduler_job_disabled:{job_id}"
    current = await db.repos.settings.get_setting(key)
    new_disabled = current != "1"
    await db.repos.settings.set_setting(key, "1" if new_disabled else "0")
    return await enqueue_scheduler_command(
        request,
        "scheduler.reconcile",
        redirect_code="job_toggled",
    )


async def set_job_interval(request: Request, job_id: str) -> SchedulerRedirect:
    if _shutting_down(request):
        return SchedulerRedirect(error="shutting_down")
    if not forms.is_valid_job_id(job_id):
        return SchedulerRedirect(error="invalid_job")
    if job_id in ("photo_due", "photo_auto"):
        return SchedulerRedirect(error="invalid_job")
    form = await request.form()
    minutes = forms.parse_interval_minutes(form)
    if minutes is None:
        return SchedulerRedirect(error="invalid_interval")
    db = deps.get_db(request)
    if job_id == "collect_all":
        await db.repos.settings.set_setting("collect_interval_minutes", str(minutes))
    elif job_id.startswith("sq_"):
        sq_id = int(job_id.removeprefix("sq_"))
        sq = await db.repos.search_queries.get_by_id(sq_id)
        if sq:
            await db.repos.search_queries.update(sq_id, sq.model_copy(update={"interval_minutes": minutes}))
    elif job_id.startswith(("pipeline_run_", "content_generate_")):
        pid_str = job_id.removeprefix("pipeline_run_").removeprefix("content_generate_")
        pid = int(pid_str)
        pipeline = await db.repos.content_pipelines.get_by_id(pid)
        if pipeline:
            await db.repos.content_pipelines.update_generate_interval(pid, minutes)
    elif job_id == "warm_all_dialogs":
        await db.repos.settings.set_setting("warm_dialogs_interval_minutes", str(minutes))
    return await enqueue_scheduler_command(
        request,
        "scheduler.reconcile",
        redirect_code="interval_updated",
    )


async def start_scheduler(request: Request) -> SchedulerRedirect:
    if _shutting_down(request):
        return SchedulerRedirect(error="shutting_down")
    await deps.get_db(request).repos.settings.set_setting("scheduler_autostart", "1")
    return await enqueue_scheduler_command(
        request,
        "scheduler.reconcile",
        redirect_code="scheduler_started",
    )


async def stop_scheduler(request: Request) -> SchedulerRedirect:
    await deps.get_db(request).repos.settings.set_setting("scheduler_autostart", "0")
    return await enqueue_scheduler_command(
        request,
        "scheduler.reconcile",
        redirect_code="scheduler_stopped",
    )


async def pause_collection_queue(request: Request) -> SchedulerRedirect:
    if _shutting_down(request):
        return SchedulerRedirect(error="shutting_down")
    await deps.get_db(request).repos.settings.set_setting("collection_queue_paused", "1")
    return await enqueue_scheduler_command(
        request,
        "collection.pause",
        redirect_code="queue_paused",
    )


async def resume_collection_queue(request: Request) -> SchedulerRedirect:
    if _shutting_down(request):
        return SchedulerRedirect(error="shutting_down")
    await deps.get_db(request).repos.settings.set_setting("collection_queue_paused", "0")
    return await enqueue_scheduler_command(
        request,
        "collection.resume",
        redirect_code="queue_resumed",
    )


async def trigger_collection(request: Request) -> SchedulerRedirect:
    if _shutting_down(request):
        return SchedulerRedirect(error="shutting_down")
    service = deps.collection_service(request)
    result = await service.enqueue_all_channels()
    msg = bulk_enqueue_msg(result)
    return SchedulerRedirect(msg=msg)


async def trigger_warm_dialogs(request: Request) -> SchedulerRedirect:
    if _shutting_down(request):
        return SchedulerRedirect(error="shutting_down")
    return await enqueue_scheduler_command(
        request,
        "scheduler.trigger_warm",
        redirect_code="warm_dialogs_started",
    )


async def test_notification(request: Request) -> SchedulerRedirect:
    if _shutting_down(request):
        return SchedulerRedirect(error="shutting_down")
    return await enqueue_scheduler_command(
        request,
        "notifications.test",
        redirect_code="test_notification_queued",
    )


async def dry_run_notifications(request: Request) -> SchedulerTemplate:
    """Count notification matches from the last collection cycle without sending."""
    db = deps.get_db(request)

    # Find the last completed channel_collect task to determine the time window
    last_task = await db.repos.tasks.get_last_completed_collect_task()
    if last_task and last_task.completed_at:
        # SQLite datetime('now') stores as 'YYYY-MM-DD HH:MM:SS' (no T, no tz)
        since = last_task.completed_at.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    else:
        since = None

    queries = await db.get_notification_queries(active_only=True)
    # Exclude queries whose scheduler job is disabled
    filtered = []
    for sq in queries:
        val = await db.repos.settings.get_setting(f"scheduler_job_disabled:sq_{sq.id}")
        if val != "1":
            filtered.append(sq)
    queries = filtered
    if not queries:
        return SchedulerTemplate(
            "_dry_run_results.html",
            {"results": [], "since": since, "no_queries": True},
        )

    # Match with the SAME engine production uses (regex/substring), not FTS, so the preview
    # agrees with what would actually fire (#838/3). Counts are uncapped via dry_run_counts
    # (paged over the whole window); a capped fetch backs the 2 example previews only.
    from src.services.notification_matcher import dry_run_counts, dry_run_matches

    try:
        counts = await dry_run_counts(db, queries, since)
    except Exception:
        logger.exception("Dry-run failed to count matches")
        counts = {sq.id: 0 for sq in queries}
    try:
        preview_messages = await db.repos.messages.get_messages_collected_since(since) if since else []
    except Exception:
        logger.exception("Dry-run failed to load recent messages")
        preview_messages = []
    channels = await db.get_channels() if preview_messages else []

    results: list[dict[str, object]] = []
    total_matches = 0
    for sq in queries:
        matched, _ = dry_run_matches(preview_messages, sq, channels)
        count = counts.get(sq.id, 0)
        total_matches += count
        results.append({
            "query": sq.name or sq.query,
            "count": count,
            "previews": [(m.text or "")[:150] for m in matched[:2]],
        })

    logger.info("Dry-run notifications: %d queries, %d matches, since=%s", len(queries), total_matches, since)

    return SchedulerTemplate(
        "_dry_run_results.html",
        {"results": results, "since": since, "no_queries": False},
    )
