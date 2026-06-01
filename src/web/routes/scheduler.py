import asyncio
import logging
import re
from datetime import timezone
from urllib.parse import urlencode

from fastapi import APIRouter, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from src.web import deps
from src.web.routes.channel_collection import bulk_enqueue_msg

# Heavy context construction lives in src/web/scheduler/context.py (#654). The
# helpers below are re-exported here for backward compatibility: src/web/search
# imports `_is_worker_alive` from this module, and several test modules import
# the pure presentation helpers + monkeypatch `_is_worker_alive` via this path.
from src.web.scheduler.context import (
    JOB_LABELS,
    WORKER_HEARTBEAT_STALE_AFTER_SEC,
    _build_collector_health_context,
    _build_jobs_context,
    _collector_health_border_severity,
    _collector_health_recommendations,
    _compute_load_level,
    _dedupe_recent_unavailability_events,
    _format_retry_hint,
    _is_worker_alive,
    _job_label,
    _load_pipeline_run_result_meta,
    _notification_snapshot_payload,
    _worker_status,
)

logger = logging.getLogger(__name__)

router = APIRouter()

# Re-exported for backward compatibility (importers/tests reach these via
# src.web.routes.scheduler). Listed here so the imports above are not flagged
# as unused.
__all__ = [
    "router",
    "WORKER_HEARTBEAT_STALE_AFTER_SEC",
    "JOB_LABELS",
    "_build_collector_health_context",
    "_build_jobs_context",
    "_collector_health_border_severity",
    "_collector_health_recommendations",
    "_compute_load_level",
    "_dedupe_recent_unavailability_events",
    "_format_retry_hint",
    "_is_worker_alive",
    "_job_label",
    "_load_pipeline_run_result_meta",
    "_notification_snapshot_payload",
    "_worker_status",
]


@router.post("/tasks/{task_id}/cancel")
async def cancel_task(request: Request, task_id: int):
    if getattr(request.app.state, "shutting_down", False):
        return _scheduler_redirect(request, error="shutting_down")
    # Go through CollectionService so web-mode (where collection_queue is None)
    # falls back to a DB-only cancellation instead of raising AttributeError.
    service = deps.collection_service(request)
    await service.cancel_task(task_id)
    return _scheduler_redirect(request, msg="task_cancelled")


@router.post("/tasks/clear-pending-collect")
async def clear_pending_collect_tasks(request: Request):
    if getattr(request.app.state, "shutting_down", False):
        return _scheduler_redirect(request, error="shutting_down")
    service = deps.collection_service(request)
    deleted = await service.clear_pending_collect_tasks()
    msg = "pending_collect_tasks_deleted" if deleted > 0 else "pending_collect_tasks_empty"
    return _scheduler_redirect(request, msg=msg)


VALID_STATUS_FILTERS = {"all", "active", "completed"}

_VALID_JOB_ID_RE = re.compile(
    r"^(collect_all|photo_due|photo_auto|sq_\d+|pipeline_run_\d+|content_generate_\d+)$"
)


_PRESERVED_SCHEDULER_QUERY_KEYS = ("status", "page", "limit")


def _scheduler_redirect(
    request: Request,
    *,
    msg: str | None = None,
    error: str | None = None,
    extra: dict[str, object] | None = None,
) -> RedirectResponse:
    """Redirect to /scheduler preserving the user's current filter/page.

    Fix for #457 round 3: POST routes used to redirect to `/scheduler?msg=...`
    and drop `?status=active&page=N&limit=M` — the user ended up back on the
    default `status=all` view, which on a large DB could look like "the button
    replaced the page with 143 pages of noise". Now every POST keeps the tab
    the user was looking at when they clicked.

    Only the whitelisted `status/page/limit` params travel — we deliberately
    drop any inbound `msg=`, `error=`, `command_id=` so they don't stack.
    """
    qp: dict[str, str] = {}
    for key in _PRESERVED_SCHEDULER_QUERY_KEYS:
        value = request.query_params.get(key)
        if value is not None and value != "":
            qp[key] = value
    if msg is not None:
        qp["msg"] = msg
    if error is not None:
        qp["error"] = error
    if extra:
        for k, v in extra.items():
            if v is not None:
                qp[k] = str(v)
    suffix = f"?{urlencode(qp)}" if qp else ""
    return RedirectResponse(url=f"/scheduler/{suffix}", status_code=303)


async def _enqueue_scheduler_command(
    request: Request,
    command_type: str,
    *,
    payload: dict[str, object] | None = None,
    redirect_code: str,
):
    command_id = await deps.telegram_command_service(request).enqueue(
        command_type,
        payload=payload or {},
        requested_by=f"web:scheduler.{command_type}",
    )
    return _scheduler_redirect(
        request, msg=redirect_code, extra={"command_id": command_id}
    )


@router.get("/", response_class=HTMLResponse)
async def scheduler_page(
    request: Request,
    page: int = Query(1),
    status: str = Query("all"),
    limit: int = Query(50),
):
    try:
        return await _scheduler_page_inner(request, page, status, limit)
    except Exception:
        logger.exception("scheduler_page failed")
        raise


async def _scheduler_page_inner(
    request: Request, page: int, status: str, limit: int
) -> HTMLResponse:
    sched = deps.get_scheduler(request)
    db = deps.get_db(request)
    msg = request.query_params.get("msg")

    # Validation
    page = max(1, page)
    limit = max(10, min(limit, 100))
    status_filter = status if status in VALID_STATUS_FILTERS else "all"

    offset = (page - 1) * limit

    notification_snapshot = await _notification_snapshot_payload(request)
    bot_payload = notification_snapshot.get("bot")
    bot_configured = bool(isinstance(bot_payload, dict) and bot_payload.get("configured"))

    (
        tasks_page,
        all_count,
        active_count,
        pending_collect,
        search_log,
        scheduler_jobs,
        collector_health,
    ) = await asyncio.gather(
        db.get_collection_tasks_paginated(limit=limit, offset=offset, status_filter=status_filter),
        db.count_collection_tasks(),
        db.count_collection_tasks("active"),
        db.get_pending_channel_tasks(),
        db.get_recent_searches(),
        _build_jobs_context(sched, db),
        _build_collector_health_context(request),
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

    completed_count = all_count - active_count
    has_active_tasks = active_count > 0
    pending_collect_count = len(pending_collect)
    queue_paused = (await db.repos.settings.get_setting("collection_queue_paused")) == "1"

    context = {
        "is_running": sched.is_running,
        "queue_paused": queue_paused,
        "interval_minutes": sched.interval_minutes,
        "msg": msg,
        "tasks": tasks,
        "has_active_tasks": has_active_tasks,
        "page": page,
        "total_pages": total_pages,
        "all_count": all_count,
        "active_count": active_count,
        "completed_count": completed_count,
        "status_filter": status_filter,
        "limit": limit,
        "search_log": search_log,
        "bot_configured": bot_configured,
        "pending_collect_count": pending_collect_count,
        "scheduler_jobs": scheduler_jobs,
        "collector_health": collector_health,
        "pipeline_result_meta": pipeline_result_meta,
        "result_column_title": result_column_title,
    }

    templates = deps.get_templates(request)
    tpl = templates.env.get_template("scheduler.html")
    body = tpl.render({**context, "request": request})
    return HTMLResponse(body)


@router.post("/jobs/{job_id}/toggle")
async def toggle_scheduler_job(request: Request, job_id: str):
    if getattr(request.app.state, "shutting_down", False):
        return _scheduler_redirect(request, error="shutting_down")
    if not _VALID_JOB_ID_RE.match(job_id):
        return _scheduler_redirect(request, error="invalid_job")
    db = deps.get_db(request)
    key = f"scheduler_job_disabled:{job_id}"
    current = await db.repos.settings.get_setting(key)
    new_disabled = current != "1"
    await db.repos.settings.set_setting(key, "1" if new_disabled else "0")
    return await _enqueue_scheduler_command(
        request,
        "scheduler.reconcile",
        redirect_code="job_toggled",
    )


@router.post("/jobs/{job_id}/set-interval")
async def set_job_interval(request: Request, job_id: str):
    if getattr(request.app.state, "shutting_down", False):
        return _scheduler_redirect(request, error="shutting_down")
    if not _VALID_JOB_ID_RE.match(job_id):
        return _scheduler_redirect(request, error="invalid_job")
    if job_id in ("photo_due", "photo_auto"):
        return _scheduler_redirect(request, error="invalid_job")
    form = await request.form()
    try:
        minutes = int(form["interval_minutes"])
        minutes = max(1, min(minutes, 1440))
    except (KeyError, ValueError):
        return _scheduler_redirect(request, error="invalid_interval")
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
    return await _enqueue_scheduler_command(
        request,
        "scheduler.reconcile",
        redirect_code="interval_updated",
    )


@router.post("/start")
async def start_scheduler(request: Request):
    if getattr(request.app.state, "shutting_down", False):
        return _scheduler_redirect(request, error="shutting_down")
    await deps.get_db(request).set_setting("scheduler_autostart", "1")
    return await _enqueue_scheduler_command(
        request,
        "scheduler.reconcile",
        redirect_code="scheduler_started",
    )


@router.post("/stop")
async def stop_scheduler(request: Request):
    await deps.get_db(request).set_setting("scheduler_autostart", "0")
    return await _enqueue_scheduler_command(
        request,
        "scheduler.reconcile",
        redirect_code="scheduler_stopped",
    )


@router.post("/pause")
async def pause_collection_queue(request: Request):
    if getattr(request.app.state, "shutting_down", False):
        return _scheduler_redirect(request, error="shutting_down")
    await deps.get_db(request).set_setting("collection_queue_paused", "1")
    return await _enqueue_scheduler_command(
        request,
        "collection.pause",
        redirect_code="queue_paused",
    )


@router.post("/resume")
async def resume_collection_queue(request: Request):
    if getattr(request.app.state, "shutting_down", False):
        return _scheduler_redirect(request, error="shutting_down")
    await deps.get_db(request).set_setting("collection_queue_paused", "0")
    return await _enqueue_scheduler_command(
        request,
        "collection.resume",
        redirect_code="queue_resumed",
    )


@router.post("/trigger")
async def trigger_collection(request: Request):
    if getattr(request.app.state, "shutting_down", False):
        return _scheduler_redirect(request, error="shutting_down")
    service = deps.collection_service(request)
    result = await service.enqueue_all_channels()
    msg = bulk_enqueue_msg(result)
    return _scheduler_redirect(request, msg=msg)


@router.post("/trigger-warm")
async def trigger_warm_dialogs(request: Request):
    if getattr(request.app.state, "shutting_down", False):
        return _scheduler_redirect(request, error="shutting_down")
    return await _enqueue_scheduler_command(
        request,
        "scheduler.trigger_warm",
        redirect_code="warm_dialogs_started",
    )


@router.post("/test-notification")
async def test_notification(request: Request):
    if getattr(request.app.state, "shutting_down", False):
        return _scheduler_redirect(request, error="shutting_down")
    return await _enqueue_scheduler_command(
        request,
        "notifications.test",
        redirect_code="test_notification_queued",
    )


@router.post("/dry-run-notifications", response_class=HTMLResponse)
async def dry_run_notifications(request: Request):
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
        return deps.get_templates(request).TemplateResponse(
            request,
            "_dry_run_results.html",
            {"results": [], "since": since, "no_queries": True},
        )

    results = []
    for sq in queries:
        if since:
            try:
                previews, total = await db.search_messages_for_query_since(sq, since, limit=2)
            except Exception:
                logger.exception("Dry-run match error for sq_id=%s", sq.id)
                previews, total = [], 0
        else:
            previews, total = [], 0
        results.append({
            "query": sq.name or sq.query,
            "count": total,
            "previews": [(m.text or "")[:150] for m in previews],
        })

    total_matches = sum(r["count"] for r in results)
    logger.info("Dry-run notifications: %d queries, %d matches, since=%s", len(queries), total_matches, since)

    return deps.get_templates(request).TemplateResponse(
        request,
        "_dry_run_results.html",
        {"results": results, "since": since, "no_queries": False},
    )
