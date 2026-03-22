import logging
import re
from datetime import timezone

from fastapi import APIRouter, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from src.telegram.notifier import Notifier
from src.web import deps
from src.web.routes.channel_collection import bulk_enqueue_msg

logger = logging.getLogger(__name__)

router = APIRouter()

JOB_LABELS = {
    "collect_all": "Сбор всех каналов",
    "photo_due": "Фото по расписанию",
    "photo_auto": "Автозагрузка фото",
}


def _job_label(job_id: str) -> str:
    if job_id in JOB_LABELS:
        return JOB_LABELS[job_id]
    if job_id.startswith("sq_"):
        return f"Стат. запроса #{job_id.removeprefix('sq_')}"
    if job_id.startswith("pipeline_run_"):
        return f"Пайплайн #{job_id.removeprefix('pipeline_run_')}"
    if job_id.startswith("content_generate_"):
        return f"Генерация #{job_id.removeprefix('content_generate_')}"
    return job_id


@router.post("/tasks/{task_id}/cancel")
async def cancel_task(request: Request, task_id: int):
    if getattr(request.app.state, "shutting_down", False):
        return RedirectResponse(url="/scheduler?error=shutting_down", status_code=303)
    queue = deps.get_queue(request)
    await queue.cancel_task(task_id)
    return RedirectResponse(url="/scheduler?msg=task_cancelled", status_code=303)


@router.post("/tasks/clear-pending-collect")
async def clear_pending_collect_tasks(request: Request):
    if getattr(request.app.state, "shutting_down", False):
        return RedirectResponse(url="/scheduler?error=shutting_down", status_code=303)
    queue = deps.get_queue(request)
    deleted = await queue.clear_pending_tasks()
    msg = "pending_collect_tasks_deleted" if deleted > 0 else "pending_collect_tasks_empty"
    return RedirectResponse(url=f"/scheduler?msg={msg}", status_code=303)


VALID_STATUS_FILTERS = {"all", "active", "completed"}

_VALID_JOB_ID_RE = re.compile(
    r"^(collect_all|photo_due|photo_auto|sq_\d+|pipeline_run_\d+|content_generate_\d+)$"
)


async def _build_jobs_context(sched, db) -> list[dict]:
    """Build a list of job dicts for the scheduler page template."""
    # Always fetch potential jobs to get DB-sourced interval_minutes
    potential = await sched.get_potential_jobs()
    potential_map = {j["job_id"]: j for j in potential}

    if sched.is_running:
        raw = sched.get_all_jobs_next_run()
        jobs = []
        for job_id, next_run in raw.items():
            db_interval = potential_map.get(job_id, {}).get("interval_minutes")
            jobs.append({
                "job_id": job_id,
                "label": _job_label(job_id),
                "next_run": next_run,
                "interval_minutes": db_interval,
            })
        # Also include disabled jobs (not in APScheduler but exist in potential_map)
        running_ids = set(raw.keys())
        for j in potential:
            if j["job_id"] not in running_ids:
                jobs.append({
                    "job_id": j["job_id"],
                    "label": _job_label(j["job_id"]),
                    "next_run": None,
                    "interval_minutes": j["interval_minutes"],
                })
    else:
        jobs = [
            {
                "job_id": j["job_id"],
                "label": _job_label(j["job_id"]),
                "next_run": None,
                "interval_minutes": j["interval_minutes"],
            }
            for j in potential
        ]

    for j in jobs:
        val = await db.repos.settings.get_setting(f"scheduler_job_disabled:{j['job_id']}")
        j["enabled"] = val != "1"
        j["interval_editable"] = j["job_id"] not in ("photo_due", "photo_auto")
    return jobs


@router.get("/", response_class=HTMLResponse)
async def scheduler_page(
    request: Request,
    page: int = Query(1),
    status: str = Query("all"),
    limit: int = Query(50),
):
    sched = deps.get_scheduler(request)
    db = deps.get_db(request)
    msg = request.query_params.get("msg")

    # Validation
    page = max(1, page)
    limit = max(10, min(limit, 100))
    status_filter = status if status in VALID_STATUS_FILTERS else "all"

    # Get tasks with filter and pagination
    offset = (page - 1) * limit
    tasks, filtered_count = await db.get_collection_tasks_paginated(
        limit=limit, offset=offset, status_filter=status_filter
    )

    # Calculate pagination; clamp page and re-fetch if needed
    total_pages = max(1, (filtered_count + limit - 1) // limit)
    if page > total_pages:
        page = total_pages
        offset = (page - 1) * limit
        tasks, filtered_count = await db.get_collection_tasks_paginated(
            limit=limit, offset=offset, status_filter=status_filter
        )

    # Get counts for all tabs
    all_count = await db.count_collection_tasks()
    active_count = await db.count_collection_tasks("active")
    completed_count = all_count - active_count

    has_active_tasks = active_count > 0
    pending_collect_count = len(await db.get_pending_channel_tasks())

    search_log = await db.get_recent_searches()
    notifier = deps.get_notifier(request)
    try:
        bot = await deps.notification_service(request).get_status()
    except Exception:
        bot = None
    bot_configured = (
        notifier is not None and notifier.admin_chat_id is not None
    ) or bot is not None

    scheduler_jobs = await _build_jobs_context(sched, db)

    return deps.get_templates(request).TemplateResponse(
        request,
        "scheduler.html",
        {
            "is_running": sched.is_running,
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
        },
    )


@router.post("/jobs/{job_id}/toggle")
async def toggle_scheduler_job(request: Request, job_id: str):
    if getattr(request.app.state, "shutting_down", False):
        return RedirectResponse(url="/scheduler?error=shutting_down", status_code=303)
    if not _VALID_JOB_ID_RE.match(job_id):
        return RedirectResponse(url="/scheduler?error=invalid_job", status_code=303)
    db = deps.get_db(request)
    key = f"scheduler_job_disabled:{job_id}"
    current = await db.repos.settings.get_setting(key)
    new_disabled = current != "1"
    await db.repos.settings.set_setting(key, "1" if new_disabled else "0")
    sched = deps.get_scheduler(request)
    if sched.is_running:
        await sched.sync_job_state(job_id, enabled=not new_disabled)
    return RedirectResponse(url="/scheduler?msg=job_toggled", status_code=303)


@router.post("/jobs/{job_id}/set-interval")
async def set_job_interval(request: Request, job_id: str):
    if getattr(request.app.state, "shutting_down", False):
        return RedirectResponse(url="/scheduler?error=shutting_down", status_code=303)
    if not _VALID_JOB_ID_RE.match(job_id):
        return RedirectResponse(url="/scheduler?error=invalid_job", status_code=303)
    if job_id in ("photo_due", "photo_auto"):
        return RedirectResponse(url="/scheduler?error=invalid_job", status_code=303)
    form = await request.form()
    try:
        minutes = int(form["interval_minutes"])
        minutes = max(1, min(minutes, 1440))
    except (KeyError, ValueError):
        return RedirectResponse(url="/scheduler?error=invalid_interval", status_code=303)
    db = deps.get_db(request)
    sched = deps.get_scheduler(request)
    if job_id == "collect_all":
        await db.repos.settings.set_setting("collect_interval_minutes", str(minutes))
        sched.update_interval(minutes)
    elif job_id.startswith("sq_"):
        sq_id = int(job_id.removeprefix("sq_"))
        sq = await db.repos.search_queries.get_by_id(sq_id)
        if sq:
            await db.repos.search_queries.update(sq_id, sq.model_copy(update={"interval_minutes": minutes}))
            if sched.is_running:
                await sched.sync_search_query_jobs()
    elif job_id.startswith(("pipeline_run_", "content_generate_")):
        pid_str = job_id.removeprefix("pipeline_run_").removeprefix("content_generate_")
        pid = int(pid_str)
        pipeline = await db.repos.content_pipelines.get_by_id(pid)
        if pipeline:
            await db.repos.content_pipelines.update_generate_interval(pid, minutes)
            if sched.is_running:
                await sched.sync_pipeline_jobs()
    return RedirectResponse(url="/scheduler?msg=interval_updated", status_code=303)


@router.post("/start")
async def start_scheduler(request: Request):
    if getattr(request.app.state, "shutting_down", False):
        return RedirectResponse(url="/scheduler?error=shutting_down", status_code=303)
    await deps.scheduler_service(request).start()
    await deps.get_db(request).set_setting("scheduler_autostart", "1")
    return RedirectResponse(url="/scheduler?msg=scheduler_started", status_code=303)


@router.post("/stop")
async def stop_scheduler(request: Request):
    await deps.scheduler_service(request).stop()
    await deps.get_db(request).set_setting("scheduler_autostart", "0")
    return RedirectResponse(url="/scheduler?msg=scheduler_stopped", status_code=303)


@router.post("/trigger")
async def trigger_collection(request: Request):
    if getattr(request.app.state, "shutting_down", False):
        return RedirectResponse(url="/scheduler?error=shutting_down", status_code=303)
    service = deps.collection_service(request)
    result = await service.enqueue_all_channels()
    msg = bulk_enqueue_msg(result)
    return RedirectResponse(url=f"/scheduler?msg={msg}", status_code=303)


@router.post("/test-notification")
async def test_notification(request: Request):
    if getattr(request.app.state, "shutting_down", False):
        return RedirectResponse(url="/scheduler?error=shutting_down", status_code=303)

    notifier = deps.get_notifier(request)
    admin_chat_id = notifier.admin_chat_id if notifier else None
    try:
        bot = await deps.notification_service(request).get_status()
    except Exception:
        bot = None
    if not admin_chat_id and bot:
        admin_chat_id = bot.tg_user_id
    if not admin_chat_id and not bot:
        return RedirectResponse(url="/scheduler?error=bot_not_configured", status_code=303)

    db = deps.get_db(request)
    queries = await db.repos.search_queries.get_all(active_only=True)
    sample_query = next((sq for sq in queries if not sq.is_regex), None)
    if not sample_query:
        text = "🔔 Тест уведомлений: нет поисковых запросов"
    else:
        messages, _ = await db.search_messages_for_query(sample_query, limit=1)
        if messages:
            msg = messages[0]
            preview = (msg.text or "")[:200]
            if msg.channel_username:
                link = f"https://t.me/{msg.channel_username}/{msg.message_id}"
            else:
                bare_id = str(msg.channel_id).lstrip("-").removeprefix("100")
                link = f"https://t.me/c/{bare_id}/{msg.message_id}"
            text = f"🔔 Тест уведомлений:\n{preview}\n{link}"
        else:
            text = "🔔 Тест уведомлений: нет сообщений для отправки"

    target_svc = deps.get_notification_target_service(request)
    test_notifier = Notifier(target_svc, admin_chat_id, deps.get_notification_bundle(request))
    ok = await test_notifier.notify(text)
    msg = "test_notification_sent" if ok else "test_notification_failed"
    return RedirectResponse(url=f"/scheduler?msg={msg}", status_code=303)


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
