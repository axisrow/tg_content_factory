import asyncio
import logging
import re
from datetime import datetime, timezone
from urllib.parse import urlencode

from fastapi import APIRouter, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from src.models import AccountSessionStatus
from src.services.pipeline_result import result_kind_label
from src.web import deps
from src.web.routes.channel_collection import bulk_enqueue_msg

logger = logging.getLogger(__name__)
_PIPELINE_RUN_NOTE_RE = re.compile(r"Pipeline run id=(\d+)")

router = APIRouter()

JOB_LABELS = {
    "collect_all": "Сбор всех каналов",
    "photo_due": "Фото по расписанию",
    "photo_auto": "Автозагрузка фото",
    "warm_all_dialogs": "Прогрев кэша диалогов",
}

# Worker publishes `worker_heartbeat` every ~5s (src/runtime/worker.py:_publish_snapshots).
# 60s gives us 12 missed publishes before we conclude the worker is down.
WORKER_HEARTBEAT_STALE_AFTER_SEC = 60


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


def _format_retry_hint(run_after) -> str:
    if run_after is None:
        return ""
    return run_after.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def _compute_load_level(
    *,
    interval_minutes: int,
    active_unfiltered_channels: int,
    available_accounts_now: int,
    state: str,
) -> str:
    if state in {"worker_down", "all_flooded", "no_clients", "session_degraded"}:
        return "overload"
    capacity_accounts = max(1, available_accounts_now)
    pressure = active_unfiltered_channels / capacity_accounts
    if interval_minutes <= 15 and pressure >= 60:
        return "overload"
    if interval_minutes <= 30 and pressure >= 40:
        return "high"
    if pressure >= 75:
        return "high"
    return "ok"


def _collector_health_recommendations(
    *,
    state: str,
    load_level: str,
    interval_minutes: int,
    active_unfiltered_channels: int,
    available_accounts_now: int,
) -> list[str]:
    recommendations: list[str] = []
    if state == "worker_down":
        recommendations.append(
            "Telegram-воркер не запущен. Если используете `serve` — перезапустите его; "
            "если `serve --no-worker` — запустите воркер отдельно: `python -m src.main worker`. "
            "Без воркера задачи сбора копятся в БД, но не исполняются."
        )
    if state == "all_flooded":
        recommendations.append("Дождаться ближайшего окна после Flood Wait и не запускать ручной collect-all повторно.")
    if state == "no_clients":
        recommendations.append("Проверить активность аккаунтов и переподключить хотя бы один рабочий клиент.")
    if state == "session_degraded":
        recommendations.append(
            "Восстановить прежний SESSION_ENCRYPTION_KEY или повторно войти в Telegram-аккаунты."
        )
    if load_level in {"high", "overload"}:
        recommendations.append(
            f"Поднять интервал автосбора выше текущих {interval_minutes} мин, чтобы снизить частоту обращений."
        )
        recommendations.append(
            "Сократить число активных отслеживаемых каналов: "
            f"сейчас активных неотфильтрованных {active_unfiltered_channels}."
        )
    if available_accounts_now <= 1:
        recommendations.append("Добавить ещё Telegram-аккаунты, чтобы распределить нагрузку по чтению.")
    return recommendations


async def _worker_status(db) -> tuple[bool, str]:
    """Return True when the worker-process heartbeat snapshot is fresh.

    The worker publishes `worker_heartbeat` every ~5s
    (`src/runtime/worker.py:_publish_snapshots`). We treat anything older than
    `WORKER_HEARTBEAT_STALE_AFTER_SEC` as the worker being down — that turns the
    silent failure from #444 (serve running alone, collection tasks piling up
    with no executor) into an explicit banner.
    """
    try:
        snapshot = await db.repos.runtime_snapshots.get_snapshot("worker_heartbeat")
    except Exception as exc:  # noqa: BLE001
        logger.warning("Failed to read worker_heartbeat snapshot: %s", exc)
        return True, ""
    if snapshot is None or snapshot.updated_at is None:
        return False, ""
    payload = snapshot.payload if isinstance(snapshot.payload, dict) else {}
    status = str(payload.get("status", "alive"))
    if status not in {"alive", "ok"}:
        reason = str(payload.get("reason", "") or payload.get("detail", ""))
        return False, reason
    updated_at = snapshot.updated_at
    if updated_at.tzinfo is None:
        updated_at = updated_at.replace(tzinfo=timezone.utc)
    age = (datetime.now(timezone.utc) - updated_at).total_seconds()
    if age > WORKER_HEARTBEAT_STALE_AFTER_SEC:
        return False, ""
    return True, ""


async def _is_worker_alive(db) -> bool:
    alive, _ = await _worker_status(db)
    return alive


async def _build_collector_health_context(request: Request) -> dict[str, object]:
    db = deps.get_db(request)
    pool = deps.get_pool(request)
    collector = deps.get_collector(request)
    accounts = await db.get_account_summaries(active_only=False)
    connected_phones = set(pool.clients.keys())
    degraded_session_accounts = [
        acc for acc in accounts if acc.is_active and acc.session_status != AccountSessionStatus.OK
    ]
    active_accounts = [
        acc for acc in accounts if acc.is_active and acc.session_status == AccountSessionStatus.OK
    ]
    connected_active_accounts = [acc for acc in active_accounts if acc.phone in connected_phones]
    now = datetime.now(timezone.utc)
    worker_alive, worker_reason = await _worker_status(db)

    flooded_accounts = []
    next_available_at = None
    for acc in connected_active_accounts:
        flood_until = acc.flood_wait_until
        if flood_until is None:
            continue
        if flood_until.tzinfo is None:
            flood_until = flood_until.replace(tzinfo=timezone.utc)
        if flood_until <= now:
            continue
        flooded_accounts.append({"phone": acc.phone, "until": flood_until})
        if next_available_at is None or flood_until < next_available_at:
            next_available_at = flood_until

    try:
        availability = await asyncio.wait_for(collector.get_collection_availability(), timeout=1.0)
    except (asyncio.TimeoutError, Exception) as exc:  # noqa: BLE001
        logger.warning("get_collection_availability timed out or failed: %s", exc)
        availability = None
    availability_state = getattr(availability, "state", "no_connected_active")
    availability_retry_after = getattr(availability, "retry_after_sec", None)
    availability_next = getattr(availability, "next_available_at_utc", None)
    if not isinstance(availability_next, datetime):
        availability_next = None
    if not isinstance(availability_retry_after, int):
        availability_retry_after = None
    available_accounts_now = max(0, len(connected_active_accounts) - len(flooded_accounts))
    active_unfiltered_channels = len(await db.get_channels(active_only=True, include_filtered=False))
    recent_tasks = await db.get_collection_tasks(limit=200)
    recent_zero_collect_count = sum(
        1
        for task in recent_tasks
        if task.task_type.value == "channel_collect"
        and task.status == "completed"
        and task.messages_collected == 0
    )
    recent_unavailability_events = [
        task.note or task.error or ""
        for task in recent_tasks
        if (task.note and "Flood Wait" in task.note) or (task.error and "No active connected clients" in task.error)
    ][:5]

    state = "healthy"
    if not worker_alive:
        # Worker-process absent dominates: without it `no_clients` /
        # `all_flooded` are symptoms, not the root cause.
        state = "worker_down"
    elif degraded_session_accounts and not active_accounts:
        state = "session_degraded"
    elif not connected_active_accounts:
        state = "no_clients"
    elif availability_state == "all_flooded" or available_accounts_now == 0:
        state = "all_flooded"
    elif flooded_accounts:
        state = "degraded"

    interval_minutes = max(1, getattr(deps.get_scheduler(request), "interval_minutes", 60))
    load_level = _compute_load_level(
        interval_minutes=interval_minutes,
        active_unfiltered_channels=active_unfiltered_channels,
        available_accounts_now=available_accounts_now,
        state=state,
    )
    # Compute retry_after_sec from next_available_at if available
    computed_retry_after_sec = availability_retry_after
    if computed_retry_after_sec is None and (next_available_at or availability_next):
        effective_next = next_available_at or availability_next
        delta = effective_next - now
        computed_retry_after_sec = max(0, int(delta.total_seconds()))
    return {
        "state": state,
        "connected_accounts": len(connected_phones),
        "active_accounts": len(active_accounts),
        "session_degraded_accounts": len(degraded_session_accounts),
        "worker_reason": worker_reason,
        "available_accounts_now": available_accounts_now,
        "flooded_accounts": flooded_accounts,
        "flooded_accounts_count": len(flooded_accounts),
        "next_available_at": next_available_at or availability_next,
        "retry_after_sec": computed_retry_after_sec,
        "active_unfiltered_channels": active_unfiltered_channels,
        "collect_interval_minutes": interval_minutes,
        "load_level": load_level,
        "recommendations": _collector_health_recommendations(
            state=state,
            load_level=load_level,
            interval_minutes=interval_minutes,
            active_unfiltered_channels=active_unfiltered_channels,
            available_accounts_now=available_accounts_now,
        ),
        "recent_zero_collect_count": recent_zero_collect_count,
        "recent_unavailability_events": recent_unavailability_events,
        "is_running": collector.is_running,
        "next_available_label": _format_retry_hint(next_available_at or availability_next),
    }


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

    disabled_map = await db.repos.settings.get_settings_by_prefix("scheduler_job_disabled:")
    for j in jobs:
        val = disabled_map.get(f"scheduler_job_disabled:{j['job_id']}")
        j["enabled"] = val != "1"
        j["interval_editable"] = j["job_id"] not in ("photo_due", "photo_auto")
    return jobs


async def _notification_snapshot_payload(request: Request) -> dict[str, object]:
    snapshot = await deps.get_db(request).repos.runtime_snapshots.get_snapshot("notification_target_status")
    payload = snapshot.payload if snapshot is not None else {}
    return payload if isinstance(payload, dict) else {}


async def _load_pipeline_run_result_meta(db, tasks) -> dict[int, dict[str, object]]:
    run_ids_by_task_id: dict[int, int] = {}
    for task in tasks:
        if task.id is None or task.task_type.value != "pipeline_run" or not task.note:
            continue
        match = _PIPELINE_RUN_NOTE_RE.search(task.note)
        if match is None:
            continue
        run_ids_by_task_id[task.id] = int(match.group(1))
    if not run_ids_by_task_id:
        return {}

    runs = await asyncio.gather(*(db.repos.generation_runs.get(run_id) for run_id in run_ids_by_task_id.values()))
    result: dict[int, dict[str, object]] = {}
    for task_id, run in zip(run_ids_by_task_id.keys(), runs, strict=False):
        if run is None:
            continue
        metadata = run.metadata if isinstance(run.metadata, dict) else {}
        raw_errors = metadata.get("node_errors")
        node_errors = raw_errors if isinstance(raw_errors, list) else []
        errors_count = len(node_errors)
        first_error_detail: str | None = None
        if node_errors and isinstance(node_errors[0], dict):
            detail = node_errors[0].get("detail")
            if isinstance(detail, str):
                first_error_detail = detail
        result[task_id] = {
            "kind": run.result_kind,
            "count": run.result_count,
            "label": result_kind_label(run.result_kind),
            "errors_count": errors_count,
            "first_error_detail": first_error_detail,
        }
    return result


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
    return RedirectResponse(url=f"/scheduler{suffix}", status_code=303)


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

    context = {
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
