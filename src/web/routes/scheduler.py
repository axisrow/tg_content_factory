import logging

from fastapi import APIRouter, Query, Request
from fastapi.responses import HTMLResponse

from src.web import deps  # noqa: F401  (re-export: tests patch src.web.routes.scheduler.deps.*)
from src.web.routes.channel_collection import bulk_enqueue_msg  # noqa: F401  (test monkeypatch target)
from src.web.scheduler import forms, handlers  # noqa: F401  (forms re-exported for back-compat)

# Heavy context construction lives in src/web/scheduler/context.py and the
# endpoint orchestration in src/web/scheduler/handlers.py (#654). The helpers
# below are re-exported here for backward compatibility: src/web/search imports
# `_is_worker_alive` from this module (function-level), and several test modules
# import the pure presentation helpers + monkeypatch `_is_worker_alive` via this
# path.
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
from src.web.scheduler.responses import scheduler_response

logger = logging.getLogger(__name__)

router = APIRouter()

# Re-exported for backward compatibility (importers/tests reach these via
# src.web.routes.scheduler). Listed here so the imports above are not flagged
# as unused.
__all__ = [
    "router",
    "deps",
    "bulk_enqueue_msg",
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
    return scheduler_response(request, await handlers.cancel_task(request, task_id))


@router.post("/tasks/clear-pending-collect")
async def clear_pending_collect_tasks(request: Request):
    return scheduler_response(request, await handlers.clear_pending_collect_tasks(request))


@router.get("/", response_class=HTMLResponse)
async def scheduler_page(
    request: Request,
    page: int = Query(1),
    status: str = Query("all"),
    limit: int = Query(50),
):
    try:
        result = await handlers.render_scheduler_page(request, page, status, limit)
    except Exception:
        logger.exception("scheduler_page failed")
        raise
    return scheduler_response(request, result)


@router.post("/jobs/{job_id}/toggle")
async def toggle_scheduler_job(request: Request, job_id: str):
    return scheduler_response(request, await handlers.toggle_scheduler_job(request, job_id))


@router.post("/jobs/{job_id}/set-interval")
async def set_job_interval(request: Request, job_id: str):
    return scheduler_response(request, await handlers.set_job_interval(request, job_id))


@router.post("/start")
async def start_scheduler(request: Request):
    return scheduler_response(request, await handlers.start_scheduler(request))


@router.post("/stop")
async def stop_scheduler(request: Request):
    return scheduler_response(request, await handlers.stop_scheduler(request))


@router.post("/pause")
async def pause_collection_queue(request: Request):
    return scheduler_response(request, await handlers.pause_collection_queue(request))


@router.post("/resume")
async def resume_collection_queue(request: Request):
    return scheduler_response(request, await handlers.resume_collection_queue(request))


@router.post("/trigger")
async def trigger_collection(request: Request):
    return scheduler_response(request, await handlers.trigger_collection(request))


@router.post("/trigger-warm")
async def trigger_warm_dialogs(request: Request):
    return scheduler_response(request, await handlers.trigger_warm_dialogs(request))


@router.post("/test-notification")
async def test_notification(request: Request):
    return scheduler_response(request, await handlers.test_notification(request))


@router.post("/dry-run-notifications", response_class=HTMLResponse)
async def dry_run_notifications(request: Request):
    return scheduler_response(request, await handlers.dry_run_notifications(request))
