"""Unified background-jobs read API + fragment (#964) + dashboard page (#965).

Views over JobsReadModel (the #963 unified read-model): a JSON list and an HTML
table fragment, both filterable by source / runtime-state, plus the lazyloaded
dashboard page that hosts the fragment. The only writes are collection-task
cancel/clear actions delegated to the existing scheduler handlers (#1318).
"""

from __future__ import annotations

from urllib.parse import urlencode

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse

from src.models import JobRuntimeState, JobSource
from src.web import deps
from src.web.scheduler import handlers as scheduler_handlers
from src.web.scheduler.context import _load_pipeline_run_result_meta
from src.web.scheduler.responses import SchedulerRedirect

router = APIRouter()

_MAX_JOBS_LIMIT = 500
_ACTIVE_STATES = (
    JobRuntimeState.RUNNING,
    JobRuntimeState.PENDING,
    JobRuntimeState.SCHEDULED,
    JobRuntimeState.PAUSE_GATE,
    JobRuntimeState.FLOOD_WAIT,
)
_COMPLETED_STATES = (
    JobRuntimeState.COMPLETED,
    JobRuntimeState.FAILED,
    JobRuntimeState.CANCELLED,
    JobRuntimeState.INACTIVE,
)
_PRESERVED_JOBS_QUERY_KEYS = ("source", "status", "page", "limit")


def _jobs_model(request: Request):
    from src.services.jobs_read_model import JobsReadModel

    return JobsReadModel(deps.get_db(request))


def _parse_enum_csv(raw: str | None, enum_cls):
    """Parse a comma-separated query value into enum members, dropping unknown
    tokens (so a bogus filter degrades to 'no filter', never a 422/500)."""
    if not raw:
        return None
    valid = {e.value for e in enum_cls}
    return [enum_cls(s) for s in raw.split(",") if s in valid] or None


def _parse_status_filter(raw: str | None):
    if not raw or raw == "all":
        return None
    if raw == "active":
        return _ACTIVE_STATES
    if raw == "completed":
        return _COMPLETED_STATES
    return _parse_enum_csv(raw, JobRuntimeState)


def _jobs_redirect(request: Request, result: SchedulerRedirect) -> RedirectResponse:
    """Map shared task-action results back to /jobs, preserving its filters."""
    query = {
        key: value
        for key in _PRESERVED_JOBS_QUERY_KEYS
        if (value := request.query_params.get(key))
    }
    if result.msg:
        query["msg"] = result.msg
    if result.error:
        query["error"] = result.error
    suffix = f"?{urlencode(query)}" if query else ""
    return RedirectResponse(url=f"/jobs{suffix}", status_code=303)


async def _list(
    request: Request,
    source: str | None,
    status: str | None,
    limit: int,
    page: int | None,
):
    model = _jobs_model(request)
    sources = _parse_enum_csv(source, JobSource)
    statuses = _parse_status_filter(status)
    limit = max(1, min(limit, _MAX_JOBS_LIMIT))
    if page is None:
        jobs = await model.list_jobs(sources=sources, statuses=statuses, limit=limit)
        return jobs, None, None, limit

    page = max(1, page)
    jobs, total = await model.list_jobs_paginated(
        sources=sources,
        statuses=statuses,
        page=page,
        limit=limit,
    )
    return jobs, total, page, limit


@router.get("", response_class=HTMLResponse)
async def jobs_page(
    request: Request,
    source: str | None = None,
    status: str | None = None,
    page: int = 1,
    limit: int = 100,
):
    """Unified jobs dashboard (#965).

    Paints the page shell instantly (no DB query); the filterable table is loaded
    lazily via the ``/jobs/fragments/list`` fragment with ``hx-trigger="load"``
    (the #756 lazyload pattern), so TTFB stays flat on large databases.
    """
    page = max(1, page)
    limit = max(1, min(limit, _MAX_JOBS_LIMIT))
    query_params: dict[str, object] = {}
    if source:
        query_params["source"] = source
    if status and status != "all":
        query_params["status"] = status
    if page != 1:
        query_params["page"] = page
    if limit != 100:
        query_params["limit"] = limit
    query = urlencode(query_params)
    fragment_url = f"/jobs/fragments/list?{query}" if query else "/jobs/fragments/list"
    return deps.get_templates(request).TemplateResponse(
        request,
        "jobs.html",
        {"fragment_url": fragment_url},
    )


@router.post("/tasks/{task_id}/cancel")
async def cancel_task(request: Request, task_id: int):
    result = await scheduler_handlers.cancel_task(request, task_id)
    return _jobs_redirect(request, result)


@router.post("/tasks/clear-pending-collect")
async def clear_pending_collect_tasks(request: Request):
    result = await scheduler_handlers.clear_pending_collect_tasks(request)
    return _jobs_redirect(request, result)


@router.get("/api/list")
async def api_jobs_list(
    request: Request,
    source: str | None = None,
    status: str | None = None,
    limit: int = 100,
    page: int | None = None,
):
    """Unified jobs as JSON (filters: comma-separated source / status)."""
    jobs, total, page, limit = await _list(request, source, status, limit, page)
    if page is not None:
        return JSONResponse(
            {
                "jobs": [j.model_dump(mode="json") for j in jobs],
                "total_count": total,
                "page": page,
                "limit": limit,
            }
        )
    return JSONResponse([j.model_dump(mode="json") for j in jobs])


@router.get("/fragments/list", response_class=HTMLResponse)
async def jobs_table_fragment(
    request: Request,
    source: str | None = None,
    status: str | None = None,
    limit: int = 100,
    page: int = 1,
):
    """Unified jobs table fragment (consumed by the lazyloaded dashboard, #965)."""
    jobs, total, page, limit = await _list(request, source, status, limit, page)
    total = total or 0
    total_pages = max(1, (total + limit - 1) // limit)
    if page > total_pages:
        page = total_pages
        jobs, total, page, limit = await _list(request, source, status, limit, page)

    model = _jobs_model(request)
    sources_filter = _parse_enum_csv(source, JobSource)

    async def count_for(states):
        _, count = await model.list_jobs_paginated(
            sources=sources_filter,
            statuses=states,
            page=1,
            limit=1,
        )
        return count

    if status in {None, "", "all"}:
        all_count = total
        active_count = await count_for(_ACTIVE_STATES)
    elif status == "active":
        active_count = total
        all_count = await count_for(None)
    elif status == "completed":
        completed_count = total
        all_count = await count_for(None)
        active_count = all_count - completed_count
    else:
        all_count = await count_for(None)
        active_count = await count_for(_ACTIVE_STATES)
    completed_count = all_count - active_count
    collection_visible = sources_filter is None or JobSource.COLLECTION_TASK in sources_filter
    pending_collect = (
        await deps.get_db(request).get_pending_channel_tasks() if collection_visible else []
    )
    pipeline_result_meta = await _load_pipeline_run_result_meta(deps.get_db(request), jobs)
    result_column_title = "Результат"
    visible_pipeline_labels = {
        str(meta["label"])
        for meta in pipeline_result_meta.values()
        if isinstance(meta.get("label"), str)
    }
    collection_jobs = [job for job in jobs if job.source == JobSource.COLLECTION_TASK]
    if (
        collection_jobs
        and len(collection_jobs) == len(jobs)
        and all(job.job_type == "pipeline_run" for job in collection_jobs)
        and len(visible_pipeline_labels) == 1
    ):
        result_column_title = next(iter(visible_pipeline_labels))

    return deps.get_templates(request).TemplateResponse(
        request,
        "jobs_table.html",
        {
            "jobs": jobs,
            "sources": [s.value for s in JobSource],
            "states": [s.value for s in JobRuntimeState],
            "selected_source": source or "",
            "selected_status": status or "",
            "page": page,
            "limit": limit,
            "total_count": total,
            "total_pages": total_pages,
            "all_count": all_count,
            "active_count": active_count,
            "completed_count": completed_count,
            "has_active_tasks": active_count > 0,
            "pending_collect_count": len(pending_collect),
            "pipeline_result_meta": pipeline_result_meta,
            "result_column_title": result_column_title,
        },
    )
