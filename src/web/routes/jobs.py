"""Unified background-jobs read API + fragment (#964).

Read-only views over JobsReadModel (the #963 unified read-model): a JSON list and
an HTML table fragment, both filterable by source / runtime-state. No DB writes
and no Telegram API calls — purely reads collection_tasks/telegram_commands/
photo_* tables plus the runtime snapshots.
"""

from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse

from src.models import JobRuntimeState, JobSource
from src.web import deps

router = APIRouter()

_MAX_JOBS_LIMIT = 500


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


async def _list(request: Request, source: str | None, status: str | None, limit: int):
    return await _jobs_model(request).list_jobs(
        sources=_parse_enum_csv(source, JobSource),
        statuses=_parse_enum_csv(status, JobRuntimeState),
        limit=max(1, min(limit, _MAX_JOBS_LIMIT)),
    )


@router.get("", response_class=HTMLResponse)
async def jobs_page(request: Request):
    """Unified jobs dashboard (#965).

    Paints the page shell instantly (no DB query); the filterable table is loaded
    lazily via the ``/jobs/fragments/list`` fragment with ``hx-trigger="load"``
    (the #756 lazyload pattern), so TTFB stays flat on large databases.
    """
    return deps.get_templates(request).TemplateResponse(request, "jobs.html", {})


@router.get("/api/list")
async def api_jobs_list(
    request: Request,
    source: str | None = None,
    status: str | None = None,
    limit: int = 100,
):
    """Unified jobs as JSON (filters: comma-separated source / status)."""
    jobs = await _list(request, source, status, limit)
    return JSONResponse([j.model_dump(mode="json") for j in jobs])


@router.get("/fragments/list", response_class=HTMLResponse)
async def jobs_table_fragment(
    request: Request,
    source: str | None = None,
    status: str | None = None,
    limit: int = 100,
):
    """Unified jobs table fragment (consumed by the lazyloaded dashboard, #965)."""
    jobs = await _list(request, source, status, limit)
    return deps.get_templates(request).TemplateResponse(
        request,
        "jobs_table.html",
        {
            "jobs": jobs,
            "sources": [s.value for s in JobSource],
            "states": [s.value for s in JobRuntimeState],
            "selected_source": source or "",
            "selected_status": status or "",
        },
    )
