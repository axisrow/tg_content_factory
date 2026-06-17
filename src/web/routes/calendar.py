from __future__ import annotations

from fastapi import APIRouter, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse

from src.services.content_calendar_service import ContentCalendarService
from src.web import deps
from src.web.query_params import parse_optional_int

router = APIRouter()


def _calendar(request: Request) -> ContentCalendarService:
    return ContentCalendarService(deps.get_db(request))


@router.get("/", response_class=HTMLResponse)
async def calendar_page(
    request: Request,
    days: int = Query(default=7, ge=1, le=90),
    pipeline_id: str | None = None,
):
    """Render the calendar skeleton; the grid/stats/upcoming load lazily (#756)."""
    db = deps.get_db(request)
    selected_pipeline_id = parse_optional_int(pipeline_id)
    pipelines = await db.repos.content_pipelines.get_all()

    return deps.get_templates(request).TemplateResponse(
        request,
        "calendar.html",
        {
            "pipelines": pipelines,
            "selected_pipeline_id": selected_pipeline_id,
            "days": days,
        },
    )


@router.get("/fragments/stats", response_class=HTMLResponse)
async def fragment_stats(request: Request):
    stats = await _calendar(request).get_stats()
    return deps.get_templates(request).TemplateResponse(
        request, "calendar/_stats.html", {"stats": stats}
    )


@router.get("/fragments/grid", response_class=HTMLResponse)
async def fragment_grid(
    request: Request,
    days: int = Query(default=7, ge=1, le=90),
    pipeline_id: str | None = None,
):
    calendar_days = await _calendar(request).get_calendar(
        days=days, pipeline_id=parse_optional_int(pipeline_id)
    )
    return deps.get_templates(request).TemplateResponse(
        request, "calendar/_grid.html", {"calendar_days": calendar_days, "days": days}
    )


@router.get("/fragments/upcoming", response_class=HTMLResponse)
async def fragment_upcoming(request: Request, pipeline_id: str | None = None):
    upcoming = await _calendar(request).get_upcoming(
        limit=10, pipeline_id=parse_optional_int(pipeline_id)
    )
    return deps.get_templates(request).TemplateResponse(
        request, "calendar/_upcoming.html", {"upcoming": upcoming}
    )


@router.get("/api/calendar")
async def api_calendar(
    request: Request,
    days: int = Query(default=7, ge=1, le=90),
    pipeline_id: str | None = None,
):
    """Get calendar data as JSON."""
    db = deps.get_db(request)
    calendar = ContentCalendarService(db)
    selected_pipeline_id = parse_optional_int(pipeline_id)

    calendar_days = await calendar.get_calendar(days=days, pipeline_id=selected_pipeline_id)

    return JSONResponse([
        {
            "date": day.date,
            "events": [
                {
                    "run_id": e.run_id,
                    "pipeline_id": e.pipeline_id,
                    "pipeline_name": e.pipeline_name,
                    "status": e.status,
                    "moderation_status": e.moderation_status,
                    "scheduled_time": e.scheduled_time.isoformat() if e.scheduled_time else None,
                    "preview": e.preview,
                }
                for e in day.events
            ],
        }
        for day in calendar_days
    ])


@router.get("/api/upcoming")
async def api_upcoming(
    request: Request,
    limit: int = Query(default=20, ge=1, le=200),
    pipeline_id: str | None = None,
):
    """Get upcoming events as JSON."""
    db = deps.get_db(request)
    calendar = ContentCalendarService(db)
    selected_pipeline_id = parse_optional_int(pipeline_id)

    events = await calendar.get_upcoming(limit=limit, pipeline_id=selected_pipeline_id)

    return JSONResponse([
        {
            "run_id": e.run_id,
            "pipeline_id": e.pipeline_id,
            "pipeline_name": e.pipeline_name,
            "status": e.status,
            "moderation_status": e.moderation_status,
            "scheduled_time": e.scheduled_time.isoformat() if e.scheduled_time else None,
            "preview": e.preview,
        }
        for e in events
    ])


@router.get("/api/stats")
async def api_stats(request: Request):
    """Get calendar statistics as JSON."""
    db = deps.get_db(request)
    calendar = ContentCalendarService(db)
    return JSONResponse(await calendar.get_stats())
