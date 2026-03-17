from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse

from src.services.content_calendar_service import ContentCalendarService
from src.web import deps

router = APIRouter()


@router.get("/", response_class=HTMLResponse)
async def calendar_page(request: Request, days: int = 7, pipeline_id: int | None = None):
    """Render content calendar page."""
    db = deps.get_db(request)
    calendar = ContentCalendarService(db)
    
    calendar_days = await calendar.get_calendar(days=days, pipeline_id=pipeline_id)
    stats = await calendar.get_stats()
    upcoming = await calendar.get_upcoming(limit=10, pipeline_id=pipeline_id)
    
    pipelines = await db.repos.content_pipelines.get_all()
    
    return deps.get_templates(request).TemplateResponse(
        request,
        "calendar.html",
        {
            "calendar_days": calendar_days,
            "stats": stats,
            "upcoming": upcoming,
            "pipelines": pipelines,
            "selected_pipeline_id": pipeline_id,
            "days": days,
        },
    )


@router.get("/api/calendar")
async def api_calendar(request: Request, days: int = 7, pipeline_id: int | None = None):
    """Get calendar data as JSON."""
    db = deps.get_db(request)
    calendar = ContentCalendarService(db)
    
    calendar_days = await calendar.get_calendar(days=days, pipeline_id=pipeline_id)
    
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
async def api_upcoming(request: Request, limit: int = 20, pipeline_id: int | None = None):
    """Get upcoming events as JSON."""
    db = deps.get_db(request)
    calendar = ContentCalendarService(db)
    
    events = await calendar.get_upcoming(limit=limit, pipeline_id=pipeline_id)
    
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
