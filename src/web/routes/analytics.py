from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse

from src.services.content_analytics_service import ContentAnalyticsService
from src.web import deps

router = APIRouter()


@router.get("", response_class=HTMLResponse)
async def analytics_page(
    request: Request,
    date_from: str = "",
    date_to: str = "",
    limit: int = 50,
):
    limit = limit if limit in (20, 50, 100) else 50
    db = deps.get_db(request)
    df = date_from or None
    dt = date_to or None

    top_messages = await db.get_top_messages(limit=limit, date_from=df, date_to=dt)
    by_media_type = await db.get_engagement_by_media_type(date_from=df, date_to=dt)
    hourly = await db.get_hourly_activity(date_from=df, date_to=dt)

    return deps.get_templates(request).TemplateResponse(
        request,
        "analytics.html",
        {
            "top_messages": top_messages,
            "by_media_type": by_media_type,
            "hourly": hourly,
            "date_from": date_from,
            "date_to": date_to,
            "limit": limit,
        },
    )


@router.get("/content", response_class=HTMLResponse)
async def content_analytics_page(request: Request):
    """Render content analytics dashboard page."""
    db = deps.get_db(request)
    analytics = ContentAnalyticsService(db)

    summary = await analytics.get_summary()
    pipeline_stats = await analytics.get_pipeline_stats()

    return deps.get_templates(request).TemplateResponse(
        request,
        "analytics/content.html",
        {
            "summary": summary,
            "pipeline_stats": pipeline_stats,
        },
    )


@router.get("/content/api/summary")
async def api_content_summary(request: Request):
    """Get content summary statistics as JSON."""
    db = deps.get_db(request)
    analytics = ContentAnalyticsService(db)
    return JSONResponse(await analytics.get_summary())


@router.get("/content/api/pipelines")
async def api_pipeline_stats(request: Request, pipeline_id: int | None = None):
    """Get pipeline statistics as JSON."""
    db = deps.get_db(request)
    analytics = ContentAnalyticsService(db)
    stats = await analytics.get_pipeline_stats(pipeline_id)
    return JSONResponse([
        {
            "pipeline_id": s.pipeline_id,
            "pipeline_name": s.pipeline_name,
            "total_generations": s.total_generations,
            "total_published": s.total_published,
            "total_rejected": s.total_rejected,
            "pending_moderation": s.pending_moderation,
            "success_rate": s.success_rate,
        }
        for s in stats
    ])
