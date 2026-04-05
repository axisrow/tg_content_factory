from __future__ import annotations

import dataclasses

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse

from src.services.channel_analytics_service import ChannelAnalyticsService
from src.services.content_analytics_service import ContentAnalyticsService
from src.services.trend_service import TrendService
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


@router.get("/trends", response_class=HTMLResponse)
async def trends_page(request: Request, days: int = 7):
    """Render trending topics and channels page."""
    days = days if days in (7, 14, 30) else 7
    db = deps.get_db(request)
    trend = TrendService(db)
    topics = await trend.get_trending_topics(days=days, limit=20)
    channels = await trend.get_trending_channels(days=days, limit=10)
    emojis = await trend.get_trending_emojis(days=days, limit=15)
    return deps.get_templates(request).TemplateResponse(
        request,
        "analytics/trends.html",
        {
            "topics": topics,
            "channels": channels,
            "emojis": emojis,
            "days": days,
        },
    )


# ── Channel analytics ────────────────────────────────────────────


@router.get("/channels", response_class=HTMLResponse)
async def channel_analytics_page(request: Request, channel_id: int = 0, days: int = 30):
    """Render per-channel analytics dashboard."""
    days = days if days in (7, 14, 30, 90) else 30
    db = deps.get_db(request)
    svc = ChannelAnalyticsService(db)
    channels = await svc.get_active_channels()
    return deps.get_templates(request).TemplateResponse(
        request,
        "analytics/channels.html",
        {
            "channels": [dataclasses.asdict(c) for c in channels],
            "selected_channel_id": channel_id,
            "days": days,
        },
    )


def _svc(request: Request) -> ChannelAnalyticsService:
    return ChannelAnalyticsService(deps.get_db(request))


@router.get("/channels/api/overview")
async def api_channel_overview(request: Request, channel_id: int):
    overview = await _svc(request).get_channel_overview(channel_id)
    return JSONResponse(dataclasses.asdict(overview))


@router.get("/channels/api/subscribers")
async def api_subscriber_history(request: Request, channel_id: int, days: int = 30):
    data = await _svc(request).get_subscriber_history(channel_id, days)
    return JSONResponse(data)


@router.get("/channels/api/views")
async def api_views_timeseries(request: Request, channel_id: int, days: int = 30):
    data = await _svc(request).get_views_timeseries(channel_id, days)
    return JSONResponse(data)


@router.get("/channels/api/frequency")
async def api_post_frequency(request: Request, channel_id: int, days: int = 30):
    data = await _svc(request).get_post_frequency(channel_id, days)
    return JSONResponse(data)


@router.get("/channels/api/err")
async def api_err(request: Request, channel_id: int):
    svc = _svc(request)
    err = await svc.get_err(channel_id)
    err24 = await svc.get_err24(channel_id)
    return JSONResponse({"err": err, "err24": err24})


@router.get("/channels/api/hourly")
async def api_hourly_activity(request: Request, channel_id: int, days: int = 30):
    data = await _svc(request).get_hourly_activity(channel_id, days)
    return JSONResponse(data)


@router.get("/channels/api/citation")
async def api_citation_stats(request: Request, channel_id: int):
    stats = await _svc(request).get_citation_stats(channel_id)
    return JSONResponse(dataclasses.asdict(stats))
