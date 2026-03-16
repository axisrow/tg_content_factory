from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse

from src.web import deps

router = APIRouter()


@router.get("", response_class=HTMLResponse)
async def analytics_page(
    request: Request,
    date_from: str = "",
    date_to: str = "",
    sort_by: str = "reactions",
    limit: int = 50,
):
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
            "sort_by": sort_by,
            "limit": limit,
        },
    )
