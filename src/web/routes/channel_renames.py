from __future__ import annotations

import logging

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from src.web import deps

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/renames", response_class=HTMLResponse)
async def rename_events_page(request: Request):
    db = deps.get_db(request)
    events = await db.list_pending_rename_events()
    return deps.get_templates(request).TemplateResponse(
        request,
        "channel_renames.html",
        {"events": events, "total": len(events)},
    )


@router.get("/renames/count")
async def rename_events_count(request: Request):
    db = deps.get_db(request)
    count = await db.count_pending_rename_events()
    if count == 0:
        return HTMLResponse(content="", status_code=200)
    return HTMLResponse(
        content=f'<span class="badge bg-warning text-dark ms-1">{count}</span>',
        status_code=200,
    )


@router.post("/renames/{event_id}/filter")
async def rename_event_filter(request: Request, event_id: int):
    """Keep the channel filtered (default after rename — just mark decision)."""
    db = deps.get_db(request)
    await db.decide_rename_event(event_id, "filter")
    return RedirectResponse(url="/channels/renames?msg=rename_filtered", status_code=303)


@router.post("/renames/{event_id}/keep")
async def rename_event_keep(request: Request, event_id: int):
    """Keep the channel (unfilter it, remove rename-related filter flags)."""
    db = deps.get_db(request)
    events = await db.list_pending_rename_events()
    event = next((e for e in events if e["id"] == event_id), None)
    if event:
        channel_id = event["channel_id"]
        channels = await db.get_channels(active_only=False, include_filtered=True)
        ch = next((c for c in channels if c.channel_id == channel_id), None)
        if ch and ch.id:
            current_flags = {f.strip() for f in (ch.filter_flags or "").split(",") if f.strip()}
            current_flags -= {"username_changed", "title_changed"}
            if current_flags:
                await db.set_channels_filtered_bulk(
                    [(channel_id, ",".join(sorted(current_flags)))]
                )
            else:
                await db.set_channel_filtered(ch.id, False)
    await db.decide_rename_event(event_id, "keep")
    return RedirectResponse(url="/channels/renames?msg=rename_kept", status_code=303)
