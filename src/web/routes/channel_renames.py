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


def _rename_required_flags(event: dict) -> set[str]:
    """Derive which rename-related filter flags should apply given the event diff."""
    flags: set[str] = set()
    if event.get("old_title") != event.get("new_title"):
        flags.add("title_changed")
    if event.get("old_username") != event.get("new_username"):
        flags.add("username_changed")
    if not flags:
        # Safety fallback: collector creates events only on detected diff,
        # but a manual admin poke could produce a no-op event. Treat as username.
        flags.add("username_changed")
    return flags


@router.post("/renames/{event_id}/filter")
async def rename_event_filter(request: Request, event_id: int):
    """Keep channel in filter. Guarantees the final filtered state regardless
    of any prior manual unfilter between detection and this action."""
    db = deps.get_db(request)
    event = await db.get_rename_event(event_id)
    if event is None or event.get("decision") is not None:
        return RedirectResponse(
            url="/channels/renames?msg=rename_already_decided", status_code=303
        )
    required_flags = _rename_required_flags(event)
    await db.ensure_channel_filtered(event["channel_id"], required_flags)
    await db.decide_rename_event(event_id, "filter")
    return RedirectResponse(url="/channels/renames?msg=rename_filtered", status_code=303)


@router.post("/renames/{event_id}/keep")
async def rename_event_keep(request: Request, event_id: int):
    """Accept the new name/username. Removes rename-related filter flags.

    Three honest outcomes:
    - event missing or already decided -> rename_already_decided
    - channel has no other filter reasons -> unfilter -> rename_accepted
    - channel has other filter reasons -> keep filtered, strip rename flags ->
      rename_accepted_still_filtered
    """
    db = deps.get_db(request)
    event = await db.get_rename_event(event_id)
    if event is None or event.get("decision") is not None:
        return RedirectResponse(
            url="/channels/renames?msg=rename_already_decided", status_code=303
        )

    channel = await db.get_channel_by_channel_id(event["channel_id"])
    if channel is None:
        # Channel was removed while the event was pending — just close the event.
        await db.decide_rename_event(event_id, "keep")
        return RedirectResponse(
            url="/channels/renames?msg=rename_already_decided", status_code=303
        )

    existing_flags = {
        f.strip() for f in (channel.filter_flags or "").split(",") if f.strip()
    }
    remaining = existing_flags - {"username_changed", "title_changed"}

    if remaining:
        # Other filter reasons exist -> channel stays filtered, rename flags stripped.
        await db.set_channels_filtered_bulk(
            [(channel.channel_id, ",".join(sorted(remaining)))]
        )
        msg = "rename_accepted_still_filtered"
    elif channel.id is not None:
        # No other reasons -> channel returns to active collection.
        await db.set_channel_filtered(channel.id, False)
        msg = "rename_accepted"
    else:
        # Should not happen but avoid NPE on malformed channel row.
        msg = "rename_accepted"

    await db.decide_rename_event(event_id, "keep")
    return RedirectResponse(url=f"/channels/renames?msg={msg}", status_code=303)
