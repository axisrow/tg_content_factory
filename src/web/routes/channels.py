import logging
import sqlite3

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse

from src.web import deps

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/", response_class=HTMLResponse)
async def channels_list(request: Request):
    service = deps.channel_service(request)
    show_all = request.query_params.get("view") == "all"
    channels, latest_stats, prev_subscriber_counts = await service.list_for_page(
        include_filtered=show_all
    )
    error = request.query_params.get("error")
    msg = request.query_params.get("msg")
    return deps.get_templates(request).TemplateResponse(
        request,
        "channels.html",
        {
            "channels": channels,
            "latest_stats": latest_stats,
            "prev_subscriber_counts": prev_subscriber_counts,
            "error": error,
            "msg": msg,
            "show_all": show_all,
        },
    )


@router.post("/add")
async def add_channel(request: Request, identifier: str = Form(...)):
    service = deps.channel_service(request)
    try:
        ok = await service.add_by_identifier(identifier)
    except RuntimeError as exc:
        if str(exc) == "no_client":
            logger.warning("Channel add failed: no client for identifier=%r", identifier)
            return RedirectResponse(url="/channels?error=no_client", status_code=303)
        logger.exception("Channel add runtime failure: identifier=%r", identifier)
        ok = False
    except Exception:
        logger.exception("Channel add failed: identifier=%r", identifier)
        ok = False

    if not ok:
        return RedirectResponse(url="/channels?error=resolve", status_code=303)
    return RedirectResponse(url="/channels?msg=channel_added", status_code=303)


@router.get("/dialogs")
async def get_dialogs(request: Request):
    service = deps.channel_service(request)
    dialogs = await service.get_dialogs_with_added_flags()
    return JSONResponse(content=dialogs)


@router.post("/add-bulk")
async def add_bulk(request: Request):
    form = await request.form()
    service = deps.channel_service(request)
    await service.add_bulk_by_dialog_ids(form.getlist("channel_ids"))
    return RedirectResponse(url="/channels?msg=channels_added", status_code=303)


@router.post("/{pk}/toggle")
async def toggle_channel(request: Request, pk: int):
    await deps.channel_service(request).toggle(pk)
    return RedirectResponse(url="/channels?msg=channel_toggled", status_code=303)


@router.post("/{pk}/delete")
async def delete_channel(request: Request, pk: int):
    try:
        await deps.channel_service(request).delete(pk)
    except sqlite3.IntegrityError:
        return RedirectResponse(url="/channels?error=channel_in_pipeline", status_code=303)
    return RedirectResponse(url="/channels?msg=channel_deleted", status_code=303)


@router.post("/refresh-types")
async def refresh_channel_types(request: Request):
    db = deps.get_db(request)
    pool = deps.get_pool(request)
    channels = await db.get_channels(active_only=True)
    updated = 0
    failed = 0
    for ch in channels:
        identifier = ch.username or str(ch.channel_id)
        try:
            info = await pool.resolve_channel(identifier)
        except Exception:
            info = None
        if info is False:
            await db.set_channel_active(ch.id, False)
            await db.set_channel_type(ch.channel_id, "unavailable")
            failed += 1
            continue
        if not info or info.get("channel_type") is None:
            failed += 1
            continue
        await db.set_channel_type(ch.channel_id, info["channel_type"])
        updated += 1
    return RedirectResponse(
        url=f"/channels?msg=types_refreshed&updated={updated}&failed={failed}",
        status_code=303,
    )


@router.post("/refresh-meta")
async def refresh_channel_meta(request: Request):
    db = deps.get_db(request)
    pool = deps.get_pool(request)
    channels = await db.get_channels(active_only=True)
    ok = 0
    failed = 0
    for ch in channels:
        try:
            meta = await pool.fetch_channel_meta(ch.channel_id, ch.channel_type)
        except Exception:
            meta = None
        if meta:
            await db.update_channel_full_meta(
                ch.channel_id,
                about=meta["about"],
                linked_chat_id=meta["linked_chat_id"],
                has_comments=meta["has_comments"],
            )
            ok += 1
        else:
            failed += 1
    return RedirectResponse(
        url=f"/channels?msg=meta_refreshed&updated={ok}&failed={failed}",
        status_code=303,
    )
