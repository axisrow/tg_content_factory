import logging
import sqlite3

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse

from src.web import deps

logger = logging.getLogger(__name__)

router = APIRouter()


async def _enqueue_channel_command(request: Request, command_type: str, payload: dict) -> RedirectResponse:
    command_id = await deps.telegram_command_service(request).enqueue(
        command_type,
        payload=payload,
        requested_by="web:channels",
    )
    return RedirectResponse(url=f"/channels?command_id={command_id}", status_code=303)


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
    if not identifier.strip():
        return RedirectResponse(url="/channels?error=resolve", status_code=303)
    return await _enqueue_channel_command(
        request,
        "channels.add_identifier",
        {"identifier": identifier.strip()},
    )


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
    return await _enqueue_channel_command(request, "channels.refresh_types", {})


@router.post("/refresh-meta")
async def refresh_channel_meta(request: Request):
    return await _enqueue_channel_command(request, "channels.refresh_meta", {})


# ── Tag endpoints ────────────────────────────────────────────────────────────

@router.get("/tags")
async def list_tags(request: Request):
    db = deps.get_db(request)
    tags = await db.repos.channels.list_all_tags()
    return JSONResponse(content={"tags": tags})


@router.post("/tags")
async def create_tag(request: Request, name: str = Form(...)):
    db = deps.get_db(request)
    await db.repos.channels.create_tag(name.strip())
    return RedirectResponse(url="/channels?msg=tag_created", status_code=303)


@router.delete("/tags/{name}")
async def delete_tag(request: Request, name: str):
    db = deps.get_db(request)
    await db.repos.channels.delete_tag(name)
    return JSONResponse(content={"ok": True})


@router.get("/{pk}/tags")
async def get_channel_tags(request: Request, pk: int):
    db = deps.get_db(request)
    tags = await db.repos.channels.get_channel_tags(pk)
    return JSONResponse(content={"tags": tags})


@router.post("/{pk}/tags")
async def set_channel_tags(request: Request, pk: int):
    db = deps.get_db(request)
    form = await request.form()
    raw = form.get("tags", "")
    tag_names = [t.strip() for t in str(raw).split(",") if t.strip()]
    await db.repos.channels.set_channel_tags(pk, tag_names)
    return RedirectResponse(url="/channels?msg=tags_updated", status_code=303)
