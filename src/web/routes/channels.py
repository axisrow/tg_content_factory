import logging

from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from starlette.background import BackgroundTask

from src.models import Channel, Keyword

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/", response_class=HTMLResponse)
async def channels_list(request: Request):
    db = request.app.state.db
    channels = await db.get_channels_with_counts()
    keywords = await db.get_keywords()
    latest_stats = await db.get_latest_stats_for_all()
    error = request.query_params.get("error")
    msg = request.query_params.get("msg")
    return request.app.state.templates.TemplateResponse(
        request,
        "channels.html",
        {
            "channels": channels,
            "keywords": keywords,
            "latest_stats": latest_stats,
            "error": error,
            "msg": msg,
        },
    )


@router.post("/add")
async def add_channel(request: Request, identifier: str = Form(...)):
    pool = request.app.state.pool
    db = request.app.state.db
    try:
        info = await pool.resolve_channel(identifier.strip())
    except RuntimeError as exc:
        if str(exc) == "no_client":
            return RedirectResponse(url="/channels?error=no_client", status_code=303)
        info = None
    except Exception:
        info = None
    if not info:
        return RedirectResponse(url="/channels?error=resolve", status_code=303)
    channel = Channel(
        channel_id=info["channel_id"],
        title=info["title"],
        username=info["username"],
        channel_type=info.get("channel_type"),
    )
    await db.add_channel(channel)
    return RedirectResponse(url="/channels?msg=channel_added", status_code=303)


@router.get("/dialogs")
async def get_dialogs(request: Request):
    pool = request.app.state.pool
    db = request.app.state.db
    existing = await db.get_channels()
    existing_ids = {ch.channel_id for ch in existing}
    dialogs = await pool.get_dialogs()
    for d in dialogs:
        d["already_added"] = d["channel_id"] in existing_ids
    return JSONResponse(content=dialogs)


@router.post("/add-bulk")
async def add_bulk(request: Request):
    form = await request.form()
    pool = request.app.state.pool
    db = request.app.state.db
    channel_ids = form.getlist("channel_ids")
    dialogs = await pool.get_dialogs()
    dialogs_map = {str(d["channel_id"]): d for d in dialogs}
    for cid in channel_ids:
        if cid in dialogs_map:
            d = dialogs_map[cid]
            channel = Channel(
                channel_id=d["channel_id"],
                title=d["title"],
                username=d["username"],
                channel_type=d.get("channel_type"),
            )
            await db.add_channel(channel)
    return RedirectResponse(url="/channels?msg=channels_added", status_code=303)


@router.post("/{pk}/toggle")
async def toggle_channel(request: Request, pk: int):
    db = request.app.state.db
    channels = await db.get_channels()
    for ch in channels:
        if ch.id == pk:
            await db.set_channel_active(pk, not ch.is_active)
            break
    return RedirectResponse(url="/channels?msg=channel_toggled", status_code=303)


@router.post("/{pk}/collect")
async def collect_channel(request: Request, pk: int):
    if getattr(request.app.state, "shutting_down", False):
        return RedirectResponse(url="/channels?error=shutting_down", status_code=303)
    db = request.app.state.db
    queue = request.app.state.collection_queue
    collector = request.app.state.collector
    channels = await db.get_channels()
    channel = next((ch for ch in channels if ch.id == pk), None)
    if not channel:
        return RedirectResponse(url="/channels", status_code=303)
    await queue.enqueue(channel)
    msg = "collect_queued" if collector.is_running else "collect_started"
    return RedirectResponse(url=f"/channels?msg={msg}", status_code=303)


@router.post("/{pk}/delete")
async def delete_channel(request: Request, pk: int):
    db = request.app.state.db
    await db.delete_channel(pk)
    return RedirectResponse(url="/channels?msg=channel_deleted", status_code=303)


@router.post("/stats/all")
async def collect_all_stats(request: Request):
    collector = request.app.state.collector
    task = BackgroundTask(collector.collect_all_stats)
    return RedirectResponse(
        url="/channels?msg=stats_collection_started", status_code=303, background=task
    )


@router.post("/{pk}/stats")
async def collect_stats(request: Request, pk: int):
    db = request.app.state.db
    collector = request.app.state.collector
    channels = await db.get_channels()
    channel = next((ch for ch in channels if ch.id == pk), None)
    if not channel:
        return RedirectResponse(url="/channels", status_code=303)
    await collector.collect_channel_stats(channel)
    return RedirectResponse(url="/channels?msg=stats_collected", status_code=303)


@router.post("/keywords/add")
async def add_keyword(
    request: Request,
    pattern: str = Form(...),
    is_regex: bool = Form(False),
):
    db = request.app.state.db
    kw = Keyword(pattern=pattern, is_regex=is_regex)
    await db.add_keyword(kw)
    return RedirectResponse(url="/channels?msg=keyword_added", status_code=303)


@router.post("/keywords/{keyword_id}/toggle")
async def toggle_keyword(request: Request, keyword_id: int):
    db = request.app.state.db
    keywords = await db.get_keywords()
    for kw in keywords:
        if kw.id == keyword_id:
            await db.set_keyword_active(keyword_id, not kw.is_active)
            break
    return RedirectResponse(url="/channels?msg=keyword_toggled", status_code=303)


@router.post("/keywords/{keyword_id}/delete")
async def delete_keyword(request: Request, keyword_id: int):
    db = request.app.state.db
    await db.delete_keyword(keyword_id)
    return RedirectResponse(url="/channels?msg=keyword_deleted", status_code=303)
