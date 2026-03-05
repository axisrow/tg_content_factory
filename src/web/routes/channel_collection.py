from fastapi import APIRouter, Request
from fastapi.responses import RedirectResponse
from starlette.background import BackgroundTask

from src.web import deps

router = APIRouter()


@router.post("/{pk}/collect")
async def collect_channel(request: Request, pk: int):
    if getattr(request.app.state, "shutting_down", False):
        return RedirectResponse(url="/channels?error=shutting_down", status_code=303)

    queue = deps.get_queue(request)
    db = deps.get_db(request)
    channels = await db.get_channels()
    channel = next((ch for ch in channels if ch.id == pk), None)
    if not channel:
        return RedirectResponse(url="/channels", status_code=303)
    await queue.enqueue(channel)

    collector = deps.get_collector(request)
    msg = "collect_queued" if collector.is_running else "collect_started"
    return RedirectResponse(url=f"/channels?msg={msg}", status_code=303)


@router.post("/stats/all")
async def collect_all_stats(request: Request):
    collector = deps.get_collector(request)
    task = BackgroundTask(collector.collect_all_stats)
    return RedirectResponse(
        url="/channels?msg=stats_collection_started", status_code=303, background=task
    )


@router.post("/{pk}/stats")
async def collect_stats(request: Request, pk: int):
    channel = await deps.channel_service(request).get_by_pk(pk)
    if not channel:
        return RedirectResponse(url="/channels", status_code=303)
    collector = deps.get_collector(request)
    task = BackgroundTask(collector.collect_channel_stats, channel)
    return RedirectResponse(
        url="/channels?msg=stats_collection_started", status_code=303, background=task
    )
