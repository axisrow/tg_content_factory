import logging

from fastapi import APIRouter, Request
from fastapi.responses import RedirectResponse
from starlette.background import BackgroundTask

from src.web import deps

router = APIRouter()
logger = logging.getLogger(__name__)


@router.post("/{pk}/collect")
async def collect_channel(request: Request, pk: int):
    if getattr(request.app.state, "shutting_down", False):
        return RedirectResponse(url="/channels?error=shutting_down", status_code=303)

    service = deps.collection_service(request)
    ok = await service.enqueue_channel_by_pk(pk)
    if not ok:
        return RedirectResponse(url="/channels", status_code=303)

    collector = deps.get_collector(request)
    msg = "collect_queued" if collector.is_running else "collect_started"
    return RedirectResponse(url=f"/channels?msg={msg}", status_code=303)


@router.post("/stats/all")
async def collect_all_stats(request: Request):
    collector = deps.get_collector(request)
    if collector.is_stats_running:
        return RedirectResponse(url="/channels?error=stats_running", status_code=303)

    db = deps.get_db(request)
    task_id = await db.create_collection_task(0, "Обновление статистики")
    await db.update_collection_task(task_id, "running")

    async def _run_all_stats():
        try:
            stats = await collector.collect_all_stats()
            count = stats.get("channels", 0) if stats else 0
            await db.update_collection_task(task_id, "completed", messages_collected=count)
        except Exception as exc:
            logger.exception("collect_all_stats failed")
            await db.update_collection_task(task_id, "failed", error=str(exc))

    task = BackgroundTask(_run_all_stats)
    return RedirectResponse(
        url="/channels?msg=stats_collection_started", status_code=303, background=task
    )


@router.post("/{pk}/stats")
async def collect_stats(request: Request, pk: int):
    channel = await deps.channel_service(request).get_by_pk(pk)
    if not channel:
        return RedirectResponse(url="/channels", status_code=303)

    collector = deps.get_collector(request)
    if collector.is_stats_running:
        return RedirectResponse(url="/channels?error=stats_running", status_code=303)

    db = deps.get_db(request)
    task_id = await db.create_collection_task(channel.channel_id, channel.title)
    await db.update_collection_task(task_id, "running")

    async def _run_channel_stats():
        try:
            result = await collector.collect_channel_stats(channel)
            await db.update_collection_task(
                task_id, "completed", messages_collected=1 if result else 0
            )
        except Exception as exc:
            logger.exception("collect_channel_stats failed")
            await db.update_collection_task(task_id, "failed", error=str(exc))

    task = BackgroundTask(_run_channel_stats)
    return RedirectResponse(
        url="/channels?msg=stats_collection_started", status_code=303, background=task
    )
