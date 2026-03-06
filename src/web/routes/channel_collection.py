import logging

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from starlette.background import BackgroundTask

from src.web import deps

router = APIRouter()
logger = logging.getLogger(__name__)


@router.post("/{pk}/collect")
async def collect_channel(request: Request, pk: int):
    is_htmx = request.headers.get("HX-Request") == "true"

    if getattr(request.app.state, "shutting_down", False):
        if is_htmx:
            return HTMLResponse(
                f'<span id="collect-btn-{pk}" title="Сервер останавливается">'
                f'⚠️</span>'
            )
        return RedirectResponse(url="/channels?error=shutting_down", status_code=303)

    service = deps.collection_service(request)
    db = deps.get_db(request)
    channel = await db.get_channel_by_pk(pk)
    is_filtered = channel.is_filtered if channel else False
    enqueue_status = await service.enqueue_channel_by_pk(pk, force=True)

    if is_htmx:
        if enqueue_status == "not_found":
            return HTMLResponse(f'<span id="collect-btn-{pk}">❓</span>')
        collector = deps.get_collector(request)
        label = "В очереди" if collector.is_running else "Запущен"
        filtered_badge = ' <small title="Канал отфильтрован">⚡</small>' if is_filtered else ""
        return HTMLResponse(
            f'<span id="collect-btn-{pk}">'
            f'<button class="outline emoji-btn" disabled title="{label}">⏳</button>'
            f'{filtered_badge}'
            f'</span>'
        )

    if enqueue_status == "not_found":
        return RedirectResponse(url="/channels?msg=channel_not_found", status_code=303)
    collector = deps.get_collector(request)
    msg = "collect_queued" if collector.is_running else "collect_started"
    return RedirectResponse(url=f"/channels?msg={msg}", status_code=303)


@router.post("/stats/all")
async def collect_all_stats(request: Request):
    if getattr(request.app.state, "shutting_down", False):
        return RedirectResponse(url="/channels?error=shutting_down", status_code=303)

    collector = deps.get_collector(request)
    db = deps.get_db(request)
    existing = await db.get_active_stats_task()
    if existing:
        return RedirectResponse(url="/channels?error=stats_running", status_code=303)

    channels = await db.get_channels(active_only=True, include_filtered=False)
    latest_stats = await db.get_latest_stats_for_all()
    channels_without_stats = [
        ch for ch in channels if ch.channel_id not in latest_stats
    ]
    channels_with_stats = [
        ch for ch in channels if ch.channel_id in latest_stats
    ]
    ordered_channels = channels_without_stats + channels_with_stats
    payload = {
        "task_kind": "stats_all",
        "channel_ids": [ch.channel_id for ch in ordered_channels],
        "next_index": 0,
        "batch_size": 20,
        "channels_ok": 0,
        "channels_err": 0,
    }
    await db.create_collection_task(
        0,
        "Обновление статистики",
        payload=payload,
    )

    msg = "stats_collection_queued" if collector.is_running else "stats_collection_started"
    return RedirectResponse(url=f"/channels?msg={msg}", status_code=303)


@router.post("/{pk}/stats")
async def collect_stats(request: Request, pk: int):
    channel = await deps.channel_service(request).get_by_pk(pk)
    if not channel:
        return RedirectResponse(url="/channels", status_code=303)

    collector = deps.get_collector(request)
    if collector.is_stats_running:
        return RedirectResponse(url="/channels?error=stats_running", status_code=303)

    db = deps.get_db(request)
    task_id = await db.create_collection_task(
        channel.channel_id, channel.title, channel_username=channel.username
    )
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
