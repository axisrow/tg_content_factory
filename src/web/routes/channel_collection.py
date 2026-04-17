import logging

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from src.models import StatsAllTaskPayload
from src.services.collection_service import BulkEnqueueResult
from src.web import deps

router = APIRouter()
logger = logging.getLogger(__name__)

_COLLECT_ALL_FORM = (
    '<form method="post" action="/channels/collect-all" class="d-inline"'
    ' hx-post="/channels/collect-all" hx-target="#collect-all-btn" hx-swap="outerHTML">'
    '<button type="submit" class="btn btn-secondary btn-sm">Собрать все каналы</button>'
    "</form>"
)

# NOTE: _COLLECT_ALL_BTN and _COLLECT_ALL_FORM must stay in sync with the
# corresponding fragment in templates/channels.html. The initial page render uses
# the Jinja template; HTMX responses reuse these Python constants.
_COLLECT_ALL_BTN = f'<span id="collect-all-btn">{_COLLECT_ALL_FORM}</span>'


def _collect_all_result_fragment(result: BulkEnqueueResult) -> str:
    scheduler_link = (
        '<a href="/scheduler" class="btn btn-outline-secondary btn-sm">' "Открыть планировщик</a>"
    )
    if result.total_candidates == 0:
        message = "Нет активных каналов для загрузки."
        extra = ""
    elif result.queued_count > 0:
        message = f"Добавлено задач: {result.queued_count}."
        extra = scheduler_link
    else:
        message = "Новых задач не добавлено: всё уже в очереди."
        extra = scheduler_link
    return (
        '<span id="collect-all-btn">'
        '<span style="display:inline-flex;gap:0.5rem;align-items:center;flex-wrap:wrap">'
        f"<small>{message}</small>"
        f"{extra}"
        f"{_COLLECT_ALL_FORM}"
        "</span>"
        "</span>"
    )


def bulk_enqueue_msg(result: BulkEnqueueResult) -> str:
    """Map BulkEnqueueResult to a flash-message key."""
    if result.total_candidates == 0:
        return "collect_all_empty"
    if result.queued_count > 0:
        return "collect_all_queued"
    return "collect_all_noop"


def _collect_all_redirect_url(result: BulkEnqueueResult) -> str:
    return f"/channels?msg={bulk_enqueue_msg(result)}"


@router.post("/collect-all")
async def collect_all_channels(request: Request):
    is_htmx = request.headers.get("HX-Request") == "true"

    if getattr(request.app.state, "shutting_down", False):
        if is_htmx:
            return HTMLResponse(
                '<span id="collect-all-btn" title="Сервер останавливается">⚠️</span>'
            )
        return RedirectResponse(url="/channels?error=shutting_down", status_code=303)

    service = deps.collection_service(request)
    result = await service.enqueue_all_channels()

    if is_htmx:
        return HTMLResponse(_collect_all_result_fragment(result))
    return RedirectResponse(url=_collect_all_redirect_url(result), status_code=303)


@router.post("/{pk}/collect")
async def collect_channel(request: Request, pk: int):
    is_htmx = request.headers.get("HX-Request") == "true"

    if getattr(request.app.state, "shutting_down", False):
        if is_htmx:
            return HTMLResponse(
                f'<span id="collect-btn-{pk}" title="Сервер останавливается">' f"⚠️</span>"
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
        if enqueue_status == "already_active":
            btn = (
                '<button class="btn btn-outline-secondary btn-sm emoji-btn"'
                ' disabled title="Уже в очереди">⏳</button>'
            )
            fragment = (
                f'<span id="collect-btn-{pk}">{btn}</span>'
                f'<span id="collect-btn-m-{pk}" hx-swap-oob="true">'
                f"{btn}</span>"
            )
            return HTMLResponse(fragment)
        collector = deps.get_collector(request)
        label = "В очереди" if collector.is_running else "Запущен"
        filtered_badge = ' <small title="Канал отфильтрован">⚡</small>' if is_filtered else ""
        btn = (
            f'<button class="btn btn-outline-primary btn-sm emoji-btn"'
            f' disabled title="{label}">⏳</button>'
        )
        # Update both desktop and mobile buttons via HTMX OOB swap
        fragment = (
            f'<span id="collect-btn-{pk}">{btn}{filtered_badge}</span>'
            f'<span id="collect-btn-m-{pk}" hx-swap-oob="true">'
            f"{btn}{filtered_badge}</span>"
        )
        return HTMLResponse(fragment)

    if enqueue_status == "not_found":
        return RedirectResponse(url="/channels?msg=channel_not_found", status_code=303)
    if enqueue_status == "already_active":
        return RedirectResponse(url="/channels?msg=collect_already_active", status_code=303)
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
    channels_without_stats = [ch for ch in channels if ch.channel_id not in latest_stats]
    channels_with_stats = [ch for ch in channels if ch.channel_id in latest_stats]
    ordered_channels = channels_without_stats + channels_with_stats
    payload = StatsAllTaskPayload(
        channel_ids=[ch.channel_id for ch in ordered_channels],
    )
    await db.create_stats_task(
        payload,
    )

    msg = "stats_collection_queued" if collector.is_running else "stats_collection_started"
    return RedirectResponse(url=f"/channels?msg={msg}", status_code=303)


@router.post("/{pk}/stats")
async def collect_stats(request: Request, pk: int):
    channel = await deps.channel_service(request).get_by_pk(pk)
    if not channel:
        return RedirectResponse(url="/channels", status_code=303)

    collector = deps.get_collector(request)
    if getattr(collector, "is_stats_running", False):
        return RedirectResponse(url="/channels?error=stats_running", status_code=303)

    cmd_id = await deps.telegram_command_service(request).enqueue(
        "channels.collect_stats",
        payload={"channel_pk": pk},
        requested_by="web:collect_stats",
    )
    return RedirectResponse(
        url=f"/channels?msg=stats_collection_queued&command_id={cmd_id}",
        status_code=303,
    )
