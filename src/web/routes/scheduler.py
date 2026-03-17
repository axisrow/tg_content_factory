from fastapi import APIRouter, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from src.telegram.notifier import Notifier
from src.web import deps
from src.web.routes.channel_collection import bulk_enqueue_msg

router = APIRouter()


@router.post("/tasks/{task_id}/cancel")
async def cancel_task(request: Request, task_id: int):
    if getattr(request.app.state, "shutting_down", False):
        return RedirectResponse(url="/scheduler?error=shutting_down", status_code=303)
    queue = deps.get_queue(request)
    await queue.cancel_task(task_id)
    return RedirectResponse(url="/scheduler?msg=task_cancelled", status_code=303)


@router.post("/tasks/clear-pending-collect")
async def clear_pending_collect_tasks(request: Request):
    if getattr(request.app.state, "shutting_down", False):
        return RedirectResponse(url="/scheduler?error=shutting_down", status_code=303)
    queue = deps.get_queue(request)
    deleted = await queue.clear_pending_tasks()
    msg = "pending_collect_tasks_deleted" if deleted > 0 else "pending_collect_tasks_empty"
    return RedirectResponse(url=f"/scheduler?msg={msg}", status_code=303)


VALID_STATUS_FILTERS = {"all", "active", "completed"}


@router.get("/", response_class=HTMLResponse)
async def scheduler_page(
    request: Request,
    page: int = Query(1),
    status: str = Query("all"),
    limit: int = Query(50),
):
    sched = deps.get_scheduler(request)
    db = deps.get_db(request)
    msg = request.query_params.get("msg")

    # Validation
    page = max(1, page)
    limit = max(10, min(limit, 100))
    status_filter = status if status in VALID_STATUS_FILTERS else "all"

    # Get tasks with filter and pagination
    offset = (page - 1) * limit
    tasks, filtered_count = await db.get_collection_tasks_paginated(
        limit=limit, offset=offset, status_filter=status_filter
    )

    # Calculate pagination; clamp page and re-fetch if needed
    total_pages = max(1, (filtered_count + limit - 1) // limit)
    if page > total_pages:
        page = total_pages
        offset = (page - 1) * limit
        tasks, filtered_count = await db.get_collection_tasks_paginated(
            limit=limit, offset=offset, status_filter=status_filter
        )

    # Get counts for all tabs
    all_count = await db.count_collection_tasks()
    active_count = await db.count_collection_tasks("active")
    completed_count = all_count - active_count

    has_active_tasks = active_count > 0
    pending_collect_count = len(await db.get_pending_channel_tasks())

    search_log = await db.get_recent_searches()
    notifier = deps.get_notifier(request)
    try:
        bot = await deps.notification_service(request).get_status()
    except Exception:
        bot = None
    bot_configured = (
        notifier is not None and notifier.admin_chat_id is not None
    ) or bot is not None
    return deps.get_templates(request).TemplateResponse(
        request,
        "scheduler.html",
        {
            "is_running": sched.is_running,
            "interval_minutes": sched.interval_minutes,
            "msg": msg,
            "tasks": tasks,
            "has_active_tasks": has_active_tasks,
            "page": page,
            "total_pages": total_pages,
            "all_count": all_count,
            "active_count": active_count,
            "completed_count": completed_count,
            "status_filter": status_filter,
            "limit": limit,
            "search_log": search_log,
            "bot_configured": bot_configured,
            "pending_collect_count": pending_collect_count,
        },
    )


@router.post("/start")
async def start_scheduler(request: Request):
    if getattr(request.app.state, "shutting_down", False):
        return RedirectResponse(url="/scheduler?error=shutting_down", status_code=303)
    await deps.scheduler_service(request).start()
    return RedirectResponse(url="/scheduler?msg=scheduler_started", status_code=303)


@router.post("/stop")
async def stop_scheduler(request: Request):
    await deps.scheduler_service(request).stop()
    return RedirectResponse(url="/scheduler?msg=scheduler_stopped", status_code=303)


@router.post("/trigger")
async def trigger_collection(request: Request):
    if getattr(request.app.state, "shutting_down", False):
        return RedirectResponse(url="/scheduler?error=shutting_down", status_code=303)
    service = deps.collection_service(request)
    result = await service.enqueue_all_channels()
    msg = bulk_enqueue_msg(result)
    return RedirectResponse(url=f"/scheduler?msg={msg}", status_code=303)


@router.post("/test-notification")
async def test_notification(request: Request):
    if getattr(request.app.state, "shutting_down", False):
        return RedirectResponse(url="/scheduler?error=shutting_down", status_code=303)

    notifier = deps.get_notifier(request)
    admin_chat_id = notifier.admin_chat_id if notifier else None
    try:
        bot = await deps.notification_service(request).get_status()
    except Exception:
        bot = None
    if not admin_chat_id and bot:
        admin_chat_id = bot.tg_user_id
    if not admin_chat_id and not bot:
        return RedirectResponse(url="/scheduler?error=bot_not_configured", status_code=303)

    db = deps.get_db(request)
    queries = await db.repos.search_queries.get_all(active_only=True)
    sample_query = next((sq for sq in queries if not sq.is_regex), None)
    if not sample_query:
        text = "🔔 Тест уведомлений: нет поисковых запросов"
    else:
        messages, _ = await db.search_messages_for_query(sample_query, limit=1)
        if messages:
            msg = messages[0]
            preview = (msg.text or "")[:200]
            if msg.channel_username:
                link = f"https://t.me/{msg.channel_username}/{msg.message_id}"
            else:
                bare_id = str(msg.channel_id).lstrip("-").removeprefix("100")
                link = f"https://t.me/c/{bare_id}/{msg.message_id}"
            text = f"🔔 Тест уведомлений:\n{preview}\n{link}"
        else:
            text = "🔔 Тест уведомлений: нет сообщений для отправки"

    target_svc = deps.get_notification_target_service(request)
    test_notifier = Notifier(target_svc, admin_chat_id, deps.get_notification_bundle(request))
    ok = await test_notifier.notify(text)
    msg = "test_notification_sent" if ok else "test_notification_failed"
    return RedirectResponse(url=f"/scheduler?msg={msg}", status_code=303)
