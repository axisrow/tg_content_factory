from fastapi import APIRouter, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from src.web import deps
from src.web.routes.channel_collection import bulk_enqueue_msg

router = APIRouter()


@router.post("/tasks/{task_id}/cancel")
async def cancel_task(request: Request, task_id: int):
    queue = deps.get_queue(request)
    await queue.cancel_task(task_id)
    return RedirectResponse(url="/scheduler?msg=task_cancelled", status_code=303)


VALID_STATUS_FILTERS = {"all", "active", "completed"}


@router.get("/", response_class=HTMLResponse)
async def scheduler_page(
    request: Request,
    page: int = Query(1),
    status: str = Query("all"),
    limit: int = Query(50),
):
    sched = deps.get_scheduler(request)
    collector = deps.get_collector(request)
    db = deps.get_db(request)
    msg = request.query_params.get("msg")

    # Validation
    page = max(1, page)
    limit = max(10, min(limit, 100))  # 10-100 задач
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

    # Get counts for all tabs (cheap count-only queries)
    all_count = await db.count_collection_tasks()
    active_count = await db.count_collection_tasks("active")
    completed_count = all_count - active_count

    # Check if there are any active tasks (for auto-refresh)
    has_active_tasks = active_count > 0

    search_log = await db.get_recent_searches()
    notifier = deps.get_notifier(request)
    bot_configured = notifier is not None and notifier.admin_chat_id is not None
    return deps.get_templates(request).TemplateResponse(
        request,
        "scheduler.html",
        {
            "is_running": sched.is_running,
            "last_run": sched.last_run,
            "last_stats": sched.last_stats,
            "interval_minutes": sched.interval_minutes,
            "search_interval_minutes": sched.search_interval_minutes,
            "last_search_run": sched.last_search_run,
            "last_search_stats": sched.last_search_stats,
            "collecting_now": collector.is_running,
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
    if not notifier or not notifier.admin_chat_id:
        return RedirectResponse(url="/scheduler?error=bot_not_configured", status_code=303)
    db = deps.get_db(request)
    queries = await db.get_notification_queries(active_only=True)
    text = "🔔 Тест уведомлений: соединение установлено"
    if queries:
        q = queries[0]
        messages, _ = await db.search_messages(query=q.query, limit=1)
        if messages:
            preview = (messages[0].text or "")[:200]
            text = f"🔔 Тест: Query '{q.query}' matched in channel:\n{preview}"
    ok = await notifier.notify(text)
    msg = "test_notification_sent" if ok else "test_notification_failed"
    return RedirectResponse(url=f"/scheduler?msg={msg}", status_code=303)


@router.post("/trigger-search")
async def trigger_search(request: Request):
    if getattr(request.app.state, "shutting_down", False):
        return RedirectResponse(url="/scheduler?error=shutting_down", status_code=303)
    await deps.scheduler_service(request).trigger_search()
    return RedirectResponse(url="/scheduler?msg=search_triggered", status_code=303)
