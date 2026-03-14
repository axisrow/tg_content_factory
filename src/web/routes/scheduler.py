from fastapi import APIRouter, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from src.web import deps

router = APIRouter()


@router.post("/tasks/{task_id}/cancel")
async def cancel_task(request: Request, task_id: int):
    queue = deps.get_queue(request)
    await queue.cancel_task(task_id)
    return RedirectResponse(url="/scheduler?msg=task_cancelled", status_code=303)


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

    # Get tasks with filter and pagination
    offset = (page - 1) * limit
    tasks, total_count = await db.get_collection_tasks_paginated(
        limit=limit, offset=offset, status_filter=status
    )

    # Calculate pagination
    total_pages = (total_count + limit - 1) // limit if total_count > 0 else 1
    if page > total_pages:
        page = max(1, total_pages)

    # Count active tasks (for counter)
    _, active_count = await db.get_collection_tasks_paginated(
        limit=10000, status_filter="active"
    )

    # Check if there are any active tasks (for auto-refresh)
    has_active_tasks = active_count > 0

    search_log = await db.get_recent_searches()
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
            "total_count": total_count,
            "active_count": active_count,
            "status_filter": status,
            "limit": limit,
            "search_log": search_log,
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
    collector = deps.get_collector(request)
    if collector.is_running:
        return RedirectResponse(url="/scheduler?msg=already_running", status_code=303)
    await deps.scheduler_service(request).trigger_collection()
    return RedirectResponse(url="/scheduler?msg=triggered", status_code=303)


@router.post("/trigger-search")
async def trigger_search(request: Request):
    if getattr(request.app.state, "shutting_down", False):
        return RedirectResponse(url="/scheduler?error=shutting_down", status_code=303)
    await deps.scheduler_service(request).trigger_search()
    return RedirectResponse(url="/scheduler?msg=search_triggered", status_code=303)
