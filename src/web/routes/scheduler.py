from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from src.web import deps

router = APIRouter()


@router.post("/tasks/{task_id}/cancel")
async def cancel_task(request: Request, task_id: int):
    queue = deps.get_queue(request)
    await queue.cancel_task(task_id)
    return RedirectResponse(url="/scheduler?msg=task_cancelled", status_code=303)


@router.get("/", response_class=HTMLResponse)
async def scheduler_page(request: Request):
    sched = deps.get_scheduler(request)
    collector = deps.get_collector(request)
    db = deps.get_db(request)
    msg = request.query_params.get("msg")
    tasks = await db.get_collection_tasks()
    has_active_tasks = any(t.status in ("pending", "running") for t in tasks)
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
