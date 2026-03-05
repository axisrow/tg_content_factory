from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, RedirectResponse

router = APIRouter()


@router.post("/tasks/{task_id}/cancel")
async def cancel_task(request: Request, task_id: int):
    queue = request.app.state.collection_queue
    await queue.cancel_task(task_id)
    return RedirectResponse(url="/scheduler?msg=task_cancelled", status_code=303)


@router.get("/", response_class=HTMLResponse)
async def scheduler_page(request: Request):
    sched = request.app.state.scheduler
    collector = request.app.state.collector
    db = request.app.state.db
    msg = request.query_params.get("msg")
    tasks = await db.get_collection_tasks()
    search_log = await db.get_recent_searches()
    return request.app.state.templates.TemplateResponse(
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
            "search_log": search_log,
        },
    )


@router.post("/start")
async def start_scheduler(request: Request):
    if getattr(request.app.state, "shutting_down", False):
        return RedirectResponse(url="/scheduler?error=shutting_down", status_code=303)
    sched = request.app.state.scheduler
    await sched.start()
    return RedirectResponse(url="/scheduler?msg=scheduler_started", status_code=303)


@router.post("/stop")
async def stop_scheduler(request: Request):
    sched = request.app.state.scheduler
    await sched.stop()
    return RedirectResponse(url="/scheduler?msg=scheduler_stopped", status_code=303)


@router.post("/trigger")
async def trigger_collection(request: Request):
    if getattr(request.app.state, "shutting_down", False):
        return RedirectResponse(url="/scheduler?error=shutting_down", status_code=303)
    sched = request.app.state.scheduler
    collector = request.app.state.collector
    if collector.is_running:
        return RedirectResponse(url="/scheduler?msg=already_running", status_code=303)
    await sched.trigger_background()
    return RedirectResponse(url="/scheduler?msg=triggered", status_code=303)


@router.post("/trigger-search")
async def trigger_search(request: Request):
    if getattr(request.app.state, "shutting_down", False):
        return RedirectResponse(url="/scheduler?error=shutting_down", status_code=303)
    sched = request.app.state.scheduler
    await sched.trigger_search_background()
    return RedirectResponse(url="/scheduler?msg=search_triggered", status_code=303)
