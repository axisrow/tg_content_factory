from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from src.web import deps

router = APIRouter()


@router.get("/", response_class=HTMLResponse)
async def search_queries_page(request: Request):
    svc = deps.search_query_service(request)
    items = await svc.get_with_stats()
    return deps.get_templates(request).TemplateResponse(
        request, "search_queries.html", {"items": items}
    )


@router.post("/add")
async def add_search_query(
    request: Request,
    name: str = Form(...),
    query: str = Form(...),
    interval_minutes: int = Form(60),
):
    svc = deps.search_query_service(request)
    await svc.add(name, query, interval_minutes)
    scheduler = deps.get_scheduler(request)
    if scheduler.is_running and hasattr(scheduler, "_sq_bundle") and scheduler._sq_bundle:
        await scheduler.sync_search_query_jobs()
    return RedirectResponse(url="/search-queries?msg=sq_added", status_code=303)


@router.post("/{sq_id}/toggle")
async def toggle_search_query(request: Request, sq_id: int):
    svc = deps.search_query_service(request)
    await svc.toggle(sq_id)
    scheduler = deps.get_scheduler(request)
    if scheduler.is_running and hasattr(scheduler, "_sq_bundle") and scheduler._sq_bundle:
        await scheduler.sync_search_query_jobs()
    return RedirectResponse(url="/search-queries?msg=sq_toggled", status_code=303)


@router.post("/{sq_id}/delete")
async def delete_search_query(request: Request, sq_id: int):
    svc = deps.search_query_service(request)
    await svc.delete(sq_id)
    scheduler = deps.get_scheduler(request)
    if scheduler.is_running and hasattr(scheduler, "_sq_bundle") and scheduler._sq_bundle:
        await scheduler.sync_search_query_jobs()
    return RedirectResponse(url="/search-queries?msg=sq_deleted", status_code=303)


@router.post("/{sq_id}/run")
async def run_search_query(request: Request, sq_id: int):
    svc = deps.search_query_service(request)
    await svc.run_once(sq_id)
    return RedirectResponse(url="/search-queries?msg=sq_run", status_code=303)
