from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, RedirectResponse
from pydantic import ValidationError

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
    query: str = Form(""),
    interval_minutes: int = Form(60),
    is_regex: bool = Form(False),
    is_fts: bool = Form(False),
    notify_on_collect: bool = Form(False),
    track_stats: bool = Form(False),
    exclude_patterns: str = Form(""),
    max_length: int | None = Form(None),
):
    if not query.strip():
        return RedirectResponse(url="/search-queries?error=invalid_value", status_code=303)
    svc = deps.search_query_service(request)
    try:
        await svc.add(
            query,
            interval_minutes,
            is_regex=is_regex,
            is_fts=is_fts,
            notify_on_collect=notify_on_collect,
            track_stats=track_stats,
            exclude_patterns=exclude_patterns,
            max_length=max_length,
        )
    except ValidationError:
        return RedirectResponse(url="/search-queries?error=invalid_value", status_code=303)
    scheduler = deps.get_scheduler(request)
    if scheduler.is_running:
        await scheduler.sync_search_query_jobs()
    return RedirectResponse(url="/search-queries?msg=sq_added", status_code=303)


@router.post("/{sq_id}/toggle")
async def toggle_search_query(request: Request, sq_id: int):
    svc = deps.search_query_service(request)
    await svc.toggle(sq_id)
    scheduler = deps.get_scheduler(request)
    if scheduler.is_running:
        await scheduler.sync_search_query_jobs()
    return RedirectResponse(url="/search-queries?msg=sq_toggled", status_code=303)


@router.post("/{sq_id}/edit")
async def edit_search_query(
    request: Request,
    sq_id: int,
    query: str = Form(""),
    interval_minutes: int = Form(60),
    is_regex: bool = Form(False),
    is_fts: bool = Form(False),
    notify_on_collect: bool = Form(False),
    track_stats: bool = Form(False),
    exclude_patterns: str = Form(""),
    max_length: int | None = Form(None),
):
    if not query.strip():
        return RedirectResponse(url="/search-queries?error=invalid_value", status_code=303)
    svc = deps.search_query_service(request)
    try:
        await svc.update(
            sq_id,
            query,
            interval_minutes,
            is_regex=is_regex,
            is_fts=is_fts,
            notify_on_collect=notify_on_collect,
            track_stats=track_stats,
            exclude_patterns=exclude_patterns,
            max_length=max_length,
        )
    except ValidationError:
        return RedirectResponse(url="/search-queries?error=invalid_value", status_code=303)
    scheduler = deps.get_scheduler(request)
    if scheduler.is_running:
        await scheduler.sync_search_query_jobs()
    return RedirectResponse(url="/search-queries?msg=sq_edited", status_code=303)


@router.post("/{sq_id}/delete")
async def delete_search_query(request: Request, sq_id: int):
    svc = deps.search_query_service(request)
    await svc.delete(sq_id)
    scheduler = deps.get_scheduler(request)
    if scheduler.is_running:
        await scheduler.sync_search_query_jobs()
    return RedirectResponse(url="/search-queries?msg=sq_deleted", status_code=303)


@router.post("/{sq_id}/run")
async def run_search_query(request: Request, sq_id: int):
    svc = deps.search_query_service(request)
    await svc.run_once(sq_id)
    return RedirectResponse(url="/search-queries?msg=sq_run", status_code=303)
