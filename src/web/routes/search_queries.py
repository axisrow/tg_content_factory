from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse, JSONResponse

from src.web import deps
from src.web.search_queries import handlers
from src.web.search_queries.forms import SearchQueryForm
from src.web.search_queries.responses import search_query_response

router = APIRouter()


@router.get("/{sq_id}", response_class=JSONResponse)
async def get_search_query(request: Request, sq_id: int):
    """Get one search query as JSON (parity with CLI `search-query get`)."""
    sq = await deps.search_query_service(request).get(sq_id)
    if sq is None:
        return JSONResponse({"error": "search_query_not_found"}, status_code=404)
    return JSONResponse(sq.model_dump(mode="json"))


@router.get("/{sq_id}/stats", response_class=JSONResponse)
async def get_search_query_stats(request: Request, sq_id: int, days: int = 30):
    """Get daily match stats for a search query (parity with CLI `search-query stats`)."""
    svc = deps.search_query_service(request)
    if await svc.get(sq_id) is None:
        return JSONResponse({"error": "search_query_not_found"}, status_code=404)
    stats = await svc.get_daily_stats(sq_id, days=days)
    return JSONResponse([s.model_dump(mode="json") for s in stats])


@router.get("/", response_class=HTMLResponse)
async def search_queries_page(request: Request):
    return search_query_response(request, await handlers.search_queries_page(request))


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
    chat_filter: str = Form(""),
):
    form = SearchQueryForm(
        query=query,
        interval_minutes=interval_minutes,
        is_regex=is_regex,
        is_fts=is_fts,
        notify_on_collect=notify_on_collect,
        track_stats=track_stats,
        exclude_patterns=exclude_patterns,
        max_length=max_length,
        chat_filter=chat_filter,
    )
    return search_query_response(request, await handlers.add_search_query(request, form))


@router.post("/{sq_id}/toggle")
async def toggle_search_query(request: Request, sq_id: int):
    return search_query_response(request, await handlers.toggle_search_query(request, sq_id))


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
    chat_filter: str = Form(""),
):
    form = SearchQueryForm(
        query=query,
        interval_minutes=interval_minutes,
        is_regex=is_regex,
        is_fts=is_fts,
        notify_on_collect=notify_on_collect,
        track_stats=track_stats,
        exclude_patterns=exclude_patterns,
        max_length=max_length,
        chat_filter=chat_filter,
    )
    return search_query_response(request, await handlers.edit_search_query(request, sq_id, form))


@router.post("/{sq_id}/delete")
async def delete_search_query(request: Request, sq_id: int):
    return search_query_response(request, await handlers.delete_search_query(request, sq_id))


@router.post("/{sq_id}/run")
async def run_search_query(request: Request, sq_id: int):
    return search_query_response(request, await handlers.run_search_query(request, sq_id))
