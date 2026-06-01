from fastapi import APIRouter, Form, Request
from fastapi.responses import HTMLResponse

from src.web.search_queries import handlers
from src.web.search_queries.forms import SearchQueryForm
from src.web.search_queries.responses import search_query_response

router = APIRouter()


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
