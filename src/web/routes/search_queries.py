from typing import cast

from fastapi import APIRouter, Depends, Request
from fastapi.responses import HTMLResponse, JSONResponse

from src.models import SearchQuery, SearchQueryDailyStat
from src.web import deps
from src.web.schemas.common import ErrorResponse
from src.web.search_queries import handlers
from src.web.search_queries.forms import SearchQueryForm, search_query_form
from src.web.search_queries.responses import search_query_response

router = APIRouter()


@router.get(
    "/{sq_id}",
    response_class=JSONResponse,
    response_model=SearchQuery,
    status_code=200,
    tags=["search-queries"],
    summary="Get one saved search query",
    responses={404: {"model": ErrorResponse, "description": "Search query not found"}},
)
async def get_search_query(request: Request, sq_id: int):
    """Get one search query as JSON (parity with CLI `search-query get`).

    Returns 404 with ``{"error": "search_query_not_found"}`` for an unknown id.
    """
    sq = await deps.search_query_service(request).get(sq_id)
    if sq is None:
        return JSONResponse({"error": "search_query_not_found"}, status_code=404)
    return JSONResponse(sq.model_dump(mode="json"))


@router.get(
    "/{sq_id}/stats",
    response_class=JSONResponse,
    response_model=list[SearchQueryDailyStat],
    status_code=200,
    tags=["search-queries"],
    summary="Daily match stats for a saved search query",
    responses={404: {"model": ErrorResponse, "description": "Search query not found"}},
)
async def get_search_query_stats(request: Request, sq_id: int, days: int = 30):
    """Get daily match stats for a search query (parity with CLI `search-query stats`).

    Returns 404 with ``{"error": "search_query_not_found"}`` for an unknown id.
    """
    svc = deps.search_query_service(request)
    if await svc.get(sq_id) is None:
        return JSONResponse({"error": "search_query_not_found"}, status_code=404)
    stats = cast(list[SearchQueryDailyStat], await svc.get_daily_stats(sq_id, days=days))
    return JSONResponse([s.model_dump(mode="json") for s in stats])


@router.get("/", response_class=HTMLResponse)
async def search_queries_page(request: Request):
    return search_query_response(request, await handlers.search_queries_page(request))


@router.post("/add")
async def add_search_query(
    request: Request,
    form: SearchQueryForm = Depends(search_query_form),
):
    return search_query_response(request, await handlers.add_search_query(request, form))


@router.post("/{sq_id}/toggle")
async def toggle_search_query(request: Request, sq_id: int):
    return search_query_response(request, await handlers.toggle_search_query(request, sq_id))


@router.post("/{sq_id}/edit")
async def edit_search_query(
    request: Request,
    sq_id: int,
    form: SearchQueryForm = Depends(search_query_form),
):
    return search_query_response(request, await handlers.edit_search_query(request, sq_id, form))


@router.post("/{sq_id}/delete")
async def delete_search_query(request: Request, sq_id: int):
    return search_query_response(request, await handlers.delete_search_query(request, sq_id))


@router.post("/{sq_id}/run")
async def run_search_query(request: Request, sq_id: int):
    return search_query_response(request, await handlers.run_search_query(request, sq_id))
