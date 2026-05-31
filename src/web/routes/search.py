from fastapi import APIRouter, Query, Request
from fastapi.responses import HTMLResponse

from src.web.search import handlers
from src.web.search.forms import extract_length as _extract_length  # noqa: F401  (back-compat import)
from src.web.search.responses import search_response

router = APIRouter()


@router.get("/", response_class=HTMLResponse)
async def root_page(request: Request):
    return search_response(request, await handlers.root_page(request))


@router.get("/search", response_class=HTMLResponse)
async def search_page(
    request: Request,
    q: str = Query(""),
    channel_id: str = Query(""),
    date_from: str = Query(""),
    date_to: str = Query(""),
    mode: str = Query("local"),
    is_fts: bool = Query(False),
    page: int = Query(1),
):
    return search_response(
        request,
        await handlers.render_search_page(
            request=request,
            q=q,
            channel_id=channel_id,
            date_from=date_from,
            date_to=date_to,
            mode=mode,
            is_fts=is_fts,
            page=page,
        ),
    )


@router.post("/search/translate/{message_db_id}")
async def translate_message_endpoint(message_db_id: int, request: Request):
    return search_response(request, await handlers.translate_message(request, message_db_id))
