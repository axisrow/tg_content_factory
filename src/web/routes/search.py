from fastapi import APIRouter, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse

from src.cli.commands.common import resolve_channel
from src.web import deps
from src.web.search import handlers
from src.web.search.forms import extract_length as _extract_length  # noqa: F401  (back-compat import)
from src.web.search.responses import search_response

router = APIRouter()


@router.get("/messages/{identifier}", response_class=JSONResponse)
async def read_messages(
    request: Request,
    identifier: str,
    query: str = Query(""),
    limit: int = Query(50),
    date_from: str = Query(""),
    date_to: str = Query(""),
    topic_id: int | None = Query(None),
):
    """Read collected messages of a channel from the DB (parity with CLI `messages read`)."""
    db = deps.get_db(request)
    channels = await db.get_channels()
    ch = resolve_channel(channels, identifier)
    if ch is None:
        return JSONResponse({"error": "channel_not_found"}, status_code=404)
    limit = max(1, min(limit, 500))
    messages, total = await db.search_messages(
        query=query,
        channel_id=ch.channel_id,
        date_from=date_from or None,
        date_to=date_to or None,
        limit=limit,
        topic_id=topic_id,
    )
    return JSONResponse({
        "channel_id": ch.channel_id,
        "total": total,
        "messages": [m.model_dump(mode="json") for m in messages],
    })


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


@router.post("/search/purge-cache")
async def purge_premium_search_cache_endpoint(request: Request):
    return search_response(request, await handlers.purge_premium_search_cache(request))


@router.post("/search/translate/{message_db_id}")
async def translate_message_endpoint(message_db_id: int, request: Request):
    return search_response(request, await handlers.translate_message(request, message_db_id))
