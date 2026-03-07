import logging

from fastapi import APIRouter, Query, Request
from fastapi.responses import HTMLResponse

from src.models import SearchResult
from src.web import deps

router = APIRouter()
logger = logging.getLogger(__name__)


@router.get("/", response_class=HTMLResponse)
async def search_page(
    request: Request,
    q: str = Query(""),
    channel_id: str = Query(""),
    date_from: str = Query(""),
    date_to: str = Query(""),
    mode: str = Query("local"),
    page: int = Query(1),
):
    result = None
    limit = 50
    offset = (page - 1) * limit
    channel_id_int: int | None = None
    channel_id_error: str | None = None
    if channel_id:
        try:
            channel_id_int = int(channel_id)
        except ValueError:
            channel_id_error = f"Некорректный ID канала: {channel_id}"

    service = deps.search_service(request)

    if q:
        if channel_id_error and mode in {"local", "channel"}:
            result = SearchResult(messages=[], total=0, query=q, error=channel_id_error)
        else:
            try:
                result = await service.search(
                    mode=mode,
                    query=q,
                    limit=limit,
                    channel_id=channel_id_int,
                    date_from=date_from or None,
                    date_to=date_to or None,
                    offset=offset,
                )
            except Exception as exc:
                logger.exception("Search request failed: mode=%s query=%r", mode, q)
                result = SearchResult(
                    messages=[],
                    total=0,
                    query=q,
                    error=f"Ошибка поиска: {exc}",
                )

    db = deps.get_db(request)
    channels = await db.get_channels()
    ai_enabled = deps.get_ai_search(request).enabled
    try:
        search_quota = await service.check_quota()
    except Exception:
        logger.exception("Failed to load search quota")
        search_quota = None

    total_pages = 0
    if result and result.total > 0:
        total_pages = (result.total + limit - 1) // limit

    return deps.get_templates(request).TemplateResponse(
        request,
        "search.html",
        {
            "result": result,
            "channels": channels,
            "q": q,
            "channel_id": channel_id_int,
            "date_from": date_from,
            "date_to": date_to,
            "mode": mode,
            "page": page,
            "total_pages": total_pages,
            "ai_enabled": ai_enabled,
            "search_quota": search_quota,
        },
    )
