import logging

from fastapi import APIRouter, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from src.models import SearchResult
from src.web import deps

router = APIRouter()
logger = logging.getLogger(__name__)


async def _render_search_page(
    request: Request,
    q: str,
    channel_id: str,
    date_from: str,
    date_to: str,
    mode: str,
    is_fts: bool,
    msg_length_op: str,
    msg_length_val: int | None,
    page: int,
) -> HTMLResponse | RedirectResponse:
    # Onboarding: redirect if no accounts configured
    auth = deps.get_auth(request)
    if not auth.is_configured:
        return RedirectResponse(url="/settings", status_code=303)
    db = deps.get_db(request)
    if not await db.get_accounts(active_only=False):
        return RedirectResponse(url="/settings?msg=no_accounts", status_code=303)

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

    min_length = msg_length_val if msg_length_op == "gt" and msg_length_val is not None else None
    max_length = msg_length_val if msg_length_op == "lt" and msg_length_val is not None else None

    service = deps.search_service(request)
    channels = await db.get_channels()

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
                    is_fts=is_fts,
                    min_length=min_length,
                    max_length=max_length,
                )
            except Exception as exc:
                logger.exception("Search request failed: mode=%s query=%r", mode, q)
                result = SearchResult(
                    messages=[],
                    total=0,
                    query=q,
                    error=f"Ошибка поиска: {exc}",
                )

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
            "is_fts": is_fts,
            "msg_length_op": msg_length_op,
            "msg_length_val": msg_length_val,
            "page": page,
            "total_pages": total_pages,
            "ai_enabled": ai_enabled,
            "search_quota": search_quota,
        },
    )


@router.get("/", response_class=HTMLResponse)
@router.get("/search", response_class=HTMLResponse)
async def search_page(
    request: Request,
    q: str = Query(""),
    channel_id: str = Query(""),
    date_from: str = Query(""),
    date_to: str = Query(""),
    mode: str = Query("local"),
    is_fts: bool = Query(False),
    msg_length_op: str = Query(""),
    msg_length_val: int | None = Query(None),
    page: int = Query(1),
):
    return await _render_search_page(
        request=request,
        q=q,
        channel_id=channel_id,
        date_from=date_from,
        date_to=date_to,
        mode=mode,
        is_fts=is_fts,
        msg_length_op=msg_length_op,
        msg_length_val=msg_length_val,
        page=page,
    )
