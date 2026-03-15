import logging
import re

from fastapi import APIRouter, Query, Request
from fastapi.responses import HTMLResponse, RedirectResponse

from src.models import SearchResult
from src.web import deps
from src.web.template_globals import _agent_available_for_request

router = APIRouter()
logger = logging.getLogger(__name__)

_LEN_RE = re.compile(r"\blen\s*(<|>)\s*(\d+)[,;]?")


def _extract_length(q: str) -> tuple[str, int | None, int | None]:
    """Extract ``len<N`` / ``len>N`` tokens from *q*, return cleaned query."""
    min_length: int | None = None
    max_length: int | None = None
    for m in _LEN_RE.finditer(q):
        op, val = m.group(1), int(m.group(2))
        if op == "<":
            max_length = val
        else:
            min_length = val
    cleaned = re.sub(r"\s+", " ", _LEN_RE.sub("", q)).strip()
    return cleaned, min_length, max_length


@router.get("/", response_class=HTMLResponse)
async def root_page(request: Request):
    if _agent_available_for_request(request):
        return RedirectResponse(url="/agent", status_code=303)
    return RedirectResponse(url="/search", status_code=303)


async def _render_search_page(
    request: Request,
    q: str = "",
    channel_id: str = "",
    date_from: str = "",
    date_to: str = "",
    mode: str = "local",
    is_fts: bool = False,
    page: int = 1,
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

    if mode == "local":
        fts_query, min_length, max_length = _extract_length(q)
    else:
        fts_query, min_length, max_length = q, None, None

    service = deps.search_service(request)
    channels = await db.get_channels()

    if q:
        if channel_id_error and mode in {"local", "channel"}:
            result = SearchResult(messages=[], total=0, query=q, error=channel_id_error)
        else:
            try:
                result = await service.search(
                    mode=mode,
                    query=fts_query,
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
            "page": page,
            "total_pages": total_pages,
            "ai_enabled": ai_enabled,
            "search_quota": search_quota,
        },
    )

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
    return await _render_search_page(
        request=request,
        q=q,
        channel_id=channel_id,
        date_from=date_from,
        date_to=date_to,
        mode=mode,
        is_fts=is_fts,
        page=page,
    )
