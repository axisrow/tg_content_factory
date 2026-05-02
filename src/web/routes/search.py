import logging
import re

from fastapi import APIRouter, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse

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
    if not await db.get_account_summaries(active_only=False):
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

    fts_query, min_length, max_length = _extract_length(q)
    if mode not in {"local", "semantic", "hybrid"}:
        min_length, max_length = None, None

    service = deps.search_service(request)
    channels = await db.get_channels()

    # Browse mode: channel_id without query shows latest messages from that channel
    if not q and channel_id_int and mode in {"local", "semantic", "hybrid"}:
        try:
            result = await service.search(
                mode="local",
                query="",
                limit=limit,
                channel_id=channel_id_int,
                date_from=None,
                date_to=None,
                offset=offset,
                is_fts=False,
            )
        except Exception as exc:
            logger.exception("Browse mode failed: channel_id=%s", channel_id_int)
            result = SearchResult(
                messages=[],
                total=0,
                query="",
                error=f"Ошибка загрузки сообщений: {exc}",
            )
    elif q:
        if channel_id_error and mode in {"local", "semantic", "hybrid", "channel"}:
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

    # Browse mode: viewing channel messages without search query
    browse_mode = bool(not q and channel_id_int and mode in {"local", "semantic", "hybrid"})
    selected_channel = None
    if browse_mode and channel_id_int:
        selected_channel = next((ch for ch in channels if ch.channel_id == channel_id_int), None)

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
            "browse_mode": browse_mode,
            "selected_channel": selected_channel,
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


@router.post("/search/translate/{message_db_id}")
async def translate_message_endpoint(message_db_id: int, request: Request):
    """Translate a single message on demand. Returns JSON."""
    db = deps.get_db(request)
    translation_service = getattr(request.app.state, "container", None)
    if translation_service:
        translation_service = translation_service.translation_service

    body = await request.json() if request.headers.get("content-type", "").startswith("application/json") else {}
    target_lang = body.get("target_lang", "en")

    # Get the message
    msg = await db.repos.messages.get_message_by_id(message_db_id)
    if not msg:
        return JSONResponse({"ok": False, "error": "Message not found"}, status_code=404)

    # Check if translation already cached
    cached = msg.translation_en if target_lang == "en" else msg.translation_custom
    if cached:
        return JSONResponse({"ok": True, "translation": cached, "detected_lang": msg.detected_lang, "cached": True})

    if not msg.text:
        return JSONResponse({"ok": False, "error": "Message has no text"}, status_code=400)

    # Detect language if missing
    detected = msg.detected_lang
    if not detected:
        from src.services.translation_service import TranslationService

        detected = TranslationService.detect_language(msg.text)
        if detected:
            await db.repos.messages.update_detected_lang(message_db_id, detected)

    if not detected:
        return JSONResponse({"ok": False, "error": "Cannot detect language"}, status_code=400)

    if detected == target_lang:
        return JSONResponse({"ok": True, "translation": None, "detected_lang": detected, "same_lang": True})

    if not translation_service:
        return JSONResponse({"ok": False, "error": "Translation service not configured"}, status_code=503)

    translated = await translation_service.translate_message(
        msg.text, detected, target_lang,
        provider_name=await db.get_setting("translation_provider"),
        model=await db.get_setting("translation_model"),
    )
    if translated:
        target = "en" if target_lang == "en" else "custom"
        await db.repos.messages.update_translation(message_db_id, target, translated)

    return JSONResponse({
        "ok": bool(translated),
        "translation": translated,
        "detected_lang": detected,
        "cached": False,
    })
