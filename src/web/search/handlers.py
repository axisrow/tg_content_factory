"""Application orchestration for the search web domain."""

from __future__ import annotations

import asyncio
import logging
import time

from fastapi import Request
from pydantic import ValidationError

from src.models import SearchResult, TelegramCommandStatus
from src.utils.safe_logging import elapsed_ms, query_log_fields
from src.web import deps
from src.web.search.forms import extract_length, parse_channel_id
from src.web.search.responses import SearchJson, SearchRedirect, SearchTemplate
from src.web.template_globals import _agent_available_for_request

logger = logging.getLogger(__name__)

# Telegram-backed search modes need a live ClientPool. The web container has
# none (runtime_mode="web"), so these are proxied to the worker process (#643).
_TELEGRAM_SEARCH_MODES = {"telegram", "my_chats", "channel"}
_WORKER_SEARCH_TIMEOUT_SEC = 130.0
_WORKER_SEARCH_POLL_SEC = 0.4


def _status_value(status: TelegramCommandStatus | str | None) -> str:
    if status is None:
        return "unknown"
    return getattr(status, "value", str(status))


async def _telegram_search_via_worker(
    request: Request,
    *,
    mode: str,
    query: str,
    limit: int,
    channel_id: int | None,
) -> SearchResult:
    """Proxy a live Telegram search to the worker and await its result (#643).

    The web container cannot open Telegram connections, so it enqueues a
    ``search.telegram`` command and polls ``telegram_commands.result_payload``
    until the worker (embedded or standalone) finishes it.
    """
    from src.web.routes.scheduler import _is_worker_alive

    db = deps.get_db(request)
    if not await _is_worker_alive(db):
        return SearchResult(
            messages=[],
            total=0,
            query=query,
            error="Telegram-worker не запущен — premium-поиск недоступен. Запустите worker.",
        )
    cmd_service = deps.telegram_command_service(request)
    payload: dict = {"mode": mode, "query": query, "limit": limit}
    if channel_id is not None:
        payload["channel_id"] = channel_id
    command_id = await cmd_service.enqueue(
        "search.telegram", payload=payload, requested_by="web:search"
    )
    fields = query_log_fields(query)
    started_at = time.monotonic()
    logger.info(
        "telegram_search_worker enqueue command_id=%s mode=%s limit=%d channel_id=%s "
        "query_hash=%s query_len=%d",
        command_id,
        mode,
        limit,
        channel_id,
        fields["query_hash"],
        fields["query_len"],
    )
    deadline = time.monotonic() + _WORKER_SEARCH_TIMEOUT_SEC
    last_status = "unknown"
    while time.monotonic() < deadline:
        command = await cmd_service.get(command_id)
        if command is None:
            logger.warning(
                "telegram_search_worker missing command_id=%s elapsed_ms=%d mode=%s query_hash=%s",
                command_id,
                elapsed_ms(started_at),
                mode,
                fields["query_hash"],
            )
            break
        last_status = _status_value(command.status)
        if command.status == TelegramCommandStatus.SUCCEEDED:
            try:
                result = SearchResult.model_validate(command.result_payload or {})
                logger.info(
                    "telegram_search_worker success command_id=%s elapsed_ms=%d mode=%s "
                    "total=%d result_error=%s query_hash=%s",
                    command_id,
                    elapsed_ms(started_at),
                    mode,
                    result.total,
                    bool(result.error),
                    fields["query_hash"],
                )
                return result
            except ValidationError:
                logger.warning(
                    "Malformed worker search result for command %s mode=%s query_hash=%s",
                    command_id,
                    mode,
                    fields["query_hash"],
                )
                return SearchResult(messages=[], total=0, query=query, error="Некорректный ответ worker.")
        if command.status == TelegramCommandStatus.FAILED:
            logger.warning(
                "telegram_search_worker failed command_id=%s elapsed_ms=%d mode=%s error=%s "
                "query_hash=%s",
                command_id,
                elapsed_ms(started_at),
                mode,
                command.error,
                fields["query_hash"],
            )
            return SearchResult(
                messages=[], total=0, query=query,
                error=command.error or "Ошибка поиска в worker.",
            )
        if command.status == TelegramCommandStatus.CANCELLED:
            logger.warning(
                "telegram_search_worker cancelled command_id=%s elapsed_ms=%d mode=%s query_hash=%s",
                command_id,
                elapsed_ms(started_at),
                mode,
                fields["query_hash"],
            )
            return SearchResult(messages=[], total=0, query=query, error="Поиск отменён.")
        await asyncio.sleep(_WORKER_SEARCH_POLL_SEC)
    logger.warning(
        "telegram_search_worker timeout command_id=%s elapsed_ms=%d timeout_sec=%.0f "
        "last_status=%s mode=%s limit=%d query_hash=%s query_len=%d",
        command_id,
        elapsed_ms(started_at),
        _WORKER_SEARCH_TIMEOUT_SEC,
        last_status,
        mode,
        limit,
        fields["query_hash"],
        fields["query_len"],
    )
    return SearchResult(
        messages=[],
        total=0,
        query=query,
        error=(
            f"Worker не ответил за {_WORKER_SEARCH_TIMEOUT_SEC:.0f}с "
            f"(command_id={command_id}, status={last_status}). "
            "Задача могла ещё выполняться в Telegram-worker; проверьте логи."
        ),
    )


async def root_page(request: Request) -> SearchRedirect:
    if _agent_available_for_request(request):
        return SearchRedirect(url="/agent")
    return SearchRedirect(url="/search")


async def render_search_page(
    request: Request,
    q: str = "",
    channel_id: str = "",
    date_from: str = "",
    date_to: str = "",
    mode: str = "local",
    is_fts: bool = False,
    include_filtered: bool = False,
    page: int = 1,
) -> SearchTemplate | SearchRedirect:
    # Onboarding: redirect if no accounts configured
    auth = deps.get_auth(request)
    if not auth.is_configured:
        return SearchRedirect(url="/settings")
    db = deps.get_db(request)
    if not await db.get_account_summaries(active_only=False):
        return SearchRedirect(url="/settings?msg=no_accounts")

    result = None
    limit = 50
    offset = (page - 1) * limit
    channel_id_int, channel_id_error = parse_channel_id(channel_id)

    fts_query, min_length, max_length = extract_length(q)
    if mode not in {"local", "semantic", "hybrid"}:
        min_length, max_length = None, None

    service = deps.search_service(request)
    channels = await db.repos.channels.get_channels()
    runtime_mode = getattr(request.app.state, "runtime_mode", "web")

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
                include_filtered=include_filtered,
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
        elif mode in _TELEGRAM_SEARCH_MODES and runtime_mode == "web":
            # Web container has no live ClientPool — run it on the worker (#643).
            try:
                result = await _telegram_search_via_worker(
                    request, mode=mode, query=fts_query, limit=limit, channel_id=channel_id_int
                )
            except Exception as exc:
                logger.exception(
                    "Worker search proxy failed: mode=%s query_hash=%s",
                    mode,
                    query_log_fields(q)["query_hash"],
                )
                result = SearchResult(messages=[], total=0, query=q, error=f"Ошибка поиска: {exc}")
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
                    include_filtered=include_filtered,
                )
            except Exception as exc:
                logger.exception(
                    "Search request failed: mode=%s query_hash=%s",
                    mode,
                    query_log_fields(q)["query_hash"],
                )
                result = SearchResult(
                    messages=[],
                    total=0,
                    query=q,
                    error=f"Ошибка поиска: {exc}",
                )

    semantic_available = deps.get_search_engine(request).semantic_available
    telegram_available = bool(deps.get_pool(request).clients)
    ai_enabled = deps.get_ai_search(request).enabled
    try:
        search_quota = await service.check_quota()
    except Exception:
        logger.exception("Failed to load search quota")
        search_quota = None

    # Page-based navigation without an exact total (#766): «Далее» is shown when
    # the LIMIT N+1 probe saw another page. Semantic/hybrid/telegram modes still
    # return an exact total without setting has_more — derive it from the total
    # there so their deeper pages stay reachable (review on #824).
    has_more = bool(result and (result.has_more or result.total > page * limit))

    # Browse mode: viewing channel messages without search query
    browse_mode = bool(not q and channel_id_int and mode in {"local", "semantic", "hybrid"})
    selected_channel = None
    if browse_mode and channel_id_int:
        selected_channel = next((ch for ch in channels if ch.channel_id == channel_id_int), None)

    return SearchTemplate(
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
            "include_filtered": include_filtered,
            "page": page,
            "has_more": has_more,
            "semantic_available": semantic_available,
            "telegram_available": telegram_available,
            "ai_enabled": ai_enabled,
            "search_quota": search_quota,
            "browse_mode": browse_mode,
            "selected_channel": selected_channel,
        },
    )


async def _json_body(request: Request) -> dict:
    """Parse a JSON request body, tolerating a bodyless POST (returns ``{}``)."""
    if request.headers.get("content-type", "").startswith("application/json"):
        return await request.json()
    return {}


async def purge_premium_search_cache(request: Request) -> SearchJson:
    """Delete messages cached by a previous Premium global search for a query.

    JSON endpoint (no DOM swap), mirrors the CLI ``search <query> --purge-cache``.
    Only rows tagged with ``premium_search_query`` are removed, so collected user
    data is never touched.
    """
    db = deps.get_db(request)
    body = await _json_body(request)
    query = (body.get("query") or "").strip()
    if not query:
        return SearchJson({"ok": False, "error": "query is required"}, status_code=400)
    deleted = await db.repos.messages.delete_premium_search_results(query)
    return SearchJson({"ok": True, "deleted": deleted, "query": query})


async def translate_message(request: Request, message_db_id: int) -> SearchJson:
    """Translate a single message on demand. Returns JSON."""
    db = deps.get_db(request)
    translation_service = getattr(request.app.state, "container", None)
    if translation_service:
        translation_service = translation_service.translation_service

    body = await _json_body(request)
    target_lang = body.get("target_lang", "en")

    # Get the message
    msg = await db.repos.messages.get_message_by_id(message_db_id)
    if not msg:
        return SearchJson({"ok": False, "error": "Message not found"}, status_code=404)

    # Check if translation already cached
    cached = msg.translation_en if target_lang == "en" else msg.translation_custom
    if cached:
        return SearchJson({"ok": True, "translation": cached, "detected_lang": msg.detected_lang, "cached": True})

    if not msg.text:
        return SearchJson({"ok": False, "error": "Message has no text"}, status_code=400)

    # Detect language if missing
    detected = msg.detected_lang
    if not detected:
        from src.services.translation_service import TranslationService

        detected = TranslationService.detect_language(msg.text)
        if detected:
            await db.repos.messages.update_detected_lang(message_db_id, detected)

    if not detected:
        return SearchJson({"ok": False, "error": "Cannot detect language"}, status_code=400)

    if detected == target_lang:
        return SearchJson({"ok": True, "translation": None, "detected_lang": detected, "same_lang": True})

    if not translation_service:
        return SearchJson({"ok": False, "error": "Translation service not configured"}, status_code=503)

    translated = await translation_service.translate_message(
        msg.text, detected, target_lang,
        provider_name=await db.repos.settings.get_setting("translation_provider"),
        model=await db.repos.settings.get_setting("translation_model"),
    )
    if translated:
        target = "en" if target_lang == "en" else "custom"
        await db.repos.messages.update_translation(message_db_id, target, translated)

    return SearchJson({
        "ok": bool(translated),
        "translation": translated,
        "detected_lang": detected,
        "cached": False,
    })
