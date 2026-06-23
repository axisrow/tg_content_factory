"""Application orchestration for the search web domain."""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Awaitable
from dataclasses import dataclass, field

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
# Local-DB-backed modes (no Telegram round-trip): plain local search plus the
# semantic/hybrid retrievers that read the same SQLite store.
_DB_SEARCH_MODES = {"local", "semantic", "hybrid"}
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


async def _safe_search(
    coro: Awaitable[SearchResult],
    *,
    log_msg: str,
    log_args: tuple[object, ...] = (),
    error_text: str,
    error_query: str,
) -> SearchResult:
    """Await a search coroutine, converting any failure into an error
    ``SearchResult`` so the fragment renders a message instead of a 500.

    Centralises the ``try/except logger.exception → fallback SearchResult``
    boilerplate that the three search branches in :func:`render_search_results`
    each repeated verbatim (#1009). ``error_text`` is formatted with the caught
    exception (it must contain a single ``{exc}`` placeholder); ``log_msg`` and
    ``log_args`` are passed straight to ``logger.exception``.
    """
    try:
        return await coro
    except Exception as exc:
        logger.exception(log_msg, *log_args)
        return SearchResult(messages=[], total=0, query=error_query, error=error_text.format(exc=exc))


async def root_page(request: Request) -> SearchRedirect:
    if _agent_available_for_request(request):
        return SearchRedirect(url="/agent")
    return SearchRedirect(url="/search")


@dataclass
class _SearchContext:
    """Shared search setup — everything needed to render the form *and* run the
    search, minus the search itself. Built once and used by both the page
    skeleton (#946 lazyload) and the results fragment."""

    redirect: SearchRedirect | None = None
    db: object = None
    service: object = None
    channels: list = field(default_factory=list)
    channel_id_int: int | None = None
    channel_id_error: str | None = None
    mode: str = "local"
    fts_query: str = ""
    min_length: int | None = None
    max_length: int | None = None
    available_modes: set = field(default_factory=set)
    runtime_mode: str = "web"
    limit: int = 50
    offset: int = 0


async def _build_search_context(
    request: Request, *, q: str, channel_id: str, mode: str, page: int
) -> _SearchContext:
    limit = 50
    offset = (page - 1) * limit
    # Onboarding: redirect if no accounts configured
    auth = deps.get_auth(request)
    if not auth.is_configured:
        return _SearchContext(redirect=SearchRedirect(url="/settings"))
    db = deps.get_db(request)
    if not await db.get_account_summaries(active_only=False):
        return _SearchContext(redirect=SearchRedirect(url="/settings?msg=no_accounts"))

    channel_id_int, channel_id_error = parse_channel_id(channel_id)
    fts_query, length_lo, length_hi = extract_length(q)
    service = deps.search_service(request)
    # Loaded eagerly for the page's channel <select>; the results fragment reuses
    # it only for the browse-mode selected_channel lookup. It's a light bounded
    # query (channel list, not messages), so loading it on both calls is cheap.
    channels = await db.repos.channels.get_channels()
    runtime_mode = getattr(request.app.state, "runtime_mode", "web")

    # Single source of truth for which modes are offered: the template hides the
    # radios for modes not in this set, and the handler normalises an unavailable
    # mode (reachable via a hand-crafted URL/POST) back to "local" before searching (#833).
    semantic_available = deps.get_search_engine(request).semantic_available
    telegram_available = bool(deps.get_pool(request).clients)
    ai_enabled = deps.get_ai_search(request).enabled
    available_modes = {"local"}
    if semantic_available:
        available_modes |= {"semantic", "hybrid"}
    if telegram_available:
        available_modes |= _TELEGRAM_SEARCH_MODES
    if ai_enabled:
        available_modes.add("ai")

    # `available_modes` drives which radios the template shows. Normalisation uses a
    # slightly wider set: in web runtime the snapshot pool can show no clients even
    # though the worker is connected (timing/stale snapshot), and telegram modes are
    # proxied to the worker (line below) which surfaces its own worker-down/no-account
    # error — so don't silently fold them into a local search here (Codex review #876).
    runnable_modes = (
        available_modes | _TELEGRAM_SEARCH_MODES if runtime_mode == "web" else available_modes
    )
    if mode not in runnable_modes:
        mode = "local"

    # Length filters (e.g. "foo:50") only apply to local-DB modes; resolve them
    # against the *normalised* mode so a fallback to local keeps/drops them correctly.
    min_length, max_length = (length_lo, length_hi) if mode in _DB_SEARCH_MODES else (None, None)

    return _SearchContext(
        db=db,
        service=service,
        channels=channels,
        channel_id_int=channel_id_int,
        channel_id_error=channel_id_error,
        mode=mode,
        fts_query=fts_query,
        min_length=min_length,
        max_length=max_length,
        available_modes=available_modes,
        runtime_mode=runtime_mode,
        limit=limit,
        offset=offset,
    )


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
    """Lazyload skeleton (#946): render the form only; the heavy search runs in
    the ``/search/fragments/results`` fragment, triggered by HTMX on load."""
    ctx = await _build_search_context(request, q=q, channel_id=channel_id, mode=mode, page=page)
    if ctx.redirect is not None:
        return ctx.redirect

    try:
        search_quota = await ctx.service.check_quota()
    except Exception:
        logger.exception("Failed to load search quota")
        search_quota = None

    browse_mode = bool(not q and ctx.channel_id_int and ctx.mode in _DB_SEARCH_MODES)
    return SearchTemplate(
        "search.html",
        {
            "channels": ctx.channels,
            "q": q,
            "channel_id": ctx.channel_id_int,
            "date_from": date_from,
            "date_to": date_to,
            "mode": ctx.mode,
            "is_fts": is_fts,
            "include_filtered": include_filtered,
            "page": page,
            "available_modes": ctx.available_modes,
            "search_quota": search_quota,
            # Only fetch results when there's actually something to search/browse.
            "trigger_search": bool(q) or browse_mode,
        },
    )


async def render_search_results(
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
    """Heavy search, rendered as an HTMX fragment (#946)."""
    ctx = await _build_search_context(request, q=q, channel_id=channel_id, mode=mode, page=page)
    if ctx.redirect is not None:
        # Onboarding race (accounts removed between page load and fragment): the
        # full-page handler already redirects, so the fragment just shows nothing.
        return SearchTemplate("search_results.html", {"result": None})

    service = ctx.service
    channel_id_int = ctx.channel_id_int
    mode = ctx.mode
    limit, offset = ctx.limit, ctx.offset
    result = None

    # Browse mode: channel_id without query shows latest messages from that channel
    if not q and channel_id_int and mode in _DB_SEARCH_MODES:
        result = await _safe_search(
            service.search(
                mode="local",
                query="",
                limit=limit,
                channel_id=channel_id_int,
                date_from=None,
                date_to=None,
                offset=offset,
                is_fts=False,
                include_filtered=include_filtered,
            ),
            log_msg="Browse mode failed: channel_id=%s",
            log_args=(channel_id_int,),
            error_text="Ошибка загрузки сообщений: {exc}",
            error_query="",
        )
    elif q:
        if ctx.channel_id_error and mode in _DB_SEARCH_MODES | {"channel"}:
            result = SearchResult(messages=[], total=0, query=q, error=ctx.channel_id_error)
        elif mode in _TELEGRAM_SEARCH_MODES and ctx.runtime_mode == "web":
            # Web container has no live ClientPool — run it on the worker (#643).
            result = await _safe_search(
                _telegram_search_via_worker(
                    request, mode=mode, query=ctx.fts_query, limit=limit, channel_id=channel_id_int
                ),
                log_msg="Worker search proxy failed: mode=%s query_hash=%s",
                log_args=(mode, query_log_fields(q)["query_hash"]),
                error_text="Ошибка поиска: {exc}",
                error_query=q,
            )
        else:
            result = await _safe_search(
                service.search(
                    mode=mode,
                    query=ctx.fts_query,
                    limit=limit,
                    channel_id=channel_id_int,
                    date_from=date_from or None,
                    date_to=date_to or None,
                    offset=offset,
                    is_fts=is_fts,
                    min_length=ctx.min_length,
                    max_length=ctx.max_length,
                    include_filtered=include_filtered,
                ),
                log_msg="Search request failed: mode=%s query_hash=%s",
                log_args=(mode, query_log_fields(q)["query_hash"]),
                error_text="Ошибка поиска: {exc}",
                error_query=q,
            )

    # Page-based navigation without an exact total (#766): «Далее» is shown when
    # the LIMIT N+1 probe saw another page. Semantic/hybrid/telegram modes still
    # return an exact total without setting has_more — derive it from the total
    # there so their deeper pages stay reachable (review on #824).
    has_more = bool(result and (result.has_more or result.total > page * limit))

    browse_mode = bool(not q and channel_id_int and mode in _DB_SEARCH_MODES)
    selected_channel = None
    if browse_mode and channel_id_int:
        selected_channel = next(
            (ch for ch in ctx.channels if ch.channel_id == channel_id_int), None
        )

    return SearchTemplate(
        "search_results.html",
        {
            "result": result,
            "q": q,
            "channel_id": channel_id_int,
            "date_from": date_from,
            "date_to": date_to,
            "mode": mode,
            "is_fts": is_fts,
            "include_filtered": include_filtered,
            "page": page,
            "has_more": has_more,
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
