from __future__ import annotations

from typing import Annotated

from claude_agent_sdk import tool

from src.agent.tools._formatters import format_channel_identity, format_sender_identity
from src.agent.tools._registry import _text_response, require_pool


def register(db, client_pool, embedding_service, **kwargs):
    """Register search-related agent tools."""
    tools = []

    def _render_search_result(
        *,
        query: str,
        messages,
        total: int,
        empty_prefix: str,
        found_prefix: str,
    ) -> str:
        if not messages:
            return f"{empty_prefix}: {query!r}"
        lines = [
            f"{found_prefix} {total} сообщений для '{query}'. "
            f"Показаны первые {len(messages)}:"
        ]
        for message in messages:
            preview = (message.text or "")[:300]
            channel = format_channel_identity(message)
            sender = format_sender_identity(message)
            lines.append(f"- [{channel}, date={message.date}, sender={sender}]: {preview}")
        return "\n".join(lines)

    # ------------------------------------------------------------------
    # search_messages
    # ------------------------------------------------------------------

    @tool(
        "search_messages",
        "Full-text search in collected messages stored in the local DB. "
        "channel_id is the Telegram numeric ID (from list_channels, not pk). "
        "Supports date_from/date_to (YYYY-MM-DD), min_length, max_length filters.",
        {
            "query": Annotated[str, "Поисковый запрос"],
            "limit": Annotated[int, "Максимальное количество результатов"],
            "channel_id": Annotated[int, "Числовой Telegram ID канала для фильтрации"],
            "date_from": Annotated[str, "Начало периода в формате YYYY-MM-DD"],
            "date_to": Annotated[str, "Конец периода в формате YYYY-MM-DD"],
            "min_length": Annotated[int, "Минимальная длина текста сообщения в символах"],
            "max_length": Annotated[int, "Максимальная длина текста сообщения в символах"],
        },
    )
    async def search_messages(args):
        query = args.get("query", "")
        limit = int(args.get("limit", 20))
        channel_id = args.get("channel_id")
        date_from = args.get("date_from")
        date_to = args.get("date_to")
        min_length = args.get("min_length")
        max_length = args.get("max_length")
        try:
            messages, total = await db.search_messages(
                query=query,
                channel_id=int(channel_id) if channel_id else None,
                date_from=date_from,
                date_to=date_to,
                limit=limit,
                min_length=int(min_length) if min_length is not None else None,
                max_length=int(max_length) if max_length is not None else None,
            )
            text = _render_search_result(
                query=query,
                messages=messages,
                total=total,
                empty_prefix="Ничего не найдено по запросу",
                found_prefix="Найдено",
            )
        except Exception as e:
            text = f"Ошибка поиска сообщений: {e}"
        return _text_response(text)

    tools.append(search_messages)

    # ------------------------------------------------------------------
    # semantic_search
    # ------------------------------------------------------------------

    @tool(
        "semantic_search",
        "Search collected messages by semantic (embedding) similarity. "
        "Requires messages to be indexed first via index_messages, and an OpenAI/embedding API key configured.",
        {"query": Annotated[str, "Поисковый запрос"], "limit": Annotated[int, "Максимальное количество результатов"]},
    )
    async def semantic_search(args):
        query = args.get("query", "")
        limit = int(args.get("limit", 10))
        try:
            query_embedding = await embedding_service.embed_query(query)
            messages, total = await db.search_semantic_messages(query_embedding, limit=limit)
            text = _render_search_result(
                query=query,
                messages=messages,
                total=total,
                empty_prefix="Семантически похожие сообщения не найдены по запросу",
                found_prefix="Семантически найдено",
            )
        except Exception as e:
            text = f"Ошибка семантического поиска: {e}"
        return _text_response(text)

    tools.append(semantic_search)

    # ------------------------------------------------------------------
    # index_messages
    # ------------------------------------------------------------------

    @tool(
        "index_messages",
        "Create semantic embeddings for all not-yet-indexed messages in the DB. "
        "Required before semantic_search or search_hybrid work. Needs an embedding API key configured.",
        {},
    )
    async def index_messages(args):
        try:
            indexed = await embedding_service.index_pending_messages()
            if _engine_cache:
                _engine_cache[0].invalidate_numpy_index()
            text = f"Индексация завершена. Проиндексировано сообщений: {indexed}"
        except Exception as e:
            text = f"Ошибка индексации: {e}"
        return _text_response(text)

    tools.append(index_messages)

    # ------------------------------------------------------------------
    # Lazy SearchEngine — created once on first Telegram/hybrid search
    # ------------------------------------------------------------------

    _engine_cache: list = []

    def _get_engine():
        if not _engine_cache:
            from src.search.engine import SearchEngine

            _engine_cache.append(SearchEngine(db, pool=client_pool, config=kwargs.get("config")))
        return _engine_cache[0]

    def _render_search_response(result) -> str:
        """Render SearchResult into text, handling .error field."""
        if result.error:
            return f"Ошибка: {result.error}"
        return _render_search_result(
            query=result.query,
            messages=result.messages,
            total=result.total,
            empty_prefix="Ничего не найдено по запросу",
            found_prefix="Найдено",
        )

    # ------------------------------------------------------------------
    # search_telegram  (Telegram Premium global search)
    # ------------------------------------------------------------------

    @tool(
        "search_telegram",
        "Search messages across all public Telegram channels via Telegram API. Requires a connected "
        "Premium account. Use for discovering content beyond collected channels.",
        {"query": Annotated[str, "Поисковый запрос"], "limit": Annotated[int, "Максимальное количество результатов"]},
    )
    async def search_telegram(args):
        pool_gate = require_pool(client_pool, "Telegram-поиск")
        if pool_gate:
            return pool_gate
        query = args.get("query", "")
        limit = int(args.get("limit", 50))
        try:
            result = await _get_engine().search_telegram(query, limit=limit)
            text = _render_search_response(result)
        except Exception as e:
            text = f"Ошибка Telegram-поиска: {e}"
        return _text_response(text)

    tools.append(search_telegram)

    # ------------------------------------------------------------------
    # search_my_chats  (search across user's own dialogs)
    # ------------------------------------------------------------------

    @tool(
        "search_my_chats",
        "Search messages in your own Telegram dialogs (private chats, groups, saved messages) "
        "via Telegram API. Uses the primary connected account.",
        {"query": Annotated[str, "Поисковый запрос"], "limit": Annotated[int, "Максимальное количество результатов"]},
    )
    async def search_my_chats(args):
        pool_gate = require_pool(client_pool, "Поиск по личным чатам")
        if pool_gate:
            return pool_gate
        query = args.get("query", "")
        limit = int(args.get("limit", 50))
        try:
            result = await _get_engine().search_my_chats(query, limit=limit)
            text = _render_search_response(result)
        except Exception as e:
            text = f"Ошибка поиска по чатам: {e}"
        return _text_response(text)

    tools.append(search_my_chats)

    # ------------------------------------------------------------------
    # search_in_channel  (search within a specific channel via Telegram)
    # ------------------------------------------------------------------

    @tool(
        "search_in_channel",
        "Search messages within a specific channel via Telegram API. "
        "channel_id = Telegram numeric ID (from list_channels or search_dialogs). "
        "Searches live Telegram, not local DB.",
        {
            "channel_id": Annotated[int, "Числовой Telegram ID канала для фильтрации"],
            "query": Annotated[str, "Поисковый запрос"],
            "limit": Annotated[int, "Максимальное количество результатов"],
        },
    )
    async def search_in_channel(args):
        pool_gate = require_pool(client_pool, "Поиск в канале")
        if pool_gate:
            return pool_gate
        channel_id = args.get("channel_id")
        if not channel_id:
            return _text_response("Ошибка: channel_id обязателен.")
        query = args.get("query", "")
        limit = int(args.get("limit", 50))
        try:
            result = await _get_engine().search_in_channel(
                int(channel_id), query, limit=limit,
            )
            text = _render_search_response(result)
        except Exception as e:
            text = f"Ошибка поиска в канале: {e}"
        return _text_response(text)

    tools.append(search_in_channel)

    # ------------------------------------------------------------------
    # search_hybrid  (FTS + semantic combined, local DB)
    # ------------------------------------------------------------------

    @tool(
        "search_hybrid",
        "Hybrid search combining FTS and semantic similarity on collected local messages. "
        "Semantic part requires prior index_messages call. Supports same filters as search_messages.",
        {
            "query": Annotated[str, "Поисковый запрос"],
            "limit": Annotated[int, "Максимальное количество результатов"],
            "channel_id": Annotated[int, "Числовой Telegram ID канала для фильтрации"],
            "date_from": Annotated[str, "Начало периода в формате YYYY-MM-DD"],
            "date_to": Annotated[str, "Конец периода в формате YYYY-MM-DD"],
            "min_length": Annotated[int, "Минимальная длина текста сообщения в символах"],
            "max_length": Annotated[int, "Максимальная длина текста сообщения в символах"],
        },
    )
    async def search_hybrid(args):
        query = args.get("query", "")
        limit = int(args.get("limit", 20))
        channel_id = args.get("channel_id")
        date_from = args.get("date_from")
        date_to = args.get("date_to")
        min_length = args.get("min_length")
        max_length = args.get("max_length")
        try:
            result = await _get_engine().search_hybrid(
                query=query,
                channel_id=int(channel_id) if channel_id else None,
                date_from=date_from,
                date_to=date_to,
                limit=limit,
                min_length=int(min_length) if min_length is not None else None,
                max_length=int(max_length) if max_length is not None else None,
            )
            text = _render_search_response(result)
        except Exception as e:
            text = f"Ошибка гибридного поиска: {e}"
        return _text_response(text)

    tools.append(search_hybrid)

    return tools
