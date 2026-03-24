from __future__ import annotations

from claude_agent_sdk import tool

from src.agent.tools._registry import _text_response


def register(db, client_pool, embedding_service):
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
            lines.append(f"- [channel_id={message.channel_id}, date={message.date}]: {preview}")
        return "\n".join(lines)

    # ------------------------------------------------------------------
    # search_messages
    # ------------------------------------------------------------------

    @tool(
        "search_messages",
        "Search messages in Telegram channels by query text. "
        "Supports optional filters: channel_id, date_from/date_to (YYYY-MM-DD), min_length, max_length, mode.",
        {
            "query": str,
            "limit": int,
            "channel_id": int,
            "date_from": str,
            "date_to": str,
            "min_length": int,
            "max_length": int,
            "mode": str,
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
        "Search messages in the local database by semantic similarity.",
        {"query": str, "limit": int},
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
        "Index pending messages for semantic search. No confirmation required.",
        {},
    )
    async def index_messages(args):
        try:
            indexed = await embedding_service.index_pending_messages()
            text = f"Индексация завершена. Проиндексировано сообщений: {indexed}"
        except Exception as e:
            text = f"Ошибка индексации: {e}"
        return _text_response(text)

    tools.append(index_messages)

    return tools
