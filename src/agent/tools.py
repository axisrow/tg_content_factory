from __future__ import annotations

from claude_agent_sdk import create_sdk_mcp_server, tool

from src.database import Database
from src.services.embedding_service import EmbeddingService


def make_mcp_server(db: Database):
    """Create an in-process MCP server with DB-bound tools."""

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

    @tool(
        "search_messages",
        "Search messages in Telegram channels by query text",
        {"query": str, "limit": int},
    )
    async def search_messages(args):
        query = args.get("query", "")
        limit = int(args.get("limit", 20))
        try:
            messages, total = await db.search_messages(query=query, limit=limit)
            text = _render_search_result(
                query=query,
                messages=messages,
                total=total,
                empty_prefix="Ничего не найдено по запросу",
                found_prefix="Найдено",
            )
        except Exception as e:
            text = f"Ошибка поиска сообщений: {e}"
        return {"content": [{"type": "text", "text": text}]}

    @tool(
        "semantic_search",
        "Search messages in the local database by semantic similarity",
        {"query": str, "limit": int},
    )
    async def semantic_search(args):
        query = args.get("query", "")
        limit = int(args.get("limit", 10))
        embedding_service = EmbeddingService(db)
        try:
            await embedding_service.index_pending_messages()
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
        return {"content": [{"type": "text", "text": text}]}

    @tool("get_channels", "List all available Telegram channels in the database", {})
    async def get_channels(args):
        try:
            channels = await db.get_channels()
            if not channels:
                text = "Каналы не найдены."
            else:
                lines = [f"Доступные каналы ({len(channels)}):"]
                for ch in channels:
                    status = "активен" if ch.is_active else "неактивен"
                    filtered = " [отфильтрован]" if ch.is_filtered else ""
                    lines.append(
                        f"- {ch.title} (@{ch.username}, id={ch.channel_id}, {status}{filtered})"
                    )
                text = "\n".join(lines)
        except Exception as e:
            text = f"Ошибка получения каналов: {e}"
        return {"content": [{"type": "text", "text": text}]}

    @tool("generate_draft", "Generate a draft from a query using RAG (returns draft text and citations)", {"query": str, "pipeline_id": int, "limit": int})
    async def generate_draft(args):
        query = args.get("query", "")
        pipeline_id = args.get("pipeline_id")
        limit = int(args.get("limit", 8))
        try:
            from src.search.engine import SearchEngine
            engine = SearchEngine(db)
            # provider stub: real provider wiring should be done via AgentProviderService
            async def _provider_stub(**kwargs):
                prompt = kwargs.get("prompt") or ""
                return "DRAFT: " + (prompt[:400])

            from src.services.generation_service import GenerationService

            gen = GenerationService(engine, provider_callable=_provider_stub)
            result = await gen.generate(query=query, limit=limit, prompt_template=None)
            text = result.get("generated_text", "")
            citations = result.get("citations", [])
            content = f"Generated draft:\n\n{text}\n\nCitations:\n" + "\n".join(
                f"- {c['channel_title']} id={c['message_id']} date={c['date']}" for c in citations
            )
        except Exception as e:
            content = f"Ошибка генерации: {e}"
        return {"content": [{"type": "text", "text": content}]}

    return create_sdk_mcp_server(
        name="telegram_db",
        tools=[search_messages, semantic_search, get_channels, generate_draft],
    )
