from __future__ import annotations

from claude_agent_sdk import create_sdk_mcp_server, tool

from src.database import Database


def make_mcp_server(db: Database):
    """Create an in-process MCP server with DB-bound tools."""

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
            if not messages:
                text = f"Ничего не найдено по запросу: {query!r}"
            else:
                lines = [
                    f"Найдено {total} сообщений для '{query}'. Показаны первые {len(messages)}:"
                ]
                for m in messages:
                    preview = (m.text or "")[:300]
                    lines.append(f"- [channel_id={m.channel_id}, date={m.date}]: {preview}")
                text = "\n".join(lines)
        except Exception as e:
            text = f"Ошибка поиска сообщений: {e}"
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

    return create_sdk_mcp_server(
        name="telegram_db",
        tools=[search_messages, get_channels],
    )
