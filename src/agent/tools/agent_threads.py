"""Agent tools for managing agent chat threads."""

from __future__ import annotations

from typing import Annotated

from claude_agent_sdk import tool
from mcp.types import ToolAnnotations

from src.agent.tools._registry import _text_response, require_confirmation


def register(db, client_pool, embedding_service, **kwargs):
    tools = []

    @tool("list_agent_threads", "List all agent chat threads", {})
    async def list_agent_threads(args):
        try:
            threads = await db.get_agent_threads()
            if not threads:
                return _text_response("Треды не найдены.")
            lines = [f"Треды ({len(threads)}):"]
            for t in threads:
                lines.append(f"- id={t['id']}: {t['title']} (создан {t.get('created_at', '?')})")
            return _text_response("\n".join(lines))
        except Exception as e:
            return _text_response(f"Ошибка получения тредов: {e}")

    tools.append(list_agent_threads)

    @tool("create_agent_thread", "Create a new agent chat thread", {"title": Annotated[str, "Название треда"]})
    async def create_agent_thread(args):
        try:
            title = args.get("title", "Новый тред")
            thread_id = await db.create_agent_thread(title)
            return _text_response(f"Тред создан: id={thread_id}, title='{title}'")
        except Exception as e:
            return _text_response(f"Ошибка создания треда: {e}")

    tools.append(create_agent_thread)

    @tool(
        "delete_agent_thread",
        "⚠️ DANGEROUS: Delete an agent chat thread and all its messages. "
        "Always ask user for confirmation first.",
        {
            "thread_id": Annotated[int, "ID треда агента"],
            "confirm": Annotated[bool, "Установите true для подтверждения действия"],
        },
        annotations=ToolAnnotations(destructiveHint=True),
    )
    async def delete_agent_thread(args):
        thread_id = args.get("thread_id")
        if thread_id is None:
            return _text_response("Ошибка: thread_id обязателен.")
        thread = await db.get_agent_thread(int(thread_id))
        name = thread["title"] if thread else f"id={thread_id}"
        gate = require_confirmation(f"удалит тред '{name}' и все его сообщения", args)
        if gate:
            return gate
        try:
            await db.delete_agent_thread(int(thread_id))
            return _text_response(f"Тред '{name}' удалён.")
        except Exception as e:
            return _text_response(f"Ошибка удаления треда: {e}")

    tools.append(delete_agent_thread)

    @tool(
        "rename_agent_thread",
        "Rename an agent chat thread",
        {"thread_id": Annotated[int, "ID треда агента"], "title": Annotated[str, "Название треда"]},
    )
    async def rename_agent_thread(args):
        thread_id = args.get("thread_id")
        title = args.get("title", "")
        if thread_id is None or not title:
            return _text_response("Ошибка: thread_id и title обязательны.")
        try:
            await db.rename_agent_thread(int(thread_id), title)
            return _text_response(f"Тред id={thread_id} переименован в '{title}'.")
        except Exception as e:
            return _text_response(f"Ошибка переименования треда: {e}")

    tools.append(rename_agent_thread)

    @tool(
        "get_thread_messages",
        "Get messages from an agent chat thread",
        {
            "thread_id": Annotated[int, "ID треда агента"],
            "limit": Annotated[int, "Максимальное количество результатов"],
        },
    )
    async def get_thread_messages(args):
        thread_id = args.get("thread_id")
        if thread_id is None:
            return _text_response("Ошибка: thread_id обязателен.")
        try:
            messages = await db.get_agent_messages(int(thread_id))
            limit = int(args.get("limit", 50))
            messages = messages[-limit:]
            if not messages:
                return _text_response(f"Нет сообщений в треде id={thread_id}.")
            lines = [f"Сообщения треда id={thread_id} ({len(messages)} шт.):"]
            for m in messages:
                role = m.get("role", "?")
                content = (m.get("content", "") or "")[:200]
                lines.append(f"[{role}]: {content}")
            return _text_response("\n".join(lines))
        except Exception as e:
            return _text_response(f"Ошибка получения сообщений: {e}")

    tools.append(get_thread_messages)

    return tools
