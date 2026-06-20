from __future__ import annotations

from typing import Annotated

from claude_agent_sdk import tool

from src.agent.tools._categories import ToolCategory, ToolMeta
from src.agent.tools._registry import _text_response

# Permission metadata (#245). Export only reads collected data and writes an
# export tree to disk under data/exports/ — classified READ (no DB mutation).
TOOL_GROUPS: list[tuple[str, dict[str, ToolMeta]]] = [
    ("Экспорт", {
        "export_messages": ToolMeta(ToolCategory.READ),
    }),
]


def register(db, client_pool, embedding_service, **kwargs):
    """Register the Telegram-Desktop export agent tool."""
    tools = []

    @tool(
        "export_messages",
        "Export a channel's collected messages into a Telegram-Desktop-compatible tree "
        "(result.json and/or messages*.html) under data/exports/. channel_id is the numeric "
        "Telegram ID (from list_channels). format: json (default), html, or both. Media is not "
        "downloaded in this offline path — files are marked 'not included'.",
        {
            "channel_id": Annotated[int, "Числовой Telegram ID канала"],
            "format": Annotated[str, "Формат: json, html или both (по умолчанию json)"],
            "date_from": Annotated[str, "Начало периода YYYY-MM-DD (опционально)"],
            "date_to": Annotated[str, "Конец периода YYYY-MM-DD (опционально)"],
            "limit": Annotated[int, "Максимум сообщений (по умолчанию 5000)"],
        },
    )
    async def export_messages(args):
        from src.services.export_service import run_offline_export

        channel_id = args.get("channel_id")
        if not channel_id:
            return _text_response("Ошибка: channel_id обязателен.")
        fmt = args.get("format", "json")
        fmt = fmt if fmt in ("json", "html", "both") else "json"
        try:
            summary = await run_offline_export(
                db,
                int(channel_id),
                fmt=fmt,
                date_from=args.get("date_from") or None,
                date_to=args.get("date_to") or None,
                limit=int(args.get("limit", 5000)),
            )
        except Exception as e:
            return _text_response(f"Ошибка экспорта: {e}")
        if summary is None:
            return _text_response(f"Сообщения для канала {channel_id} не найдены.")
        return _text_response(
            f"Экспортировано {summary.message_count} сообщений в {summary.out_dir}. "
            f"Файлы: {', '.join(summary.files)}. Медиа пропущено: {summary.media_skipped}."
        )

    tools.append(export_messages)
    return tools
