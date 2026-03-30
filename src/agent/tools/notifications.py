"""Agent tools for notification bot management."""

from __future__ import annotations

from typing import Annotated

from claude_agent_sdk import tool
from mcp.types import ToolAnnotations

from src.agent.tools._registry import _text_response, require_confirmation, require_pool


def register(db, client_pool, embedding_service, **kwargs):
    tools = []

    @tool("get_notification_status", "Get notification bot status and configuration", {})
    async def get_notification_status(args):
        try:
            from src.services.notification_service import NotificationService
            from src.services.notification_target_service import NotificationTargetService

            svc = NotificationService(db, NotificationTargetService(db, client_pool))
            bot = await svc.get_status()
            if bot is None:
                return _text_response("Бот уведомлений не настроен.")
            return _text_response(
                f"Бот уведомлений:\n"
                f"- Username: @{bot.bot_username}\n"
                f"- Chat ID: {bot.chat_id}\n"
                f"- Создан: {bot.created_at}"
            )
        except Exception as e:
            return _text_response(f"Ошибка получения статуса бота: {e}")

    tools.append(get_notification_status)

    @tool(
        "setup_notification_bot",
        "⚠️ Set up a notification bot via BotFather. Requires Telegram client. "
        "Ask user for confirmation first.",
        {"confirm": Annotated[bool, "Установите true для подтверждения действия"]},
    )
    async def setup_notification_bot(args):
        pool_gate = require_pool(client_pool, "Настройка бота уведомлений")
        if pool_gate:
            return pool_gate
        gate = require_confirmation("создаст нового бота уведомлений через BotFather", args)
        if gate:
            return gate
        try:
            from src.services.notification_service import NotificationService
            from src.services.notification_target_service import NotificationTargetService

            svc = NotificationService(db, NotificationTargetService(db, client_pool))
            bot = await svc.setup_bot()
            return _text_response(
                f"Бот уведомлений создан!\n"
                f"- Username: @{bot.bot_username}\n"
                f"- Chat ID: {bot.chat_id}"
            )
        except Exception as e:
            return _text_response(f"Ошибка настройки бота: {e}")

    tools.append(setup_notification_bot)

    @tool(
        "delete_notification_bot",
        "⚠️ DANGEROUS: Delete the notification bot. Always ask user for confirmation first.",
        {"confirm": Annotated[bool, "Установите true для подтверждения действия"]},
        annotations=ToolAnnotations(destructiveHint=True),
    )
    async def delete_notification_bot(args):
        pool_gate = require_pool(client_pool, "Удаление бота уведомлений")
        if pool_gate:
            return pool_gate
        gate = require_confirmation("удалит бота уведомлений", args)
        if gate:
            return gate
        try:
            from src.services.notification_service import NotificationService
            from src.services.notification_target_service import NotificationTargetService

            svc = NotificationService(db, NotificationTargetService(db, client_pool))
            await svc.teardown_bot()
            return _text_response("Бот уведомлений удалён.")
        except Exception as e:
            return _text_response(f"Ошибка удаления бота: {e}")

    tools.append(delete_notification_bot)

    @tool("test_notification", "Send a test notification message via the bot", {})
    async def test_notification(args):
        pool_gate = require_pool(client_pool, "Тестовое уведомление")
        if pool_gate:
            return pool_gate
        try:
            from src.services.notification_service import NotificationService
            from src.services.notification_target_service import NotificationTargetService

            svc = NotificationService(db, NotificationTargetService(db, client_pool))
            bot = await svc.get_status()
            if bot is None:
                return _text_response("Бот уведомлений не настроен. Сначала вызовите setup_notification_bot.")
            await svc.send_notification("🔔 Тестовое уведомление от агента")
            return _text_response("Тестовое уведомление отправлено.")
        except Exception as e:
            return _text_response(f"Ошибка отправки уведомления: {e}")

    tools.append(test_notification)

    # ------------------------------------------------------------------
    # notification_dry_run (READ)
    # ------------------------------------------------------------------

    @tool(
        "notification_dry_run",
        "Preview how many matches each active search query with notify_on_collect=true would produce. "
        "Does NOT send notifications. Queries managed via add_search_query / list_search_queries.",
        {},
    )
    async def notification_dry_run(args):
        try:
            from datetime import timezone

            last_task = await db.repos.tasks.get_last_completed_collect_task()
            since = None
            if last_task and last_task.completed_at:
                since = last_task.completed_at.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            queries = await db.get_notification_queries(active_only=True)
            # Filter out disabled jobs
            filtered = []
            for sq in queries:
                val = await db.repos.settings.get_setting(f"scheduler_job_disabled:sq_{sq.id}")
                if val != "1":
                    filtered.append(sq)
            queries = filtered
            if not queries:
                return _text_response("Нет активных запросов уведомлений.")
            total_matches = 0
            lines = [f"Dry-run уведомлений (с {since or 'N/A'}):"]
            for sq in queries:
                count = 0
                if since:
                    try:
                        _, count = await db.search_messages_for_query_since(sq, since, limit=0)
                    except Exception:
                        count = 0
                name = getattr(sq, "name", None) or sq.query
                lines.append(f"  {name}: {count} совпадений")
                total_matches += count
            lines.append(f"\nИтого: {total_matches} совпадений")
            return _text_response("\n".join(lines))
        except Exception as e:
            return _text_response(f"Ошибка dry-run уведомлений: {e}")

    tools.append(notification_dry_run)

    return tools
