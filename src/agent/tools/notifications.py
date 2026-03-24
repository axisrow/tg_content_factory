"""Agent tools for notification bot management."""

from __future__ import annotations

from claude_agent_sdk import tool
from mcp.types import ToolAnnotations

from src.agent.tools._registry import _text_response, require_confirmation, require_pool


def register(db, client_pool, embedding_service, **kwargs):
    tools = []

    @tool("get_notification_status", "Get notification bot status and configuration", {})
    async def get_notification_status(args):
        try:
            from src.services.notification_service import NotificationService

            svc = NotificationService(db, client_pool)
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
        {"confirm": bool},
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

            svc = NotificationService(db, client_pool)
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
        {"confirm": bool},
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

            svc = NotificationService(db, client_pool)
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

            svc = NotificationService(db, client_pool)
            bot = await svc.get_status()
            if bot is None:
                return _text_response("Бот уведомлений не настроен. Сначала вызовите setup_notification_bot.")
            await svc.send_notification("🔔 Тестовое уведомление от агента")
            return _text_response("Тестовое уведомление отправлено.")
        except Exception as e:
            return _text_response(f"Ошибка отправки уведомления: {e}")

    tools.append(test_notification)

    return tools
