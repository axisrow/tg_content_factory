"""Tests for agent tools: notifications.py."""
from __future__ import annotations

from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.models import NotificationBot
from tests.agent_tools_helpers import _get_tool_handlers, _text


@contextmanager
def _notif_ctx(notif_svc):
    """Context manager that patches notification services at their source modules."""
    with (
        patch("src.services.notification_service.NotificationService", return_value=notif_svc),
        patch(
            "src.services.notification_target_service.NotificationTargetService",
            return_value=MagicMock(),
        ),
    ):
        yield


def _make_mock_pool():
    """Create a mock client pool."""
    mock_client = AsyncMock()
    mock_session = MagicMock()
    mock_pool = MagicMock()
    mock_pool.get_native_client_by_phone = AsyncMock(return_value=(mock_client, None))
    mock_pool.get_client_by_phone = AsyncMock(return_value=(mock_session, None))
    mock_pool.resolve_dialog_entity = AsyncMock(return_value=MagicMock(id=123456))
    return mock_pool


class TestGetNotificationStatusTool:
    @pytest.mark.anyio
    async def test_not_configured_returns_message(self, mock_db):
        with (
            patch("src.services.notification_service.NotificationService") as mock_ns,
            patch("src.services.notification_target_service.NotificationTargetService"),
        ):
            mock_ns.return_value.get_status = AsyncMock(return_value=None)
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["get_notification_status"]({})
        assert "не настроен" in _text(result)

    @pytest.mark.anyio
    async def test_target_unavailable_returns_unknown_status(self, mock_db):
        target_status = SimpleNamespace(
            mode="primary",
            state="disconnected",
            message="Аккаунт +100 не подключён.",
            effective_phone="+100",
            configured_phone=None,
        )
        target_svc = MagicMock()
        target_svc.describe_target = AsyncMock(return_value=target_status)
        with (
            patch("src.services.notification_service.NotificationService") as mock_ns,
            patch(
                "src.services.notification_target_service.NotificationTargetService",
                return_value=target_svc,
            ),
        ):
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["get_notification_status"]({})
        text = _text(result)
        assert "невозможно проверить" in text
        assert "не настроен" not in text
        assert "disconnected" in text
        mock_ns.return_value.get_status.assert_not_called()

    @pytest.mark.anyio
    async def test_configured_shows_bot_info(self, mock_db):
        bot = NotificationBot(
            tg_user_id=12345,
            tg_username="target",
            bot_id=67890,
            bot_username="mybot",
            bot_token="token",
            created_at="2025-01-01",
        )
        with (
            patch("src.services.notification_service.NotificationService") as mock_ns,
            patch("src.services.notification_target_service.NotificationTargetService"),
        ):
            mock_ns.return_value.get_status = AsyncMock(return_value=bot)
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["get_notification_status"]({})
        text = _text(result)
        assert "@mybot" in text
        assert "67890" in text
        assert "12345" in text

    @pytest.mark.anyio
    async def test_error_returns_text(self, mock_db):
        with (
            patch("src.services.notification_service.NotificationService") as mock_ns,
            patch("src.services.notification_target_service.NotificationTargetService"),
        ):
            mock_ns.return_value.get_status = AsyncMock(side_effect=Exception("db fail"))
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["get_notification_status"]({})
        assert "Ошибка получения статуса бота" in _text(result)

    @pytest.mark.anyio
    async def test_not_configured_with_pool(self, mock_db):
        notif_svc = MagicMock()
        notif_svc.get_status = AsyncMock(return_value=None)
        mock_pool = _make_mock_pool()

        with _notif_ctx(notif_svc):
            handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
            result = await handlers["get_notification_status"]({})
        assert "не настроен" in _text(result)

    @pytest.mark.anyio
    async def test_configured_shows_bot_details_with_pool(self, mock_db):
        notif_svc = MagicMock()
        bot = NotificationBot(
            tg_user_id=123456,
            bot_id=987654,
            bot_username="my_bot",
            bot_token="token",
            created_at="2025-01-01",
        )
        notif_svc.get_status = AsyncMock(return_value=bot)
        mock_pool = _make_mock_pool()

        with _notif_ctx(notif_svc):
            handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
            result = await handlers["get_notification_status"]({})
        text = _text(result)
        assert "my_bot" in text
        assert "987654" in text
        assert "123456" in text


class TestSetupNotificationBotTool:
    @pytest.mark.anyio
    async def test_no_pool_returns_pool_gate(self, mock_db):
        handlers = _get_tool_handlers(mock_db, client_pool=None)
        result = await handlers["setup_notification_bot"]({"confirm": True})
        assert "Telegram-клиент" in _text(result)

    @pytest.mark.anyio
    async def test_no_confirm_returns_gate(self, mock_db):
        pool = MagicMock()
        handlers = _get_tool_handlers(mock_db, client_pool=pool)
        result = await handlers["setup_notification_bot"]({})
        assert "confirm=true" in _text(result)

    @pytest.mark.anyio
    async def test_with_confirm_creates_bot(self, mock_db):
        pool = MagicMock()
        bot = SimpleNamespace(bot_username="newbot", bot_id=99999, tg_user_id=111)
        with (
            patch("src.services.notification_service.NotificationService") as mock_ns,
            patch("src.services.notification_target_service.NotificationTargetService"),
        ):
            mock_ns.return_value.setup_bot = AsyncMock(return_value=bot)
            handlers = _get_tool_handlers(mock_db, client_pool=pool)
            result = await handlers["setup_notification_bot"]({"confirm": True})
        text = _text(result)
        assert "создан" in text
        assert "@newbot" in text

    @pytest.mark.anyio
    async def test_no_pool_returns_gate_cli_check(self, mock_db):
        handlers = _get_tool_handlers(mock_db, client_pool=None)
        result = await handlers["setup_notification_bot"]({"confirm": True})
        assert "CLI-режиме" in _text(result) or "Telegram-клиент" in _text(result)

    @pytest.mark.anyio
    async def test_with_pool_and_confirm_success(self, mock_db):
        notif_svc = MagicMock()
        bot = MagicMock()
        bot.bot_username = "test_notify_bot"
        bot.bot_id = 789
        bot.tg_user_id = 111
        notif_svc.setup_bot = AsyncMock(return_value=bot)
        mock_pool = _make_mock_pool()

        with _notif_ctx(notif_svc):
            handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
            result = await handlers["setup_notification_bot"]({"confirm": True})
        text = _text(result)
        assert "создан" in text


class TestDeleteNotificationBotTool:
    @pytest.mark.anyio
    async def test_no_pool_returns_pool_gate(self, mock_db):
        handlers = _get_tool_handlers(mock_db, client_pool=None)
        result = await handlers["delete_notification_bot"]({"confirm": True})
        assert "Telegram-клиент" in _text(result)

    @pytest.mark.anyio
    async def test_no_confirm_returns_gate(self, mock_db):
        pool = MagicMock()
        handlers = _get_tool_handlers(mock_db, client_pool=pool)
        result = await handlers["delete_notification_bot"]({})
        assert "confirm=true" in _text(result)

    @pytest.mark.anyio
    async def test_with_confirm_deletes_bot(self, mock_db):
        pool = MagicMock()
        with (
            patch("src.services.notification_service.NotificationService") as mock_ns,
            patch("src.services.notification_target_service.NotificationTargetService"),
        ):
            mock_ns.return_value.teardown_bot = AsyncMock()
            handlers = _get_tool_handlers(mock_db, client_pool=pool)
            result = await handlers["delete_notification_bot"]({"confirm": True})
        assert "удалён" in _text(result)

    @pytest.mark.anyio
    async def test_with_confirm_deleted_via_ctx(self, mock_db):
        notif_svc = MagicMock()
        notif_svc.teardown_bot = AsyncMock()
        mock_pool = _make_mock_pool()

        with _notif_ctx(notif_svc):
            handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
            result = await handlers["delete_notification_bot"]({"confirm": True})
        assert "удалён" in _text(result)


class TestTestNotificationTool:
    @pytest.mark.anyio
    async def test_no_pool_returns_pool_gate(self, mock_db):
        handlers = _get_tool_handlers(mock_db, client_pool=None)
        result = await handlers["test_notification"]({})
        assert "Telegram-клиент" in _text(result)

    @pytest.mark.anyio
    async def test_not_configured_returns_message(self, mock_db):
        pool = MagicMock()
        with (
            patch("src.services.notification_service.NotificationService") as mock_ns,
            patch("src.services.notification_target_service.NotificationTargetService"),
        ):
            mock_ns.return_value.get_status = AsyncMock(return_value=None)
            handlers = _get_tool_handlers(mock_db, client_pool=pool)
            result = await handlers["test_notification"]({})
        assert "не настроен" in _text(result)

    @pytest.mark.anyio
    async def test_sends_notification(self, mock_db):
        pool = MagicMock()
        bot = SimpleNamespace(bot_username="testbot", bot_id=1, tg_user_id=111)
        with (
            patch("src.services.notification_service.NotificationService") as mock_ns,
            patch("src.services.notification_target_service.NotificationTargetService"),
        ):
            inst = mock_ns.return_value
            inst.get_status = AsyncMock(return_value=bot)
            inst.send_notification = AsyncMock()
            handlers = _get_tool_handlers(mock_db, client_pool=pool)
            result = await handlers["test_notification"]({})
        assert "отправлено" in _text(result)
        inst.send_notification.assert_awaited_once()

    @pytest.mark.anyio
    async def test_configured_sends_test_via_ctx(self, mock_db):
        notif_svc = MagicMock()
        bot = MagicMock()
        notif_svc.get_status = AsyncMock(return_value=bot)
        notif_svc.send_notification = AsyncMock()
        mock_pool = _make_mock_pool()

        with _notif_ctx(notif_svc):
            handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
            result = await handlers["test_notification"]({})
        text = _text(result)
        assert "отправлено" in text
