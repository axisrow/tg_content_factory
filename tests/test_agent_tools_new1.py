"""Tests for agent tools: accounts, channels, collection, notifications, settings, messaging.

These tests call actual tool handler functions via the @tool decorator's
.handler attribute, ensuring argument parsing, formatting, and error handling
are all exercised.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.database import Database

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_db():
    """Create a mock Database for testing tools."""
    db = MagicMock(spec=Database)
    db.get_setting = AsyncMock(return_value=None)
    return db


def _get_tool_handlers(mock_db, client_pool=None, config=None, **kwargs):
    """Build MCP tools and return their handlers keyed by name."""
    captured_tools = []

    with patch(
        "src.agent.tools.create_sdk_mcp_server",
        side_effect=lambda **kw: captured_tools.extend(kw.get("tools", [])),
    ):
        from src.agent.tools import make_mcp_server

        make_mcp_server(mock_db, client_pool=client_pool, config=config, **kwargs)

    return {t.name: t.handler for t in captured_tools}


def _text(result: dict) -> str:
    """Extract text from tool result payload."""
    return result["content"][0]["text"]


def _make_account(
    acc_id=1,
    phone="+79001234567",
    is_active=True,
    flood_wait_until=None,
    is_primary=True,
):
    a = MagicMock()
    a.id = acc_id
    a.phone = phone
    a.is_active = is_active
    a.flood_wait_until = flood_wait_until
    a.is_primary = is_primary
    return a


def _make_channel(
    pk=1,
    channel_id=100,
    title="TestChan",
    username="testchan",
    is_active=True,
    is_filtered=False,
    channel_type="channel",
):
    ch = MagicMock()
    ch.id = pk
    ch.channel_id = channel_id
    ch.title = title
    ch.username = username
    ch.is_active = is_active
    ch.is_filtered = is_filtered
    ch.channel_type = channel_type
    return ch


# ===========================================================================
# accounts.py
# ===========================================================================


class TestListAccountsTool:
    @pytest.mark.asyncio
    async def test_empty_returns_not_found(self, mock_db):
        mock_db.get_accounts = AsyncMock(return_value=[])
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["list_accounts"]({})
        assert "Аккаунты не найдены" in _text(result)

    @pytest.mark.asyncio
    async def test_with_accounts_shows_phone_and_status(self, mock_db):
        acc = _make_account(phone="+71112223344", is_active=True)
        mock_db.get_accounts = AsyncMock(return_value=[acc])
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["list_accounts"]({})
        text = _text(result)
        assert "+71112223344" in text
        assert "активен" in text
        assert "Аккаунты (1)" in text

    @pytest.mark.asyncio
    async def test_inactive_account_shows_inactive(self, mock_db):
        acc = _make_account(phone="+70000000000", is_active=False)
        mock_db.get_accounts = AsyncMock(return_value=[acc])
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["list_accounts"]({})
        assert "неактивен" in _text(result)

    @pytest.mark.asyncio
    async def test_flood_wait_shown(self, mock_db):
        acc = _make_account(flood_wait_until="2025-01-01 12:00:00")
        mock_db.get_accounts = AsyncMock(return_value=[acc])
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["list_accounts"]({})
        assert "flood_wait" in _text(result)

    @pytest.mark.asyncio
    async def test_error_returns_text(self, mock_db):
        mock_db.get_accounts = AsyncMock(side_effect=Exception("conn error"))
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["list_accounts"]({})
        assert "Ошибка получения аккаунтов" in _text(result)
        assert "conn error" in _text(result)


class TestToggleAccountTool:
    @pytest.mark.asyncio
    async def test_missing_account_id_returns_error(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["toggle_account"]({})
        assert "account_id обязателен" in _text(result)

    @pytest.mark.asyncio
    async def test_account_not_found_returns_error(self, mock_db):
        mock_db.get_accounts = AsyncMock(return_value=[])
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["toggle_account"]({"account_id": 999})
        assert "не найден" in _text(result)
        assert "999" in _text(result)

    @pytest.mark.asyncio
    async def test_active_account_gets_deactivated(self, mock_db):
        acc = _make_account(acc_id=1, phone="+71111111111", is_active=True)
        mock_db.get_accounts = AsyncMock(return_value=[acc])
        mock_db.set_account_active = AsyncMock()
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["toggle_account"]({"account_id": 1})
        text = _text(result)
        assert "деактивирован" in text
        mock_db.set_account_active.assert_awaited_once_with(1, False)

    @pytest.mark.asyncio
    async def test_inactive_account_gets_activated(self, mock_db):
        acc = _make_account(acc_id=2, phone="+72222222222", is_active=False)
        mock_db.get_accounts = AsyncMock(return_value=[acc])
        mock_db.set_account_active = AsyncMock()
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["toggle_account"]({"account_id": 2})
        text = _text(result)
        assert "активирован" in text
        mock_db.set_account_active.assert_awaited_once_with(2, True)

    @pytest.mark.asyncio
    async def test_error_returns_text(self, mock_db):
        mock_db.get_accounts = AsyncMock(side_effect=Exception("db fail"))
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["toggle_account"]({"account_id": 1})
        assert "Ошибка переключения аккаунта" in _text(result)


class TestDeleteAccountTool:
    @pytest.mark.asyncio
    async def test_missing_account_id_returns_error(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["delete_account"]({})
        assert "account_id обязателен" in _text(result)

    @pytest.mark.asyncio
    async def test_no_confirm_returns_gate(self, mock_db):
        mock_db.get_accounts = AsyncMock(return_value=[])
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["delete_account"]({"account_id": 1})
        assert "confirm=true" in _text(result)

    @pytest.mark.asyncio
    async def test_with_confirm_deletes_account(self, mock_db):
        acc = _make_account(acc_id=5, phone="+75555555555")
        mock_db.get_accounts = AsyncMock(return_value=[acc])
        mock_db.delete_account = AsyncMock()
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["delete_account"]({"account_id": 5, "confirm": True})
        assert "удалён" in _text(result)
        mock_db.delete_account.assert_awaited_once_with(5)

    @pytest.mark.asyncio
    async def test_error_returns_text(self, mock_db):
        mock_db.get_accounts = AsyncMock(return_value=[])
        mock_db.delete_account = AsyncMock(side_effect=Exception("constraint"))
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["delete_account"]({"account_id": 1, "confirm": True})
        assert "Ошибка удаления аккаунта" in _text(result)


class TestGetFloodStatusTool:
    @pytest.mark.asyncio
    async def test_no_accounts_returns_not_found(self, mock_db):
        mock_db.get_accounts = AsyncMock(return_value=[])
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["get_flood_status"]({})
        assert "Аккаунты не найдены" in _text(result)

    @pytest.mark.asyncio
    async def test_no_flood_shows_no_restrictions(self, mock_db):
        acc = _make_account(phone="+71234567890", flood_wait_until=None)
        mock_db.get_accounts = AsyncMock(return_value=[acc])
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["get_flood_status"]({})
        assert "нет ограничений" in _text(result)

    @pytest.mark.asyncio
    async def test_flood_wait_until_shown(self, mock_db):
        acc = _make_account(phone="+70001112233", flood_wait_until="2025-06-01 10:00")
        mock_db.get_accounts = AsyncMock(return_value=[acc])
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["get_flood_status"]({})
        assert "заблокирован" in _text(result)
        assert "2025-06-01 10:00" in _text(result)

    @pytest.mark.asyncio
    async def test_error_returns_text(self, mock_db):
        mock_db.get_accounts = AsyncMock(side_effect=RuntimeError("oops"))
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["get_flood_status"]({})
        assert "Ошибка получения flood-статуса" in _text(result)


class TestClearFloodStatusTool:
    @pytest.mark.asyncio
    async def test_no_confirm_returns_gate(self, mock_db):
        mock_db.get_accounts = AsyncMock(return_value=[_make_account(phone="+71111111111")])
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["clear_flood_status"]({"phone": "+71111111111"})
        assert "confirm=true" in _text(result)

    @pytest.mark.asyncio
    async def test_account_not_found_returns_error(self, mock_db):
        mock_db.get_accounts = AsyncMock(return_value=[])
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["clear_flood_status"]({"phone": "+79999999999", "confirm": True})
        assert "не найден" in _text(result)

    @pytest.mark.asyncio
    async def test_with_confirm_clears_flood(self, mock_db):
        acc = _make_account(phone="+71111111111", flood_wait_until="2025-01-01")
        mock_db.get_accounts = AsyncMock(return_value=[acc])
        mock_db.update_account_flood = AsyncMock()
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["clear_flood_status"]({"phone": "+71111111111", "confirm": True})
        assert "сброшен" in _text(result)
        mock_db.update_account_flood.assert_awaited_once_with("+71111111111", None)


# ===========================================================================
# channels.py
# ===========================================================================


class TestAddChannelTool:
    @pytest.mark.asyncio
    async def test_missing_identifier_returns_error(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["add_channel"]({})
        assert "identifier обязателен" in _text(result)

    @pytest.mark.asyncio
    async def test_no_confirm_returns_gate(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["add_channel"]({"identifier": "@testchan"})
        assert "confirm=true" in _text(result)

    @pytest.mark.asyncio
    async def test_with_confirm_adds_channel(self, mock_db):
        with patch("src.services.channel_service.ChannelService") as mock_svc:
            mock_svc.return_value.add_by_identifier = AsyncMock(return_value=True)
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["add_channel"]({"identifier": "@mychan", "confirm": True})
        assert "успешно добавлен" in _text(result)

    @pytest.mark.asyncio
    async def test_already_exists_returns_message(self, mock_db):
        with patch("src.services.channel_service.ChannelService") as mock_svc:
            mock_svc.return_value.add_by_identifier = AsyncMock(return_value=False)
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["add_channel"]({"identifier": "@existing", "confirm": True})
        assert "уже существует" in _text(result) or "не удалось добавить" in _text(result)

    @pytest.mark.asyncio
    async def test_error_returns_text(self, mock_db):
        with patch("src.services.channel_service.ChannelService") as mock_svc:
            mock_svc.return_value.add_by_identifier = AsyncMock(side_effect=Exception("API error"))
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["add_channel"]({"identifier": "@broken", "confirm": True})
        assert "Ошибка добавления канала" in _text(result)


class TestDeleteChannelTool:
    @pytest.mark.asyncio
    async def test_missing_pk_returns_error(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["delete_channel"]({})
        assert "pk обязателен" in _text(result)

    @pytest.mark.asyncio
    async def test_no_confirm_returns_gate(self, mock_db):
        mock_db.get_channel_by_pk = AsyncMock(return_value=_make_channel())
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["delete_channel"]({"pk": 1})
        assert "confirm=true" in _text(result)

    @pytest.mark.asyncio
    async def test_with_confirm_deletes_channel(self, mock_db):
        ch = _make_channel(title="DeleteMe")
        mock_db.get_channel_by_pk = AsyncMock(return_value=ch)
        with patch("src.services.channel_service.ChannelService") as mock_svc:
            mock_svc.return_value.delete = AsyncMock()
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["delete_channel"]({"pk": 1, "confirm": True})
        assert "удалён" in _text(result)
        assert "DeleteMe" in _text(result)

    @pytest.mark.asyncio
    async def test_error_returns_text(self, mock_db):
        mock_db.get_channel_by_pk = AsyncMock(return_value=_make_channel())
        with patch("src.services.channel_service.ChannelService") as mock_svc:
            mock_svc.return_value.delete = AsyncMock(side_effect=Exception("fk constraint"))
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["delete_channel"]({"pk": 1, "confirm": True})
        assert "Ошибка удаления канала" in _text(result)


class TestToggleChannelTool:
    @pytest.mark.asyncio
    async def test_missing_pk_returns_error(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["toggle_channel"]({})
        assert "pk обязателен" in _text(result)

    @pytest.mark.asyncio
    async def test_active_channel_gets_deactivated(self, mock_db):
        ch_after = _make_channel(is_active=False, title="MyChan")
        with patch("src.services.channel_service.ChannelService") as mock_svc:
            mock_svc.return_value.toggle = AsyncMock()
            mock_db.get_channel_by_pk = AsyncMock(return_value=ch_after)
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["toggle_channel"]({"pk": 1})
        assert "неактивен" in _text(result)
        assert "MyChan" in _text(result)

    @pytest.mark.asyncio
    async def test_error_returns_text(self, mock_db):
        with patch("src.services.channel_service.ChannelService") as mock_svc:
            mock_svc.return_value.toggle = AsyncMock(side_effect=Exception("not found"))
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["toggle_channel"]({"pk": 1})
        assert "Ошибка переключения канала" in _text(result)


class TestImportChannelsTool:
    @pytest.mark.asyncio
    async def test_missing_text_returns_error(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["import_channels"]({})
        assert "text обязателен" in _text(result)

    @pytest.mark.asyncio
    async def test_no_identifiers_returns_error(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["import_channels"]({"text": "hello world nothing here"})
        assert "Не удалось распознать" in _text(result)

    @pytest.mark.asyncio
    async def test_no_confirm_returns_gate(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["import_channels"]({"text": "@chan1 @chan2"})
        assert "confirm=true" in _text(result)

    @pytest.mark.asyncio
    async def test_with_confirm_imports_channels(self, mock_db):
        with patch("src.services.channel_service.ChannelService") as mock_svc:
            mock_svc.return_value.add_by_identifier = AsyncMock(return_value=True)
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["import_channels"]({"text": "@chan1 @chan2", "confirm": True})
        text = _text(result)
        assert "Импорт завершён" in text
        assert "2/2" in text

    @pytest.mark.asyncio
    async def test_partial_failure_reported(self, mock_db):
        call_count = 0

        async def flaky_add(ident):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return True
            raise Exception("API error")

        with patch("src.services.channel_service.ChannelService") as mock_svc:
            mock_svc.return_value.add_by_identifier = flaky_add
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["import_channels"]({"text": "@chan1 @chan2", "confirm": True})
        text = _text(result)
        assert "Ошибки" in text


# ===========================================================================
# collection.py
# ===========================================================================


class TestCollectChannelTool:
    @pytest.mark.asyncio
    async def test_no_pool_returns_pool_gate(self, mock_db):
        handlers = _get_tool_handlers(mock_db, client_pool=None)
        result = await handlers["collect_channel"]({"pk": 1})
        assert "Telegram-клиент" in _text(result)

    @pytest.mark.asyncio
    async def test_missing_pk_returns_error(self, mock_db):
        pool = MagicMock()
        handlers = _get_tool_handlers(mock_db, client_pool=pool)
        result = await handlers["collect_channel"]({})
        assert "pk обязателен" in _text(result)

    @pytest.mark.asyncio
    async def test_channel_not_found_returns_error(self, mock_db):
        pool = MagicMock()
        mock_db.get_channel_by_pk = AsyncMock(return_value=None)
        handlers = _get_tool_handlers(mock_db, client_pool=pool)
        result = await handlers["collect_channel"]({"pk": 999})
        assert "не найден" in _text(result)

    @pytest.mark.asyncio
    async def test_filtered_channel_without_force_returns_warning(self, mock_db):
        pool = MagicMock()
        ch = _make_channel(is_filtered=True, title="SpamChan")
        mock_db.get_channel_by_pk = AsyncMock(return_value=ch)
        handlers = _get_tool_handlers(mock_db, client_pool=pool)
        result = await handlers["collect_channel"]({"pk": 1})
        assert "отфильтрован" in _text(result)
        assert "force=true" in _text(result)

    @pytest.mark.asyncio
    async def test_filtered_channel_with_force_enqueues(self, mock_db):
        pool = MagicMock()
        ch = _make_channel(is_filtered=True, title="SpamChan")
        mock_db.get_channel_by_pk = AsyncMock(return_value=ch)
        mock_db.create_collection_task = AsyncMock()
        handlers = _get_tool_handlers(mock_db, client_pool=pool)
        result = await handlers["collect_channel"]({"pk": 1, "force": True})
        assert "поставлен в очередь" in _text(result)

    @pytest.mark.asyncio
    async def test_normal_channel_enqueues(self, mock_db):
        pool = MagicMock()
        ch = _make_channel(title="GoodChan")
        mock_db.get_channel_by_pk = AsyncMock(return_value=ch)
        mock_db.create_collection_task = AsyncMock()
        handlers = _get_tool_handlers(mock_db, client_pool=pool)
        result = await handlers["collect_channel"]({"pk": 1})
        assert "поставлен в очередь" in _text(result)
        assert "GoodChan" in _text(result)
        mock_db.create_collection_task.assert_awaited_once()


class TestCollectAllChannelsTool:
    @pytest.mark.asyncio
    async def test_no_pool_returns_pool_gate(self, mock_db):
        handlers = _get_tool_handlers(mock_db, client_pool=None)
        result = await handlers["collect_all_channels"]({})
        assert "Telegram-клиент" in _text(result)

    @pytest.mark.asyncio
    async def test_no_channels_returns_message(self, mock_db):
        pool = MagicMock()
        mock_db.get_channels = AsyncMock(return_value=[])
        handlers = _get_tool_handlers(mock_db, client_pool=pool)
        result = await handlers["collect_all_channels"]({})
        assert "Нет активных каналов" in _text(result)

    @pytest.mark.asyncio
    async def test_enqueues_all_active_channels(self, mock_db):
        pool = MagicMock()
        channels = [_make_channel(pk=i, channel_id=i * 100, title=f"Chan{i}") for i in range(3)]
        mock_db.get_channels = AsyncMock(return_value=channels)
        mock_db.create_collection_task = AsyncMock()
        handlers = _get_tool_handlers(mock_db, client_pool=pool)
        result = await handlers["collect_all_channels"]({})
        text = _text(result)
        assert "3 каналов" in text
        assert mock_db.create_collection_task.await_count == 3


class TestCollectChannelStatsTool:
    @pytest.mark.asyncio
    async def test_no_pool_returns_pool_gate(self, mock_db):
        handlers = _get_tool_handlers(mock_db, client_pool=None)
        result = await handlers["collect_channel_stats"]({"pk": 1})
        assert "Telegram-клиент" in _text(result)

    @pytest.mark.asyncio
    async def test_missing_pk_returns_error(self, mock_db):
        pool = MagicMock()
        handlers = _get_tool_handlers(mock_db, client_pool=pool)
        result = await handlers["collect_channel_stats"]({})
        assert "pk обязателен" in _text(result)

    @pytest.mark.asyncio
    async def test_channel_not_found_returns_error(self, mock_db):
        pool = MagicMock()
        mock_db.get_channel_by_pk = AsyncMock(return_value=None)
        handlers = _get_tool_handlers(mock_db, client_pool=pool)
        result = await handlers["collect_channel_stats"]({"pk": 99})
        assert "не найден" in _text(result)

    @pytest.mark.asyncio
    async def test_enqueues_stats_task(self, mock_db):
        pool = MagicMock()
        ch = _make_channel(title="StatsChan")
        mock_db.get_channel_by_pk = AsyncMock(return_value=ch)
        mock_db.create_stats_task = AsyncMock()
        handlers = _get_tool_handlers(mock_db, client_pool=pool)
        result = await handlers["collect_channel_stats"]({"pk": 1})
        assert "поставлен в очередь" in _text(result)
        assert "StatsChan" in _text(result)
        mock_db.create_stats_task.assert_awaited_once()


class TestCollectAllStatsTool:
    @pytest.mark.asyncio
    async def test_no_pool_returns_pool_gate(self, mock_db):
        handlers = _get_tool_handlers(mock_db, client_pool=None)
        result = await handlers["collect_all_stats"]({})
        assert "Telegram-клиент" in _text(result)

    @pytest.mark.asyncio
    async def test_no_channels_returns_message(self, mock_db):
        pool = MagicMock()
        mock_db.get_channels = AsyncMock(return_value=[])
        handlers = _get_tool_handlers(mock_db, client_pool=pool)
        result = await handlers["collect_all_stats"]({})
        assert "Нет активных каналов" in _text(result)

    @pytest.mark.asyncio
    async def test_enqueues_stats_for_all(self, mock_db):
        pool = MagicMock()
        channels = [_make_channel(pk=i, channel_id=i * 100) for i in range(5)]
        mock_db.get_channels = AsyncMock(return_value=channels)
        mock_db.create_stats_task = AsyncMock()
        handlers = _get_tool_handlers(mock_db, client_pool=pool)
        result = await handlers["collect_all_stats"]({})
        assert "5 каналов" in _text(result)
        mock_db.create_stats_task.assert_awaited_once()


# ===========================================================================
# notifications.py
# ===========================================================================


class TestGetNotificationStatusTool:
    @pytest.mark.asyncio
    async def test_not_configured_returns_message(self, mock_db):
        with (
            patch("src.services.notification_service.NotificationService") as mock_ns,
            patch("src.services.notification_target_service.NotificationTargetService"),
        ):
            mock_ns.return_value.get_status = AsyncMock(return_value=None)
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["get_notification_status"]({})
        assert "не настроен" in _text(result)

    @pytest.mark.asyncio
    async def test_configured_shows_bot_info(self, mock_db):
        bot = SimpleNamespace(bot_username="mybot", chat_id=12345, created_at="2025-01-01")
        with (
            patch("src.services.notification_service.NotificationService") as mock_ns,
            patch("src.services.notification_target_service.NotificationTargetService"),
        ):
            mock_ns.return_value.get_status = AsyncMock(return_value=bot)
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["get_notification_status"]({})
        text = _text(result)
        assert "@mybot" in text
        assert "12345" in text

    @pytest.mark.asyncio
    async def test_error_returns_text(self, mock_db):
        with (
            patch("src.services.notification_service.NotificationService") as mock_ns,
            patch("src.services.notification_target_service.NotificationTargetService"),
        ):
            mock_ns.return_value.get_status = AsyncMock(side_effect=Exception("db fail"))
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["get_notification_status"]({})
        assert "Ошибка получения статуса бота" in _text(result)


class TestSetupNotificationBotTool:
    @pytest.mark.asyncio
    async def test_no_pool_returns_pool_gate(self, mock_db):
        handlers = _get_tool_handlers(mock_db, client_pool=None)
        result = await handlers["setup_notification_bot"]({"confirm": True})
        assert "Telegram-клиент" in _text(result)

    @pytest.mark.asyncio
    async def test_no_confirm_returns_gate(self, mock_db):
        pool = MagicMock()
        handlers = _get_tool_handlers(mock_db, client_pool=pool)
        result = await handlers["setup_notification_bot"]({})
        assert "confirm=true" in _text(result)

    @pytest.mark.asyncio
    async def test_with_confirm_creates_bot(self, mock_db):
        pool = MagicMock()
        bot = SimpleNamespace(bot_username="newbot", chat_id=99999)
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


class TestDeleteNotificationBotTool:
    @pytest.mark.asyncio
    async def test_no_pool_returns_pool_gate(self, mock_db):
        handlers = _get_tool_handlers(mock_db, client_pool=None)
        result = await handlers["delete_notification_bot"]({"confirm": True})
        assert "Telegram-клиент" in _text(result)

    @pytest.mark.asyncio
    async def test_no_confirm_returns_gate(self, mock_db):
        pool = MagicMock()
        handlers = _get_tool_handlers(mock_db, client_pool=pool)
        result = await handlers["delete_notification_bot"]({})
        assert "confirm=true" in _text(result)

    @pytest.mark.asyncio
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


class TestTestNotificationTool:
    @pytest.mark.asyncio
    async def test_no_pool_returns_pool_gate(self, mock_db):
        handlers = _get_tool_handlers(mock_db, client_pool=None)
        result = await handlers["test_notification"]({})
        assert "Telegram-клиент" in _text(result)

    @pytest.mark.asyncio
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

    @pytest.mark.asyncio
    async def test_sends_notification(self, mock_db):
        pool = MagicMock()
        bot = SimpleNamespace(bot_username="testbot", chat_id=1)
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


# ===========================================================================
# settings.py
# ===========================================================================


class TestGetSettingsTool:
    @pytest.mark.asyncio
    async def test_shows_all_settings_keys(self, mock_db):
        mock_db.get_setting = AsyncMock(return_value=None)
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["get_settings"]({})
        text = _text(result)
        assert "scheduler_interval_minutes" in text
        assert "agent_prompt_template" in text
        assert "Настройки системы" in text

    @pytest.mark.asyncio
    async def test_shows_set_values(self, mock_db):
        async def fake_get(key):
            return "60" if key == "scheduler_interval_minutes" else None

        mock_db.get_setting = fake_get
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["get_settings"]({})
        assert "60" in _text(result)

    @pytest.mark.asyncio
    async def test_error_returns_text(self, mock_db):
        mock_db.get_setting = AsyncMock(side_effect=Exception("no table"))
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["get_settings"]({})
        assert "Ошибка получения настроек" in _text(result)


class TestSaveSchedulerSettingsTool:
    @pytest.mark.asyncio
    async def test_no_confirm_returns_gate(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["save_scheduler_settings"]({"interval_minutes": 30})
        assert "confirm=true" in _text(result)

    @pytest.mark.asyncio
    async def test_with_confirm_saves_interval(self, mock_db):
        mock_db.set_setting = AsyncMock()
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["save_scheduler_settings"]({"interval_minutes": 30, "confirm": True})
        assert "сохранены" in _text(result)
        mock_db.set_setting.assert_any_await("scheduler_interval_minutes", "30")

    @pytest.mark.asyncio
    async def test_with_confirm_saves_enabled_flag(self, mock_db):
        mock_db.set_setting = AsyncMock()
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["save_scheduler_settings"]({"enabled": False, "confirm": True})
        assert "сохранены" in _text(result)
        mock_db.set_setting.assert_any_await("scheduler_enabled", "false")

    @pytest.mark.asyncio
    async def test_error_returns_text(self, mock_db):
        mock_db.set_setting = AsyncMock(side_effect=Exception("write error"))
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["save_scheduler_settings"]({"interval_minutes": 10, "confirm": True})
        assert "Ошибка сохранения настроек" in _text(result)


class TestSaveAgentSettingsTool:
    @pytest.mark.asyncio
    async def test_no_confirm_returns_gate(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["save_agent_settings"]({"backend": "claude"})
        assert "confirm=true" in _text(result)

    @pytest.mark.asyncio
    async def test_saves_prompt_template(self, mock_db):
        mock_db.set_setting = AsyncMock()
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["save_agent_settings"](
            {"prompt_template": "You are a helpful bot.", "confirm": True}
        )
        assert "сохранены" in _text(result)
        mock_db.set_setting.assert_any_await("agent_prompt_template", "You are a helpful bot.")

    @pytest.mark.asyncio
    async def test_saves_backend_override(self, mock_db):
        mock_db.set_setting = AsyncMock()
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["save_agent_settings"]({"backend": "deepagents", "confirm": True})
        assert "сохранены" in _text(result)
        mock_db.set_setting.assert_any_await("agent_backend_override", "deepagents")


class TestSaveFilterSettingsTool:
    @pytest.mark.asyncio
    async def test_no_confirm_returns_gate(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["save_filter_settings"]({"low_uniqueness_threshold": 0.5})
        assert "confirm=true" in _text(result)

    @pytest.mark.asyncio
    async def test_saves_thresholds(self, mock_db):
        mock_db.set_setting = AsyncMock()
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["save_filter_settings"](
            {"low_uniqueness_threshold": 0.3, "low_subscriber_ratio_threshold": 0.1, "confirm": True}
        )
        assert "сохранены" in _text(result)
        mock_db.set_setting.assert_any_await("low_uniqueness_threshold", "0.3")
        mock_db.set_setting.assert_any_await("low_subscriber_ratio_threshold", "0.1")


class TestGetSystemInfoTool:
    @pytest.mark.asyncio
    async def test_shows_stats(self, mock_db):
        mock_db.get_stats = AsyncMock(return_value={"channels": 10, "messages": 1000})
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["get_system_info"]({})
        text = _text(result)
        assert "channels" in text
        assert "10" in text
        assert "Системная информация" in text

    @pytest.mark.asyncio
    async def test_error_returns_text(self, mock_db):
        mock_db.get_stats = AsyncMock(side_effect=Exception("no stats"))
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["get_system_info"]({})
        assert "Ошибка получения системной информации" in _text(result)


# ===========================================================================
# messaging.py — no-pool & validation tests (no real Telegram)
# ===========================================================================


class TestSendMessageTool:
    @pytest.mark.asyncio
    async def test_no_pool_returns_pool_gate(self, mock_db):
        handlers = _get_tool_handlers(mock_db, client_pool=None)
        result = await handlers["send_message"](
            {"recipient": "@user", "text": "hello", "confirm": True}
        )
        assert "Telegram-клиент" in _text(result)

    @pytest.mark.asyncio
    async def test_missing_recipient_or_text_returns_error(self, mock_db):
        pool = MagicMock()
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        mock_db.get_setting = AsyncMock(return_value=None)
        handlers = _get_tool_handlers(mock_db, client_pool=pool)
        result = await handlers["send_message"](
            {"recipient": "", "text": "", "confirm": True, "phone": "+71111111111"}
        )
        assert "обязательны" in _text(result)

    @pytest.mark.asyncio
    async def test_no_confirm_returns_gate(self, mock_db):
        pool = MagicMock()
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        mock_db.get_setting = AsyncMock(return_value=None)
        handlers = _get_tool_handlers(mock_db, client_pool=pool)
        result = await handlers["send_message"](
            {"recipient": "@user", "text": "hi", "phone": "+71111111111"}
        )
        assert "confirm=true" in _text(result)


class TestEditMessageTool:
    @pytest.mark.asyncio
    async def test_no_pool_returns_pool_gate(self, mock_db):
        handlers = _get_tool_handlers(mock_db, client_pool=None)
        result = await handlers["edit_message"](
            {"chat_id": "100", "message_id": 1, "text": "new", "confirm": True}
        )
        assert "Telegram-клиент" in _text(result)

    @pytest.mark.asyncio
    async def test_missing_fields_returns_error(self, mock_db):
        pool = MagicMock()
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        mock_db.get_setting = AsyncMock(return_value=None)
        handlers = _get_tool_handlers(mock_db, client_pool=pool)
        result = await handlers["edit_message"](
            {"chat_id": "", "message_id": None, "text": "", "confirm": True, "phone": "+71111111111"}
        )
        assert "обязательны" in _text(result)


class TestDeleteMessageTool:
    @pytest.mark.asyncio
    async def test_no_pool_returns_pool_gate(self, mock_db):
        handlers = _get_tool_handlers(mock_db, client_pool=None)
        result = await handlers["delete_message"](
            {"chat_id": "100", "message_ids": "1,2", "confirm": True}
        )
        assert "Telegram-клиент" in _text(result)

    @pytest.mark.asyncio
    async def test_missing_chat_id_returns_error(self, mock_db):
        pool = MagicMock()
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        mock_db.get_setting = AsyncMock(return_value=None)
        handlers = _get_tool_handlers(mock_db, client_pool=pool)
        result = await handlers["delete_message"](
            {"chat_id": "", "message_ids": "1", "confirm": True, "phone": "+71111111111"}
        )
        assert "обязательны" in _text(result)

    @pytest.mark.asyncio
    async def test_invalid_message_ids_returns_error(self, mock_db):
        pool = MagicMock()
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        mock_db.get_setting = AsyncMock(return_value=None)
        handlers = _get_tool_handlers(mock_db, client_pool=pool)
        result = await handlers["delete_message"](
            {"chat_id": "100", "message_ids": "abc,xyz", "confirm": True, "phone": "+71111111111"}
        )
        assert "валидные message_ids" in _text(result)


class TestGetParticipantsTool:
    @pytest.mark.asyncio
    async def test_no_pool_returns_pool_gate(self, mock_db):
        handlers = _get_tool_handlers(mock_db, client_pool=None)
        result = await handlers["get_participants"]({"chat_id": "@grp"})
        assert "Telegram-клиент" in _text(result)

    @pytest.mark.asyncio
    async def test_missing_chat_id_returns_error(self, mock_db):
        pool = MagicMock()
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        mock_db.get_setting = AsyncMock(return_value=None)
        handlers = _get_tool_handlers(mock_db, client_pool=pool)
        result = await handlers["get_participants"](
            {"chat_id": "", "phone": "+71111111111"}
        )
        assert "обязателен" in _text(result)


class TestKickParticipantTool:
    @pytest.mark.asyncio
    async def test_no_pool_returns_pool_gate(self, mock_db):
        handlers = _get_tool_handlers(mock_db, client_pool=None)
        result = await handlers["kick_participant"](
            {"chat_id": "100", "user_id": "@bad", "confirm": True}
        )
        assert "Telegram-клиент" in _text(result)

    @pytest.mark.asyncio
    async def test_no_confirm_returns_gate(self, mock_db):
        pool = MagicMock()
        mock_db.get_accounts = AsyncMock(return_value=[_make_account()])
        mock_db.get_setting = AsyncMock(return_value=None)
        handlers = _get_tool_handlers(mock_db, client_pool=pool)
        result = await handlers["kick_participant"](
            {"chat_id": "100", "user_id": "@bad", "phone": "+71111111111"}
        )
        assert "confirm=true" in _text(result)
