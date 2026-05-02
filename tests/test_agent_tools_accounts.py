"""Tests for agent tools: accounts.py."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from tests.agent_tools_helpers import _get_tool_handlers, _text


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


class TestListAccountsTool:
    @pytest.mark.anyio
    async def test_empty_returns_not_found(self, mock_db):
        mock_db.get_accounts = AsyncMock(return_value=[])
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["list_accounts"]({})
        assert "Аккаунты не найдены" in _text(result)

    @pytest.mark.anyio
    async def test_with_accounts_shows_phone_and_status(self, mock_db):
        acc = _make_account(phone="+71112223344", is_active=True)
        mock_db.get_accounts = AsyncMock(return_value=[acc])
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["list_accounts"]({})
        text = _text(result)
        assert "+71112223344" in text
        assert "активен" in text
        assert "Аккаунты (1)" in text

    @pytest.mark.anyio
    async def test_inactive_account_shows_inactive(self, mock_db):
        acc = _make_account(phone="+70000000000", is_active=False)
        mock_db.get_accounts = AsyncMock(return_value=[acc])
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["list_accounts"]({})
        assert "неактивен" in _text(result)

    @pytest.mark.anyio
    async def test_flood_wait_shown(self, mock_db):
        acc = _make_account(flood_wait_until="2030-01-01 12:00:00")
        mock_db.get_accounts = AsyncMock(return_value=[acc])
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["list_accounts"]({})
        assert "flood_wait" in _text(result)

    @pytest.mark.anyio
    async def test_expired_flood_wait_is_cleared(self, mock_db):
        past = datetime.now(timezone.utc) - timedelta(minutes=5)
        acc = _make_account(flood_wait_until=past)
        mock_db.get_accounts = AsyncMock(return_value=[acc])
        mock_db.update_account_flood = AsyncMock()
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["list_accounts"]({})
        text = _text(result)
        assert "flood_wait до" not in text
        assert acc.flood_wait_until is None
        mock_db.update_account_flood.assert_awaited_once_with("+79001234567", None)

    @pytest.mark.anyio
    async def test_error_returns_text(self, mock_db):
        mock_db.get_accounts = AsyncMock(side_effect=Exception("conn error"))
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["list_accounts"]({})
        assert "Ошибка получения аккаунтов" in _text(result)
        assert "conn error" in _text(result)


class TestToggleAccountTool:
    @pytest.mark.anyio
    async def test_missing_account_id_returns_error(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["toggle_account"]({})
        assert "account_id обязателен" in _text(result)

    @pytest.mark.anyio
    async def test_account_not_found_returns_error(self, mock_db):
        mock_db.get_accounts = AsyncMock(return_value=[])
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["toggle_account"]({"account_id": 999})
        assert "не найден" in _text(result)
        assert "999" in _text(result)

    @pytest.mark.anyio
    async def test_active_account_gets_deactivated(self, mock_db):
        acc = _make_account(acc_id=1, phone="+71111111111", is_active=True)
        mock_db.get_accounts = AsyncMock(return_value=[acc])
        mock_db.set_account_active = AsyncMock()
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["toggle_account"]({"account_id": 1})
        text = _text(result)
        assert "деактивирован" in text
        mock_db.set_account_active.assert_awaited_once_with(1, False)

    @pytest.mark.anyio
    async def test_inactive_account_gets_activated(self, mock_db):
        acc = _make_account(acc_id=2, phone="+72222222222", is_active=False)
        mock_db.get_accounts = AsyncMock(return_value=[acc])
        mock_db.set_account_active = AsyncMock()
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["toggle_account"]({"account_id": 2})
        text = _text(result)
        assert "активирован" in text
        mock_db.set_account_active.assert_awaited_once_with(2, True)

    @pytest.mark.anyio
    async def test_error_returns_text(self, mock_db):
        mock_db.get_accounts = AsyncMock(side_effect=Exception("db fail"))
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["toggle_account"]({"account_id": 1})
        assert "Ошибка переключения аккаунта" in _text(result)


class TestDeleteAccountTool:
    @pytest.mark.anyio
    async def test_missing_account_id_returns_error(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["delete_account"]({})
        assert "account_id обязателен" in _text(result)

    @pytest.mark.anyio
    async def test_no_confirm_returns_gate(self, mock_db):
        mock_db.get_accounts = AsyncMock(return_value=[])
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["delete_account"]({"account_id": 1})
        assert "confirm=true" in _text(result)

    @pytest.mark.anyio
    async def test_with_confirm_deletes_account(self, mock_db):
        acc = _make_account(acc_id=5, phone="+75555555555")
        mock_db.get_accounts = AsyncMock(return_value=[acc])
        mock_db.delete_account = AsyncMock()
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["delete_account"]({"account_id": 5, "confirm": True})
        assert "удалён" in _text(result)
        mock_db.delete_account.assert_awaited_once_with(5)

    @pytest.mark.anyio
    async def test_error_returns_text(self, mock_db):
        mock_db.get_accounts = AsyncMock(return_value=[])
        mock_db.delete_account = AsyncMock(side_effect=Exception("constraint"))
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["delete_account"]({"account_id": 1, "confirm": True})
        assert "Ошибка удаления аккаунта" in _text(result)


class TestGetFloodStatusTool:
    @pytest.mark.anyio
    async def test_no_accounts_returns_not_found(self, mock_db):
        mock_db.get_accounts = AsyncMock(return_value=[])
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["get_flood_status"]({})
        assert "Аккаунты не найдены" in _text(result)

    @pytest.mark.anyio
    async def test_no_flood_shows_no_restrictions(self, mock_db):
        acc = _make_account(phone="+71234567890", flood_wait_until=None)
        mock_db.get_accounts = AsyncMock(return_value=[acc])
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["get_flood_status"]({})
        assert "нет ограничений" in _text(result)

    @pytest.mark.anyio
    async def test_flood_wait_until_shown(self, mock_db):
        acc = _make_account(phone="+70001112233", flood_wait_until="2030-06-01 10:00")
        mock_db.get_accounts = AsyncMock(return_value=[acc])
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["get_flood_status"]({})
        assert "заблокирован" in _text(result)
        assert "2030-06-01 10:00" in _text(result)

    @pytest.mark.anyio
    async def test_expired_flood_status_shows_no_restrictions(self, mock_db):
        past = datetime.now(timezone.utc) - timedelta(minutes=5)
        acc = _make_account(phone="+70001112233", flood_wait_until=past)
        mock_db.get_accounts = AsyncMock(return_value=[acc])
        mock_db.update_account_flood = AsyncMock()
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["get_flood_status"]({})
        text = _text(result)
        assert "нет ограничений" in text
        assert "заблокирован" not in text
        mock_db.update_account_flood.assert_awaited_once_with("+70001112233", None)

    @pytest.mark.anyio
    async def test_error_returns_text(self, mock_db):
        mock_db.get_accounts = AsyncMock(side_effect=RuntimeError("oops"))
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["get_flood_status"]({})
        assert "Ошибка получения flood-статуса" in _text(result)


class TestGetAccountInfoTool:
    @pytest.mark.anyio
    async def test_snapshot_runtime_is_explicitly_unavailable(self, mock_db):
        class SnapshotClientPool:
            clients = {"+71112223344": object()}

        handlers = _get_tool_handlers(mock_db, client_pool=SnapshotClientPool())
        result = await handlers["get_account_info"]({})
        text = _text(result)
        assert "live Telegram runtime unavailable" in text
        assert "worker snapshot видит подключенные телефоны" in text
        assert "Web snapshot runtime" in text
        assert "+71112223344" in text
        lowered = text.lower()
        assert "disabled" not in lowered
        assert "not connected" not in lowered
        assert "sms" not in lowered
        assert "2fa" not in lowered

    @pytest.mark.anyio
    async def test_exact_phone_filter_annotations(self, mock_db):
        pool = MagicMock()
        pool.get_users_info = AsyncMock(return_value=[
            SimpleNamespace(
                phone="+71112223344",
                first_name="Live",
                last_name="User",
                username="liveuser",
                is_premium=True,
            ),
            SimpleNamespace(
                phone="+75556667788",
                first_name="Other",
                last_name="",
                username=None,
                is_premium=False,
            ),
        ])
        mock_db.get_accounts = AsyncMock(return_value=[
            _make_account(phone="+71112223344", is_active=True, is_primary=True),
            _make_account(phone="+75556667788", is_active=False, is_primary=False),
        ])

        handlers = _get_tool_handlers(mock_db, client_pool=pool)
        result = await handlers["get_account_info"]({"phone": "71112223344"})
        text = _text(result)
        assert "+71112223344" in text
        assert "+75556667788" not in text
        assert "Live User" in text
        assert "premium=да" in text
        assert "db_active=да" in text
        assert "db_primary=да" in text
        assert "session-present=да" in text


class TestClearFloodStatusTool:
    @pytest.mark.anyio
    async def test_no_confirm_returns_gate(self, mock_db):
        mock_db.get_accounts = AsyncMock(return_value=[_make_account(phone="+71111111111")])
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["clear_flood_status"]({"phone": "+71111111111"})
        assert "confirm=true" in _text(result)

    @pytest.mark.anyio
    async def test_account_not_found_returns_error(self, mock_db):
        mock_db.get_accounts = AsyncMock(return_value=[])
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["clear_flood_status"]({"phone": "+79999999999", "confirm": True})
        assert "не найден" in _text(result)

    @pytest.mark.anyio
    async def test_with_confirm_clears_flood(self, mock_db):
        acc = _make_account(phone="+71111111111", flood_wait_until="2025-01-01")
        mock_db.get_accounts = AsyncMock(return_value=[acc])
        mock_db.update_account_flood = AsyncMock()
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["clear_flood_status"]({"phone": "+71111111111", "confirm": True})
        assert "сброшен" in _text(result)
        mock_db.update_account_flood.assert_awaited_once_with("+71111111111", None)
