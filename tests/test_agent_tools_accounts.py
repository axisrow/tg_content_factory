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
    async def test_with_confirm_clears_stale_flood(self, mock_db):
        # Past timestamp → flood already expired → safe to clear.
        acc = _make_account(phone="+71111111111", flood_wait_until="2025-01-01")
        mock_db.get_accounts = AsyncMock(return_value=[acc])
        mock_db.update_account_flood = AsyncMock()
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["clear_flood_status"]({"phone": "+71111111111", "confirm": True})
        assert "сброшен" in _text(result)
        mock_db.update_account_flood.assert_awaited_once_with("+71111111111", None)

    @pytest.mark.anyio
    async def test_refuses_to_clear_active_flood(self, mock_db):
        # #597: an active (future) flood wait is Telegram-mandated — the agent
        # must NOT be able to clear it as a retry hack.
        future = datetime.now(timezone.utc) + timedelta(hours=2)
        acc = _make_account(phone="+71111111111", flood_wait_until=future)
        mock_db.get_accounts = AsyncMock(return_value=[acc])
        mock_db.update_account_flood = AsyncMock()
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["clear_flood_status"]({"phone": "+71111111111", "confirm": True})
        text = _text(result)
        assert "Отклонено" in text
        assert "Telegram" in text
        mock_db.update_account_flood.assert_not_awaited()


def _avail_account(phone, *, is_active=True, flood_wait_until=None, session_status=None):
    from src.models import AccountSessionStatus

    return SimpleNamespace(
        phone=phone,
        is_active=is_active,
        flood_wait_until=flood_wait_until,
        session_status=session_status or AccountSessionStatus.OK,
        is_primary=False,
        id=1,
    )


def _pool_with(*connected_phones):
    return SimpleNamespace(clients={p: object() for p in connected_phones})


class TestGetAccountAvailabilityTool:
    """#529: agent availability must match the Settings UI and distinguish a
    saved-session reconnect from interactive Telegram login."""

    @pytest.mark.anyio
    async def test_available_account_reports_ok(self, mock_db):
        acc = _avail_account("+8613000000000")
        mock_db.get_accounts = AsyncMock(return_value=[acc])
        mock_db.update_account_flood = AsyncMock()
        handlers = _get_tool_handlers(mock_db, client_pool=_pool_with("+8613000000000"))
        text = _text(await handlers["get_account_availability"]({"phone": "+8613000000000"}))
        assert "available" in text
        assert "OK" in text
        # An available account must NOT be described as unavailable / needing re-auth.
        assert "session_unavailable" not in text
        assert "SMS" not in text

    @pytest.mark.anyio
    async def test_session_unavailable_reports_interactive_login(self, mock_db):
        from src.models import AccountSessionStatus

        acc = _avail_account(
            "+8613000000000", session_status=AccountSessionStatus.DECRYPT_FAILED
        )
        mock_db.get_accounts = AsyncMock(return_value=[acc])
        mock_db.update_account_flood = AsyncMock()
        handlers = _get_tool_handlers(mock_db, client_pool=_pool_with("+8613000000000"))
        text = _text(await handlers["get_account_availability"]({}))
        assert "session_unavailable" in text
        assert "decrypt_failed" in text
        assert "SMS" in text and "/auth/login" in text

    @pytest.mark.anyio
    async def test_disconnected_saved_session_is_reconnect_not_reauth(self, mock_db):
        # session ok, active, but not in the pool → reconnect saved session.
        acc = _avail_account("+8613000000000")
        mock_db.get_accounts = AsyncMock(return_value=[acc])
        mock_db.update_account_flood = AsyncMock()
        handlers = _get_tool_handlers(mock_db, client_pool=_pool_with())  # nothing connected
        text = _text(await handlers["get_account_availability"]({}))
        assert "disconnected" in text
        assert "reconnect" in text.lower()
        # The guidance must explicitly state SMS/2FA is NOT required for a
        # saved-session reconnect (the bug was conflating it with re-auth).
        assert "НЕ требуется" in text
        assert "/auth/login" not in text

    @pytest.mark.anyio
    async def test_flood_reports_actual_reason(self, mock_db):
        future = datetime.now(timezone.utc) + timedelta(minutes=10)
        acc = _avail_account("+8613000000000", flood_wait_until=future)
        mock_db.get_accounts = AsyncMock(return_value=[acc])
        mock_db.update_account_flood = AsyncMock()
        handlers = _get_tool_handlers(mock_db, client_pool=_pool_with("+8613000000000"))
        text = _text(await handlers["get_account_availability"]({}))
        assert "flood" in text
        assert "осталось" in text


class SnapshotClientPool:  # noqa: N801 — exact name drives detect_runtime_kind
    """Stand-in whose class name triggers runtime_kind == 'snapshot'."""

    __test__ = False

    def __init__(self, *connected):
        self.clients = {p: object() for p in connected}


class TestGetRuntimeDiagnosticsTool:
    """#530: grounded runtime diagnostics — live pool kept separate from DB flags,
    snapshot freshness reported only as snapshot health."""

    @pytest.mark.anyio
    async def test_live_runtime_omits_snapshot_health(self, mock_db):
        mock_db.get_accounts = AsyncMock(return_value=[_avail_account("+8613000000000")])
        mock_db.update_account_flood = AsyncMock()
        handlers = _get_tool_handlers(mock_db, client_pool=_pool_with("+8613000000000"))
        text = _text(await handlers["get_runtime_diagnostics"]({}))
        assert "runtime_kind: live" in text
        assert "+8613000000000" in text
        # Live runtime proves connectivity directly — no snapshot-health line.
        assert "снапшота воркера" not in text
        assert "get_account_availability" in text

    @pytest.mark.anyio
    async def test_snapshot_runtime_warns_on_stale_heartbeat(self, mock_db):
        stale = SimpleNamespace(
            updated_at=datetime.now(timezone.utc) - timedelta(minutes=10),
            payload={"status": "alive"},
        )
        mock_db.get_accounts = AsyncMock(return_value=[])
        mock_db.update_account_flood = AsyncMock()
        mock_db.repos.runtime_snapshots.get_snapshot = AsyncMock(return_value=stale)
        handlers = _get_tool_handlers(mock_db, client_pool=SnapshotClientPool())
        text = _text(await handlers["get_runtime_diagnostics"]({}))
        assert "snapshot" in text
        assert "устаревший" in text
        assert "Не делайте выводов" in text

    @pytest.mark.anyio
    async def test_none_runtime_reported(self, mock_db):
        mock_db.get_accounts = AsyncMock(return_value=[])
        mock_db.update_account_flood = AsyncMock()
        mock_db.repos.runtime_snapshots.get_snapshot = AsyncMock(return_value=None)
        handlers = _get_tool_handlers(mock_db, client_pool=None)
        text = _text(await handlers["get_runtime_diagnostics"]({}))
        assert "runtime_kind: none" in text
        assert "get_account_availability" in text
