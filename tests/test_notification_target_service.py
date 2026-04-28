"""Tests for NotificationTargetService."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock

import pytest

from src.models import Account
from src.services.notification_target_service import NotificationTargetService


def _make_account(
    phone="+70001112233",
    is_active=True,
    is_primary=True,
    flood_wait_until=None,
) -> Account:
    return Account(
        phone=phone,
        session_string="sess",
        is_active=is_active,
        is_primary=is_primary,
        flood_wait_until=flood_wait_until,
    )


def _make_service(accounts=None, clients=None, configured_phone=None):
    notifications = MagicMock()
    notifications.list_accounts = AsyncMock(return_value=accounts or [])
    notifications.get_setting = AsyncMock(return_value=configured_phone or "")
    notifications.set_setting = AsyncMock()

    pool = MagicMock()
    pool.clients = clients if clients is not None else {}
    pool.get_native_client_by_phone = AsyncMock(return_value=(MagicMock(), "phone"))
    pool.release_client = AsyncMock()

    svc = NotificationTargetService(notifications, pool)
    return svc, notifications


@pytest.mark.anyio
async def test_get_configured_phone_set():
    svc, notifications = _make_service(configured_phone="+70001112233")
    result = await svc.get_configured_phone()
    assert result == "+70001112233"


@pytest.mark.anyio
async def test_get_configured_phone_empty():
    svc, _ = _make_service(configured_phone="")
    result = await svc.get_configured_phone()
    assert result is None


@pytest.mark.anyio
async def test_set_configured_phone():
    svc, notifications = _make_service()
    await svc.set_configured_phone("+70001112233")
    notifications.set_setting.assert_called_once()


@pytest.mark.anyio
async def test_describe_target_selected_account_available():
    acc = _make_account(phone="+70001112233", is_primary=False)
    svc, _ = _make_service(
        accounts=[acc],
        clients={"+70001112233": MagicMock()},
        configured_phone="+70001112233",
    )
    status = await svc.describe_target()
    assert status.mode == "selected"
    assert status.state == "available"
    assert status.effective_phone == "+70001112233"


@pytest.mark.anyio
async def test_describe_target_selected_account_missing():
    svc, _ = _make_service(
        accounts=[],
        configured_phone="+70001112233",
    )
    status = await svc.describe_target()
    assert status.mode == "selected"
    assert status.state == "missing"


@pytest.mark.anyio
async def test_describe_target_selected_account_inactive():
    acc = _make_account(phone="+70001112233", is_active=False)
    svc, _ = _make_service(
        accounts=[acc],
        configured_phone="+70001112233",
    )
    status = await svc.describe_target()
    assert status.state == "inactive"


@pytest.mark.anyio
async def test_describe_target_selected_account_flood_wait():
    until = datetime.now(timezone.utc) + timedelta(seconds=300)
    acc = _make_account(phone="+70001112233", flood_wait_until=until)
    svc, _ = _make_service(
        accounts=[acc],
        configured_phone="+70001112233",
    )
    status = await svc.describe_target()
    assert status.state == "flood_wait"


@pytest.mark.anyio
async def test_describe_target_selected_account_disconnected():
    acc = _make_account(phone="+70001112233")
    svc, _ = _make_service(
        accounts=[acc],
        clients={},
        configured_phone="+70001112233",
    )
    status = await svc.describe_target()
    assert status.state == "disconnected"


@pytest.mark.anyio
async def test_describe_target_primary_fallback():
    acc = _make_account(phone="+70001112233", is_primary=True)
    svc, _ = _make_service(
        accounts=[acc],
        clients={"+70001112233": MagicMock()},
    )
    status = await svc.describe_target()
    assert status.mode == "primary"
    assert status.state == "available"


@pytest.mark.anyio
async def test_describe_target_primary_missing():
    svc, _ = _make_service(accounts=[], clients={})
    status = await svc.describe_target()
    assert status.mode == "primary"
    assert status.state == "missing"


@pytest.mark.anyio
async def test_use_client_success():
    acc = _make_account(phone="+70001112233")
    svc, _ = _make_service(
        accounts=[acc],
        clients={"+70001112233": MagicMock()},
    )
    mock_client = MagicMock()
    svc._pool.get_native_client_by_phone = AsyncMock(
        return_value=(mock_client, "+70001112233")
    )

    async with svc.use_client() as (client, phone):
        assert client == mock_client
        assert phone == "+70001112233"

    svc._pool.release_client.assert_called_once_with("+70001112233")


@pytest.mark.anyio
async def test_use_client_not_available():
    svc, _ = _make_service(accounts=[], clients={})
    with pytest.raises(RuntimeError):
        async with svc.use_client():
            pass
