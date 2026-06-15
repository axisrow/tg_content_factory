"""Tests for ClientPool release/shutdown lifecycle (audit #838/8, #836/11)."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from src.telegram.client_pool import ClientPool


def _bare_pool() -> ClientPool:
    pool = ClientPool.__new__(ClientPool)
    pool._lock = asyncio.Lock()
    pool._active_leases = {}
    pool._lease_pool = MagicMock()
    pool._lease_pool.release = AsyncMock()
    pool._backend_router = MagicMock()
    pool._backend_router.release = AsyncMock()
    return pool


@pytest.mark.anyio
async def test_release_client_prefers_disconnect_on_release_lease():
    """A mixed stack must release the ephemeral native lease first so it is torn
    down promptly, not left until an unrelated caller releases (audit #838/8)."""
    pool = _bare_pool()
    direct = SimpleNamespace(disconnect_on_release=False, name="direct")
    native = SimpleNamespace(disconnect_on_release=True, name="native")
    pool._active_leases = {"+7": [direct, native]}

    await pool.release_client("+7")

    pool._backend_router.release.assert_awaited_once_with(native)
    # The direct lease remains, so the phone is not fully released yet.
    assert pool._active_leases["+7"] == [direct]
    pool._lease_pool.release.assert_not_awaited()


@pytest.mark.anyio
async def test_release_client_falls_back_to_lifo_without_native_lease():
    pool = _bare_pool()
    a = SimpleNamespace(disconnect_on_release=False)
    b = SimpleNamespace(disconnect_on_release=False)
    pool._active_leases = {"+7": [a, b]}

    await pool.release_client("+7")

    # No disconnect-on-release lease -> pop the most recent (b).
    assert pool._active_leases["+7"] == [a]
    pool._backend_router.release.assert_not_awaited()


@pytest.mark.anyio
async def test_disconnect_all_cancels_background_tasks():
    """Warm/refresh background tasks must be cancelled on teardown (audit #836/11)."""
    pool = ClientPool.__new__(ClientPool)
    pool.clients = {}
    pool._in_use = set()
    pool._active_leases = {}
    pool._dialogs_fetched = set()
    pool._mtproto_watchdog = None

    warm = asyncio.create_task(asyncio.sleep(100))
    refresh = asyncio.create_task(asyncio.sleep(100))
    pool._warming_task = warm
    pool._dialog_refresh_tasks = {("+7", "collect"): refresh}

    await pool.disconnect_all()

    assert warm.cancelled()
    assert refresh.cancelled()
    assert pool._dialog_refresh_tasks == {}
