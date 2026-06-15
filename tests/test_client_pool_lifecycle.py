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
async def test_release_client_releases_native_lease_on_top():
    """When a native (disconnect_on_release) lease is the most recently acquired (on top),
    LIFO release tears it down promptly — the original #838/8 leak target."""
    pool = _bare_pool()
    direct = SimpleNamespace(disconnect_on_release=False, name="direct")
    native = SimpleNamespace(disconnect_on_release=True, name="native")
    pool._active_leases = {"+7": [direct, native]}  # native on top

    await pool.release_client("+7")

    pool._backend_router.release.assert_awaited_once_with(native)
    assert pool._active_leases["+7"] == [direct]
    pool._lease_pool.release.assert_not_awaited()


@pytest.mark.anyio
async def test_release_client_does_not_tear_down_native_lease_under_a_live_direct_lease():
    """Regression (#868 review): release_client takes only a phone, not a lease handle.
    With [native, direct] (native acquired first and STILL in use, direct on top), releasing
    must pop the TOP (direct, LIFO) and must NOT reach past it to backend-release the native
    lease that another caller is still using — that would close that session mid-operation."""
    pool = _bare_pool()
    native = SimpleNamespace(disconnect_on_release=True, name="native")
    direct = SimpleNamespace(disconnect_on_release=False, name="direct")
    pool._active_leases = {"+7": [native, direct]}  # native buried under a live direct lease

    await pool.release_client("+7")

    # Direct (top) is popped; the in-use native lease is untouched.
    pool._backend_router.release.assert_not_awaited()
    assert pool._active_leases["+7"] == [native]
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


@pytest.mark.anyio
async def test_stats_taskgroup_unwraps_database_busy_error():
    """Regression (#868 review): handle_stats_all wraps its TaskGroup in
    `except* DatabaseBusyError` and re-raises a PLAIN DatabaseBusyError, so the
    dispatcher's `except DatabaseBusyError` requeue path still fires on a transient lock
    instead of the ExceptionGroup falling through to `except Exception` -> permanent FAILED.
    This asserts the unwrap contract the fix relies on."""
    from src.database import DatabaseBusyError

    async def _busy_worker():
        raise DatabaseBusyError("database is locked")

    # Mirror the exact construct used in stats.handle_stats_all.
    with pytest.raises(DatabaseBusyError) as excinfo:
        try:
            async with asyncio.TaskGroup() as tg:
                tg.create_task(_busy_worker())
        except* DatabaseBusyError as eg:
            raise DatabaseBusyError(str(eg.exceptions[0])) from eg.exceptions[0]

    # A PLAIN DatabaseBusyError escapes (not a BaseExceptionGroup), so the dispatcher's
    # `except DatabaseBusyError` matches it.
    assert isinstance(excinfo.value, DatabaseBusyError)
    assert not isinstance(excinfo.value, BaseExceptionGroup)
