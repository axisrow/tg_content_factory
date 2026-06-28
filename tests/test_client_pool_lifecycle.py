"""Tests for ClientPool release/shutdown lifecycle (audit #838/8, #836/11)."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock

import pytest

from src.models import Account
from src.telegram.account_lease_pool import AccountLease
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


def _pool_with_in_use(in_use: set[str]) -> ClientPool:
    """A bare pool whose _in_use is a real, observable set (shared with the
    lease pool in production). The #1181 race is about the visibility of this
    set's exclusive marker, so the tests must poke the real object."""
    pool = ClientPool.__new__(ClientPool)
    pool._lock = asyncio.Lock()
    pool._in_use = in_use
    pool._active_leases = {}
    pool._lease_pool = MagicMock()
    pool._lease_pool.release = AsyncMock()
    pool._backend_router = MagicMock()
    pool._backend_router.release = AsyncMock()
    return pool


def _pool_with_lifecycle_state(in_use: set[str]) -> ClientPool:
    pool = _pool_with_in_use(in_use)
    pool.clients = {}
    pool._session_overrides = {}
    pool._dialogs_fetched = set()
    pool._dialogs_fetched_at_monotonic = {}
    pool._dialogs_cache = {}
    pool._premium_flood_wait_until = {}
    pool._mtproto_watchdog = None
    return pool


@pytest.mark.anyio
async def test_release_client_discards_in_use_under_client_pool_lock():
    """#1181 regression: the exclusive _in_use marker must be discarded in the
    SAME ClientPool._lock critical section that pops the _active_leases stack.
    Previously release_client released ClientPool._lock, THEN called
    _lease_pool.release (which takes AccountLeasePool._lock to discard) — an
    await window in which a concurrent _acquire_phone_lease could observe the
    stale marker, grab a shared lease, and let a later caller take exclusive
    after the discard: shared+exclusive on one session.

    Pins the fix by asserting ClientPool._lock is still held at the moment
    _lease_pool.release runs (no window), and that the pop has already emptied
    the stack by then (pop -> discard ordering under one lock)."""
    pool = _pool_with_in_use(in_use={"+7"})
    lease = SimpleNamespace(disconnect_on_release=False)
    pool._active_leases["+7"] = [lease]

    observed: dict[str, object] = {}

    async def _spy_release(phone):
        # _lease_pool.release performs _in_use.discard under AccountLeasePool._lock.
        # For the race window to be closed it must ALSO run under ClientPool._lock.
        observed["client_lock_held"] = pool._lock.locked()
        observed["active_leases_snapshot"] = {k: list(v) for k, v in pool._active_leases.items()}
        observed["in_use_snapshot"] = set(pool._in_use)
        # Mirror the real AccountLeasePool.release: drop the marker here.
        pool._in_use.discard(phone)

    pool._lease_pool.release = _spy_release

    await pool.release_client("+7")

    # The discard ran while ClientPool._lock was held (old bug: False here).
    assert observed["client_lock_held"] is True, (
        "_in_use.discard ran outside ClientPool._lock — pop/discard window reopened (#1181)"
    )
    # The pop already emptied the stack before the discard ran (no reordering).
    assert observed["active_leases_snapshot"] == {}
    # _in_use still holds the marker at the instant release is entered (discard
    # happens inside _lease_pool.release, after this snapshot).
    assert observed["in_use_snapshot"] == {"+7"}
    # After release_client returns, both the stack and the marker are gone.
    assert "+7" not in pool._active_leases
    assert "+7" not in pool._in_use


@pytest.mark.anyio
async def test_release_client_skips_discard_under_lock_when_stack_remains():
    """#1181: when the popped lease leaves remaining leases on the stack, the
    phone is still exclusively held, so _in_use must NOT be discarded — and the
    decision still happens under ClientPool._lock (atomic with the pop)."""
    pool = _pool_with_in_use(in_use={"+7"})
    kept = SimpleNamespace(disconnect_on_release=False)
    top = SimpleNamespace(disconnect_on_release=False)
    pool._active_leases["+7"] = [kept, top]

    await pool.release_client("+7")

    pool._lease_pool.release.assert_not_awaited()
    assert pool._active_leases["+7"] == [kept]
    assert pool._in_use == {"+7"}, "marker must survive while leases remain on the stack"


@pytest.mark.anyio
async def test_release_client_no_await_window_between_pop_and_discard():
    """#1181 contract (the issue's minimum bar): there is no scheduling point
    between popping _active_leases and discarding the _in_use marker. Spies
    _lease_pool.release (the call that owns the discard under
    AccountLeasePool._lock in production) and records ClientPool._lock state at
    the exact moment of discard. With the fix the only await between pop and
    discard is _lease_pool.release itself, executed while ClientPool._lock is
    held — so no concurrent acquirer can interleave.

    Regression guard: on the pre-fix code _lease_pool.release ran AFTER
    ClientPool._lock was released, so the snapshot here would catch the lock
    FREE mid-discard."""
    in_use: set[str] = {"+7"}
    pool = _pool_with_in_use(in_use=in_use)
    lease = SimpleNamespace(disconnect_on_release=False)
    pool._active_leases["+7"] = [lease]

    # Stand in for AccountLeasePool.release: it owns the discard under its own
    # lock; we record the ClientPool._lock state at the exact moment of discard.
    timeline: list[tuple[str, bool]] = []

    async def _discard_under_lease_lock(phone):
        timeline.append(("discard", pool._lock.locked()))
        # Mirror the real AccountLeasePool.release: drop the marker here.
        pool._in_use.discard(phone)

    pool._lease_pool.release = _discard_under_lease_lock

    await pool.release_client("+7")

    assert timeline == [("discard", True)], (
        "_in_use.discard did not run under ClientPool._lock — the pop/discard "
        "window is open, allowing shared+exclusive on one session (#1181)"
    )
    assert in_use == set()
    assert "+7" not in pool._active_leases


@pytest.mark.anyio
async def test_remove_client_releases_in_use_under_client_pool_lock():
    """#1191: remove_client must release the shared _in_use marker through
    AccountLeasePool.release while ClientPool._lock still protects the local
    teardown state. On the old code this spy was never called because
    remove_client did a bare _in_use.discard outside both locks."""
    pool = _pool_with_lifecycle_state(in_use={"+7"})
    lease = SimpleNamespace(disconnect_on_release=False)
    pool.clients["+7"] = object()
    pool._active_leases["+7"] = [lease]

    observed: dict[str, object] = {}

    async def _spy_release(phone):
        observed["client_lock_held"] = pool._lock.locked()
        observed["active_leases_snapshot"] = {k: list(v) for k, v in pool._active_leases.items()}
        observed["client_present_snapshot"] = phone in pool.clients
        observed["in_use_snapshot"] = set(pool._in_use)
        pool._in_use.discard(phone)

    pool._lease_pool.release = _spy_release

    await pool.remove_client("+7")

    assert observed.get("client_lock_held") is True, (
        "remove_client did not release _in_use through AccountLeasePool.release "
        "while holding ClientPool._lock (#1191)"
    )
    assert observed["active_leases_snapshot"] == {}
    assert observed["client_present_snapshot"] is False
    assert observed["in_use_snapshot"] == {"+7"}
    assert "+7" not in pool.clients
    assert "+7" not in pool._active_leases
    assert "+7" not in pool._in_use


@pytest.mark.anyio
async def test_disconnect_all_timeout_releases_in_use_under_client_pool_lock():
    """#1191: the forced timeout cleanup path must use AccountLeasePool.release
    under ClientPool._lock, not a bare _in_use.discard."""
    pool = _pool_with_lifecycle_state(in_use={"+7"})
    pool.clients["+7"] = object()
    pool._active_leases["+7"] = [SimpleNamespace(disconnect_on_release=False)]

    async def _timeout_remove(phone):
        raise asyncio.TimeoutError

    pool.remove_client = _timeout_remove  # type: ignore[method-assign]

    observed: dict[str, object] = {}

    async def _spy_release(phone):
        observed["client_lock_held"] = pool._lock.locked()
        observed["active_leases_snapshot"] = {k: list(v) for k, v in pool._active_leases.items()}
        observed["client_present_snapshot"] = phone in pool.clients
        observed["in_use_snapshot"] = set(pool._in_use)
        pool._in_use.discard(phone)

    pool._lease_pool.release = _spy_release

    await pool.disconnect_all()

    assert observed.get("client_lock_held") is True, (
        "disconnect_all timeout cleanup did not release _in_use through "
        "AccountLeasePool.release while holding ClientPool._lock (#1191)"
    )
    assert observed["active_leases_snapshot"] == {}
    assert observed["client_present_snapshot"] is False
    assert observed["in_use_snapshot"] == {"+7"}
    assert "+7" not in pool.clients
    assert "+7" not in pool._active_leases
    assert "+7" not in pool._in_use


@pytest.mark.anyio
async def test_acquire_from_lease_failure_releases_in_use_under_client_pool_lock():
    """#1191: when a non-shared lease fails before becoming active, the release
    must still happen inside ClientPool._lock. On the old code the spy saw the
    release with ClientPool._lock free."""
    pool = _pool_with_in_use(in_use={"+7"})
    pool.clients = {}
    pool._backend_router.acquire_client = AsyncMock(side_effect=RuntimeError("boom"))
    account_lease = AccountLease(
        account=Account(phone="+7", session_string="session", is_active=True),
        shared=False,
    )

    observed: dict[str, object] = {}

    async def _spy_release(phone):
        observed["client_lock_held"] = pool._lock.locked()
        observed["in_use_snapshot"] = set(pool._in_use)
        pool._in_use.discard(phone)

    pool._lease_pool.release = _spy_release

    result = await pool._acquire_from_lease(account_lease)

    assert result is None
    assert observed.get("client_lock_held") is True, (
        "_acquire_from_lease failure release did not run under ClientPool._lock (#1191)"
    )
    assert observed["in_use_snapshot"] == {"+7"}
    assert "+7" not in pool._in_use


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
