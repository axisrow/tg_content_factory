from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from telethon.errors import FloodWaitError

from src.telegram.client_pool import ClientPool
from src.telegram.flood_wait import HandledFloodWaitError
from src.telegram.rate_limiter import ResolveRateLimiter, UsernameResolveRateLimitedError
from src.telegram.resolve_guard import (
    RESOLVE_BACKOFF_BY_PHONE_SETTING,
    RESOLVE_BACKOFF_LEGACY_SETTING,
    ResolveGuardMixin,
)


@pytest.mark.anyio
async def test_live_username_resolve_uses_shared_rate_limiter():
    pool = ClientPool.__new__(ClientPool)
    pool.report_flood = AsyncMock()
    pool._resolve_rate_limiter = ResolveRateLimiter(
        max_calls=1,
        window_sec=60.0,
        jitter_sec=0.0,
    )
    assert pool._resolve_rate_limiter.try_acquire("+7001") == 0.0

    resolver = AsyncMock(return_value=object())

    with pytest.raises(UsernameResolveRateLimitedError) as exc_info:
        await pool.run_live_username_resolve(
            lambda: resolver("guarded"),
            phone="+7001",
            username="guarded",
            operation="test_live_username_resolve",
        )

    assert exc_info.value.phone == "+7001"
    assert 0 < exc_info.value.retry_after_sec <= 60
    resolver.assert_not_called()
    pool.report_flood.assert_not_awaited()


@pytest.mark.anyio
async def test_live_username_resolve_records_per_phone_long_flood_backoff():
    """#790: a long flood on one phone freezes only that phone, not the pool."""
    pool = ClientPool.__new__(ClientPool)
    pool.report_flood = AsyncMock()
    pool._db = SimpleNamespace(set_setting=AsyncMock())
    pool.clients = {"+7001": object(), "+7002": object()}
    pool._resolve_rate_limiter = ResolveRateLimiter(jitter_sec=0.0)

    async def _flood():
        raise FloodWaitError(request=None, capture=7200)

    with pytest.raises(HandledFloodWaitError):
        await pool.run_live_username_resolve(
            _flood,
            phone="+7001",
            username="long_flood",
            operation="test_live_username_resolve",
        )

    pool.report_flood.assert_awaited_once_with("+7001", 7200)
    remaining_a = pool.get_resolve_username_backoff_remaining_sec("+7001")
    assert 7100 < remaining_a <= 7200
    # The other connected phone is untouched...
    assert pool.get_resolve_username_backoff_remaining_sec("+7002") == 0
    # ...so the pool-level aggregate reports "a free phone exists".
    assert pool.get_resolve_username_backoff_remaining_sec() == 0
    pool._db.set_setting.assert_awaited_once()
    key, value = pool._db.set_setting.await_args.args
    assert key == RESOLVE_BACKOFF_BY_PHONE_SETTING
    stored = json.loads(value)
    assert set(stored) == {"+7001"}
    assert datetime.fromisoformat(stored["+7001"]) == pool.get_resolve_username_backoff_until(
        "+7001"
    )


def _make_pool(clients: dict | None = None):
    class FakePool(ResolveGuardMixin):
        def __init__(self):
            self._resolve_rate_limiter = None
            self._resolve_username_backoff_until_utc = {}
            self._resolve_ramp_up_until_utc = {}
            self._resolve_ramp_up_last_call_utc = {}
            self._resolve_ramp_up_min_interval_sec = 5.0
            self.clients = clients or {}

    return FakePool()


class TestPerPhoneBackoffIsolation:
    def test_backoff_on_one_phone_does_not_block_other(self):
        pool = _make_pool(clients={"+7001": object(), "+7002": object()})
        pool.set_resolve_username_backoff(600, phone="+7001")
        assert pool.get_resolve_username_backoff_remaining_sec("+7001") > 500
        assert pool.get_resolve_username_backoff_remaining_sec("+7002") == 0
        # reserve on the free phone passes the backoff layer entirely
        assert pool.reserve_resolve_username_call("+7002") == 0.0
        # reserve on the backoff phone is deferred for its own remaining window
        assert pool.reserve_resolve_username_call("+7001") > 500

    def test_until_is_per_phone(self):
        pool = _make_pool(clients={"+7001": object(), "+7002": object()})
        deadline = pool.set_resolve_username_backoff(600, phone="+7001")
        assert pool.get_resolve_username_backoff_until("+7001") == deadline
        assert pool.get_resolve_username_backoff_until("+7002") is None


class TestBackoffNeverShortens:
    def test_keeps_longer_backoff_same_phone(self):
        pool = _make_pool()
        first = pool.set_resolve_username_backoff(10000, phone="+7001")
        second = pool.set_resolve_username_backoff(100, phone="+7001")
        assert second == first
        assert pool.get_resolve_username_backoff_remaining_sec("+7001") > 9000

    def test_replaces_shorter_backoff_same_phone(self):
        pool = _make_pool()
        pool.set_resolve_username_backoff(100, phone="+7001")
        pool.set_resolve_username_backoff(10000, phone="+7001")
        assert pool.get_resolve_username_backoff_remaining_sec("+7001") > 9000

    def test_other_phone_unaffected_by_never_shorten(self):
        pool = _make_pool()
        pool.set_resolve_username_backoff(10000, phone="+7001")
        pool.set_resolve_username_backoff(100, phone="+7002")
        assert pool.get_resolve_username_backoff_remaining_sec("+7002") <= 100


class TestAggregateBackoff:
    def test_aggregate_zero_when_one_connected_phone_free(self):
        pool = _make_pool(clients={"+7001": object(), "+7002": object()})
        pool.set_resolve_username_backoff(600, phone="+7001")
        assert pool.get_resolve_username_backoff_remaining_sec() == 0
        assert pool.get_resolve_username_backoff_until() is None

    def test_aggregate_min_when_all_connected_phones_blocked(self):
        pool = _make_pool(clients={"+7001": object(), "+7002": object()})
        pool.set_resolve_username_backoff(600, phone="+7001")
        until_b = pool.set_resolve_username_backoff(300, phone="+7002")
        remaining = pool.get_resolve_username_backoff_remaining_sec()
        assert 200 < remaining <= 300
        assert pool.get_resolve_username_backoff_until() == until_b

    def test_aggregate_without_clients_uses_backoff_entries(self):
        pool = _make_pool()
        pool.set_resolve_username_backoff(600, phone="+7001")
        remaining = pool.get_resolve_username_backoff_remaining_sec()
        assert 500 < remaining <= 600

    def test_aggregate_zero_when_no_backoff(self):
        pool = _make_pool(clients={"+7001": object()})
        assert pool.get_resolve_username_backoff_remaining_sec() == 0


class TestHasResolveCapablePhone:
    def test_free_phone_is_capable(self):
        pool = _make_pool(clients={"+7001": object(), "+7002": object()})
        pool.set_resolve_username_backoff(600, phone="+7001")
        assert pool.has_resolve_capable_phone() is True
        assert pool.has_resolve_capable_phone(exclude={"+7002"}) is False

    def test_all_blocked_not_capable(self):
        pool = _make_pool(clients={"+7001": object(), "+7002": object()})
        pool.set_resolve_username_backoff(600, phone="+7001")
        pool.set_resolve_username_backoff(600, phone="+7002")
        assert pool.has_resolve_capable_phone() is False

    def test_exclude_respected(self):
        pool = _make_pool(clients={"+7001": object(), "+7002": object()})
        assert pool.has_resolve_capable_phone(exclude={"+7001"}) is True
        assert pool.has_resolve_capable_phone(exclude={"+7001", "+7002"}) is False


class TestHasRotatableResolvePhone:
    """#790 F1: rotation eligibility must also reject *generic* flood-waited
    accounts, not just resolve-backoff ones — that knowledge lives in the lease
    pool (DB-backed), so the check is async and delegates the flood/in-use
    filter to ``available_exclusive_count``."""

    def _pool(self, clients, available_count):
        pool = ClientPool.__new__(ClientPool)
        pool._resolve_rate_limiter = ResolveRateLimiter()
        pool._resolve_username_backoff_until_utc = {}
        pool._resolve_ramp_up_until_utc = {}
        pool._resolve_ramp_up_last_call_utc = {}
        pool._resolve_ramp_up_min_interval_sec = 5.0
        pool.clients = clients
        # available_exclusive_count is the async lease-pool filter for generic
        # flood wait + in-use; capture the candidate set it is asked about.
        pool._lease_pool = SimpleNamespace(
            available_exclusive_count=AsyncMock(side_effect=available_count)
        )
        return pool

    @pytest.mark.anyio
    async def test_false_when_only_free_phone_is_generically_flooded(self):
        # +7001 in resolve backoff; +7002 free of backoff but the lease pool
        # reports zero available (it is generically flooded) → not rotatable.
        seen = {}

        async def _count(candidates):
            seen["candidates"] = set(candidates)
            return 0

        pool = self._pool({"+7001": object(), "+7002": object()}, _count)
        pool.set_resolve_username_backoff(600, phone="+7001")
        assert await pool.has_rotatable_resolve_phone(exclude={"+7001"}) is False
        # The backoff phone must be narrowed out before hitting the lease pool.
        assert seen["candidates"] == {"+7002"}

    @pytest.mark.anyio
    async def test_true_when_free_phone_is_available(self):
        pool = self._pool({"+7001": object(), "+7002": object()}, lambda c: 1)
        pool.set_resolve_username_backoff(600, phone="+7001")
        assert await pool.has_rotatable_resolve_phone(exclude={"+7001"}) is True

    @pytest.mark.anyio
    async def test_false_when_all_phones_in_resolve_backoff(self):
        # No candidate survives the sync backoff filter → lease pool not queried.
        count = AsyncMock(return_value=5)
        pool = self._pool({"+7001": object(), "+7002": object()}, count)
        pool.set_resolve_username_backoff(600, phone="+7001")
        pool.set_resolve_username_backoff(600, phone="+7002")
        assert await pool.has_rotatable_resolve_phone() is False
        count.assert_not_awaited()


class TestRampUpMode:
    def test_ramp_up_active_after_backoff_set(self):
        pool = _make_pool()
        pool.set_resolve_username_backoff(600, phone="+7001")
        assert pool.is_resolve_ramp_up_active("+7001")
        assert not pool.is_resolve_ramp_up_active("+7002")

    def test_ramp_up_rate_limits_only_its_phone(self):
        pool = _make_pool()
        now = datetime.now(timezone.utc)
        pool._resolve_ramp_up_until_utc["+7001"] = now + timedelta(seconds=300)
        pool._resolve_ramp_up_min_interval_sec = 5.0
        pool._resolve_ramp_up_last_call_utc["+7001"] = now
        assert pool.reserve_resolve_username_call("+7001") > 0
        # The other phone has no ramp-up and an empty limiter window.
        pool._resolve_rate_limiter = ResolveRateLimiter(jitter_sec=0.0)
        assert pool.reserve_resolve_username_call("+7002") == 0.0

    def test_ramp_up_allows_slow_calls(self):
        pool = _make_pool()
        now = datetime.now(timezone.utc)
        pool._resolve_ramp_up_until_utc["+7001"] = now + timedelta(seconds=300)
        pool._resolve_ramp_up_min_interval_sec = 0.01
        pool._resolve_ramp_up_last_call_utc["+7001"] = now - timedelta(seconds=1)
        assert pool.reserve_resolve_username_call("+7001") == 0.0


class TestPersistence:
    @pytest.mark.anyio
    async def test_persist_writes_pruned_json_map(self):
        pool = _make_pool()
        pool._db = SimpleNamespace(set_setting=AsyncMock())
        until = pool.set_resolve_username_backoff(600, phone="+7001")
        # Expired entry must be pruned out of the persisted payload.
        pool._resolve_username_backoff_until_utc["+7002"] = datetime.now(
            timezone.utc
        ) - timedelta(seconds=10)

        await pool.persist_resolve_username_backoff()

        key, value = pool._db.set_setting.await_args.args
        assert key == RESOLVE_BACKOFF_BY_PHONE_SETTING
        stored = json.loads(value)
        assert set(stored) == {"+7001"}
        assert datetime.fromisoformat(stored["+7001"]) == until

    @pytest.mark.anyio
    async def test_restore_round_trip(self):
        until = datetime.now(timezone.utc) + timedelta(seconds=900)
        payload = json.dumps({"+7001": until.isoformat()})

        async def get_setting(key):
            return payload if key == RESOLVE_BACKOFF_BY_PHONE_SETTING else None

        pool = _make_pool(clients={"+7001": object(), "+7002": object()})
        await pool.restore_resolve_username_backoff(
            SimpleNamespace(get_setting=get_setting, set_setting=AsyncMock())
        )

        assert pool.get_resolve_username_backoff_until("+7001") == until
        assert pool.get_resolve_username_backoff_remaining_sec("+7002") == 0
        # Ramp-up restored for the blocked phone only.
        assert pool.is_resolve_ramp_up_active("+7001")
        assert not pool.is_resolve_ramp_up_active("+7002")

    @pytest.mark.anyio
    async def test_restore_skips_expired_entries(self):
        until = datetime.now(timezone.utc) - timedelta(seconds=10)
        payload = json.dumps({"+7001": until.isoformat()})

        async def get_setting(key):
            return payload if key == RESOLVE_BACKOFF_BY_PHONE_SETTING else None

        pool = _make_pool()
        await pool.restore_resolve_username_backoff(
            SimpleNamespace(get_setting=get_setting, set_setting=AsyncMock())
        )
        assert pool.get_resolve_username_backoff_remaining_sec("+7001") == 0

    @pytest.mark.anyio
    async def test_legacy_global_value_migrates_to_all_known_phones(self):
        """Upgrade mid-flood: the legacy single deadline conservatively applies
        to every known phone, is re-persisted in the new format, and the legacy
        key is cleared."""
        legacy_until = datetime.now(timezone.utc) + timedelta(seconds=1800)

        async def get_setting(key):
            if key == RESOLVE_BACKOFF_BY_PHONE_SETTING:
                return None
            if key == RESOLVE_BACKOFF_LEGACY_SETTING:
                return legacy_until.isoformat()
            return None

        set_setting = AsyncMock()
        pool = _make_pool()
        pool._db = SimpleNamespace(get_setting=get_setting, set_setting=set_setting)
        await pool.restore_resolve_username_backoff(
            pool._db, phones=["+7001", "+7002"]
        )

        assert pool.get_resolve_username_backoff_until("+7001") == legacy_until
        assert pool.get_resolve_username_backoff_until("+7002") == legacy_until
        written = {call.args[0]: call.args[1] for call in set_setting.await_args_list}
        assert RESOLVE_BACKOFF_LEGACY_SETTING in written
        assert written[RESOLVE_BACKOFF_LEGACY_SETTING] == ""
        stored = json.loads(written[RESOLVE_BACKOFF_BY_PHONE_SETTING])
        assert set(stored) == {"+7001", "+7002"}

    @pytest.mark.anyio
    async def test_legacy_migration_without_phones_is_skipped(self):
        legacy_until = datetime.now(timezone.utc) + timedelta(seconds=1800)

        async def get_setting(key):
            if key == RESOLVE_BACKOFF_LEGACY_SETTING:
                return legacy_until.isoformat()
            return None

        set_setting = AsyncMock()
        pool = _make_pool()
        await pool.restore_resolve_username_backoff(
            SimpleNamespace(get_setting=get_setting, set_setting=set_setting)
        )
        # Nothing restored, legacy key left intact for the next start.
        assert pool.get_resolve_username_backoff_remaining_sec() == 0
        set_setting.assert_not_awaited()
