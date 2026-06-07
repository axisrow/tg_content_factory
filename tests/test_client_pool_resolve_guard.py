from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from telethon.errors import FloodWaitError

from src.telegram.client_pool import ClientPool
from src.telegram.flood_wait import HandledFloodWaitError
from src.telegram.rate_limiter import ResolveRateLimiter, UsernameResolveRateLimitedError
from src.telegram.resolve_guard import ResolveGuardMixin


@pytest.mark.anyio
async def test_live_username_resolve_uses_shared_rate_limiter():
    pool = ClientPool.__new__(ClientPool)
    pool.report_flood = AsyncMock()
    pool._resolve_username_backoff_until_utc = None
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
async def test_live_username_resolve_records_full_long_flood_backoff():
    pool = ClientPool.__new__(ClientPool)
    pool.report_flood = AsyncMock()
    pool._db = SimpleNamespace(set_setting=AsyncMock())
    pool._resolve_username_backoff_until_utc = None
    pool._resolve_rate_limiter = ResolveRateLimiter(max_calls=1, jitter_sec=0.0)

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
    remaining = pool.get_resolve_username_backoff_remaining_sec()
    assert 7100 < remaining <= 7200
    pool._db.set_setting.assert_awaited_once()
    key, value = pool._db.set_setting.await_args.args
    assert key == "resolve_username_backoff_until_utc"
    assert datetime.fromisoformat(value) == pool.get_resolve_username_backoff_until()


def _make_pool():
    class FakePool(ResolveGuardMixin):
        def __init__(self):
            self._resolve_rate_limiter = None
            self._resolve_username_backoff_until_utc = None
            self._resolve_ramp_up_until_utc = None
            self._resolve_ramp_up_last_call_utc = None
            self._resolve_ramp_up_min_interval_sec = 5.0

    return FakePool()


class TestBackoffNeverShortens:
    def test_keeps_longer_backoff(self):
        pool = _make_pool()
        first = pool.set_resolve_username_backoff(10000)
        second = pool.set_resolve_username_backoff(100)
        assert second == first
        assert pool.get_resolve_username_backoff_remaining_sec() > 9000

    def test_replaces_shorter_backoff(self):
        pool = _make_pool()
        pool.set_resolve_username_backoff(100)
        pool.set_resolve_username_backoff(10000)
        assert pool.get_resolve_username_backoff_remaining_sec() > 9000

    def test_sets_backoff_when_none_active(self):
        pool = _make_pool()
        pool.set_resolve_username_backoff(5000)
        assert pool.get_resolve_username_backoff_remaining_sec() > 4000


class TestRampUpMode:
    def test_ramp_up_active_after_backoff_set(self):
        pool = _make_pool()
        pool.set_resolve_username_backoff(600)
        assert pool.is_resolve_ramp_up_active()

    def test_ramp_up_rate_limits(self):
        pool = _make_pool()
        pool._resolve_username_backoff_until_utc = datetime.now(timezone.utc) - timedelta(seconds=1)
        pool._resolve_ramp_up_until_utc = datetime.now(timezone.utc) + timedelta(seconds=300)
        pool._resolve_ramp_up_min_interval_sec = 5.0
        pool._resolve_ramp_up_last_call_utc = datetime.now(timezone.utc)
        retry_after = pool.reserve_resolve_username_call("+1234567890")
        assert retry_after > 0

    def test_ramp_up_allows_slow_calls(self):
        pool = _make_pool()
        pool._resolve_username_backoff_until_utc = datetime.now(timezone.utc) - timedelta(seconds=1)
        pool._resolve_ramp_up_until_utc = datetime.now(timezone.utc) + timedelta(seconds=300)
        pool._resolve_ramp_up_min_interval_sec = 0.01
        pool._resolve_ramp_up_last_call_utc = datetime.now(timezone.utc) - timedelta(seconds=1)
        retry_after = pool.reserve_resolve_username_call("+1234567890")
        assert retry_after == 0.0
