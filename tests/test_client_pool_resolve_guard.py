from __future__ import annotations

from unittest.mock import AsyncMock

import pytest
from telethon.errors import FloodWaitError

from src.telegram.client_pool import ClientPool
from src.telegram.flood_wait import HandledFloodWaitError
from src.telegram.rate_limiter import ResolveRateLimiter, UsernameResolveRateLimitedError


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
