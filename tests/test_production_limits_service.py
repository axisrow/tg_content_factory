from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, patch

import pytest

from src.services.production_limits_service import (
    CostConfig,
    CostTracker,
    ProductionLimitsService,
    RateLimitConfig,
    RateLimiter,
)

# === RateLimiter tests ===


@pytest.mark.asyncio
async def test_rate_limiter_allows_under_limits():
    """Requests under all limits succeed."""
    limiter = RateLimiter(RateLimitConfig(requests_per_minute=10, tokens_per_minute=1000))

    for _ in range(5):
        allowed, wait_time = await limiter.check_and_acquire(tokens=100, is_image=False)
        assert allowed is True
        assert wait_time == 0.0


@pytest.mark.asyncio
async def test_rate_limiter_blocks_over_rpm():
    """Requests over requests_per_minute limit are blocked."""
    limiter = RateLimiter(RateLimitConfig(requests_per_minute=3, tokens_per_minute=1000))

    for _ in range(3):
        await limiter.check_and_acquire(tokens=100, is_image=False)

    allowed, wait_time = await limiter.check_and_acquire(tokens=100, is_image=False)
    assert allowed is False
    assert wait_time > 0


@pytest.mark.asyncio
async def test_rate_limiter_blocks_over_tpm():
    """Requests over tokens_per_minute limit are blocked."""
    limiter = RateLimiter(RateLimitConfig(requests_per_minute=10, tokens_per_minute=500))

    allowed, _ = await limiter.check_and_acquire(tokens=400, is_image=False)
    assert allowed is True

    allowed, wait_time = await limiter.check_and_acquire(tokens=200, is_image=False)
    assert allowed is False
    assert wait_time > 0


@pytest.mark.asyncio
async def test_rate_limiter_blocks_over_tpd():
    """Requests over tokens_per_day limit are blocked."""
    limiter = RateLimiter(
        RateLimitConfig(requests_per_minute=100, tokens_per_minute=100_000, tokens_per_day=500)
    )

    allowed, _ = await limiter.check_and_acquire(tokens=400, is_image=False)
    assert allowed is True

    allowed, wait_time = await limiter.check_and_acquire(tokens=200, is_image=False)
    assert allowed is False
    assert wait_time > 0


@pytest.mark.asyncio
async def test_rate_limiter_get_usage():
    """get_usage returns correct structure with accumulated counts."""
    limiter = RateLimiter(
        RateLimitConfig(requests_per_minute=60, tokens_per_minute=1000, tokens_per_day=10000)
    )

    await limiter.check_and_acquire(tokens=100, is_image=True)
    await limiter.check_and_acquire(tokens=200, is_image=False)

    usage = limiter.get_usage()

    assert usage["minute"]["requests"] == 2
    assert usage["minute"]["tokens"] == 300
    assert usage["minute"]["images"] == 1
    assert usage["minute"]["limit_requests"] == 60
    assert usage["minute"]["limit_tokens"] == 1000
    assert usage["day"]["tokens"] == 300
    assert usage["day"]["images"] == 1
    assert usage["day"]["limit_tokens"] == 10000


@pytest.mark.asyncio
async def test_rate_limiter_window_reset():
    """Minute and day windows reset after their respective durations."""
    fake_time = 1000.0
    with patch("src.services.production_limits_service.time.time", return_value=fake_time):
        limiter = RateLimiter(RateLimitConfig(requests_per_minute=2, tokens_per_minute=10000))
        # Override window_start that was set by the real time.time in default_factory
        limiter._minute_stats.window_start = fake_time
        limiter._day_stats.window_start = fake_time
        await limiter.check_and_acquire(tokens=10)
        await limiter.check_and_acquire(tokens=10)

        # Should be blocked at rpm limit
        allowed, _ = await limiter.check_and_acquire(tokens=10)
        assert allowed is False

    # Advance time past the 60-second minute window
    with patch("src.services.production_limits_service.time.time", return_value=fake_time + 61):
        allowed, wait_time = await limiter.check_and_acquire(tokens=10)
        assert allowed is True
        assert wait_time == 0.0
        # Verify minute counters reset
        usage = limiter.get_usage()
        assert usage["minute"]["requests"] == 1
        assert usage["minute"]["tokens"] == 10


# === CostTracker tests ===


@pytest.mark.asyncio
async def test_cost_tracker_estimate_text_cost():
    """Token cost estimation: (tokens / 1000) * cost_per_1k_tokens."""
    tracker = CostTracker(CostConfig(cost_per_1k_tokens=2.0))

    assert await tracker.estimate_cost(tokens=500, is_image=False) == 1.0
    assert await tracker.estimate_cost(tokens=1500, is_image=False) == 3.0


@pytest.mark.asyncio
async def test_cost_tracker_estimate_image_cost():
    """Image cost estimation returns fixed cost_per_image regardless of tokens."""
    tracker = CostTracker(CostConfig(cost_per_image=0.05))

    assert await tracker.estimate_cost(tokens=0, is_image=True) == 0.05
    assert await tracker.estimate_cost(tokens=1000, is_image=True) == 0.05


@pytest.mark.asyncio
async def test_cost_tracker_cap_not_exceeded():
    """check_cost_cap allows when within budget and does not charge."""
    tracker = CostTracker(CostConfig(cost_per_1k_tokens=1.0, daily_cost_cap=10.0))

    allowed, estimated = await tracker.check_cost_cap(tokens=500, is_image=False)

    assert allowed is True
    assert estimated == 0.5
    assert tracker.get_daily_cost() == 0.0


@pytest.mark.asyncio
async def test_cost_tracker_cap_exceeded():
    """check_cost_cap blocks when the request would exceed the daily cap."""
    tracker = CostTracker(CostConfig(cost_per_1k_tokens=1.0, daily_cost_cap=1.0))

    # Spend up to the cap
    await tracker.record_cost(tokens=1000)  # costs 1.0

    allowed, estimated = await tracker.check_cost_cap(tokens=1, is_image=False)
    assert allowed is False
    assert estimated > 0


@pytest.mark.asyncio
async def test_cost_tracker_record_cost_accumulates():
    """record_cost adds to the running daily total."""
    tracker = CostTracker(CostConfig(cost_per_1k_tokens=2.0, daily_cost_cap=100.0))

    c1 = await tracker.record_cost(tokens=500, is_image=False)
    assert c1 == 1.0

    c2 = await tracker.record_cost(tokens=1000, is_image=False)
    assert c2 == 2.0

    assert tracker.get_daily_cost() == 3.0


@pytest.mark.asyncio
async def test_cost_tracker_remaining_budget():
    """remaining_budget returns cap minus accumulated cost, floored at 0."""
    tracker = CostTracker(CostConfig(cost_per_1k_tokens=1.0, daily_cost_cap=5.0))

    assert tracker.get_remaining_budget() == 5.0

    await tracker.record_cost(tokens=2000)  # 2.0

    assert tracker.get_remaining_budget() == 3.0


@pytest.mark.asyncio
async def test_cost_tracker_day_reset():
    """Daily cost resets after 86400 seconds."""
    fake_time = 5000.0
    with patch("src.services.production_limits_service.time.time", return_value=fake_time):
        tracker = CostTracker(CostConfig(cost_per_1k_tokens=1.0, daily_cost_cap=100.0))
        await tracker.record_cost(tokens=3000)
        assert tracker.get_daily_cost() == 3.0

    # Advance past 24 hours
    with patch("src.services.production_limits_service.time.time", return_value=fake_time + 86401):
        # check_cost_cap triggers the day reset
        allowed, _ = await tracker.check_cost_cap(tokens=0)
        assert allowed is True
        assert tracker.get_daily_cost() == 0.0


# === ProductionLimitsService tests ===


@pytest.mark.asyncio
async def test_production_limits_acquire_allowed():
    """Acquire succeeds when both rate and cost caps are within limits."""
    service = ProductionLimitsService(
        db=SimpleNamespace(),
        rate_config=RateLimitConfig(requests_per_minute=10, tokens_per_minute=1000),
        cost_config=CostConfig(cost_per_1k_tokens=1.0, daily_cost_cap=10.0),
    )

    allowed, error = await service.acquire(tokens=500, max_wait=1.0)

    assert allowed is True
    assert error is None


@pytest.mark.asyncio
async def test_production_limits_acquire_blocked_by_cost_cap():
    """Acquire fails with cost-cap message when daily budget is exhausted."""
    service = ProductionLimitsService(
        db=SimpleNamespace(),
        rate_config=RateLimitConfig(requests_per_minute=10, tokens_per_minute=1000),
        cost_config=CostConfig(cost_per_1k_tokens=1.0, daily_cost_cap=0.5),
    )

    allowed, error = await service.acquire(tokens=1000, max_wait=1.0)

    assert allowed is False
    assert "Daily cost cap exceeded" in error


@pytest.mark.asyncio
async def test_production_limits_get_stats():
    """get_stats aggregates rate-limiter usage and cost tracker state."""
    service = ProductionLimitsService(
        db=SimpleNamespace(),
        rate_config=RateLimitConfig(requests_per_minute=10, tokens_per_minute=1000),
        cost_config=CostConfig(cost_per_1k_tokens=1.0, daily_cost_cap=10.0),
    )

    await service.acquire(tokens=500, max_wait=1.0)

    stats = service.get_stats()

    assert stats["rate_limits"]["minute"]["requests"] == 1
    assert stats["rate_limits"]["minute"]["tokens"] == 500
    assert stats["cost"]["daily_cost"] == 0.0  # acquire does not record cost
    assert stats["cost"]["daily_cap"] == 10.0


# === RateLimiter wait_and_acquire tests ===


@pytest.mark.asyncio
async def test_rate_limiter_wait_and_acquire_times_out():
    """wait_and_acquire returns False when max_wait is exceeded."""
    limiter = RateLimiter(RateLimitConfig(requests_per_minute=0))
    # With 0 rpm limit, every request is blocked — should time out immediately
    result = await limiter.wait_and_acquire(tokens=10, max_wait=0.01)
    assert result is False


@pytest.mark.asyncio
async def test_rate_limiter_wait_and_acquire_succeeds_after_window():
    """wait_and_acquire returns True once a window opens."""
    limiter = RateLimiter(RateLimitConfig(requests_per_minute=1))
    allowed, _ = await limiter.check_and_acquire(tokens=10)
    assert allowed is True

    # Now blocked — advance time to reset the minute window before calling wait_and_acquire
    limiter._minute_stats.window_start = 0.0  # Force window to look expired

    # Patch check_and_acquire to return True on the second call
    original_check = limiter.check_and_acquire
    call_count = 0

    async def mock_check(tokens=0, is_image=False):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return False, 1.0  # Blocked first
        return await original_check(tokens, is_image)

    with patch.object(limiter, "check_and_acquire", mock_check):
        with patch("asyncio.sleep", new_callable=AsyncMock):
            result = await limiter.wait_and_acquire(tokens=10, max_wait=5.0)
    assert result is True


# === CostTracker record_cost day reset ===


@pytest.mark.asyncio
async def test_cost_tracker_record_cost_day_reset():
    """record_cost resets daily_cost when 86400s have elapsed."""
    fake_time = 5000.0
    with patch("src.services.production_limits_service.time.time", return_value=fake_time):
        tracker = CostTracker(CostConfig(cost_per_1k_tokens=1.0, daily_cost_cap=100.0))
        await tracker.record_cost(tokens=3000)
        assert tracker.get_daily_cost() == 3.0

    # Advance past 24h — record_cost should reset
    with patch("src.services.production_limits_service.time.time", return_value=fake_time + 86401):
        cost = await tracker.record_cost(tokens=1000)
        assert cost == 1.0
        assert tracker.get_daily_cost() == 1.0  # reset happened


# === ProductionLimitsService execute_with_retry ===


@pytest.mark.asyncio
async def test_execute_with_retry_success():
    """execute_with_retry returns result on first successful call."""
    service = ProductionLimitsService(
        db=SimpleNamespace(),
        rate_config=RateLimitConfig(requests_per_minute=10, tokens_per_minute=1000),
        cost_config=CostConfig(cost_per_1k_tokens=1.0, daily_cost_cap=10.0),
    )
    result = await service.execute_with_retry(func=AsyncMock(return_value="ok"), tokens=10)
    assert result == "ok"


@pytest.mark.asyncio
async def test_execute_with_retry_retries_on_failure():
    """execute_with_retry retries and eventually succeeds."""
    service = ProductionLimitsService(
        db=SimpleNamespace(),
        rate_config=RateLimitConfig(requests_per_minute=100, tokens_per_minute=100000),
        cost_config=CostConfig(daily_cost_cap=100.0),
    )
    call_count = 0

    async def flaky():
        nonlocal call_count
        call_count += 1
        if call_count < 3:
            raise RuntimeError("transient")
        return "recovered"

    with patch("src.services.production_limits_service.asyncio.sleep", new_callable=AsyncMock):
        result = await service.execute_with_retry(func=flaky, tokens=10, max_retries=3, base_delay=0.01)
    assert result == "recovered"
    assert call_count == 3


@pytest.mark.asyncio
async def test_execute_with_retry_raises_after_max_retries():
    """execute_with_retry raises when all retries are exhausted."""
    service = ProductionLimitsService(
        db=SimpleNamespace(),
        rate_config=RateLimitConfig(requests_per_minute=100, tokens_per_minute=100000),
        cost_config=CostConfig(daily_cost_cap=100.0),
    )

    with patch("src.services.production_limits_service.asyncio.sleep", new_callable=AsyncMock):
        with pytest.raises(RuntimeError, match="always fails"):
            await service.execute_with_retry(
                func=AsyncMock(side_effect=RuntimeError("always fails")),
                tokens=10,
                max_retries=2,
                base_delay=0.01,
            )


@pytest.mark.asyncio
async def test_execute_with_retry_rate_limit_raises():
    """execute_with_retry raises RuntimeError when rate limit blocks acquire."""
    service = ProductionLimitsService(
        db=SimpleNamespace(),
        rate_config=RateLimitConfig(requests_per_minute=0),
        cost_config=CostConfig(daily_cost_cap=100.0),
    )
    # Mock acquire to return blocked immediately
    service.acquire = AsyncMock(return_value=(False, "Rate limit timeout"))
    with pytest.raises(RuntimeError, match="Rate limit"):
        await service.execute_with_retry(func=AsyncMock(return_value="x"), tokens=10)
