from __future__ import annotations

from types import SimpleNamespace

import pytest

from src.services.production_limits_service import (
    CostConfig,
    CostTracker,
    ProductionLimitsService,
    RateLimitConfig,
    RateLimiter,
)


@pytest.mark.asyncio
async def test_cost_tracker_check_does_not_charge_budget():
    tracker = CostTracker(CostConfig(cost_per_1k_tokens=1.0, daily_cost_cap=1.0))

    allowed, estimated = await tracker.check_cost_cap(tokens=500)

    assert allowed is True
    assert estimated == 0.5
    assert tracker.get_daily_cost() == 0.0


@pytest.mark.asyncio
async def test_production_limits_acquire_does_not_charge_on_rate_limit_timeout():
    service = ProductionLimitsService(
        db=SimpleNamespace(),
        rate_config=RateLimitConfig(requests_per_minute=0),
        cost_config=CostConfig(cost_per_1k_tokens=1.0, daily_cost_cap=1.0),
    )

    allowed, error = await service.acquire(tokens=500, max_wait=0.0)

    assert allowed is False
    assert error == "Rate limit timeout"
    assert service.get_stats()["cost"]["daily_cost"] == 0.0


@pytest.mark.asyncio
async def test_execute_with_retry_charges_cost_once_after_success():
    service = ProductionLimitsService(
        db=SimpleNamespace(),
        rate_config=RateLimitConfig(requests_per_minute=10, tokens_per_minute=1000),
        cost_config=CostConfig(cost_per_1k_tokens=1.0, daily_cost_cap=10.0),
    )
    attempts = 0

    async def flaky() -> str:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise RuntimeError("temporary failure")
        return "ok"

    result = await service.execute_with_retry(flaky, tokens=500, max_retries=1, base_delay=0.0)

    assert result == "ok"
    assert attempts == 2
    assert service.get_stats()["cost"]["daily_cost"] == 0.5


@pytest.mark.asyncio
async def test_execute_with_retry_respects_daily_cost_cap():
    service = ProductionLimitsService(
        db=SimpleNamespace(),
        rate_config=RateLimitConfig(requests_per_minute=10, tokens_per_minute=1000),
        cost_config=CostConfig(cost_per_1k_tokens=1.0, daily_cost_cap=0.4),
    )

    async def ok() -> str:
        return "ok"

    with pytest.raises(RuntimeError, match="Daily cost cap exceeded"):
        await service.execute_with_retry(ok, tokens=500, max_retries=0)


# === RateLimiter unit tests ===


@pytest.mark.asyncio
async def test_rate_limiter_allows_under_limits():
    """Requests under all limits succeed."""
    limiter = RateLimiter(RateLimitConfig(requests_per_minute=10, tokens_per_minute=1000))

    for _ in range(5):
        allowed, wait_time = await limiter.check_and_acquire(tokens=100, is_image=False)
        assert allowed is True
        assert wait_time == 0.0


@pytest.mark.asyncio
async def test_rate_limiter_blocks_over_request_limit():
    """Requests over requests_per_minute limit are blocked."""
    limiter = RateLimiter(RateLimitConfig(requests_per_minute=3, tokens_per_minute=1000))

    for _ in range(3):
        await limiter.check_and_acquire(tokens=100, is_image=False)

    # 4th request should be blocked
    allowed, wait_time = await limiter.check_and_acquire(tokens=100, is_image=False)
    assert allowed is False
    assert wait_time > 0


@pytest.mark.asyncio
async def test_rate_limiter_blocks_over_token_limit():
    """Requests over tokens_per_minute limit are blocked."""
    limiter = RateLimiter(RateLimitConfig(requests_per_minute=10, tokens_per_minute=500))

    # First request uses 400 tokens (under limit)
    allowed, _ = await limiter.check_and_acquire(tokens=400, is_image=False)
    assert allowed is True

    # Second request uses 200 tokens (total 600, over limit)
    allowed, wait_time = await limiter.check_and_acquire(tokens=200, is_image=False)
    assert allowed is False
    assert wait_time > 0


@pytest.mark.asyncio
async def test_rate_limiter_tracks_images():
    """Image requests increment image_count."""
    limiter = RateLimiter(RateLimitConfig(requests_per_minute=10))

    await limiter.check_and_acquire(tokens=100, is_image=True)
    await limiter.check_and_acquire(tokens=100, is_image=True)

    usage = limiter.get_usage()
    assert usage["minute"]["images"] == 2


@pytest.mark.asyncio
async def test_rate_limiter_get_usage_structure():
    """get_usage returns correct structure."""
    limiter = RateLimiter(RateLimitConfig(requests_per_minute=60, tokens_per_minute=1000, tokens_per_day=10000))

    usage = limiter.get_usage()

    assert "minute" in usage
    assert "day" in usage
    assert usage["minute"]["limit_requests"] == 60
    assert usage["minute"]["limit_tokens"] == 1000
    assert usage["day"]["limit_tokens"] == 10000


# === CostTracker unit tests ===


@pytest.mark.asyncio
async def test_cost_tracker_estimate_cost_tokens():
    """Token cost estimation: (tokens/1000) * cost_per_1k."""
    tracker = CostTracker(CostConfig(cost_per_1k_tokens=2.0))

    cost = await tracker.estimate_cost(tokens=500, is_image=False)
    assert cost == 1.0  # 500/1000 * 2.0

    cost = await tracker.estimate_cost(tokens=1500, is_image=False)
    assert cost == 3.0  # 1500/1000 * 2.0


@pytest.mark.asyncio
async def test_cost_tracker_estimate_cost_image():
    """Image cost estimation returns fixed cost_per_image."""
    tracker = CostTracker(CostConfig(cost_per_image=0.5))

    cost = await tracker.estimate_cost(tokens=0, is_image=True)
    assert cost == 0.5

    # Tokens ignored for images
    cost = await tracker.estimate_cost(tokens=1000, is_image=True)
    assert cost == 0.5
