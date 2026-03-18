from __future__ import annotations

from types import SimpleNamespace

import pytest

from src.services.production_limits_service import (
    CostConfig,
    CostTracker,
    ProductionLimitsService,
    RateLimitConfig,
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
