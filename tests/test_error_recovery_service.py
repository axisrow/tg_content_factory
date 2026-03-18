from __future__ import annotations

import pytest

from src.services.error_recovery_service import (
    CircuitBreaker,
    CircuitBreakerConfig,
    ErrorCategory,
    ErrorClassifier,
    ErrorRecoveryService,
    RetryPolicy,
)


@pytest.mark.asyncio
async def test_execute_with_recovery_awaits_async_fallback():
    service = ErrorRecoveryService(retry_policy=RetryPolicy(max_retries=0, jitter=False))

    async def fail():
        raise RuntimeError("boom")

    async def fallback():
        return "fallback-value"

    result = await service.execute_with_recovery(fail, fallback=fallback)

    assert result == "fallback-value"


@pytest.mark.asyncio
async def test_circuit_breaker_enforces_half_open_limit():
    breaker = CircuitBreaker(
        CircuitBreakerConfig(failure_threshold=1, recovery_timeout=0.0, half_open_max_calls=1)
    )

    await breaker.record_failure()

    allowed, state = await breaker.can_execute()
    assert allowed is True
    assert state == "half_open"

    allowed, state = await breaker.can_execute()
    assert allowed is False
    assert state == "half_open_limit"


@pytest.mark.asyncio
async def test_half_open_success_closes_circuit():
    breaker = CircuitBreaker(
        CircuitBreakerConfig(failure_threshold=1, recovery_timeout=0.0, half_open_max_calls=1)
    )

    await breaker.record_failure()
    allowed, _state = await breaker.can_execute()
    assert allowed is True

    await breaker.record_success()

    state = breaker.get_state()
    assert state["state"] == "closed"
    assert state["failure_count"] == 0
    assert state["half_open_calls"] == 0


def test_error_classifier_detects_rate_limit():
    err = RuntimeError("HTTP 429 rate limit exceeded")
    assert ErrorClassifier.classify(err) == ErrorCategory.RATE_LIMIT
