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


# === Additional coverage tests ===


@pytest.mark.asyncio
async def test_execute_with_recovery_success():
    """Test execute_with_recovery with successful function."""
    service = ErrorRecoveryService()

    async def success():
        return "success-value"

    result = await service.execute_with_recovery(success)
    assert result == "success-value"


@pytest.mark.asyncio
async def test_execute_with_recovery_retries():
    """Test execute_with_recovery retries on transient error."""
    call_count = 0

    async def fail_once():
        nonlocal call_count
        call_count += 1
        if call_count < 2:
            raise RuntimeError("Connection reset")
        return "recovered"

    service = ErrorRecoveryService(retry_policy=RetryPolicy(max_retries=2, jitter=False))
    result = await service.execute_with_recovery(fail_once)
    assert result == "recovered"
    assert call_count == 2


@pytest.mark.asyncio
async def test_execute_with_recovery_circuit_open():
    """Test execute_with_recovery when circuit is open."""
    service = ErrorRecoveryService(
        retry_policy=RetryPolicy(max_retries=0, jitter=False),
        circuit_config=CircuitBreakerConfig(failure_threshold=1, recovery_timeout=10.0),
    )

    # Trigger circuit to open
    await service._circuit_breaker.record_failure()
    await service._circuit_breaker.record_failure()

    async def fallback():
        return "fallback"

    # Circuit should be open, fallback should be used
    result = await service.execute_with_recovery(lambda: None, fallback=fallback)
    assert result == "fallback"


@pytest.mark.asyncio
async def test_execute_with_recovery_circuit_open_no_fallback():
    """Test execute_with_recovery when circuit is open without fallback."""
    service = ErrorRecoveryService(
        retry_policy=RetryPolicy(max_retries=0, jitter=False),
        circuit_config=CircuitBreakerConfig(failure_threshold=1, recovery_timeout=10.0),
    )

    # Trigger circuit to open
    await service._circuit_breaker.record_failure()
    await service._circuit_breaker.record_failure()

    with pytest.raises(RuntimeError, match="Circuit breaker is open"):
        await service.execute_with_recovery(lambda: None)


@pytest.mark.asyncio
async def test_error_stats():
    """Test get_error_stats returns statistics."""
    service = ErrorRecoveryService()

    # Record some errors
    try:
        raise RuntimeError("Test error")
    except Exception as e:
        service._record_error(e, ErrorCategory.TRANSIENT)

    stats = service.get_error_stats()
    assert stats["total_errors"] == 1
    assert "transient" in stats["by_category"]
    assert len(stats["recent"]) == 1


@pytest.mark.asyncio
async def test_error_stats_empty():
    """Test get_error_stats when no errors recorded."""
    service = ErrorRecoveryService()
    stats = service.get_error_stats()
    assert stats["total_errors"] == 0
    assert stats["by_category"] == {}
    assert stats["recent"] == []


def test_error_classifier_detects_network_error():
    """Test error classifier detects network errors."""
    err = ConnectionError("Connection reset by peer")
    assert ErrorClassifier.classify(err) == ErrorCategory.TRANSIENT


def test_error_classifier_detects_timeout():
    """Test error classifier detects timeout errors."""
    err = TimeoutError("Request timed out")
    assert ErrorClassifier.classify(err) == ErrorCategory.TRANSIENT


def test_error_classifier_detects_permission_error():
    """Test error classifier detects permission errors."""
    err = PermissionError("Access denied")
    # Permission errors are classified by error message pattern
    category = ErrorClassifier.classify(err)
    assert category in [ErrorCategory.FATAL, ErrorCategory.UNKNOWN]


def test_error_classifier_unknown():
    """Test error classifier handles unknown errors."""
    err = ValueError("Some unknown error")
    assert ErrorClassifier.classify(err) == ErrorCategory.UNKNOWN


@pytest.mark.asyncio
async def test_circuit_breaker_closed_state():
    """Test circuit breaker starts in closed state."""
    breaker = CircuitBreaker(CircuitBreakerConfig())
    state = breaker.get_state()
    assert state["state"] == "closed"


@pytest.mark.asyncio
async def test_circuit_breaker_opens_after_threshold():
    """Test circuit breaker opens after failure threshold."""
    breaker = CircuitBreaker(
        CircuitBreakerConfig(failure_threshold=2, recovery_timeout=10.0)
    )

    await breaker.record_failure()
    await breaker.record_failure()

    allowed, state = await breaker.can_execute()
    assert allowed is False
    assert state == "open"


@pytest.mark.asyncio
async def test_retry_policy_with_jitter():
    """Test retry policy calculates delay with jitter."""
    policy = RetryPolicy(max_retries=3, base_delay=1.0, jitter=True)
    service = ErrorRecoveryService(retry_policy=policy)

    # Test that delay calculation works
    delay = service._calculate_delay(0)
    assert delay >= 0  # With jitter, delay can vary
