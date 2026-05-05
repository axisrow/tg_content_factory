from __future__ import annotations

import asyncio
import inspect
import logging
import time
from dataclasses import dataclass
from enum import Enum
from typing import Awaitable, Callable, TypeVar

from tenacity import AsyncRetrying, RetryCallState, retry_if_exception, stop_after_attempt
from tenacity.wait import wait_exponential, wait_exponential_jitter

logger = logging.getLogger(__name__)

T = TypeVar("T")


class ErrorCategory(Enum):
    TRANSIENT = "transient"
    RATE_LIMIT = "rate_limit"
    FATAL = "fatal"
    UNKNOWN = "unknown"


@dataclass
class ErrorRecord:
    timestamp: float
    error_type: str
    message: str
    category: ErrorCategory


@dataclass
class CircuitState:
    failure_count: int = 0
    last_failure_time: float = 0.0
    state: str = "closed"  # closed, open, half_open
    half_open_calls: int = 0


@dataclass
class RetryPolicy:
    max_retries: int = 3
    base_delay: float = 1.0
    max_delay: float = 60.0
    exponential_base: float = 2.0
    jitter: bool = True


@dataclass
class CircuitBreakerConfig:
    failure_threshold: int = 5
    recovery_timeout: float = 60.0
    half_open_max_calls: int = 3


class ErrorClassifier:
    """Classifies errors for appropriate handling."""

    TRANSIENT_ERRORS = [
        "timeout",
        "connection reset",
        "connection refused",
        "temporarily unavailable",
        "rate limit",
        "429",
        "503",
        "502",
        "bad gateway",
        "gateway timeout",
    ]

    FATAL_ERRORS = [
        "authentication",
        "unauthorized",
        "forbidden",
        "401",
        "403",
        "invalid api key",
        "quota exceeded",
    ]

    @classmethod
    def classify(cls, error: Exception) -> ErrorCategory:
        """Classify an error into a category."""
        error_str = str(error).lower()
        error_type = type(error).__name__.lower()

        for pattern in cls.TRANSIENT_ERRORS:
            if pattern in error_str or pattern in error_type:
                if "rate" in pattern or "429" in pattern:
                    return ErrorCategory.RATE_LIMIT
                return ErrorCategory.TRANSIENT

        for pattern in cls.FATAL_ERRORS:
            if pattern in error_str or pattern in error_type:
                return ErrorCategory.FATAL

        return ErrorCategory.UNKNOWN


class CircuitBreaker:
    """Circuit breaker pattern implementation."""

    def __init__(self, config: CircuitBreakerConfig | None = None):
        self._config = config or CircuitBreakerConfig()
        self._state = CircuitState()
        self._lock = asyncio.Lock()

    async def can_execute(self) -> tuple[bool, str]:
        """Check if execution is allowed."""
        async with self._lock:
            now = time.time()

            if self._state.state == "closed":
                return True, "closed"

            if self._state.state == "open":
                if now - self._state.last_failure_time >= self._config.recovery_timeout:
                    self._state.state = "half_open"
                    self._state.half_open_calls = 0
                else:
                    return False, "open"

            if self._state.state == "half_open":
                if self._state.half_open_calls >= self._config.half_open_max_calls:
                    return False, "half_open_limit"
                self._state.half_open_calls += 1

            return True, self._state.state

    async def record_success(self) -> None:
        """Record a successful execution."""
        async with self._lock:
            if self._state.state == "half_open":
                self._state.failure_count = 0
                self._state.half_open_calls = 0
                self._state.state = "closed"

    async def record_failure(self) -> None:
        """Record a failed execution."""
        async with self._lock:
            self._state.failure_count += 1
            self._state.last_failure_time = time.time()

            if self._state.state == "half_open" or self._state.failure_count >= self._config.failure_threshold:
                self._state.half_open_calls = 0
                self._state.state = "open"

    def get_state(self) -> dict:
        """Get current circuit breaker state."""
        return {
            "state": self._state.state,
            "failure_count": self._state.failure_count,
            "last_failure_time": self._state.last_failure_time,
            "half_open_calls": self._state.half_open_calls,
        }


class ErrorRecoveryService:
    """Service for error recovery with retry policies and circuit breakers."""

    def __init__(
        self,
        retry_policy: RetryPolicy | None = None,
        circuit_config: CircuitBreakerConfig | None = None,
    ):
        self._retry_policy = retry_policy or RetryPolicy()
        self._circuit_breaker = CircuitBreaker(circuit_config)
        self._error_history: list[ErrorRecord] = []
        self._max_history = 100

    def _calculate_delay(self, attempt: int) -> float:
        """Calculate delay for retry attempt."""
        delay = self._retry_policy.base_delay * (
            self._retry_policy.exponential_base ** attempt
        )
        delay = min(delay, self._retry_policy.max_delay)

        if self._retry_policy.jitter:
            import random
            delay *= 0.5 + random.random()

        return delay

    def _wait_strategy(self):
        if self._retry_policy.jitter:
            return wait_exponential_jitter(
                initial=self._retry_policy.base_delay,
                max=self._retry_policy.max_delay,
                exp_base=self._retry_policy.exponential_base,
            )
        return wait_exponential(
            multiplier=self._retry_policy.base_delay,
            max=self._retry_policy.max_delay,
            exp_base=self._retry_policy.exponential_base,
        )

    @staticmethod
    def _should_retry(error: BaseException) -> bool:
        if not isinstance(error, Exception):
            return False
        return ErrorClassifier.classify(error) != ErrorCategory.FATAL

    def _log_before_retry(self, retry_state: RetryCallState) -> None:
        if retry_state.outcome is None or not retry_state.outcome.failed:
            return
        error = retry_state.outcome.exception()
        if error is None:
            return
        category = ErrorClassifier.classify(error)
        delay = retry_state.next_action.sleep if retry_state.next_action else 0.0
        logger.warning(
            "Attempt %d/%d failed (%s): %s. Retrying in %.1fs",
            retry_state.attempt_number,
            self._retry_policy.max_retries,
            category.value,
            str(error)[:100],
            delay,
        )

    def _record_error(self, error: Exception, category: ErrorCategory) -> None:
        """Record an error in history."""
        record = ErrorRecord(
            timestamp=time.time(),
            error_type=type(error).__name__,
            message=str(error)[:200],
            category=category,
        )
        self._error_history.append(record)
        if len(self._error_history) > self._max_history:
            self._error_history = self._error_history[-self._max_history :]

    async def _run_fallback(
        self,
        fallback: Callable[[], T | Awaitable[T]],
    ) -> T:
        result = fallback()
        if inspect.isawaitable(result):
            return await result
        return result

    async def execute_with_recovery(
        self,
        func: Callable[[], Awaitable[T]],
        fallback: Callable[[], T | Awaitable[T]] | None = None,
    ) -> T:
        """Execute a function with error recovery.

        Args:
            func: Async function to execute
            fallback: Optional fallback function if all retries fail

        Returns:
            Result of the function

        Raises:
            Exception: If all retries fail and no fallback
        """
        last_error: Exception | None = None
        retrying = AsyncRetrying(
            stop=stop_after_attempt(self._retry_policy.max_retries + 1),
            wait=self._wait_strategy(),
            retry=retry_if_exception(self._should_retry),
            before_sleep=self._log_before_retry,
            reraise=True,
        )

        try:
            async for attempt in retrying:
                with attempt:
                    can_execute, _state = await self._circuit_breaker.can_execute()

                    if not can_execute:
                        logger.warning("Circuit breaker is open, using fallback")
                        if fallback:
                            return await self._run_fallback(fallback)
                        raise RuntimeError("Circuit breaker is open")

                    try:
                        result = await func()
                    except Exception as e:
                        last_error = e
                        category = ErrorClassifier.classify(e)
                        self._record_error(e, category)
                        await self._circuit_breaker.record_failure()

                        if category == ErrorCategory.FATAL:
                            logger.error("Fatal error: %s", str(e)[:100])
                            if fallback:
                                return await self._run_fallback(fallback)
                        raise

                    await self._circuit_breaker.record_success()
                    return result
        except Exception as e:
            last_error = e

        if fallback:
            logger.info("All retries failed, using fallback")
            return await self._run_fallback(fallback)

        raise last_error or RuntimeError("Unknown error")

    def get_error_stats(self) -> dict:
        """Get error statistics."""
        if not self._error_history:
            return {"total_errors": 0, "by_category": {}, "recent": []}

        by_category: dict[str, int] = {}
        for record in self._error_history:
            cat = record.category.value
            by_category[cat] = by_category.get(cat, 0) + 1

        recent = [
            {
                "timestamp": r.timestamp,
                "type": r.error_type,
                "message": r.message,
                "category": r.category.value,
            }
            for r in self._error_history[-10:]
        ]

        return {
            "total_errors": len(self._error_history),
            "by_category": by_category,
            "recent": recent,
            "circuit_breaker": self._circuit_breaker.get_state(),
        }
