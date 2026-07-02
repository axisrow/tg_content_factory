from __future__ import annotations

import asyncio
import inspect
import logging
import time
import weakref
from collections import Counter
from dataclasses import dataclass
from enum import Enum
from typing import Awaitable, Callable, TypeVar, cast

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


class _CircuitOpenError(RuntimeError):
    pass


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
        """Classify an error into a category.

        FATAL patterns are checked FIRST so a non-retryable error that also
        happens to mention a transient phrase still classifies as FATAL. This
        matters for billing-adjacent LLM calls: a genuine quota-exhaustion error
        worded like ``"rate limit / quota exceeded"`` must NOT be retried (it
        would burn the remaining quota), even though it contains ``"rate limit"``.
        """
        error_str = str(error).lower()
        error_type = type(error).__name__.lower()

        for pattern in cls.FATAL_ERRORS:
            if pattern in error_str or pattern in error_type:
                return ErrorCategory.FATAL

        for pattern in cls.TRANSIENT_ERRORS:
            if pattern in error_str or pattern in error_type:
                if "rate" in pattern or "429" in pattern:
                    return ErrorCategory.RATE_LIMIT
                return ErrorCategory.TRANSIENT

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

    # Process-wide weak registry of every live recovery service. Each instance
    # keeps its OWN ``_error_history`` (the stats live in the instance), but the
    # five LLM/embedding services build a *fresh* instance per pipeline-run /
    # request / task via ``for_llm()`` / ``for_embeddings()`` — there is no
    # single long-lived instance to query. The registry lets a debug surface
    # aggregate the histories of all instances that are still alive without
    # threading a shared instance through every constructor and call site.
    #
    # WeakSet => an instance is dropped automatically once its owning service is
    # garbage-collected, so the registry never keeps an otherwise-dead service
    # alive and never leaks. The trade-off is that an ephemeral instance's
    # history is only visible while it (and its owner) are still referenced —
    # i.e. the aggregate is a live snapshot of in-flight/recent work, not a
    # durable error log. See ``aggregate_error_stats``.
    _instances: "weakref.WeakSet[ErrorRecoveryService]" = weakref.WeakSet()

    def __init__(
        self,
        retry_policy: RetryPolicy | None = None,
        circuit_config: CircuitBreakerConfig | None = None,
    ):
        self._retry_policy = retry_policy or RetryPolicy()
        self._circuit_breaker = CircuitBreaker(circuit_config)
        self._error_history: list[ErrorRecord] = []
        self._max_history = 100
        # Self-register so ``aggregate_error_stats`` can find this instance.
        ErrorRecoveryService._instances.add(self)

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
        if isinstance(error, _CircuitOpenError):
            return False
        if not isinstance(error, Exception):
            return False
        return ErrorClassifier.classify(error) != ErrorCategory.FATAL

    def _log_before_retry(self, retry_state: RetryCallState) -> None:
        if retry_state.outcome is None or not retry_state.outcome.failed:
            return
        error = retry_state.outcome.exception()
        if error is None:
            return
        category = ErrorClassifier.classify(cast(Exception, error))
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
                        raise _CircuitOpenError("Circuit breaker is open")

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

    async def execute_provider_call(
        self,
        func: Callable[[], Awaitable[T]],
        fallback: Callable[[], T | Awaitable[T]] | None = None,
        *,
        provider: Callable[..., object] | None = None,
    ) -> T:
        """Run an *idempotent* LLM/embedding provider call with recovery.

        Names the intended use of :meth:`execute_with_recovery`: wrapping
        idempotent provider calls (LLM text, embeddings, quality scoring) so a
        transient failure is retried while a FATAL one is not.

        ``provider`` is the *actual* provider callable being invoked inside
        ``func``. It is screened by :func:`guard_not_image` BEFORE any retry can
        happen, so a billed, non-idempotent image adapter can never be replayed
        through this path (would double-bill, #958/#1003). Call sites build a
        zero-arg ``func`` closure around the provider (to bind prompt/kwargs), and
        that closure hides the underlying callable from inspection — passing the
        provider explicitly is what keeps the guard effective at every wired site,
        not only at :meth:`RuntimeProviderRegistry.get_recovered_provider_callable`.
        Omitting ``provider`` is allowed only when the wrapped callable provably
        cannot be an image adapter (e.g. embeddings, which never produce images).
        """
        if provider is not None:
            guard_not_image(provider)
        return await self.execute_with_recovery(func, fallback=fallback)

    def get_error_stats(self) -> dict:
        """Get error statistics."""
        if not self._error_history:
            return {"total_errors": 0, "by_category": {}, "recent": []}

        by_category = dict(Counter(record.category.value for record in self._error_history))

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

    @classmethod
    def aggregate_error_stats(cls) -> dict:
        """Aggregate error stats across every live recovery instance.

        The five idempotent LLM/embedding services each build their own
        ``ErrorRecoveryService`` (via ``for_llm()`` / ``for_embeddings()``), and
        those instances are short-lived (one per pipeline-run / request / task).
        Querying any single instance would only ever show one service's slice, so
        this walks the weak registry (``cls._instances``) and folds every live
        instance's :meth:`get_error_stats` into one process-wide view.

        Returns a dict shaped like :meth:`get_error_stats` plus:
          - ``instances``: number of live recovery instances folded in;
          - ``open_circuits``: how many of them have a tripped (open/half_open)
            circuit breaker right now.

        ``recent`` is the 10 most-recent error records across all instances
        (sorted by timestamp), so the snapshot reads chronologically regardless
        of which service produced each error. Because the registry holds only
        *live* instances, this is a snapshot of in-flight/recent activity — it is
        intentionally not a durable error log (see the ``_instances`` note).
        """
        # Materialize to a list first. WeakSet.__iter__ already guards against
        # "set changed size during iteration" (it defers referent removal while
        # iterating), so this is not about avoiding a crash — it pins a stable
        # set for the whole aggregation: a fixed len() for the ``instances``
        # count, and strong refs so no instance is collected mid-loop.
        live = list(cls._instances)

        total = 0
        by_category: dict[str, int] = {}
        all_recent: list[dict] = []
        open_circuits = 0

        for inst in live:
            stats = inst.get_error_stats()
            total += stats.get("total_errors", 0)
            for cat, count in stats.get("by_category", {}).items():
                by_category[cat] = by_category.get(cat, 0) + count
            all_recent.extend(stats.get("recent", []))
            # Read the breaker state straight from the instance: get_error_stats
            # omits the ``circuit_breaker`` key on an empty history, but a breaker
            # can be open with no recorded errors (it counts failures, not stored
            # records), so we must not infer "closed" from a missing key.
            cb_state = inst._circuit_breaker.get_state().get("state", "closed")
            if cb_state != "closed":
                open_circuits += 1

        all_recent.sort(key=lambda r: r.get("timestamp", 0.0), reverse=True)

        return {
            "instances": len(live),
            "total_errors": total,
            "by_category": by_category,
            "recent": all_recent[:10],
            "open_circuits": open_circuits,
        }


# ---------------------------------------------------------------------------
# Pre-configured factories for the two idempotent provider classes.
#
# These are the *only* sanctioned ways to obtain a recovery service for
# provider calls. Keeping the policy here (not duplicated at every call site)
# guarantees the LLM/embedding retry budgets stay consistent and that the image
# path never accidentally inherits a retrying service (see ``guard_not_image``).
# ---------------------------------------------------------------------------

# LLM text generation (generation, refinement, quality scoring, A/B variants):
# idempotent, so a transient failure is safe to replay. 3 retries + a circuit
# breaker that trips after 5 consecutive provider failures.
LLM_RETRY_POLICY = RetryPolicy(max_retries=3)
LLM_CIRCUIT_CONFIG = CircuitBreakerConfig(failure_threshold=5, recovery_timeout=60.0)

# Embeddings: cheaper and less critical than text generation, so a smaller
# retry budget (2) is enough to ride out a transient blip without amplifying
# load on the embedding endpoint.
EMBEDDING_RETRY_POLICY = RetryPolicy(max_retries=2)
EMBEDDING_CIRCUIT_CONFIG = CircuitBreakerConfig(failure_threshold=5, recovery_timeout=60.0)


def for_llm() -> ErrorRecoveryService:
    """Recovery service tuned for idempotent LLM-text provider calls."""
    return ErrorRecoveryService(
        retry_policy=LLM_RETRY_POLICY,
        circuit_config=LLM_CIRCUIT_CONFIG,
    )


def for_embeddings() -> ErrorRecoveryService:
    """Recovery service tuned for idempotent embedding provider calls."""
    return ErrorRecoveryService(
        retry_policy=EMBEDDING_RETRY_POLICY,
        circuit_config=EMBEDDING_CIRCUIT_CONFIG,
    )


class ImageAdapterRetryError(RuntimeError):
    """Raised when an image-generation adapter is routed through recovery.

    Image generation is a *billed, non-idempotent* POST. Retrying it would
    re-charge the user for a request that may already have produced an image
    (#958, #1003 — every image client is pinned to ``max_retries=0`` for exactly
    this reason). ``ErrorRecoveryService`` retries by design, so wrapping an
    image adapter is always a bug. :func:`guard_not_image` raises this to fail
    loudly at the call site instead of silently double-billing in production.
    """


# Heuristic markers that identify an image-generation callable. Image adapters
# carry the ``(prompt, model) -> url`` shape and live in ``provider_adapters`` /
# ``image_generation_service``; their factory/closure names contain "image".
_IMAGE_CALLABLE_MARKERS = ("image", "img")


def guard_not_image(func: Callable[..., object]) -> None:
    """Assert *func* is not an image-generation adapter; raise otherwise.

    Defence-in-depth so a future refactor cannot accidentally feed a billed
    image adapter into the retrying recovery path. Inspection-only: it never
    calls *func*. The check is deliberately conservative — it matches on the
    callable's own name and its defining module/qualname so it catches both the
    ``make_*_image_adapter`` factories and the inner ``adapter`` closures they
    return, without flagging ordinary LLM providers.
    """
    name = (getattr(func, "__name__", "") or "").lower()
    qualname = (getattr(func, "__qualname__", "") or "").lower()
    module = (getattr(func, "__module__", "") or "").lower()

    # The inner image closures are literally named ``adapter`` and are defined in
    # the image modules, so the module/qualname carries the signal even when the
    # bare ``__name__`` does not.
    haystacks = (name, qualname, module)
    is_image_module = "image_generation_service" in module or (
        "provider_adapters" in module and "image" in qualname
    )
    has_image_marker = any(marker in h for h in haystacks for marker in _IMAGE_CALLABLE_MARKERS)

    if is_image_module or has_image_marker:
        raise ImageAdapterRetryError(
            f"Refusing to wrap image adapter {qualname or name!r} in ErrorRecoveryService: "
            "image generation is billed per request and must never be retried (#958)."
        )
