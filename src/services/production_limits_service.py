from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Awaitable, Callable, TypeVar

from tenacity import AsyncRetrying, RetryCallState, retry_if_exception, stop_after_attempt
from tenacity.wait import wait_exponential_jitter

from src.utils.json import safe_json_dumps, safe_json_loads_dict

if TYPE_CHECKING:
    from src.database import Database

logger = logging.getLogger(__name__)

T = TypeVar("T")

# settings key under which the daily cost counter is persisted so it survives restarts (#233)
_COST_STATE_KEY = "production_limits_daily_cost"
_DAY_SECONDS = 86400


class _AcquireLimitError(RuntimeError):
    """Raised when rate or cost limits block an attempt before provider execution."""


@dataclass
class RateLimitConfig:
    requests_per_minute: int = 60
    tokens_per_minute: int = 100000
    tokens_per_day: int = 1000000


@dataclass
class CostConfig:
    cost_per_1k_tokens: float = 0.002
    cost_per_image: float = 0.02
    daily_cost_cap: float = 10.0


@dataclass
class UsageStats:
    request_count: int = 0
    token_count: int = 0
    image_count: int = 0
    total_cost: float = 0.0
    window_start: float = field(default_factory=time.time)


class RateLimiter:
    """Rate limiter for API calls with sliding window.

    Enforces:
    - Requests per minute
    - Tokens per minute
    - Tokens per day
    """

    def __init__(self, config: RateLimitConfig | None = None):
        self._config = config or RateLimitConfig()
        self._minute_stats = UsageStats()
        self._day_stats = UsageStats()
        self._lock = asyncio.Lock()

    async def check_and_acquire(
        self,
        tokens: int = 0,
        is_image: bool = False,
    ) -> tuple[bool, float]:
        """Check if request is allowed and acquire if so.

        Args:
            tokens: Number of tokens for this request
            is_image: Whether this is an image generation request

        Returns:
            Tuple of (allowed, wait_time_seconds)
        """
        async with self._lock:
            now = time.time()

            # Reset windows if needed
            if now - self._minute_stats.window_start >= 60:
                self._minute_stats = UsageStats(window_start=now)
            if now - self._day_stats.window_start >= 86400:
                self._day_stats = UsageStats(window_start=now)

            # Check limits
            if self._minute_stats.request_count >= self._config.requests_per_minute:
                wait_time = 60 - (now - self._minute_stats.window_start)
                return False, wait_time

            if self._minute_stats.token_count + tokens > self._config.tokens_per_minute:
                wait_time = 60 - (now - self._minute_stats.window_start)
                return False, wait_time

            if self._day_stats.token_count + tokens > self._config.tokens_per_day:
                wait_time = 86400 - (now - self._day_stats.window_start)
                return False, wait_time

            # Acquire
            self._minute_stats.request_count += 1
            self._minute_stats.token_count += tokens
            self._day_stats.token_count += tokens
            if is_image:
                self._minute_stats.image_count += 1
                self._day_stats.image_count += 1

            return True, 0.0

    async def wait_and_acquire(
        self,
        tokens: int = 0,
        is_image: bool = False,
        max_wait: float = 300.0,
    ) -> bool:
        """Wait if necessary and acquire when available.

        Args:
            tokens: Number of tokens for this request
            is_image: Whether this is an image generation request
            max_wait: Maximum time to wait in seconds

        Returns:
            True if acquired, False if timed out
        """
        waited = 0.0
        while waited < max_wait:
            allowed, wait_time = await self.check_and_acquire(tokens, is_image)
            if allowed:
                return True
            if wait_time <= 0:
                wait_time = 1.0
            actual_wait = min(wait_time, max_wait - waited, 10.0)
            await asyncio.sleep(actual_wait)
            waited += actual_wait
        return False

    def get_usage(self) -> dict:
        """Get current usage statistics."""
        return {
            "minute": {
                "requests": self._minute_stats.request_count,
                "tokens": self._minute_stats.token_count,
                "images": self._minute_stats.image_count,
                "limit_requests": self._config.requests_per_minute,
                "limit_tokens": self._config.tokens_per_minute,
            },
            "day": {
                "tokens": self._day_stats.token_count,
                "images": self._day_stats.image_count,
                "limit_tokens": self._config.tokens_per_day,
            },
        }


class CostTracker:
    """Track and enforce cost caps for API usage."""

    def __init__(self, config: CostConfig | None = None, db: "Database | None" = None):
        self._config = config or CostConfig()
        self._daily_cost = 0.0
        self._day_start = time.time()
        self._lock = asyncio.Lock()
        # When a DB is provided, the daily cost is persisted to the settings table and
        # restored lazily on first use so a process restart cannot reset the counter and
        # let usage blow past daily_cost_cap (#233). db=None keeps pure in-memory behaviour.
        self._db = db
        self._loaded = False

    async def _ensure_loaded(self) -> None:
        """Restore the persisted daily cost on first use. Must be called under self._lock."""
        if self._db is None or self._loaded:
            return
        try:
            raw = await self._db.get_setting(_COST_STATE_KEY)
        except Exception:
            # Leave _loaded False so a transient startup error (e.g. SQLite busy) is retried
            # on the next call instead of silently disabling persistence for the whole
            # process — which would let a restart reset the counter past daily_cost_cap.
            logger.warning("CostTracker: failed to load persisted daily cost", exc_info=True)
            return
        # A successful read (even of malformed/empty state) is authoritative — mark loaded so
        # we don't re-read every call; retrying only helps when the read itself failed.
        self._loaded = True
        state = safe_json_loads_dict(raw)
        if not state:
            return
        try:
            day_start = float(state["day_start"])
            daily_cost = float(state["daily_cost"])
        except (KeyError, TypeError, ValueError):
            logger.warning("CostTracker: malformed persisted daily cost: %r", raw)
            return
        # Only restore if the saved window is still the current day; otherwise start fresh.
        if time.time() - day_start < _DAY_SECONDS:
            self._daily_cost = daily_cost
            self._day_start = day_start

    def _maybe_reset_day(self, now: float) -> bool:
        """Reset the daily counter when the day window rolls over. Caller holds self._lock.

        Returns True if a reset happened, so callers can decide whether to persist.
        """
        if now - self._day_start >= _DAY_SECONDS:
            self._daily_cost = 0.0
            self._day_start = now
            return True
        return False

    async def _persist(self) -> None:
        """Persist the current daily cost. Must be called under self._lock."""
        if self._db is None:
            return
        try:
            await self._db.set_setting(
                _COST_STATE_KEY,
                safe_json_dumps({"daily_cost": self._daily_cost, "day_start": self._day_start}),
            )
        except Exception:
            logger.warning("CostTracker: failed to persist daily cost", exc_info=True)

    @staticmethod
    def _parse_state(state: dict | None, now: float) -> tuple[float, float]:
        """(day_start, daily_cost) from persisted state, or a fresh day on garbage."""
        if state:
            try:
                return float(state["day_start"]), float(state["daily_cost"])
            except (KeyError, TypeError, ValueError):
                logger.warning("CostTracker: malformed persisted daily cost: %r", state)
        return now, 0.0

    async def _read_persisted_cost(self, now: float) -> float:
        """Read the authoritative daily cost from the DB, applying day-rollover.

        Updates ``self._day_start`` so a subsequent write keeps the same window.
        On read failure, falls back to the in-memory value (fail-open read; the
        cap check below still guards with whatever total we have).
        """
        assert self._db is not None, (
            "CostTracker._read_persisted_cost requires initialized Database"
        )
        try:
            raw = await self._db.get_setting(_COST_STATE_KEY)
        except Exception:
            logger.warning("CostTracker: failed to read persisted daily cost", exc_info=True)
            return self._daily_cost
        day_start, daily_cost = self._parse_state(safe_json_loads_dict(raw), now)
        if now - day_start >= _DAY_SECONDS:
            self._day_start = now
            return 0.0
        self._day_start = day_start
        return daily_cost

    async def _atomic_increment(self, delta: float) -> float:
        """Read-increment-write the persisted daily cost atomically. Caller holds
        self._lock.

        Uses the connection-wide write transaction (BEGIN IMMEDIATE) so two
        instances sharing the SQLite file accumulate into the same total instead
        of clobbering each other (the multi-instance bug from #814). On any DB
        error, falls back to an in-memory increment so a transient failure never
        crashes the caller (matching the previous best-effort persist).
        """
        now = time.time()
        try:
            async with self._db.transaction() as conn:
                cur = await conn.execute(
                    "SELECT value FROM settings WHERE key = ?", (_COST_STATE_KEY,)
                )
                row = await cur.fetchone()
                day_start, daily_cost = self._parse_state(
                    safe_json_loads_dict(row[0] if row else None), now
                )
                if now - day_start >= _DAY_SECONDS:
                    day_start, daily_cost = now, 0.0
                daily_cost += delta
                self._day_start = day_start
                await conn.execute(
                    "INSERT INTO settings (key, value) VALUES (?, ?) "
                    "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
                    (
                        _COST_STATE_KEY,
                        safe_json_dumps({"daily_cost": daily_cost, "day_start": day_start}),
                    ),
                )
            return daily_cost
        except Exception:
            logger.warning("CostTracker: atomic cost increment failed; counting in-memory", exc_info=True)
            self._maybe_reset_day(now)
            self._daily_cost += delta
            return self._daily_cost

    async def estimate_cost(
        self,
        tokens: int = 0,
        is_image: bool = False,
    ) -> float:
        """Estimate cost for a request.

        Args:
            tokens: Number of tokens
            is_image: Whether this is an image generation

        Returns:
            Estimated cost in dollars
        """
        if is_image:
            return self._config.cost_per_image
        return (tokens / 1000) * self._config.cost_per_1k_tokens

    async def check_cost_cap(
        self,
        tokens: int = 0,
        is_image: bool = False,
    ) -> tuple[bool, float]:
        """Check if request is within cost cap.

        Args:
            tokens: Number of tokens
            is_image: Whether this is an image generation

        Returns:
            Tuple of (allowed, estimated_cost)
        """
        async with self._lock:
            now = time.time()
            if self._db is not None:
                # Re-read the authoritative total so a sibling instance's spend is
                # seen before we approve another call against the shared cap (#814).
                self._daily_cost = await self._read_persisted_cost(now)
                self._loaded = True
            else:
                await self._ensure_loaded()
                if self._maybe_reset_day(now):
                    await self._persist()

            estimated = await self.estimate_cost(tokens, is_image)

            if self._daily_cost + estimated > self._config.daily_cost_cap:
                return False, estimated

            return True, estimated

    async def record_cost(self, tokens: int = 0, is_image: bool = False) -> float:
        """Record cost for a request after it actually executes."""
        async with self._lock:
            estimated = await self.estimate_cost(tokens, is_image)
            if self._db is None:
                self._maybe_reset_day(time.time())
                self._daily_cost += estimated
                return estimated
            # DB-authoritative atomic accumulation across instances (#814).
            self._daily_cost = await self._atomic_increment(estimated)
            self._loaded = True
            return estimated

    def get_daily_cost(self) -> float:
        """Get current daily cost."""
        return self._daily_cost

    def get_remaining_budget(self) -> float:
        """Get remaining daily budget."""
        return max(0.0, self._config.daily_cost_cap - self._daily_cost)


class ProductionLimitsService:
    """Combined service for rate limiting and cost tracking.

    Provides:
    - Rate limiting (requests, tokens per minute/day)
    - Cost tracking and caps
    - Retry policies with exponential backoff
    """

    def __init__(
        self,
        db: Database,
        rate_config: RateLimitConfig | None = None,
        cost_config: CostConfig | None = None,
    ):
        self._db = db
        self._rate_limiter = RateLimiter(rate_config)
        self._cost_tracker = CostTracker(cost_config, db=db)

    @classmethod
    def from_config(cls, db: Database, config) -> "ProductionLimitsService | None":
        """Build from ``AppConfig.production_limits``; ``None`` when disabled (#814)."""
        pl = getattr(config, "production_limits", None)
        if pl is None or not pl.enabled:
            return None
        return cls(
            db,
            RateLimitConfig(
                requests_per_minute=pl.requests_per_minute,
                tokens_per_minute=pl.tokens_per_minute,
                tokens_per_day=pl.tokens_per_day,
            ),
            CostConfig(
                cost_per_1k_tokens=pl.cost_per_1k_tokens,
                cost_per_image=pl.cost_per_image,
                daily_cost_cap=pl.daily_cost_cap,
            ),
        )

    async def acquire(
        self,
        tokens: int = 0,
        is_image: bool = False,
        max_wait: float = 300.0,
    ) -> tuple[bool, str | None]:
        """Acquire permission for an API call.

        Args:
            tokens: Number of tokens
            is_image: Whether this is an image generation
            max_wait: Maximum wait time in seconds

        Returns:
            Tuple of (allowed, error_message)
        """
        # Check cost cap first
        cost_allowed, estimated_cost = await self._cost_tracker.check_cost_cap(
            tokens, is_image
        )
        if not cost_allowed:
            return False, f"Daily cost cap exceeded (estimated: ${estimated_cost:.4f})"

        # Check rate limits
        rate_allowed = await self._rate_limiter.wait_and_acquire(
            tokens, is_image, max_wait
        )
        if not rate_allowed:
            return False, "Rate limit timeout"

        return True, None

    async def record_cost(self, tokens: int = 0, is_image: bool = False) -> float:
        """Record the actual cost of a completed call against the daily cap.

        Pair with ``acquire`` for call sites that don't use ``execute_with_retry``
        (e.g. image generation): ``acquire`` reserves the rate slot and checks the
        cap, ``record_cost`` books the spend once the paid call has succeeded.
        """
        return await self._cost_tracker.record_cost(tokens, is_image)

    async def execute_with_retry(
        self,
        func: Callable[[], Awaitable[T]],
        tokens: int = 0,
        is_image: bool = False,
        max_retries: int = 3,
        base_delay: float = 1.0,
        max_delay: float = 60.0,
    ) -> T:
        """Execute a function with retry logic.

        Args:
            func: Async function to execute
            tokens: Number of tokens for rate limiting
            is_image: Whether this is an image generation
            max_retries: Maximum number of retries
            base_delay: Base delay for exponential backoff
            max_delay: Maximum delay between retries

        Returns:
            Result of the function

        Raises:
            Exception: After max retries exceeded
        """
        def should_retry(error: BaseException) -> bool:
            if not isinstance(error, Exception):
                return False
            return not isinstance(error, _AcquireLimitError)

        def log_before_retry(retry_state: RetryCallState) -> None:
            if retry_state.outcome is None or not retry_state.outcome.failed:
                return
            error = retry_state.outcome.exception()
            delay = retry_state.next_action.sleep if retry_state.next_action else 0.0
            logger.warning(
                "Attempt %d failed: %s. Retrying in %.1fs",
                retry_state.attempt_number,
                str(error)[:100],
                delay,
            )

        retrying = AsyncRetrying(
            stop=stop_after_attempt(max_retries + 1),
            wait=wait_exponential_jitter(initial=base_delay, max=max_delay, jitter=base_delay),
            retry=retry_if_exception(should_retry),
            before_sleep=log_before_retry,
            reraise=True,
        )

        async for attempt in retrying:
            with attempt:
                allowed, error = await self.acquire(tokens, is_image)
                if not allowed:
                    raise _AcquireLimitError(error or "Rate limit exceeded")

                result = await func()
                await self._cost_tracker.record_cost(tokens, is_image)
                return result

        raise RuntimeError("Unknown error")

    def get_stats(self) -> dict:
        """Get current usage statistics."""
        return {
            "rate_limits": self._rate_limiter.get_usage(),
            "cost": {
                "daily_cost": self._cost_tracker.get_daily_cost(),
                "remaining_budget": self._cost_tracker.get_remaining_budget(),
                "daily_cap": self._cost_tracker._config.daily_cost_cap,
            },
        }
