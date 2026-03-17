from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from src.database import Database

logger = logging.getLogger(__name__)


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

    def __init__(self, config: CostConfig | None = None):
        self._config = config or CostConfig()
        self._daily_cost = 0.0
        self._day_start = time.time()
        self._lock = asyncio.Lock()

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
            if now - self._day_start >= 86400:
                self._daily_cost = 0.0
                self._day_start = now
            
            estimated = await self.estimate_cost(tokens, is_image)
            
            if self._daily_cost + estimated > self._config.daily_cost_cap:
                return False, estimated
            
            self._daily_cost += estimated
            return True, estimated

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
        self._cost_tracker = CostTracker(cost_config)

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

    async def execute_with_retry(
        self,
        func,
        tokens: int = 0,
        is_image: bool = False,
        max_retries: int = 3,
        base_delay: float = 1.0,
        max_delay: float = 60.0,
    ):
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
        last_error = None
        for attempt in range(max_retries + 1):
            allowed, error = await self.acquire(tokens, is_image)
            if not allowed:
                raise RuntimeError(error or "Rate limit exceeded")

            try:
                return await func()
            except Exception as e:
                last_error = e
                if attempt < max_retries:
                    delay = min(base_delay * (2 ** attempt), max_delay)
                    logger.warning(
                        "Attempt %d failed: %s. Retrying in %.1fs",
                        attempt + 1,
                        str(e)[:100],
                        delay,
                    )
                    await asyncio.sleep(delay)
                else:
                    raise

        raise last_error or RuntimeError("Unknown error")

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
