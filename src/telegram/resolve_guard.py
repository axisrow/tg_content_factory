from __future__ import annotations

import logging
from collections.abc import Awaitable, Callable
from datetime import datetime, timedelta, timezone

from src.telegram.flood_wait import HandledFloodWaitError, run_with_flood_wait
from src.telegram.rate_limiter import (
    GLOBAL_RESOLVE_BACKOFF_THRESHOLD_SEC,
    ResolveRateLimiter,
    UsernameResolveRateLimitedError,
)

logger = logging.getLogger(__name__)


class ResolveGuardMixin:
    """Shared live-username-resolve FloodWait guard (#785).

    Owns the single source of truth for the per-account resolve rate limiter and
    the global cross-account backoff window. Every runtime path that performs a
    live ``auth.resolveUsername`` (collection, stats, search fallback, agent
    tools) routes through :meth:`run_live_username_resolve` so they share one
    budget and one flood-wait policy.

    Concrete pools must initialise ``self._resolve_rate_limiter`` and
    ``self._resolve_username_backoff_until_utc`` in their ``__init__``.
    """

    _resolve_rate_limiter: ResolveRateLimiter
    _resolve_username_backoff_until_utc: datetime | None

    def _get_resolve_rate_limiter(self) -> ResolveRateLimiter:
        limiter = getattr(self, "_resolve_rate_limiter", None)
        if not isinstance(limiter, ResolveRateLimiter):
            limiter = ResolveRateLimiter()
            self._resolve_rate_limiter = limiter
        return limiter

    def get_resolve_username_backoff_remaining_sec(self) -> int:
        """Return global live username-resolve backoff remaining seconds."""
        backoff_until = getattr(self, "_resolve_username_backoff_until_utc", None)
        if backoff_until is None:
            return 0
        remaining = (backoff_until - datetime.now(timezone.utc)).total_seconds()
        if remaining <= 0:
            self._resolve_username_backoff_until_utc = None
            return 0
        return int(remaining)

    def get_resolve_username_backoff_until(self) -> datetime | None:
        """Return the active backoff deadline, clearing it once elapsed."""
        backoff_until = getattr(self, "_resolve_username_backoff_until_utc", None)
        if backoff_until is None:
            return None
        if (backoff_until - datetime.now(timezone.utc)).total_seconds() <= 0:
            self._resolve_username_backoff_until_utc = None
            return None
        return backoff_until

    def set_resolve_username_backoff(self, wait_seconds: int) -> datetime:
        """Pause live username resolves for Telegram's full FloodWait window."""
        self._resolve_username_backoff_until_utc = datetime.now(timezone.utc) + timedelta(
            seconds=max(0, int(wait_seconds))
        )
        return self._resolve_username_backoff_until_utc

    def reserve_resolve_username_call(self, phone: str) -> float:
        """Reserve one live username resolve slot, or return retry-after seconds."""
        remaining = self.get_resolve_username_backoff_remaining_sec()
        if remaining > 0:
            return float(remaining)
        return self._get_resolve_rate_limiter().try_acquire(str(phone or "unknown"))

    @staticmethod
    def _is_live_username_peer(peer: object) -> bool:
        if not isinstance(peer, str):
            return False
        value = peer.strip()
        if not value:
            return False
        return not value.lstrip("-").isdigit()

    def _record_resolve_username_flood(self, wait_seconds: int) -> datetime | None:
        if wait_seconds <= GLOBAL_RESOLVE_BACKOFF_THRESHOLD_SEC:
            return None
        return self.set_resolve_username_backoff(wait_seconds)

    async def run_live_username_resolve(
        self,
        awaitable_factory: Callable[[], Awaitable[object]],
        *,
        phone: str,
        username: str,
        operation: str,
        logger_: logging.Logger | None = None,
        timeout: float | None = 30.0,
    ) -> object:
        """Run a live username resolve behind the shared FloodWait guard."""
        retry_after = self.reserve_resolve_username_call(phone)
        if retry_after > 0:
            raise UsernameResolveRateLimitedError(phone, retry_after)
        try:
            return await run_with_flood_wait(
                awaitable_factory(),
                operation=operation,
                phone=phone,
                pool=self,
                logger_=logger_ or logger,
                timeout=timeout,
            )
        except HandledFloodWaitError as exc:
            next_available_at = self._record_resolve_username_flood(exc.info.wait_seconds)
            if next_available_at is not None:
                (logger_ or logger).warning(
                    "%s got FloodWait %ss on %s while resolving %s; "
                    "pausing live username resolves until %s",
                    operation,
                    exc.info.wait_seconds,
                    phone,
                    username,
                    next_available_at.isoformat(),
                )
            raise
