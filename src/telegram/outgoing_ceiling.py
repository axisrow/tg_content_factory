"""Process-wide ceiling over outgoing Telegram calls (#1417).

Per-account gate buckets are independent, so ten accounts in one process can
emit ten synchronized bursts without any single bucket refusing — production
log 2026-08-26 21:00:15 shows four accounts flood-waited in the same second,
each individually within its limits.  This coordinator shares ONE sliding
window across every outgoing transport call in the process.

Semantics are "wait, don't refuse": callers await :meth:`acquire` instead of
growing another exception-handling site, unlike the per-account gate whose
``TelegramRateLimitedError`` every caller must catch.

Built on ``ResolveRateLimiter`` from telethon-floodgate — the same
sliding-window math the per-account gate uses — rather than aiolimiter,
whose leaky bucket is softer than the window semantics the rest of the
flood stack assumes (#954) and whose clock is not injectable, which these
fake-clock tests require.
"""
from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from typing import Any

from telethon_floodgate import RateLimitSpec, ResolveRateLimiter

# The shared window is process-global, so the per-phone key is a constant.
_PROCESS_KEY = "process"

# ponytail: single global window; per-category sub-ceilings if calibration
# (#1331 step 0.2) ever shows one category starving the others.


class OutgoingRateCeiling:
    """Sliding-window ceiling all bound transport sessions wait on."""

    def __init__(
        self,
        spec: RateLimitSpec,
        *,
        time_func: Callable[[], float] | None = None,
        sleep_func: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        kwargs: dict[str, Any] = {}
        if time_func is not None:
            kwargs["time_func"] = time_func
        # Exact delays: jitter would make waited callers re-burst on wake
        # instead of pacing through the window in order.
        self._limiter = ResolveRateLimiter(
            max_calls=spec.max_calls,
            window_sec=spec.window_sec,
            jitter_sec=0.0,
            **kwargs,
        )
        self._sleep = sleep_func

    async def acquire(self) -> None:
        """Consume one process-wide slot, sleeping while the window is full."""
        while True:
            retry_after = self._limiter.try_acquire(_PROCESS_KEY)
            if retry_after <= 0:
                return
            await self._sleep(retry_after)
