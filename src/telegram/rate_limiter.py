"""Per-account sliding-window rate limiter for live ``auth.resolveUsername``
calls (#551).

The reactive backoff added in #502 only kicks in *after* Telegram has already
returned a multi-hour ``FLOOD_WAIT_X``. By then the damage is done: production
logs show a single fresh session firing 160+ ``resolve_username`` calls in
seconds, escalating to 15–18 hour flood waits.

This limiter caps the *burst* before it happens. It is a pure in-memory,
per-account sliding window — no DB, no locks — so it is cheap to consult on the
hot resolve path. When an account exceeds its window the caller defers the
channel (short reschedule) instead of issuing the live API call.
"""

from __future__ import annotations

import random
import time
from collections import defaultdict, deque

# Telegram does not publish the ``auth.resolveUsername`` limit; production
# evidence (#464/#551) points at roughly 30 calls / account / minute before
# escalation begins, so we stay just under that.
DEFAULT_MAX_CALLS = 30
DEFAULT_WINDOW_SEC = 60.0
DEFAULT_JITTER_SEC = 5.0


class ResolveRateLimiter:
    """Sliding-window limiter keyed by account phone.

    ``try_acquire`` is the only method that mutates state: it prunes the
    window, and either records the call and returns ``0.0`` (allowed) or
    returns a positive number of seconds the caller should defer for. The
    deferral includes a small ``±jitter`` so that many accounts unblocking at
    the same instant do not re-burst in lockstep.
    """

    def __init__(
        self,
        *,
        max_calls: int = DEFAULT_MAX_CALLS,
        window_sec: float = DEFAULT_WINDOW_SEC,
        jitter_sec: float = DEFAULT_JITTER_SEC,
        time_func=time.monotonic,
        jitter_func=random.uniform,
    ) -> None:
        self._max_calls = max(1, int(max_calls))
        self._window_sec = float(window_sec)
        self._jitter_sec = max(0.0, float(jitter_sec))
        self._time = time_func
        self._jitter = jitter_func
        self._calls: dict[str, deque[float]] = defaultdict(deque)

    def _prune(self, phone: str, now: float) -> deque[float]:
        window = self._calls[phone]
        cutoff = now - self._window_sec
        while window and window[0] <= cutoff:
            window.popleft()
        return window

    def try_acquire(self, phone: str) -> float:
        """Reserve one resolve slot for ``phone``.

        Returns ``0.0`` when the call is allowed (and records it). Otherwise
        returns the number of seconds to defer before retrying — the window is
        full and no slot is consumed.
        """
        now = self._time()
        window = self._prune(phone, now)
        if len(window) < self._max_calls:
            window.append(now)
            return 0.0
        # Window full: the oldest call falls out of the window at
        # ``oldest + window_sec``. Defer until then, plus jitter.
        retry_after = (window[0] + self._window_sec) - now
        if self._jitter_sec:
            retry_after += self._jitter(0.0, self._jitter_sec)
        return max(retry_after, 0.0)

    def reset(self, phone: str | None = None) -> None:
        """Drop recorded history for ``phone`` (or all accounts)."""
        if phone is None:
            self._calls.clear()
        else:
            self._calls.pop(phone, None)
