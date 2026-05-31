"""Unit tests for the per-account resolve_username rate limiter (#551)."""
from __future__ import annotations

from src.telegram.rate_limiter import ResolveRateLimiter


class _FakeClock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


def _limiter(clock: _FakeClock, *, max_calls: int = 3, window: float = 60.0, jitter: float = 0.0):
    return ResolveRateLimiter(
        max_calls=max_calls,
        window_sec=window,
        jitter_sec=jitter,
        time_func=clock,
        jitter_func=lambda _a, _b: 0.0,
    )


def test_allows_up_to_max_then_defers():
    clock = _FakeClock()
    limiter = _limiter(clock, max_calls=3, window=60.0)

    assert limiter.try_acquire("+1") == 0.0
    assert limiter.try_acquire("+1") == 0.0
    assert limiter.try_acquire("+1") == 0.0
    # Fourth call within the window is throttled.
    retry = limiter.try_acquire("+1")
    assert retry > 0.0
    # Defer until the oldest call (t=1000) exits the 60s window.
    assert retry == 60.0


def test_window_slides_and_frees_slots():
    clock = _FakeClock()
    limiter = _limiter(clock, max_calls=2, window=60.0)

    assert limiter.try_acquire("+1") == 0.0  # t=1000
    clock.advance(30)
    assert limiter.try_acquire("+1") == 0.0  # t=1030
    # Window full now.
    assert limiter.try_acquire("+1") > 0.0
    # Advance past the first call's expiry (1000 + 60 = 1060).
    clock.advance(31)  # t=1061
    assert limiter.try_acquire("+1") == 0.0


def test_limit_is_per_account():
    clock = _FakeClock()
    limiter = _limiter(clock, max_calls=1, window=60.0)

    assert limiter.try_acquire("+1") == 0.0
    assert limiter.try_acquire("+1") > 0.0  # +1 throttled
    # A different account has its own independent window.
    assert limiter.try_acquire("+2") == 0.0


def test_jitter_added_to_retry():
    clock = _FakeClock()
    limiter = ResolveRateLimiter(
        max_calls=1,
        window_sec=60.0,
        jitter_sec=5.0,
        time_func=clock,
        jitter_func=lambda _a, b: b,  # always max jitter
    )
    assert limiter.try_acquire("+1") == 0.0
    retry = limiter.try_acquire("+1")
    assert retry == 65.0  # 60s window + 5s jitter


def test_reset_clears_history():
    clock = _FakeClock()
    limiter = _limiter(clock, max_calls=1, window=60.0)

    assert limiter.try_acquire("+1") == 0.0
    assert limiter.try_acquire("+1") > 0.0
    limiter.reset("+1")
    assert limiter.try_acquire("+1") == 0.0

    # reset() with no arg clears all accounts.
    limiter.try_acquire("+2")
    limiter.reset()
    assert limiter.try_acquire("+2") == 0.0
