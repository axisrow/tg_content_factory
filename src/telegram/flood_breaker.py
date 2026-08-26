"""Per-(operation, phone) circuit breaker for Telegram calls (#1330/#1368).

Telegram publishes no numeric rate limits: ``messages.getDialogs`` does not even
list FLOOD_WAIT among its documented errors, and error 420 is defined only as
"the maximum allowed number of attempts to invoke the given method with the
given input parameters has been exceeded". There is nothing to calibrate a
proactive limit against.

So instead of guessing a rate, this reacts to the fact that Telegram is already
throttling us: once one operation has been flood-waited repeatedly on one
account, stop calling it for a while rather than keep hammering. On 2026-08-26 a
single operation (``telegram_warm_dialog_cache``) took 85 flood waits on one
account over a day and ended in a 14.8-hour ban -- every one of them logged at
INFO, so nothing stood out.

The key is (operation, phone), not the account alone: a broken warm-up on one
account must not stop message collection everywhere else.

pybreaker drives the state machine, following the notifier's proven usage
(``src/telegram/notifier.py``): it is a *synchronous* library, so the async call
runs outside ``CircuitBreaker.call`` and the breaker is advanced afterwards with
a trivial sync callable. See :meth:`record_flood` / :meth:`record_success`.
"""

from __future__ import annotations

import logging
import time

import pybreaker

from src.telegram.rate_limit_gate import TelegramRateLimitedError

logger = logging.getLogger(__name__)

# After this many flood waits on the same (operation, phone) the breaker opens.
# Mirrors the notifier's threshold (#553); deliberately small because each
# flood wait is itself already evidence of overuse.
DEFAULT_FLOOD_THRESHOLD = 3
# How long the operation stays blocked for that account before a single trial
# call is allowed through.
DEFAULT_COOLDOWN_SECONDS = 300.0


class TelegramOperationSuspendedError(TelegramRateLimitedError):
    """Raised instead of calling Telegram while the breaker is open.

    Deliberately a subclass of ``TelegramRateLimitedError``: ten call sites
    already handle "this operation is unavailable right now, move on" for the
    proactive gate, and the breaker means exactly the same thing to them. A new
    sibling type would have sailed past every one of those handlers and turned a
    protective refusal into a crashed collection pass.
    """

    def __init__(self, operation: str, phone: str, retry_after_sec: float) -> None:
        # ``category`` carries the operation so existing log lines that print it
        # stay meaningful ("Proactive %s rate limit ...").
        super().__init__(phone, operation, retry_after_sec)
        self.operation = operation


class _FloodProbeError(RuntimeError):
    """Marker fed to pybreaker to count a failure; never escapes this module."""


class _BreakerListener(pybreaker.CircuitBreakerListener):
    """Log state transitions -- the signal that was missing for four months."""

    def __init__(self, key: tuple[str, str], cooldown_seconds: float) -> None:
        self._operation, self._phone = key
        self._cooldown_seconds = cooldown_seconds

    def state_change(
        self,
        cb: pybreaker.CircuitBreaker,
        old_state: pybreaker.CircuitBreakerState | None,
        new_state: pybreaker.CircuitBreakerState,
    ) -> None:
        old_name = old_state.name if old_state is not None else None
        if new_state.name == pybreaker.STATE_OPEN:
            reason = (
                "half-open probe flooded again"
                if old_name == pybreaker.STATE_HALF_OPEN
                else f"{cb.fail_counter} flood waits"
            )
            logger.warning(
                "Telegram breaker OPEN: %s on %s (%s); suspending this operation for %.0fs",
                self._operation,
                self._phone,
                reason,
                self._cooldown_seconds,
            )
        elif new_state.name == pybreaker.STATE_CLOSED and old_name in (
            pybreaker.STATE_OPEN,
            pybreaker.STATE_HALF_OPEN,
        ):
            logger.info(
                "Telegram breaker closed: %s on %s resumed",
                self._operation,
                self._phone,
            )


class FloodCircuitBreaker:
    """Tracks repeated flood waits per (operation, phone) and suspends the pair.

    Not thread-safe by design: like the rate-limit gate it guards, it is driven
    from a single event loop. The check/record cycle is synchronous, so no await
    can interleave between them.
    """

    def __init__(
        self,
        *,
        threshold: int = DEFAULT_FLOOD_THRESHOLD,
        cooldown_seconds: float = DEFAULT_COOLDOWN_SECONDS,
        time_func=None,
    ) -> None:
        # Both knobs clamped: a non-positive cooldown expires instantly and
        # turns the breaker into a no-op, defeating the point (#955).
        self._threshold = max(1, int(threshold))
        self._cooldown_seconds = max(0.1, float(cooldown_seconds))
        self._time = time_func or time.monotonic
        self._breakers: dict[tuple[str, str], pybreaker.CircuitBreaker] = {}
        self._open_until: dict[tuple[str, str], float] = {}

    def _breaker_for(self, key: tuple[str, str]) -> pybreaker.CircuitBreaker:
        breaker = self._breakers.get(key)
        if breaker is None:
            breaker = pybreaker.CircuitBreaker(
                fail_max=self._threshold,
                reset_timeout=self._cooldown_seconds,
                success_threshold=1,
                name=f"{key[0]}@{key[1]}",
                throw_new_error_on_trip=False,
                listeners=[_BreakerListener(key, self._cooldown_seconds)],
            )
            self._breakers[key] = breaker
        return breaker

    def check(self, operation: str, phone: str | None) -> None:
        """Raise if this operation is suspended for this account.

        Calls with no phone are never suspended: the breaker is per-account and
        an unattributed call cannot be charged to one.
        """
        if not phone:
            return
        key = (operation, phone)
        breaker = self._breakers.get(key)
        if breaker is None or breaker.current_state != pybreaker.STATE_OPEN:
            return
        open_until = self._open_until.get(key)
        now = self._time()
        if open_until is not None and now < open_until:
            raise TelegramOperationSuspendedError(operation, phone, open_until - now)
        # Cooldown elapsed: flip to half-open directly rather than through the
        # open state's before_call, which would spend the trial slot on a no-op
        # probe. The upcoming real call becomes the genuine trial.
        breaker.half_open()

    def record_flood(self, operation: str, phone: str | None) -> None:
        """Count a flood wait against this (operation, phone)."""
        if not phone:
            return
        key = (operation, phone)
        breaker = self._breaker_for(key)
        try:
            breaker.call(self._raise_probe)
        except (_FloodProbeError, pybreaker.CircuitBreakerError):
            # Expected: the marker counts the failure, the trip error is the
            # breaker opening. Neither must reach the caller -- the real
            # FloodWaitError it is handling stays the exception that propagates.
            pass
        if breaker.current_state == pybreaker.STATE_OPEN:
            self._open_until[key] = self._time() + self._cooldown_seconds

    def record_success(self, operation: str, phone: str | None) -> None:
        """Clear accumulated flood waits after a call went through."""
        if not phone:
            return
        key = (operation, phone)
        breaker = self._breakers.get(key)
        if breaker is None or breaker.fail_counter == 0:
            # Nothing to reset. Skipping the no-op keeps the hot path free of
            # pybreaker bookkeeping on the overwhelmingly common success case.
            return
        try:
            breaker.call(lambda: None)
        except pybreaker.CircuitBreakerError:
            # Open with cooldown unexpired: check() would have raised, so this
            # is defence in depth only.
            return
        self._open_until.pop(key, None)

    def reset(self, operation: str | None = None, phone: str | None = None) -> None:
        """Drop breaker state (all, or one operation/phone slice)."""
        if operation is None and phone is None:
            self._breakers.clear()
            self._open_until.clear()
            return
        for key in [k for k in self._breakers if _matches(k, operation, phone)]:
            self._breakers.pop(key, None)
            self._open_until.pop(key, None)

    @staticmethod
    def _raise_probe() -> None:
        raise _FloodProbeError


def _matches(key: tuple[str, str], operation: str | None, phone: str | None) -> bool:
    return (operation is None or key[0] == operation) and (phone is None or key[1] == phone)
