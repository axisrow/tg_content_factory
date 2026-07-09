from __future__ import annotations

import asyncio
import logging
import time

import aiohttp
import pybreaker

from src.database.bundles import NotificationBundle
from src.services.notification_target_service import NotificationTargetService

logger = logging.getLogger(__name__)

_BOT_API_URL = "https://api.telegram.org/bot{token}/sendMessage"

# Circuit-breaker defaults (issue #553): after this many consecutive failures
# the notifier stops attempting sends for a cooldown window instead of logging
# the same persistent error indefinitely.
_DEFAULT_FAILURE_THRESHOLD = 3
_DEFAULT_COOLDOWN_SECONDS = 3600.0


class _NotifierProbeError(RuntimeError):
    """Sentinel raised to drive the pybreaker state machine on a failed send.

    ``_attempt_send`` already swallows the real exception and returns a bool, so
    we synthesise this marker exception purely to feed pybreaker's failure path
    (``CircuitBreaker.call`` counts a failure only when the wrapped callable
    raises). It never escapes the notifier.
    """


class _NotifierBreakerListener(pybreaker.CircuitBreakerListener):
    """Bridge pybreaker state transitions back to the notifier.

    Two jobs: (1) emit the exact "entering degraded state" warning the
    hand-rolled breaker logged on every open (#553), and (2) stamp/clear the
    cooldown deadline on a monotonic clock so the notifier never has to read
    pybreaker's private storage to decide whether the cooldown has elapsed.

    Note: the "recovered" INFO is NOT emitted here. The old breaker logged it on
    *any* successful send that followed accumulated failures — including
    below-threshold streaks that never tripped the circuit, which produce no
    state transition. ``Notifier._record_outcome`` owns that log instead.
    """

    def __init__(self, notifier: Notifier, cooldown_seconds: float) -> None:
        self._notifier = notifier
        self._cooldown_seconds = cooldown_seconds

    def state_change(
        self,
        cb: pybreaker.CircuitBreaker,
        old_state: pybreaker.CircuitBreakerState | None,
        new_state: pybreaker.CircuitBreakerState,
    ) -> None:
        old_name = old_state.name if old_state is not None else None
        new_name = new_state.name
        if new_name == pybreaker.STATE_OPEN:
            self._notifier._degraded_until = time.monotonic() + self._cooldown_seconds
            # Mirror the previous warning wording/format so log-scraping and the
            # "no more than N errors per outage" guarantee (#553) are unchanged.
            if old_name == pybreaker.STATE_HALF_OPEN:
                reason = "half-open probe failed"
            else:
                reason = f"{cb.fail_counter} consecutive failures"
            logger.warning(
                "Notifier entering degraded state (%s); suppressing further attempts for %.0fs",
                reason,
                self._cooldown_seconds,
            )
        elif new_name == pybreaker.STATE_CLOSED and old_name in (
            pybreaker.STATE_OPEN,
            pybreaker.STATE_HALF_OPEN,
        ):
            self._notifier._degraded_until = None


class Notifier:
    """Send notifications to admin via Telegram."""

    def __init__(
        self,
        target_service: NotificationTargetService,
        admin_chat_id: int | None,
        notification_bundle: NotificationBundle | None = None,
        *,
        failure_threshold: int = _DEFAULT_FAILURE_THRESHOLD,
        cooldown_seconds: float = _DEFAULT_COOLDOWN_SECONDS,
    ):
        self._target_service = target_service
        self._admin_chat_id = admin_chat_id
        self._notification_bundle = notification_bundle
        self._cached_me_id: int | None = None
        # Serialises the whole gate → send → record cycle of notify(). The
        # Notifier is a long-lived instance shared by concurrent collection
        # workers (each can alert on FloodWait), and pybreaker's own threading
        # lock cannot bridge the `await` between the breaker check and the
        # outcome record. Without this, two coroutines could both pass the
        # half-open gate (double-probe), or one could re-open the breaker while
        # another is mid-send and then trip an uncaught CircuitBreakerError on
        # the success path (#955 cycle-review).
        self._send_lock = asyncio.Lock()
        # Circuit-breaker state. Both knobs are clamped to sane minimums: a
        # non-positive cooldown would expire immediately and turn the breaker
        # into a no-op (degraded state never holds), defeating the whole point.
        failure_threshold = max(1, failure_threshold)
        self._cooldown_seconds = max(0.1, cooldown_seconds)
        # Monotonic deadline until which sends are skipped while degraded. Kept
        # in sync by the listener on every open/close transition; the cooldown
        # check reads this instead of pybreaker's private opened_at storage.
        self._degraded_until: float | None = None
        # pybreaker drives the state machine; the previously bespoke semantics
        # map 1:1 — open after `fail_max` consecutive failures, hold the circuit
        # open for `reset_timeout`, then allow a single half-open trial call
        # (`success_threshold=1`) that closes on success or re-opens on failure.
        self._breaker = pybreaker.CircuitBreaker(
            fail_max=failure_threshold,
            reset_timeout=self._cooldown_seconds,
            success_threshold=1,
            name="notifier",
            throw_new_error_on_trip=False,
            listeners=[_NotifierBreakerListener(self, self._cooldown_seconds)],
        )

    @property
    def admin_chat_id(self) -> int | None:
        return self._admin_chat_id

    @property
    def is_degraded(self) -> bool:
        """True while the circuit breaker is open and the cooldown still holds."""
        if self._degraded_until is None:
            return False
        return time.monotonic() < self._degraded_until

    def invalidate_me_cache(self) -> None:
        """Invalidate the cached me.id. Call when the notification account changes."""
        self._cached_me_id = None

    async def notify(self, text: str) -> bool:
        # The send is async and already returns a bool (it swallows its own
        # exceptions), so it runs outside pybreaker; we only feed the outcome
        # through the synchronous state machine. pybreaker owns open/half-open/
        # cooldown exactly as the previous hand-rolled logic did.
        #
        # The whole cycle is serialised so the breaker can never observe an
        # interleaved state change across the `await` below: that would let two
        # callers both probe in half-open, or re-open the breaker mid-send and
        # surface an uncaught CircuitBreakerError to the caller (#955).
        async with self._send_lock:
            if not self._enter_attempt():
                # Degraded and still within cooldown: skip (no log spam).
                return False

            # _enter_attempt may have flipped the breaker to half-open, so the
            # outcome MUST be recorded even if the send is cancelled — otherwise
            # the breaker is stranded half-open and admits every later send with
            # no cooldown. Treat cancellation as a failed probe (re-opens).
            ok = False
            try:
                ok = await self._attempt_send(text)
            finally:
                self._record_outcome(ok)
            return ok

    def _enter_attempt(self) -> bool:
        """Decide whether a send may be attempted, advancing the breaker.

        Returns ``True`` if the circuit is closed/half-open (a real send should
        run), ``False`` if it is open and the cooldown has not elapsed (skip).
        When the cooldown has elapsed this transitions the breaker to half-open
        so the next send becomes the single half-open trial call.
        """
        if self._breaker.current_state != pybreaker.STATE_OPEN:
            return True
        if self._degraded_until is not None and time.monotonic() < self._degraded_until:
            return False
        # Cooldown elapsed: flip to half-open directly (NOT via the open state's
        # before_call, which would consume the trial slot on a no-op probe and
        # close the circuit). The upcoming real send becomes the genuine trial:
        # success closes the breaker, failure re-opens it immediately.
        self._breaker.half_open()
        return True

    def _record_outcome(self, ok: bool) -> None:
        """Advance the breaker state machine for the just-completed send."""
        if ok:
            # Recovery log: the hand-rolled breaker logged "recovered" on ANY
            # successful send that followed accumulated failures — including a
            # below-threshold streak (e.g. fail, fail, success at threshold=3)
            # where the circuit never opened, so no state transition fires. The
            # listener only sees open/half-open → closed transitions, so we own
            # the recovery log here (and the listener does NOT duplicate it) to
            # preserve the exact #553 log contract 1:1.
            had_failures = self._breaker.fail_counter != 0 or self._degraded_until is not None
            try:
                self._breaker.call(lambda: None)
            except pybreaker.CircuitBreakerError:
                # Defence in depth: the _send_lock means the breaker should not
                # be open here, but pybreaker raises if it ever is (open + cooldown
                # not elapsed). Swallow it so notify() keeps its bool contract.
                return
            if had_failures:
                logger.info("Notifier recovered; resuming notifications")
        else:
            try:
                self._breaker.call(self._raise_probe_error)
            except (_NotifierProbeError, pybreaker.CircuitBreakerError):
                # Expected: the marker (counted as a failure) or the trip error
                # are both consumed here so notify() never leaks an exception.
                pass

    @staticmethod
    def _raise_probe_error() -> None:
        raise _NotifierProbeError

    async def _attempt_send(self, text: str) -> bool:
        try:
            # Fast path: if me.id is cached and a bot is configured, skip the
            # Telegram client entirely — _send_via_bot_api is a pure HTTP call.
            if self._notification_bundle is not None and self._cached_me_id is not None:
                bot = await self._notification_bundle.get_bot(self._cached_me_id)
                if bot is not None:
                    return await _send_via_bot_api(bot.bot_token, self._cached_me_id, text)

            # Slow path: need a client either to populate me.id or to send directly.
            async with self._target_service.use_client() as (client, _phone):
                if self._notification_bundle is not None:
                    if self._cached_me_id is None:
                        me = await asyncio.wait_for(client.get_me(), timeout=15.0)
                        self._cached_me_id = me.id
                    bot = await self._notification_bundle.get_bot(self._cached_me_id)
                    if bot is not None:
                        return await _send_via_bot_api(bot.bot_token, self._cached_me_id, text)
                    logger.warning(
                        "No bot found for account %s, falling back to direct message "
                        "(push notifications will not be delivered)",
                        self._cached_me_id,
                    )
                target = self._admin_chat_id or "me"
                # The direct send is bounded by a 30s timeout (issue #1239). The
                # timeout is REQUIRED, not optional: with connection_retries=None
                # (auth.py, backends.py) a send on a dead connection would hang
                # forever and, because notify() holds _send_lock across the await,
                # freeze every later notification and strand the breaker. BUT a
                # client-side timeout cancels only the local wait — the MTProto
                # request may already have reached Telegram and the message may be
                # delivered. So a timeout HERE is NOT a known failure: returning
                # False would report the send as failed, and callers that retry on
                # False (notification_matcher "will retry next pass",
                # draft_notification_service) would re-send an already-delivered
                # message → duplicate. We instead treat the timed-out send as an
                # UNCONFIRMED delivery and report success so no retry duplicates
                # it. This mirrors the #1239/#1253 decision for publish_service and
                # the #795 decision that dropped wait_for from resolve_entity — the
                # read-only get_me above is reversible and stays a plain failure.
                try:
                    await asyncio.wait_for(client.send_message(target, text), timeout=30.0)
                except asyncio.TimeoutError:
                    logger.error(
                        "Timeout sending notification to %s — delivery unconfirmed; "
                        "not retrying to avoid a duplicate message",
                        target,
                    )
            return True
        except asyncio.CancelledError:
            raise
        except Exception as e:
            logger.error("Failed to send notification: %s", e)
            return False


async def _send_via_bot_api(token: str, chat_id: int, text: str) -> bool:
    url = _BOT_API_URL.format(token=token)
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                url,
                json={"chat_id": chat_id, "text": text},
                timeout=aiohttp.ClientTimeout(total=30),
            ) as resp:
                data = await resp.json()
                if not data.get("ok"):
                    logger.error("Bot API error: %s", data)
                    return False
        return True
    except asyncio.CancelledError:
        raise
    except Exception as e:
        logger.error("Bot API call failed: %s", e)
        return False
