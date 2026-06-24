"""Tests for notifier delivery paths, error handling, and bot API integration."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pybreaker
import pytest

from src.database.bundles import NotificationBundle
from src.models import NotificationBot
from src.telegram.notifier import Notifier, _send_via_bot_api
from tests.helpers import wait_until


@pytest.fixture
def mock_target_service():
    """Create a mock NotificationTargetService."""
    service = MagicMock()
    service.use_client = MagicMock()
    return service


@pytest.fixture
def mock_notification_bundle():
    """Create a mock NotificationBundle."""
    bundle = MagicMock(spec=NotificationBundle)
    bundle.get_bot = AsyncMock(return_value=None)
    return bundle


class TestNotifierFastPath:
    """Tests for Notifier fast path with cached me.id and bot."""

    @pytest.mark.anyio
    async def test_notify_with_cached_me_id_and_bot(
        self,
        mock_target_service,
        mock_notification_bundle,
    ):
        """Test fast path when me.id is cached and bot is available."""
        bot = NotificationBot(
            tg_user_id=123,
            tg_username="test_user",
            bot_id=456,
            bot_username="test_bot",
            bot_token="123456:ABC-DEF",
        )
        mock_notification_bundle.get_bot = AsyncMock(return_value=bot)

        notifier = Notifier(
            target_service=mock_target_service,
            admin_chat_id=789,
            notification_bundle=mock_notification_bundle,
        )
        # Simulate cached me.id
        notifier._cached_me_id = 123

        with patch(
            "src.telegram.notifier._send_via_bot_api",
            new_callable=AsyncMock,
            return_value=True,
        ) as mock_send:
            result = await notifier.notify("Test message")

        assert result is True
        mock_send.assert_awaited_once_with("123456:ABC-DEF", 123, "Test message")
        # Should not use client since fast path succeeded
        mock_target_service.use_client.assert_not_called()

    @pytest.mark.anyio
    async def test_notify_with_cached_me_id_but_no_bot_falls_back(self, mock_target_service, mock_notification_bundle):
        """Test fallback when me.id is cached but bot lookup returns None."""
        mock_notification_bundle.get_bot = AsyncMock(return_value=None)

        mock_client = MagicMock()
        mock_client.get_me = AsyncMock(return_value=SimpleNamespace(id=123))
        mock_client.send_message = AsyncMock()

        mock_cm = AsyncMock()
        mock_cm.__aenter__ = AsyncMock(return_value=(mock_client, "+70001112233"))
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_target_service.use_client.return_value = mock_cm

        notifier = Notifier(
            target_service=mock_target_service,
            admin_chat_id=789,
            notification_bundle=mock_notification_bundle,
        )
        notifier._cached_me_id = 123

        result = await notifier.notify("Test message")

        assert result is True
        mock_client.send_message.assert_awaited_once_with(789, "Test message")


class TestNotifierSlowPath:
    """Tests for Notifier slow path that requires client connection."""

    @pytest.mark.anyio
    async def test_notify_without_cached_me_id_fetches_it(self, mock_target_service, mock_notification_bundle):
        """Test that notifier fetches and caches me.id when not cached."""
        bot = NotificationBot(
            tg_user_id=123,
            tg_username="test_user",
            bot_id=456,
            bot_username="test_bot",
            bot_token="123456:ABC-DEF",
        )
        mock_notification_bundle.get_bot = AsyncMock(return_value=bot)

        mock_client = MagicMock()
        mock_client.get_me = AsyncMock(return_value=SimpleNamespace(id=123))
        mock_client.send_message = AsyncMock()

        mock_cm = AsyncMock()
        mock_cm.__aenter__ = AsyncMock(return_value=(mock_client, "+70001112233"))
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_target_service.use_client.return_value = mock_cm

        notifier = Notifier(
            target_service=mock_target_service,
            admin_chat_id=789,
            notification_bundle=mock_notification_bundle,
        )
        # me.id is not cached initially
        assert notifier._cached_me_id is None

        with patch(
            "src.telegram.notifier._send_via_bot_api",
            new_callable=AsyncMock,
            return_value=True,
        ):
            result = await notifier.notify("Test message")

        assert result is True
        # Should have cached the me.id
        assert notifier._cached_me_id == 123
        mock_client.get_me.assert_awaited_once()

    @pytest.mark.anyio
    async def test_notify_fallback_to_client_when_no_bot(self, mock_target_service, mock_notification_bundle):
        """Test fallback to client.send_message when no bot is configured."""
        mock_notification_bundle.get_bot = AsyncMock(return_value=None)

        mock_client = MagicMock()
        mock_client.get_me = AsyncMock(return_value=SimpleNamespace(id=123))
        mock_client.send_message = AsyncMock()

        mock_cm = AsyncMock()
        mock_cm.__aenter__ = AsyncMock(return_value=(mock_client, "+70001112233"))
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_target_service.use_client.return_value = mock_cm

        notifier = Notifier(
            target_service=mock_target_service,
            admin_chat_id=789,
            notification_bundle=mock_notification_bundle,
        )

        result = await notifier.notify("Test message")

        assert result is True
        mock_client.send_message.assert_awaited_once_with(789, "Test message")

    @pytest.mark.anyio
    async def test_notify_admin_chat_id_me(self, mock_target_service, mock_notification_bundle):
        """Test sending to 'me' when admin_chat_id is None."""
        mock_notification_bundle.get_bot = AsyncMock(return_value=None)

        mock_client = MagicMock()
        mock_client.get_me = AsyncMock(return_value=SimpleNamespace(id=123))
        mock_client.send_message = AsyncMock()

        mock_cm = AsyncMock()
        mock_cm.__aenter__ = AsyncMock(return_value=(mock_client, "+70001112233"))
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_target_service.use_client.return_value = mock_cm

        notifier = Notifier(
            target_service=mock_target_service,
            admin_chat_id=None,  # No admin_chat_id, should use "me"
            notification_bundle=mock_notification_bundle,
        )

        result = await notifier.notify("Test message")

        assert result is True
        mock_client.send_message.assert_awaited_once_with("me", "Test message")


class TestNotifierWithoutBundle:
    """Tests for Notifier when notification_bundle is None."""

    @pytest.mark.anyio
    async def test_notify_without_bundle_uses_client_directly(self, mock_target_service):
        """Test that without bundle, notifier uses client.send_message directly."""
        mock_client = MagicMock()
        mock_client.send_message = AsyncMock()

        mock_cm = AsyncMock()
        mock_cm.__aenter__ = AsyncMock(return_value=(mock_client, "+70001112233"))
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_target_service.use_client.return_value = mock_cm

        notifier = Notifier(
            target_service=mock_target_service,
            admin_chat_id=789,
            notification_bundle=None,  # No bundle
        )

        result = await notifier.notify("Test message")

        assert result is True
        mock_client.send_message.assert_awaited_once_with(789, "Test message")


class TestNotifierErrorHandling:
    """Tests for Notifier error handling."""

    @pytest.mark.anyio
    async def test_notify_cancelled_error_propagates(
        self,
        mock_target_service,
        mock_notification_bundle,
    ):
        """Test that CancelledError is re-raised, not swallowed."""
        mock_client = MagicMock()
        mock_client.get_me = AsyncMock(side_effect=asyncio.CancelledError)

        mock_cm = AsyncMock()
        mock_cm.__aenter__ = AsyncMock(return_value=(mock_client, "+70001112233"))
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_target_service.use_client.return_value = mock_cm

        notifier = Notifier(
            target_service=mock_target_service,
            admin_chat_id=789,
            notification_bundle=mock_notification_bundle,
        )

        with pytest.raises(asyncio.CancelledError):
            await notifier.notify("Test message")

    @pytest.mark.anyio
    async def test_notify_general_exception_returns_false(
        self,
        mock_target_service,
        mock_notification_bundle,
    ):
        """Test that general exceptions are caught and result in False return."""
        mock_client = MagicMock()
        mock_client.get_me = AsyncMock(side_effect=RuntimeError("Connection failed"))

        mock_cm = AsyncMock()
        mock_cm.__aenter__ = AsyncMock(return_value=(mock_client, "+70001112233"))
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_target_service.use_client.return_value = mock_cm

        notifier = Notifier(
            target_service=mock_target_service,
            admin_chat_id=789,
            notification_bundle=mock_notification_bundle,
        )

        result = await notifier.notify("Test message")

        assert result is False


class TestNotifierCircuitBreaker:
    """Tests for the circuit breaker that stops retrying persistent failures (#553)."""

    def _failing_notifier(self, mock_target_service, *, threshold=3, cooldown=3600.0):
        mock_client = MagicMock()
        mock_client.get_me = AsyncMock(side_effect=RuntimeError("account deleted"))
        mock_cm = AsyncMock()
        mock_cm.__aenter__ = AsyncMock(return_value=(mock_client, "+70001112233"))
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_target_service.use_client.return_value = mock_cm
        return Notifier(
            target_service=mock_target_service,
            admin_chat_id=789,
            notification_bundle=MagicMock(spec=NotificationBundle),
            failure_threshold=threshold,
            cooldown_seconds=cooldown,
        )

    @pytest.mark.anyio
    async def test_opens_after_threshold_and_stops_attempting(self, mock_target_service):
        notifier = self._failing_notifier(mock_target_service, threshold=3)

        for _ in range(3):
            assert await notifier.notify("x") is False

        # use_client invoked exactly threshold times so far
        assert mock_target_service.use_client.call_count == 3
        assert notifier.is_degraded is True

        # Further calls are skipped entirely — no new client acquisition.
        assert await notifier.notify("x") is False
        assert await notifier.notify("x") is False
        assert mock_target_service.use_client.call_count == 3

    @pytest.mark.anyio
    async def test_recovers_after_cooldown(self, mock_target_service):
        notifier = self._failing_notifier(mock_target_service, threshold=2, cooldown=100.0)

        with patch("src.telegram.notifier.time.monotonic", return_value=1000.0):
            assert await notifier.notify("x") is False
            assert await notifier.notify("x") is False
            assert notifier.is_degraded is True
            # Still degraded just before cooldown elapses.
            assert await notifier.notify("x") is False
            assert mock_target_service.use_client.call_count == 2

        # After cooldown the breaker half-opens and attempts again.
        with patch("src.telegram.notifier.time.monotonic", return_value=1101.0):
            assert await notifier.notify("x") is False
            assert mock_target_service.use_client.call_count == 3

    @pytest.mark.anyio
    async def test_success_resets_failure_counter(self, mock_target_service):
        mock_client = MagicMock()
        mock_client.get_me = AsyncMock(side_effect=RuntimeError("boom"))
        mock_cm = AsyncMock()
        mock_cm.__aenter__ = AsyncMock(return_value=(mock_client, "+70001112233"))
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_target_service.use_client.return_value = mock_cm
        notifier = Notifier(
            target_service=mock_target_service,
            admin_chat_id=789,
            notification_bundle=None,  # no bundle → send_message path
            failure_threshold=3,
        )

        # Two failures, then succeed → counter resets, breaker never opens.
        assert await notifier.notify("x") is False
        assert await notifier.notify("x") is False
        assert notifier._breaker.fail_counter == 2

        mock_client.send_message = AsyncMock(return_value=None)
        mock_client.get_me = AsyncMock(return_value=SimpleNamespace(id=42))
        assert await notifier.notify("x") is True
        assert notifier._breaker.fail_counter == 0
        assert notifier.is_degraded is False

    @pytest.mark.anyio
    async def test_below_threshold_recovery_emits_log(self, mock_target_service, caplog):
        """Regression (#955 cycle-review): the hand-rolled breaker logged
        'recovered' on a successful send after ANY accumulated failures, even a
        below-threshold streak that never opened the circuit. The pybreaker
        migration must preserve that — a state-change listener alone misses it
        because no transition fires when the circuit never opened.
        """
        mock_client = MagicMock()
        mock_client.get_me = AsyncMock(side_effect=RuntimeError("boom"))
        mock_cm = AsyncMock()
        mock_cm.__aenter__ = AsyncMock(return_value=(mock_client, "+70001112233"))
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_target_service.use_client.return_value = mock_cm
        notifier = Notifier(
            target_service=mock_target_service,
            admin_chat_id=789,
            notification_bundle=None,  # direct send_message path
            failure_threshold=3,
        )

        # Two failures (below threshold=3 → breaker never opens), then success.
        assert await notifier.notify("x") is False
        assert await notifier.notify("x") is False
        assert notifier.is_degraded is False  # never opened

        mock_client.get_me = AsyncMock(return_value=SimpleNamespace(id=42))
        mock_client.send_message = AsyncMock(return_value=None)
        with caplog.at_level("INFO", logger="src.telegram.notifier"):
            assert await notifier.notify("x") is True

        assert any("recovered" in r.message.lower() for r in caplog.records), (
            "below-threshold recovery must still log 'recovered'"
        )

    @pytest.mark.anyio
    async def test_clean_success_does_not_log_recovery(self, mock_target_service, caplog):
        """No prior failures → no 'recovered' line (matches the old _on_success
        guard: log only when failures were accumulated)."""
        mock_client = MagicMock()
        mock_client.get_me = AsyncMock(return_value=SimpleNamespace(id=42))
        mock_client.send_message = AsyncMock(return_value=None)
        mock_cm = AsyncMock()
        mock_cm.__aenter__ = AsyncMock(return_value=(mock_client, "+70001112233"))
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_target_service.use_client.return_value = mock_cm
        notifier = Notifier(
            target_service=mock_target_service,
            admin_chat_id=789,
            notification_bundle=None,
            failure_threshold=3,
        )

        with caplog.at_level("INFO", logger="src.telegram.notifier"):
            assert await notifier.notify("x") is True
            assert await notifier.notify("x") is True

        assert not any("recovered" in r.message.lower() for r in caplog.records), (
            "a clean success with no prior failures must not log recovery"
        )

    @pytest.mark.anyio
    async def test_recovery_after_cooldown_emits_log(self, mock_target_service, caplog):
        """The 'recovered' INFO must fire when a half-open probe succeeds."""
        notifier = self._failing_notifier(mock_target_service, threshold=2, cooldown=100.0)

        with patch("src.telegram.notifier.time.monotonic", return_value=1000.0):
            assert await notifier.notify("x") is False
            assert await notifier.notify("x") is False
            assert notifier.is_degraded is True

        # Make the underlying send succeed for the half-open probe.
        mock_client = MagicMock()
        mock_client.get_me = AsyncMock(return_value=SimpleNamespace(id=42))
        mock_client.send_message = AsyncMock(return_value=None)
        mock_cm = AsyncMock()
        mock_cm.__aenter__ = AsyncMock(return_value=(mock_client, "+70001112233"))
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_target_service.use_client.return_value = mock_cm
        notifier._notification_bundle = None  # take the direct send_message path

        with caplog.at_level("INFO", logger="src.telegram.notifier"):
            with patch("src.telegram.notifier.time.monotonic", return_value=1101.0):
                assert await notifier.notify("x") is True

        assert any("recovered" in r.message.lower() for r in caplog.records), (
            "recovery from degraded state must be logged"
        )
        assert notifier.is_degraded is False
        assert notifier._breaker.fail_counter == 0

    @pytest.mark.anyio
    async def test_failed_half_open_probe_reopens_immediately(self, mock_target_service):
        """A failed single half-open probe must re-open the breaker at once,
        not grant another full failure_threshold window of error logs (#553)."""
        notifier = self._failing_notifier(mock_target_service, threshold=3, cooldown=100.0)

        with patch("src.telegram.notifier.time.monotonic", return_value=1000.0):
            for _ in range(3):
                assert await notifier.notify("x") is False
            assert notifier.is_degraded is True
            assert mock_target_service.use_client.call_count == 3

        # Cooldown elapsed → exactly one probe attempt, which fails → re-degraded.
        with patch("src.telegram.notifier.time.monotonic", return_value=1101.0):
            assert await notifier.notify("x") is False
            assert mock_target_service.use_client.call_count == 4  # one probe only
            assert notifier.is_degraded is True
            # Immediately suppressed again — no extra attempts before re-degrading.
            assert await notifier.notify("x") is False
            assert mock_target_service.use_client.call_count == 4

    def test_nonpositive_cooldown_is_clamped(self, mock_target_service):
        """cooldown_seconds <= 0 would make the breaker a no-op; it is clamped."""
        notifier = Notifier(
            target_service=mock_target_service,
            admin_chat_id=789,
            notification_bundle=None,
            failure_threshold=2,
            cooldown_seconds=0,
        )
        assert notifier._cooldown_seconds >= 0.1

    @pytest.mark.anyio
    async def test_underlying_breaker_walks_closed_open_halfopen_closed(self, mock_target_service):
        """The pybreaker state machine transitions through the expected states.

        This pins the migration to pybreaker (#955): the visible behaviour
        (is_degraded, skip-while-open, single half-open probe) is unchanged, and
        the underlying breaker reports the canonical closed → open → closed path.
        """
        notifier = self._failing_notifier(mock_target_service, threshold=2, cooldown=100.0)
        assert notifier._breaker.current_state == pybreaker.STATE_CLOSED

        with patch("src.telegram.notifier.time.monotonic", return_value=1000.0):
            await notifier.notify("x")
            await notifier.notify("x")
            assert notifier._breaker.current_state == pybreaker.STATE_OPEN

        # After cooldown, a *successful* half-open probe closes the circuit and
        # resets the failure counter (success_threshold=1).
        mock_client = MagicMock()
        mock_client.get_me = AsyncMock(return_value=SimpleNamespace(id=42))
        mock_client.send_message = AsyncMock(return_value=None)
        mock_cm = AsyncMock()
        mock_cm.__aenter__ = AsyncMock(return_value=(mock_client, "+70001112233"))
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_target_service.use_client.return_value = mock_cm
        notifier._notification_bundle = None  # direct send_message path

        with patch("src.telegram.notifier.time.monotonic", return_value=1101.0):
            assert await notifier.notify("x") is True
        assert notifier._breaker.current_state == pybreaker.STATE_CLOSED
        assert notifier._breaker.fail_counter == 0

    @pytest.mark.anyio
    async def test_notify_skips_while_open_returns_false(self, mock_target_service):
        """While the circuit is open within cooldown, notify() returns False
        without attempting a send (no log spam)."""
        notifier = self._failing_notifier(mock_target_service, threshold=1, cooldown=100.0)

        # First failure trips the breaker (threshold=1).
        assert await notifier.notify("x") is False
        assert notifier.is_degraded is True
        # While open, every call returns False rather than raising, and never
        # acquires a client (the send is skipped at the gate).
        attempts_after_open = mock_target_service.use_client.call_count
        for _ in range(3):
            assert await notifier.notify("x") is False
        assert mock_target_service.use_client.call_count == attempts_after_open

    @pytest.mark.anyio
    async def test_record_outcome_suppresses_breaker_error_on_success_path(self, mock_target_service):
        """Defence-in-depth (#955 cycle-review A1): if a successful send is
        recorded while the breaker is OPEN (a race where another caller re-opened
        it mid-send), pybreaker raises CircuitBreakerError from the success-path
        breaker.call — it must be swallowed, not leaked out of the notifier.
        """
        notifier = Notifier(
            target_service=mock_target_service,
            admin_chat_id=789,
            notification_bundle=None,
            failure_threshold=1,
            cooldown_seconds=100.0,
        )
        # Drive the breaker OPEN directly (simulates a concurrent re-open while a
        # send that began in CLOSED/HALF_OPEN was still in flight).
        notifier._breaker.open()
        assert notifier._breaker.current_state == pybreaker.STATE_OPEN

        # Recording a success against an open breaker must NOT raise.
        notifier._record_outcome(True)  # would raise CircuitBreakerError if unguarded

    @pytest.mark.anyio
    async def test_concurrent_notify_single_half_open_probe(self, mock_target_service):
        """#955 cycle-review A2: concurrent notify() calls must not double-probe
        in half-open. The _send_lock serialises the gate→send→record cycle, so a
        persistent outage emits exactly one probe send per cooldown window."""
        # A send that blocks until released, so we can hold two callers in flight.
        gate = asyncio.Event()

        mock_client = MagicMock()
        mock_client.get_me = AsyncMock(return_value=SimpleNamespace(id=42))

        async def _blocking_send(*_a, **_k):
            await gate.wait()
            raise RuntimeError("still down")

        mock_client.send_message = AsyncMock(side_effect=_blocking_send)
        mock_cm = AsyncMock()
        mock_cm.__aenter__ = AsyncMock(return_value=(mock_client, "+70001112233"))
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_target_service.use_client.return_value = mock_cm

        notifier = Notifier(
            target_service=mock_target_service,
            admin_chat_id=789,
            notification_bundle=None,
            failure_threshold=1,
            cooldown_seconds=100.0,
        )

        with patch("src.telegram.notifier.time.monotonic", return_value=1000.0):
            # Trip the breaker open with one failure (gate already releasable).
            gate.set()
            assert await notifier.notify("x") is False
            assert notifier.is_degraded is True

        # Cooldown elapsed: launch two concurrent probes. The lock must admit
        # only one real send; the second waits, then sees the breaker open again
        # (the first probe failed) and skips.
        gate.clear()
        with patch("src.telegram.notifier.time.monotonic", return_value=1101.0):
            before = mock_target_service.use_client.call_count
            task_a = asyncio.create_task(notifier.notify("x"))
            task_b = asyncio.create_task(notifier.notify("x"))
            # Wait until the first caller has grabbed the lock and reached the
            # send (use_client opened) — then it is parked on gate.wait().
            await wait_until(
                lambda: mock_target_service.use_client.call_count > before
            )
            gate.set()  # release the blocked probe
            results = await asyncio.gather(task_a, task_b)
            after = mock_target_service.use_client.call_count

        assert results == [False, False]
        # Exactly ONE real send was attempted across both concurrent callers.
        assert after - before == 1

    @pytest.mark.anyio
    async def test_cancelled_send_does_not_strand_half_open(self, mock_target_service):
        """#955 cycle-review A4: if notify() is cancelled mid-send while the
        breaker is half-open, the outcome is still recorded (treated as a failed
        probe) so the breaker re-opens instead of being stranded half-open and
        admitting every later send with no cooldown."""
        started = asyncio.Event()

        mock_client = MagicMock()
        mock_client.get_me = AsyncMock(return_value=SimpleNamespace(id=42))

        async def _hang(*_a, **_k):
            started.set()
            await asyncio.Event().wait()  # never completes → must be cancelled

        mock_client.send_message = AsyncMock(side_effect=_hang)
        mock_cm = AsyncMock()
        mock_cm.__aenter__ = AsyncMock(return_value=(mock_client, "+70001112233"))
        mock_cm.__aexit__ = AsyncMock(return_value=None)
        mock_target_service.use_client.return_value = mock_cm

        notifier = Notifier(
            target_service=mock_target_service,
            admin_chat_id=789,
            notification_bundle=None,
            failure_threshold=1,
            cooldown_seconds=100.0,
        )

        with patch("src.telegram.notifier.time.monotonic", return_value=1000.0):
            # Trip open with one failed (hanging) send that we cancel.
            task = asyncio.create_task(notifier.notify("x"))
            await started.wait()
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
            # The cancelled send was recorded as a failure → breaker is OPEN,
            # not stranded half-open.
            assert notifier.is_degraded is True
            assert notifier._breaker.current_state == pybreaker.STATE_OPEN


class TestNotifierInvalidateCache:
    """Tests for cache invalidation."""

    def test_invalidate_me_cache(self, mock_target_service, mock_notification_bundle):
        """Test that invalidate_me_cache clears the cached me.id."""
        notifier = Notifier(
            target_service=mock_target_service,
            admin_chat_id=789,
            notification_bundle=mock_notification_bundle,
        )
        notifier._cached_me_id = 123

        notifier.invalidate_me_cache()

        assert notifier._cached_me_id is None

    def test_admin_chat_id_property(self, mock_target_service, mock_notification_bundle):
        """Test admin_chat_id property accessor."""
        notifier = Notifier(
            target_service=mock_target_service,
            admin_chat_id=789,
            notification_bundle=mock_notification_bundle,
        )

        assert notifier.admin_chat_id == 789


class TestSendViaBotApi:
    """Tests for _send_via_bot_api helper function."""

    @pytest.mark.anyio
    async def test_send_via_bot_api_success(self):
        """Test successful bot API call."""
        mock_response = AsyncMock()
        mock_response.json = AsyncMock(return_value={"ok": True, "result": {}})

        # session.post(...) is used as async context manager
        mock_post_context = AsyncMock()
        mock_post_context.__aenter__ = AsyncMock(return_value=mock_response)
        mock_post_context.__aexit__ = AsyncMock(return_value=None)

        mock_session = MagicMock()
        mock_session.post = MagicMock(return_value=mock_post_context)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=None)

        with patch("src.telegram.notifier.aiohttp.ClientSession", return_value=mock_session):
            result = await _send_via_bot_api("123456:ABC-DEF", 789, "Test message")

        assert result is True

    @pytest.mark.anyio
    async def test_send_via_bot_api_error_response(self):
        """Test bot API call when response has ok=false."""
        mock_response = AsyncMock()
        mock_response.json = AsyncMock(return_value={"ok": False, "error_code": 400, "description": "Bad Request"})

        # session.post(...) is used as async context manager
        mock_post_context = AsyncMock()
        mock_post_context.__aenter__ = AsyncMock(return_value=mock_response)
        mock_post_context.__aexit__ = AsyncMock(return_value=None)

        mock_session = MagicMock()
        mock_session.post = MagicMock(return_value=mock_post_context)
        mock_session.__aenter__ = AsyncMock(return_value=mock_session)
        mock_session.__aexit__ = AsyncMock(return_value=None)

        with patch("src.telegram.notifier.aiohttp.ClientSession", return_value=mock_session):
            result = await _send_via_bot_api("123456:ABC-DEF", 789, "Test message")

        assert result is False

    @pytest.mark.anyio
    async def test_send_via_bot_api_network_error(self):
        """Test bot API call when network error occurs during ClientSession creation."""
        # The error happens inside the async with ClientSession() block
        # We need to make the session's __aenter__ raise the error
        mock_session = MagicMock()
        mock_session.__aenter__ = AsyncMock(side_effect=ConnectionError("Network unreachable"))
        mock_session.__aexit__ = AsyncMock(return_value=None)

        with patch("src.telegram.notifier.aiohttp.ClientSession", return_value=mock_session):
            result = await _send_via_bot_api("123456:ABC-DEF", 789, "Test message")

        assert result is False

    @pytest.mark.anyio
    async def test_send_via_bot_api_cancelled_error_propagates(self):
        """Test that CancelledError in bot API call is re-raised."""
        mock_session = MagicMock()
        mock_session.__aenter__ = AsyncMock(side_effect=asyncio.CancelledError)
        mock_session.__aexit__ = AsyncMock(return_value=None)

        with patch("src.telegram.notifier.aiohttp.ClientSession", return_value=mock_session):
            with pytest.raises(asyncio.CancelledError):
                await _send_via_bot_api("123456:ABC-DEF", 789, "Test message")

    @pytest.mark.anyio
    async def test_send_via_bot_api_timeout_error(self):
        """Test bot API call when timeout occurs during session creation."""
        mock_session = MagicMock()
        mock_session.__aenter__ = AsyncMock(side_effect=asyncio.TimeoutError)
        mock_session.__aexit__ = AsyncMock(return_value=None)

        with patch("src.telegram.notifier.aiohttp.ClientSession", return_value=mock_session):
            result = await _send_via_bot_api("123456:ABC-DEF", 789, "Test message")

        assert result is False
