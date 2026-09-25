"""Process-wide outgoing ceiling (#1417): fake clocks, no real sleeps."""
from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from telethon_floodgate import RateLimitSpec, TelegramRateLimitedError, TelegramRateLimitGate

from src.telegram.backends import TelegramTransportSession
from src.telegram.client_pool import PROCESS_OUTGOING_SPEC, ClientPool
from src.telegram.outgoing_ceiling import OutgoingRateCeiling


class _FakeClock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


class _FakeSleep:
    """Wait that advances the shared clock, then yields to the event loop.

    Advancing to the caller's deadline keeps the retry exact; the ``sleep(0)``
    lets sibling coroutines contend for the freed slots the way real waiters
    waking at the same instant would.
    """

    def __init__(self, clock: _FakeClock) -> None:
        self.clock = clock
        self.waits: list[float] = []

    async def __call__(self, delay: float) -> None:
        self.waits.append(delay)
        self.clock.now += delay
        await asyncio.sleep(0)


def _max_calls_in_window(times: list[float], window: float) -> int:
    """Largest number of calls inside any ``window``-wide sliding slice."""
    times = sorted(times)
    best = 0
    left = 0
    for right, t in enumerate(times):
        while times[left] + window <= t:
            left += 1
        best = max(best, right - left + 1)
    return best


class _RecordingClient:
    def __init__(self, clock: _FakeClock) -> None:
        self.clock = clock
        self.sent_at: list[tuple[float, object]] = []

    def send_message(self, entity, message, **kwargs):
        async def _result():
            # Executed only after the ceiling released the slot (#1417 AC:
            # nothing reaches Telegram before budget frees up).
            self.sent_at.append((self.clock.now, entity))
            return "ok"

        return _result()


class _EmptyStreamClient:
    def iter_messages(self, entity, **kwargs):
        async def _iterator():
            return
            yield  # pragma: no cover

        return _iterator()


# --- the volley from the production log (#1417 AC: burst regression) ---------


@pytest.mark.asyncio
async def test_volley_of_ten_accounts_is_paced_to_the_ceiling() -> None:
    """Ten accounts fire at once; the process ceiling paces, nobody refuses."""
    clock = _FakeClock()
    sleeper = _FakeSleep(clock)
    client = _RecordingClient(clock)

    class Pool:
        _outgoing_ceiling = OutgoingRateCeiling(
            RateLimitSpec(max_calls=3, window_sec=10.0),
            time_func=clock,
            sleep_func=sleeper,
        )

    sessions = [
        TelegramTransportSession(client, phone=f"+700{i}", pool=Pool()) for i in range(10)
    ]

    results = await asyncio.gather(
        *[session.send_message(SimpleNamespace(user_id=i), "hi") for i, session in enumerate(sessions)]
    )

    assert results == ["ok"] * 10, "wait semantics: no caller is refused"
    times = [t for t, _ in client.sent_at]
    assert len(times) == 10, "every volleyed send reached the transport exactly once"
    # Aggregate pace never exceeds the process-wide ceiling of 3 per 10s,
    # no matter how many accounts burst simultaneously.
    assert _max_calls_in_window(times, 10.0) == 3
    # Nothing leaves before budget frees: the first wave carries exactly the
    # initial budget, the rest wait for expired slots.
    assert sorted(times) == [1000.0] * 3 + [1010.0] * 3 + [1020.0] * 3 + [1030.0]
    assert sleeper.waits, "excess calls actually deferred instead of passing through"


# --- coexistence with the per-account gate (#1417 AC: no double duty) --------


@pytest.mark.asyncio
async def test_gate_refusal_leaves_ceiling_budget_untouched() -> None:
    """Gate refuses first (sync); the ceiling is charged only by passed calls."""
    clock = _FakeClock()
    sleeper = _FakeSleep(clock)

    class Pool:
        _rate_limit_gate = TelegramRateLimitGate(
            category_limits={"send": RateLimitSpec(max_calls=1, window_sec=60.0)},
            time_func=clock,
        )
        _outgoing_ceiling = OutgoingRateCeiling(
            RateLimitSpec(max_calls=1, window_sec=60.0),
            time_func=clock,
            sleep_func=sleeper,
        )

    client = _RecordingClient(clock)
    session = TelegramTransportSession(client, phone="+7000", pool=Pool())

    assert await session.send_message(SimpleNamespace(user_id=1), "a") == "ok"
    # One passed call is charged to each mechanism exactly once (no double duty).
    assert len(Pool._outgoing_ceiling._limiter._calls["process"]) == 1
    with pytest.raises(TelegramRateLimitedError):
        await session.send_message(SimpleNamespace(user_id=2), "b")
    assert len(client.sent_at) == 1, "refused send still reached Telegram"
    assert len(Pool._outgoing_ceiling._limiter._calls["process"]) == 1
    assert sleeper.waits == [], "a refusal must not spend time waiting on the ceiling"


@pytest.mark.asyncio
async def test_unbound_session_is_noop_safe_for_the_ceiling() -> None:
    class Client:
        def send_message(self, entity, message, **kwargs):
            async def _result():
                return "ok"

            return _result()

    # No pool at all...
    assert (
        await TelegramTransportSession(Client()).send_message(SimpleNamespace(user_id=1), "m")
        == "ok"
    )
    # ...and a pool without a ceiling attribute (older fakes).
    assert (
        await TelegramTransportSession(Client(), phone="+7000", pool=SimpleNamespace()).send_message(
            SimpleNamespace(user_id=1), "m"
        )
        == "ok"
    )


# --- throughput of legit pagination (#1417 AC: explicit measurement) ---------


@pytest.mark.asyncio
async def test_calibrated_pagination_runs_without_ceiling_delay() -> None:
    """A single account at the calibrated history pace never touches the ceiling.

    Measurement: 24 logical stream_messages calls (the 24/30s per-account
    history spec) consume zero ceiling waits and zero fake time.
    """
    clock = _FakeClock()
    sleeper = _FakeSleep(clock)

    class Pool:
        _rate_limit_gate = TelegramRateLimitGate(time_func=clock)
        _outgoing_ceiling = OutgoingRateCeiling(
            PROCESS_OUTGOING_SPEC, time_func=clock, sleep_func=sleeper
        )

    session = TelegramTransportSession(_EmptyStreamClient(), phone="+7000", pool=Pool())
    for _ in range(24):
        async for _item in session.stream_messages("peer"):
            pass  # pragma: no cover - the fake stream is empty

    assert sleeper.waits == [], "legit pagination must not defer behind the ceiling"
    assert clock.now == 1000.0, "ceiling consumed zero fake time"


# --- pool wiring --------------------------------------------------------------


def test_pool_ships_process_wide_ceiling() -> None:
    """The pool constructs the shared ceiling with the (uncalibrated) spec.

    Literals on purpose: if someone retunes PROCESS_OUTGOING_SPEC, this test
    must go red so the recalibration is a visible, reviewed change (#1417).
    """
    pool = ClientPool(MagicMock(api_id=1, api_hash="h"), MagicMock())
    ceiling = pool._outgoing_ceiling
    assert isinstance(ceiling, OutgoingRateCeiling)
    assert ceiling._limiter._max_calls == 120
    assert ceiling._limiter._window_sec == 30.0


@pytest.mark.asyncio
async def test_ceiling_defer_is_exact_not_jittered() -> None:
    """Waited callers sleep exactly to the window boundary (no jitter)."""
    clock = _FakeClock()
    sleeper = _FakeSleep(clock)
    ceiling = OutgoingRateCeiling(
        RateLimitSpec(max_calls=2, window_sec=30.0), time_func=clock, sleep_func=sleeper
    )

    await ceiling.acquire()
    await ceiling.acquire()
    await ceiling.acquire()

    assert sleeper.waits == [30.0]
    assert clock.now == 1030.0
