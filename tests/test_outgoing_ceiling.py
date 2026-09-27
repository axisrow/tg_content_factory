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
async def test_gate_refusal_surrenders_its_ceiling_slot() -> None:
    """Ceiling-first order: every limit holds at dispatch time (#1444 review).

    The per-account gate is re-checked synchronously right before the RPC, so
    a refused call has already drawn its ceiling slot; surrendering it to
    window expiry is the accepted cost of dispatch-accurate per-account and
    per-peer pacing.  A passed call is still charged exactly once everywhere.
    """
    clock = _FakeClock()
    sleeper = _FakeSleep(clock)

    class Pool:
        _rate_limit_gate = TelegramRateLimitGate(
            category_limits={"send": RateLimitSpec(max_calls=1, window_sec=60.0)},
            time_func=clock,
        )
        _outgoing_ceiling = OutgoingRateCeiling(
            RateLimitSpec(max_calls=2, window_sec=60.0),
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
    # The refused call surrendered its ceiling slot (documented cost).
    assert len(Pool._outgoing_ceiling._limiter._calls["process"]) == 2
    assert sleeper.waits == [], "ceiling had room: no wait was needed"


@pytest.mark.asyncio
async def test_same_peer_sends_cannot_bundle_after_ceiling_wait() -> None:
    """Two same-peer sends sharing a ceiling wait must not fire in one instant.

    Codex repro on the pre-reorder code: both sends passed the 1/s peer gate
    at reservation time, slept on the ceiling together, and dispatched in the
    same tick.  With the ceiling first, the second send re-checks the peer
    bucket at dispatch and is refused.
    """
    clock = _FakeClock()
    sleeper = _FakeSleep(clock)

    class Pool:
        _rate_limit_gate = TelegramRateLimitGate(time_func=clock)
        _outgoing_ceiling = OutgoingRateCeiling(
            RateLimitSpec(max_calls=2, window_sec=30.0),
            time_func=clock,
            sleep_func=sleeper,
        )

    client = _RecordingClient(clock)
    session = TelegramTransportSession(client, phone="+7000", pool=Pool())
    peer = SimpleNamespace(user_id=7)

    # Burn the ceiling before the volley: both sends will wait for t=1030.
    await Pool._outgoing_ceiling.acquire()
    await Pool._outgoing_ceiling.acquire()

    first = asyncio.create_task(session.send_message(peer, "a"))
    for _ in range(3):
        await asyncio.sleep(0)
    second = asyncio.create_task(session.send_message(peer, "b"))
    results = await asyncio.gather(first, second, return_exceptions=True)

    assert results[0] == "ok"
    assert isinstance(results[1], TelegramRateLimitedError), (
        "same-peer sends must not bundle into one instant after the wait"
    )
    assert len(client.sent_at) == 1
    assert clock.now == 1030.0, "the second send was refused only after the wait"


@pytest.mark.asyncio
async def test_history_stream_pages_draw_ceiling_slots() -> None:
    """Every raw GetHistory RPC of a stream charges the ceiling (#1444 review).

    Telethon drives pages through ``iterator.client``; the bound proxy routes
    them through ``_run`` with the gate skipped, so the #1418 history
    calibration stays on logical streams while the ceiling sees each page.
    """
    from telethon.tl.functions.messages import GetHistoryRequest

    clock = _FakeClock()
    sleeper = _FakeSleep(clock)
    page_calls: list[GetHistoryRequest] = []

    class PageIterator:
        def __init__(self, client_obj) -> None:
            self.client = client_obj
            self.page = 0

        def __aiter__(self):
            return self

        async def __anext__(self):
            if self.page >= 2:
                raise StopAsyncIteration
            request = GetHistoryRequest(
                peer=None,
                offset_id=0,
                offset_date=None,
                add_offset=0,
                limit=1,
                max_id=0,
                min_id=0,
                hash=0,
            )
            await self.client(request)
            self.page += 1
            return self.page

    class Client:
        def iter_messages(self, entity, **kwargs):
            return PageIterator(self)

        async def __call__(self, request):
            page_calls.append(request)

    class Pool:
        _rate_limit_gate = TelegramRateLimitGate(time_func=clock)
        _outgoing_ceiling = OutgoingRateCeiling(
            RateLimitSpec(max_calls=10, window_sec=30.0),
            time_func=clock,
            sleep_func=sleeper,
        )

    session = TelegramTransportSession(Client(), phone="+7000", pool=Pool())
    stream = session.stream_messages("peer")
    assert [await stream.__anext__(), await stream.__anext__()] == [1, 2]
    with pytest.raises(StopAsyncIteration):
        await stream.__anext__()
    await stream.aclose()

    assert len(page_calls) == 2
    # Logical slot + two page slots: the ceiling never under-counts a stream.
    assert len(Pool._outgoing_ceiling._limiter._calls["process"]) == 3
    # The per-account history bucket is charged once (logical stream only).
    history = Pool._rate_limit_gate._limiters["history"]._calls["+7000"]
    assert len(history) == 1


@pytest.mark.asyncio
async def test_stream_pages_do_not_recheck_the_stream_breaker_probe() -> None:
    """A page must not reject its own stream's half-open probe (Codex recheck).

    The outer stream claims the breaker probe before iterating; a page re-run
    through ``_run`` used to see that very probe in flight and raise
    ``TelegramOperationSuspendedError``, stranding the stream forever.
    """
    from telethon.tl.functions.messages import GetHistoryRequest
    from telethon_floodgate import FloodCircuitBreaker

    clock = _FakeClock()

    class PageIterator:
        def __init__(self, client_obj) -> None:
            self.client = client_obj
            self.page = 0

        def __aiter__(self):
            return self

        async def __anext__(self):
            if self.page >= 1:
                raise StopAsyncIteration
            request = GetHistoryRequest(
                peer=None,
                offset_id=0,
                offset_date=None,
                add_offset=0,
                limit=1,
                max_id=0,
                min_id=0,
                hash=0,
            )
            await self.client(request)
            self.page += 1
            return self.page

    class Client:
        def iter_messages(self, entity, **kwargs):
            return PageIterator(self)

        async def __call__(self, request):
            pass

    class Pool:
        _rate_limit_gate = TelegramRateLimitGate(time_func=clock)
        _flood_breaker = FloodCircuitBreaker(
            threshold=1, cooldown_seconds=60.0, time_func=clock
        )
        _outgoing_ceiling = OutgoingRateCeiling(
            RateLimitSpec(max_calls=10, window_sec=30.0), time_func=clock
        )

    # Burn the breaker: one flood trips it OPEN.
    Pool._flood_breaker.record_flood("telegram_stream_messages", "+7000")
    clock.now += 61.0  # cooldown elapsed: the next check claims the half-open probe

    session = TelegramTransportSession(Client(), phone="+7000", pool=Pool())
    stream = session.stream_messages("peer")
    assert await stream.__anext__() == 1
    with pytest.raises(StopAsyncIteration):
        await stream.__anext__()
    await stream.aclose()

    # The stream drained: the probe was claimed by the outer check and
    # released by its record_success — no page may have re-checked it.
    assert Pool._flood_breaker._probe_in_flight == set()


@pytest.mark.asyncio
async def test_history_page_flood_is_recorded_once_by_the_stream() -> None:
    """A page flood surfaces as HandledFloodWaitError and feeds the breaker once.

    Pages skip breaker recording themselves; the enclosing stream is the
    sole recorder — verified here for the history path (the dialogs path has
    its own test in test_rate_limit_gate.py).
    """
    from telethon.errors import FloodWaitError
    from telethon.tl.functions.messages import GetHistoryRequest
    from telethon_floodgate import FloodCircuitBreaker, HandledFloodWaitError

    flood = FloodWaitError(request=None, capture=0)
    flood.seconds = 23

    class PageIterator:
        def __init__(self, client_obj) -> None:
            self.client = client_obj
            self.page = 0

        def __aiter__(self):
            return self

        async def __anext__(self):
            if self.page >= 2:
                raise StopAsyncIteration
            request = GetHistoryRequest(
                peer=None,
                offset_id=0,
                offset_date=None,
                add_offset=0,
                limit=1,
                max_id=0,
                min_id=0,
                hash=0,
            )
            await self.client(request)
            self.page += 1
            return self.page

    class Client:
        def __init__(self) -> None:
            self.page_calls = 0

        def iter_messages(self, entity, **kwargs):
            return PageIterator(self)

        async def __call__(self, request):
            self.page_calls += 1
            if self.page_calls == 2:
                raise flood

    class Pool:
        _rate_limit_gate = TelegramRateLimitGate(time_func=_FakeClock())
        _flood_breaker = FloodCircuitBreaker(
            threshold=3, cooldown_seconds=300.0, time_func=_FakeClock()
        )
        _outgoing_ceiling = OutgoingRateCeiling(
            RateLimitSpec(max_calls=10, window_sec=30.0), time_func=_FakeClock()
        )

    client = Client()
    session = TelegramTransportSession(client, phone="+7000", pool=Pool())
    stream = session.stream_messages("peer")
    assert await stream.__anext__() == 1
    with pytest.raises(HandledFloodWaitError):
        await stream.__anext__()

    assert client.page_calls == 2
    key = ("telegram_stream_messages", "+7000")
    breaker = Pool._flood_breaker._breakers[key]
    assert breaker.fail_counter == 1, "the page flood is recorded exactly once"


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


# --- cancellation during the ceiling wait (dual review: MINOR, closed) -------


class _ClosableAwaitable:
    """Eager awaitable that records ``close()`` like an unstarted coroutine."""

    def __init__(self) -> None:
        self.closed = False
        self.ran = False

    def __await__(self):
        if False:  # pragma: no cover - makes this a generator-based awaitable
            yield
        self.ran = True
        return "ok"

    def close(self) -> None:
        self.closed = True


class _HangingCeilingPool:
    """Pool whose ceiling consumes the caller into an endless wait."""

    def __init__(self) -> None:
        self._outgoing_ceiling = OutgoingRateCeiling(
            RateLimitSpec(max_calls=1, window_sec=60.0),
            time_func=_FakeClock(),
            sleep_func=self._hang,
        )
        self._outgoing_ceiling._limiter.try_acquire("process")

    async def _hang(self, delay: float) -> None:
        await asyncio.Event().wait()


@pytest.mark.asyncio
async def test_cancelled_wait_still_closes_the_eager_awaitable() -> None:
    """Cancelling a caller suspended on the ceiling closes the eager call.

    The ceiling wait is the first await point of ``_run``; before the
    BaseException cleanup, cancellation there leaked an unstarted (and
    never closed) Telethon coroutine.
    """
    pool = _HangingCeilingPool()
    awaitable = _ClosableAwaitable()

    class Client:
        def send_message(self, entity, message, **kwargs):
            return awaitable

    session = TelegramTransportSession(Client(), phone="+7000", pool=pool)
    task = asyncio.create_task(session.send_message(SimpleNamespace(user_id=1), "hi"))
    for _ in range(5):
        await asyncio.sleep(0)  # let the task suspend inside the ceiling wait
    assert not awaitable.ran, "the call must not start before its ceiling slot"

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    assert awaitable.closed, "cancelled caller must close the eager awaitable"
    assert not awaitable.ran


@pytest.mark.asyncio
async def test_cancelled_wait_still_closes_the_stream_iterator() -> None:
    """Same cancellation window on the ``_stream`` path."""
    pool = _HangingCeilingPool()

    class Iterator:
        def __init__(self) -> None:
            self.closed = False

        def __aiter__(self):
            return self

        async def __anext__(self):  # pragma: no cover - never iterated
            raise StopAsyncIteration

        async def aclose(self):
            self.closed = True

    iterator = Iterator()

    class Client:
        def iter_messages(self, entity, **kwargs):
            return iterator

    session = TelegramTransportSession(Client(), phone="+7000", pool=pool)
    stream = session.stream_messages("peer")
    task = asyncio.create_task(stream.__anext__())
    for _ in range(5):
        await asyncio.sleep(0)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    await stream.aclose()

    assert iterator.closed, "cancelled stream must aclose the inner iterator"
