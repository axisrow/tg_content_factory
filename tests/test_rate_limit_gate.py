from __future__ import annotations

import pytest
from telethon import TelegramClient
from telethon.sessions import MemorySession

from src.telegram.backends import TelegramTransportSession
from src.telegram.flood_breaker import FloodCircuitBreaker
from src.telegram.flood_wait import HandledFloodWaitError
from src.telegram.rate_limit_gate import (
    RateLimitSpec,
    TelegramRateLimitedError,
    TelegramRateLimitGate,
)


class _Clock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


def test_dialogs_gate_is_per_phone_and_conservative() -> None:
    clock = _Clock()
    gate = TelegramRateLimitGate(time_func=clock)

    assert gate.try_acquire("+1", "dialogs") == 0.0
    assert gate.try_acquire("+2", "dialogs") == 0.0
    assert gate.try_acquire("+1", "dialogs") == 60.0


def test_categories_have_independent_buckets() -> None:
    clock = _Clock()
    gate = TelegramRateLimitGate(
        category_limits={"history": RateLimitSpec(max_calls=1, window_sec=60)},
        time_func=clock,
    )
    assert gate.try_acquire("+1", "dialogs") == 0.0
    assert gate.try_acquire("+1", "history") == 0.0
    assert gate.try_acquire("+1", "history") == 60.0


def test_resolve_and_reaction_keep_their_existing_dedicated_gates() -> None:
    gate = TelegramRateLimitGate(
        category_limits={"default": RateLimitSpec(max_calls=1, window_sec=60)},
    )

    for operation, expected_category in (
        ("telegram_resolve_entity", "resolve"),
        ("telegram_resolve_input_entity", "resolve"),
        ("telegram_send_reaction", "reaction"),
    ):
        category = gate.category_for(operation)
        assert category == expected_category
        assert gate.try_acquire("+1", category) == 0.0
        assert gate.try_acquire("+1", category) == 0.0


@pytest.mark.parametrize(
    "operation",
    (
        "resolve_channel_warm_dialog_cache",
        "fetch_channel_meta_warm_dialog_cache",
        "get_forum_topics_warm_dialog_cache",
        "search_warm_dialog_cache",
        "leave_channels:123_warm_dialog_cache",
        "delete_dialogs:123_warm_dialog_cache",
    ),
)
def test_decorated_dialog_operations_use_the_dialogs_bucket(operation: str) -> None:
    assert TelegramRateLimitGate.category_for(operation) == "dialogs"


@pytest.mark.parametrize(
    "operation",
    ("telegram_stream_dialogs", "resume_stream_dialogs"),
)
def test_dialog_sweep_has_its_own_bucket(operation: str) -> None:
    """A sweep is one operation continued across passes, not repeated calls.

    Sharing the warm-up's 1/min bucket meant the second pass of a resumable
    sweep was always refused, so the sweep could never resume (#1359). The
    warm-up keeps its own strict bucket — that is what #1330 needs.
    """
    assert TelegramRateLimitGate.category_for(operation) == "dialog_sweep"
    assert TelegramRateLimitGate.category_for("telegram_warm_dialog_cache") == "dialogs"


def test_sweep_budget_covers_every_pass_the_loop_will_attempt() -> None:
    """The gate must never be the thing that truncates a resumable sweep.

    Its budget is kept in step with the sweep loop's own pass limit, so the
    loop's limiters (max passes, time budget, no-progress check) and the flood
    breaker are what bound a sweep — not a starved bucket.
    """
    from src.telegram.pool_dialogs import DIALOG_FETCH_MAX_PASSES
    from src.telegram.rate_limit_gate import DIALOG_SWEEP_MAX_CALLS

    assert DIALOG_SWEEP_MAX_CALLS >= DIALOG_FETCH_MAX_PASSES

    clock = _Clock()
    gate = TelegramRateLimitGate(time_func=clock)
    for _ in range(DIALOG_FETCH_MAX_PASSES):
        assert gate.try_acquire("+1", "dialog_sweep") == 0.0


def test_phase_two_categories_are_separately_calibrated() -> None:
    gate = TelegramRateLimitGate()
    assert gate.category_for("telegram_stream_messages") == "history"
    assert gate.category_for("telegram_edit_admin") == "admin_action"
    assert gate.category_for("telegram_send_message") == "send"
    assert gate.category_for("telegram_publish_files") == "send"
    assert gate.category_for("telegram_create_channel") == "channel_lifecycle"
    assert gate.category_for("telegram_import_chat_invite") == "channel_lifecycle"
    assert gate.category_for("telegram_delete_chat") == "channel_lifecycle"

    # A history stream must not consume a write-operation slot.
    assert gate.try_acquire("+1", "history") == 0.0
    assert gate.try_acquire("+1", "send") == 0.0


def test_compound_slot_reservation_is_atomic() -> None:
    clock = _Clock()
    gate = TelegramRateLimitGate(time_func=clock)

    assert gate.try_acquire("+1", "channel_lifecycle", slots=2) == 0.0
    assert gate.try_acquire("+1", "channel_lifecycle", slots=2) == 300.0
    # The rejected two-slot reservation must not consume the one remaining slot.
    assert gate.try_acquire("+1", "channel_lifecycle") == 0.0


@pytest.mark.asyncio
async def test_issue_1330_repeated_get_dialogs_is_stopped_before_telegram() -> None:
    calls = 0

    class Client:
        def get_dialogs(self):
            async def result():
                nonlocal calls
                calls += 1
                return []

            return result()

    class Pool:
        _rate_limit_gate = TelegramRateLimitGate()

    session = TelegramTransportSession(Client(), phone="+66...2247", pool=Pool())
    assert await session.get_dialogs() == []
    for _ in range(5):
        with pytest.raises(TelegramRateLimitedError):
            await session.get_dialogs()
    assert calls == 1


@pytest.mark.asyncio
async def test_paginated_dialog_stream_gates_each_telethon_page() -> None:
    """A single ``iter_dialogs`` operation must not burst across pages."""

    class PageIterator:
        def __init__(self, client) -> None:
            self.client = client
            self.page = 0

        def __aiter__(self):
            return self

        async def __anext__(self):
            if self.page >= 2:
                raise StopAsyncIteration
            await self.client(f"page-{self.page}")
            self.page += 1
            return self.page

    calls = []

    class Client:
        def iter_dialogs(self, **kwargs):
            return PageIterator(self)

        async def __call__(self, request):
            calls.append(request)

    class Pool:
        _rate_limit_gate = TelegramRateLimitGate(
            category_limits={
                "dialog_sweep": RateLimitSpec(max_calls=1, window_sec=60),
                "dialogs_page": RateLimitSpec(max_calls=1, window_sec=60),
            }
        )

    session = TelegramTransportSession(Client(), phone="+66...2247", pool=Pool())
    stream = session.stream_dialogs()

    assert await stream.__anext__() == 1
    with pytest.raises(TelegramRateLimitedError):
        await stream.__anext__()
    assert calls == ["page-0"]

    await stream.aclose()


@pytest.mark.asyncio
async def test_paginated_dialog_stream_allows_pages_with_separate_budget() -> None:
    """The logical-operation slot must not truncate a normal multi-page sweep."""

    class PageIterator:
        def __init__(self, client) -> None:
            self.client = client
            self.page = 0

        def __aiter__(self):
            return self

        async def __anext__(self):
            if self.page >= 2:
                raise StopAsyncIteration
            await self.client(f"page-{self.page}")
            self.page += 1
            return self.page

    calls = []

    class Client:
        def iter_dialogs(self, **kwargs):
            return PageIterator(self)

        async def __call__(self, request):
            calls.append(request)

    class Pool:
        _rate_limit_gate = TelegramRateLimitGate()

    session = TelegramTransportSession(Client(), phone="+66...2247", pool=Pool())
    stream = session.stream_dialogs()
    assert [await stream.__anext__(), await stream.__anext__()] == [1, 2]
    with pytest.raises(StopAsyncIteration):
        await stream.__anext__()
    assert calls == ["page-0", "page-1"]


@pytest.mark.asyncio
async def test_paginated_get_dialogs_gates_each_telethon_page() -> None:
    """The ``get_dialogs`` convenience method must receive the same protection."""

    class PageIterator:
        def __init__(self, client) -> None:
            self.client = client
            self.page = 0

        def __aiter__(self):
            return self

        async def __anext__(self):
            if self.page >= 2:
                raise StopAsyncIteration
            await self.client(f"page-{self.page}")
            self.page += 1
            return self.page

        async def collect(self):
            result = []
            async for item in self:
                result.append(item)
            return result

    class Client(TelegramClient):
        def __init__(self) -> None:
            super().__init__(MemorySession(), 1, "a" * 32)
            self.calls = []

        def iter_dialogs(self, **kwargs):
            return PageIterator(self)

        async def __call__(self, request):
            self.calls.append(request)

    class Pool:
        _rate_limit_gate = TelegramRateLimitGate(
            category_limits={
                "dialogs": RateLimitSpec(max_calls=1, window_sec=60),
                "dialogs_page": RateLimitSpec(max_calls=1, window_sec=60),
            }
        )

    client = Client()
    session = TelegramTransportSession(client, phone="+66...2247", pool=Pool())
    with pytest.raises(TelegramRateLimitedError):
        await session.warm_dialog_cache()
    assert client.calls == ["page-0"]


@pytest.mark.asyncio
async def test_paginated_flood_updates_breaker_once() -> None:
    """A page flood is recorded by the enclosing logical stream only once."""
    from telethon.errors import FloodWaitError

    class PageIterator:
        def __init__(self, client) -> None:
            self.client = client
            self.page = 0

        def __aiter__(self):
            return self

        async def __anext__(self):
            if self.page >= 2:
                raise StopAsyncIteration
            await self.client(f"page-{self.page}")
            self.page += 1
            return self.page

    flood = FloodWaitError(request=None, capture=0)
    flood.seconds = 23

    class Client:
        def iter_dialogs(self, **kwargs):
            return PageIterator(self)

        async def __call__(self, request):
            if request == "page-1":
                raise flood

    class Pool:
        _rate_limit_gate = TelegramRateLimitGate()
        _flood_breaker = FloodCircuitBreaker(threshold=3, cooldown_seconds=300)

    session = TelegramTransportSession(Client(), phone="+66...2247", pool=Pool())
    stream = session.stream_dialogs()
    assert await stream.__anext__() == 1
    with pytest.raises(HandledFloodWaitError):
        await stream.__anext__()

    breaker = Pool._flood_breaker._breakers[("telegram_stream_dialogs", "+66...2247")]
    assert breaker.fail_counter == 1


@pytest.mark.asyncio
async def test_unbound_session_is_noop_safe() -> None:
    class Client:
        async def get_dialogs(self):
            return []

    assert await TelegramTransportSession(Client()).get_dialogs() == []
