from __future__ import annotations

from types import SimpleNamespace

import pytest
from telethon import TelegramClient
from telethon.sessions import MemorySession
from telethon.tl.functions.messages import GetDialogsRequest
from telethon_floodgate import (
    FloodCircuitBreaker,
    HandledFloodWaitError,
    RateLimitSpec,
    TelegramPeerRateLimitedError,
    TelegramRateLimitedError,
    TelegramRateLimitGate,
)

from src.telegram.backends import TelegramTransportSession


class _Clock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


def _dialog_page_request(offset_id: int = 0) -> GetDialogsRequest:
    return GetDialogsRequest(
        offset_date=None,
        offset_id=offset_id,
        offset_peer=None,
        limit=1,
        hash=0,
    )


def test_sweep_budget_covers_every_pass_the_loop_will_attempt() -> None:
    """The gate must never be the thing that truncates a resumable sweep.

    Its budget is kept in step with the sweep loop's own pass limit, so the
    loop's limiters (max passes, time budget, no-progress check) and the flood
    breaker are what bound a sweep — not a starved bucket.
    """
    from telethon_floodgate.rate_limit_gate import DIALOG_SWEEP_MAX_CALLS

    from src.telegram.pool_dialogs import DIALOG_FETCH_MAX_PASSES

    assert DIALOG_SWEEP_MAX_CALLS >= DIALOG_FETCH_MAX_PASSES

    clock = _Clock()
    gate = TelegramRateLimitGate(time_func=clock)
    for _ in range(DIALOG_FETCH_MAX_PASSES):
        assert gate.try_acquire("+1", "dialog_sweep") == 0.0


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
            await self.client(_dialog_page_request())
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
    assert len(calls) == 1
    assert isinstance(calls[0], GetDialogsRequest)

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
            await self.client(_dialog_page_request())
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
    assert len(calls) == 2
    assert all(isinstance(request, GetDialogsRequest) for request in calls)


@pytest.mark.asyncio
async def test_dialog_page_proxy_does_not_gate_emitted_object_requests() -> None:
    """The proxy must not charge later Dialog/Draft/Message operations as pages."""

    class EmittedDialog:
        def __init__(self, client) -> None:
            self.client = client

        async def unrelated_request(self):
            return await self.client(object())

    class PageIterator:
        def __init__(self, client) -> None:
            self.client = client
            self.page = 0

        def __aiter__(self):
            return self

        async def __anext__(self):
            if self.page >= 1:
                raise StopAsyncIteration
            await self.client(_dialog_page_request())
            self.page += 1
            return EmittedDialog(self.client)

    calls = []

    class Client:
        def iter_dialogs(self, **kwargs):
            return PageIterator(self)

        async def __call__(self, request):
            calls.append(request)
            return "ok"

    class Pool:
        _rate_limit_gate = TelegramRateLimitGate(
            category_limits={
                "dialog_sweep": RateLimitSpec(max_calls=1, window_sec=60),
                "dialogs_page": RateLimitSpec(max_calls=1, window_sec=60),
            }
        )

    session = TelegramTransportSession(Client(), phone="+66...2247", pool=Pool())
    stream = session.stream_dialogs()
    dialog = await stream.__anext__()
    assert await dialog.unrelated_request() == "ok"
    assert len(calls) == 2
    assert isinstance(calls[0], GetDialogsRequest)
    await stream.aclose()


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
            await self.client(_dialog_page_request())
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
    assert len(client.calls) == 1
    assert isinstance(client.calls[0], GetDialogsRequest)


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
            await self.client(_dialog_page_request(self.page))
            self.page += 1
            return self.page

    flood = FloodWaitError(request=None, capture=0)
    flood.seconds = 23

    class Client:
        def iter_dialogs(self, **kwargs):
            return PageIterator(self)

        async def __call__(self, request):
            if request.offset_id == 1:
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


# --- per-peer send gating (telethon-floodgate peer buckets) -----------------


@pytest.mark.asyncio
async def test_send_message_is_gated_per_peer() -> None:
    class Client:
        def __init__(self) -> None:
            self.calls = 0

        def send_message(self, entity, message, **kwargs):
            async def _result():
                self.calls += 1
                return "ok"

            return _result()

    clock = _Clock()

    class Pool:
        _rate_limit_gate = TelegramRateLimitGate(time_func=clock)

    client = Client()
    session = TelegramTransportSession(client, phone="+7000", pool=Pool())
    peer = SimpleNamespace(user_id=42)

    assert await session.send_message(peer, "hi") == "ok"
    # The per-peer bucket (1/s for a user peer) refuses the immediate second send.
    with pytest.raises(TelegramPeerRateLimitedError):
        await session.send_message(peer, "again")
    assert client.calls == 1, "refused send still reached Telegram"
    # A different peer passes: the per-peer refusal did not burn the category slot.
    assert await session.send_message(SimpleNamespace(user_id=43), "hi") == "ok"


@pytest.mark.asyncio
async def test_all_send_paths_pass_their_destination_as_peer() -> None:
    class Client:
        def __init__(self) -> None:
            self.sent: list[str] = []

        def _call(self, name):
            async def _result():
                self.sent.append(name)
                return "ok"

            return _result()

        def send_message(self, entity, message, **kwargs):
            return self._call("send_message")

        def send_file(self, entity, files, *, caption=None, schedule=None):
            return self._call("send_file")

        def forward_messages(self, entity, messages, from_peer):
            return self._call("forward_messages")

        def edit_message(self, entity, message, text, **kwargs):
            return self._call("edit_message")

    clock = _Clock()

    class Pool:
        _rate_limit_gate = TelegramRateLimitGate(time_func=clock)

    client = Client()
    session = TelegramTransportSession(client, phone="+7001", pool=Pool())
    peer = SimpleNamespace(user_id=7)

    assert await session.send_message(peer, "m") == "ok"
    clock.now += 1.0  # slide the 1/s user bucket between methods
    assert await session.publish_files(peer, ["f"]) == "ok"
    clock.now += 1.0
    assert await session.forward_messages(peer, [1], SimpleNamespace(user_id=8)) == "ok"
    clock.now += 1.0
    assert await session.edit_message(peer, 1, "t") == "ok"
    assert client.sent == ["send_message", "send_file", "forward_messages", "edit_message"]
    # All four share the SAME per-peer bucket: an immediate fifth call is refused.
    with pytest.raises(TelegramPeerRateLimitedError):
        await session.send_message(peer, "m")


@pytest.mark.asyncio
async def test_send_message_with_unknown_peer_kind_degrades_to_category() -> None:
    class Client:
        def send_message(self, entity, message, **kwargs):
            async def _result():
                return "ok"

            return _result()

    clock = _Clock()

    class Pool:
        _rate_limit_gate = TelegramRateLimitGate(time_func=clock)

    session = TelegramTransportSession(Client(), phone="+7002", pool=Pool())

    # A bare int peer key ("id:12345") has no per-peer spec configured, so only
    # the account-wide send category (30/min) applies — quick repeats still pass.
    assert await session.send_message(12345, "a") == "ok"
    assert await session.send_message(12345, "b") == "ok"
    # An entity peer_key cannot read at all yields None and also proceeds.
    assert await session.send_message(object(), "c") == "ok"
