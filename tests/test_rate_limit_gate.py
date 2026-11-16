from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from telethon import TelegramClient
from telethon import types as tl_types
from telethon.sessions import MemorySession
from telethon.tl.functions.channels import (
    GetAdminLogRequest,
    GetParticipantsRequest,
)
from telethon.tl.functions.channels import (
    GetMessagesRequest as GetChannelMessagesRequest,
)
from telethon.tl.functions.messages import (
    GetDialogsRequest,
    GetHistoryRequest,
    GetMessagesRequest,
    GetRepliesRequest,
    GetScheduledHistoryRequest,
    SearchGlobalRequest,
    SearchRequest,
)
from telethon.tl.functions.photos import GetUserPhotosRequest
from telethon_floodgate import (
    FloodCircuitBreaker,
    HandledFloodWaitError,
    RateLimitSpec,
    TelegramOperationSuspendedError,
    TelegramPeerRateLimitedError,
    TelegramRateLimitedError,
    TelegramRateLimitGate,
)

from tg_agent.telegram.backends import TelegramTransportSession
from tg_agent.telegram.client_pool import HISTORY_CALIBRATED_SPEC
from tg_agent.telegram.outgoing_ceiling import OutgoingRateCeiling


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
    breaker are what bound a sweep — not a starved bucket.  Proven through the
    public contract: a DEFAULT gate must admit every pass the loop attempts
    (a shrunk package budget or a raised pass limit goes red here), so no
    private package constant needs importing.
    """
    from tg_agent.telegram.pool_dialogs import DIALOG_FETCH_MAX_PASSES

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
        # Threshold 2 = exactly one recorded flood short of tripping: the
        # exactly-once contract below is provable through the public check/
        # record_flood surface alone.
        _flood_breaker = FloodCircuitBreaker(threshold=2, cooldown_seconds=300)

    session = TelegramTransportSession(Client(), phone="+66...2247", pool=Pool())
    stream = session.stream_dialogs()
    assert await stream.__anext__() == 1
    with pytest.raises(HandledFloodWaitError):
        await stream.__anext__()

    # Exactly once, observable through the breaker contract: one page flood
    # leaves the pair one failure short of the threshold...
    Pool._flood_breaker.check("telegram_stream_dialogs", "+66...2247")
    # ...and one more reported flood trips it.  A double record would have
    # tripped it already; a lost record would keep this final check passing.
    Pool._flood_breaker.record_flood("telegram_stream_dialogs", "+66...2247")
    with pytest.raises(TelegramOperationSuspendedError):
        Pool._flood_breaker.check("telegram_stream_dialogs", "+66...2247")


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
    clock.now += 1.1  # slide the calibrated 1/1.1s user bucket between methods
    assert await session.publish_files(peer, ["f"]) == "ok"
    clock.now += 1.1
    assert await session.forward_messages(peer, [1], SimpleNamespace(user_id=8)) == "ok"
    clock.now += 1.1
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


def test_pool_applies_production_history_calibration() -> None:
    """The pool's gate must ship the #1418 calibrated history spec.

    The released floodgate default (600/min) never bound: production peaks ran
    at 115 fetches/min.  The override lives in the pool, so the pool is what
    this pins — read through the gate's public ``snapshot()``, not limiter
    internals.  Other categories must keep the package defaults, asserted as
    equality with a fresh default gate rather than literals: the calibration
    of those belongs to the package.
    """
    from typing import cast

    from tg_agent.telegram.client_pool import ClientPool

    pool = ClientPool(MagicMock(api_id=1, api_hash="h"), MagicMock())
    categories = cast(
        "dict[str, dict[str, object]]", pool._rate_limit_gate.snapshot("+7000")["categories"]
    )
    assert categories["history"]["max_calls"] == 24
    assert categories["history"]["window_sec"] == 30.0
    # Untouched categories keep their package defaults.
    default = cast("dict[str, dict[str, object]]", TelegramRateLimitGate().snapshot("+7000")["categories"])
    assert categories["send"] == default["send"]


def test_history_calibration_stops_a_collector_burst_before_telegram() -> None:
    """A peak burst (p95 74-101 fetches/min in app.log) defers, not floods.

    24 calls fit the calibrated 30s window; the 25th is refused before any
    Telegram call, and the budget returns once the window slides.
    """
    clock = _Clock()
    gate = TelegramRateLimitGate(
        category_limits={"history": HISTORY_CALIBRATED_SPEC},
        time_func=clock,
    )
    for _ in range(HISTORY_CALIBRATED_SPEC.max_calls):
        assert gate.try_acquire("+7001", "history") == 0.0
    deferred = gate.try_acquire("+7001", "history")
    assert deferred > 0
    # Sliding window: after the window passes, the bucket accepts again.
    clock.now += HISTORY_CALIBRATED_SPEC.window_sec
    assert gate.try_acquire("+7001", "history") == 0.0


# --- physical page accounting across supported read iterators (#1827) -------


class _SpyCeiling(OutgoingRateCeiling):
    """Test double: records every ``acquire`` without ever waiting.

    Subclasses the real ceiling so ``_run``'s isinstance probe passes; slots
    are counted, not consumed.
    """

    def __init__(self) -> None:
        super().__init__(RateLimitSpec(max_calls=10**6, window_sec=30.0))
        self.acquires = 0

    async def acquire(self) -> None:
        self.acquires += 1


def _page_gate_pool() -> tuple[type, _SpyCeiling]:
    """Pool whose calibrated history bucket holds exactly ONE slot.

    The outer logical stream consumes it.  Any page that still charged the
    per-account history bucket would refuse mid-stream and go red — pages
    must ride the process ceiling, not the logical quota (#1827).
    """
    ceiling = _SpyCeiling()

    class Pool:
        _rate_limit_gate = TelegramRateLimitGate(
            category_limits={"history": RateLimitSpec(max_calls=1, window_sec=60)}
        )
        _outgoing_ceiling = ceiling

    return Pool, ceiling


def _channel_message(mid: int) -> tl_types.Message:
    return tl_types.Message(
        id=mid,
        peer_id=tl_types.PeerChannel(channel_id=100),
        date=datetime(2026, 11, 16, tzinfo=timezone.utc),
        message="body",
    )


def _message_slice(mids: list[int]) -> tl_types.MessagesSlice:
    return tl_types.messages.MessagesSlice(
        count=1000,
        messages=[_channel_message(mid) for mid in mids],
        next_rate=0,
        chats=[],
        users=[],
        topics=[],
    )


class _RecordingTelethonClient(TelegramClient):
    """Real Telethon client with a recording sender, offline page replays.

    A warm session cache is simulated by returning the peer unchanged from
    ``get_input_entity`` — resolved InputPeers must produce zero username
    resolves (#1827).
    """

    def __init__(self) -> None:
        super().__init__(MemorySession(), 1, "a" * 32)
        self.sent = []
        self.respond = self._no_responder

    @staticmethod
    def _no_responder(request) -> object:
        raise AssertionError(f"unexpected dispatch: {request!r}")

    async def __call__(self, request, ordered=False):
        self.sent.append(request)
        return self.respond(request)

    async def get_input_entity(self, peer):
        return peer


class _PageIterator:
    """RequestIter shape: N pages, each dispatched through ``iterator.client``."""

    def __init__(self, client, request_factory, pages) -> None:
        self.client = client
        self._request_factory = request_factory
        self._pages = pages
        self.page = 0
        self.closed = False

    def __aiter__(self):
        return self

    async def __anext__(self):
        if self.page >= self._pages:
            raise StopAsyncIteration
        await self.client(self._request_factory(self.page))
        self.page += 1
        return self.page

    async def aclose(self):
        self.closed = True

    async def collect(self):
        result = []
        async for item in self:
            result.append(item)
        return result


class _PageDispatchClient:
    """Fake client: one iterator method dispatches pages via ``client()``."""

    def __init__(self, request_factory, pages, *, iterator_method) -> None:
        self._request_factory = request_factory
        self._pages = pages
        self._iterator_method = iterator_method
        self.calls = []

    def _iter(self, *args, **kwargs):
        return _PageIterator(self, self._request_factory, self._pages)

    def __getattr__(self, name):
        if name == self._iterator_method:
            return self._iter
        raise AttributeError(name)

    async def __call__(self, request):
        self.calls.append(request)


def _history_request(page: int) -> GetHistoryRequest:
    return GetHistoryRequest(
        peer=None, limit=100, offset_id=page, add_offset=0, offset_date=None, max_id=0, min_id=0, hash=0
    )


def _search_request(page: int) -> SearchRequest:
    return SearchRequest(
        peer=None, q="audit", filter=None, min_date=None, max_date=None,
        offset_id=page, add_offset=0, limit=100, max_id=0, min_id=0, hash=0, from_id=None,
    )


def _search_global_request(page: int) -> SearchGlobalRequest:
    return SearchGlobalRequest(
        q="audit", filter=None, min_date=None, max_date=None,
        offset_rate=0, offset_peer=None, offset_id=page, limit=100,
    )


def _replies_request(page: int) -> GetRepliesRequest:
    return GetRepliesRequest(
        peer=None, msg_id=7, offset_id=page, offset_date=None, add_offset=0,
        limit=100, max_id=0, min_id=0, hash=0,
    )


def _ids_request(page: int):
    if page == 0:
        return GetChannelMessagesRequest(channel=None, id=[100 + page])
    return GetMessagesRequest(id=[100 + page])


def _participants_request(page: int) -> GetParticipantsRequest:
    return GetParticipantsRequest(channel=None, filter=None, offset=page, limit=100, hash=0)


def _profile_photos_request(page: int) -> GetUserPhotosRequest:
    return GetUserPhotosRequest(user_id=None, offset=0, max_id=page, limit=100)


def _admin_log_request(page: int) -> GetAdminLogRequest:
    return GetAdminLogRequest(
        channel=None, q="", events_filter=None, admins=None, max_id=0, min_id=page, limit=100
    )


async def _drain(stream) -> list:
    items = []
    async for item in stream:
        items.append(item)
    return items


@pytest.mark.asyncio
async def test_real_iter_messages_history_pages_are_ceiling_accounted() -> None:
    """Working control (#1827 row 1): 2-page history stream = 3 admissions.

    The outer logical stream holds the calibrated history slot; the physical
    GetHistory pages must ride the process ceiling only.
    """
    pool, ceiling = _page_gate_pool()
    client = _RecordingTelethonClient()
    slices = [_message_slice([302, 301]), _message_slice([51])]
    state = {"page": 0}

    def respond(request):
        response = slices[state["page"]]
        state["page"] += 1
        return response

    client.respond = respond
    session = TelegramTransportSession(client, phone="+66...2247", pool=pool)
    entity = tl_types.InputPeerChannel(channel_id=100, access_hash=0)
    stream = session.stream_messages(entity, limit=250)
    assert [item.id for item in await _drain(stream)] == [302, 301, 51]
    assert state["page"] == 2
    assert ceiling.acquires == 3  # outer stream + two physical pages
    assert all(isinstance(request, GetHistoryRequest) for request in client.sent)


@pytest.mark.asyncio
async def test_real_iter_messages_search_pages_are_ceiling_accounted() -> None:
    """Search pages (#1827 row 2) reach the ceiling like history pages do."""
    pool, ceiling = _page_gate_pool()
    client = _RecordingTelethonClient()
    slices = [_message_slice([302, 301]), _message_slice([51])]
    state = {"page": 0}

    def respond(request):
        response = slices[state["page"]]
        state["page"] += 1
        return response

    client.respond = respond
    session = TelegramTransportSession(client, phone="+66...2247", pool=pool)
    entity = tl_types.InputPeerChannel(channel_id=100, access_hash=0)
    stream = session.stream_messages(entity, search="audit", limit=250)
    assert [item.id for item in await _drain(stream)] == [302, 301, 51]
    assert ceiling.acquires == 3  # outer stream + two physical pages
    assert [type(request).__name__ for request in client.sent] == [
        "SearchRequest",
        "SearchRequest",
    ]


@pytest.mark.asyncio
async def test_real_iter_messages_reverse_mode_pages_are_ceiling_accounted() -> None:
    """Reverse mode keeps Telethon's own offset math and ceiling accounting.

    Reverse chunks are sent with a negative add_offset; Telegram answers in
    descending order and Telethon reverses each chunk locally. The page gate
    must not disturb the offset math, and the empty terminal chunk stops the
    stream (#1827).
    """
    pool, ceiling = _page_gate_pool()
    client = _RecordingTelethonClient()
    slices = [_message_slice([51]), _message_slice([302, 301]), _message_slice([])]
    state = {"page": 0}

    def respond(request):
        response = slices[state["page"]]
        state["page"] += 1
        return response

    client.respond = respond
    session = TelegramTransportSession(client, phone="+66...2247", pool=pool)
    entity = tl_types.InputPeerChannel(channel_id=100, access_hash=0)
    stream = session.stream_messages(entity, limit=250, reverse=True)
    assert [item.id for item in await _drain(stream)] == [51, 301, 302]
    assert ceiling.acquires == 4  # outer stream + three physical pages
    assert [type(request).__name__ for request in client.sent] == [
        "GetHistoryRequest",
        "GetHistoryRequest",
        "GetHistoryRequest",
    ]


@pytest.mark.asyncio
async def test_real_iter_messages_topic_reply_pages_are_ceiling_accounted() -> None:
    """Topic replies (#1827 row 3) dispatch GetReplies through the ceiling."""
    pool, ceiling = _page_gate_pool()
    client = _RecordingTelethonClient()
    slices = [_message_slice([302, 301]), _message_slice([51])]
    state = {"page": 0}

    def respond(request):
        response = slices[state["page"]]
        state["page"] += 1
        return response

    client.respond = respond
    session = TelegramTransportSession(client, phone="+66...2247", pool=pool)
    entity = tl_types.InputPeerChannel(channel_id=100, access_hash=0)
    stream = session.stream_messages(entity, reply_to=7, limit=250)
    assert [item.id for item in await _drain(stream)] == [302, 301, 51]
    assert ceiling.acquires == 3  # outer stream + two physical pages
    assert [type(request).__name__ for request in client.sent] == [
        "GetRepliesRequest",
        "GetRepliesRequest",
    ]


@pytest.mark.asyncio
async def test_real_iter_messages_global_search_page_is_ceiling_accounted() -> None:
    """Global search (entity=None) dispatches SearchGlobal through the ceiling."""
    pool, ceiling = _page_gate_pool()
    client = _RecordingTelethonClient()
    client.respond = lambda request: _message_slice([51])
    session = TelegramTransportSession(client, phone="+66...2247", pool=pool)
    stream = session.stream_messages(None, search="audit", limit=250)
    assert [item.id for item in await _drain(stream)] == [51]
    assert ceiling.acquires == 2  # outer stream + one physical page
    assert isinstance(client.sent[0], SearchGlobalRequest)


@pytest.mark.asyncio
async def test_real_iter_messages_scheduled_page_is_ceiling_accounted() -> None:
    """The scheduled iterator variant is one accounted GetScheduledHistory RPC."""
    pool, ceiling = _page_gate_pool()
    client = _RecordingTelethonClient()
    client.respond = lambda request: tl_types.messages.Messages(
        messages=[_channel_message(51)], chats=[], users=[], topics=[]
    )
    session = TelegramTransportSession(client, phone="+66...2247", pool=pool)
    entity = tl_types.InputPeerChannel(channel_id=100, access_hash=0)
    stream = session.stream_messages(entity, scheduled=True, limit=250)
    assert [item.id for item in await _drain(stream)] == [51]
    assert ceiling.acquires == 2  # outer stream + one physical page
    assert isinstance(client.sent[0], GetScheduledHistoryRequest)


@pytest.mark.asyncio
async def test_real_iter_messages_ids_page_is_ceiling_accounted() -> None:
    """The ids iterator variant charges its physical GetMessages dispatch."""
    pool, ceiling = _page_gate_pool()
    client = _RecordingTelethonClient()
    client.respond = lambda request: tl_types.messages.Messages(
        messages=[_channel_message(301), _channel_message(300), _channel_message(299)],
        chats=[],
        users=[],
        topics=[],
    )
    session = TelegramTransportSession(client, phone="+66...2247", pool=pool)
    entity = tl_types.InputPeerChannel(channel_id=100, access_hash=0)
    stream = session.stream_messages(entity, ids=[301, 300, 299])
    assert [item.id for item in await _drain(stream)] == [301, 300, 299]
    assert ceiling.acquires == 2  # outer stream + one physical page
    assert isinstance(client.sent[0], GetChannelMessagesRequest)


@pytest.mark.asyncio
async def test_real_iter_pages_trigger_no_username_resolves() -> None:
    """A resolved InputPeer walks pages without any username resolve (#1827)."""
    pool, ceiling = _page_gate_pool()
    client = _RecordingTelethonClient()
    client.respond = lambda request: _message_slice([51])
    session = TelegramTransportSession(client, phone="+66...2247", pool=pool)
    entity = tl_types.InputPeerChannel(channel_id=100, access_hash=0)
    stream = session.stream_messages(entity, search="audit", limit=250)
    assert [item.id for item in await _drain(stream)] == [51]
    assert not any("ResolveUsername" in type(request).__name__ for request in client.sent)


@pytest.mark.asyncio
async def test_stream_participants_pages_are_ceiling_accounted() -> None:
    pool, ceiling = _page_gate_pool()
    client = _PageDispatchClient(_participants_request, 2, iterator_method="iter_participants")
    session = TelegramTransportSession(client, phone="+66...2247", pool=pool)
    entity = SimpleNamespace(channel_id=100)
    stream = session.stream_participants(entity, limit=450)
    assert await _drain(stream) == [1, 2]
    assert ceiling.acquires == 3  # outer stream + two physical pages
    assert all(isinstance(request, GetParticipantsRequest) for request in client.calls)


@pytest.mark.asyncio
async def test_stream_profile_photos_pages_are_ceiling_accounted() -> None:
    pool, ceiling = _page_gate_pool()
    client = _PageDispatchClient(_profile_photos_request, 2, iterator_method="iter_profile_photos")
    session = TelegramTransportSession(client, phone="+66...2247", pool=pool)
    entity = SimpleNamespace(user_id=42)
    stream = session.stream_profile_photos(entity)
    assert await _drain(stream) == [1, 2]
    assert ceiling.acquires == 3  # outer stream + two physical pages
    assert all(isinstance(request, GetUserPhotosRequest) for request in client.calls)


@pytest.mark.asyncio
async def test_stream_admin_log_pages_are_ceiling_accounted() -> None:
    pool, ceiling = _page_gate_pool()
    client = _PageDispatchClient(_admin_log_request, 2, iterator_method="iter_admin_log")
    session = TelegramTransportSession(client, phone="+66...2247", pool=pool)
    entity = SimpleNamespace(channel_id=100)
    stream = session.stream_admin_log(entity, limit=100)
    assert await _drain(stream) == [1, 2]
    assert ceiling.acquires == 3  # outer stream + two physical pages
    assert all(isinstance(request, GetAdminLogRequest) for request in client.calls)


class _CollectingTelethonClient(TelegramClient):
    """Native get_* methods over fake page iterators for collect-path probes."""

    def __init__(self, request_factory, pages, *, iterator_method) -> None:
        super().__init__(MemorySession(), 1, "a" * 32)
        self._request_factory = request_factory
        self._pages = pages
        self._iterator_method = iterator_method
        self.calls = []
        # Instance attribute shadows the inherited native iter_* so the
        # get_* collect runs over the fake page iterator.
        setattr(self, iterator_method, self._iter)

    async def get_input_entity(self, peer):
        # Warm session cache: the collect path must not resolve anything.
        return peer

    def _iter(self, *args, **kwargs):
        return _PageIterator(self, self._request_factory, self._pages)

    async def __call__(self, request):
        self.calls.append(request)


@pytest.mark.asyncio
async def test_get_participants_collect_gates_each_telethon_page() -> None:
    """``get_participants`` (collect of N pages) gates every physical page."""
    pool, ceiling = _page_gate_pool()
    client = _CollectingTelethonClient(_participants_request, 2, iterator_method="iter_participants")
    session = TelegramTransportSession(client, phone="+66...2247", pool=pool)
    entity = SimpleNamespace(channel_id=100)
    assert await session.get_participants(entity, limit=450) == [1, 2]
    assert ceiling.acquires == 3  # outer collect + two physical pages
    assert all(isinstance(request, GetParticipantsRequest) for request in client.calls)


@pytest.mark.asyncio
async def test_get_profile_photos_collect_gates_each_telethon_page() -> None:
    pool, ceiling = _page_gate_pool()
    client = _CollectingTelethonClient(_profile_photos_request, 2, iterator_method="iter_profile_photos")
    session = TelegramTransportSession(client, phone="+66...2247", pool=pool)
    entity = SimpleNamespace(user_id=42)
    assert await session.get_profile_photos(entity) == [1, 2]
    assert ceiling.acquires == 3  # outer collect + two physical pages
    assert all(isinstance(request, GetUserPhotosRequest) for request in client.calls)


@pytest.mark.asyncio
async def test_get_admin_log_collect_gates_each_telethon_page() -> None:
    pool, ceiling = _page_gate_pool()
    client = _CollectingTelethonClient(_admin_log_request, 2, iterator_method="iter_admin_log")
    session = TelegramTransportSession(client, phone="+66...2247", pool=pool)
    entity = SimpleNamespace(channel_id=100)
    assert await session.get_admin_log(entity, limit=100) == [1, 2]
    assert ceiling.acquires == 3  # outer collect + two physical pages
    assert all(isinstance(request, GetAdminLogRequest) for request in client.calls)


@pytest.mark.asyncio
async def test_dialog_page_budget_refusal_keeps_partial_stream_state() -> None:
    """A refused page leaves received items intact and the stream closable."""
    ceiling = _SpyCeiling()

    class Pool:
        _rate_limit_gate = TelegramRateLimitGate(
            category_limits={
                "dialog_sweep": RateLimitSpec(max_calls=1, window_sec=60),
                "dialogs_page": RateLimitSpec(max_calls=1, window_sec=60),
            }
        )
        _outgoing_ceiling = ceiling

    client = _PageDispatchClient(_dialog_page_request, 3, iterator_method="iter_dialogs")
    session = TelegramTransportSession(client, phone="+66...2247", pool=Pool())
    stream = session.stream_dialogs()
    assert await stream.__anext__() == 1  # received item stays with the caller
    with pytest.raises(TelegramRateLimitedError):
        await stream.__anext__()
    assert len(client.calls) == 1  # the refused page never reached Telethon
    await stream.aclose()


@pytest.mark.asyncio
async def test_message_page_proxy_passes_emitted_object_requests() -> None:
    """Emitted Message objects may use the client for unrelated requests.

    Only the iterator's own page requests belong to the page budget; an
    unrelated request must pass through uncharged.
    """
    pool, ceiling = _page_gate_pool()
    client = _PageDispatchClient(_history_request, 1, iterator_method="iter_messages")
    session = TelegramTransportSession(client, phone="+66...2247", pool=pool)
    entity = SimpleNamespace(channel_id=100)
    stream = session.stream_messages(entity, limit=250)
    assert await _drain(stream) == [1]
    unrelated = _dialog_page_request()
    await client(unrelated)
    assert ceiling.acquires == 2  # outer stream + one page; unrelated uncharged
    assert unrelated in client.calls
