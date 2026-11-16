"""Gateway message-page reads: reaction-user RPC accounting (#1826).

``include_reaction_users`` fans out one ``messages.getReactionsList`` RPC
per reacted message through the session's raw client — every one of them
bypassed the adapter gate and the process ceiling (issue evidence: one
40-message page = 1 GetHistory + 40 unaccounted reaction RPCs).  These
tests drive the ACTUAL gateway handler (``execute_read``) over a real
Telethon dispatch boundary with offline replayed responses.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest
from telethon import TelegramClient, functions
from telethon import types as tl_types
from telethon.sessions import MemorySession
from telethon_floodgate import (
    HandledFloodWaitError,
    RateLimitSpec,
    TelegramRateLimitedError,
    TelegramRateLimitGate,
)

from tg_agent.gateway.protocol import MessagePage
from tg_agent.gateway.reads import execute_read
from tg_agent.telegram.backends import TelegramTransportSession
from tg_agent.telegram.outgoing_ceiling import OutgoingRateCeiling


class _SpyCeiling(OutgoingRateCeiling):
    """Test double: records every ``acquire`` without ever waiting."""

    def __init__(self) -> None:
        super().__init__(RateLimitSpec(max_calls=10**6, window_sec=30.0))
        self.acquires = 0

    async def acquire(self) -> None:
        self.acquires += 1


class _ReactionRecordingClient(TelegramClient):
    """Real dispatch boundary: one history page + per-message reaction RPCs."""

    def __init__(self, history, *, reaction_response=None, reaction_error=None) -> None:
        super().__init__(MemorySession(), 1, "a" * 32)
        self.sent = []
        self._history = history
        self._reaction_response = reaction_response
        self._reaction_error = reaction_error

    async def __call__(self, request, ordered=False):
        self.sent.append(request)
        if isinstance(request, functions.messages.GetMessageReactionsListRequest):
            if self._reaction_error is not None:
                raise self._reaction_error
            return self._reaction_response
        if isinstance(request, functions.messages.GetHistoryRequest):
            return self._history
        raise AssertionError(f"unexpected dispatch: {request!r}")

    async def get_input_entity(self, peer):
        # Warm session cache: resolved InputPeers resolve without RPCs.
        return peer


class _FakePool:
    """Recording stand-in for the gateway pool surface the read path uses."""

    def __init__(self, gate=None) -> None:
        self._rate_limit_gate = gate if gate is not None else TelegramRateLimitGate()
        self._outgoing_ceiling = _SpyCeiling()
        self.flood_reports: list[tuple[str, int]] = []

    async def resolve_dialog_entity(self, session, account, peer, target_type=None):
        return tl_types.InputPeerChannel(channel_id=100, access_hash=0)

    async def resolve_entity_with_warm(self, session, account, peer, operation=None, reuse_warm_cache=False):
        return tl_types.InputPeerChannel(channel_id=100, access_hash=0)

    async def report_flood(self, phone, wait_seconds):
        self.flood_reports.append((phone, wait_seconds))


class _FakeAccess:
    def __init__(self, session, pool) -> None:
        self._session = session
        self._pool = pool

    async def connection(self, project, account):
        return self._session


def _reacted_message(mid: int) -> tl_types.Message:
    return tl_types.Message(
        id=mid,
        peer_id=tl_types.PeerChannel(channel_id=100),
        date=datetime(2026, 11, 16, tzinfo=timezone.utc),
        message="body",
        reactions=tl_types.MessageReactions(
            results=[
                tl_types.ReactionCount(reaction=tl_types.ReactionEmoji(emoticon="👍"), count=2)
            ]
        ),
    )


def _plain_message(mid: int) -> tl_types.Message:
    return tl_types.Message(
        id=mid,
        peer_id=tl_types.PeerChannel(channel_id=100),
        date=datetime(2026, 11, 16, tzinfo=timezone.utc),
        message="body",
    )


def _empty_reaction_response() -> tl_types.messages.MessageReactionsList:
    return tl_types.messages.MessageReactionsList(
        count=0, reactions=[], chats=[], users=[], next_offset=None
    )


def _page_operation(*, limit: int, include: bool) -> MessagePage:
    return MessagePage(
        kind="message_page",
        account_id=uuid.uuid4(),
        peer=-100100,
        peer_type="channel",
        limit=limit,
        offset_id=0,
        include_reaction_users=include,
    )


async def _run_page(client, pool, *, include: bool, limit: int = 40):
    session = TelegramTransportSession(client, phone="gateway-account", pool=pool)
    access = _FakeAccess(session, pool)
    return await execute_read(access, "project", _page_operation(limit=limit, include=include))


@pytest.mark.asyncio
async def test_message_page_reaction_fanout_is_gated_per_rpc() -> None:
    """40 reacted messages = 40 gated reaction dispatches, ceiling accounted.

    The issue's fixture: 1 GetHistory + 40 reaction RPCs; previously only the
    outer stream and the history page drew ceiling slots.  Every reaction
    dispatch must draw its own (42 total).
    """
    pool = _FakePool()
    history = tl_types.messages.Messages(
        messages=[_reacted_message(mid) for mid in range(40, 0, -1)],
        chats=[],
        users=[],
        topics=[],
    )
    client = _ReactionRecordingClient(history, reaction_response=_empty_reaction_response())
    result = await _run_page(client, pool, include=True)

    assert len(result["messages"]) == 40
    history_dispatches = [r for r in client.sent if isinstance(r, functions.messages.GetHistoryRequest)]
    reaction_dispatches = [
        r for r in client.sent if isinstance(r, functions.messages.GetMessageReactionsListRequest)
    ]
    assert len(history_dispatches) == 1
    assert len(reaction_dispatches) == 40
    assert pool._outgoing_ceiling.acquires == 42  # outer + history page + 40 reactions


@pytest.mark.asyncio
async def test_no_optin_and_no_reactions_make_zero_reaction_rpcs() -> None:
    """``include_reaction_users=False`` or unreacted messages: zero fan-out."""
    pool = _FakePool()
    history = tl_types.messages.Messages(
        messages=[_reacted_message(mid) for mid in range(5, 0, -1)],
        chats=[],
        users=[],
        topics=[],
    )
    client = _ReactionRecordingClient(history, reaction_response=_empty_reaction_response())
    result = await _run_page(client, pool, include=False)
    assert len(result["messages"]) == 5
    assert not any(
        isinstance(r, functions.messages.GetMessageReactionsListRequest) for r in client.sent
    )

    plain = tl_types.messages.Messages(
        messages=[_plain_message(mid) for mid in range(5, 0, -1)],
        chats=[],
        users=[],
        topics=[],
    )
    client2 = _ReactionRecordingClient(plain, reaction_response=_empty_reaction_response())
    result2 = await _run_page(client2, pool, include=True)
    assert len(result2["messages"]) == 5
    assert not any(
        isinstance(r, functions.messages.GetMessageReactionsListRequest) for r in client2.sent
    )


@pytest.mark.asyncio
async def test_reaction_budget_exhaustion_marks_page_incomplete() -> None:
    """A local budget refusal stops the fan-out and is NOT read as complete.

    Two reaction dispatches fit the default bucket; the third is refused
    locally.  The 40 received messages stay, the read reports the reaction
    data as incomplete, and no further reaction RPC is dispatched.
    """
    gate = TelegramRateLimitGate(
        category_limits={"default": RateLimitSpec(max_calls=2, window_sec=60)}
    )
    pool = _FakePool(gate=gate)
    history = tl_types.messages.Messages(
        messages=[_reacted_message(mid) for mid in range(40, 0, -1)],
        chats=[],
        users=[],
        topics=[],
    )
    client = _ReactionRecordingClient(history, reaction_response=_empty_reaction_response())
    result = await _run_page(client, pool, include=True)

    reaction_dispatches = [
        r for r in client.sent if isinstance(r, functions.messages.GetMessageReactionsListRequest)
    ]
    assert len(reaction_dispatches) == 2  # fan-out stopped at the refusal
    # The page stream stops at the refusal: the three already-received
    # messages (40..38) stay, the page cursor resumes the rest on retry, and
    # no further RPC of any kind is dispatched (nothing after message 38).
    assert [row["message_id"] for row in result["messages"]] == [40, 39, 38]
    assert result["reaction_users_incomplete"] is True
    # The refused third reaction still waited on the ceiling first — _run's
    # documented ceiling-then-gate order surrenders that slot on refusal.
    assert pool._outgoing_ceiling.acquires == 5  # outer + history page + 2 fetched + 1 refused


@pytest.mark.asyncio
async def test_reaction_floodwait_follows_shared_flood_handling() -> None:
    """A Telegram FloodWait on a reaction RPC is translated and reported.

    The raw-client path raised a bare FloodWaitError that no shared handler
    saw; through the adapter the wait is reported to the pool and surfaces
    as HandledFloodWaitError with its retry deadline.
    """
    from telethon.errors import FloodWaitError

    flood = FloodWaitError(request=None, capture=0)
    flood.seconds = 23
    pool = _FakePool()
    history = tl_types.messages.Messages(
        messages=[_reacted_message(mid) for mid in range(3, 0, -1)],
        chats=[],
        users=[],
        topics=[],
    )
    client = _ReactionRecordingClient(history, reaction_error=flood)
    with pytest.raises(HandledFloodWaitError):
        await _run_page(client, pool, include=True)
    assert pool.flood_reports == [("gateway-account", 23)]


@pytest.mark.asyncio
async def test_helper_refuses_to_swallow_local_throttling() -> None:
    """``fetch_message_reaction_users`` must not mask gate refusals as gaps.

    A TelegramRateLimitedError (local budget) and a HandledFloodWaitError
    (Telegram's wait, already reported) must propagate instead of degrading
    into an ``unavailable`` result that reads like "no reaction data".
    """
    from telethon.errors import FloodWaitError

    from tg_agent.telegram.reactions import fetch_message_reaction_users

    gate = TelegramRateLimitGate(
        category_limits={"default": RateLimitSpec(max_calls=1, window_sec=60)}
    )
    gate.try_acquire("+15550000001", "default")  # consume the only slot

    class _LimitedPool:
        _rate_limit_gate = gate
        _outgoing_ceiling = _SpyCeiling()

    class _NeverDispatchClient:
        async def __call__(self, request):
            raise AssertionError("a refused dispatch must not reach the client")

    session = TelegramTransportSession(
        _NeverDispatchClient(), phone="+15550000001", pool=_LimitedPool()
    )
    with pytest.raises(TelegramRateLimitedError):
        await fetch_message_reaction_users(session, SimpleNamespace(), 1)

    flood = FloodWaitError(request=None, capture=0)
    flood.seconds = 30

    class _FloodClient(TelegramClient):
        def __init__(self) -> None:
            super().__init__(MemorySession(), 1, "a" * 32)

        async def __call__(self, request, ordered=False):
            raise flood

    flood_pool = _FakePool()
    flood_session = TelegramTransportSession(
        _FloodClient(), phone="+15550000002", pool=flood_pool
    )
    with pytest.raises(HandledFloodWaitError):
        await fetch_message_reaction_users(flood_session, SimpleNamespace(), 1)
    assert flood_pool.flood_reports  # reported through the shared machinery
