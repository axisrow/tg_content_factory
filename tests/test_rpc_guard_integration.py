"""Offline publication regressions at the real Telethon sender boundary."""
import asyncio
from collections import Counter
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from telethon import TelegramClient, errors, functions, types
from telethon.sessions import MemorySession
from telethon_floodgate import (
    FloodCircuitBreaker,
    FloodWaitInfo,
    HandledFloodWaitError,
    RateLimitSpec,
    ResolveRateLimiter,
    TelegramRateLimitedError,
    TelegramRateLimitGate,
    TelegramRpcDeferredError,
)

from tg_agent.telegram.backends import TelegramTransportSession
from tg_agent.telegram.outgoing_ceiling import OutgoingRateCeiling
from tg_agent.telegram.rpc_guard import describe_rpc_failure, diagnostic_fields

PHONE = "11111111-1111-1111-1111-111111111111"
PEER = types.InputPeerUser(42, 42)


def make_pool(*, send_cap=100, resolve_cap=2):
    return SimpleNamespace(
        _rate_limit_gate=TelegramRateLimitGate(
            category_limits={"send": RateLimitSpec(send_cap, 60, 0)},
            peer_limits={"send:user": RateLimitSpec(send_cap, 60, 0)},
        ),
        _resolve_rate_limiter=ResolveRateLimiter(max_calls=resolve_cap, jitter_sec=0),
        _outgoing_ceiling=OutgoingRateCeiling(RateLimitSpec(10000, 30)),
        _flood_breaker=FloodCircuitBreaker(),
        report_flood=AsyncMock(), report_family_flood=AsyncMock(),
        family_flood_deadline=AsyncMock(return_value=None),
        get_resolve_username_backoff_remaining_sec=lambda phone: 0,
        is_resolve_ramp_up_active=lambda phone: False,
        persist_resolve_limiter_snapshot=AsyncMock(), persist_resolve_username_backoff=AsyncMock(),
    )


def make_session(pool, *, flood=None, phone=PHONE, send_flood=0, pending_send=False):
    client = TelegramClient(MemorySession(), 1, "offline", flood_sleep_threshold=0)
    client.session.process_entities([types.User(id=42, access_hash=42, first_name="Known")])
    dispatched = []

    def send(request, ordered=False):
        futures = []
        for raw in request if isinstance(request, list) else [request]:
            rpc = raw
            while hasattr(rpc, "query"):
                rpc = rpc.query
            dispatched.append(rpc)
            future = asyncio.get_running_loop().create_future()
            if isinstance(rpc, functions.contacts.ResolveUsernameRequest):
                if flood:
                    future.set_exception(errors.FloodWaitError(request=rpc, capture=flood))
                    futures.append(future)
                    continue
                uid = 1000 + len(dispatched)
                user = types.User(id=uid, access_hash=uid, first_name="Mention", username=rpc.username)
                result = types.contacts.ResolvedPeer(peer=types.PeerUser(uid), chats=[], users=[user])
            elif isinstance(rpc, functions.users.GetUsersRequest):
                result = [types.User(id=999, access_hash=999, first_name="Self", is_self=True)]
            elif isinstance(rpc, (functions.messages.SendMessageRequest, functions.messages.SendMediaRequest)):
                if send_flood:
                    future.set_exception(errors.FloodWaitError(request=rpc, capture=send_flood))
                    futures.append(future)
                    continue
                if pending_send:
                    futures.append(future)
                    continue
                result = types.UpdateShortSentMessage(id=len(dispatched), pts=1, pts_count=1,
                                                     date=datetime.now(timezone.utc), out=True)
            elif isinstance(rpc, functions.messages.EditMessageRequest):
                result = types.Updates(updates=[], users=[], chats=[], date=datetime.now(timezone.utc), seq=0)
            else:
                raise AssertionError(type(rpc).__name__)
            future.set_result(result)
            futures.append(future)
        return futures if isinstance(request, list) else futures[0]

    client._sender.send = send
    session = TelegramTransportSession(client, phone=phone, pool=pool)
    return session, dispatched


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", ["send", "edit", "caption"])
async def test_exhausted_resolve_budget_blocks_hidden_mentions(mode):
    pool = make_pool()
    for _ in range(2):
        assert pool._resolve_rate_limiter.try_acquire(PHONE) == 0
    session, sent = make_session(pool)
    text = " ".join(f"[x](@audituser{i})" for i in range(100))
    with pytest.raises(TelegramRateLimitedError):
        if mode == "send":
            await session.send_message(PEER, text)
        elif mode == "edit":
            await session.edit_message(PEER, 5, text)
        else:
            await session.publish_files(PEER, types.InputMediaDice("🎲"), caption=text)
    assert sent == []
    assert pool._rate_limit_gate.snapshot(PHONE)["categories"]["send"]["used"] == 0


@pytest.mark.asyncio
async def test_plain_text_does_not_resolve_unrelated_usernames():
    pool = make_pool()
    session, sent = make_session(pool)
    await session.send_message(PEER, "plain @audituser0 https://t.me/audituser1")
    assert [type(r).__name__ for r in sent] == ["SendMessageRequest"]
    assert pool._resolve_rate_limiter.used(PHONE) == 0
    assert pool._rate_limit_gate.snapshot(PHONE)["categories"]["send"]["used"] == 1


@pytest.mark.asyncio
async def test_native_conversation_uses_shared_send_budget():
    pool = make_pool(send_cap=1)
    session, sent = make_session(pool)
    assert pool._rate_limit_gate.try_acquire(PHONE, "send") == 0
    with pytest.raises(TelegramRateLimitedError):
        async with session.conversation(PEER) as conversation:
            await conversation.send_message("/newbot")
    assert sent == []


@pytest.mark.asyncio
async def test_nested_resolve_flood_reports_resolve_family():
    pool = make_pool()
    session, sent = make_session(pool, flood=3600)
    with pytest.raises(HandledFloodWaitError) as caught:
        await session.send_message(PEER, "[x](@audituser)")
    assert [type(r).__name__ for r in sent] == ["ResolveUsernameRequest"]
    pool.report_flood.assert_not_awaited()
    pool.report_family_flood.assert_awaited_once()
    assert pool.report_family_flood.await_args.args[:3] == (PHONE, 3600, "resolve")
    assert caught.value.info.rpc == "ResolveUsernameRequest"
    # #1828: a nested resolve flood is not a send failure — nothing was dispatched.
    assert caught.value.info.delivery_state == "not_sent"
    assert caught.value.info.source == "telegram"


@pytest.mark.asyncio
async def test_send_flood_is_attributed_to_sendmessage_and_stays_uncertain():
    pool = make_pool()
    session, sent = make_session(pool, send_flood=3600)
    with pytest.raises(HandledFloodWaitError) as caught:
        await session.send_message(PEER, "plain text")
    assert [type(r).__name__ for r in sent] == ["SendMessageRequest"]
    # #1828: the send-side flood names SendMessage and must stay distinct from
    # the resolve-side case — the write WAS dispatched, so delivery is uncertain.
    assert caught.value.info.rpc == "SendMessageRequest"
    assert caught.value.info.delivery_state == "uncertain"
    assert getattr(caught.value, "rpc", None) == "SendMessageRequest"
    # write-family flood: account-wide report, never the read-only family
    pool.report_flood.assert_awaited_once()
    pool.report_family_flood.assert_not_awaited()


@pytest.mark.asyncio
async def test_local_refusal_names_the_rpc_and_stays_local():
    pool = make_pool()
    for _ in range(2):
        assert pool._resolve_rate_limiter.try_acquire(PHONE) == 0
    session, sent = make_session(pool)
    with pytest.raises(TelegramRpcDeferredError) as caught:
        await session.send_message(PEER, "[x](@audituser)")
    assert sent == []
    exc = caught.value
    assert exc.rpc == "ResolveUsernameRequest"
    assert exc.source == "local"
    assert exc.rpc_dispatched is False
    fields = diagnostic_fields(exc)
    assert fields["outer_operation"] == "telegram_send_message"
    assert fields["rpc"] == "ResolveUsernameRequest"
    assert fields["source"] == "local"
    assert fields["delivery_state"] == "not_sent"
    text = describe_rpc_failure(exc)
    assert "local throttling by resolve_username" in text
    assert "rpc=ResolveUsernameRequest" in text
    assert "operation=telegram_send_message" in text
    assert "not dispatched" in text
    assert "retry in" in text


@pytest.mark.asyncio
async def test_timeout_after_a_dispatched_send_stays_delivery_uncertain():
    pool = make_pool()
    session, sent = make_session(pool, pending_send=True)
    with pytest.raises(asyncio.TimeoutError) as caught:
        await asyncio.wait_for(session.send_message(PEER, "hello"), timeout=0.05)
    assert [type(r).__name__ for r in sent] == ["SendMessageRequest"]
    # #1828: the timeout may have left the request in flight — the stamp via
    # wait_for's cause must read "uncertain", never a safe-retry signal.
    fields = diagnostic_fields(caught.value)
    assert fields["rpc"] == "SendMessageRequest"
    assert fields["delivery_state"] == "uncertain"
    assert fields["outer_operation"] == "telegram_send_message"


@pytest.mark.asyncio
async def test_describe_rpc_failure_formats_curated_lines_only():
    info = FloodWaitInfo(
        operation="telegram_send_message", phone=PHONE, wait_seconds=3600,
        next_available_at_utc=datetime.now(timezone.utc), detail="Flood wait 3600s",
        rpc="ResolveUsernameRequest", source="telegram", delivery_state="not_sent",
    )
    text = describe_rpc_failure(HandledFloodWaitError(info))
    assert "Telegram FloodWait" in text
    assert "rpc=ResolveUsernameRequest" in text
    assert "not dispatched" in text
    assert "retry in 3600s" in text
    # old receipt without metadata stays readable and leaks no exception text
    plain = describe_rpc_failure(ValueError("secret body @user"))
    assert plain == "ValueError"
    assert "secret" not in plain


@pytest.mark.asyncio
async def test_two_clients_share_resolve_budget_and_warm_cache_is_free():
    pool = make_pool(resolve_cap=1)
    first, sent1 = make_session(pool)
    second, sent2 = make_session(pool)
    await first.send_message(PEER, "[x](@audituser)")
    await first.send_message(PEER, "[x](@audituser)")
    with pytest.raises(TelegramRateLimitedError):
        await second.send_message(PEER, "[x](@otheruser)")
    assert Counter(type(r).__name__ for r in sent1) == {"ResolveUsernameRequest": 1, "SendMessageRequest": 2}
    assert sent2 == []
    assert pool._resolve_rate_limiter.used(PHONE) == 1


@pytest.mark.asyncio
async def test_cold_mentions_stop_at_remaining_budget_and_spend_only_actual_rpcs():
    pool = make_pool()
    session, sent = make_session(pool)
    with pytest.raises(TelegramRateLimitedError):
        await session.send_message(PEER, " ".join(f"[x](@audituser{i})" for i in range(100)))
    assert [type(r).__name__ for r in sent] == ["ResolveUsernameRequest"] * 2
    assert pool._resolve_rate_limiter.used(PHONE) == 2
    assert pool._outgoing_ceiling._limiter.used("process") == 2
    assert pool._rate_limit_gate.snapshot(PHONE)["categories"]["send"]["used"] == 0


@pytest.mark.asyncio
async def test_rebinding_keeps_guard_and_budget_and_independent_accounts_stay_independent():
    from tg_agent.telegram.rpc_guard import bind_rpc_guard

    pool = make_pool(resolve_cap=1)
    first, sent1 = make_session(pool)
    second, sent2 = make_session(pool, phone="other-account")
    handle = first.raw_client._tg_owned_rpc_guard.handle
    for _ in range(3):
        bind_rpc_guard(first.raw_client, pool, PHONE)
    await first.send_message(PEER, "[x](@audituser)")
    await second.send_message(PEER, "[x](@audituser)")
    assert first.raw_client._tg_owned_rpc_guard.handle is handle
    assert len(sent1) == len(sent2) == 2
    assert pool._resolve_rate_limiter.used(PHONE) == pool._resolve_rate_limiter.used("other-account") == 1


@pytest.mark.asyncio
async def test_owned_custom_client_cannot_silently_bypass_native_dispatch_contract():
    class CustomClient(TelegramClient):
        async def __call__(self, request, ordered=False):
            return None

    pool = make_pool()
    pool._require_rpc_guard = True
    with pytest.raises(ValueError, match="native dispatch contract"):
        TelegramTransportSession(CustomClient(MemorySession(), 1, "offline"), phone=PHONE, pool=pool)


@pytest.mark.asyncio
async def test_botfather_happy_flow_paces_short_peer_limits_without_replaying(monkeypatch):
    from telethon.tl.custom.conversation import Conversation

    from tg_shared import botfather

    clock = [100.0]
    pool = make_pool()
    pool._rate_limit_gate = TelegramRateLimitGate(time_func=lambda: clock[0])
    session, sent = make_session(pool)
    session.raw_client.session.process_entities([
        types.User(id=777, access_hash=777, first_name="BotFather", username="botfather", bot=True),
    ])
    waits = []

    async def advance(seconds):
        waits.append(seconds)
        clock[0] += seconds + 0.001

    session.raw_client._tg_owned_rpc_guard.handle._sleep = advance
    token = "1234567890:" + "A" * 40
    monkeypatch.setattr(Conversation, "get_response", AsyncMock(side_effect=[
        SimpleNamespace(text="Choose a display name"),
        SimpleNamespace(text="Choose a username"),
        SimpleNamespace(text=token),
    ]))
    assert await botfather.create_bot(session, "Audit Name", "audit_test_bot") == token
    assert [type(r).__name__ for r in sent] == ["SendMessageRequest"] * 3
    assert [r.message for r in sent] == ["/newbot", "Audit Name", "audit_test_bot"]
    assert len(waits) == 2 and all(0 < seconds <= 5 for seconds in waits)
    assert pool._rate_limit_gate.snapshot(PHONE)["categories"]["send"]["used"] == 3
