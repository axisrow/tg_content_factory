"""Relayed comment_to send (#1903): round trip, gate pricing, teardown.

The live proof lives in the issue: a comment went out from a secondary account
through the daemon relay + floodgate, Telethon's inherited
``send_message(comment_to=…)`` routing through ``RelayTelegramClient``. These
harness-first tests pin the contract around it:

- the discussion lookup (GetDiscussionMessageRequest) and the actual comment
  send both round-trip through the owner connection, the comment landing on
  the linked discussion group as a reply to the thread's top message;
- the gate classifies the lookup under a deliberate rpc category — no
  "not registered in _OPERATION_CATEGORIES" warning, no permissive-default
  pricing (#1903);
- teardown after such a send completes: ``pool.disconnect_all()`` and the
  relay close finish inside the test timeout (the short-lived-CLI hang
  reported in #1903 must not return silently).
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
from datetime import datetime, timezone
from types import SimpleNamespace
from typing import Any, cast

from telethon import functions, types

from tests.test_relay import FakeOwnerClient
from tg_agent.models import Account
from tg_agent.services.telegram_actions import TelegramActionService
from tg_agent.telegram import backends
from tg_agent.telegram.client_pool import ClientPool
from tg_agent.telegram.relay import RelayConnection, RelayTelegramClient, serve_pool_relay
from tg_shared.auth import TelegramAuth

PHONE = "+10000000001"
POST_ID = 5
COMMENT_ID = 6
DATE = datetime(2026, 11, 25, tzinfo=timezone.utc)
CHANNEL_ID = 123
DISCUSSION_ID = 2000

CHANNEL = types.Channel(id=CHANNEL_ID, title="source", photo=types.ChatPhotoEmpty(), date=DATE,
                        access_hash=99, username="source", broadcast=True)
DISCUSSION_GROUP = types.Channel(id=DISCUSSION_ID, title="discussion", photo=types.ChatPhotoEmpty(),
                                 date=DATE, access_hash=77, megagroup=True)
CHANNEL_PEER = types.InputPeerChannel(channel_id=CHANNEL_ID, access_hash=99)


def _message(message_id: int, text: str) -> types.Message:
    return types.Message(id=message_id, peer_id=types.PeerChannel(DISCUSSION_ID), date=DATE,
                         message=text)


def _comment_handler(request):
    if isinstance(request, functions.messages.GetDiscussionMessageRequest):
        assert request.msg_id == POST_ID
        return types.messages.DiscussionMessage(
            messages=[_message(POST_ID, "the post")],
            unread_count=0,
            chats=[DISCUSSION_GROUP],
            users=[],
        )
    if isinstance(request, functions.messages.SendMessageRequest):
        return types.Updates(
            updates=[
                types.UpdateMessageID(random_id=request.random_id, id=COMMENT_ID),
                types.UpdateNewChannelMessage(
                    message=_message(COMMENT_ID, request.message), pts=1, pts_count=1
                ),
            ],
            users=[],
            chats=[DISCUSSION_GROUP],
            date=DATE,
            seq=0,
        )
    return None


def _comment_owner() -> FakeOwnerClient:
    return FakeOwnerClient(_comment_handler, entities={(CHANNEL_ID, 99, "source", None, "source")})


async def test_relayed_comment_to_round_trips_through_the_owner():
    """send_message(comment_to=…) resolves the discussion group and lands there."""
    owner = _comment_owner()
    owner_pool = SimpleNamespace(clients={PHONE: SimpleNamespace(raw_client=owner)})
    handle = await serve_pool_relay(owner_pool, None)
    connection = await RelayConnection.open(handle.endpoint)
    client = RelayTelegramClient(connection, PHONE, flood_sleep_threshold=0)
    try:
        await client.connect()
        await client.load_owner_entities()

        async with asyncio.timeout(10):
            sent = await client.send_message(CHANNEL_PEER, "harness comment", comment_to=POST_ID)

        assert sent.id == COMMENT_ID
        kinds = [type(request).__name__ for _sender, request in owner.calls]
        assert kinds.index("GetDiscussionMessageRequest") < kinds.index("SendMessageRequest")
        send = next(r for _s, r in owner.calls if isinstance(r, functions.messages.SendMessageRequest))
        assert cast(Any, send.peer).channel_id == DISCUSSION_ID
        assert cast(Any, send.reply_to).reply_to_msg_id == POST_ID
        assert send.message == "harness comment"
    finally:
        await connection.close()
        await handle.stop()


async def test_relayed_comment_to_send_then_pool_teardown_completes(db, monkeypatch, caplog):
    """The #1903 hang guard: disconnect_all after a relayed comment send finishes.

    A short-lived CLI once hung here until killed. The whole teardown —
    per-client disconnect, relay close, the post-teardown settle — runs inside
    one ``wait_for`` so a regression fails the test instead of freezing it.
    """
    me = types.User(id=4242, access_hash=1, first_name="Owner", is_self=True)

    def owner_answer(request):
        outcome = _comment_handler(request)
        if outcome is not None:
            return outcome
        # Pool warm-up requests (fetch_me, state, dialogs) answer like the
        # test_client_pool_relay_mode fixture does: a minimal valid shape.
        if isinstance(request, functions.users.GetUsersRequest):
            return [me]
        return me

    owner = FakeOwnerClient(owner_answer, entities={(CHANNEL_ID, 99, "source", None, "source")})
    owner_pool = SimpleNamespace(clients={PHONE: SimpleNamespace(raw_client=owner)})
    handle = await serve_pool_relay(owner_pool, None)
    connection = await RelayConnection.open(handle.endpoint)

    async def no_lock(*_args, **_kwargs):
        raise AssertionError("a relayed pool must not take the session lock")

    monkeypatch.setattr(backends, "acquire_session_lock", no_lock)
    pool = None
    try:
        await db.add_account(Account(phone=PHONE, session_string="owner-only-session", is_active=True))
        pool = ClientPool(TelegramAuth(1, "hash"), db, relay=connection)
        pool._materializer = cast(Any, SimpleNamespace(
            materialize=lambda *_a, **_k: (_ for _ in ()).throw(AssertionError("no session file in relay mode")),
            ensure_empty_env_file=lambda: "",
        ))
        await pool.initialize()

        service = TelegramActionService(pool, db=db)
        with caplog.at_level(logging.WARNING, logger="telethon_floodgate.rate_limit_gate"):
            async with asyncio.timeout(30):
                result = await service.send_message(
                    phone=PHONE,
                    recipient=CHANNEL_PEER,
                    text="teardown comment",
                    comment_to=POST_ID,
                    source="relay_comment_test",
                )
                assert result.message_id == COMMENT_ID
                warnings = [
                    record.getMessage()
                    for record in caplog.records
                    if "not registered in _OPERATION_CATEGORIES" in record.getMessage()
                ]
                assert not warnings, warnings

            # The issue's symptom: a short-lived CLI hung here until killed.
            await asyncio.wait_for(pool.disconnect_all(), timeout=15)
    finally:
        if pool is not None and pool.clients:
            # Teardown already ran when the test body passed; this is the
            # best-effort path when the wait_for above raised instead.
            with contextlib.suppress(Exception):
                await asyncio.wait_for(pool.disconnect_all(), timeout=10)
        await handle.stop()


async def test_relayed_comment_send_costs_a_deliberate_gate_category(caplog):
    """The #1903 pricing: the discussion lookup is not an unknown-op fallback.

    At the policy level: the owned-guard policy resolves the rpc tag to a
    real configured bucket, with no warn-once from the gate.
    """
    from telethon_floodgate import TelegramRateLimitGate

    from tg_agent.telegram.rpc_guard import GAP_RPC_POLICY

    gate = TelegramRateLimitGate()
    request = functions.messages.GetDiscussionMessageRequest(peer=CHANNEL_PEER, msg_id=POST_ID)
    operation = GAP_RPC_POLICY.operation_for(request)
    assert operation == "rpc_default"  # floodgate's registered boundary tag
    with caplog.at_level(logging.WARNING):
        category = gate.category_for(operation)
    assert category == "default"
    assert not [r for r in caplog.records if "not registered" in r.getMessage()]
    # The gap rows must not clobber floodgate's own registrations.
    assert GAP_RPC_POLICY.operation_for(
        functions.messages.SendMessageRequest(peer=CHANNEL_PEER, message="x")
    ) == "rpc_send"
    # And the category is a real bucket, not a typo that silently re-prices.
    snapshot_categories = cast(dict, TelegramRateLimitGate().snapshot("+15551230001"))["categories"]
    assert category in snapshot_categories
