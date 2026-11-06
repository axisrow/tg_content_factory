"""GatewayPool.collect_channel — username fallback on gateway numeric-miss (#1779).

Collection twin of the refresh-card fallback (#1769): history by bare numeric
peer fails with ValueError on every gateway account for channels outside the
account dialog caches (~1400 tasks/day); one stored-@username resolve — which
does not require membership and caches the entity into the account session —
unlocks the numeric history. Real GatewayPool over a scripted stub client,
real :memory: DB.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from datetime import datetime, timezone
from uuid import uuid4

import pytest

from tg_agent.config import AppConfig
from tg_agent.gateway.pool import GatewayOperationError, GatewayPool
from tg_agent.models import Channel


class ScriptedGatewayClient:
    """Receipts by rule, with gateway request-UUID semantics: the first wait
    computes a receipt from stub state, any later wait of the same request
    UUID replays that stored terminal receipt — so a retry reusing an intent
    key can never execute, exactly as in production. History fails with a
    peer-miss ValueError until the username inspect returns a matching
    channel_id (the session-cache unlock), then serves one message page and
    pages out; inspect receipts are scripted per test (a dict, or a callable
    of the submitted peer)."""

    def __init__(self, inspect_receipt: dict | Callable[[object], dict], *, numeric: int,
                 history_always_fails: bool = False):
        self.inspect_receipt = inspect_receipt
        self.numeric = numeric
        self.history_always_fails = history_always_fails
        self.unlocked = False
        self.pages_served = 0
        self.receipts: dict[object, dict] = {}
        self.log: list[tuple[object, object]] = []

    async def submit(self, request, operation):
        self.log.append((request, operation))

    async def wait(self, request, timeout=None):
        if request in self.receipts:
            return self.receipts[request]
        receipt = self.compute_receipt(
            next(op for req, op in self.log if req is request))
        self.receipts[request] = receipt
        return receipt

    def compute_receipt(self, operation) -> dict:
        if operation.kind == "inspect_peer":
            receipt = (self.inspect_receipt(operation.peer) if callable(self.inspect_receipt)
                       else self.inspect_receipt)
            if (receipt.get("result") or {}).get("channel_id") == self.numeric:
                self.unlocked = True
            return receipt
        if self.history_always_fails:
            return {"state": "failed", "result": {"error": "ValueError"}}
        if self.unlocked:
            if self.pages_served:
                return {"state": "succeeded", "result": {"messages": [], "next_id": 0}}
            self.pages_served += 1
            return {"state": "succeeded", "result": {"messages": [
                {"channel_id": self.numeric, "message_id": index + 1, "text": f"post {index + 1}",
                 "date": "2026-10-01T12:00:00Z"}
                for index in range(2)
            ], "next_id": 2}}
        return {"state": "failed", "result": {"error": "ValueError"}}

    async def close(self):
        pass


def _make_pool(db, inspect_receipt, *, numeric: int = 902,
               history_always_fails: bool = False) -> tuple[GatewayPool, ScriptedGatewayClient]:
    """Real GatewayPool over the scripted client; two shared project accounts."""
    stub = ScriptedGatewayClient(inspect_receipt, numeric=numeric,
                                 history_always_fails=history_always_fails)
    pool = GatewayPool(AppConfig(), db, client=stub)
    primary = {"id": str(uuid4()), "phone": "+70000000001", "active": True,
               "flood_until": 0.0, "is_primary": True}
    owner = {"id": str(uuid4()), "phone": "+70000000002", "active": True,
             "flood_until": 0.0, "is_primary": False}
    pool.clients = {primary["phone"]: primary, owner["phone"]: owner}
    asyncio.run(pool.outbox.initialize())
    return pool, stub


def _channel(username: str | None = "fallback902", channel_id: int = 902) -> Channel:
    return Channel(channel_id=channel_id, title="Stale", username=username, channel_type="channel",
                   about="old about", linked_chat_id=None, has_comments=False)


def _add_channel(db, channel: Channel) -> None:
    asyncio.run(db.add_channel(channel))


def _ops(stub) -> list[tuple[str, object]]:
    return [(op.kind, op.peer) for _req, op in stub.log]


_CARD = {
    "channel_id": 902,
    "title": "Fresh",
    "username": "fallback902",
    "channel_type": "channel",
    "deactivate": False,
    "about": "new about",
    "linked_chat_id": 999,
    "has_comments": True,
    "created_at": "2020-01-01T00:00:00+00:00",
}


def test_collect_numeric_miss_falls_back_to_username_and_collects(db):
    pool, stub = _make_pool(db, {"state": "succeeded", "result": dict(_CARD)})
    channel = _channel()
    _add_channel(db, channel)

    collected = asyncio.run(pool.collect_channel(channel))

    assert collected == 2
    ops = _ops(stub)
    # Rotation burns both accounts on the numeric miss, then one username
    # resolve, then the retrying history hits the cached entity.
    assert ops[0] == ("history", 902)
    assert ops[1] == ("history", 902)
    assert ops[2] == ("inspect_peer", "fallback902")
    assert ops[3] == ("history", 902)
    assert ops[-1] == ("history", 902)
    # The retried history is a fresh intent: the failed request's UUID carries
    # a terminal failed receipt the gateway would replay verbatim, so reusing
    # it can never execute (#1782).
    assert stub.log[3][0] != stub.log[0][0]
    # One resolve, one job (#1788): the id-verified card refreshes the row's
    # open metadata — the same fields update_channel_card writes in
    # refresh-card, so no second resolve is ever needed for meta.
    row = asyncio.run(db.repos.channels.get_channel_by_channel_id(902))
    assert row.title == "Fresh"
    assert row.username == "fallback902"
    assert row.about == "new about"
    assert row.linked_chat_id == 999
    assert row.has_comments is True


def test_collect_fallback_clears_terminal_stamp_on_verified_card(db):
    """#1788/#1783 parity: the verified identity clears a stale terminal stamp
    exactly like a successful refresh-card does (update_channel_card)."""
    pool, stub = _make_pool(db, {"state": "succeeded", "result": dict(_CARD)})
    channel = _channel()
    _add_channel(db, channel)
    row = asyncio.run(db.repos.channels.get_channel_by_channel_id(902))
    asyncio.run(db.repos.channels.set_channel_terminal(
        row.id, "UsernameInvalidError", datetime.now(timezone.utc)))

    asyncio.run(pool.collect_channel(channel))

    row = asyncio.run(db.repos.channels.get_channel_by_channel_id(902))
    assert row.title == "Fresh"
    assert row.terminal_code is None
    assert row.terminal_at is None


def test_collect_username_mismatch_stops_collection(db):
    intruder = dict(_CARD, channel_id=555555)
    pool, stub = _make_pool(db, {"state": "succeeded", "result": intruder})
    channel = _channel()
    _add_channel(db, channel)

    with pytest.raises(GatewayOperationError) as excinfo:
        asyncio.run(pool.collect_channel(channel))

    assert excinfo.value.code == "UsernameChannelIdMismatch"
    ops = _ops(stub)
    assert ops.count(("history", 902)) == 2
    assert ops[2] == ("inspect_peer", "fallback902")
    assert len(ops) == 3, "no history retry after an id mismatch"
    row = asyncio.run(db.repos.channels.get_channel_by_channel_id(902))
    assert row.title == "Stale"


def test_collect_username_resolve_miss_is_reported_not_valueerror(db):
    pool, stub = _make_pool(db, {"state": "succeeded", "result": {"gone": True}})
    channel = _channel()
    _add_channel(db, channel)

    with pytest.raises(GatewayOperationError) as excinfo:
        asyncio.run(pool.collect_channel(channel))

    assert excinfo.value.code == "UsernameResolveMiss"
    assert _ops(stub).count(("inspect_peer", "fallback902")) == 1


def test_collect_without_username_keeps_old_valueerror_path(db):
    pool, stub = _make_pool(db, {"state": "succeeded", "result": dict(_CARD)})
    channel = _channel(username=None)
    _add_channel(db, channel)

    with pytest.raises(GatewayOperationError) as excinfo:
        asyncio.run(pool.collect_channel(channel))

    assert excinfo.value.code == "ValueError"
    assert all(kind == "history" for kind, _peer in _ops(stub))


def test_collect_fallback_is_once_per_call(db):
    """History keeps failing even after the unlock: the one-shot fallback must
    give up (re-raise) instead of looping resolve→history forever."""
    pool, stub = _make_pool(db, {"state": "succeeded", "result": dict(_CARD)},
                            history_always_fails=True)
    channel = _channel()
    _add_channel(db, channel)

    async def collect():
        return await asyncio.wait_for(pool.collect_channel(channel), timeout=5)

    with pytest.raises(GatewayOperationError) as excinfo:
        asyncio.run(collect())

    assert excinfo.value.code == "ValueError"
    assert _ops(stub).count(("inspect_peer", "fallback902")) == 1
