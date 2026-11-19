"""Flood-guard state survives the process (#1868 follow-up).

The proactive gate (category and per-peer windows), the flood circuit breaker
and the process-wide outgoing ceiling used to live only in memory, so every
daemon restart and every daemon-less CLI run started on a full flood budget.
These tests pin the round trip through the DB setting and who may write it.
"""

from __future__ import annotations

import asyncio
import json
import logging
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from telethon_floodgate import FloodCircuitBreaker, RateLimitSpec, TelegramRateLimitGate
from telethon_floodgate.flood_breaker import TelegramOperationSuspendedError

from tg_agent.runtime import worker
from tg_agent.telegram.client_pool import ClientPool
from tg_agent.telegram.flood_snapshot import (
    FLOOD_GUARD_SNAPSHOT_SETTING,
    restore_flood_guards,
    snapshot_flood_guards,
)
from tg_agent.telegram.outgoing_ceiling import OutgoingRateCeiling
from tg_shared.auth import TelegramAuth

PHONE = "+10000000001"
OTHER = "+10000000002"


def _round_trip(payload: dict) -> dict:
    """What the next process reads back from the settings table."""
    return json.loads(json.dumps(payload))


def _gate(**limits) -> TelegramRateLimitGate:
    return TelegramRateLimitGate(
        category_limits={"history": RateLimitSpec(max_calls=3, window_sec=60.0, jitter_sec=0.0)},
        peer_limits={"send:user": RateLimitSpec(max_calls=1, window_sec=60.0, jitter_sec=0.0)},
        jitter_func=lambda low, high: 0.0,
        **limits,
    )


def test_category_windows_carry_the_spent_budget_into_a_fresh_gate():
    old = _gate()
    for _ in range(3):
        assert old.try_acquire(PHONE, "history") == 0.0
    assert old.try_acquire(PHONE, "history") > 0  # the window is full

    fresh = _gate()
    counts = restore_flood_guards(_round_trip(snapshot_flood_guards(gate=old)), gate=fresh)

    assert counts["categories"] >= 1
    assert fresh.try_acquire(PHONE, "history") > 0  # still spent after the "restart"
    assert fresh.try_acquire(OTHER, "history") == 0.0  # other accounts are untouched


def test_per_peer_windows_survive_too():
    old = _gate()
    assert old.try_acquire(PHONE, "send", peer="user:42") == 0.0

    fresh = _gate()
    counts = restore_flood_guards(_round_trip(snapshot_flood_guards(gate=old)), gate=fresh)

    assert counts["peers"] == 1
    assert fresh.try_acquire(PHONE, "send", peer="user:42") > 0
    assert fresh.try_acquire(PHONE, "send", peer="user:43") == 0.0


def test_an_open_circuit_keeps_its_remaining_cooldown():
    old = FloodCircuitBreaker(threshold=1, cooldown_seconds=300.0)
    old.record_flood("telegram_stream_messages", PHONE)
    payload = _round_trip(snapshot_flood_guards(breaker=old))
    payload["taken_at"] -= 200  # the restart took 200 s

    fresh = FloodCircuitBreaker(threshold=1, cooldown_seconds=300.0)
    assert restore_flood_guards(payload, breaker=fresh)["breaker"] == 1

    with pytest.raises(TelegramOperationSuspendedError) as suspended:
        fresh.check("telegram_stream_messages", PHONE)
    assert 90 < suspended.value.retry_after_sec <= 100
    fresh.check("telegram_stream_messages", OTHER)  # other accounts are not suspended


def test_a_half_open_trial_of_the_old_process_becomes_a_fresh_trial():
    clock = [1000.0]
    old = FloodCircuitBreaker(threshold=1, cooldown_seconds=10.0, time_func=lambda: clock[0])
    old.record_flood("telegram_send_message", PHONE)
    clock[0] += 11
    old.check("telegram_send_message", PHONE)  # the trial is granted, then the process dies

    fresh = FloodCircuitBreaker(threshold=1, cooldown_seconds=10.0)
    restore_flood_guards(_round_trip(snapshot_flood_guards(breaker=old)), breaker=fresh)

    fresh.check("telegram_send_message", PHONE)  # one caller gets the new trial…
    with pytest.raises(TelegramOperationSuspendedError):
        fresh.check("telegram_send_message", PHONE)  # …everyone else waits for its outcome


def test_flood_counts_below_the_threshold_are_kept():
    old = FloodCircuitBreaker(threshold=2, cooldown_seconds=300.0)
    old.record_flood("telegram_send_message", PHONE)

    fresh = FloodCircuitBreaker(threshold=2, cooldown_seconds=300.0)
    restore_flood_guards(_round_trip(snapshot_flood_guards(breaker=old)), breaker=fresh)
    fresh.record_flood("telegram_send_message", PHONE)  # the second flood trips it

    with pytest.raises(TelegramOperationSuspendedError):
        fresh.check("telegram_send_message", PHONE)


def test_the_process_wide_ceiling_window_survives():
    spec = RateLimitSpec(max_calls=2, window_sec=60.0)
    old = OutgoingRateCeiling(spec)
    old.commit_batch([1, 2])

    fresh = OutgoingRateCeiling(spec)
    assert restore_flood_guards(_round_trip(snapshot_flood_guards(ceiling=old)), ceiling=fresh)["ceiling"] == 1

    assert fresh.peek_batch([1]) > 0


def test_a_malformed_component_is_skipped_and_the_rest_still_apply(caplog):
    old_breaker = FloodCircuitBreaker(threshold=1, cooldown_seconds=300.0)
    old_breaker.record_flood("telegram_send_message", PHONE)
    payload = _round_trip(snapshot_flood_guards(breaker=old_breaker))
    payload["gate"] = {"categories": {"history": "garbage"}}

    fresh_gate, fresh_breaker = _gate(), FloodCircuitBreaker(threshold=1, cooldown_seconds=300.0)
    with caplog.at_level(logging.WARNING):
        counts = restore_flood_guards(payload, gate=fresh_gate, breaker=fresh_breaker)

    assert "gate skipped" in caplog.text
    assert counts["breaker"] == 1
    assert fresh_gate.try_acquire(PHONE, "history") == 0.0


def test_an_unknown_snapshot_format_is_ignored(caplog):
    with caplog.at_level(logging.WARNING):
        counts = restore_flood_guards({"version": 99}, gate=_gate())
    assert not any(counts.values())
    assert "unknown format" in caplog.text


# --------------------------------------------------------------------- pools


async def _restored(pool: ClientPool, db) -> ClientPool:
    """What ``initialize`` does: replay once, remember that it did."""
    pool._flood_guard_restore_task = asyncio.ensure_future(pool.restore_flood_guard_snapshot(db))
    await pool._flood_guard_restore_task
    return pool


async def test_an_owner_pool_persists_and_the_next_one_restores(db):
    first = await _restored(ClientPool(TelegramAuth(1, "hash"), db), db)
    gate = first._rate_limit_gate
    while gate.try_acquire(PHONE, "history") == 0.0:
        pass  # spend the history window

    await first.persist_flood_guard_snapshot()
    second = await _restored(ClientPool(TelegramAuth(1, "hash"), db), db)

    assert json.loads(await db.get_setting(FLOOD_GUARD_SNAPSHOT_SETTING))["version"] == 1
    assert second._rate_limit_gate.try_acquire(PHONE, "history") > 0


async def test_a_pool_that_never_restored_does_not_overwrite_the_snapshot(db):
    owner = await _restored(ClientPool(TelegramAuth(1, "hash"), db), db)
    owner._rate_limit_gate.try_acquire(PHONE, "history")
    await owner.persist_flood_guard_snapshot()
    stored = await db.get_setting(FLOOD_GUARD_SNAPSHOT_SETTING)

    await ClientPool(TelegramAuth(1, "hash"), db).persist_flood_guard_snapshot()

    assert await db.get_setting(FLOOD_GUARD_SNAPSHOT_SETTING) == stored


async def test_a_relayed_pool_neither_restores_nor_writes(db):
    owner = await _restored(ClientPool(TelegramAuth(1, "hash"), db), db)
    while owner._rate_limit_gate.try_acquire(PHONE, "history") == 0.0:
        pass
    await owner.persist_flood_guard_snapshot()
    stored = await db.get_setting(FLOOD_GUARD_SNAPSHOT_SETTING)

    relayed = await _restored(ClientPool(TelegramAuth(1, "hash"), db, relay=object()), db)
    # The owner gates a relayed pool's requests; its local guards stay fresh…
    assert relayed._rate_limit_gate.try_acquire(PHONE, "history") == 0.0
    await relayed.persist_flood_guard_snapshot()
    # …and it never overwrites the owner's snapshot.
    assert await db.get_setting(FLOOD_GUARD_SNAPSHOT_SETTING) == stored


async def test_a_corrupt_setting_never_fails_the_pool(db, caplog):
    await db.set_setting(FLOOD_GUARD_SNAPSHOT_SETTING, "not-json")
    with caplog.at_level(logging.WARNING):
        pool = await _restored(ClientPool(TelegramAuth(1, "hash"), db), db)
    assert "not JSON" in caplog.text
    assert pool._rate_limit_gate.try_acquire(PHONE, "history") == 0.0


async def test_initialize_restores_once_per_pool_lifetime(db, monkeypatch):
    pool = ClientPool(TelegramAuth(1, "hash"), db)
    calls = []

    async def restore(db_arg):
        calls.append(db_arg)

    monkeypatch.setattr(pool, "restore_flood_guard_snapshot", restore)
    await pool.initialize()
    await pool.initialize()

    assert calls == [db]


async def test_the_worker_saves_the_guards_through_its_pool():
    pool = SimpleNamespace(persist_flood_guard_snapshot=AsyncMock())

    await worker._persist_flood_guards(SimpleNamespace(pool=pool))

    pool.persist_flood_guard_snapshot.assert_awaited_once()


async def test_the_gateway_saves_its_shared_pool_guards():
    from tg_agent.gateway.telegram import LiveTelegramAccess

    access = LiveTelegramAccess(SimpleNamespace(), SimpleNamespace())
    await access.persist_flood_guards()  # no pool yet: nothing to save, no error
    access._pool = SimpleNamespace(persist_flood_guard_snapshot=AsyncMock())

    await access.persist_flood_guards()

    access._pool.persist_flood_guard_snapshot.assert_awaited_once()
