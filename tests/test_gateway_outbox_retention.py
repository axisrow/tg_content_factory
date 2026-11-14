"""#1773: bounded retention for gateway_outbox.

Receipted rows are dropped after their short window; pending rows (uncertain
writes, deferred reads, in-flight intents) keep the longer window so live
retries and idempotent replay always find their intent in place.
"""

import time
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import UUID, uuid4

import pytest

from tg_agent.config import AppConfig
from tg_agent.database import Database
from tg_agent.gateway.outbox import PENDING_RETENTION_DAYS, RECEIPTED_RETENTION_DAYS, ProjectOutbox
from tg_agent.gateway.pool import GatewayPool, GatewayReadDeferredError, GatewayUncertainError
from tg_agent.gateway.protocol import History, Send


async def _insert(db: Database, action_key: str, *, receipt: str | None, age_days: int) -> None:
    created = (datetime.now(timezone.utc) - timedelta(days=age_days)).isoformat()
    await db.execute_write(
        "INSERT INTO gateway_outbox (action_key, request_id, payload, receipt, created_at) VALUES (?,?,?,?,?)",
        (action_key, str(uuid4()), "{}", receipt, created))


async def _row_count(db: Database, where: str) -> int:
    async with db.db.execute(f"SELECT count(*) FROM gateway_outbox WHERE {where}") as cursor:
        return (await cursor.fetchone())[0]


async def test_prepare_stamps_created_at(db):
    outbox = ProjectOutbox(db)
    await outbox.initialize()
    await outbox.prepare(f"inspect:{uuid4()}", History(kind="history", account_id=uuid4(), peer=1))
    async with db.db.execute("SELECT created_at FROM gateway_outbox") as cursor:
        (stamp,) = await cursor.fetchone()
    age = datetime.now(timezone.utc) - datetime.fromisoformat(stamp)
    assert timedelta() <= age < timedelta(minutes=1)


async def test_prune_deletes_rows_past_their_window_and_keeps_recent(db):
    outbox = ProjectOutbox(db)
    await outbox.initialize()
    await _insert(db, "old-receipted", receipt='{"state": "succeeded"}', age_days=RECEIPTED_RETENTION_DAYS + 2)
    await _insert(db, "new-receipted", receipt='{"state": "succeeded"}', age_days=2)
    await _insert(db, "old-pending", receipt=None, age_days=PENDING_RETENTION_DAYS + 1)
    await _insert(db, "new-pending", receipt=None, age_days=2)
    assert await outbox.prune(force=True) == {"receipted": 1, "pending": 1}
    assert await _row_count(db, "action_key='old-receipted'") == 0
    assert await _row_count(db, "action_key='new-receipted'") == 1
    assert await _row_count(db, "action_key='old-pending'") == 0
    assert await _row_count(db, "action_key='new-pending'") == 1


async def test_prune_throttles_between_runs(db):
    outbox = ProjectOutbox(db)
    await outbox.initialize()
    await _insert(db, "old-pending", receipt=None, age_days=PENDING_RETENTION_DAYS + 1)
    assert await outbox.prune() == {"receipted": 0, "pending": 1}
    await _insert(db, "old-pending-2", receipt=None, age_days=PENDING_RETENTION_DAYS + 1)
    # GatewayPool.initialize runs every few seconds; the second call inside the
    # interval must not re-scan.
    assert await outbox.prune() == {"receipted": 0, "pending": 0}
    assert await _row_count(db, "action_key='old-pending-2'") == 1
    assert await outbox.prune(force=True) == {"receipted": 0, "pending": 1}


async def test_old_schema_table_migrates_and_legacy_row_stays_replayable(db):
    async with db.transaction() as connection:
        await connection.execute(
            "CREATE TABLE gateway_outbox ("
            "action_key TEXT PRIMARY KEY, request_id TEXT UNIQUE NOT NULL, "
            "payload TEXT NOT NULL, receipt TEXT)")
        await connection.execute("INSERT INTO gateway_outbox VALUES ('legacy:1', 'req-legacy', '{}', NULL)")
    outbox = ProjectOutbox(db)
    await outbox.initialize()
    async with db.db.execute("SELECT created_at FROM gateway_outbox WHERE action_key='legacy:1'") as cursor:
        (stamp,) = await cursor.fetchone()
    assert stamp is not None
    operation = History(kind="history", account_id=uuid4(), peer=1)
    with pytest.raises(ValueError, match="different intent"):
        # The migrated row still answers prepare: the legacy intent keeps its
        # identity, only the retention clock started.
        await outbox.prepare("legacy:1", operation)
    await outbox.prepare(f"fresh:{uuid4()}", operation)


async def test_request_records_receipt_but_uncertain_write_stays_pending(db):
    pool = GatewayPool(AppConfig(), db, client=SimpleNamespace(
        submit=AsyncMock(),
        wait=AsyncMock(return_value={"state": "succeeded", "result": {"messages": []}}),
    ))
    await pool.outbox.initialize()
    key = f"inspect:{uuid4()}"
    result = await pool.request(History(kind="history", account_id=uuid4(), peer=1), key)
    assert result == {"messages": []}
    async with db.db.execute("SELECT receipt FROM gateway_outbox WHERE action_key=?", (key,)) as cursor:
        (receipt,) = await cursor.fetchone()
    assert receipt is not None and "succeeded" in receipt

    pool.client = SimpleNamespace(submit=AsyncMock(), wait=AsyncMock(side_effect=TimeoutError("deadline")))
    key = f"send:{uuid4()}"
    with pytest.raises(GatewayUncertainError):
        await pool.request(Send(kind="send", account_id=UUID(int=1), peer=123, text="maybe delivered"), key)
    async with db.db.execute("SELECT receipt FROM gateway_outbox WHERE action_key=?", (key,)) as cursor:
        (receipt,) = await cursor.fetchone()
    assert receipt is None

    # A non-terminal receipt handed back by the server must also stay pending:
    # pruning such a row would re-send the same deterministic key as a new
    # intent (#1814 review).
    keys = []
    for state, payload in (
        ("uncertain", {}),
        ("deferred", {"retry_at": time.time() + 60, "reason": "TelegramRateLimitedError", "wait_seconds": 60}),
    ):
        pool.client = SimpleNamespace(submit=AsyncMock(), wait=AsyncMock(return_value={"state": state,
                                                                                          "result": payload}))
        key = f"send:{uuid4()}"
        keys.append(key)
        with pytest.raises((GatewayUncertainError, GatewayReadDeferredError)):
            await pool.request(Send(kind="send", account_id=UUID(int=1), peer=123, text="unconfirmed"), key)
        async with db.db.execute("SELECT receipt FROM gateway_outbox WHERE action_key=?", (key,)) as cursor:
            (receipt,) = await cursor.fetchone()
        assert receipt is None, state

    # Uncertain/deferred intents survive prune inside the long pending window.
    assert await pool.outbox.prune(force=True) == {"receipted": 0, "pending": 0}
    for key in keys:
        assert await _row_count(db, f"action_key='{key}'") == 1


async def test_pool_initialize_prunes(db):
    outbox = ProjectOutbox(db)
    await outbox.initialize()
    await _insert(db, "stale-pending", receipt=None, age_days=PENDING_RETENTION_DAYS + 1)
    pool = GatewayPool(AppConfig(), db, client=SimpleNamespace(
        accounts=AsyncMock(return_value=[]),
        status=AsyncMock(return_value={"accounts": []}),
        close=AsyncMock(),
    ))
    try:
        await pool.initialize()
    finally:
        await pool.disconnect_all()
    assert await _row_count(db, "action_key='stale-pending'") == 0
