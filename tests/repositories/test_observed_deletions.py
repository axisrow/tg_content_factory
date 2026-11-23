"""Tests for the observed message-deletion store (deletion capture, step 1)."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from tg_agent.utils.datetime import normalize_utc


@pytest.mark.anyio
async def test_record_many_fans_out_and_dedupes(db):
    repo = db.repos.observed_deletions

    assert await repo.record_many("+7001", -100123, [41, 42]) == 2
    # A replayed event inserts nothing new.
    assert await repo.record_many("+7001", -100123, [42]) == 0

    rows = await db.execute_fetchall(
        "SELECT phone, chat_id, message_id FROM observed_deletions ORDER BY message_id"
    )
    assert [tuple(row) for row in rows] == [
        ("+7001", -100123, 41),
        ("+7001", -100123, 42),
    ]


@pytest.mark.anyio
async def test_private_chat_deletions_dedupe_with_null_chat(db):
    repo = db.repos.observed_deletions

    assert await repo.record_many("+7001", None, [7]) == 1
    assert await repo.record_many("+7001", None, [7]) == 0
    # A different phone deleting the same id is a distinct fact.
    assert await repo.record_many("+7002", None, [7]) == 1


@pytest.mark.anyio
async def test_was_deleted_reads_by_phone_id_and_window(db):
    repo = db.repos.observed_deletions

    assert await repo.was_deleted("+7001", 41) is None

    await repo.record_many("+7001", None, [41])
    deleted = await repo.was_deleted("+7001", 41)
    assert deleted is not None
    assert abs((normalize_utc(deleted) - datetime.now(timezone.utc)).total_seconds()) < 5

    old = datetime.now(timezone.utc) - timedelta(hours=2)
    await repo.record_many("+7002", -100200, [5], deleted_at=old)
    since = datetime.now(timezone.utc) - timedelta(minutes=30)
    assert await repo.was_deleted("+7002", 5, since=since) is None
    assert await repo.was_deleted("+7002", 5) is not None
    # Another phone's deletion of the same id does not leak.
    assert await repo.was_deleted("+7003", 5) is None


@pytest.mark.anyio
async def test_prune_drops_only_old_rows(db):
    repo = db.repos.observed_deletions

    old = datetime.now(timezone.utc) - timedelta(days=8)
    await repo.record_many("+7001", None, [1], deleted_at=old)
    await repo.record_many("+7001", None, [2])

    assert await repo.prune(days=7) == 1
    assert await repo.was_deleted("+7001", 1) is None
    assert await repo.was_deleted("+7001", 2) is not None


@pytest.mark.anyio
async def test_record_many_joins_the_callers_transaction_wrapper(db):
    from typing import cast

    from tg_agent.database.facade import Database
    from tg_agent.database.repositories.messages import _TransactionWrites
    from tg_agent.database.repositories.observed_deletions import ObservedDeletionsRepository

    async with db.transaction() as conn:
        writer = cast("Database", _TransactionWrites(conn))
        await ObservedDeletionsRepository(conn, database=writer).record_many("+7001", None, [9])
        cur = await conn.execute("SELECT COUNT(*) FROM observed_deletions")
        assert (await cur.fetchone())[0] == 1

    assert await db.repos.observed_deletions.was_deleted("+7001", 9) is not None
