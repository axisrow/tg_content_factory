"""Tests for IncomingDmsRepository (#1427)."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from src.models import IncomingDm


def _dm(
    phone: str = "+111",
    chat_id: int = 42,
    message_id: int = 7,
    *,
    received_at: datetime | None = None,
    text: str | None = "hi",
) -> IncomingDm:
    return IncomingDm(
        phone=phone,
        chat_id=chat_id,
        message_id=message_id,
        text=text,
        message_date=received_at,
        received_at=received_at or datetime.now(timezone.utc),
    )


@pytest.fixture
async def dms_repo(db):
    from src.database.repositories.incoming_dms import IncomingDmsRepository

    return IncomingDmsRepository(db.db, database=db)


async def test_record_inserts_and_sets_received_at(dms_repo, db):
    assert await dms_repo.record(_dm()) is True

    cur = await db.db.execute("SELECT * FROM incoming_dms")
    row = await cur.fetchone()
    assert row["phone"] == "+111"
    assert row["chat_id"] == 42
    assert row["message_id"] == 7
    assert row["text"] == "hi"
    assert row["received_at"]
    assert row["processed"] == 0


async def test_record_duplicate_is_idempotent(dms_repo, db):
    dm = _dm()
    assert await dms_repo.record(dm) is True
    assert await dms_repo.record(dm) is False

    cur = await db.db.execute("SELECT COUNT(*) AS n FROM incoming_dms")
    assert (await cur.fetchone())["n"] == 1


async def test_record_duplicate_keeps_processed(dms_repo, db):
    """Догон #1428: повторная запись дубля не перезаписывает решение о разборе."""
    assert await dms_repo.record(_dm(message_id=7), processed=True) is True

    assert await dms_repo.record(_dm(message_id=7), processed=False) is False
    cur = await db.db.execute("SELECT processed FROM incoming_dms WHERE message_id = 7")
    assert (await cur.fetchone())["processed"] == 1  # не откатилось в unprocessed

    assert await dms_repo.record(_dm(chat_id=43, message_id=8), processed=False) is True
    assert await dms_repo.record(_dm(chat_id=43, message_id=8), processed=True) is False
    cur = await db.db.execute("SELECT processed FROM incoming_dms WHERE message_id = 8")
    assert (await cur.fetchone())["processed"] == 0


async def test_max_message_id_is_dialog_watermark(dms_repo):
    """Водяной знак догона: MAX(message_id) по диалогу; пустой диалог — 0."""
    assert await dms_repo.max_message_id("+111", 42) == 0

    await dms_repo.record(_dm(chat_id=42, message_id=7))
    await dms_repo.record(_dm(chat_id=42, message_id=9))
    await dms_repo.record(_dm(chat_id=43, message_id=100))

    assert await dms_repo.max_message_id("+111", 42) == 9


async def test_uniqueness_is_per_dialog_not_global(dms_repo):
    assert await dms_repo.record(_dm(chat_id=1, message_id=7)) is True
    # Тот же message_id в другом диалоге — отдельное сообщение.
    assert await dms_repo.record(_dm(chat_id=2, message_id=7)) is True
    assert await dms_repo.count_unprocessed() == 2


async def test_prune_expired_deletes_only_stale(dms_repo, db):
    old = datetime.now(timezone.utc) - timedelta(hours=2)
    await dms_repo.record(_dm(chat_id=1, message_id=1, received_at=old))
    await dms_repo.record(_dm(chat_id=1, message_id=2))

    deleted = await dms_repo.prune_expired(older_than_seconds=3600)

    assert deleted == 1
    cur = await db.db.execute("SELECT message_id FROM incoming_dms")
    assert [r["message_id"] for r in await cur.fetchall()] == [2]


async def test_record_prunes_expired_on_write(dms_repo, db):
    old = datetime.now(timezone.utc) - timedelta(hours=25)
    await dms_repo.record(_dm(chat_id=1, message_id=1, received_at=old))

    await dms_repo.record(_dm(chat_id=1, message_id=2))

    cur = await db.db.execute("SELECT message_id FROM incoming_dms")
    assert [r["message_id"] for r in await cur.fetchall()] == [2]


async def test_mark_processed_and_count_unprocessed(dms_repo):
    await dms_repo.record(_dm(chat_id=1, message_id=1))
    await dms_repo.record(_dm(chat_id=1, message_id=2))
    await dms_repo.record(_dm(chat_id=2, message_id=3))

    updated = await dms_repo.mark_processed("+111", 1, [1, 2])

    assert updated == 2
    assert await dms_repo.count_unprocessed() == 1
    # Повторная пометка не меняет счётчик.
    await dms_repo.mark_processed("+111", 1, [1, 2])
    assert await dms_repo.count_unprocessed() == 1


async def test_mark_processed_empty_ids_is_noop(dms_repo):
    assert await dms_repo.mark_processed("+111", 1, []) == 0
