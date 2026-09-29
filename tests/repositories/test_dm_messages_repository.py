"""Tests for DmMessagesRepository (#1453) — постоянный архив DM, обе стороны."""

from __future__ import annotations

from datetime import datetime, timezone

import pytest

from src.models import DmMessage


def _dm(
    phone: str = "+111",
    chat_id: int = 42,
    message_id: int = 7,
    *,
    out: bool = False,
    text: str | None = "hi",
) -> DmMessage:
    return DmMessage(
        phone=phone,
        chat_id=chat_id,
        message_id=message_id,
        out=out,
        text=text,
        message_date=datetime.now(timezone.utc),
        received_at=datetime.now(timezone.utc),
    )


@pytest.fixture
async def repo(db):
    from src.database.repositories.dm_messages import DmMessagesRepository

    return DmMessagesRepository(db.db, database=db)


async def test_record_inserts_direction_and_timestamps(repo, db):
    assert await repo.record(_dm()) is True
    assert await repo.record(_dm(message_id=8, out=True, text="ответ")) is True

    cur = await db.db.execute(
        "SELECT * FROM dm_messages WHERE message_id = 8"
    )
    row = await cur.fetchone()
    assert row["out"] == 1
    assert row["text"] == "ответ"
    assert row["received_at"]


async def test_record_duplicate_is_idempotent(repo):
    dm = _dm()
    assert await repo.record(dm) is True
    assert await repo.record(dm) is False
    assert await repo.record(_dm(out=True, text="переписанный")) is False


async def test_max_message_id_is_backfill_watermark(repo):
    """Возобновление бэкфилла: MAX(message_id) диалога; пустой диалог — 0."""
    assert await repo.max_message_id("+111", 42) == 0

    await repo.record(_dm(chat_id=42, message_id=7))
    await repo.record(_dm(chat_id=42, message_id=9, out=True))
    await repo.record(_dm(chat_id=43, message_id=100))

    assert await repo.max_message_id("+111", 42) == 9
    assert await repo.max_message_id("+111", 43) == 100
    assert await repo.max_message_id("+999", 42) == 0


async def test_count_and_direction_split(repo):
    assert await repo.count() == 0
    assert await repo.count("+111") == 0

    await repo.record(_dm(chat_id=42, message_id=1))
    await repo.record(_dm(chat_id=42, message_id=2, out=True))
    await repo.record(_dm(phone="+222", chat_id=42, message_id=3))

    assert await repo.count() == 3
    assert await repo.count("+111") == 2
    assert await repo.count_by_direction("+111") == (1, 1)
    assert await repo.count_by_direction("+222") == (1, 0)
