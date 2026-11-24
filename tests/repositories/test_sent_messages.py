"""Tests for the sent-messages registry (#1893)."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest


@pytest.mark.anyio
async def test_record_many_inserts_and_dedupes(db):
    repo = db.repos.sent_messages

    assert (
        await repo.record_many(
            "+7001", -100123, [41], chat_type="channel", source="dialogs_send",
            recipient="@chat", text="привет",
        )
        == 1
    )
    # A replayed write inserts nothing new.
    assert (
        await repo.record_many("+7001", -100123, [41], chat_type="channel", source="dialogs_send")
        == 0
    )
    # The DM shape (NULL chat) dedupes the same way.
    assert await repo.record_many("+7001", None, [7], chat_type="dm", source="dm_draft") == 1
    assert await repo.record_many("+7001", None, [7], chat_type="dm", source="dm_draft") == 0
    # Another phone is a distinct fact.
    assert await repo.record_many("+7002", -100123, [41], chat_type="channel", source="agent") == 1

    rows = await db.execute_fetchall(
        "SELECT phone, chat_id, message_id, chat_type, source FROM sent_messages ORDER BY id"
    )
    assert [tuple(row) for row in rows] == [
        ("+7001", -100123, 41, "channel", "dialogs_send"),
        ("+7001", None, 7, "dm", "dm_draft"),
        ("+7002", -100123, 41, "channel", "agent"),
    ]


@pytest.mark.anyio
async def test_list_joins_deletion_status_by_chat_shape(db):
    sm = db.repos.sent_messages
    od = db.repos.observed_deletions

    await sm.record_many("+7001", -100200, [5], chat_type="channel", source="pipeline_publish")
    await sm.record_many("+7001", None, [7], chat_type="dm", source="dm_draft")
    await sm.record_many("+7001", -100200, [9], chat_type="channel", source="agent")
    await od.record_many("+7001", -100200, [5])
    await od.record_many("+7001", None, [7])

    rows = {row["message_id"]: row for row in await sm.list()}
    assert rows[5]["deleted_at"] is not None
    assert rows[7]["deleted_at"] is not None
    assert rows[9]["deleted_at"] is None


@pytest.mark.anyio
async def test_chat_shape_never_leaks_between_rows(db):
    sm = db.repos.sent_messages
    od = db.repos.observed_deletions

    # A DM registry row must not be matched by a channel deletion of the same
    # id, and a channel row must not be matched by a DM (NULL-chat) deletion.
    await sm.record_many("+7001", None, [11], chat_type="dm", source="dm_draft")
    await sm.record_many("+7001", -100200, [12], chat_type="channel", source="agent")
    await od.record_many("+7001", -100200, [11])
    await od.record_many("+7001", None, [12])

    rows = {row["message_id"]: row for row in await sm.list()}
    assert rows[11]["deleted_at"] is None
    assert rows[12]["deleted_at"] is None


@pytest.mark.anyio
async def test_deletion_observed_by_another_account_still_marks_deleted(db):
    """The observer of a channel deletion is usually a different pool account
    than the sender (#1898): the report join must not require phone equality —
    "any account saw it deleted" is deleted, same semantics as `messages wait`."""
    sm = db.repos.sent_messages
    od = db.repos.observed_deletions

    await sm.record_many("+7001", -100200, [21], chat_type="channel", source="dialogs_send")
    await od.record_many("+7002", -100200, [21])

    rows = await sm.list(phone="+7001")
    assert len(rows) == 1
    assert rows[0]["deleted_at"] is not None


@pytest.mark.anyio
async def test_multiple_observers_dedupe_into_one_status(db):
    """Two accounts observing the same deletion must not duplicate the row."""
    sm = db.repos.sent_messages
    od = db.repos.observed_deletions

    await sm.record_many("+7001", -100200, [22], chat_type="channel", source="dialogs_send")
    await od.record_many("+7002", -100200, [22])
    await od.record_many("+7003", -100200, [22])

    rows = await sm.list(phone="+7001")
    assert len(rows) == 1
    assert rows[0]["deleted_at"] is not None


@pytest.mark.anyio
async def test_null_chat_deletion_by_other_account_does_not_mark_deleted(db):
    """NULL-chat rows (dm/small-group) carry per-account message ids: the same
    id deleted in another account's DM world is a different message — there,
    unlike channels, the phone must match."""
    sm = db.repos.sent_messages
    od = db.repos.observed_deletions

    await sm.record_many("+7001", None, [23], chat_type="dm", source="dialogs_send")
    await od.record_many("+7002", None, [23])

    rows = await sm.list(phone="+7001")
    assert len(rows) == 1
    assert rows[0]["deleted_at"] is None


@pytest.mark.anyio
async def test_list_filters_by_phone_source_chat_and_period(db):
    sm = db.repos.sent_messages
    old = datetime.now(timezone.utc) - timedelta(days=3)

    await sm.record_many(
        "+7001", -100123, [1], chat_type="channel", source="dialogs_send", sent_at=old
    )
    await sm.record_many("+7002", None, [2], chat_type="dm", source="dm_draft")
    await sm.record_many("+7001", None, [3], chat_type="dm", source="agent", ref="monitor:5")

    assert [row["message_id"] for row in await sm.list(phone="+7002")] == [2]
    assert [row["message_id"] for row in await sm.list(source="agent")] == [3]
    assert [row["message_id"] for row in await sm.list(chat_id=-100123)] == [1]

    since = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
    recent = await sm.list(date_from=since)
    assert [row["message_id"] for row in recent] == [3, 2]

    until = (datetime.now(timezone.utc) - timedelta(days=1)).isoformat()
    assert [row["message_id"] for row in await sm.list(date_to=until)] == [1]

    assert len(await sm.list(limit=2)) == 2
    # Newest first.
    assert [row["message_id"] for row in await sm.list()] == [3, 2, 1]
