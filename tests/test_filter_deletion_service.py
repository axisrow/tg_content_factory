"""Tests for FilterDeletionService."""
from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock

import aiosqlite
import pytest

from src.database import Database
from src.database.bundles import ChannelBundle
from src.models import Channel, Message
from src.services.channel_service import ChannelService
from src.services.filter_deletion_service import FilterDeletionService


@pytest.fixture
async def db(tmp_path):
    """Create in-memory database."""
    db = Database(":memory:")
    await db.initialize()
    yield db
    await db.close()


@pytest.fixture
def channel_service(db):
    """Create channel service mock."""
    service = MagicMock(spec=ChannelService)
    service._db = db
    return service


async def _add_filtered_channel(db: Database, channel_id: int, title: str | None) -> int:
    """Add a channel and mark it as filtered."""
    await db.add_channel(Channel(channel_id=channel_id, title=title))
    # Get the channel to find its pk
    channel = await db.get_channel_by_channel_id(channel_id)
    pk = channel.id
    # Mark as filtered using the repository method
    await db.repos.channels.set_channel_filtered(pk, filtered=True)
    return pk


@pytest.mark.anyio
async def test_purge_channels_by_pks_empty_list(db):
    """Test purge with empty list returns empty result."""
    service = FilterDeletionService(db)
    result = await service.purge_channels_by_pks([])
    assert result.purged_count == 0
    assert result.skipped_count == 0
    assert result.purged_titles == []


@pytest.mark.anyio
async def test_purge_channels_by_pks_channel_not_found(db):
    """Test purge skips non-existent channels."""
    service = FilterDeletionService(db)
    result = await service.purge_channels_by_pks([999])
    assert result.purged_count == 0
    assert result.skipped_count == 1


@pytest.mark.anyio
async def test_purge_channels_by_pks_not_filtered(db):
    """Test purge skips channels that are not filtered."""
    await db.add_channel(Channel(channel_id=100, title="Test"))

    service = FilterDeletionService(db)
    result = await service.purge_channels_by_pks([1])
    assert result.purged_count == 0
    assert result.skipped_count == 1
    # A benign skip is NOT an error — errors stays empty so callers don't false-alarm (#676).
    assert result.errors == []


@pytest.mark.anyio
async def test_purge_channels_by_pks_success(db):
    """Test successful purge of filtered channel."""
    pk = await _add_filtered_channel(db, channel_id=100, title="Filtered Channel")
    # Add some messages
    await db.insert_message(
        Message(
            channel_id=100,
            message_id=1,
            text="test message",
            date=datetime(2024, 1, 1, tzinfo=timezone.utc),
        )
    )

    service = FilterDeletionService(db)
    result = await service.purge_channels_by_pks([pk])
    assert result.purged_count == 1
    assert result.skipped_count == 0
    assert "Filtered Channel" in result.purged_titles
    assert result.total_messages_deleted == 1


@pytest.mark.anyio
async def test_purge_channels_by_pks_no_title(db):
    """Test purge with channel that has no title."""
    pk = await _add_filtered_channel(db, channel_id=100, title=None)

    service = FilterDeletionService(db)
    result = await service.purge_channels_by_pks([pk])
    assert result.purged_count == 1
    assert f"pk={pk}" in result.purged_titles[0]


@pytest.mark.anyio
async def test_purge_channels_by_pks_exception_handling(db):
    """Test purge handles exceptions gracefully."""
    # Create a mock db that raises exception
    mock_db = MagicMock(spec=Database)
    mock_db.get_channel_by_pk = AsyncMock(side_effect=Exception("DB error"))

    service = FilterDeletionService(mock_db)
    result = await service.purge_channels_by_pks([1])
    assert result.purged_count == 0
    assert result.skipped_count == 1
    # A real exception IS recorded so callers can distinguish it from a benign skip (#676).
    assert len(result.errors) == 1
    assert "DB error" in result.errors[0]


@pytest.mark.anyio
async def test_purge_all_filtered_no_channels(db):
    """Test purge all when no filtered channels exist."""
    await db.add_channel(Channel(channel_id=100, title="Active"))

    service = FilterDeletionService(db)
    result = await service.purge_all_filtered()
    assert result.purged_count == 0


@pytest.mark.anyio
async def test_purge_all_filtered_with_channels(db):
    """Test purge all with filtered channels."""
    _pk1 = await _add_filtered_channel(db, channel_id=100, title="Filtered 1")
    _pk2 = await _add_filtered_channel(db, channel_id=200, title="Filtered 2")
    await db.add_channel(Channel(channel_id=300, title="Not Filtered"))

    await db.insert_message(
        Message(
            channel_id=100,
            message_id=1,
            text="msg1",
            date=datetime(2024, 1, 1, tzinfo=timezone.utc),
        )
    )
    await db.insert_message(
        Message(
            channel_id=200,
            message_id=2,
            text="msg2",
            date=datetime(2024, 1, 1, tzinfo=timezone.utc),
        )
    )

    service = FilterDeletionService(db)
    result = await service.purge_all_filtered()
    assert result.purged_count == 2
    assert result.total_messages_deleted == 2


@pytest.mark.anyio
async def test_hard_delete_requires_channel_service(db):
    """Test hard delete raises error without channel service."""
    service = FilterDeletionService(db, channel_service=None)

    with pytest.raises(RuntimeError, match="hard_delete requires channel_service"):
        await service.hard_delete_channels_by_pks([1])


@pytest.mark.anyio
async def test_hard_delete_channel_not_found(db, channel_service):
    """Test hard delete skips non-existent channels."""
    channel_service.get_by_pk = AsyncMock(return_value=None)

    service = FilterDeletionService(db, channel_service=channel_service)
    result = await service.hard_delete_channels_by_pks([999])
    assert result.purged_count == 0
    assert result.skipped_count == 1


@pytest.mark.anyio
async def test_hard_delete_not_filtered(db, channel_service):
    """Test hard delete skips channels that are not filtered."""
    channel = Channel(channel_id=100, title="Test", is_filtered=False)
    channel_service.get_by_pk = AsyncMock(return_value=channel)

    service = FilterDeletionService(db, channel_service=channel_service)
    result = await service.hard_delete_channels_by_pks([1])
    assert result.purged_count == 0
    assert result.skipped_count == 1
    assert result.errors == []


@pytest.mark.anyio
async def test_hard_delete_success(db, channel_service):
    """Test successful hard delete of filtered channel."""
    channel = Channel(channel_id=100, title="To Delete", is_filtered=True)
    channel_service.get_by_pk = AsyncMock(return_value=channel)
    channel_service.delete = AsyncMock()

    service = FilterDeletionService(db, channel_service=channel_service)
    result = await service.hard_delete_channels_by_pks([1])
    assert result.purged_count == 1
    assert "To Delete" in result.purged_titles
    channel_service.delete.assert_called_once_with(1)


@pytest.mark.anyio
async def test_hard_delete_no_title(db, channel_service):
    """Test hard delete with channel that has no title."""
    channel = Channel(channel_id=100, title=None, is_filtered=True)
    channel_service.get_by_pk = AsyncMock(return_value=channel)
    channel_service.delete = AsyncMock()

    service = FilterDeletionService(db, channel_service=channel_service)
    result = await service.hard_delete_channels_by_pks([1])
    assert result.purged_count == 1
    assert "pk=1" in result.purged_titles[0]


@pytest.mark.anyio
async def test_hard_delete_exception_handling(db, channel_service):
    """Test hard delete handles exceptions gracefully."""
    channel_service.get_by_pk = AsyncMock(side_effect=Exception("DB error"))

    service = FilterDeletionService(db, channel_service=channel_service)
    result = await service.hard_delete_channels_by_pks([1])
    assert result.purged_count == 0
    assert result.skipped_count == 1
    assert len(result.errors) == 1
    assert "DB error" in result.errors[0]


# ─────────────────────────────────────────────────────────────────────────────
# Cascade / orphan / atomicity / race coverage (issue #1039)
#
# The unit tests above mock ChannelService, so they never exercise the real
# DELETE cascade. These tests wire a *real* ChannelService over the same
# in-memory DB the CLI builds (filter.py `_build_deletion_service`), then assert
# that no sidecar table is left pointing at a deleted channel or message.
# JOIN convention: every sidecar table keys on the Telegram channel_id, never the
# `channels.id` pk (see CLAUDE.md "JOIN on channels").
# ─────────────────────────────────────────────────────────────────────────────


def _real_deletion_service(db: Database) -> FilterDeletionService:
    """Mirror the CLI's `_build_deletion_service`: a real ChannelService over db
    with no client pool / queue. hard_delete then runs the genuine cascade."""
    channel_bundle = ChannelBundle.from_database(db)
    channel_service = ChannelService(channel_bundle, None, queue=None)  # type: ignore[arg-type]
    return FilterDeletionService(db, channel_service)


async def _seed_channel_with_sidecars(
    db: Database, *, channel_id: int, message_id: int = 1
) -> int:
    """Add a filtered channel plus one message and a row in every sidecar table
    that references it, so a delete that leaks orphans is observable. Returns pk."""
    pk = await _add_filtered_channel(db, channel_id=channel_id, title="Sidecar Channel")
    await db.insert_message(
        Message(
            channel_id=channel_id,
            message_id=message_id,
            text="msg",
            date=datetime(2024, 1, 1, tzinfo=timezone.utc),
        )
    )
    # Embeddings key on the message's rowid (messages.id), no FK. Seed BOTH
    # stores — the JSON one (#173) and the older BLOB index — so a delete that
    # cleans only one of them is observable.
    msg_row = await db.execute(
        "SELECT id FROM messages WHERE channel_id = ? AND message_id = ?",
        (channel_id, message_id),
    )
    msg_pk = (await msg_row.fetchone())["id"]
    await db.execute_write(
        "INSERT INTO message_embeddings_json (message_id, embedding, dims) "
        "VALUES (?, ?, ?)",
        (msg_pk, "[0.1,0.2]", 2),
    )
    await db.execute_write(
        "INSERT INTO message_embeddings (message_id, embedding) VALUES (?, ?)",
        (msg_pk, b"\x00\x01\x02\x03"),
    )
    # message-keyed sidecars
    await db.execute_write(
        "INSERT INTO message_reactions (channel_id, message_id, emoji, count) "
        "VALUES (?, ?, ?, ?)",
        (channel_id, message_id, "👍", 3),
    )
    await db.execute_write(
        "INSERT INTO notified_messages (query_id, channel_id, message_id) VALUES (?, ?, ?)",
        (1, channel_id, message_id),
    )
    await db.execute_write(
        "INSERT INTO pipeline_action_log "
        "(pipeline_id, node_id, action, channel_id, message_id) VALUES (?, ?, ?, ?, ?)",
        (1, "node-a", "publish", channel_id, message_id),
    )
    # channel-keyed sidecars
    await db.execute_write(
        "INSERT INTO channel_stats (channel_id, subscriber_count) VALUES (?, ?)",
        (channel_id, 100),
    )
    await db.execute_write(
        "INSERT INTO channel_ratings (channel_id, title, username, useful, genre) "
        "VALUES (?, ?, ?, ?, ?)",
        (channel_id, "Sidecar Channel", None, "yes", "news"),
    )
    await db.execute_write(
        "INSERT INTO channel_rename_events (channel_id, old_title, new_title) "
        "VALUES (?, ?, ?)",
        (channel_id, "old", "new"),
    )
    await db.execute_write(
        "INSERT INTO forum_topics (channel_id, topic_id, title) VALUES (?, ?, ?)",
        (channel_id, 7, "General"),
    )
    return pk


async def _count(db: Database, table: str, channel_id: int) -> int:
    cur = await db.execute(
        f"SELECT COUNT(*) AS c FROM {table} WHERE channel_id = ?", (channel_id,)
    )
    row = await cur.fetchone()
    return row["c"]


async def _count_embeddings_total(db: Database) -> int:
    """Total rows across BOTH embedding stores — the JSON one (#173) and the older
    BLOB index. Both key on messages.id (not channel_id) with no FK. Each delete
    path removes the channel's only seeded message, so a non-zero total here means
    an embedding was left orphaned in at least one store (the bug #1039 / the PR
    #1078 cycle-1+2 reviews found). Counting both catches a fix that cleans only
    one table."""
    cur = await db.execute(
        "SELECT (SELECT COUNT(*) FROM message_embeddings_json) "
        "+ (SELECT COUNT(*) FROM message_embeddings) AS c"
    )
    row = await cur.fetchone()
    return row["c"]


# ── Cascade & orphans: purge (soft-delete, channel stays) ────────────────────


@pytest.mark.anyio
async def test_purge_cascades_to_message_reactions(db):
    """purge deletes messages; message_reactions FK (ON DELETE CASCADE) must
    follow so no reaction is left pointing at a now-deleted message."""
    cid = 100
    await _seed_channel_with_sidecars(db, channel_id=cid)
    pk = (await db.get_channel_by_channel_id(cid)).id

    result = await _real_deletion_service(db).purge_channels_by_pks([pk])

    assert result.purged_count == 1
    assert await _count(db, "messages", cid) == 0
    assert await _count(db, "message_reactions", cid) == 0


@pytest.mark.anyio
async def test_purge_removes_message_embeddings(db):
    """purge deletes BOTH embedding stores (JSON + BLOB) keyed on the deleted
    messages' ids, so no embedding is left orphaned and can't be mis-attached to
    a reused rowid (#1039 cycle-2: the BLOB store was missed at first)."""
    cid = 100
    await _seed_channel_with_sidecars(db, channel_id=cid)
    pk = (await db.get_channel_by_channel_id(cid)).id

    await _real_deletion_service(db).purge_channels_by_pks([pk])

    assert await _count_embeddings_total(db) == 0


@pytest.mark.anyio
async def test_purge_preserves_notified_messages_dedup_ledger(db):
    """Regression (#1039, Codex review of PR #1078): purge is a SOFT delete — the
    channel stays tracked and the same (channel_id, message_id) can be collected
    again. `notified_messages` is a sent-notification dedup ledger, NOT a
    message-owned sidecar: wiping it on purge would re-send notifications for
    messages already delivered once the channel is recollected. It must survive."""
    cid = 100
    await _seed_channel_with_sidecars(db, channel_id=cid)
    pk = (await db.get_channel_by_channel_id(cid)).id

    await _real_deletion_service(db).purge_channels_by_pks([pk])

    assert await _count(db, "notified_messages", cid) == 1


@pytest.mark.anyio
async def test_purge_preserves_pipeline_action_log_dedup_ledger(db):
    """Regression (#1039, Codex review of PR #1078): `pipeline_action_log` is the
    'already reacted/forwarded/deleted' ledger (#471). purge must NOT clear it,
    or a pipeline re-run after recollection would re-perform external Telegram
    actions on the same messages."""
    cid = 100
    await _seed_channel_with_sidecars(db, channel_id=cid)
    pk = (await db.get_channel_by_channel_id(cid)).id

    await _real_deletion_service(db).purge_channels_by_pks([pk])

    assert await _count(db, "pipeline_action_log", cid) == 1


@pytest.mark.anyio
async def test_purge_then_recollect_does_not_replay_notifications(db):
    """End-to-end idempotency guard (#1039): seed a notified message, purge it,
    'recollect' the same (channel_id, message_id), and confirm the notification
    ledger still suppresses it as already-sent. This is the invariant the naive
    'delete the orphan' fix would have broken."""
    cid = 100
    pk = await _add_filtered_channel(db, channel_id=cid, title="Recollect")
    msg = Message(
        channel_id=cid,
        message_id=42,
        text="m",
        date=datetime(2024, 1, 1, tzinfo=timezone.utc),
    )
    await db.insert_message(msg)
    await db.execute_write(
        "INSERT INTO notified_messages (query_id, channel_id, message_id) VALUES (?, ?, ?)",
        (7, cid, 42),
    )

    await _real_deletion_service(db).purge_channels_by_pks([pk])

    # Recollect the same message id and check the ledger still marks it notified.
    await db.insert_message(msg)
    unnotified = await db.repos.notified_messages.filter_unnotified(7, cid, [42])
    assert unnotified == set()  # already-notified → suppressed, no replay


@pytest.mark.anyio
async def test_purge_keeps_channel_and_channel_keyed_stats(db):
    """purge is a *soft* delete: the channel row and its channel-level stats stay.
    Only message-derived data is removed."""
    cid = 100
    await _seed_channel_with_sidecars(db, channel_id=cid)
    pk = (await db.get_channel_by_channel_id(cid)).id

    await _real_deletion_service(db).purge_channels_by_pks([pk])

    assert await db.get_channel_by_channel_id(cid) is not None
    assert await _count(db, "channel_stats", cid) == 1


# ── Cascade & orphans: hard_delete (channel removed entirely) ────────────────


@pytest.mark.anyio
async def test_hard_delete_removes_channel_and_message_data(db):
    """hard_delete removes the channel plus its messages, reactions, stats and
    forum topics — the data delete_channel already handled."""
    cid = 200
    pk = await _seed_channel_with_sidecars(db, channel_id=cid)

    result = await _real_deletion_service(db).hard_delete_channels_by_pks([pk])

    assert result.purged_count == 1
    assert await db.get_channel_by_channel_id(cid) is None
    assert await _count(db, "messages", cid) == 0
    assert await _count(db, "message_reactions", cid) == 0
    assert await _count(db, "channel_stats", cid) == 0
    assert await _count(db, "forum_topics", cid) == 0


@pytest.mark.anyio
async def test_hard_delete_leaves_no_orphan_embeddings(db):
    """Regression (#1039, PR #1078 reviews): delete_channel deleted the messages
    but left embeddings orphaned (both stores key on messages.id, no FK). SQLite
    can reissue a deleted rowid to a future message, and INSERT OR REPLACE keys
    only on message_id — a new message could inherit a stale embedding. Cycle-1
    caught the JSON store; cycle-2 caught the BLOB twin. hard-delete must clear
    both, like purge does."""
    cid = 200
    pk = await _seed_channel_with_sidecars(db, channel_id=cid)

    await _real_deletion_service(db).hard_delete_channels_by_pks([pk])

    assert await _count_embeddings_total(db) == 0


@pytest.mark.anyio
async def test_hard_delete_leaves_no_orphan_channel_ratings(db):
    """Regression (#1039): channel_ratings (PK=channel_id, no FK) survived
    hard_delete as an orphan rating for a channel that no longer exists."""
    cid = 200
    pk = await _seed_channel_with_sidecars(db, channel_id=cid)

    await _real_deletion_service(db).hard_delete_channels_by_pks([pk])

    assert await _count(db, "channel_ratings", cid) == 0


@pytest.mark.anyio
async def test_hard_delete_leaves_no_orphan_rename_events(db):
    """Regression (#1039): channel_rename_events orphaned after hard_delete."""
    cid = 200
    pk = await _seed_channel_with_sidecars(db, channel_id=cid)

    await _real_deletion_service(db).hard_delete_channels_by_pks([pk])

    assert await _count(db, "channel_rename_events", cid) == 0


@pytest.mark.anyio
async def test_hard_delete_leaves_no_orphan_notified_or_action_log(db):
    """Regression (#1039): message-keyed sidecars must also be gone after the
    channel is hard-deleted."""
    cid = 200
    pk = await _seed_channel_with_sidecars(db, channel_id=cid)

    await _real_deletion_service(db).hard_delete_channels_by_pks([pk])

    assert await _count(db, "notified_messages", cid) == 0
    assert await _count(db, "pipeline_action_log", cid) == 0


# ── Atomicity: hard_delete must roll back fully on FK RESTRICT ────────────────


async def _attach_pipeline_source(db: Database, channel_id: int) -> None:
    """Create a pipeline + pipeline_source row → a FK RESTRICT on channels."""
    await db.execute_write(
        "INSERT INTO content_pipelines (id, name, prompt_template) VALUES (?, ?, ?)",
        (1, "Pipeline", "tpl"),
    )
    await db.execute_write(
        "INSERT INTO pipeline_sources (pipeline_id, channel_id) VALUES (?, ?)",
        (1, channel_id),
    )


@pytest.mark.anyio
async def test_hard_delete_rolls_back_on_fk_restrict(db):
    """Atomicity (#1039): pipeline_sources.channel_id is FK RESTRICT. delete_channel
    preflights it and raises *before* deleting any child rows, so a blocked
    hard_delete leaves messages, reactions and the channel fully intact — no
    half-deleted state."""
    cid = 300
    pk = await _seed_channel_with_sidecars(db, channel_id=cid)
    await _attach_pipeline_source(db, cid)

    result = await _real_deletion_service(db).hard_delete_channels_by_pks([pk])

    # The service swallows the per-channel error into result.errors, not a raise.
    assert result.purged_count == 0
    assert result.skipped_count == 1
    assert len(result.errors) == 1
    assert "FOREIGN KEY" in result.errors[0] or "pipeline_sources" in result.errors[0]
    # Nothing was half-deleted: every row survives the blocked delete.
    assert await db.get_channel_by_channel_id(cid) is not None
    assert await _count(db, "messages", cid) == 1
    assert await _count(db, "message_reactions", cid) == 1
    assert await _count(db, "channel_stats", cid) == 1


@pytest.mark.anyio
async def test_delete_channel_atomic_rollback_keeps_messages(db):
    """Lower-level guard on ChannelService.delete: a FK RESTRICT failure raises
    IntegrityError and the messages DELETE that ran first inside the same
    transaction is rolled back (BEGIN IMMEDIATE, issue #569)."""
    cid = 301
    await _seed_channel_with_sidecars(db, channel_id=cid)
    pk = (await db.get_channel_by_channel_id(cid)).id
    await _attach_pipeline_source(db, cid)

    service = ChannelService(ChannelBundle.from_database(db), None, queue=None)  # type: ignore[arg-type]
    with pytest.raises(aiosqlite.IntegrityError):
        await service.delete(pk)

    assert await _count(db, "messages", cid) == 1
    assert await db.get_channel_by_channel_id(cid) is not None


# ── Race: ChannelService.delete vs scheduler (task cancel ordering) ───────────


@pytest.mark.anyio
async def test_delete_cancels_tasks_only_after_successful_delete(db):
    """Race window (#1039, Codex round 11): active collection tasks are *collected*
    before delete but *cancelled* only after delete_channel succeeds. On FK
    RESTRICT the delete fails, the channel survives, and its tasks must NOT be
    cancelled — otherwise a live channel keeps its work silently disabled."""
    cid = 400
    await _seed_channel_with_sidecars(db, channel_id=cid)
    pk = (await db.get_channel_by_channel_id(cid)).id
    await _attach_pipeline_source(db, cid)
    # An active collection task the scheduler could still pick up.
    await db.execute_write(
        "INSERT INTO collection_tasks (channel_id, task_type, status) VALUES (?, ?, ?)",
        (cid, "channel_collect", "pending"),
    )

    queue = MagicMock()
    queue.cancel_task = AsyncMock()
    service = ChannelService(ChannelBundle.from_database(db), None, queue=queue)  # type: ignore[arg-type]

    with pytest.raises(aiosqlite.IntegrityError):
        await service.delete(pk)

    # Delete failed → the surviving channel's task must be left alone.
    queue.cancel_task.assert_not_awaited()
    cur = await db.execute(
        "SELECT status FROM collection_tasks WHERE channel_id = ?", (cid,)
    )
    row = await cur.fetchone()
    assert row["status"] == "pending"
