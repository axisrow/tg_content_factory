"""Terminal handling for dead channels (#1783).

Terminal username failures — gateway receipt codes UsernameInvalidError /
UsernameChannelIdMismatch / UsernameResolveMiss, plus the raw telethon pair
on the direct path — stamp the channel row (terminal_code/terminal_at), fail
the task WITHOUT the retry pass, and the bulk enqueue skips the channel for a
configurable defer window (default 30 days) instead of burning one resolve
per channel per day. UsernameChannelIdMismatch additionally lands in the
operator review quarantine (needs_review): the stored username points at
someone else's channel. Verified meta writes and a successful collection
clear the stamp; ValueError stays non-terminal.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from typing import Any, cast

import pytest
from telethon.errors import UsernameNotOccupiedError

from tg_agent.collection_queue import CollectionFailureOutcome, CollectionQueue
from tg_agent.database import Database
from tg_agent.gateway.pool import GatewayOperationError
from tg_agent.models import Channel
from tg_agent.services.collection_service import CollectionService
from tg_agent.services.task_enqueuer import TaskEnqueuer

_TERMINAL_AT = datetime(2026, 11, 6, 12, 0, tzinfo=timezone.utc)


async def _stamp(db: Database, pk: int | None, code: str = "UsernameInvalidError", at=None) -> None:
    assert pk is not None
    await db.repos.channels.set_channel_terminal(pk, code, at or _TERMINAL_AT)


async def _row(db: Database, pk: int | None) -> Channel:
    assert pk is not None
    channel = await db.repos.channels.get_channel_by_pk(pk)
    assert channel is not None
    return channel


# ---------------------------------------------------------------------------
# Repository: stamp / clear / list
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_set_channel_terminal_stamps_row(db):
    await db.add_channel(Channel(channel_id=-4101, title="t", username="deadname"))
    pk = (await db.repos.channels.get_channels())[0].id
    assert pk is not None

    await _stamp(db, pk, "UsernameResolveMiss")

    row = await _row(db, pk)
    assert row.terminal_code == "UsernameResolveMiss"
    assert row.terminal_at == _TERMINAL_AT


@pytest.mark.anyio
async def test_clear_channel_terminal_unstamps(db):
    await db.add_channel(Channel(channel_id=-4102, title="t"))
    pk = (await db.repos.channels.get_channels())[0].id
    assert pk is not None
    await _stamp(db, pk)

    await db.repos.channels.clear_channel_terminal(pk)

    row = await _row(db, pk)
    assert row.terminal_code is None
    assert row.terminal_at is None


@pytest.mark.anyio
async def test_list_terminal_channels_newest_first(db):
    await db.add_channel(Channel(channel_id=-4103, title="old", username="a"))
    await db.add_channel(Channel(channel_id=-4104, title="new", username="b"))
    rows = {ch.title: ch.id for ch in await db.repos.channels.get_channels()}

    await _stamp(db, rows["old"], at=_TERMINAL_AT - timedelta(days=1))
    await _stamp(db, rows["new"], at=_TERMINAL_AT)

    listed = await db.repos.channels.list_terminal_channels()
    assert [ch.title for ch in listed] == ["new", "old"]
    assert listed[0].terminal_code == "UsernameInvalidError"


@pytest.mark.anyio
async def test_update_channel_meta_clears_terminal(db):
    """A verified meta write (username fix) resumes collection immediately."""
    await db.add_channel(Channel(channel_id=-4105, title="t", username="dead"))
    ch = (await db.repos.channels.get_channels())[0]
    await _stamp(db, ch.id)

    await db.repos.channels.update_channel_meta(
        ch.channel_id, username="fresh", title="renamed"
    )

    row = await _row(db, ch.id)
    assert row.terminal_code is None
    assert row.username == "fresh"


@pytest.mark.anyio
async def test_update_channel_card_clears_terminal(db):
    await db.add_channel(Channel(channel_id=-4106, title="t", username="dead"))
    ch = (await db.repos.channels.get_channels())[0]
    await _stamp(db, ch.id)

    await db.repos.channels.update_channel_card(
        ch.channel_id, username="fresh", title="verified",
        channel_type="channel", meta_present=True,
        about="a", linked_chat_id=None, has_comments=False,
    )

    assert (await _row(db, ch.id)).terminal_code is None


# ---------------------------------------------------------------------------
# CollectionQueue: stamping on terminal failures
# ---------------------------------------------------------------------------


async def _seed(tmp_path, *, username="deadname"):
    db = Database(str(tmp_path / "terminal.db"))
    await db.initialize()
    await db.add_channel(
        Channel(channel_id=-4110, title="t", username=username, is_active=True)
    )
    channel = (await db.repos.channels.get_channels())[0]
    assert channel.id is not None
    task_id = await db.repos.tasks.create_collection_task_if_not_active(
        -4110, "t", channel_username=username
    )
    assert task_id is not None
    await db.repos.tasks.update_collection_task(task_id, "running")
    queue = CollectionQueue(cast(Any, object()), db)
    return db, queue, channel, task_id


@pytest.mark.anyio
async def test_gateway_terminal_code_stamps_and_fails_without_retry(tmp_path):
    db, queue, channel, task_id = await _seed(tmp_path)
    try:
        exc = GatewayOperationError("failed", {"error": "UsernameResolveMiss"})
        outcome = await queue._handle_collection_exception(
            exc, task_id=task_id, channel=channel, force=False, full=False
        )
        assert outcome == CollectionFailureOutcome()
        assert queue._retry_pass == {}
        row = await _row(db, channel.id)
        assert row.terminal_code == "UsernameResolveMiss"
        assert row.terminal_at is not None
        task = await db.get_collection_task(task_id)
        assert task is not None and task.status == "failed"
    finally:
        await queue.shutdown()
        await db.close()


@pytest.mark.anyio
async def test_mismatch_code_also_quarantines_for_review(tmp_path):
    db, queue, channel, task_id = await _seed(tmp_path)
    try:
        exc = GatewayOperationError("failed", {"error": "UsernameChannelIdMismatch"})
        await queue._handle_collection_exception(
            exc, task_id=task_id, channel=channel, force=False, full=False
        )
        row = await _row(db, channel.id)
        assert row.terminal_code == "UsernameChannelIdMismatch"
        assert row.needs_review is True
        assert "UsernameChannelIdMismatch" in (row.review_reason or "")
    finally:
        await queue.shutdown()
        await db.close()


@pytest.mark.anyio
async def test_gateway_valueerror_is_not_terminal(tmp_path):
    """#1782's transient numeric-miss must keep today's behavior: no stamp,
    retry pass still re-enqueues."""
    db, queue, channel, task_id = await _seed(tmp_path)
    try:
        exc = GatewayOperationError("failed", {"error": "ValueError"})
        await queue._handle_collection_exception(
            exc, task_id=task_id, channel=channel, force=False, full=False
        )
        row = await _row(db, channel.id)
        assert row.terminal_code is None
        task = await db.get_collection_task(task_id)
        assert task is not None and task.status == "pending"
    finally:
        await queue.shutdown()
        await db.close()


@pytest.mark.anyio
async def test_direct_path_telethon_permanent_error_stamps(tmp_path):
    from telethon.errors import UsernameInvalidError

    db, queue, channel, task_id = await _seed(tmp_path)
    try:
        await queue._handle_collection_exception(
            UsernameInvalidError(request=None),
            task_id=task_id, channel=channel, force=False, full=False,
        )
        assert (await _row(db, channel.id)).terminal_code == "UsernameInvalidError"
    finally:
        await queue.shutdown()
        await db.close()


@pytest.mark.anyio
async def test_telethon_username_not_occupied_stamps(tmp_path):
    db, queue, channel, task_id = await _seed(tmp_path)
    try:
        await queue._handle_collection_exception(
            UsernameNotOccupiedError(request=None),
            task_id=task_id, channel=channel, force=False, full=False,
        )
        assert (await _row(db, channel.id)).terminal_code == "UsernameNotOccupiedError"
    finally:
        await queue.shutdown()
        await db.close()


@pytest.mark.anyio
async def test_successful_collection_clears_stamp(tmp_path):
    db, queue, channel, task_id = await _seed(tmp_path)
    try:
        await _stamp(db, channel.id)
        stamped = await _row(db, channel.id)
        await queue._handle_collection_completion(
            task_id, stamped, 3,
            cancel_event=asyncio.Event(), force=False,
        )
        assert (await _row(db, channel.id)).terminal_code is None
    finally:
        await queue.shutdown()
        await db.close()


# ---------------------------------------------------------------------------
# Bulk enqueue gate
# ---------------------------------------------------------------------------


async def _gated_channel(db: Database, channel_id: int, *, username: str) -> int:
    await db.add_channel(Channel(channel_id=channel_id, title="t", username=username))
    pk = (await db.repos.channels.get_channels())[0].id
    assert pk is not None
    return pk


@pytest.mark.anyio
async def test_enqueue_all_skips_fresh_terminal(db):
    pk = await _gated_channel(db, -4120, username="dead")
    await _stamp(db, pk)

    result = await CollectionService(db, None, collection_queue=None).enqueue_all_channels()

    assert result.queued_count == 0
    assert result.skipped_terminal_count == 1
    tasks = await db.repos.tasks.get_collection_tasks()
    assert not [t for t in tasks if t.task_type == "channel_collect"]


@pytest.mark.anyio
async def test_enqueue_all_admits_expired_terminal(db):
    pk = await _gated_channel(db, -4121, username="stale")
    await _stamp(db, pk, at=datetime.now(timezone.utc) - timedelta(days=31))

    result = await CollectionService(db, None, collection_queue=None).enqueue_all_channels()

    assert result.queued_count == 1
    assert result.skipped_terminal_count == 0


@pytest.mark.anyio
async def test_enqueue_all_honours_explicit_window(db):
    """The service gate takes the window as a parameter; the settings wiring
    (setting -> parameter) is covered by the TaskEnqueuer test below."""
    pk = await _gated_channel(db, -4122, username="override")
    await _stamp(db, pk, at=datetime.now(timezone.utc) - timedelta(days=31))

    result = await CollectionService(db, None, collection_queue=None).enqueue_all_channels(
        terminal_defer_days=60
    )

    assert result.skipped_terminal_count == 1


@pytest.mark.anyio
async def test_enqueue_by_pk_is_not_gated(db):
    """Operator-run `channel collect` stays an explicit override of the defer."""
    pk = await _gated_channel(db, -4123, username="dead")
    await _stamp(db, pk)

    result = await CollectionService(db, None, collection_queue=None).enqueue_channel_by_pk(
        pk, force=True
    )

    assert result == "queued"


@pytest.mark.anyio
async def test_task_enqueuer_wires_settings_window(db):
    pk = await _gated_channel(db, -4124, username="wired")
    await _stamp(db, pk, at=datetime.now(timezone.utc) - timedelta(days=31))
    await db.repos.settings.set_setting("collection_terminal_defer_days", "60")
    service = CollectionService(db, None, collection_queue=None)

    result = await TaskEnqueuer(db, service).enqueue_all_channels()

    assert result.skipped_terminal_count == 1
    assert result.queued_count == 0


# ---------------------------------------------------------------------------
# CLI review list
# ---------------------------------------------------------------------------


class TestReviewListTerminal:
    def test_terminal_lists_code_and_timestamp(self, cli_env, capsys):
        from tests.helpers import cli_add_channel
        from tests.test_cli_channel_commands import _ns, run

        pk = cli_add_channel(cli_env, channel_id=700, title="DeadCh")
        asyncio.run(_stamp(cli_env, pk, "UsernameResolveMiss"))

        run(_ns(channel_action="review-list", terminal=True))
        out = capsys.readouterr().out
        assert "DeadCh" in out
        assert "UsernameResolveMiss" in out
        assert "2026-11-06T12:00" in out

    def test_mismatch_marked_loudest(self, cli_env, capsys):
        from tests.helpers import cli_add_channel
        from tests.test_cli_channel_commands import _ns, run

        pk = cli_add_channel(cli_env, channel_id=701, title="HijackedCh")
        asyncio.run(_stamp(cli_env, pk, "UsernameChannelIdMismatch"))

        run(_ns(channel_action="review-list", terminal=True))
        out = capsys.readouterr().out
        assert "HijackedCh" in out
        assert "!!" in out

    def test_default_keeps_quarantine_only(self, cli_env, capsys):
        """A terminal stamp alone does not leak into the quarantine list."""
        from tests.helpers import cli_add_channel
        from tests.test_cli_channel_commands import _ns, run

        pk = cli_add_channel(cli_env, channel_id=702, title="StampedCh")
        asyncio.run(_stamp(cli_env, pk))

        run(_ns(channel_action="review-list"))
        out = capsys.readouterr().out
        assert "StampedCh" not in out
