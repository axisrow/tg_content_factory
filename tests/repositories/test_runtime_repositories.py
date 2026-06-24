from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from unittest.mock import patch

import pytest

from src.database import Database
from src.models import RuntimeSnapshot, TelegramCommand, TelegramCommandStatus
from src.services.telegram_command_service import TelegramCommandService


@pytest.mark.anyio
async def test_telegram_commands_repository_round_trip(tmp_path):
    db = Database(str(tmp_path / "test.db"))
    await db.initialize()

    try:
        command = TelegramCommand(
            command_type="dialogs.refresh",
            payload={"phone": "+1234567890"},
            requested_by="web:test",
        )
        command_id = await db.repos.telegram_commands.create_command(command)

        stored = await db.repos.telegram_commands.get_command(command_id)

        assert stored is not None
        assert stored.command_type == "dialogs.refresh"
        assert stored.status == TelegramCommandStatus.PENDING
        assert stored.payload == {"phone": "+1234567890"}
        assert stored.requested_by == "web:test"
    finally:
        await db.close()


@pytest.mark.anyio
async def test_get_messages_collected_since(tmp_path):
    """Notification dry-run scans messages collected since a timestamp (audit #838/3)."""
    from src.models import Channel, Message

    db = Database(str(tmp_path / "test.db"))
    await db.initialize()
    try:
        await db.add_channel(Channel(channel_id=100, title="C"))
        await db.insert_message(
            Message(channel_id=100, message_id=1, text="продаю", date="2025-01-01T00:00:00")
        )

        recent = await db.repos.messages.get_messages_collected_since("2000-01-01 00:00:00")
        assert [m.message_id for m in recent] == [1]

        assert await db.repos.messages.get_messages_collected_since("2999-01-01 00:00:00") == []
    finally:
        await db.close()


@pytest.mark.anyio
async def test_iter_messages_collected_since_pages_entire_window(tmp_path):
    """iter_messages_collected_since must yield EVERY message in the window across pages,
    so the dry-run total is uncapped (the live notification path has no LIMIT) — a single
    capped fetch would undercount when the window exceeds one page (#838/3 review)."""
    from src.models import Channel, Message

    db = Database(str(tmp_path / "test.db"))
    await db.initialize()
    try:
        await db.add_channel(Channel(channel_id=100, title="C"))
        # 12 messages, distinct dates so the keyset cursor advances deterministically.
        for i in range(1, 13):
            await db.insert_message(
                Message(
                    channel_id=100,
                    message_id=i,
                    text=f"msg {i}",
                    date=f"2025-01-01T00:00:{i:02d}",
                )
            )

        seen: list[int] = []
        # page_size=5 forces 3 pages (5 + 5 + 2) — exercises the keyset cursor.
        async for page in db.repos.messages.iter_messages_collected_since(
            "2000-01-01 00:00:00", page_size=5
        ):
            seen.extend(m.message_id for m in page)

        # Every message returned exactly once (no cap, no duplicate across page boundaries).
        assert sorted(seen) == list(range(1, 13))
        assert len(seen) == 12

        # Empty window yields nothing.
        empty = [p async for p in db.repos.messages.iter_messages_collected_since("2999-01-01 00:00:00")]
        assert empty == []
    finally:
        await db.close()


@pytest.mark.anyio
async def test_notified_messages_filter_and_record(tmp_path):
    """notified_messages ledger: filter_unnotified + idempotent record (audit #838/1)."""
    db = Database(str(tmp_path / "test.db"))
    await db.initialize()
    try:
        repo = db.repos.notified_messages
        assert await repo.filter_unnotified(1, 100, [1, 2, 3]) == {1, 2, 3}

        await repo.record(1, 100, [1, 2])
        assert await repo.filter_unnotified(1, 100, [1, 2, 3]) == {3}

        # Different query id is an independent ledger.
        assert await repo.filter_unnotified(2, 100, [1, 2, 3]) == {1, 2, 3}

        # record is idempotent (INSERT OR IGNORE on the composite PK).
        await repo.record(1, 100, [1, 2])
        assert await repo.filter_unnotified(1, 100, [1, 2, 3]) == {3}

        assert await repo.filter_unnotified(1, 100, []) == set()

        # has_any: True only for channels with at least one recorded row (gates the
        # backlog rescan so an empty ledger never replays history — #850 review).
        assert await repo.has_any([100]) is True
        assert await repo.has_any([999]) is False
        assert await repo.has_any([999, 100]) is True
        assert await repo.has_any([]) is False
    finally:
        await db.close()


@pytest.mark.anyio
async def test_update_command_terminal_preserves_prior_result_payload(tmp_path):
    """A terminal update without a fresh result_payload must keep earlier
    diagnostics (e.g. flood-wait context) instead of wiping them (audit #835/15)."""
    db = Database(str(tmp_path / "test.db"))
    await db.initialize()
    try:
        cmd_id = await db.repos.telegram_commands.create_command(
            TelegramCommand(command_type="dialogs.refresh", payload={"phone": "+7"})
        )
        diagnostics = {"operation": "resolve", "phone": "+7", "next_available_at_utc": "2026-06-15T10:00:00+00:00"}
        await db.repos.telegram_commands.update_command(
            cmd_id,
            status=TelegramCommandStatus.PENDING,
            result_payload=diagnostics,
        )

        # Terminal FAILED with no fresh result_payload — prior must survive.
        await db.repos.telegram_commands.update_command(
            cmd_id,
            status=TelegramCommandStatus.FAILED,
            error="boom",
        )

        stored = await db.repos.telegram_commands.get_command(cmd_id)
        assert stored.status == TelegramCommandStatus.FAILED
        assert stored.error == "boom"
        assert stored.result_payload == diagnostics
    finally:
        await db.close()


@pytest.mark.anyio
async def test_claim_next_command_rolls_back_on_error(tmp_path):
    """If an exception happens mid-claim, the DB must not stay locked."""
    db = Database(str(tmp_path / "test.db"))
    await db.initialize()

    try:
        await db.repos.telegram_commands.create_command(
            TelegramCommand(
                command_type="dialogs.refresh",
                payload={"phone": "+1"},
                requested_by="test",
            )
        )

        repo = db.repos.telegram_commands
        # claim_next_command writes inside db.transaction(), i.e. on the write
        # connection (db._db) — not the repo's read proxy (#760). Patch the write conn.
        original_execute = db._db.execute
        call_count = {"n": 0}

        async def boom(sql, *args, **kwargs):
            call_count["n"] += 1
            # Fail after BEGIN IMMEDIATE but before UPDATE
            if call_count["n"] == 2:
                raise RuntimeError("simulated fetch failure")
            return await original_execute(sql, *args, **kwargs)

        with patch.object(db._db, "execute", side_effect=boom):
            with pytest.raises(RuntimeError, match="simulated"):
                await repo.claim_next_command()

        # After rollback, next claim must work — no "database is locked".
        claimed = await repo.claim_next_command()
        assert claimed is not None
        assert claimed.status == TelegramCommandStatus.RUNNING
    finally:
        await db.close()


@pytest.mark.anyio
async def test_claim_next_command_recovers_from_stale_transaction(tmp_path):
    db = Database(str(tmp_path / "test.db"))
    await db.initialize()

    try:
        await db.repos.telegram_commands.create_command(
            TelegramCommand(
                command_type="dialogs.refresh",
                payload={"phone": "+1"},
                requested_by="test",
            )
        )

        assert db.db is not None
        await db.db.execute("BEGIN")

        claimed = await db.repos.telegram_commands.claim_next_command()
        assert claimed is not None
        assert claimed.status == TelegramCommandStatus.RUNNING
    finally:
        await db.close()


@pytest.mark.anyio
async def test_claim_next_command_skips_future_run_after(tmp_path):
    db = Database(str(tmp_path / "test.db"))
    await db.initialize()

    try:
        future_id = await db.repos.telegram_commands.create_command(
            TelegramCommand(
                command_type="dialogs.react",
                payload={"phone": "+1", "message_id": 1},
                requested_by="test",
                run_after=datetime.now(timezone.utc) + timedelta(minutes=5),
            )
        )
        due_id = await db.repos.telegram_commands.create_command(
            TelegramCommand(
                command_type="dialogs.react",
                payload={"phone": "+1", "message_id": 2},
                requested_by="test",
            )
        )

        claimed = await db.repos.telegram_commands.claim_next_command()

        assert claimed is not None
        assert claimed.id == due_id
        future = await db.repos.telegram_commands.get_command(future_id)
        assert future is not None
        assert future.status == TelegramCommandStatus.PENDING
        assert future.run_after is not None
    finally:
        await db.close()


@pytest.mark.anyio
async def test_command_service_queues_distinct_reactions_and_deduplicates_exact_payload(tmp_path):
    db = Database(str(tmp_path / "test.db"))
    await db.initialize()

    try:
        service = TelegramCommandService(db)
        ids = []
        for message_id in range(10):
            ids.append(
                await service.enqueue(
                    "dialogs.react",
                    payload={
                        "phone": "+1",
                        "chat_id": "5832576119",
                        "message_id": message_id,
                        "emoji": "👍",
                    },
                    requested_by="test",
                )
            )
        duplicate_id = await service.enqueue(
            "dialogs.react",
            payload={
                "phone": "+1",
                "chat_id": "5832576119",
                "message_id": 3,
                "emoji": "👍",
            },
            requested_by="test",
        )

        assert len(set(ids)) == 10
        assert duplicate_id == ids[3]
    finally:
        await db.close()


@pytest.mark.anyio
async def test_command_service_lists_and_summarizes_with_filters(tmp_path):
    db = Database(str(tmp_path / "test.db"))
    await db.initialize()

    try:
        service = TelegramCommandService(db)
        react_id = await service.enqueue(
            "dialogs.react",
            payload={"phone": "+1", "chat_id": "5832576119", "message_id": 1, "emoji": "👍"},
            requested_by="test",
        )
        await service.enqueue(
            "dialogs.react",
            payload={"phone": "+2", "chat_id": "5832576119", "message_id": 2, "emoji": "🔥"},
            requested_by="test",
        )
        send_id = await service.enqueue(
            "dialogs.send_message",
            payload={"phone": "+1", "recipient": "@chat", "text": "hello"},
            requested_by="test",
        )
        await db.repos.telegram_commands.update_command(
            send_id,
            status=TelegramCommandStatus.SUCCEEDED,
            payload={"phone": "+1", "recipient": "@chat", "text": "hello"},
            result_payload={"phone": "+1"},
        )
        await db.repos.telegram_commands.update_command(
            react_id,
            status=TelegramCommandStatus.PENDING,
            payload={"phone": "+1", "chat_id": "5832576119", "message_id": 1, "emoji": "👍"},
            result_payload={"state": "waiting_flood_wait"},
            run_after=datetime.now(timezone.utc) + timedelta(minutes=1),
        )

        phone_commands = await service.list(phone="+1", limit=10)
        react_summary = await service.summary(command_type="dialogs.react")
        state_summary = await service.result_state_summary(command_type="dialogs.react")

        assert {item.command_type for item in phone_commands} == {"dialogs.react", "dialogs.send_message"}
        assert react_summary[TelegramCommandStatus.PENDING] == 2
        assert react_summary[TelegramCommandStatus.SUCCEEDED] == 0
        assert state_summary["waiting_flood_wait"] == 1
    finally:
        await db.close()


@pytest.mark.anyio
async def test_cancel_command_only_cancels_pending(tmp_path):
    """Issue #621: reaction tasks (PENDING `dialogs.react` rows) must be
    cancellable. RUNNING ones must be left alone — their Telegram API call
    is already in flight."""
    db = Database(str(tmp_path / "test.db"))
    await db.initialize()

    try:
        pending_id = await db.repos.telegram_commands.create_command(
            TelegramCommand(
                command_type="dialogs.react",
                payload={"phone": "+1", "message_id": 1, "emoji": "👍"},
                requested_by="test",
            )
        )
        running_id = await db.repos.telegram_commands.create_command(
            TelegramCommand(
                command_type="dialogs.react",
                payload={"phone": "+1", "message_id": 2, "emoji": "👍"},
                requested_by="test",
            )
        )
        # Promote the second row to RUNNING directly so we have one of each.
        await db.repos.telegram_commands.update_command(
            running_id, status=TelegramCommandStatus.RUNNING
        )

        ok = await db.repos.telegram_commands.cancel_command(pending_id)
        assert ok is True
        cancelled = await db.repos.telegram_commands.get_command(pending_id)
        assert cancelled is not None
        assert cancelled.status == TelegramCommandStatus.CANCELLED
        assert cancelled.finished_at is not None

        ok_running = await db.repos.telegram_commands.cancel_command(running_id)
        assert ok_running is False
        still_running = await db.repos.telegram_commands.get_command(running_id)
        assert still_running is not None
        assert still_running.status == TelegramCommandStatus.RUNNING
    finally:
        await db.close()


@pytest.mark.anyio
async def test_cancel_pending_commands_filters_by_type_and_phone(tmp_path):
    """Issue #621: bulk-cancel must respect command_type and phone filters."""
    db = Database(str(tmp_path / "test.db"))
    await db.initialize()

    try:
        for message_id in range(3):
            await db.repos.telegram_commands.create_command(
                TelegramCommand(
                    command_type="dialogs.react",
                    payload={"phone": "+1", "message_id": message_id, "emoji": "👍"},
                    requested_by="test",
                )
            )
        other_phone_id = await db.repos.telegram_commands.create_command(
            TelegramCommand(
                command_type="dialogs.react",
                payload={"phone": "+2", "message_id": 99, "emoji": "👍"},
                requested_by="test",
            )
        )
        other_type_id = await db.repos.telegram_commands.create_command(
            TelegramCommand(
                command_type="dialogs.send_message",
                payload={"phone": "+1", "recipient": "@chat", "text": "hi"},
                requested_by="test",
            )
        )

        cancelled = await db.repos.telegram_commands.cancel_pending_commands(
            command_type="dialogs.react", phone="+1"
        )
        assert cancelled == 3

        # +2 reaction and +1 send_message must be untouched.
        other_phone = await db.repos.telegram_commands.get_command(other_phone_id)
        other_type = await db.repos.telegram_commands.get_command(other_type_id)
        assert other_phone is not None and other_phone.status == TelegramCommandStatus.PENDING
        assert other_type is not None and other_type.status == TelegramCommandStatus.PENDING
    finally:
        await db.close()


@pytest.mark.anyio
async def test_cancel_pending_commands_unfiltered_skips_running(tmp_path):
    db = Database(str(tmp_path / "test.db"))
    await db.initialize()

    try:
        pending_id = await db.repos.telegram_commands.create_command(
            TelegramCommand(
                command_type="dialogs.react",
                payload={"phone": "+1", "message_id": 1, "emoji": "👍"},
                requested_by="test",
            )
        )
        running_id = await db.repos.telegram_commands.create_command(
            TelegramCommand(
                command_type="dialogs.react",
                payload={"phone": "+1", "message_id": 2, "emoji": "👍"},
                requested_by="test",
            )
        )
        await db.repos.telegram_commands.update_command(
            running_id, status=TelegramCommandStatus.RUNNING
        )

        cancelled = await db.repos.telegram_commands.cancel_pending_commands()
        assert cancelled == 1
        pending_check = await db.repos.telegram_commands.get_command(pending_id)
        assert pending_check is not None
        assert pending_check.status == TelegramCommandStatus.CANCELLED
        running_check = await db.repos.telegram_commands.get_command(running_id)
        assert running_check is not None
        assert running_check.status == TelegramCommandStatus.RUNNING
    finally:
        await db.close()


# ---------------------------------------------------------------------------
# Claim races, recovery, and run_after gating (#1030, epic #1024 tier-1).
#
# These guard the thin spots a single dispatcher worker shares with any peer:
# the claim transaction must hand a PENDING row to exactly one caller, a worker
# crash must not strand RUNNING rows, and run_after must gate by wall-clock.
# ---------------------------------------------------------------------------


@pytest.mark.anyio
async def test_concurrent_claim_hands_command_to_exactly_one_worker(tmp_path):
    """Two workers racing on one PENDING row: exactly one wins (#1030).

    ``claim_next_command`` runs SELECT+UPDATE inside ``db.transaction()`` under
    the connection-wide write lock (#569). Even with five concurrent claims,
    only one transitions the single row PENDING → RUNNING; the rest see it gone
    and return None. A regression that drops the lock or splits the
    select-then-update would let two workers run the same Telegram command.
    """
    db = Database(str(tmp_path / "test.db"))
    await db.initialize()

    try:
        command_id = await db.repos.telegram_commands.create_command(
            TelegramCommand(
                command_type="dialogs.react",
                payload={"phone": "+1", "message_id": 1, "emoji": "👍"},
                requested_by="test",
            )
        )

        results = await asyncio.gather(
            *[db.repos.telegram_commands.claim_next_command() for _ in range(5)]
        )

        claimed = [cmd for cmd in results if cmd is not None]
        assert len(claimed) == 1, f"expected exactly one winner, got {len(claimed)}"
        assert claimed[0].id == command_id
        assert claimed[0].status == TelegramCommandStatus.RUNNING

        stored = await db.repos.telegram_commands.get_command(command_id)
        assert stored is not None
        assert stored.status == TelegramCommandStatus.RUNNING
        # No second claim possible — the queue is now empty.
        assert await db.repos.telegram_commands.claim_next_command() is None
    finally:
        await db.close()


@pytest.mark.anyio
async def test_concurrent_claim_two_commands_two_workers_no_overlap(tmp_path):
    """Two PENDING rows, four racing claims: each row goes to a distinct worker.

    Guards against a claim that re-reads the same lowest-id row twice before the
    UPDATE lands — every claimed command id must be unique.
    """
    db = Database(str(tmp_path / "test.db"))
    await db.initialize()

    try:
        first = await db.repos.telegram_commands.create_command(
            TelegramCommand(command_type="dialogs.react", payload={"phone": "+1"}, requested_by="t")
        )
        second = await db.repos.telegram_commands.create_command(
            TelegramCommand(command_type="dialogs.react", payload={"phone": "+2"}, requested_by="t")
        )

        results = await asyncio.gather(
            *[db.repos.telegram_commands.claim_next_command() for _ in range(4)]
        )
        claimed_ids = sorted(cmd.id for cmd in results if cmd is not None)

        assert claimed_ids == [first, second]
        assert len(claimed_ids) == len(set(claimed_ids)), "a command was claimed twice"
    finally:
        await db.close()


@pytest.mark.anyio
async def test_claim_and_cancel_race_never_leaves_command_in_two_states(tmp_path):
    """claim ↔ cancel on the same PENDING row resolve to one outcome (#1030).

    Either cancel wins (CANCELLED, claim returns None) or claim wins (RUNNING,
    cancel returns False). The row must never end up RUNNING *and* reported
    cancelled, which would let a cancelled command still fire a Telegram call.
    """
    db = Database(str(tmp_path / "test.db"))
    await db.initialize()

    try:
        command_id = await db.repos.telegram_commands.create_command(
            TelegramCommand(command_type="dialogs.react", payload={"phone": "+1"}, requested_by="t")
        )

        claimed, cancelled = await asyncio.gather(
            db.repos.telegram_commands.claim_next_command(),
            db.repos.telegram_commands.cancel_command(command_id),
        )
        final = await db.repos.telegram_commands.get_command(command_id)
        assert final is not None

        claim_won = claimed is not None
        # Exactly one side took effect — never both, never neither.
        assert claim_won != cancelled, (
            f"claim_won={claim_won} cancelled={cancelled} — both or neither acted"
        )
        if claim_won:
            assert final.status == TelegramCommandStatus.RUNNING
            assert claimed.id == command_id
        else:
            assert final.status == TelegramCommandStatus.CANCELLED
    finally:
        await db.close()


@pytest.mark.anyio
async def test_reset_running_on_startup_requeues_orphaned_commands(tmp_path):
    """A worker crash leaves rows RUNNING; startup must requeue them (#1030).

    ``claim_next_command`` only picks PENDING rows, so a RUNNING row left behind
    by a killed worker would stay claimed forever. ``reset_running_on_startup``
    flips RUNNING → PENDING and clears started_at so the command is eligible and
    its retry shows a fresh run timestamp. PENDING / terminal rows are untouched.
    """
    db = Database(str(tmp_path / "test.db"))
    await db.initialize()

    try:
        repo = db.repos.telegram_commands
        # Orphaned RUNNING (claimed, then "crash" before finishing).
        orphan_id = await repo.create_command(
            TelegramCommand(command_type="dialogs.react", payload={"phone": "+1"}, requested_by="t")
        )
        claimed = await repo.claim_next_command()
        assert claimed is not None and claimed.id == orphan_id
        assert claimed.status == TelegramCommandStatus.RUNNING
        assert claimed.started_at is not None

        # An untouched PENDING row and a terminal SUCCEEDED row must survive intact.
        pending_id = await repo.create_command(
            TelegramCommand(command_type="dialogs.react", payload={"phone": "+2"}, requested_by="t")
        )
        done_id = await repo.create_command(
            TelegramCommand(command_type="dialogs.react", payload={"phone": "+3"}, requested_by="t")
        )
        await repo.update_command(done_id, status=TelegramCommandStatus.SUCCEEDED)

        reset_count = await repo.reset_running_on_startup()
        assert reset_count == 1, "only the single RUNNING row should be requeued"

        recovered = await repo.get_command(orphan_id)
        assert recovered is not None
        assert recovered.status == TelegramCommandStatus.PENDING
        assert recovered.started_at is None, "started_at must reset so retry shows fresh run"

        pending = await repo.get_command(pending_id)
        assert pending is not None and pending.status == TelegramCommandStatus.PENDING
        done = await repo.get_command(done_id)
        assert done is not None and done.status == TelegramCommandStatus.SUCCEEDED

        # The recovered command is claimable again — recovery actually worked.
        reclaimed = await repo.claim_next_command()
        assert reclaimed is not None and reclaimed.id == orphan_id
    finally:
        await db.close()


@pytest.mark.anyio
async def test_claim_next_command_picks_due_run_after(tmp_path):
    """A run_after in the past is due and gets claimed (#1030).

    Complements ``test_claim_next_command_skips_future_run_after``: that proves
    the future row is *skipped*; this proves a past row is *picked*, so a delayed
    retry actually resumes once its run_after passes.
    """
    db = Database(str(tmp_path / "test.db"))
    await db.initialize()

    try:
        due_id = await db.repos.telegram_commands.create_command(
            TelegramCommand(
                command_type="dialogs.react",
                payload={"phone": "+1", "message_id": 1},
                requested_by="test",
                run_after=datetime.now(timezone.utc) - timedelta(minutes=5),
            )
        )

        claimed = await db.repos.telegram_commands.claim_next_command()
        assert claimed is not None
        assert claimed.id == due_id
        assert claimed.status == TelegramCommandStatus.RUNNING
    finally:
        await db.close()


@pytest.mark.anyio
async def test_runtime_snapshots_repository_upsert_and_get(tmp_path):
    db = Database(str(tmp_path / "test.db"))
    await db.initialize()

    try:
        snapshot = RuntimeSnapshot(
            snapshot_type="worker_heartbeat",
            scope="global",
            payload={"status": "alive"},
            updated_at=datetime.now(timezone.utc),
        )
        await db.repos.runtime_snapshots.upsert_snapshot(snapshot)

        stored = await db.repos.runtime_snapshots.get_snapshot("worker_heartbeat", "global")

        assert stored is not None
        assert stored.snapshot_type == "worker_heartbeat"
        assert stored.scope == "global"
        assert stored.payload == {"status": "alive"}
    finally:
        await db.close()


@pytest.mark.anyio
async def test_runtime_snapshots_repository_handles_non_primitive_payload(tmp_path):
    """Regression for #473 tail: payload with datetime/bytes must round-trip
    via safe_json_dumps without raising TypeError.
    """
    db = Database(str(tmp_path / "test.db"))
    await db.initialize()

    try:
        snap_dt = datetime(2026, 4, 27, 12, 0, 0, tzinfo=timezone.utc)
        snapshot = RuntimeSnapshot(
            snapshot_type="exotic_payload",
            scope="global",
            payload={"published_at": snap_dt, "blob": b"\xde\xad\xbe\xef"},
        )
        await db.repos.runtime_snapshots.upsert_snapshot(snapshot)

        stored = await db.repos.runtime_snapshots.get_snapshot("exotic_payload", "global")
        assert stored is not None
        assert stored.payload["published_at"] == "2026-04-27T12:00:00+00:00"
        assert stored.payload["blob"] == "deadbeef"
    finally:
        await db.close()
