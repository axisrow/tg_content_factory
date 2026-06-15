from __future__ import annotations

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
        original_execute = repo._db.execute
        call_count = {"n": 0}

        async def boom(sql, *args, **kwargs):
            call_count["n"] += 1
            # Fail after BEGIN IMMEDIATE but before UPDATE
            if call_count["n"] == 2:
                raise RuntimeError("simulated fetch failure")
            return await original_execute(sql, *args, **kwargs)

        with patch.object(repo._db, "execute", side_effect=boom):
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
