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
