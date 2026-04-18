from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import patch

import pytest

from src.database import Database
from src.models import RuntimeSnapshot, TelegramCommand, TelegramCommandStatus


@pytest.mark.asyncio
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


@pytest.mark.asyncio
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


@pytest.mark.asyncio
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


@pytest.mark.asyncio
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
