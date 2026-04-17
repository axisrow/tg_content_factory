from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

import aiosqlite

from src.models import TelegramCommand, TelegramCommandStatus


def _parse_json(raw: str | None) -> dict[str, Any] | None:
    if not raw:
        return None
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return None
    return parsed if isinstance(parsed, dict) else None


class TelegramCommandsRepository:
    def __init__(self, db: aiosqlite.Connection):
        self._db = db

    @staticmethod
    def _to_command(row: aiosqlite.Row) -> TelegramCommand:
        return TelegramCommand(
            id=row["id"],
            command_type=row["command_type"],
            payload=_parse_json(row["payload"]) or {},
            status=TelegramCommandStatus(row["status"]),
            requested_by=row["requested_by"],
            created_at=(datetime.fromisoformat(row["created_at"]) if row["created_at"] else None),
            started_at=(datetime.fromisoformat(row["started_at"]) if row["started_at"] else None),
            finished_at=(datetime.fromisoformat(row["finished_at"]) if row["finished_at"] else None),
            error=row["error"],
            result_payload=_parse_json(row["result_payload"]),
        )

    async def create_command(self, command: TelegramCommand) -> int:
        cur = await self._db.execute(
            """
            INSERT INTO telegram_commands (
                command_type, payload, status, requested_by, result_payload
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (
                command.command_type,
                json.dumps(command.payload),
                command.status.value,
                command.requested_by,
                json.dumps(command.result_payload) if command.result_payload is not None else None,
            ),
        )
        await self._db.commit()
        return cur.lastrowid or 0

    async def get_command(self, command_id: int) -> TelegramCommand | None:
        cur = await self._db.execute(
            "SELECT * FROM telegram_commands WHERE id = ?",
            (command_id,),
        )
        row = await cur.fetchone()
        return self._to_command(row) if row else None

    async def list_commands(self, *, limit: int = 100) -> list[TelegramCommand]:
        cur = await self._db.execute(
            "SELECT * FROM telegram_commands ORDER BY id DESC LIMIT ?",
            (limit,),
        )
        rows = await cur.fetchall()
        return [self._to_command(row) for row in rows]

    async def find_active_by_type(
        self, command_type: str, *, payload: dict[str, Any] | None = None
    ) -> TelegramCommand | None:
        """Return the oldest PENDING/RUNNING command of the given type (and payload, if provided)."""
        cur = await self._db.execute(
            """
            SELECT * FROM telegram_commands
            WHERE command_type = ? AND status IN (?, ?)
            ORDER BY id ASC
            """,
            (
                command_type,
                TelegramCommandStatus.PENDING.value,
                TelegramCommandStatus.RUNNING.value,
            ),
        )
        rows = await cur.fetchall()
        if not rows:
            return None
        if payload is None:
            return self._to_command(rows[0])
        for row in rows:
            row_payload = _parse_json(row["payload"]) or {}
            if row_payload == payload:
                return self._to_command(row)
        return None

    async def reset_running_on_startup(self) -> int:
        """Move RUNNING commands back to PENDING on worker startup.

        Commands can be left in RUNNING if the worker was killed mid-dispatch
        (asyncio.CancelledError re-raise, SIGTERM, etc). Without this reset
        they would stay claimed forever, since claim_next_command only picks
        PENDING rows.
        """
        cur = await self._db.execute(
            """
            UPDATE telegram_commands
            SET status = ?, started_at = NULL
            WHERE status = ?
            """,
            (TelegramCommandStatus.PENDING.value, TelegramCommandStatus.RUNNING.value),
        )
        await self._db.commit()
        return cur.rowcount or 0

    async def claim_next_command(self) -> TelegramCommand | None:
        now = datetime.now(timezone.utc).isoformat()
        await self._db.execute("BEGIN IMMEDIATE")
        try:
            cur = await self._db.execute(
                """
                SELECT * FROM telegram_commands
                WHERE status = ?
                ORDER BY id ASC
                LIMIT 1
                """,
                (TelegramCommandStatus.PENDING.value,),
            )
            row = await cur.fetchone()
            if row is None:
                await self._db.commit()
                return None
            await self._db.execute(
                """
                UPDATE telegram_commands
                SET status = ?, started_at = ?
                WHERE id = ? AND status = ?
                """,
                (
                    TelegramCommandStatus.RUNNING.value,
                    now,
                    row["id"],
                    TelegramCommandStatus.PENDING.value,
                ),
            )
            await self._db.commit()
            return await self.get_command(row["id"])
        except BaseException:
            try:
                await self._db.rollback()
            except Exception:
                pass
            raise

    async def update_command(
        self,
        command_id: int,
        *,
        status: TelegramCommandStatus,
        error: str | None = None,
        result_payload: dict[str, Any] | None = None,
    ) -> None:
        finished_at = None
        if status in {
            TelegramCommandStatus.SUCCEEDED,
            TelegramCommandStatus.FAILED,
            TelegramCommandStatus.CANCELLED,
        }:
            finished_at = datetime.now(timezone.utc).isoformat()
        await self._db.execute(
            """
            UPDATE telegram_commands
            SET status = ?, error = ?, result_payload = ?, finished_at = COALESCE(?, finished_at)
            WHERE id = ?
            """,
            (
                status.value,
                error,
                json.dumps(result_payload) if result_payload is not None else None,
                finished_at,
                command_id,
            ),
        )
        await self._db.commit()
