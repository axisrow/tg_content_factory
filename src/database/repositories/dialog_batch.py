"""Durable ledger for resumable per-dialog operations."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import TYPE_CHECKING

import aiosqlite

from src.database.pool import ReadConnection
from src.models import DialogBatchItem, DialogBatchOperation, DialogBatchStatus
from src.utils.datetime import parse_datetime

if TYPE_CHECKING:
    from src.database.facade import Database


class DialogBatchRepository:
    def __init__(self, db: ReadConnection, *, database: "Database"):
        self._db = db
        self._database = database

    @staticmethod
    def _operation(row: aiosqlite.Row) -> DialogBatchOperation:
        return DialogBatchOperation(id=row["id"], phone=row["phone"], op_type=row["op_type"],
                                    status=DialogBatchStatus(row["status"]),
                                    created_at=parse_datetime(row["created_at"]),
                                    finished_at=parse_datetime(row["finished_at"]))

    @staticmethod
    def _item(row: aiosqlite.Row) -> DialogBatchItem:
        return DialogBatchItem(id=row["id"], batch_id=row["batch_id"], phone=row["phone"],
                               dialog_id=row["dialog_id"], channel_type=row["channel_type"],
                               status=DialogBatchStatus(row["status"]), error=row["error"],
                               attempts=row["attempts"], created_at=parse_datetime(row["created_at"]),
                               finished_at=parse_datetime(row["finished_at"]))

    async def create_batch(self, batch: DialogBatchOperation, dialogs: list[tuple[int, str]]) -> int:
        async with self._database.transaction() as conn:
            cur = await conn.execute(
                "INSERT INTO dialog_batch_operations(phone, op_type, status) VALUES (?, ?, ?)",
                (batch.phone, batch.op_type, batch.status.value),
            )
            batch_id = int(cur.lastrowid)
            await conn.executemany(
                "INSERT INTO dialog_batch_items(batch_id, phone, dialog_id, channel_type) VALUES (?, ?, ?, ?)",
                [(batch_id, batch.phone, int(dialog_id), channel_type) for dialog_id, channel_type in dialogs],
            )
        return batch_id

    async def get_batch(self, batch_id: int) -> DialogBatchOperation | None:
        cur = await self._db.execute("SELECT * FROM dialog_batch_operations WHERE id = ?", (batch_id,))
        row = await cur.fetchone()
        return self._operation(row) if row else None

    async def acquire_lease(self, batch_id: int, owner: str, now: datetime, lease_seconds: int = 3600) -> bool:
        """Atomically start a batch or take over a genuinely stale worker."""
        now_iso = now.astimezone(timezone.utc).isoformat()
        until = datetime.fromtimestamp(now.timestamp() + lease_seconds, timezone.utc).isoformat()
        cur = await self._database.execute_write(
            """UPDATE dialog_batch_operations
               SET status = ?, lease_owner = ?, lease_until = ?
               WHERE id = ? AND (status = ? OR (status = ? AND (lease_until IS NULL OR lease_until < ?)))""",
            (DialogBatchStatus.RUNNING.value, owner, until, batch_id,
             DialogBatchStatus.PENDING.value, DialogBatchStatus.RUNNING.value, now_iso),
        )
        return bool(cur.rowcount)

    async def renew_lease(self, batch_id: int, owner: str, now: datetime, lease_seconds: int = 3600) -> bool:
        until = datetime.fromtimestamp(now.timestamp() + lease_seconds, timezone.utc).isoformat()
        cur = await self._database.execute_write(
            "UPDATE dialog_batch_operations SET lease_until = ? WHERE id = ? AND lease_owner = ? AND status = ?",
            (until, batch_id, owner, DialogBatchStatus.RUNNING.value),
        )
        return bool(cur.rowcount)

    async def list_items(self, batch_id: int) -> list[DialogBatchItem]:
        cur = await self._db.execute("SELECT * FROM dialog_batch_items WHERE batch_id = ? ORDER BY id", (batch_id,))
        return [self._item(row) for row in await cur.fetchall()]

    async def claim_next(self, batch_id: int, owner: str | None = None) -> DialogBatchItem | None:
        """Claim exactly one pending item; safe when multiple workers race."""
        async with self._database.transaction() as conn:
            owner_clause = (
                " AND EXISTS (SELECT 1 FROM dialog_batch_operations "
                "WHERE id = ? AND lease_owner = ? AND lease_until > ?)"
                if owner else ""
            )
            params: tuple[object, ...] = (DialogBatchStatus.RUNNING.value, batch_id, DialogBatchStatus.PENDING.value)
            if owner:
                params += (batch_id, owner, datetime.now(timezone.utc).isoformat())
            cur = await conn.execute(
                f"""UPDATE dialog_batch_items SET status = ?, attempts = attempts + 1
                   WHERE id = (SELECT id FROM dialog_batch_items
                               WHERE batch_id = ? AND status = ? ORDER BY id LIMIT 1)
                   {owner_clause} RETURNING *""",
                params,
            )
            row = await cur.fetchone()
        return self._item(row) if row else None

    async def update_item(self, item_id: int, status: DialogBatchStatus, error: str | None = None,
                          owner: str | None = None) -> None:
        finished = datetime.now(timezone.utc).isoformat() if status in {
            DialogBatchStatus.COMPLETED, DialogBatchStatus.FAILED
        } else None
        sql = "UPDATE dialog_batch_items SET status = ?, error = ?, finished_at = ? WHERE id = ?"
        params: tuple[object, ...] = (status.value, error, finished, item_id)
        if owner:
            sql += (
                " AND EXISTS (SELECT 1 FROM dialog_batch_operations o "
                "WHERE o.id = batch_id AND o.lease_owner = ? AND o.lease_until > ?)"
            )
            params += (owner, datetime.now(timezone.utc).isoformat())
        await self._database.execute_write(sql, params)

    async def recover_running(self, batch_id: int) -> int:
        cur = await self._database.execute_write(
            "UPDATE dialog_batch_items SET status = ?, error = ? WHERE batch_id = ? AND status = ?",
            (DialogBatchStatus.PENDING.value, "Recovered after interrupted run", batch_id,
             DialogBatchStatus.RUNNING.value),
        )
        return cur.rowcount or 0

    async def finish_batch(self, batch_id: int, status: DialogBatchStatus, owner: str | None = None) -> None:
        cur = await self._db.execute(
            "SELECT COUNT(*) AS n FROM dialog_batch_items WHERE batch_id = ? AND status = ?",
            (batch_id, DialogBatchStatus.RUNNING.value),
        )
        row = await cur.fetchone()
        if row and row["n"]:
            return
        sql = (
            "UPDATE dialog_batch_operations SET status = ?, finished_at = ?, "
            "lease_owner = NULL, lease_until = NULL WHERE id = ?"
        )
        params: tuple[object, ...] = (status.value, datetime.now(timezone.utc).isoformat(), batch_id)
        if owner is not None:
            sql += " AND lease_owner = ?"
            params += (owner,)
        await self._database.execute_write(sql, params)
