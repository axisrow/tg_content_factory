from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

import aiosqlite

from src.models import CollectionTask


class CollectionTasksRepository:
    def __init__(self, db: aiosqlite.Connection):
        self._db = db

    @staticmethod
    def _parse_payload(raw: str | None) -> dict[str, Any] | None:
        if not raw:
            return None
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            return None
        return parsed if isinstance(parsed, dict) else None

    @staticmethod
    def _to_task(row: aiosqlite.Row) -> CollectionTask:
        return CollectionTask(
            id=row["id"],
            channel_id=row["channel_id"],
            channel_title=row["channel_title"],
            channel_username=row["channel_username"],
            status=row["status"],
            messages_collected=row["messages_collected"],
            error=row["error"],
            note=row["note"],
            run_after=(datetime.fromisoformat(row["run_after"]) if row["run_after"] else None),
            payload=CollectionTasksRepository._parse_payload(row["payload"]),
            parent_task_id=row["parent_task_id"],
            created_at=(datetime.fromisoformat(row["created_at"]) if row["created_at"] else None),
            started_at=(datetime.fromisoformat(row["started_at"]) if row["started_at"] else None),
            completed_at=(
                datetime.fromisoformat(row["completed_at"])
                if row["completed_at"]
                else None
            ),
        )

    async def create_collection_task(
        self,
        channel_id: int,
        channel_title: str | None,
        *,
        channel_username: str | None = None,
        run_after: datetime | None = None,
        payload: dict[str, Any] | None = None,
        parent_task_id: int | None = None,
    ) -> int:
        run_after_iso = (
            run_after.astimezone(timezone.utc).isoformat() if run_after else None
        )
        payload_json = json.dumps(payload) if payload is not None else None
        cur = await self._db.execute(
            "INSERT INTO collection_tasks "
            "(channel_id, channel_title, channel_username, run_after, payload, parent_task_id) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            (
                channel_id, channel_title, channel_username,
                run_after_iso, payload_json, parent_task_id,
            ),
        )
        await self._db.commit()
        return cur.lastrowid or 0

    async def update_collection_task_progress(self, task_id: int, messages_collected: int) -> None:
        await self._db.execute(
            "UPDATE collection_tasks SET messages_collected = ? WHERE id = ?",
            (messages_collected, task_id),
        )
        await self._db.commit()

    async def update_collection_task(
        self,
        task_id: int,
        status: str,
        messages_collected: int | None = None,
        error: str | None = None,
        note: str | None = None,
    ) -> None:
        now = datetime.now(tz=timezone.utc).isoformat()
        sets = ["status = ?"]
        params: list = [status]
        if status == "running":
            sets.append("started_at = ?")
            params.append(now)
        if status in ("completed", "failed"):
            sets.append("completed_at = ?")
            params.append(now)
        if messages_collected is not None:
            sets.append("messages_collected = ?")
            params.append(messages_collected)
        if error is not None:
            sets.append("error = ?")
            params.append(error)
        if note is not None:
            sets.append("note = ?")
            params.append(note)
        params.append(task_id)
        await self._db.execute(
            f"UPDATE collection_tasks SET {', '.join(sets)} WHERE id = ?",
            tuple(params),
        )
        await self._db.commit()

    async def get_collection_task(self, task_id: int) -> CollectionTask | None:
        cur = await self._db.execute(
            "SELECT * FROM collection_tasks WHERE id = ?", (task_id,)
        )
        row = await cur.fetchone()
        if row is None:
            return None
        return self._to_task(row)

    async def get_collection_tasks(self, limit: int = 20) -> list[CollectionTask]:
        cur = await self._db.execute(
            "SELECT * FROM collection_tasks ORDER BY id DESC LIMIT ?", (limit,)
        )
        rows = await cur.fetchall()
        return [self._to_task(r) for r in rows]

    async def get_active_stats_task(self) -> CollectionTask | None:
        cur = await self._db.execute(
            "SELECT * FROM collection_tasks "
            "WHERE channel_id = 0 AND status IN ('pending', 'running') "
            "ORDER BY id ASC LIMIT 1"
        )
        row = await cur.fetchone()
        if row is None:
            return None
        return self._to_task(row)

    async def claim_next_due_stats_task(self, now: datetime) -> CollectionTask | None:
        now_iso = now.astimezone(timezone.utc).isoformat()
        selected_id: int | None = None
        try:
            await self._db.execute("BEGIN IMMEDIATE")
            cur = await self._db.execute(
                "SELECT id FROM collection_tasks "
                "WHERE channel_id = 0 "
                "AND status = 'pending' "
                "AND (run_after IS NULL OR run_after <= ?) "
                "ORDER BY COALESCE(run_after, ''), id ASC LIMIT 1",
                (now_iso,),
            )
            row = await cur.fetchone()
            if row is None:
                await self._db.commit()
                return None
            selected_id = row["id"]
            updated = await self._db.execute(
                "UPDATE collection_tasks "
                "SET status = 'running', started_at = ?, completed_at = NULL "
                "WHERE id = ? AND status = 'pending'",
                (now_iso, selected_id),
            )
            if (updated.rowcount or 0) == 0:
                await self._db.commit()
                return None
            cur = await self._db.execute(
                "SELECT * FROM collection_tasks WHERE id = ?",
                (selected_id,),
            )
            claimed = await cur.fetchone()
            await self._db.commit()
            if claimed is None:
                return None
            return self._to_task(claimed)
        except Exception:
            await self._db.rollback()
            raise

    async def create_stats_continuation_task(
        self,
        *,
        payload: dict[str, Any],
        run_after: datetime | None,
        parent_task_id: int,
    ) -> int:
        return await self.create_collection_task(
            0,
            "Обновление статистики",
            run_after=run_after,
            payload=payload,
            parent_task_id=parent_task_id,
        )

    async def requeue_running_stats_tasks_on_startup(self, now: datetime) -> int:
        now_iso = now.astimezone(timezone.utc).isoformat()
        cur = await self._db.execute(
            "UPDATE collection_tasks "
            "SET status = 'pending', started_at = NULL, run_after = COALESCE(run_after, ?) "
            "WHERE channel_id = 0 AND status = 'running'",
            (now_iso,),
        )
        await self._db.commit()
        return cur.rowcount or 0

    async def cancel_collection_task(self, task_id: int) -> bool:
        now = datetime.now(tz=timezone.utc).isoformat()
        cur = await self._db.execute(
            "UPDATE collection_tasks SET status = 'cancelled', completed_at = ? "
            "WHERE id = ? AND status IN ('pending', 'running')",
            (now, task_id),
        )
        await self._db.commit()
        return cur.rowcount > 0
