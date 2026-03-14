from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

import aiosqlite

from src.models import (
    CollectionTask,
    CollectionTaskStatus,
    CollectionTaskType,
    StatsAllTaskPayload,
)


class CollectionTasksRepository:
    def __init__(self, db: aiosqlite.Connection):
        self._db = db

    @staticmethod
    def _deserialize_payload(
        raw: str | None,
    ) -> dict[str, Any] | StatsAllTaskPayload | None:
        if not raw:
            return None
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            return None
        if not isinstance(parsed, dict):
            return None
        if parsed.get("task_kind") == CollectionTaskType.STATS_ALL.value:
            return StatsAllTaskPayload.model_validate(parsed)
        return parsed

    @staticmethod
    def _serialize_payload(payload: dict[str, Any] | StatsAllTaskPayload | None) -> str | None:
        if payload is None:
            return None
        if isinstance(payload, StatsAllTaskPayload):
            return payload.model_dump_json()
        return json.dumps(payload)

    @staticmethod
    def _to_task(row: aiosqlite.Row) -> CollectionTask:
        return CollectionTask(
            id=row["id"],
            channel_id=row["channel_id"],
            channel_title=row["channel_title"],
            channel_username=row["channel_username"],
            task_type=CollectionTaskType(row["task_type"]),
            status=CollectionTaskStatus(row["status"]),
            messages_collected=row["messages_collected"],
            error=row["error"],
            note=row["note"],
            run_after=(datetime.fromisoformat(row["run_after"]) if row["run_after"] else None),
            payload=CollectionTasksRepository._deserialize_payload(row["payload"]),
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
        payload_json = self._serialize_payload(payload)
        cur = await self._db.execute(
            "INSERT INTO collection_tasks "
            "(channel_id, channel_title, channel_username, task_type,"
            " run_after, payload, parent_task_id) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                channel_id,
                channel_title,
                channel_username,
                CollectionTaskType.CHANNEL_COLLECT.value,
                run_after_iso,
                payload_json,
                parent_task_id,
            ),
        )
        await self._db.commit()
        return cur.lastrowid or 0

    async def create_stats_task(
        self,
        payload: StatsAllTaskPayload,
        *,
        run_after: datetime | None = None,
        parent_task_id: int | None = None,
    ) -> int:
        run_after_iso = run_after.astimezone(timezone.utc).isoformat() if run_after else None
        cur = await self._db.execute(
            "INSERT INTO collection_tasks "
            "(channel_id, channel_title, channel_username, task_type,"
            " run_after, payload, parent_task_id) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                None,
                "Обновление статистики",
                None,
                CollectionTaskType.STATS_ALL.value,
                run_after_iso,
                self._serialize_payload(payload),
                parent_task_id,
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
        status: CollectionTaskStatus | str,
        messages_collected: int | None = None,
        error: str | None = None,
        note: str | None = None,
    ) -> None:
        status_value = status.value if isinstance(status, CollectionTaskStatus) else status
        now = datetime.now(tz=timezone.utc).isoformat()
        sets = ["status = ?"]
        params: list[Any] = [status_value]
        if status_value == CollectionTaskStatus.RUNNING.value:
            sets.append("started_at = ?")
            params.append(now)
        terminal = (CollectionTaskStatus.COMPLETED.value, CollectionTaskStatus.FAILED.value)
        if status_value in terminal:
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

    @staticmethod
    def _status_where(status_filter: str | None) -> tuple[str, list[Any]]:
        """Build WHERE clause for status filter. Returns (clause, params)."""
        if status_filter == "active":
            return " WHERE status IN (?, ?)", [
                CollectionTaskStatus.PENDING.value,
                CollectionTaskStatus.RUNNING.value,
            ]
        if status_filter == "completed":
            return " WHERE status IN (?, ?, ?)", [
                CollectionTaskStatus.COMPLETED.value,
                CollectionTaskStatus.FAILED.value,
                CollectionTaskStatus.CANCELLED.value,
            ]
        return "", []

    async def count_collection_tasks(self, status_filter: str | None = None) -> int:
        """Count tasks matching the given status filter."""
        where, params = self._status_where(status_filter)
        cur = await self._db.execute(
            f"SELECT COUNT(*) as cnt FROM collection_tasks{where}", params
        )
        row = await cur.fetchone()
        return row["cnt"] if row else 0

    async def get_collection_tasks_paginated(
        self, limit: int = 20, offset: int = 0, status_filter: str | None = None
    ) -> tuple[list[CollectionTask], int]:
        """Get tasks with pagination and optional status filter.

        Returns: (tasks, total_count)
        """
        where, params = self._status_where(status_filter)

        # Get total count
        cur = await self._db.execute(
            f"SELECT COUNT(*) as cnt FROM collection_tasks{where}", list(params)
        )
        count_row = await cur.fetchone()
        total = count_row["cnt"] if count_row else 0

        # Get paginated results
        query = f"SELECT * FROM collection_tasks{where} ORDER BY id DESC LIMIT ? OFFSET ?"
        params.extend([limit, offset])
        cur = await self._db.execute(query, params)
        rows = await cur.fetchall()

        return [self._to_task(r) for r in rows], total

    async def get_active_collection_tasks_for_channel(
        self,
        channel_id: int,
    ) -> list[CollectionTask]:
        cur = await self._db.execute(
            "SELECT * FROM collection_tasks "
            "WHERE task_type = ? AND channel_id = ? AND status IN (?, ?) "
            "ORDER BY id ASC",
            (
                CollectionTaskType.CHANNEL_COLLECT.value,
                channel_id,
                CollectionTaskStatus.PENDING.value,
                CollectionTaskStatus.RUNNING.value,
            ),
        )
        rows = await cur.fetchall()
        return [self._to_task(r) for r in rows]

    async def get_channel_ids_with_active_tasks(self) -> set[int]:
        cur = await self._db.execute(
            "SELECT DISTINCT channel_id FROM collection_tasks "
            "WHERE task_type = ? AND status IN (?, ?) AND channel_id IS NOT NULL",
            (
                CollectionTaskType.CHANNEL_COLLECT.value,
                CollectionTaskStatus.PENDING.value,
                CollectionTaskStatus.RUNNING.value,
            ),
        )
        rows = await cur.fetchall()
        return {int(row["channel_id"]) for row in rows}

    async def get_active_stats_task(self) -> CollectionTask | None:
        cur = await self._db.execute(
            "SELECT * FROM collection_tasks "
            "WHERE task_type = ? AND status IN (?, ?) "
            "ORDER BY id ASC LIMIT 1",
            (
                CollectionTaskType.STATS_ALL.value,
                CollectionTaskStatus.PENDING.value,
                CollectionTaskStatus.RUNNING.value,
            ),
        )
        row = await cur.fetchone()
        if row is None:
            return None
        return self._to_task(row)

    async def claim_next_due_stats_task(self, now: datetime) -> CollectionTask | None:
        now_iso = now.astimezone(timezone.utc).isoformat()
        try:
            await self._db.execute("BEGIN IMMEDIATE")
            cur = await self._db.execute(
                "SELECT id FROM collection_tasks "
                "WHERE task_type = ? "
                "AND status = ? "
                "AND (run_after IS NULL OR run_after <= ?) "
                "ORDER BY COALESCE(run_after, ''), id ASC LIMIT 1",
                (
                    CollectionTaskType.STATS_ALL.value,
                    CollectionTaskStatus.PENDING.value,
                    now_iso,
                ),
            )
            row = await cur.fetchone()
            if row is None:
                await self._db.commit()
                return None
            selected_id = row["id"]
            updated = await self._db.execute(
                "UPDATE collection_tasks "
                "SET status = 'running', started_at = ?, completed_at = NULL "
                "WHERE id = ? AND status = ?",
                (now_iso, selected_id, CollectionTaskStatus.PENDING.value),
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
        payload: StatsAllTaskPayload,
        run_after: datetime | None,
        parent_task_id: int,
    ) -> int:
        return await self.create_stats_task(
            payload,
            run_after=run_after,
            parent_task_id=parent_task_id,
        )

    async def get_pending_channel_tasks(self) -> list[CollectionTask]:
        cur = await self._db.execute(
            "SELECT * FROM collection_tasks "
            "WHERE task_type = ? AND status = ? "
            "ORDER BY id ASC",
            (
                CollectionTaskType.CHANNEL_COLLECT.value,
                CollectionTaskStatus.PENDING.value,
            ),
        )
        rows = await cur.fetchall()
        return [self._to_task(r) for r in rows]

    async def fail_running_collection_tasks_on_startup(self) -> int:
        now = datetime.now(tz=timezone.utc).isoformat()
        cur = await self._db.execute(
            "UPDATE collection_tasks "
            "SET status = 'failed', completed_at = ? "
            "WHERE task_type = ? AND status = ?",
            (
                now,
                CollectionTaskType.CHANNEL_COLLECT.value,
                CollectionTaskStatus.RUNNING.value,
            ),
        )
        await self._db.commit()
        return cur.rowcount or 0

    async def requeue_running_stats_tasks_on_startup(self, now: datetime) -> int:
        now_iso = now.astimezone(timezone.utc).isoformat()
        cur = await self._db.execute(
            "UPDATE collection_tasks "
            "SET status = 'pending', started_at = NULL, run_after = COALESCE(run_after, ?) "
            "WHERE task_type = ? AND status = ?",
            (
                now_iso,
                CollectionTaskType.STATS_ALL.value,
                CollectionTaskStatus.RUNNING.value,
            ),
        )
        await self._db.commit()
        return cur.rowcount or 0

    async def cancel_collection_task(self, task_id: int, note: str | None = None) -> bool:
        now = datetime.now(tz=timezone.utc).isoformat()
        sets = ["status = 'cancelled'", "completed_at = ?"]
        params: list[Any] = [now]
        if note is not None:
            sets.append("note = ?")
            params.append(note)
        params.append(task_id)
        cur = await self._db.execute(
            f"UPDATE collection_tasks SET {', '.join(sets)} "
            "WHERE id = ? AND status IN (?, ?)",
            (
                *params,
                CollectionTaskStatus.PENDING.value,
                CollectionTaskStatus.RUNNING.value,
            ),
        )
        await self._db.commit()
        return cur.rowcount > 0
