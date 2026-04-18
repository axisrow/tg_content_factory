from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import Any

import aiosqlite

from src.database.repositories._transactions import begin_immediate
from src.models import (
    CollectionTask,
    CollectionTaskStatus,
    CollectionTaskType,
    ContentGenerateTaskPayload,
    ContentPublishTaskPayload,
    PipelineRunTaskPayload,
    SqStatsTaskPayload,
    StatsAllTaskPayload,
    TranslateBatchTaskPayload,
)

_ALLOWED_PAYLOAD_FILTER_KEYS = frozenset({"sq_id", "pipeline_id"})


def _safe_task_type(raw: str) -> CollectionTaskType:
    try:
        return CollectionTaskType(raw)
    except ValueError:
        return CollectionTaskType.CHANNEL_COLLECT


class CollectionTasksRepository:
    def __init__(self, db: aiosqlite.Connection):
        self._db = db

    @staticmethod
    def _deserialize_payload(
        raw: str | None,
    ) -> (
        dict[str, Any] | StatsAllTaskPayload | SqStatsTaskPayload | PipelineRunTaskPayload
        | TranslateBatchTaskPayload | None
    ):
        if not raw:
            return None
        try:
            parsed = json.loads(raw)
        except json.JSONDecodeError:
            return None
        if not isinstance(parsed, dict):
            return None
        task_kind = parsed.get("task_kind")
        if task_kind == CollectionTaskType.STATS_ALL.value:
            return StatsAllTaskPayload.model_validate(parsed)
        if task_kind == CollectionTaskType.SQ_STATS.value:
            return SqStatsTaskPayload.model_validate(parsed)
        if task_kind == CollectionTaskType.PIPELINE_RUN.value:
            return PipelineRunTaskPayload.model_validate(parsed)
        if task_kind == CollectionTaskType.CONTENT_GENERATE.value:
            return ContentGenerateTaskPayload.model_validate(parsed)
        if task_kind == CollectionTaskType.CONTENT_PUBLISH.value:
            return ContentPublishTaskPayload.model_validate(parsed)
        if task_kind == CollectionTaskType.TRANSLATE_BATCH.value:
            return TranslateBatchTaskPayload.model_validate(parsed)
        return parsed

    @staticmethod
    def _serialize_payload(
        payload: (
            dict[str, Any]
            | StatsAllTaskPayload
            | SqStatsTaskPayload
            | PipelineRunTaskPayload
            | ContentGenerateTaskPayload
            | ContentPublishTaskPayload
            | TranslateBatchTaskPayload
            | None
        ),
    ) -> str | None:
        if payload is None:
            return None
        if isinstance(
            payload,
            (
                StatsAllTaskPayload,
                SqStatsTaskPayload,
                PipelineRunTaskPayload,
                ContentGenerateTaskPayload,
                ContentPublishTaskPayload,
                TranslateBatchTaskPayload,
            ),
        ):
            return payload.model_dump_json()
        return json.dumps(payload)

    @staticmethod
    def _to_task(row: aiosqlite.Row) -> CollectionTask:
        return CollectionTask(
            id=row["id"],
            channel_id=row["channel_id"],
            channel_title=row["channel_title"],
            channel_username=row["channel_username"],
            task_type=_safe_task_type(row["task_type"]),
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
                datetime.fromisoformat(row["completed_at"]) if row["completed_at"] else None
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
        run_after_iso = run_after.astimezone(timezone.utc).isoformat() if run_after else None
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

    async def create_collection_task_if_not_active(
        self,
        channel_id: int,
        channel_title: str | None,
        *,
        channel_username: str | None = None,
        run_after: datetime | None = None,
        payload: dict[str, Any] | None = None,
        parent_task_id: int | None = None,
    ) -> int | None:
        """Atomically create a collection task only if no active task exists.

        Returns the new task ID, or ``None`` if an active task already exists.
        """
        run_after_iso = run_after.astimezone(timezone.utc).isoformat() if run_after else None
        payload_json = self._serialize_payload(payload)
        cur = await self._db.execute(
            "INSERT INTO collection_tasks "
            "(channel_id, channel_title, channel_username, task_type,"
            " run_after, payload, parent_task_id) "
            "SELECT ?, ?, ?, ?, ?, ?, ? "
            "WHERE NOT EXISTS ("
            "  SELECT 1 FROM collection_tasks "
            "  WHERE channel_id = ? AND task_type = ? AND status IN (?, ?)"
            ")",
            (
                channel_id,
                channel_title,
                channel_username,
                CollectionTaskType.CHANNEL_COLLECT.value,
                run_after_iso,
                payload_json,
                parent_task_id,
                channel_id,
                CollectionTaskType.CHANNEL_COLLECT.value,
                CollectionTaskStatus.PENDING.value,
                CollectionTaskStatus.RUNNING.value,
            ),
        )
        await self._db.commit()
        if cur.rowcount == 1:
            return cur.lastrowid or 0
        return None

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

    async def persist_stats_progress(
        self,
        task_id: int,
        *,
        payload: StatsAllTaskPayload,
        messages_collected: int,
    ) -> None:
        """Persist current cursor/counters into the DB row for crash-safe resume."""
        await self._db.execute(
            "UPDATE collection_tasks SET messages_collected = ?, payload = ? WHERE id = ?",
            (messages_collected, self._serialize_payload(payload), task_id),
        )
        await self._db.commit()

    async def update_collection_task(
        self,
        task_id: int,
        status: CollectionTaskStatus | str,
        messages_collected: int | None = None,
        error: str | None = None,
        note: str | None = None,
        run_after: datetime | None = None,
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
        if run_after is not None:
            sets.append("run_after = ?")
            params.append(run_after.astimezone(timezone.utc).isoformat())
        params.append(task_id)
        await self._db.execute(
            f"UPDATE collection_tasks SET {', '.join(sets)} WHERE id = ?",
            tuple(params),
        )
        await self._db.commit()

    async def reschedule_collection_task(
        self,
        task_id: int,
        *,
        run_after: datetime,
        note: str | None = None,
        messages_collected: int = 0,
    ) -> None:
        sets = [
            "status = ?",
            "run_after = ?",
            "started_at = NULL",
            "completed_at = NULL",
            "error = NULL",
            "messages_collected = ?",
        ]
        params: list[Any] = [
            CollectionTaskStatus.PENDING.value,
            run_after.astimezone(timezone.utc).isoformat(),
            messages_collected,
        ]
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
        cur = await self._db.execute("SELECT * FROM collection_tasks WHERE id = ?", (task_id,))
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
    def _status_where(status_filter: str | None) -> tuple[str, tuple[Any, ...]]:
        """Build WHERE clause for status filter. Returns (clause, params)."""
        if status_filter == "active":
            return " WHERE status IN (?, ?)", (
                CollectionTaskStatus.PENDING.value,
                CollectionTaskStatus.RUNNING.value,
            )
        if status_filter == "completed":
            return " WHERE status IN (?, ?, ?)", (
                CollectionTaskStatus.COMPLETED.value,
                CollectionTaskStatus.FAILED.value,
                CollectionTaskStatus.CANCELLED.value,
            )
        return "", ()

    async def count_collection_tasks(self, status_filter: str | None = None) -> int:
        """Count tasks matching the given status filter."""
        where, params = self._status_where(status_filter)
        cur = await self._db.execute(f"SELECT COUNT(*) as cnt FROM collection_tasks{where}", params)
        row = await cur.fetchone()
        return row["cnt"] if row else 0

    async def get_collection_tasks_paginated(
        self, limit: int = 20, offset: int = 0, status_filter: str | None = None
    ) -> tuple[list[CollectionTask], int]:
        """Get tasks with pagination and optional status filter.

        Returns: (tasks, total_count)
        """
        where, base_params = self._status_where(status_filter)

        # Get total count
        cur = await self._db.execute(
            f"SELECT COUNT(*) as cnt FROM collection_tasks{where}", base_params
        )
        count_row = await cur.fetchone()
        total = count_row["cnt"] if count_row else 0

        # Get paginated results
        query = f"SELECT * FROM collection_tasks{where} ORDER BY id DESC LIMIT ? OFFSET ?"
        cur = await self._db.execute(query, (*base_params, limit, offset))
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

    async def reschedule_stats_task(
        self,
        task_id: int,
        *,
        payload: StatsAllTaskPayload,
        run_after: datetime,
        messages_collected: int,
    ) -> None:
        """Return an in-progress stats task to PENDING with updated payload/run_after."""
        await self._db.execute(
            "UPDATE collection_tasks "
            "SET status = ?, payload = ?, run_after = ?, messages_collected = ?, "
            "    started_at = NULL, completed_at = NULL, error = NULL "
            "WHERE id = ?",
            (
                CollectionTaskStatus.PENDING.value,
                self._serialize_payload(payload),
                run_after.astimezone(timezone.utc).isoformat(),
                messages_collected,
                task_id,
            ),
        )
        await self._db.commit()

    async def get_pending_channel_tasks(self) -> list[CollectionTask]:
        cur = await self._db.execute(
            "SELECT * FROM collection_tasks "
            "WHERE task_type = ? AND status = ? "
            "ORDER BY COALESCE(run_after, ''), id ASC",
            (
                CollectionTaskType.CHANNEL_COLLECT.value,
                CollectionTaskStatus.PENDING.value,
            ),
        )
        rows = await cur.fetchall()
        return [self._to_task(r) for r in rows]

    async def delete_pending_channel_tasks(self) -> int:
        cur = await self._db.execute(
            "DELETE FROM collection_tasks "
            "WHERE task_type = ? AND status = ?",
            (
                CollectionTaskType.CHANNEL_COLLECT.value,
                CollectionTaskStatus.PENDING.value,
            ),
        )
        await self._db.commit()
        return cur.rowcount or 0

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

    async def reset_orphaned_running_tasks(self) -> int:
        """Reset orphaned RUNNING channel tasks to PENDING status.

        Called on startup to recover from ungraceful shutdowns where RUNNING
        tasks were not properly completed or failed.
        """
        cur = await self._db.execute(
            "UPDATE collection_tasks "
            "SET status = ?, started_at = NULL "
            "WHERE task_type = ? AND status = ?",
            (
                CollectionTaskStatus.PENDING.value,
                CollectionTaskType.CHANNEL_COLLECT.value,
                CollectionTaskStatus.RUNNING.value,
            ),
        )
        await self._db.commit()
        return cur.rowcount or 0

    async def create_generic_task(
        self,
        task_type: CollectionTaskType | str,
        *,
        title: str = "",
        payload: (
            dict[str, Any]
            | StatsAllTaskPayload
            | SqStatsTaskPayload
            | PipelineRunTaskPayload
            | ContentGenerateTaskPayload
            | ContentPublishTaskPayload
            | TranslateBatchTaskPayload
            | None
        ) = None,
        run_after: datetime | None = None,
        parent_task_id: int | None = None,
    ) -> int:
        tt = task_type
        task_type_value = tt.value if isinstance(tt, CollectionTaskType) else tt
        run_after_iso = run_after.astimezone(timezone.utc).isoformat() if run_after else None
        cur = await self._db.execute(
            "INSERT INTO collection_tasks "
            "(channel_id, channel_title, channel_username, task_type,"
            " run_after, payload, parent_task_id) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            (
                None,
                title or task_type_value,
                None,
                task_type_value,
                run_after_iso,
                self._serialize_payload(payload),
                parent_task_id,
            ),
        )
        await self._db.commit()
        return cur.lastrowid or 0

    async def claim_next_due_generic_task(
        self, now: datetime, handled_types: list[str]
    ) -> CollectionTask | None:
        if not handled_types:
            return None
        now_iso = now.astimezone(timezone.utc).isoformat()
        placeholders = ", ".join("?" for _ in handled_types)

        # Phase 1: lightweight read-only peek — no write lock
        cur = await self._db.execute(
            f"SELECT id FROM collection_tasks "
            f"WHERE task_type IN ({placeholders}) "
            "AND status = ? "
            "AND (run_after IS NULL OR run_after <= ?) "
            "ORDER BY COALESCE(run_after, ''), id ASC LIMIT 1",
            (*handled_types, CollectionTaskStatus.PENDING.value, now_iso),
        )
        peek = await cur.fetchone()
        if peek is None:
            return None

        # Phase 2: acquire write lock only when there is work to claim
        try:
            await begin_immediate(self._db)
            selected_id = peek["id"]
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

    async def requeue_running_generic_tasks_on_startup(
        self, now: datetime, handled_types: list[str]
    ) -> int:
        if not handled_types:
            return 0
        now_iso = now.astimezone(timezone.utc).isoformat()
        placeholders = ", ".join("?" for _ in handled_types)
        cur = await self._db.execute(
            f"UPDATE collection_tasks "
            "SET status = 'pending', started_at = NULL, run_after = COALESCE(run_after, ?) "
            f"WHERE task_type IN ({placeholders}) AND status = ?",
            (now_iso, *handled_types, CollectionTaskStatus.RUNNING.value),
        )
        await self._db.commit()
        return cur.rowcount or 0

    async def has_active_task(
        self,
        task_type: CollectionTaskType | str,
        *,
        payload_filter_key: str | None = None,
        payload_filter_value: str | int | None = None,
    ) -> bool:
        tt = task_type
        task_type_value = tt.value if isinstance(tt, CollectionTaskType) else tt
        sql = (
            "SELECT COUNT(*) as cnt FROM collection_tasks "
            "WHERE task_type = ? AND status IN (?, ?)"
        )
        params: list[Any] = [
            task_type_value,
            CollectionTaskStatus.PENDING.value,
            CollectionTaskStatus.RUNNING.value,
        ]
        if payload_filter_key is not None and payload_filter_value is not None:
            if payload_filter_key not in _ALLOWED_PAYLOAD_FILTER_KEYS:
                raise ValueError(f"Invalid payload filter key: {payload_filter_key!r}")
            sql += f" AND json_extract(payload, '$.{payload_filter_key}') = ?"
            params.append(payload_filter_value)
        cur = await self._db.execute(sql, tuple(params))
        row = await cur.fetchone()
        return (row["cnt"] if row else 0) > 0

    async def cancel_collection_task(self, task_id: int, note: str | None = None) -> bool:
        now = datetime.now(tz=timezone.utc).isoformat()
        sets = ["status = 'cancelled'", "completed_at = ?"]
        params: list[Any] = [now]
        if note is not None:
            sets.append("note = ?")
            params.append(note)
        params.append(task_id)
        cur = await self._db.execute(
            f"UPDATE collection_tasks SET {', '.join(sets)} " "WHERE id = ? AND status IN (?, ?)",
            (
                *params,
                CollectionTaskStatus.PENDING.value,
                CollectionTaskStatus.RUNNING.value,
            ),
        )
        await self._db.commit()
        return cur.rowcount > 0

    async def get_last_completed_collect_task(self) -> CollectionTask | None:
        """Return the most recently completed channel_collect task."""
        cur = await self._db.execute(
            "SELECT * FROM collection_tasks "
            "WHERE task_type = ? AND status = ? "
            "ORDER BY completed_at DESC LIMIT 1",
            (
                CollectionTaskType.CHANNEL_COLLECT.value,
                CollectionTaskStatus.COMPLETED.value,
            ),
        )
        row = await cur.fetchone()
        if row is None:
            return None
        return self._to_task(row)
