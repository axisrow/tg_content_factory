from __future__ import annotations

from datetime import datetime

import aiosqlite

from src.models import CollectionTask


class CollectionTasksRepository:
    def __init__(self, db: aiosqlite.Connection):
        self._db = db

    async def create_collection_task(self, channel_id: int, channel_title: str | None) -> int:
        cur = await self._db.execute(
            "INSERT INTO collection_tasks (channel_id, channel_title) VALUES (?, ?)",
            (channel_id, channel_title),
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
    ) -> None:
        now = datetime.utcnow().isoformat()
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
        r = await cur.fetchone()
        if r is None:
            return None
        return CollectionTask(
            id=r["id"],
            channel_id=r["channel_id"],
            channel_title=r["channel_title"],
            status=r["status"],
            messages_collected=r["messages_collected"],
            error=r["error"],
            created_at=(datetime.fromisoformat(r["created_at"]) if r["created_at"] else None),
            started_at=(datetime.fromisoformat(r["started_at"]) if r["started_at"] else None),
            completed_at=(datetime.fromisoformat(r["completed_at"]) if r["completed_at"] else None),
        )

    async def get_collection_tasks(self, limit: int = 20) -> list[CollectionTask]:
        cur = await self._db.execute(
            "SELECT * FROM collection_tasks ORDER BY id DESC LIMIT ?", (limit,)
        )
        rows = await cur.fetchall()
        return [
            CollectionTask(
                id=r["id"],
                channel_id=r["channel_id"],
                channel_title=r["channel_title"],
                status=r["status"],
                messages_collected=r["messages_collected"],
                error=r["error"],
                created_at=(datetime.fromisoformat(r["created_at"]) if r["created_at"] else None),
                started_at=(datetime.fromisoformat(r["started_at"]) if r["started_at"] else None),
                completed_at=(datetime.fromisoformat(r["completed_at"]) if r["completed_at"] else None),
            )
            for r in rows
        ]

    async def cancel_collection_task(self, task_id: int) -> bool:
        now = datetime.utcnow().isoformat()
        cur = await self._db.execute(
            "UPDATE collection_tasks SET status = 'cancelled', completed_at = ? "
            "WHERE id = ? AND status IN ('pending', 'running')",
            (now, task_id),
        )
        await self._db.commit()
        return cur.rowcount > 0
