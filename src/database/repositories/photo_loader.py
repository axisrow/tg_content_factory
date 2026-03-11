from __future__ import annotations

import json
from datetime import datetime, timezone

import aiosqlite

from src.models import (
    PhotoAutoUploadJob,
    PhotoBatch,
    PhotoBatchItem,
    PhotoBatchStatus,
    PhotoSendMode,
)


def _dt(value: str | None) -> datetime | None:
    return datetime.fromisoformat(value) if value else None


def _json_loads_list(raw: str | None) -> list:
    if not raw:
        return []
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        return []
    return parsed if isinstance(parsed, list) else []


class PhotoLoaderRepository:
    def __init__(self, db: aiosqlite.Connection):
        self._db = db

    @staticmethod
    def _to_batch(row: aiosqlite.Row) -> PhotoBatch:
        return PhotoBatch(
            id=row["id"],
            phone=row["phone"],
            target_dialog_id=row["target_dialog_id"],
            target_title=row["target_title"],
            target_type=row["target_type"],
            send_mode=PhotoSendMode(row["send_mode"]),
            caption=row["caption"],
            status=PhotoBatchStatus(row["status"]),
            error=row["error"],
            created_at=_dt(row["created_at"]),
            last_run_at=_dt(row["last_run_at"]),
        )

    @staticmethod
    def _to_item(row: aiosqlite.Row) -> PhotoBatchItem:
        return PhotoBatchItem(
            id=row["id"],
            batch_id=row["batch_id"],
            phone=row["phone"],
            target_dialog_id=row["target_dialog_id"],
            target_title=row["target_title"],
            target_type=row["target_type"],
            file_paths=[str(x) for x in _json_loads_list(row["file_paths"])],
            send_mode=PhotoSendMode(row["send_mode"]),
            caption=row["caption"],
            schedule_at=_dt(row["schedule_at"]),
            status=PhotoBatchStatus(row["status"]),
            error=row["error"],
            telegram_message_ids=[int(x) for x in _json_loads_list(row["telegram_message_ids"])],
            created_at=_dt(row["created_at"]),
            started_at=_dt(row["started_at"]),
            completed_at=_dt(row["completed_at"]),
        )

    @staticmethod
    def _to_auto_job(row: aiosqlite.Row) -> PhotoAutoUploadJob:
        return PhotoAutoUploadJob(
            id=row["id"],
            phone=row["phone"],
            target_dialog_id=row["target_dialog_id"],
            target_title=row["target_title"],
            target_type=row["target_type"],
            folder_path=row["folder_path"],
            send_mode=PhotoSendMode(row["send_mode"]),
            caption=row["caption"],
            interval_minutes=row["interval_minutes"],
            is_active=bool(row["is_active"]),
            error=row["error"],
            last_run_at=_dt(row["last_run_at"]),
            last_seen_marker=row["last_seen_marker"],
            created_at=_dt(row["created_at"]),
        )

    async def create_batch(self, batch: PhotoBatch) -> int:
        cur = await self._db.execute(
            """
            INSERT INTO photo_batches (
                phone, target_dialog_id, target_title, target_type,
                send_mode, caption, status, error
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                batch.phone,
                batch.target_dialog_id,
                batch.target_title,
                batch.target_type,
                batch.send_mode.value,
                batch.caption,
                batch.status.value,
                batch.error,
            ),
        )
        await self._db.commit()
        return cur.lastrowid or 0

    async def update_batch(
        self,
        batch_id: int,
        *,
        status: PhotoBatchStatus | None = None,
        error: str | None = None,
        last_run_at: datetime | None = None,
    ) -> None:
        sets: list[str] = []
        params: list[object] = []
        if status is not None:
            sets.append("status = ?")
            params.append(status.value)
        if error is not None:
            sets.append("error = ?")
            params.append(error)
        if last_run_at is not None:
            sets.append("last_run_at = ?")
            params.append(last_run_at.astimezone(timezone.utc).isoformat())
        if not sets:
            return
        params.append(batch_id)
        await self._db.execute(
            f"UPDATE photo_batches SET {', '.join(sets)} WHERE id = ?",
            tuple(params),
        )
        await self._db.commit()

    async def get_batch(self, batch_id: int) -> PhotoBatch | None:
        cur = await self._db.execute("SELECT * FROM photo_batches WHERE id = ?", (batch_id,))
        row = await cur.fetchone()
        return self._to_batch(row) if row else None

    async def list_batches(self, limit: int = 50) -> list[PhotoBatch]:
        cur = await self._db.execute(
            "SELECT * FROM photo_batches ORDER BY id DESC LIMIT ?",
            (limit,),
        )
        return [self._to_batch(row) for row in await cur.fetchall()]

    async def create_item(self, item: PhotoBatchItem) -> int:
        cur = await self._db.execute(
            """
            INSERT INTO photo_batch_items (
                batch_id, phone, target_dialog_id, target_title, target_type,
                file_paths, send_mode, caption, schedule_at, status, error, telegram_message_ids
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                item.batch_id,
                item.phone,
                item.target_dialog_id,
                item.target_title,
                item.target_type,
                json.dumps(item.file_paths),
                item.send_mode.value,
                item.caption,
                item.schedule_at.astimezone(timezone.utc).isoformat() if item.schedule_at else None,
                item.status.value,
                item.error,
                json.dumps(item.telegram_message_ids),
            ),
        )
        await self._db.commit()
        return cur.lastrowid or 0

    async def get_item(self, item_id: int) -> PhotoBatchItem | None:
        cur = await self._db.execute("SELECT * FROM photo_batch_items WHERE id = ?", (item_id,))
        row = await cur.fetchone()
        return self._to_item(row) if row else None

    async def list_items(self, limit: int = 100) -> list[PhotoBatchItem]:
        cur = await self._db.execute(
            "SELECT * FROM photo_batch_items ORDER BY id DESC LIMIT ?",
            (limit,),
        )
        return [self._to_item(row) for row in await cur.fetchall()]

    async def list_items_for_batch(self, batch_id: int) -> list[PhotoBatchItem]:
        cur = await self._db.execute(
            "SELECT * FROM photo_batch_items WHERE batch_id = ? ORDER BY id ASC",
            (batch_id,),
        )
        return [self._to_item(row) for row in await cur.fetchall()]

    async def update_item(
        self,
        item_id: int,
        *,
        status: PhotoBatchStatus | None = None,
        error: str | None = None,
        telegram_message_ids: list[int] | None = None,
        started_at: datetime | None = None,
        completed_at: datetime | None = None,
    ) -> None:
        sets: list[str] = []
        params: list[object] = []
        if status is not None:
            sets.append("status = ?")
            params.append(status.value)
        if error is not None:
            sets.append("error = ?")
            params.append(error)
        if telegram_message_ids is not None:
            sets.append("telegram_message_ids = ?")
            params.append(json.dumps(telegram_message_ids))
        if started_at is not None:
            sets.append("started_at = ?")
            params.append(started_at.astimezone(timezone.utc).isoformat())
        if completed_at is not None:
            sets.append("completed_at = ?")
            params.append(completed_at.astimezone(timezone.utc).isoformat())
        if not sets:
            return
        params.append(item_id)
        await self._db.execute(
            f"UPDATE photo_batch_items SET {', '.join(sets)} WHERE id = ?",
            tuple(params),
        )
        await self._db.commit()

    async def cancel_item(self, item_id: int) -> bool:
        cur = await self._db.execute(
            """
            UPDATE photo_batch_items
            SET status = ?, completed_at = ?
            WHERE id = ? AND status IN (?, ?, ?)
            """,
            (
                PhotoBatchStatus.CANCELLED.value,
                datetime.now(timezone.utc).isoformat(),
                item_id,
                PhotoBatchStatus.PENDING.value,
                PhotoBatchStatus.SCHEDULED.value,
                PhotoBatchStatus.RUNNING.value,
            ),
        )
        await self._db.commit()
        return (cur.rowcount or 0) > 0

    async def claim_next_due_item(self, now: datetime) -> PhotoBatchItem | None:
        now_iso = now.astimezone(timezone.utc).isoformat()
        try:
            await self._db.execute("BEGIN IMMEDIATE")
            cur = await self._db.execute(
                """
                SELECT id FROM photo_batch_items
                WHERE status = ? AND (schedule_at IS NULL OR schedule_at <= ?)
                ORDER BY COALESCE(schedule_at, ''), id ASC
                LIMIT 1
                """,
                (PhotoBatchStatus.PENDING.value, now_iso),
            )
            row = await cur.fetchone()
            if row is None:
                await self._db.commit()
                return None
            item_id = row["id"]
            updated = await self._db.execute(
                """
                UPDATE photo_batch_items
                SET status = ?, started_at = ?, completed_at = NULL, error = NULL
                WHERE id = ? AND status = ?
                """,
                (
                    PhotoBatchStatus.RUNNING.value,
                    now_iso,
                    item_id,
                    PhotoBatchStatus.PENDING.value,
                ),
            )
            if (updated.rowcount or 0) == 0:
                await self._db.commit()
                return None
            cur = await self._db.execute("SELECT * FROM photo_batch_items WHERE id = ?", (item_id,))
            claimed = await cur.fetchone()
            await self._db.commit()
            return self._to_item(claimed) if claimed else None
        except Exception:
            await self._db.rollback()
            raise

    async def requeue_running_items_on_startup(self, now: datetime) -> int:
        cur = await self._db.execute(
            """
            UPDATE photo_batch_items
            SET status = ?, started_at = NULL, completed_at = NULL, error = ?
            WHERE status = ?
            """,
            (
                PhotoBatchStatus.PENDING.value,
                f"Recovered on startup at {now.astimezone(timezone.utc).isoformat()}",
                PhotoBatchStatus.RUNNING.value,
            ),
        )
        await self._db.commit()
        return cur.rowcount or 0

    async def create_auto_job(self, job: PhotoAutoUploadJob) -> int:
        cur = await self._db.execute(
            """
            INSERT INTO photo_auto_upload_jobs (
                phone, target_dialog_id, target_title, target_type, folder_path,
                send_mode, caption, interval_minutes, is_active,
                error, last_run_at, last_seen_marker
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                job.phone,
                job.target_dialog_id,
                job.target_title,
                job.target_type,
                job.folder_path,
                job.send_mode.value,
                job.caption,
                job.interval_minutes,
                1 if job.is_active else 0,
                job.error,
                job.last_run_at.astimezone(timezone.utc).isoformat() if job.last_run_at else None,
                job.last_seen_marker,
            ),
        )
        await self._db.commit()
        return cur.lastrowid or 0

    async def update_auto_job(
        self,
        job_id: int,
        *,
        folder_path: str | None = None,
        send_mode: PhotoSendMode | None = None,
        caption: str | None = None,
        interval_minutes: int | None = None,
        is_active: bool | None = None,
        error: str | None = None,
        last_run_at: datetime | None = None,
        last_seen_marker: str | None = None,
    ) -> None:
        sets: list[str] = []
        params: list[object] = []
        if folder_path is not None:
            sets.append("folder_path = ?")
            params.append(folder_path)
        if send_mode is not None:
            sets.append("send_mode = ?")
            params.append(send_mode.value)
        if caption is not None:
            sets.append("caption = ?")
            params.append(caption)
        if interval_minutes is not None:
            sets.append("interval_minutes = ?")
            params.append(interval_minutes)
        if is_active is not None:
            sets.append("is_active = ?")
            params.append(1 if is_active else 0)
        if error is not None:
            sets.append("error = ?")
            params.append(error)
        if last_run_at is not None:
            sets.append("last_run_at = ?")
            params.append(last_run_at.astimezone(timezone.utc).isoformat())
        if last_seen_marker is not None:
            sets.append("last_seen_marker = ?")
            params.append(last_seen_marker)
        if not sets:
            return
        params.append(job_id)
        await self._db.execute(
            f"UPDATE photo_auto_upload_jobs SET {', '.join(sets)} WHERE id = ?",
            tuple(params),
        )
        await self._db.commit()

    async def get_auto_job(self, job_id: int) -> PhotoAutoUploadJob | None:
        cur = await self._db.execute("SELECT * FROM photo_auto_upload_jobs WHERE id = ?", (job_id,))
        row = await cur.fetchone()
        return self._to_auto_job(row) if row else None

    async def list_auto_jobs(self, active_only: bool = False) -> list[PhotoAutoUploadJob]:
        sql = "SELECT * FROM photo_auto_upload_jobs"
        params: tuple[object, ...] = ()
        if active_only:
            sql += " WHERE is_active = 1"
        sql += " ORDER BY id DESC"
        cur = await self._db.execute(sql, params)
        return [self._to_auto_job(row) for row in await cur.fetchall()]

    async def delete_auto_job(self, job_id: int) -> None:
        await self._db.execute("DELETE FROM photo_auto_upload_jobs WHERE id = ?", (job_id,))
        await self._db.execute("DELETE FROM photo_auto_upload_files WHERE job_id = ?", (job_id,))
        await self._db.commit()

    async def has_sent_auto_file(self, job_id: int, file_path: str) -> bool:
        cur = await self._db.execute(
            "SELECT 1 FROM photo_auto_upload_files WHERE job_id = ? AND file_path = ?",
            (job_id, file_path),
        )
        return bool(await cur.fetchone())

    async def mark_auto_file_sent(self, job_id: int, file_path: str) -> None:
        await self._db.execute(
            """
            INSERT OR IGNORE INTO photo_auto_upload_files (job_id, file_path, sent_at)
            VALUES (?, ?, ?)
            """,
            (job_id, file_path, datetime.now(timezone.utc).isoformat()),
        )
        await self._db.commit()
