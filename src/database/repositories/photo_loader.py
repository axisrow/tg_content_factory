"""Репозиторий загрузчика фото: ручные батчи и авто-загрузка из папки.

Доступ через `db.repos.photo_loader`. Обслуживает три таблицы:
`photo_batches` (батч — общая цель/режим отправки), `photo_batch_items`
(конкретные отправки с расписанием и статусом) и `photo_auto_upload_jobs`
(периодическая авто-отправка новых файлов из папки, с леджером уже отправленных
в `photo_auto_upload_files`). Захват готового item на отправку — атомарный
(UPDATE…RETURNING), чтобы две корутины не отправили одно фото дважды.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import TYPE_CHECKING

import aiosqlite

from src.database.pool import ReadConnection
from src.models import (
    PhotoAutoUploadJob,
    PhotoBatch,
    PhotoBatchItem,
    PhotoBatchStatus,
    PhotoSendMode,
)
from src.utils.datetime import parse_datetime
from src.utils.json import safe_json_dumps, safe_json_loads_list


def _json_loads_list(raw: str | None) -> list:
    return safe_json_loads_list(raw)


if TYPE_CHECKING:
    from src.database.facade import Database


class PhotoLoaderRepository:
    """CRUD фото-батчей, их элементов и заданий авто-загрузки из папки."""

    def __init__(
        self,
        db: ReadConnection,
        *,
        database: "Database | None" = None,
    ):
        self._db = db
        self._database = database

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
            created_at=parse_datetime(row["created_at"]),
            last_run_at=parse_datetime(row["last_run_at"]),
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
            schedule_at=parse_datetime(row["schedule_at"]),
            status=PhotoBatchStatus(row["status"]),
            error=row["error"],
            telegram_message_ids=[int(x) for x in _json_loads_list(row["telegram_message_ids"])],
            created_at=parse_datetime(row["created_at"]),
            started_at=parse_datetime(row["started_at"]),
            completed_at=parse_datetime(row["completed_at"]),
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
            last_run_at=parse_datetime(row["last_run_at"]),
            last_seen_marker=row["last_seen_marker"],
            created_at=parse_datetime(row["created_at"]),
        )

    async def create_batch(self, batch: PhotoBatch) -> int:
        """Создать фото-батч (общая цель/режим отправки); вернуть его id."""
        assert self._database is not None, (
            "PhotoLoaderRepository.create_batch requires a Database reference"
        )
        cur = await self._database.execute_write(
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
        return cur.lastrowid or 0

    async def update_batch(
        self,
        batch_id: int,
        *,
        status: PhotoBatchStatus | None = None,
        error: str | None = None,
        last_run_at: datetime | None = None,
    ) -> None:
        """Частично обновить батч (статус/ошибку/время запуска); ни одного поля — no-op."""
        assert self._database is not None, (
            "PhotoLoaderRepository.update_batch requires a Database reference"
        )
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
        await self._database.execute_write(
            f"UPDATE photo_batches SET {', '.join(sets)} WHERE id = ?",
            tuple(params),
        )

    async def publish_batch(self, batch_id: int) -> int:
        """Atomically move HELD items in a batch into the PENDING queue; return affected rows.

        Both the items-flip and the batch-status update run inside one transaction so
        the photo_due cron cannot claim a just-published item between the two writes and
        then have ``_sync_batch_status`` flip the batch to RUNNING only for the second
        write to clobber it back to PENDING. Idempotent: re-publishing a batch with no
        HELD items is a no-op (UPDATE affects zero rows)."""
        assert self._database is not None, (
            "PhotoLoaderRepository.publish_batch requires a Database reference"
        )
        async with self._database.transaction() as conn:
            cur = await conn.execute(
                """
                UPDATE photo_batch_items
                SET status = ?, error = NULL
                WHERE batch_id = ? AND status = ?
                """,
                (
                    PhotoBatchStatus.PENDING.value,
                    batch_id,
                    PhotoBatchStatus.HELD.value,
                ),
            )
            count = cur.rowcount or 0
            if count:
                await conn.execute(
                    """
                    UPDATE photo_batches SET status = ?, error = ?
                    WHERE id = ?
                    """,
                    (PhotoBatchStatus.PENDING.value, "", batch_id),
                )
        return count

    async def get_batch(self, batch_id: int) -> PhotoBatch | None:
        """Один батч по id, либо ``None``."""
        cur = await self._db.execute("SELECT * FROM photo_batches WHERE id = ?", (batch_id,))
        row = await cur.fetchone()
        return self._to_batch(row) if row else None

    async def list_batches(self, limit: int = 50) -> list[PhotoBatch]:
        """Последние ``limit`` батчей, новые первыми."""
        cur = await self._db.execute(
            "SELECT * FROM photo_batches ORDER BY id DESC LIMIT ?",
            (limit,),
        )
        return [self._to_batch(row) for row in await cur.fetchall()]

    async def create_item(self, item: PhotoBatchItem) -> int:
        """Создать элемент батча (одна отправка с файлами и опциональным расписанием); вернуть id."""
        assert self._database is not None, (
            "PhotoLoaderRepository.create_item requires a Database reference"
        )
        cur = await self._database.execute_write(
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
                safe_json_dumps(item.file_paths),
                item.send_mode.value,
                item.caption,
                item.schedule_at.astimezone(timezone.utc).isoformat() if item.schedule_at else None,
                item.status.value,
                item.error,
                safe_json_dumps(item.telegram_message_ids),
            ),
        )
        return cur.lastrowid or 0

    async def get_item(self, item_id: int) -> PhotoBatchItem | None:
        """Один элемент батча по id, либо ``None``."""
        cur = await self._db.execute("SELECT * FROM photo_batch_items WHERE id = ?", (item_id,))
        row = await cur.fetchone()
        return self._to_item(row) if row else None

    async def list_items(self, limit: int = 100) -> list[PhotoBatchItem]:
        """Последние ``limit`` элементов по всем батчам, новые первыми."""
        cur = await self._db.execute(
            "SELECT * FROM photo_batch_items ORDER BY id DESC LIMIT ?",
            (limit,),
        )
        return [self._to_item(row) for row in await cur.fetchall()]

    async def list_items_for_batch(self, batch_id: int, limit: int | None = None) -> list[PhotoBatchItem]:
        """Элементы одного батча по возрастанию id (порядок отправки); ``limit`` ограничивает выборку."""
        sql = "SELECT * FROM photo_batch_items WHERE batch_id = ? ORDER BY id ASC"
        params: tuple[object, ...] = (batch_id,)
        if limit is not None:
            sql += " LIMIT ?"
            params += (limit,)
        cur = await self._db.execute(sql, params)
        return [self._to_item(row) for row in await cur.fetchall()]

    async def count_items_by_batch_status(self, batch_id: int) -> dict[PhotoBatchStatus, int]:
        """Count batch items grouped by status for live batch progress read-models."""
        cur = await self._db.execute(
            """
            SELECT status, COUNT(*) AS item_count
            FROM photo_batch_items
            WHERE batch_id = ?
            GROUP BY status
            """,
            (batch_id,),
        )
        return {
            PhotoBatchStatus(row["status"]): int(row["item_count"])
            for row in await cur.fetchall()
        }

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
        """Частично обновить элемент батча (статус, ошибка, id отправленных сообщений, тайминги); пусто — no-op."""
        assert self._database is not None, (
            "PhotoLoaderRepository.update_item requires a Database reference"
        )
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
            params.append(safe_json_dumps(telegram_message_ids))
        if started_at is not None:
            sets.append("started_at = ?")
            params.append(started_at.astimezone(timezone.utc).isoformat())
        if completed_at is not None:
            sets.append("completed_at = ?")
            params.append(completed_at.astimezone(timezone.utc).isoformat())
        if not sets:
            return
        params.append(item_id)
        await self._database.execute_write(
            f"UPDATE photo_batch_items SET {', '.join(sets)} WHERE id = ?",
            tuple(params),
        )

    async def cancel_item(self, item_id: int) -> bool:
        """Отменить ещё не завершённый элемент (held/pending/scheduled/running); вернуть ``True``, если отменили."""
        assert self._database is not None, (
            "PhotoLoaderRepository.cancel_item requires a Database reference"
        )
        cur = await self._database.execute_write(
            """
            UPDATE photo_batch_items
            SET status = ?, completed_at = ?
            WHERE id = ? AND status IN (?, ?, ?, ?)
            """,
            (
                PhotoBatchStatus.CANCELLED.value,
                datetime.now(timezone.utc).isoformat(),
                item_id,
                PhotoBatchStatus.HELD.value,
                PhotoBatchStatus.PENDING.value,
                PhotoBatchStatus.SCHEDULED.value,
                PhotoBatchStatus.RUNNING.value,
            ),
        )
        return (cur.rowcount or 0) > 0

    async def claim_next_due_item(
        self, now: datetime, *, item_id: int | None = None
    ) -> PhotoBatchItem | None:
        """Atomically claim a PENDING due item, transitioning it to RUNNING.

        Without ``item_id`` the earliest due item is picked; with ``item_id`` only
        that specific item is claimed (and only if it is itself due). Returns the
        claimed row, or ``None`` if nothing matched.
        """
        assert self._database is not None, (
            "PhotoLoaderRepository.claim_next_due_item requires a Database reference"
        )
        now_iso = now.astimezone(timezone.utc).isoformat()
        async with self._database.transaction() as conn:
            if item_id is None:
                # Pick which row to claim; the targeted variant already knows its id.
                cur = await conn.execute(
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
                    return None
                target_id = row["id"]
            else:
                target_id = int(item_id)
            cur = await conn.execute(
                """
                UPDATE photo_batch_items
                SET status = ?, started_at = ?, completed_at = NULL, error = NULL
                WHERE id = ? AND status = ? AND (schedule_at IS NULL OR schedule_at <= ?)
                RETURNING *
                """,
                (
                    PhotoBatchStatus.RUNNING.value,
                    now_iso,
                    target_id,
                    PhotoBatchStatus.PENDING.value,
                    now_iso,
                ),
            )
            claimed = await cur.fetchone()
        return self._to_item(claimed) if claimed else None

    async def count_due_items(self, now: datetime, *, item_id: int | None = None) -> int:
        """Count PENDING items due at ``now``; optionally narrow to one item id."""
        now_iso = now.astimezone(timezone.utc).isoformat()
        sql = """
            SELECT COUNT(*) AS item_count
            FROM photo_batch_items
            WHERE status = ? AND (schedule_at IS NULL OR schedule_at <= ?)
        """
        params: list[object] = [PhotoBatchStatus.PENDING.value, now_iso]
        if item_id is not None:
            sql += " AND id = ?"
            params.append(int(item_id))
        cur = await self._db.execute(sql, tuple(params))
        row = await cur.fetchone()
        return int(row["item_count"]) if row else 0

    async def requeue_running_items_on_startup(self, now: datetime) -> int:
        """На старте вернуть зависшие RUNNING-элементы в PENDING (авто-восстановление после сбоя); вернуть число."""
        assert self._database is not None, (
            "PhotoLoaderRepository.requeue_running_items_on_startup requires a Database reference"
        )
        cur = await self._database.execute_write(
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
        return cur.rowcount or 0

    async def create_auto_job(self, job: PhotoAutoUploadJob) -> int:
        """Создать задание авто-загрузки новых файлов из папки по интервалу; вернуть id."""
        assert self._database is not None, (
            "PhotoLoaderRepository.create_auto_job requires a Database reference"
        )
        cur = await self._database.execute_write(
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
        """Частично обновить задание авто-загрузки (папка, режим, интервал, активность, маркер…); пусто — no-op."""
        assert self._database is not None, (
            "PhotoLoaderRepository.update_auto_job requires a Database reference"
        )
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
        await self._database.execute_write(
            f"UPDATE photo_auto_upload_jobs SET {', '.join(sets)} WHERE id = ?",
            tuple(params),
        )

    async def get_auto_job(self, job_id: int) -> PhotoAutoUploadJob | None:
        """Одно задание авто-загрузки по id, либо ``None``."""
        cur = await self._db.execute("SELECT * FROM photo_auto_upload_jobs WHERE id = ?", (job_id,))
        row = await cur.fetchone()
        return self._to_auto_job(row) if row else None

    async def list_auto_jobs(self, active_only: bool = False) -> list[PhotoAutoUploadJob]:
        """Задания авто-загрузки, новые первыми; с ``active_only`` — только активные."""
        sql = "SELECT * FROM photo_auto_upload_jobs"
        params: tuple[object, ...] = ()
        if active_only:
            sql += " WHERE is_active = 1"
        sql += " ORDER BY id DESC"
        cur = await self._db.execute(sql, params)
        return [self._to_auto_job(row) for row in await cur.fetchall()]

    async def delete_auto_job(self, job_id: int) -> None:
        """Удалить задание авто-загрузки вместе с его леджером отправленных файлов (одной транзакцией).

        Леджер `photo_auto_upload_files` ссылается на задание внешним ключом без
        ON DELETE CASCADE, а соединение работает с `PRAGMA foreign_keys=ON` —
        поэтому child-строки удаляются ПЕРВЫМИ, иначе DELETE родителя падает с
        «FOREIGN KEY constraint failed» (#1134).
        """
        assert self._database is not None, (
            "PhotoLoaderRepository.delete_auto_job requires a Database reference"
        )
        async with self._database.transaction() as conn:
            await conn.execute("DELETE FROM photo_auto_upload_files WHERE job_id = ?", (job_id,))
            await conn.execute("DELETE FROM photo_auto_upload_jobs WHERE id = ?", (job_id,))

    async def has_sent_auto_file(self, job_id: int, file_path: str) -> bool:
        """Был ли файл уже отправлен этим заданием (проверка леджера перед отправкой)."""
        cur = await self._db.execute(
            "SELECT 1 FROM photo_auto_upload_files WHERE job_id = ? AND file_path = ?",
            (job_id, file_path),
        )
        return bool(await cur.fetchone())

    async def mark_auto_file_sent(self, job_id: int, file_path: str) -> None:
        """Отметить файл отправленным в леджере задания (идемпотентно, INSERT OR IGNORE)."""
        assert self._database is not None, (
            "PhotoLoaderRepository.mark_auto_file_sent requires a Database reference"
        )
        await self._database.execute_write(
            """
            INSERT OR IGNORE INTO photo_auto_upload_files (job_id, file_path, sent_at)
            VALUES (?, ?, ?)
            """,
            (job_id, file_path, datetime.now(timezone.utc).isoformat()),
        )
