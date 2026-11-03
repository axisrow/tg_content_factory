"""Хранилище движка посева диалогов (#1755, эпик #1751): треды и реплики.

Переходы статусов охраняются WHERE-гардами (``UPDATE ... WHERE id = ? AND
status IN (легальные источники)``) — по образцу guarded-методов #1307 и
``cancel_item``/``claim_next_due_item`` photo-loader: невалидный переход не
бросает исключение, а возвращает ``False`` и не пишет строку, поэтому гонка
двух потребителей безопасна — второй по гарду получает 0 строк. Прямых
``commit()`` нет: все записи идут через ``execute_write``/``transaction``.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from typing import TYPE_CHECKING

import aiosqlite

from tg_agent.database.pool import ReadConnection
from tg_agent.models import (
    SeedingMessage,
    SeedingMessageStatus,
    SeedingThread,
    SeedingThreadProgress,
    SeedingThreadStatus,
)
from tg_agent.utils.datetime import parse_datetime
from tg_agent.utils.json import safe_json_dumps

if TYPE_CHECKING:
    from tg_agent.database.facade import Database


def _iso(dt: datetime | None) -> str | None:
    return dt.astimezone(timezone.utc).isoformat() if dt else None


# Легальные переходы: цель -> множество статусов, из которых она достижима.
# Терминальные статусы (done/cancelled, sent/cancelled) не фигурируют как
# ИСТОЧНИКИ: переход из них не матчит WHERE-гард и даёт 0 строк (False с
# обращением к БД). Цель вне карты (например, обратно в draft) отсекается
# раньше, без обращения к БД.
_THREAD_SOURCES: dict[SeedingThreadStatus, frozenset[SeedingThreadStatus]] = {
    SeedingThreadStatus.SCHEDULED: frozenset({SeedingThreadStatus.DRAFT}),
    SeedingThreadStatus.ACTIVE: frozenset({SeedingThreadStatus.SCHEDULED}),
    SeedingThreadStatus.DONE: frozenset({SeedingThreadStatus.ACTIVE}),
    SeedingThreadStatus.CANCELLED: frozenset({
        SeedingThreadStatus.DRAFT,
        SeedingThreadStatus.SCHEDULED,
        SeedingThreadStatus.ACTIVE,
    }),
}

_MESSAGE_SOURCES: dict[SeedingMessageStatus, frozenset[SeedingMessageStatus]] = {
    SeedingMessageStatus.APPROVED: frozenset({SeedingMessageStatus.DRAFT}),
    SeedingMessageStatus.SCHEDULED: frozenset({
        SeedingMessageStatus.DRAFT,
        SeedingMessageStatus.APPROVED,
        # Перепланирование после неудачной отправки.
        SeedingMessageStatus.FAILED,
    }),
    # Claim публикатора (#1757): RPC в полёте; повторный due-прогон не может
    # захватить строку повторно — она уже не в `scheduled`.
    SeedingMessageStatus.SENDING: frozenset({SeedingMessageStatus.SCHEDULED}),
    SeedingMessageStatus.SENT: frozenset({SeedingMessageStatus.SCHEDULED, SeedingMessageStatus.SENDING}),
    SeedingMessageStatus.FAILED: frozenset({SeedingMessageStatus.SCHEDULED, SeedingMessageStatus.SENDING}),
    SeedingMessageStatus.CANCELLED: frozenset({
        SeedingMessageStatus.DRAFT,
        SeedingMessageStatus.APPROVED,
        SeedingMessageStatus.SCHEDULED,
        SeedingMessageStatus.FAILED,
    }),
}


class SeedingRepository:
    """CRUD тредов/реплик посева с guarded-переходами статусов и due-выборкой."""

    def __init__(
        self,
        db: ReadConnection,
        *,
        database: "Database | None" = None,
    ):
        self._db = db
        self._database = database

    # ---- threads -----------------------------------------------------------

    @staticmethod
    def _to_thread(row: aiosqlite.Row) -> SeedingThread:
        return SeedingThread(
            id=row["id"],
            chat_channel_id=row["chat_channel_id"],
            personas=json.loads(row["personas_json"]) if row["personas_json"] else {},
            status=SeedingThreadStatus(row["status"]),
            composer_run_ref=row["composer_run_ref"],
            created_at=parse_datetime(row["created_at"]),
            updated_at=parse_datetime(row["updated_at"]),
        )

    @staticmethod
    def _to_message(row: aiosqlite.Row) -> SeedingMessage:
        return SeedingMessage(
            id=row["id"],
            thread_id=row["thread_id"],
            position=row["position"],
            account_phone=row["account_phone"],
            persona_name=row["persona_name"],
            text=row["text"],
            reply_to_position=row["reply_to_position"],
            planned_at=parse_datetime(row["planned_at"]),
            sent_at=parse_datetime(row["sent_at"]),
            tg_message_id=row["tg_message_id"],
            status=SeedingMessageStatus(row["status"]),
            error_text=row["error_text"],
            created_at=parse_datetime(row["created_at"]),
            updated_at=parse_datetime(row["updated_at"]),
        )

    @staticmethod
    def _message_insert_params(msg: SeedingMessage) -> tuple:
        """Параметры INSERT без ``thread_id`` (он подставляется отдельным слотом)."""
        return (
            msg.position,
            msg.account_phone,
            msg.persona_name,
            msg.text,
            msg.reply_to_position,
            _iso(msg.planned_at),
            msg.status.value,
        )

    async def create_thread(self, thread: SeedingThread) -> int:
        """Создать тред посева; вернуть его id."""
        assert self._database is not None, (
            "SeedingRepository.create_thread requires a Database reference"
        )
        cur = await self._database.execute_write(
            """
            INSERT INTO seeding_threads (chat_channel_id, personas_json, status, composer_run_ref)
            VALUES (?, ?, ?, ?)
            """,
            (
                thread.chat_channel_id,
                safe_json_dumps(thread.personas),
                thread.status.value,
                thread.composer_run_ref,
            ),
        )
        return cur.lastrowid or 0

    async def create_messages(self, thread_id: int, messages: list[SeedingMessage]) -> list[int]:
        """Атомарно вставить сценарий реплик в тред; вернуть их id в порядке вставки.

        Один транзакционный блок: дубль ``position`` (UNIQUE(thread_id, position))
        или любая другая ошибка откатывает весь батч — частичный сценарий не
        должен переживать неудачную запись (#1601-паттерн photo-loader).
        ``msg.thread_id`` игнорируется, репликам проставляется реальный ``thread_id``.
        """
        assert self._database is not None, (
            "SeedingRepository.create_messages requires a Database reference"
        )
        ids: list[int] = []
        async with self._database.transaction() as conn:
            for msg in messages:
                cur = await conn.execute(
                    """
                    INSERT INTO seeding_messages (
                        thread_id, position, account_phone, persona_name, text,
                        reply_to_position, planned_at, status
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (thread_id, *self._message_insert_params(msg)),
                )
                ids.append(cur.lastrowid or 0)
        return ids

    async def get_thread(self, thread_id: int) -> SeedingThread | None:
        cur = await self._db.execute("SELECT * FROM seeding_threads WHERE id = ?", (thread_id,))
        row = await cur.fetchone()
        return self._to_thread(row) if row else None

    async def list_threads(
        self, limit: int = 50, *, status: SeedingThreadStatus | None = None
    ) -> list[SeedingThread]:
        """Треды свежими первыми; ``status`` фильтрует в SQL ДО LIMIT.

        Пост-фильтрация по уже срезанному LIMIT молча прячет старые треды
        запрошенного статуса (находка ревью #1763).
        """
        if status is None:
            cur = await self._db.execute(
                "SELECT * FROM seeding_threads ORDER BY id DESC LIMIT ?", (limit,)
            )
        else:
            cur = await self._db.execute(
                "SELECT * FROM seeding_threads WHERE status = ? ORDER BY id DESC LIMIT ?",
                (status.value, limit),
            )
        return [self._to_thread(row) for row in await cur.fetchall()]

    async def list_threads_for_chat(self, chat_channel_id: int) -> list[SeedingThread]:
        cur = await self._db.execute(
            "SELECT * FROM seeding_threads WHERE chat_channel_id = ? ORDER BY id DESC",
            (chat_channel_id,),
        )
        return [self._to_thread(row) for row in await cur.fetchall()]

    async def set_thread_status(self, thread_id: int, status: SeedingThreadStatus) -> bool:
        """Guarded-переход статуса треда; ``False`` — нет треда или переход нелегален."""
        assert self._database is not None, (
            "SeedingRepository.set_thread_status requires a Database reference"
        )
        sources = _THREAD_SOURCES.get(status)
        if not sources:
            return False
        placeholders = ", ".join("?" for _ in sources)
        cur = await self._database.execute_write(
            f"""
            UPDATE seeding_threads
            SET status = ?, updated_at = ?
            WHERE id = ? AND status IN ({placeholders})
            """,
            (
                status.value,
                datetime.now(timezone.utc).isoformat(),
                thread_id,
                *(s.value for s in sources),
            ),
        )
        return (cur.rowcount or 0) > 0

    async def approve_thread(self, thread_id: int) -> int | None:
        """Атомарный апрув владельца (#1757): тред draft→scheduled + все draft-реплики → approved.

        Один транзакционный блок: либо оба перехода, либо ни одного —
        «наполовину апрувнутый» тред недопустим. ``None`` — тред не найден
        или не в draft; иначе число переведённых реплик.
        """
        assert self._database is not None, (
            "SeedingRepository.approve_thread requires a Database reference"
        )
        now_iso = datetime.now(timezone.utc).isoformat()
        async with self._database.transaction() as conn:
            cur = await conn.execute(
                "UPDATE seeding_threads SET status = ?, updated_at = ? WHERE id = ? AND status = ?",
                (
                    SeedingThreadStatus.SCHEDULED.value,
                    now_iso,
                    thread_id,
                    SeedingThreadStatus.DRAFT.value,
                ),
            )
            if (cur.rowcount or 0) == 0:
                return None
            cur = await conn.execute(
                "UPDATE seeding_messages SET status = ?, updated_at = ? "
                "WHERE thread_id = ? AND status = ?",
                (
                    SeedingMessageStatus.APPROVED.value,
                    now_iso,
                    thread_id,
                    SeedingMessageStatus.DRAFT.value,
                ),
            )
            return cur.rowcount or 0

    async def cancel_thread(self, thread_id: int) -> tuple[bool, int]:
        """Отмена треда (#1757): тред → cancelled (из draft/scheduled/active),
        реплики draft/approved/scheduled/failed → cancelled одним блоком.

        Реплики в ``sending``/``sent`` не трогаются: отправка в полёте или уже
        случилась — отменить факт нельзя. ``False`` — тред не найден или уже
        терминален (done/cancelled).
        """
        assert self._database is not None, (
            "SeedingRepository.cancel_thread requires a Database reference"
        )
        now_iso = datetime.now(timezone.utc).isoformat()
        async with self._database.transaction() as conn:
            cur = await conn.execute(
                "UPDATE seeding_threads SET status = ?, updated_at = ? "
                "WHERE id = ? AND status IN (?, ?, ?)",
                (
                    SeedingThreadStatus.CANCELLED.value,
                    now_iso,
                    thread_id,
                    SeedingThreadStatus.DRAFT.value,
                    SeedingThreadStatus.SCHEDULED.value,
                    SeedingThreadStatus.ACTIVE.value,
                ),
            )
            if (cur.rowcount or 0) == 0:
                return False, 0
            cur = await conn.execute(
                "UPDATE seeding_messages SET status = ?, updated_at = ? "
                "WHERE thread_id = ? AND status IN (?, ?, ?, ?)",
                (
                    SeedingMessageStatus.CANCELLED.value,
                    now_iso,
                    thread_id,
                    SeedingMessageStatus.DRAFT.value,
                    SeedingMessageStatus.APPROVED.value,
                    SeedingMessageStatus.SCHEDULED.value,
                    SeedingMessageStatus.FAILED.value,
                ),
            )
            return True, cur.rowcount or 0

    # ---- messages ----------------------------------------------------------

    async def get_message(self, message_id: int) -> SeedingMessage | None:
        cur = await self._db.execute("SELECT * FROM seeding_messages WHERE id = ?", (message_id,))
        row = await cur.fetchone()
        return self._to_message(row) if row else None

    async def list_messages_for_thread(self, thread_id: int) -> list[SeedingMessage]:
        """Реплики треда по возрастанию position (порядок диалога)."""
        cur = await self._db.execute(
            "SELECT * FROM seeding_messages WHERE thread_id = ? ORDER BY position ASC",
            (thread_id,),
        )
        return [self._to_message(row) for row in await cur.fetchall()]

    async def list_due_messages(self, now: datetime, limit: int = 10) -> list[SeedingMessage]:
        """Реплики для публикатора: ``scheduled`` с наступившим ``planned_at``.

        Только выборка (двойная отправка исключается guarded-записью исхода:
        второй потребитель по тому же WHERE-гарду получит 0 строк).
        """
        now_iso = now.astimezone(timezone.utc).isoformat()
        cur = await self._db.execute(
            """
            SELECT * FROM seeding_messages
            WHERE status = ? AND planned_at IS NOT NULL AND planned_at <= ?
            ORDER BY planned_at ASC, id ASC
            LIMIT ?
            """,
            (SeedingMessageStatus.SCHEDULED.value, now_iso, limit),
        )
        return [self._to_message(row) for row in await cur.fetchall()]

    async def set_message_status(
        self,
        message_id: int,
        status: SeedingMessageStatus,
        *,
        error_text: str | None = None,
        tg_message_id: int | None = None,
        sent_at: datetime | None = None,
    ) -> bool:
        """Guarded-переход статуса реплики; ``False`` — нет реплики или переход нелегален.

        Для ``SENT`` обязателен ``tg_message_id`` (факт отправки без id сообщения
        невоспроизводим); ``sent_at`` по умолчанию — сейчас. ``error_text``
        пишется только если передан; при перепланировании (``→ SCHEDULED``)
        без нового текста устаревшая ошибка сбрасывается, при ``failed →
        cancelled`` сохраняется как улика.
        """
        assert self._database is not None, (
            "SeedingRepository.set_message_status requires a Database reference"
        )
        if status is SeedingMessageStatus.SENT and tg_message_id is None:
            raise ValueError("set_message_status(SENT) requires tg_message_id")
        sources = _MESSAGE_SOURCES.get(status)
        if not sources:
            return False
        sets = ["status = ?", "updated_at = ?"]
        params: list[object] = [
            status.value,
            datetime.now(timezone.utc).isoformat(),
        ]
        if status is SeedingMessageStatus.SENT:
            sets.append("sent_at = ?")
            params.append(_iso(sent_at or datetime.now(timezone.utc)))
            sets.append("tg_message_id = ?")
            params.append(tg_message_id)
        if error_text is not None:
            sets.append("error_text = ?")
            params.append(error_text)
        elif status is SeedingMessageStatus.SCHEDULED:
            sets.append("error_text = NULL")
        placeholders = ", ".join("?" for _ in sources)
        params.append(message_id)
        params.extend(s.value for s in sources)
        cur = await self._database.execute_write(
            f"""
            UPDATE seeding_messages
            SET {", ".join(sets)}
            WHERE id = ? AND status IN ({placeholders})
            """,
            tuple(params),
        )
        return (cur.rowcount or 0) > 0

    async def get_message_by_position(self, thread_id: int, position: int) -> SeedingMessage | None:
        cur = await self._db.execute(
            "SELECT * FROM seeding_messages WHERE thread_id = ? AND position = ?",
            (thread_id, position),
        )
        row = await cur.fetchone()
        return self._to_message(row) if row else None

    async def claim_next_due_message(self, now: datetime) -> SeedingMessage | None:
        """Атомарный claim самой ранней due-реплики (#1757): scheduled → sending.

        Один транзакционный блок SELECT+UPDATE…WHERE status='scheduled': гонка
        двух потребителей исключена — второй по тому же гарду получит следующую
        строку или ``None``, повторная отправка невозможна. Реплика без
        ``planned_at`` не берётся; порядок — ``planned_at`` ASC, id ASC
        (диалог идёт по расписанию).
        """
        assert self._database is not None, (
            "SeedingRepository.claim_next_due_message requires a Database reference"
        )
        now_iso = now.astimezone(timezone.utc).isoformat()
        async with self._database.transaction() as conn:
            cur = await conn.execute(
                """
                SELECT id FROM seeding_messages
                WHERE status = ? AND planned_at IS NOT NULL AND planned_at <= ?
                ORDER BY planned_at ASC, id ASC
                LIMIT 1
                """,
                (SeedingMessageStatus.SCHEDULED.value, now_iso),
            )
            row = await cur.fetchone()
            if row is None:
                return None
            cur = await conn.execute(
                """
                UPDATE seeding_messages
                SET status = ?, updated_at = ?
                WHERE id = ? AND status = ? AND planned_at IS NOT NULL AND planned_at <= ?
                RETURNING *
                """,
                (
                    SeedingMessageStatus.SENDING.value,
                    now_iso,
                    row["id"],
                    SeedingMessageStatus.SCHEDULED.value,
                    now_iso,
                ),
            )
            claimed = await cur.fetchone()
        return self._to_message(claimed) if claimed else None

    async def schedule_thread_messages(self, thread_id: int) -> int:
        """approved → scheduled для всех реплик треда с ``planned_at``; вернуть число."""
        assert self._database is not None, (
            "SeedingRepository.schedule_thread_messages requires a Database reference"
        )
        cur = await self._database.execute_write(
            "UPDATE seeding_messages SET status = ?, updated_at = ? "
            "WHERE thread_id = ? AND status = ? AND planned_at IS NOT NULL",
            (
                SeedingMessageStatus.SCHEDULED.value,
                datetime.now(timezone.utc).isoformat(),
                thread_id,
                SeedingMessageStatus.APPROVED.value,
            ),
        )
        return cur.rowcount or 0

    async def count_account_messages_by_day(
        self, phone: str, *, exclude_thread_id: int | None = None
    ) -> dict[str, int]:
        """Реплики аккаунта по UTC-дням (status in approved/scheduled/sent).

        День = первые 10 символов ISO ``planned_at``/``sent_at`` (UTC при записи).
        ``exclude_thread_id`` исключает тред планируемого сценария — валидатор
        рельсов прибавляет его вклад сам.
        """
        sql = (
            "SELECT COALESCE(substr(planned_at, 1, 10), substr(sent_at, 1, 10)) AS day, "
            "COUNT(*) AS n FROM seeding_messages "
            "WHERE account_phone = ? AND COALESCE(planned_at, sent_at) IS NOT NULL AND status IN (?, ?, ?)"
        )
        params: list[object] = [
            phone,
            SeedingMessageStatus.APPROVED.value,
            SeedingMessageStatus.SCHEDULED.value,
            SeedingMessageStatus.SENT.value,
        ]
        if exclude_thread_id is not None:
            sql += " AND thread_id != ?"
            params.append(exclude_thread_id)
        sql += " GROUP BY day"
        cur = await self._db.execute(sql, tuple(params))
        return {row["day"]: int(row["n"]) for row in await cur.fetchall()}

    async def fail_stuck_sending(self, error_text: str) -> int:
        """reconcile старта воркера (#1757): все ``sending`` → ``failed``.

        Воркер умер с RPC в полёте: исход неизвестен, автоматическая повторная
        отправка могла бы задублировать сообщение — реплика честно падает с
        ``error_text``, владелец сверяет чат и перепланирует осознанно.
        """
        assert self._database is not None, (
            "SeedingRepository.fail_stuck_sending requires a Database reference"
        )
        now_iso = datetime.now(timezone.utc).isoformat()
        cur = await self._database.execute_write(
            "UPDATE seeding_messages SET status = ?, updated_at = ?, error_text = ? WHERE status = ?",
            (
                SeedingMessageStatus.FAILED.value,
                now_iso,
                error_text,
                SeedingMessageStatus.SENDING.value,
            ),
        )
        return cur.rowcount or 0

    async def earliest_scheduled_planned_at(self) -> datetime | None:
        """Ближайший ``planned_at`` среди scheduled (глобально) — для цепочки SEED_DUE."""
        cur = await self._db.execute(
            "SELECT MIN(planned_at) AS next_at FROM seeding_messages WHERE status = ?",
            (SeedingMessageStatus.SCHEDULED.value,),
        )
        row = await cur.fetchone()
        return parse_datetime(row["next_at"]) if row and row["next_at"] else None

    # ---- агрегаты ----------------------------------------------------------

    async def get_thread_progress(self, thread_id: int) -> SeedingThreadProgress:
        """sent/total (+failed/cancelled) треда одним GROUP BY по статусам."""
        cur = await self._db.execute(
            """
            SELECT COUNT(*) AS total,
                   COALESCE(SUM(status = ?), 0) AS sent,
                   COALESCE(SUM(status = ?), 0) AS failed,
                   COALESCE(SUM(status = ?), 0) AS cancelled
            FROM seeding_messages
            WHERE thread_id = ?
            """,
            (
                SeedingMessageStatus.SENT.value,
                SeedingMessageStatus.FAILED.value,
                SeedingMessageStatus.CANCELLED.value,
                thread_id,
            ),
        )
        row = await cur.fetchone()
        if row is None:
            return SeedingThreadProgress()
        return SeedingThreadProgress(
            total=int(row["total"]),
            sent=int(row["sent"]),
            failed=int(row["failed"]),
            cancelled=int(row["cancelled"]),
        )
