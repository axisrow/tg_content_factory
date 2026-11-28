"""Черновики ответов на входящие DM — этап 2.4 эпика #1416 (#1429).

Статусная машина по образцу ``generation_runs.moderation_status``:

    draft ──approve──▶ approved ──claim──▶ sending ──mark_sent──▶ sent
      │  ▲                │                   │    │
      │  └── re-decision ─┴──◀── release ─────┘    └─no message id─▶ unconfirmed
      └─── reject ──▶ rejected

(``release`` — только с ``sending``, и не затирает ``sent``.)

Ключевой инвариант — двойная отправка невозможна: ``sending`` ставится
атомарным ``UPDATE ... WHERE status = 'approved'`` и успех определяется
``rowcount == 1``, поэтому два конкурентных потребителя клеймят ровно один
раз (паттерн ``claim_for_publish`` из #1036). Решения владельца
(approve/reject) не трогают терминальные ``sending``/``sent``/``unconfirmed``
— апрув после отправки — no-op, отклонённый черновик никогда не клеймится.
``unconfirmed`` (#1981): Telegram ответил на отправку без id сообщения — ответ
мог дойти, поэтому черновик терминален, как ``sent``, но статус называет
неподтверждённую доставку.

``phone``/``chat_id`` копируются из входящего сообщения: журнальная строка
``incoming_dms`` живёт 24ч, а апрувнутый черновик может ждать отправки дольше.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Literal

import aiosqlite

from tg_agent.database.pool import ReadConnection
from tg_agent.models import DmReplyDraft
from tg_agent.utils.datetime import parse_datetime

if TYPE_CHECKING:
    from tg_agent.database.facade import Database


class DmReplyDraftsRepository:
    """Черновики ответов: upsert, решения владельца, клейм-троица отправки."""

    def __init__(
        self,
        db: ReadConnection,
        *,
        database: "Database | None" = None,
    ):
        self._db = db
        self._database = database

    @staticmethod
    def _to_draft(row: aiosqlite.Row) -> DmReplyDraft:
        return DmReplyDraft(
            id=row["id"],
            incoming_dm_id=row["incoming_dm_id"],
            phone=row["phone"],
            chat_id=row["chat_id"],
            reply_text=row["reply_text"],
            status=row["status"],
            created_at=parse_datetime(row["created_at"]),
            updated_at=parse_datetime(row["updated_at"]),
            sent_message_id=row["sent_message_id"],
        )

    async def create_draft(
        self,
        *,
        incoming_dm_id: int,
        phone: str,
        chat_id: int,
        reply_text: str,
    ) -> tuple[int, bool]:
        """Создать черновик для входящего сообщения; вернуть ``(id, created)``.

        Один черновик на входящее (``UNIQUE(incoming_dm_id)``): повторная
        генерация обновляет текст, пока черновик ещё ``draft``; в более
        поздних статусах существующая строка не трогается — правка
        одобренного/отклонённого текста за спиной владельца недопустима.
        Вся ветвистая логика — в одной ``BEGIN IMMEDIATE``-транзакции.
        """
        assert self._database is not None, (
            "DmReplyDraftsRepository.create_draft requires a Database reference"
        )
        async with self._database.transaction() as conn:
            cur = await conn.execute(
                "SELECT id, status FROM dm_reply_drafts WHERE incoming_dm_id = ?",
                (incoming_dm_id,),
            )
            row = await cur.fetchone()
            if row is not None and row["status"] != "draft":
                return int(row["id"]), False
            if row is None:
                ins = await conn.execute(
                    "INSERT OR IGNORE INTO dm_reply_drafts "
                    "(incoming_dm_id, phone, chat_id, reply_text) VALUES (?, ?, ?, ?)",
                    (incoming_dm_id, phone, chat_id, reply_text),
                )
                if ins.rowcount:
                    return int(ins.lastrowid or 0), True
                # Проиграли гонку соседнему создателю — существующая строка важнее.
                cur = await conn.execute(
                    "SELECT id FROM dm_reply_drafts WHERE incoming_dm_id = ?",
                    (incoming_dm_id,),
                )
                winner = await cur.fetchone()
                if winner is None:  # pragma: no cover — тот же стейтмент гарантирует строку
                    raise RuntimeError(
                        f"dm_reply_drafts row for incoming_dm_id={incoming_dm_id} vanished"
                    )
                return int(winner["id"]), False
            await conn.execute(
                "UPDATE dm_reply_drafts SET reply_text = ?, updated_at = datetime('now') "
                "WHERE id = ? AND status = 'draft'",
                (reply_text, row["id"]),
            )
            return int(row["id"]), False

    async def get(self, draft_id: int) -> DmReplyDraft | None:
        cur = await self._db.execute(
            "SELECT * FROM dm_reply_drafts WHERE id = ?", (draft_id,)
        )
        row = await cur.fetchone()
        return self._to_draft(row) if row else None

    async def list_drafts(
        self,
        *,
        status: str | None = None,
        limit: int = 100,
    ) -> list[DmReplyDraft]:
        """Список черновиков, свежие первыми; ``status`` — фильтр по статусу."""
        params: list[object] = []
        where = ""
        if status is not None:
            where = "WHERE status = ?"
            params.append(status)
        params.append(limit)
        cur = await self._db.execute(
            f"SELECT * FROM dm_reply_drafts {where} ORDER BY id DESC LIMIT ?",
            tuple(params),
        )
        rows = await cur.fetchall()
        return [self._to_draft(row) for row in rows]

    async def decide(
        self, draft_id: int, decision: Literal["approved", "rejected"]
    ) -> bool:
        """Решение владельца: ``draft``/другое решение → ``decision``.

        Идемпотентно: повтор того же решения и решение на терминальных
        ``sending``/``sent``/``unconfirmed`` — no-op (гард в WHERE, атомарно с
        записью), поэтому апрув после отправки не возвращает черновик в очередь
        и не может вызвать повторную отправку, а переход ``rejected →
        approved`` (владелец передумал) остаётся возможен. True — статус изменился.
        """
        assert self._database is not None, (
            "DmReplyDraftsRepository.decide requires a Database reference"
        )
        cur = await self._database.execute_write(
            "UPDATE dm_reply_drafts SET status = ?, updated_at = datetime('now') "
            "WHERE id = ? AND status NOT IN ('sending', 'sent', 'unconfirmed') AND status != ?",
            (decision, draft_id, decision),
        )
        return bool(cur.rowcount)

    async def claim_for_send(self, draft_id: int) -> bool:
        """Атомарно клеймить ``approved`` → ``sending``; True — клейм наш.

        Второй конкурентный клейм того же черновика получает ``rowcount == 0``
        и False: отправка уходит ровно один раз (против двойной отправки,
        #1429). ``rejected``/``draft``/``sent`` не клеймятся — отклонённый
        черновик не отправляется никогда.

        Осиротевший лиза-клейм реклеймится: ``sending`` старше 10 минут
        (updated_at ставится клеймом; живой отправитель при 120-секундном
        send-timeout пула не может держать лизу дольше) — это сирота убитого
        воркера, и 60-секундный ретрай команды реанимирует её без ожидания
        рестарта (startup-sweep остаётся belt-and-braces). Живой чужой клейм
        моложе 10 минут не реклеймится — двойная отправка исключена.
        """
        assert self._database is not None, (
            "DmReplyDraftsRepository.claim_for_send requires a Database reference"
        )
        cur = await self._database.execute_write(
            "UPDATE dm_reply_drafts SET status = 'sending', updated_at = datetime('now') "
            "WHERE id = ? AND (status = 'approved' "
            "OR (status = 'sending' AND updated_at < datetime('now', '-10 minutes')))",
            (draft_id,),
        )
        return bool(cur.rowcount)

    async def release_claim(self, draft_id: int) -> bool:
        """Откатить неудачный клейм ``sending`` → ``approved``; True — откатили.

        Гард ``status = 'sending'`` не даёт затереть более позднее состояние:
        если черновик уже помечен ``sent``, отката не будет.
        """
        assert self._database is not None, (
            "DmReplyDraftsRepository.release_claim requires a Database reference"
        )
        cur = await self._database.execute_write(
            "UPDATE dm_reply_drafts SET status = 'approved', updated_at = datetime('now') "
            "WHERE id = ? AND status = 'sending'",
            (draft_id,),
        )
        return bool(cur.rowcount)

    async def mark_sent(self, draft_id: int, sent_message_id: int) -> bool:
        """Завершить отправку: ``sending`` → ``sent`` + Telegram message_id.

        Id обязателен — ``sent`` означает «воркер получил id». Отправка без
        подтверждённого id завершается в ``unconfirmed`` (#1981).
        """
        assert self._database is not None, (
            "DmReplyDraftsRepository.mark_sent requires a Database reference"
        )
        cur = await self._database.execute_write(
            "UPDATE dm_reply_drafts SET status = 'sent', sent_message_id = ?, "
            "updated_at = datetime('now') WHERE id = ? AND status = 'sending'",
            (sent_message_id, draft_id),
        )
        return bool(cur.rowcount)

    async def mark_unconfirmed(self, draft_id: int) -> bool:
        """Отправка без подтверждённого id: ``sending`` → ``unconfirmed`` (#1981).

        Ответ мог дойти, поэтому повторная отправка исключена так же, как после
        ``sent``; статус называет неподтверждённую доставку владельцу.
        """
        assert self._database is not None, (
            "DmReplyDraftsRepository.mark_unconfirmed requires a Database reference"
        )
        cur = await self._database.execute_write(
            "UPDATE dm_reply_drafts SET status = 'unconfirmed', updated_at = datetime('now') "
            "WHERE id = ? AND status = 'sending'",
            (draft_id,),
        )
        return bool(cur.rowcount)

    async def reset_sending_on_startup(self) -> int:
        """На старте воркера вернуть осиротевшие ``sending`` в ``approved``; вернуть число.

        Клейм — лиза: воркер, убитый между ``claim_for_send`` и ``mark_sent``,
        оставляет черновик в ``sending`` навсегда. Гард по staleness 10 минут
        (лейза обновляется самим клеймом; единичная отправка DM — секунды,
        120-секундный send-timeout пула) не трогает живой клейм при
        перекрывающемся роллауте — тот же трейдофф redelivery, что у
        ``generation_runs.reset_running_on_startup`` (#1036).
        """
        assert self._database is not None, (
            "DmReplyDraftsRepository.reset_sending_on_startup requires a Database reference"
        )
        cur = await self._database.execute_write(
            "UPDATE dm_reply_drafts SET status = 'approved', updated_at = datetime('now') "
            "WHERE status = 'sending' "
            "AND updated_at < datetime('now', '-10 minutes')",
        )
        return cur.rowcount or 0
