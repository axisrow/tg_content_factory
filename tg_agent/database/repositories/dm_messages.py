"""Постоянный архив личных сообщений — обе стороны, без TTL (#1453).

Дополнение к журналу `incoming_dms` (эпик #1416): журнал — суточный буфер
черновик-ассистента с планкой приватности, архив — вечное хранение полной
переписки по явному запросу владельца. Пишут все три пути: живой слушатель
(#1426), догон (#1428) и бэкфилл `dialogs archive` — поэтому `INSERT OR
IGNORE` по `UNIQUE(phone, chat_id, message_id)` обязателен, идемпотентность
на границе путей держит только база.

Prune/TTL у архива нет сознательно — это его смысл. Рост таблицы неограничен:
личных диалогов на порядки меньше канального корпуса, объём контролирует
владелец.
"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING

import aiosqlite

from tg_agent.database.pool import ReadConnection
from tg_agent.models import DmMessage
from tg_agent.utils.datetime import normalize_utc, parse_datetime

if TYPE_CHECKING:
    from tg_agent.database.facade import Database


class DmMessagesRepository:
    """Архив DM: идемпотентная запись обеих сторон, курсор бэкфилла, счётчики."""

    def __init__(
        self,
        db: ReadConnection,
        *,
        database: "Database | None" = None,
    ):
        self._db = db
        self._database = database

    @staticmethod
    def _to_dm(row: aiosqlite.Row) -> DmMessage:
        return DmMessage(
            id=row["id"],
            phone=row["phone"],
            chat_id=row["chat_id"],
            message_id=row["message_id"],
            out=bool(row["out"]),
            text=row["text"],
            message_date=parse_datetime(row["message_date"]),
            received_at=parse_datetime(row["received_at"]),
        )

    async def record(self, dm: DmMessage) -> bool:
        """Записать сообщение в архив; True — новое, False — дубль (молча)."""
        assert self._database is not None, (
            "DmMessagesRepository.record requires a Database reference"
        )
        async with self._database.transaction() as conn:
            cur = await conn.execute(
                """
                INSERT OR IGNORE INTO dm_messages
                    (phone, chat_id, message_id, out, text, message_date, received_at)
                VALUES (?, ?, ?, ?, ?, ?, COALESCE(?, datetime('now')))
                """,
                (
                    dm.phone,
                    dm.chat_id,
                    dm.message_id,
                    1 if dm.out else 0,
                    dm.text,
                    dm.message_date.isoformat() if dm.message_date else None,
                    dm.received_at.isoformat() if dm.received_at else None,
                ),
            )
            return cur.rowcount > 0

    async def max_message_id(self, phone: str, chat_id: int) -> int:
        """Водяной знак бэкфилла: максимум message_id архива диалога, 0 если пусто."""
        cur = await self._db.execute(
            "SELECT MAX(message_id) AS n FROM dm_messages WHERE phone = ? AND chat_id = ?",
            (phone, chat_id),
        )
        row = await cur.fetchone()
        return int(row["n"]) if row and row["n"] is not None else 0

    async def latest_message_date(self) -> datetime | None:
        """Дата новейшего сообщения архива: докуда проект уже видел переписку."""
        cur = await self._db.execute("SELECT MAX(message_date) AS d FROM dm_messages")
        row = await cur.fetchone()
        return normalize_utc(parse_datetime(row["d"])) if row else None

    async def count(self, phone: str | None = None) -> int:
        """Число сообщений архива; с `phone` — по одному аккаунту."""
        if phone is None:
            cur = await self._db.execute("SELECT COUNT(*) AS n FROM dm_messages")
        else:
            cur = await self._db.execute(
                "SELECT COUNT(*) AS n FROM dm_messages WHERE phone = ?", (phone,)
            )
        row = await cur.fetchone()
        return int(row["n"]) if row else 0

    async def list(
        self,
        *,
        phone: str | None = None,
        chat_id: int | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list[DmMessage]:
        """Архив, свежие первыми; фильтры по аккаунту и/или диалогу (#1580).

        Читается agent-tool `read_dm_archive` и CLI `dm archive list` — тем же
        способом, каким web/REST (#1579) читает журнал; у архива нет TTL,
        поэтому окно ограничивает только LIMIT/OFFSET.
        """
        where: list[str] = []
        params: list[object] = []
        if phone is not None:
            where.append("phone = ?")
            params.append(phone)
        if chat_id is not None:
            where.append("chat_id = ?")
            params.append(int(chat_id))
        where_sql = f"WHERE {' AND '.join(where)}" if where else ""
        cur = await self._db.execute(
            f"SELECT * FROM dm_messages {where_sql} ORDER BY id DESC LIMIT ? OFFSET ?",
            (*params, int(limit), int(offset)),
        )
        rows = await cur.fetchall()
        return [self._to_dm(row) for row in rows]

    async def count_by_direction(self, phone: str) -> tuple[int, int]:
        """(входящие, исходящие) по аккаунту — критерий приёмки #1453."""
        cur = await self._db.execute(
            """
            SELECT
                COALESCE(SUM(CASE WHEN out = 0 THEN 1 ELSE 0 END), 0) AS incoming,
                COALESCE(SUM(CASE WHEN out = 1 THEN 1 ELSE 0 END), 0) AS outgoing
            FROM dm_messages WHERE phone = ?
            """,
            (phone,),
        )
        row = await cur.fetchone()
        return int(row["incoming"]), int(row["outgoing"])
