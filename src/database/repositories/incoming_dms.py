"""Журнал входящих личных сообщений (DM) — этап 2.2 эпика #1416 (#1427).

Журнал, а не снапшот: строки накапливаются с уникальностью
`(phone, chat_id, message_id)`, чтобы догон пропущенного (#1428) был
идемпотентным. Статус самого слушателя живёт отдельно в `runtime_snapshots`
(тот upsert-ится по ключу — для потока сообщений не годится).

Хранит текст личной переписки, поэтому унаследовал планку приватности
`dialogs_history`: конечный TTL и prune-on-write при каждой записи, а
heartbeat воркера дочищает протухшее, когда новые записи перестают
приходить (prune-on-write без этого покрывал бы только активные диалоги).
Гарантия «не дольше TTL» действует, пока жив воркер.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import aiosqlite

from src.database.pool import ReadConnection
from src.models import INCOMING_DM_JOURNAL_TTL_SECONDS, IncomingDm
from src.utils.datetime import parse_datetime

if TYPE_CHECKING:
    from src.database.facade import Database

# julianday() сравнивает mixed ISO-форматы (см. комментарий в
# runtime_snapshots.prune_expired — 'T'/' ' и смещения).
_PRUNE_SQL = (
    "DELETE FROM incoming_dms WHERE julianday(received_at) < julianday('now', ?)"
)


class IncomingDmsRepository:
    """Журнал входящих DM: идемпотентная запись, TTL/prune, признак обработки."""

    def __init__(
        self,
        db: ReadConnection,
        *,
        database: "Database | None" = None,
    ):
        self._db = db
        self._database = database

    @staticmethod
    def _to_dm(row: aiosqlite.Row) -> IncomingDm:
        return IncomingDm(
            id=row["id"],
            phone=row["phone"],
            chat_id=row["chat_id"],
            message_id=row["message_id"],
            text=row["text"],
            message_date=parse_datetime(row["message_date"]),
            received_at=parse_datetime(row["received_at"]),
            processed=bool(row["processed"]),
        )

    async def record(
        self, dm: IncomingDm, *, ttl_seconds: int = INCOMING_DM_JOURNAL_TTL_SECONDS
    ) -> bool:
        """Записать входящее DM; True — строка новая, False — дубль (уже в журнале).

        В той же транзакции чистит протухшие строки (prune-on-write, как у
        dialogs_history). Это покрывает только активные диалоги: когда новые
        записи перестают приходить, DELETE не выполняется — протухшее
        дочищает `prune_expired()` в heartbeat воркера.
        """
        assert self._database is not None, (
            "IncomingDmsRepository.record requires a Database reference"
        )
        async with self._database.transaction() as conn:
            cur = await conn.execute(
                """
                INSERT OR IGNORE INTO incoming_dms
                    (phone, chat_id, message_id, text, message_date, received_at)
                VALUES (?, ?, ?, ?, ?, COALESCE(?, datetime('now')))
                """,
                (
                    dm.phone,
                    dm.chat_id,
                    dm.message_id,
                    dm.text,
                    dm.message_date.isoformat() if dm.message_date else None,
                    dm.received_at.isoformat() if dm.received_at else None,
                ),
            )
            inserted = cur.rowcount > 0
            await conn.execute(_PRUNE_SQL, (f"-{ttl_seconds} seconds",))
        return inserted

    async def prune_expired(
        self, older_than_seconds: int = INCOMING_DM_JOURNAL_TTL_SECONDS
    ) -> int:
        """Удалить записи старше `older_than_seconds`; вернуть число удалённых."""
        assert self._database is not None, (
            "IncomingDmsRepository.prune_expired requires a Database reference"
        )
        cur = await self._database.execute_write(
            _PRUNE_SQL, (f"-{older_than_seconds} seconds",)
        )
        return cur.rowcount

    async def count_unprocessed(self) -> int:
        """Число ещё не разобранных записей журнала (для снапшота статуса слушателя)."""
        cur = await self._db.execute(
            "SELECT COUNT(*) AS n FROM incoming_dms WHERE processed = 0"
        )
        row = await cur.fetchone()
        return int(row["n"]) if row else 0

    async def mark_processed(self, phone: str, chat_id: int, message_ids: list[int]) -> int:
        """Пометить сообщения одного диалога обработанными; вернуть число обновлённых."""
        assert self._database is not None, (
            "IncomingDmsRepository.mark_processed requires a Database reference"
        )
        if not message_ids:
            return 0
        cur = await self._database.executemany_write(
            "UPDATE incoming_dms SET processed = 1 "
            "WHERE phone = ? AND chat_id = ? AND message_id = ?",
            [(phone, chat_id, message_id) for message_id in message_ids],
        )
        return cur.rowcount
