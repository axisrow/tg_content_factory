"""Repository for message deletions observed by the session owner.

The gateway journals a ``kind: "deleted"`` event the moment Telegram reports
messages gone; the event consumer fans the ids out here. The store answers one
question — "was this message id deleted, and when" — without polling history:
send paths need not record anything, the lookup goes by (phone, message_id).
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING

from tg_agent.database.pool import ReadConnection
from tg_agent.utils.datetime import normalize_utc, utc_isoformat

if TYPE_CHECKING:
    from tg_agent.database.facade import Database

_RETENTION_DAYS = 7


class ObservedDeletionsRepository:
    """Record and query Telegram deletion updates seen on live connections."""

    def __init__(
        self,
        db: ReadConnection,
        *,
        database: "Database | None" = None,
    ):
        self._db = db
        self._database = database

    async def record_many(
        self,
        phone: str,
        chat_id: int | None,
        message_ids: list[int],
        *,
        deleted_at: datetime | None = None,
    ) -> int:
        """Fan a deletion event out to one row per message id; return rows added.

        Writes go through the shared ``executemany_write`` helper: standalone it
        locks and commits; inside the event consumer the caller passes its
        ``_TransactionWrites`` wrapper, so the rows commit with the cursor move
        in one transaction.
        """
        if not message_ids:
            return 0
        assert self._database is not None, (
            "ObservedDeletionsRepository.record_many requires a Database reference"
        )
        stamp = utc_isoformat(deleted_at or datetime.now(timezone.utc))
        sql = "INSERT OR IGNORE INTO observed_deletions(phone, chat_id, message_id, deleted_at) VALUES (?, ?, ?, ?)"
        seq = [(phone, chat_id, message_id, stamp) for message_id in message_ids]
        cur = await self._database.executemany_write(sql, seq)
        return cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0

    async def was_deleted(
        self,
        phone: str,
        message_id: int,
        *,
        since: datetime | None = None,
    ) -> datetime | None:
        """When ``phone`` saw ``message_id`` deleted, or None while it lives.

        ``since`` narrows the window (a deletion older than the caller cares
        about reads as "not deleted"); both stamps render on one UTC timescale,
        so the TEXT comparison stays honest (#1666).
        """
        sql = "SELECT deleted_at FROM observed_deletions WHERE phone=? AND message_id=?"
        params: list[object] = [phone, message_id]
        if since is not None:
            sql += " AND deleted_at >= ?"
            params.append(utc_isoformat(since))
        cur = await self._db.execute(sql + " ORDER BY id DESC LIMIT 1", tuple(params))
        row = await cur.fetchone()
        if row is None:
            return None
        return normalize_utc(datetime.fromisoformat(str(row["deleted_at"]).replace("Z", "+00:00")))

    async def prune(self, *, days: int = _RETENTION_DAYS) -> int:
        """Drop rows older than ``days``; mirror the gateway's event retention."""
        assert self._database is not None, (
            "ObservedDeletionsRepository.prune requires a Database reference"
        )
        threshold = utc_isoformat(datetime.now(timezone.utc) - timedelta(days=days))
        cur = await self._database.execute_write(
            "DELETE FROM observed_deletions WHERE deleted_at < ?", (threshold,)
        )
        return cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0
