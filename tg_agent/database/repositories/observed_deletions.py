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
from tg_agent.utils.datetime import parse_utc_datetime, utc_isoformat

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

    async def lookup_deletion(
        self,
        message_id: int,
        *,
        phone: str | None = None,
        chat_id: int | None = None,
    ) -> tuple[str, int | None, datetime] | None:
        """Find a recorded deletion of ``message_id``; None while it lives.

        The matching policy lives here, next to the rows it describes. A
        ``chat_id`` matches that chat strictly (channel ids are per-channel, so
        an id collision across chats must not leak). Without one, ``phone``
        demands the DM shape — Telegram names no chat for private/small-group
        deletions, and DM ids are account-global, so a channel row for the same
        id is a different message — while no phone at all means a free search
        over every row for the id. Message ids are never reused, and the table
        prunes after seven days, so no time window is needed.
        """
        sql = "SELECT phone, chat_id, deleted_at FROM observed_deletions WHERE message_id=?"
        params: list[object] = [message_id]
        if chat_id is not None:
            sql += " AND chat_id=?"
            params.append(chat_id)
        elif phone is not None:
            sql += " AND chat_id IS NULL"
        if phone is not None:
            sql += " AND phone=?"
            params.append(phone)
        cur = await self._db.execute(sql + " ORDER BY id DESC LIMIT 1", tuple(params))
        row = await cur.fetchone()
        if row is None:
            return None
        deleted_at = parse_utc_datetime(str(row["deleted_at"]))
        assert deleted_at is not None, "observed_deletions.deleted_at is NOT NULL"
        return row["phone"], row["chat_id"], deleted_at

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
