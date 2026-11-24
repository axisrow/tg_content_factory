"""Repository for the sent-messages registry (#1893).

The send layer records every outgoing message here the moment it is
delivered — what, when, from which account, through which feature. The
deletion fact itself lives in ``observed_deletions`` (deletion capture);
this store only joins it for reports, so nothing is duplicated and the
registry stays a pure accounting of sends.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import TYPE_CHECKING

from tg_agent.database.pool import ReadConnection
from tg_agent.database.repositories.message_analytics import MessageAnalyticsMixin
from tg_agent.utils.datetime import utc_isoformat

if TYPE_CHECKING:
    from tg_agent.database.facade import Database


class SentMessagesRepository:
    """Record outgoing sends and answer "what did we send" for reports."""

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
        chat_type: str,
        source: str,
        recipient: str | None = None,
        text: str | None = None,
        ref: str | None = None,
        sent_at: datetime | None = None,
    ) -> int:
        """Fan one delivery out to one row per message id; return rows added.

        ``chat_id`` follows the ``observed_deletions`` shape — the marked
        ``-100…`` form for channels/supergroups, NULL for DMs and small
        groups — so the report join needs no normalization. One
        ``executemany_write`` per batch (one lock + one commit), mirroring
        ``ObservedDeletionsRepository.record_many``.
        """
        if not message_ids:
            return 0
        assert self._database is not None, (
            "SentMessagesRepository.record_many requires a Database reference"
        )
        stamp = utc_isoformat(sent_at or datetime.now(timezone.utc))
        sql = (
            "INSERT OR IGNORE INTO sent_messages"
            "(phone, chat_id, message_id, chat_type, recipient, text, source, ref, sent_at)"
            " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)"
        )
        seq = [
            (phone, chat_id, message_id, chat_type, recipient, text, source, ref, stamp)
            for message_id in message_ids
        ]
        cur = await self._database.executemany_write(sql, seq)
        return cur.rowcount if cur.rowcount and cur.rowcount > 0 else 0

    async def list(
        self,
        *,
        phone: str | None = None,
        chat_id: int | None = None,
        source: str | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
        limit: int = 200,
    ) -> list[dict]:
        """Registry rows, newest first, each with its observed deletion status.

        The join mirrors how the deletion pipeline names chats: a registry row
        matches a deletion only when message id AND chat shape agree — a
        channel deletion of the same id never lands on a DM row and vice
        versa. The observer's phone does not participate for channels: the
        account that sees a channel deletion is usually a peer pool account,
        not the sender (#1898), and "any account saw it deleted" is deleted —
        the same semantics as ``messages wait``. NULL-chat rows (dm/
        small-group) are the exception — their message ids are per-account
        sequences, so there the phone must match. The correlated scalar
        subquery rides the ``observed_deletions_message`` index per emitted
        row (a GROUP BY over the whole table would run per report forever —
        both stores are unpruned); MIN collapses multiple observers to the
        earliest ``deleted_at``.
        """
        sql = (
            "SELECT sm.phone, sm.chat_id, sm.message_id, sm.chat_type, sm.recipient,"
            " sm.text, sm.source, sm.ref, sm.sent_at,"
            " (SELECT MIN(od.deleted_at) FROM observed_deletions od"
            "  WHERE od.message_id = sm.message_id AND od.chat_id IS sm.chat_id"
            "  AND (od.chat_id IS NOT NULL OR od.phone = sm.phone)) AS deleted_at"
            " FROM sent_messages sm"
            " WHERE 1=1"
        )
        params: list[object] = []
        if phone is not None:
            sql += " AND sm.phone=?"
            params.append(phone)
        if chat_id is not None:
            sql += " AND sm.chat_id=?"
            params.append(chat_id)
        if source is not None:
            sql += " AND sm.source=?"
            params.append(source)
        if date_from is not None:
            sql += " AND sm.sent_at>=?"
            params.append(MessageAnalyticsMixin._normalize_date_from(date_from))
        if date_to is not None:
            # Date-only bounds go exclusive next-day, ISO datetimes exact —
            # the shared #1593 semantics, one home.
            bound, operator = MessageAnalyticsMixin._normalize_date_to(date_to)
            sql += f" AND sm.sent_at{operator}?"
            params.append(bound)
        sql += " ORDER BY sm.sent_at DESC, sm.id DESC LIMIT ?"
        params.append(limit)
        cur = await self._db.execute(sql, tuple(params))
        rows = await cur.fetchall()
        return [dict(row) for row in rows]
