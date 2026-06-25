"""Леджер уже отправленных уведомлений (дедуп доставки)."""

from __future__ import annotations

from typing import TYPE_CHECKING

import aiosqlite

if TYPE_CHECKING:
    from src.database.facade import Database


class NotifiedMessagesRepository:
    """Dedup ledger for sent notifications (audit #838/1).

    Lets the notification path retry a failed send without re-notifying messages
    that already went out, decoupling delivery from the forward-only collection
    cursor.
    """

    def __init__(self, db: aiosqlite.Connection, *, database: "Database | None" = None):
        self._db = db
        self._database = database

    async def filter_unnotified(
        self, query_id: int, channel_id: int, message_ids: list[int]
    ) -> set[int]:
        """Return the subset of message_ids NOT yet notified for (query_id, channel_id)."""
        if not message_ids:
            return set()
        placeholders = ",".join("?" * len(message_ids))
        cur = await self._db.execute(
            f"""
            SELECT message_id FROM notified_messages
            WHERE query_id = ? AND channel_id = ? AND message_id IN ({placeholders})
            """,
            (query_id, channel_id, *message_ids),
        )
        rows = await cur.fetchall()
        already = {int(row["message_id"]) for row in rows}
        return {mid for mid in message_ids if mid not in already}

    async def has_any(self, channel_ids: list[int]) -> bool:
        """Return True if the ledger already has at least one row for any of these channels.

        Used to distinguish a populated ledger from a brand-new empty one: on the first pass
        after the table is created the ledger is empty, so the 24h backlog rescan must NOT be
        replayed (it would treat every already-delivered historical match as un-notified and
        re-send it). Once a channel has any ledger row, the rescan is safe — the ledger filters
        out everything already sent, leaving only genuinely failed/new matches to retry.
        """
        if not channel_ids:
            return False
        placeholders = ",".join("?" * len(channel_ids))
        cur = await self._db.execute(
            f"SELECT 1 FROM notified_messages WHERE channel_id IN ({placeholders}) LIMIT 1",
            tuple(channel_ids),
        )
        return await cur.fetchone() is not None

    async def record(self, query_id: int, channel_id: int, message_ids: list[int]) -> None:
        """Отметить сообщения как уведомлённые для (query_id, channel_id).

        Append-only вставка `INSERT OR IGNORE` — повторная запись тех же id не
        дублируется и не падает. Пустой список — no-op.
        """
        if not message_ids:
            return
        assert self._database is not None, "NotifiedMessagesRepository.record requires a Database"
        await self._database.executemany_write(
            "INSERT OR IGNORE INTO notified_messages (query_id, channel_id, message_id) VALUES (?, ?, ?)",
            [(query_id, channel_id, mid) for mid in message_ids],
        )
