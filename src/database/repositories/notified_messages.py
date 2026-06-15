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

    async def record(self, query_id: int, channel_id: int, message_ids: list[int]) -> None:
        if not message_ids:
            return
        assert self._database is not None, "NotifiedMessagesRepository.record requires a Database"
        await self._database.executemany_write(
            "INSERT OR IGNORE INTO notified_messages (query_id, channel_id, message_id) VALUES (?, ?, ?)",
            [(query_id, channel_id, mid) for mid in message_ids],
        )
