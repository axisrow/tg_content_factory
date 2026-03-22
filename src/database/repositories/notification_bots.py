from __future__ import annotations

from datetime import datetime

import aiosqlite

from src.models import NotificationBot


class NotificationBotsRepository:
    def __init__(self, db: aiosqlite.Connection):
        self._db = db

    async def get_bot(self, tg_user_id: int) -> NotificationBot | None:
        cur = await self._db.execute(
            "SELECT * FROM notification_bots WHERE tg_user_id = ? LIMIT 1",
            (tg_user_id,),
        )
        row = await cur.fetchone()
        if row is None:
            return None
        return self._row_to_model(row)

    async def save_bot(self, bot: NotificationBot) -> int:
        cur = await self._db.execute(
            """
            INSERT INTO notification_bots (tg_user_id, tg_username, bot_id, bot_username, bot_token)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(tg_user_id) DO UPDATE SET
                tg_username = excluded.tg_username,
                bot_id = excluded.bot_id,
                bot_username = excluded.bot_username,
                bot_token = excluded.bot_token
            """,
            (bot.tg_user_id, bot.tg_username, bot.bot_id, bot.bot_username, bot.bot_token),
        )
        await self._db.commit()
        return cur.lastrowid or 0

    async def count(self) -> int:
        cur = await self._db.execute("SELECT COUNT(*) FROM notification_bots")
        row = await cur.fetchone()
        return row[0] if row else 0

    async def delete_bot(self, tg_user_id: int) -> None:
        await self._db.execute(
            "DELETE FROM notification_bots WHERE tg_user_id = ?",
            (tg_user_id,),
        )
        await self._db.commit()

    @staticmethod
    def _row_to_model(row) -> NotificationBot:
        created_at = None
        if row["created_at"]:
            try:
                created_at = datetime.fromisoformat(row["created_at"])
            except ValueError:
                pass
        return NotificationBot(
            id=row["id"],
            tg_user_id=row["tg_user_id"],
            tg_username=row["tg_username"],
            bot_id=row["bot_id"],
            bot_username=row["bot_username"],
            bot_token=row["bot_token"],
            created_at=created_at,
        )
