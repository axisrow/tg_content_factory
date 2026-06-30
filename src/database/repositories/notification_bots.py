"""Репозиторий персональных ботов уведомлений (один на пользователя Telegram)."""

from __future__ import annotations

from typing import TYPE_CHECKING

import aiosqlite

from src.models import NotificationBot
from src.utils.datetime import try_parse_datetime

if TYPE_CHECKING:
    from src.database.facade import Database


class NotificationBotsRepository:
    """Персональные боты для уведомлений, созданные через BotFather.

    Один бот на пользователя Telegram (`tg_user_id` уникален): хранит токен и
    идентификаторы бота, через который [`Notifier`][] шлёт оповещения о новых
    совпадениях. Запись — upsert по `tg_user_id`.
    """

    def __init__(
        self,
        db: aiosqlite.Connection,
        *,
        database: "Database | None" = None,
    ):
        self._db = db
        self._database = database

    async def get_bot(self, tg_user_id: int) -> NotificationBot | None:
        """Бот пользователя по его Telegram id, либо None."""
        cur = await self._db.execute(
            "SELECT * FROM notification_bots WHERE tg_user_id = ? LIMIT 1",
            (tg_user_id,),
        )
        row = await cur.fetchone()
        if row is None:
            return None
        return self._row_to_model(row)

    async def save_bot(self, bot: NotificationBot) -> int:
        """Сохранить/обновить бота пользователя (upsert по `tg_user_id`).

        Возвращает `cur.lastrowid` — надёжный id только на ветке вставки; при
        конфликте-обновлении lastrowid остаётся от последней вставки в
        соединении, для точного id читайте строку по `tg_user_id`.
        """
        assert self._database is not None, (
            "NotificationBotsRepository.save_bot requires a Database reference"
        )
        cur = await self._database.execute_write(
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
        return cur.lastrowid or 0

    async def count(self) -> int:
        """Число зарегистрированных ботов уведомлений."""
        cur = await self._db.execute("SELECT COUNT(*) FROM notification_bots")
        row = await cur.fetchone()
        return row[0] if row else 0

    async def delete_bot(self, tg_user_id: int) -> None:
        """Удалить бота пользователя по его Telegram id."""
        assert self._database is not None, (
            "NotificationBotsRepository.delete_bot requires a Database reference"
        )
        await self._database.execute_write(
            "DELETE FROM notification_bots WHERE tg_user_id = ?",
            (tg_user_id,),
        )

    @staticmethod
    def _row_to_model(row) -> NotificationBot:
        return NotificationBot(
            id=row["id"],
            tg_user_id=row["tg_user_id"],
            tg_username=row["tg_username"],
            bot_id=row["bot_id"],
            bot_username=row["bot_username"],
            bot_token=row["bot_token"],
            created_at=try_parse_datetime(row["created_at"]),
        )
