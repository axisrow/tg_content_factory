from __future__ import annotations

import logging

from src.database import Database
from src.models import NotificationBot
from src.telegram import botfather
from src.telegram.client_pool import ClientPool

logger = logging.getLogger(__name__)

_BOT_NAME_PREFIX = "LeadHunter"
_BOT_USERNAME_PREFIX = "leadhunter_"


class NotificationService:
    def __init__(self, db: Database, pool: ClientPool):
        self._db = db
        self._pool = pool

    async def setup_bot(self) -> NotificationBot:
        """Create a personal notification bot via BotFather and save it to DB."""
        result = await self._pool.get_available_client()
        if not result:
            raise RuntimeError("No available Telegram client in pool")
        client, phone = result

        try:
            me = await client.get_me()
        finally:
            await self._pool.release_client(phone)

        tg_user_id: int = me.id
        tg_username: str | None = getattr(me, "username", None)

        slug = tg_username or str(tg_user_id)
        bot_username = f"{_BOT_USERNAME_PREFIX}{slug}_bot"
        bot_name = f"{_BOT_NAME_PREFIX} ({slug})"

        # Re-acquire client for BotFather conversation
        result = await self._pool.get_available_client()
        if not result:
            raise RuntimeError("No available Telegram client in pool")
        client, phone = result

        try:
            token = await botfather.create_bot(client, bot_name, bot_username)

            # Send /start to the new bot so it gets initialised
            try:
                await client.send_message(bot_username, "/start")
            except Exception as exc:
                logger.warning("Could not send /start to @%s: %s", bot_username, exc)

            # Resolve the bot's Telegram ID
            try:
                entity = await client.get_entity(bot_username)
                bot_id: int = entity.id
            except Exception as exc:
                logger.warning("Could not resolve bot entity for @%s: %s", bot_username, exc)
                bot_id = 0

        finally:
            await self._pool.release_client(phone)

        bot = NotificationBot(
            tg_user_id=tg_user_id,
            tg_username=tg_username,
            bot_id=bot_id,
            bot_username=bot_username,
            bot_token=token,
        )
        await self._db.save_notification_bot(bot)
        logger.info("Notification bot @%s set up for user %s", bot_username, tg_user_id)
        return bot

    async def get_status(self) -> NotificationBot | None:
        """Return bot info for the primary account's user, or None if not set up."""
        result = await self._pool.get_available_client()
        if not result:
            return None
        client, phone = result
        try:
            me = await client.get_me()
        finally:
            await self._pool.release_client(phone)
        return await self._db.get_notification_bot(me.id)

    async def teardown_bot(self) -> None:
        """Delete the notification bot via BotFather and remove it from DB."""
        result = await self._pool.get_available_client()
        if not result:
            raise RuntimeError("No available Telegram client in pool")
        client, phone = result

        try:
            me = await client.get_me()
            tg_user_id: int = me.id
            bot = await self._db.get_notification_bot(tg_user_id)
            if bot is None:
                raise RuntimeError("No notification bot found for this user")

            await botfather.delete_bot(client, bot.bot_username)
        finally:
            await self._pool.release_client(phone)

        await self._db.delete_notification_bot(tg_user_id)
        logger.info("Notification bot deleted for user %s", tg_user_id)
