from __future__ import annotations

import asyncio
import logging

from src.database import Database
from src.database.bundles import NotificationBundle
from src.models import NotificationBot
from src.services.notification_target_service import NotificationTargetService
from src.telegram import botfather

logger = logging.getLogger(__name__)

_DEFAULT_BOT_NAME_PREFIX = "LeadHunter"
_DEFAULT_BOT_USERNAME_PREFIX = "leadhunter_"


class NotificationService:
    def __init__(
        self,
        notifications: NotificationBundle | Database,
        target_service: NotificationTargetService,
        bot_name_prefix: str = _DEFAULT_BOT_NAME_PREFIX,
        bot_username_prefix: str = _DEFAULT_BOT_USERNAME_PREFIX,
    ):
        if isinstance(notifications, Database):
            notifications = NotificationBundle.from_database(notifications)
        self._notifications = notifications
        self._target_service = target_service
        self._bot_name_prefix = bot_name_prefix
        self._bot_username_prefix = bot_username_prefix

    async def setup_bot(self) -> NotificationBot:
        """Create a personal notification bot via BotFather and save it to DB."""
        async with self._target_service.use_client() as (client, _phone):
            me = await asyncio.wait_for(client.get_me(), timeout=15.0)
            tg_user_id: int = me.id
            tg_username: str | None = getattr(me, "username", None)

            raw_slug = tg_username or str(tg_user_id)
            if len(raw_slug) > 17:
                logger.warning(
                    "slug '%s' truncated to 17 characters for bot username", raw_slug
                )
            slug = raw_slug[:17]
            bot_username = f"{self._bot_username_prefix}{slug}_bot"
            bot_name = f"{self._bot_name_prefix} ({slug})"

            token = await botfather.create_bot(client, bot_name, bot_username)

            # Send /start to the new bot so it gets initialised
            try:
                await asyncio.wait_for(client.send_message(bot_username, "/start"), timeout=30.0)
            except Exception as exc:
                logger.warning("Could not send /start to @%s: %s", bot_username, exc)

            # Resolve the bot's Telegram ID
            bot_id: int | None = None
            try:
                entity = await asyncio.wait_for(client.get_entity(bot_username), timeout=30.0)
                bot_id = entity.id
            except Exception as exc:
                logger.warning("Could not resolve bot entity for @%s: %s", bot_username, exc)

        bot = NotificationBot(
            tg_user_id=tg_user_id,
            tg_username=tg_username,
            bot_id=bot_id,
            bot_username=bot_username,
            bot_token=token,
        )
        await self._notifications.save_bot(bot)
        logger.info("Notification bot @%s set up for user %s", bot_username, tg_user_id)
        return bot

    async def get_status(self) -> NotificationBot | None:
        """Return bot info for the selected notification account, or None if not set up."""
        async with self._target_service.use_client() as (client, _phone):
            me = await asyncio.wait_for(client.get_me(), timeout=15.0)
        return await self._notifications.get_bot(me.id)

    async def teardown_bot(self) -> None:
        """Delete the notification bot via BotFather and remove it from DB."""
        async with self._target_service.use_client() as (client, _phone):
            me = await asyncio.wait_for(client.get_me(), timeout=15.0)
            tg_user_id: int = me.id
            bot = await self._notifications.get_bot(tg_user_id)
            if bot is None:
                raise RuntimeError("No notification bot found for this user")

            await botfather.delete_bot(client, bot.bot_username)

        await self._notifications.delete_bot(tg_user_id)
        logger.info("Notification bot deleted for user %s", tg_user_id)
