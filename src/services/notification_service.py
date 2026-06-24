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
                logger.warning("slug '%s' truncated to 17 characters for bot username", raw_slug)
            slug = raw_slug[:17]
            bot_username = f"{self._bot_username_prefix}{slug}_bot"
            bot_name = f"{self._bot_name_prefix} ({slug})"

            token = await botfather.create_bot(client, bot_name, bot_username)

            # Send /start to the new bot so it gets initialised
            try:
                await asyncio.wait_for(client.send_message(bot_username, "/start"), timeout=30.0)
            except Exception:
                logger.warning("Could not send /start to @%s", bot_username, exc_info=True)

            # Resolve the bot's Telegram ID
            bot_id: int | None = None
            try:
                entity = await asyncio.wait_for(client.get_entity(bot_username), timeout=30.0)
                bot_id = entity.id
            except Exception:
                logger.warning("Could not resolve bot entity for @%s", bot_username, exc_info=True)

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

    async def send_notification(self, message: str) -> bool:
        """Send a one-off notification via the configured bot (or direct message fallback).

        Mirrors the worker's ``notifications.test`` command handler: routes through
        :class:`~src.telegram.notifier.Notifier`, which prefers the personal bot
        (delivers push notifications) and falls back to a self-message otherwise.
        Always returns ``True`` on success; raises
        ``RuntimeError('notification_test_failed')`` if delivery fails (it never
        returns ``False``).
        """
        from src.telegram.notifier import Notifier

        notifier = Notifier(self._target_service, None, self._notifications)
        text = (message or "").strip() or "✅ Тест уведомлений: соединение установлено"
        ok = await notifier.notify(text)
        if not ok:
            raise RuntimeError("notification_test_failed")
        return True

    async def teardown_bot(self) -> None:
        """Delete the notification bot via BotFather and remove it from DB.

        Idempotent after a remote delete (issue #1085): if the bot is already
        gone from Telegram, ``botfather.delete_bot`` raises
        :class:`~src.telegram.botfather.BotNotFoundError`. That is treated as
        "the Telegram delete already happened" — we skip the TG step and proceed
        straight to the DB-row cleanup, so a repeat teardown can finally remove
        an orphan row left behind when a prior DB-delete failed (#1041). Any
        *other* BotFather error means the live bot may still exist, so it is
        re-raised *before* the DB-delete: we never wipe the row while the bot
        might still be reachable in Telegram.
        """
        async with self._target_service.use_client() as (client, _phone):
            me = await asyncio.wait_for(client.get_me(), timeout=15.0)
            tg_user_id: int = me.id
            bot = await self._notifications.get_bot(tg_user_id)
            if bot is None:
                raise RuntimeError("No notification bot found for this user")

            try:
                await botfather.delete_bot(client, bot.bot_username)
            except botfather.BotNotFoundError:
                # The bot is no longer in /mybots — it was already deleted in
                # Telegram (e.g. a previous teardown that then failed at the
                # DB-delete step). Fall through to the local cleanup so the
                # orphan row can be removed; this is what makes teardown
                # idempotent and recoverable, not just observable.
                logger.info(
                    "Notification bot @%s already absent from Telegram; "
                    "proceeding with local DB cleanup for user %s",
                    bot.bot_username,
                    tg_user_id,
                )

        # BotFather already destroyed the live bot. If the DB row delete now
        # fails the row becomes an orphan: get_status() keeps reporting the bot
        # as configured while it no longer exists in Telegram (issue #1041).
        # Surface that loudly so the operator can clean the stale row instead of
        # letting it fail silently.
        try:
            await self._notifications.delete_bot(tg_user_id)
        except Exception:
            logger.error(
                "Orphan notification bot record: @%s (user %s) was deleted in "
                "Telegram via BotFather but its DB row could not be removed; "
                "the row is now stale and must be cleaned up manually",
                bot.bot_username,
                tg_user_id,
                exc_info=True,
            )
            raise
        logger.info("Notification bot deleted for user %s", tg_user_id)
