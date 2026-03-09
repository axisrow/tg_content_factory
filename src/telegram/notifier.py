from __future__ import annotations

import asyncio
import logging

import aiohttp

from src.database.bundles import NotificationBundle
from src.services.notification_target_service import NotificationTargetService

logger = logging.getLogger(__name__)

_BOT_API_URL = "https://api.telegram.org/bot{token}/sendMessage"


class Notifier:
    """Send notifications to admin via Telegram."""

    def __init__(
        self,
        target_service: NotificationTargetService,
        admin_chat_id: int | None,
        notification_bundle: NotificationBundle | None = None,
    ):
        self._target_service = target_service
        self._admin_chat_id = admin_chat_id
        self._notification_bundle = notification_bundle
        self._cached_me_id: int | None = None

    @property
    def admin_chat_id(self) -> int | None:
        return self._admin_chat_id

    async def notify(self, text: str) -> bool:
        try:
            # Fast path: if me.id is cached and a bot is configured, skip the
            # Telegram client entirely — _send_via_bot_api is a pure HTTP call.
            if self._notification_bundle is not None and self._cached_me_id is not None:
                bot = await self._notification_bundle.get_bot(self._cached_me_id)
                if bot is not None:
                    return await _send_via_bot_api(bot.bot_token, self._cached_me_id, text)

            # Slow path: need a client either to populate me.id or to send directly.
            async with self._target_service.use_client() as (client, _phone):
                if self._notification_bundle is not None:
                    if self._cached_me_id is None:
                        me = await asyncio.wait_for(client.get_me(), timeout=15.0)
                        self._cached_me_id = me.id
                    bot = await self._notification_bundle.get_bot(self._cached_me_id)
                    if bot is not None:
                        return await _send_via_bot_api(bot.bot_token, self._cached_me_id, text)
                    logger.warning(
                        "No bot found for account %s, falling back to direct message "
                        "(push notifications will not be delivered)",
                        self._cached_me_id,
                    )
                target = self._admin_chat_id or "me"
                await asyncio.wait_for(client.send_message(target, text), timeout=30.0)
            return True
        except Exception as e:
            logger.error("Failed to send notification: %s", e)
            return False


async def _send_via_bot_api(token: str, chat_id: int, text: str) -> bool:
    url = _BOT_API_URL.format(token=token)
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(
                url,
                json={"chat_id": chat_id, "text": text},
                timeout=aiohttp.ClientTimeout(total=30),
            ) as resp:
                data = await resp.json()
                if not data.get("ok"):
                    logger.error("Bot API error: %s", data)
                    return False
        return True
    except Exception as e:
        logger.error("Bot API call failed: %s", e)
        return False
