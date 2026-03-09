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

    async def notify(self, text: str) -> bool:
        try:
            async with self._target_service.use_client() as (client, _phone):
                if self._notification_bundle is not None:
                    me = await asyncio.wait_for(client.get_me(), timeout=15.0)
                    bot = await self._notification_bundle.get_bot(me.id)
                    if bot is not None:
                        return await _send_via_bot_api(bot.bot_token, me.id, text)
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
