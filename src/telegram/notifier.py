from __future__ import annotations

import logging

from telethon import TelegramClient

logger = logging.getLogger(__name__)


class Notifier:
    """Send notifications to admin via Telegram."""

    def __init__(self, client: TelegramClient | None, admin_chat_id: int | None):
        self._client = client
        self._admin_chat_id = admin_chat_id

    async def notify(self, text: str) -> bool:
        if not self._client or not self._admin_chat_id:
            logger.info("Notification (no target): %s", text[:100])
            return False

        try:
            await self._client.send_message(self._admin_chat_id, text)
            return True
        except Exception as e:
            logger.error("Failed to send notification: %s", e)
            return False
