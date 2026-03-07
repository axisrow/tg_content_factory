from __future__ import annotations

import logging

from src.services.notification_target_service import NotificationTargetService

logger = logging.getLogger(__name__)


class Notifier:
    """Send notifications to admin via Telegram."""

    def __init__(
        self,
        target_service: NotificationTargetService,
        admin_chat_id: int | None,
    ):
        self._target_service = target_service
        self._admin_chat_id = admin_chat_id

    async def notify(self, text: str) -> bool:
        if not self._admin_chat_id:
            logger.info("Notification (no target): %s", text[:100])
            return False

        try:
            async with self._target_service.use_client() as (client, _phone):
                await client.send_message(self._admin_chat_id, text)
            return True
        except Exception as e:
            logger.error("Failed to send notification: %s", e)
            return False
