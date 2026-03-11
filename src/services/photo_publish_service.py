from __future__ import annotations

import logging
from datetime import datetime

from telethon.errors import FloodWaitError

from src.models import PhotoSendMode
from src.telegram.client_pool import ClientPool

logger = logging.getLogger(__name__)


class PhotoPublishService:
    def __init__(self, pool: ClientPool):
        self._pool = pool

    async def send_now(
        self,
        *,
        phone: str,
        target_dialog_id: int,
        file_paths: list[str],
        send_mode: PhotoSendMode,
        caption: str | None = None,
        schedule_at: datetime | None = None,
    ) -> list[int]:
        result = await self._pool.get_client_by_phone(phone)
        if result is None:
            raise RuntimeError("no_client")
        client, acquired_phone = result
        try:
            if send_mode == PhotoSendMode.ALBUM and len(file_paths) > 1:
                sent = await client.send_file(
                    target_dialog_id,
                    file_paths,
                    caption=caption,
                    schedule=schedule_at,
                )
                return [int(msg.id) for msg in sent]

            message_ids: list[int] = []
            for path in file_paths:
                sent = await client.send_file(
                    target_dialog_id,
                    path,
                    caption=caption,
                    schedule=schedule_at,
                )
                message_ids.append(int(sent.id))
            return message_ids
        except FloodWaitError as exc:
            wait_for = max(1, int(getattr(exc, "seconds", 0) or 0))
            await self._pool.report_flood(acquired_phone, wait_for)
            logger.warning("Photo send flood wait for %s: %s", acquired_phone, wait_for)
            raise
        finally:
            await self._pool.release_client(acquired_phone)
