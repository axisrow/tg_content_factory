from __future__ import annotations

import inspect
import logging
from datetime import datetime

from telethon.errors import FloodWaitError

from src.models import PhotoSendMode
from src.telegram.backends import adapt_transport_session
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
        target_type: str | None = None,
        file_paths: list[str],
        send_mode: PhotoSendMode,
        caption: str | None = None,
        schedule_at: datetime | None = None,
    ) -> list[int]:
        result = await self._pool.get_client_by_phone(phone)
        if result is None:
            raise RuntimeError("no_client")
        session, acquired_phone = result
        session = adapt_transport_session(session, disconnect_on_close=False)
        try:
            entity = target_dialog_id
            resolver = getattr(self._pool, "resolve_dialog_entity", None)
            if callable(resolver):
                resolved = resolver(
                    session,
                    acquired_phone,
                    target_dialog_id,
                    target_type,
                )
                entity = await resolved if inspect.isawaitable(resolved) else resolved
            if send_mode == PhotoSendMode.ALBUM and len(file_paths) > 1:
                sent = await session.publish_files(
                    entity,
                    file_paths,
                    caption=caption,
                    schedule=schedule_at,
                )
                return [int(msg.id) for msg in sent]

            message_ids: list[int] = []
            for path in file_paths:
                sent = await session.publish_files(
                    entity,
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
