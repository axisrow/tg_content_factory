from __future__ import annotations

import inspect
import logging
from collections.abc import Awaitable, Callable
from contextlib import asynccontextmanager
from datetime import datetime

from telethon_floodgate import run_with_flood_wait

from tg_agent.models import PhotoSendMode
from tg_agent.telegram.backends import adapt_transport_session
from tg_agent.telegram.client_pool import ClientPool

logger = logging.getLogger(__name__)


class PhotoPublishService:
    def __init__(self, pool: ClientPool):
        self._pool = pool

    @asynccontextmanager
    async def _acquire_client_and_resolve(
        self,
        phone: str,
        target_dialog_id: int,
        target_type: str | None,
    ):
        """Acquire a flood-aware client for ``phone``, resolve the target entity,
        and yield ``(session, acquired_phone, entity)``; always release on exit.

        Raises ``RuntimeError("no_client")`` when no client is available, matching
        the per-method contract before this prelude was extracted.
        """
        result = await self._pool.get_client_by_phone(phone, wait_for_flood=True)
        if result is None:
            raise RuntimeError("no_client")
        session, acquired_phone = result
        session = adapt_transport_session(session, disconnect_on_close=False)
        try:
            entity = target_dialog_id
            resolver = getattr(self._pool, "resolve_dialog_entity", None)
            if callable(resolver):
                resolved = resolver(session, acquired_phone, target_dialog_id, target_type)
                entity = await resolved if inspect.isawaitable(resolved) else resolved
            yield session, acquired_phone, entity
        finally:
            await self._pool.release_client(acquired_phone)

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
        on_file_sent: Callable[[str, list[int]], Awaitable[None]] | None = None,
    ) -> list[int]:
        from tg_agent.gateway.pool import GatewayPool

        if isinstance(self._pool, GatewayPool):
            from tg_agent.gateway.context import next_intent_key
            from tg_agent.gateway.files import publish_files

            key = next_intent_key()
            groups = ([file_paths] if send_mode == PhotoSendMode.ALBUM and len(file_paths) > 1
                      else [[path] for path in file_paths])
            message_ids = []
            for index, paths in enumerate(groups):
                result = await publish_files(self._pool, phone=phone, peer=target_dialog_id, files=paths,
                    intent=f"{key}:group:{index}", caption=caption, schedule=schedule_at,
                    peer_type={"channel": "channel", "supergroup": "channel", "group": "chat",
                               "user": "user", "bot": "user", "saved": "user"}.get(target_type, "auto"))
                ids = result["message_ids"]
                message_ids.extend(ids)
                if on_file_sent is not None:
                    for path in paths:
                        await on_file_sent(path, ids)
            return message_ids
        async with self._acquire_client_and_resolve(
            phone, target_dialog_id, target_type
        ) as (session, acquired_phone, entity):
            if send_mode == PhotoSendMode.ALBUM and len(file_paths) > 1:
                sent = await run_with_flood_wait(
                    session.publish_files(
                        entity,
                        file_paths,
                        caption=caption,
                        schedule=schedule_at,
                    ),
                    operation="photo_publish_album",
                    phone=acquired_phone,
                    pool=self._pool,
                    logger_=logger,
                )
                album_ids = [int(msg.id) for msg in sent]
                if on_file_sent is not None:
                    # Atomic album send — record all files only after it succeeds.
                    for path in file_paths:
                        await on_file_sent(path, album_ids)
                return album_ids

            message_ids: list[int] = []
            for path in file_paths:
                sent = await run_with_flood_wait(
                    session.publish_files(
                        entity,
                        path,
                        caption=caption,
                        schedule=schedule_at,
                    ),
                    operation="photo_publish_single",
                    phone=acquired_phone,
                    pool=self._pool,
                    logger_=logger,
                )
                msg_id = int(sent.id)
                message_ids.append(msg_id)
                # Record progress per file so a mid-batch failure doesn't cause the
                # already-sent files to be re-sent next cycle (audit #835/4).
                if on_file_sent is not None:
                    await on_file_sent(path, [msg_id])
            return message_ids

    async def unschedule(
        self,
        *,
        phone: str,
        target_dialog_id: int,
        target_type: str | None = None,
        message_ids: list[int],
    ) -> None:
        """Cancel previously server-scheduled messages on Telegram (audit #835/3)."""
        if not message_ids:
            return
        from tg_agent.gateway.pool import GatewayPool

        if isinstance(self._pool, GatewayPool):
            from uuid import UUID

            from tg_agent.gateway.context import next_intent_key
            from tg_agent.gateway.protocol import ScheduledDelete

            account = self._pool.select(phone)
            if account["phone"] != phone:
                raise PermissionError("Scheduling account is not granted to this project")
            await self._pool.request(ScheduledDelete(kind="scheduled_delete", account_id=UUID(account["id"]),
                peer=target_dialog_id, message_ids=message_ids,
                peer_type={"group": "chat", "saved": "user", "dm": "user", "bot": "user"}.get(
                    target_type, "channel")), next_intent_key())
            return
        async with self._acquire_client_and_resolve(
            phone, target_dialog_id, target_type
        ) as (session, acquired_phone, entity):
            await run_with_flood_wait(
                session.delete_scheduled_messages(entity, message_ids),
                operation="photo_unschedule",
                phone=acquired_phone,
                pool=self._pool,
                logger_=logger,
            )
