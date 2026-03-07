from __future__ import annotations

from typing import TYPE_CHECKING

from src.database import Database
from src.models import Channel
from src.telegram.client_pool import ClientPool

if TYPE_CHECKING:
    from src.collection_queue import CollectionQueue


class ChannelService:
    def __init__(self, db: Database, pool: ClientPool, queue: CollectionQueue):
        self._db = db
        self._pool = pool
        self._queue = queue

    async def list_for_page(
        self, include_filtered: bool = True
    ) -> tuple[list[Channel], dict]:
        channels = await self._db.get_channels_with_counts(
            include_filtered=include_filtered
        )
        latest_stats = await self._db.get_latest_stats_for_all()
        return channels, latest_stats

    async def add_by_identifier(self, identifier: str) -> bool:
        info = await self._pool.resolve_channel(identifier.strip())
        if not info:
            return False
        channel = Channel(
            channel_id=info["channel_id"],
            title=info["title"],
            username=info["username"],
            channel_type=info.get("channel_type"),
            is_active=not info.get("deactivate", False),
        )
        await self._db.add_channel(channel)
        return True

    async def get_dialogs_with_added_flags(self) -> list[dict]:
        existing = await self._db.get_channels()
        existing_ids = {ch.channel_id for ch in existing}
        dialogs = await self._pool.get_dialogs()
        for dialog in dialogs:
            dialog["already_added"] = dialog["channel_id"] in existing_ids
        return dialogs

    async def add_bulk_by_dialog_ids(self, channel_ids: list[str]) -> None:
        dialogs = await self._pool.get_dialogs()
        dialogs_map = {str(d["channel_id"]): d for d in dialogs}
        for cid in channel_ids:
            if cid not in dialogs_map:
                continue
            dialog = dialogs_map[cid]
            await self._db.add_channel(
                Channel(
                    channel_id=dialog["channel_id"],
                    title=dialog["title"],
                    username=dialog["username"],
                    channel_type=dialog.get("channel_type"),
                    is_active=not dialog.get("deactivate", False),
                )
            )

    async def toggle(self, pk: int) -> None:
        channel = await self._db.get_channel_by_pk(pk)
        if not channel:
            return
        await self._db.set_channel_active(pk, not channel.is_active)

    async def delete(self, pk: int) -> None:
        channel = await self._db.get_channel_by_pk(pk)
        if channel is not None:
            tasks = await self._db.get_active_collection_tasks_for_channel(channel.channel_id)
            for task in tasks:
                if task.id is not None:
                    await self._queue.cancel_task(
                        task.id,
                        note="Канал удалён пользователем.",
                    )
        await self._db.delete_channel(pk)

    async def get_by_pk(self, pk: int) -> Channel | None:
        return await self._db.get_channel_by_pk(pk)
