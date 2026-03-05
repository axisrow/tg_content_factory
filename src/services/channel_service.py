from __future__ import annotations

from src.database import Database
from src.models import Channel
from src.telegram.client_pool import ClientPool


class ChannelService:
    def __init__(self, db: Database, pool: ClientPool):
        self._db = db
        self._pool = pool

    async def list_for_page(self) -> tuple[list[Channel], list, dict]:
        channels = await self._db.get_channels_with_counts()
        keywords = await self._db.get_keywords()
        latest_stats = await self._db.get_latest_stats_for_all()
        return channels, keywords, latest_stats

    async def add_by_identifier(self, identifier: str) -> bool:
        info = await self._pool.resolve_channel(identifier.strip())
        if not info:
            return False
        channel = Channel(
            channel_id=info["channel_id"],
            title=info["title"],
            username=info["username"],
            channel_type=info.get("channel_type"),
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
                )
            )

    async def toggle(self, pk: int) -> None:
        channels = await self._db.get_channels()
        for channel in channels:
            if channel.id == pk:
                await self._db.set_channel_active(pk, not channel.is_active)
                return

    async def delete(self, pk: int) -> None:
        await self._db.delete_channel(pk)

    async def get_by_pk(self, pk: int) -> Channel | None:
        channels = await self._db.get_channels()
        return next((ch for ch in channels if ch.id == pk), None)
