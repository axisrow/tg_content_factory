from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from src.database import Database

if TYPE_CHECKING:
    from src.services.channel_service import ChannelService

logger = logging.getLogger(__name__)


@dataclass(slots=True)
class PurgeResult:
    purged_count: int = 0
    skipped_count: int = 0
    purged_titles: list[str] = field(default_factory=list)
    total_messages_deleted: int = 0


class FilterDeletionService:
    def __init__(
        self,
        db: Database,
        channel_service: ChannelService | None = None,
    ):
        self._db = db
        self._channel_service = channel_service

    async def purge_channels_by_pks(self, pks: list[int]) -> PurgeResult:
        """Soft-delete: keep channel in DB, purge its messages."""
        result = PurgeResult()
        for pk in pks:
            try:
                channel = await self._db.get_channel_by_pk(pk)
                if not channel:
                    result.skipped_count += 1
                    continue
                if not channel.is_filtered:
                    logger.warning("Skipping pk=%d (%s): not filtered", pk, channel.title)
                    result.skipped_count += 1
                    continue
                title = channel.title or f"pk={pk}"
                deleted = await self._db.delete_messages_for_channel(channel.channel_id)
                result.purged_count += 1
                result.purged_titles.append(title)
                result.total_messages_deleted += deleted
            except Exception:
                logger.exception("Failed to purge channel pk=%d", pk)
                result.skipped_count += 1
        return result

    async def purge_all_filtered(self) -> PurgeResult:
        channels = await self._db.get_channels_with_counts(
            active_only=False,
            include_filtered=True,
        )
        pks = [ch.id for ch in channels if ch.is_filtered and ch.id is not None]
        if not pks:
            return PurgeResult()
        return await self.purge_channels_by_pks(pks)

    async def hard_delete_channels_by_pks(self, pks: list[int]) -> PurgeResult:
        """Hard-delete: remove channel + messages from DB entirely. Dev mode only."""
        if self._channel_service is None:
            raise RuntimeError("hard_delete requires channel_service (dev mode)")
        result = PurgeResult()
        for pk in pks:
            try:
                channel = await self._channel_service.get_by_pk(pk)
                if not channel:
                    result.skipped_count += 1
                    continue
                if not channel.is_filtered:
                    logger.warning("Skipping pk=%d (%s): not filtered", pk, channel.title)
                    result.skipped_count += 1
                    continue
                title = channel.title or f"pk={pk}"
                await self._channel_service.delete(pk)
                result.purged_count += 1
                result.purged_titles.append(title)
            except Exception:
                logger.exception("Failed to hard-delete channel pk=%d", pk)
                result.skipped_count += 1
        return result
