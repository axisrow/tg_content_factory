"""Persistence and cache publication for one full or partial dialog sweep."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from tg_agent.telegram.client_pool import ClientPool
    from tg_agent.telegram.pool_dialogs import DialogFetchStats

logger = logging.getLogger("tg_agent.telegram.pool_dialogs")


class DialogSnapshotWriter:
    def __init__(
        self, pool: ClientPool, phone: str, cache_mode: str,
        stats: DialogFetchStats, *, chunk_size: int,
    ) -> None:
        self._pool = pool
        self.phone = phone
        self.cache_mode = cache_mode
        self.stats = stats
        self.chunk_size = chunk_size
        self.items: list[dict] = []
        self.pending: list[dict] = []

    async def record(self, item: dict) -> None:
        """Record one reached dialog, flushing full-mode chunks as they fill."""
        self.items.append(item)
        if self.cache_mode != "full":
            # channels_only carries no dm/bot rows; flushing it would
            # look like those dialogs vanished.
            return
        self.pending.append(item)
        if len(self.pending) >= self.chunk_size:
            self.stats.saved += await self._pool._flush_dialog_chunk(self.phone, self.pending)

    async def finish(self) -> None:
        """Publish a complete snapshot, or retain partial progress without deletion."""
        self._pool.invalidate_dialogs_cache(self.phone)
        if self.stats.partial:
            # Only the partial path needs this: the success path below
            # rewrites the whole snapshot with replace_dialogs, so flushing
            # the tail first would upsert ~N rows and then immediately
            # DELETE+INSERT them again (#1365: the DB is already 45 GB).
            self.stats.saved += await self._pool._flush_dialog_chunk(self.phone, self.pending)
            # The sweep was cut short. What did arrive is already persisted
            # by the chunk flushes above, so the cache moves forward instead
            # of staying frozen (#1359) -- but nothing is deleted, since an
            # incomplete sweep is no evidence a dialog is gone. The
            # in-memory cache is deliberately NOT filled: a partial snapshot
            # would be served as complete for the whole TTL.
            logger.warning(
                "get_dialogs_for_phone: partial sweep for %s -- %d dialogs saved, "
                "stale rows kept",
                self.phone,
                self.stats.saved,
            )
        else:
            self._pool._store_cached_dialogs(self.phone, self.cache_mode, self.items)
            if self.cache_mode == "full":
                self._pool.mark_dialogs_fetched(self.phone)
                # A completed sweep is a trustworthy full snapshot.
                await self._pool._db.repos.dialog_cache.replace_dialogs(self.phone, self.items)
            # replace_dialogs already wrote every reached dialog with a
            # current timestamp. The unflushed tail is part of that write,
            # so the finally-flush below must not upsert it again: doing so
            # re-ages the whole phone to the epoch and makes the completed
            # snapshot read as stale (#1359).
            self.pending.clear()

    async def flush_remaining(self) -> None:
        """Retain the unflushed tail on an exception or cancellation."""
        if self.pending:
            try:
                self.stats.saved += await self._pool._flush_dialog_chunk(self.phone, self.pending)
            except Exception:  # pragma: no cover - never mask the real error
                logger.warning(
                    "get_dialogs_for_phone: could not flush %d pending dialogs for %s",
                    len(self.pending),
                    self.phone,
                    exc_info=True,
                )
