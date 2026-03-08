from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone

from src.database import Database
from src.database.bundles import ChannelBundle
from src.models import (
    CollectionTask,
    CollectionTaskStatus,
    StatsAllTaskPayload,
)
from src.telegram.collector import Collector

logger = logging.getLogger(__name__)


class _DatabaseChannelsAdapter:
    _RENAMES = {
        "get_by_channel_id": "get_channel_by_channel_id",
    }
    _ALLOWED = {
        "get_collection_task",
        "update_collection_task",
        "update_collection_task_progress",
        "requeue_running_stats_tasks_on_startup",
        "claim_next_due_stats_task",
        "create_stats_continuation_task",
    }

    def __init__(self, db: Database):
        self._db = db

    def __getattr__(self, name: str):
        if name in self._RENAMES:
            return getattr(self._db, self._RENAMES[name])
        if name in self._ALLOWED:
            return getattr(self._db, name)
        if name not in self._RENAMES and name not in self._ALLOWED:
            raise AttributeError(f"{type(self).__name__!s} has no attribute {name!r}")
        raise AssertionError("unreachable")


class StatsTaskDispatcher:
    """Runs deferred / pending stats collection tasks from DB."""

    def __init__(
        self,
        collector: Collector,
        channels: ChannelBundle | Database,
        *,
        default_batch_size: int = 20,
        poll_interval_sec: float = 1.0,
        channel_timeout_sec: float = 120.0,
    ):
        self._collector = collector
        if isinstance(channels, Database):
            channels = _DatabaseChannelsAdapter(channels)
        self._channels = channels
        self._default_batch_size = default_batch_size
        self._poll_interval_sec = poll_interval_sec
        self._channel_timeout_sec = channel_timeout_sec
        self._task: asyncio.Task | None = None
        self._stop_event = asyncio.Event()

    async def start(self) -> None:
        if self._task and not self._task.done():
            return
        self._stop_event.clear()
        recovered = await self._channels.requeue_running_stats_tasks_on_startup(
            datetime.now(timezone.utc)
        )
        if recovered:
            logger.warning("Recovered %d interrupted stats tasks on startup", recovered)
        self._task = asyncio.create_task(self._run_loop())

    async def stop(self) -> None:
        self._stop_event.set()
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None

    async def _run_loop(self) -> None:
        while not self._stop_event.is_set():
            task: CollectionTask | None = None
            try:
                if self._collector.is_running:
                    await asyncio.sleep(self._poll_interval_sec)
                    continue

                task = await self._channels.claim_next_due_stats_task(datetime.now(timezone.utc))
                if task is None:
                    await asyncio.sleep(self._poll_interval_sec)
                    continue

                await self._run_stats_task(task)
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception("Stats dispatcher loop failure")
                if task and task.id is not None:
                    try:
                        fresh = await self._channels.get_collection_task(task.id)
                        if fresh and fresh.status == CollectionTaskStatus.RUNNING:
                            await self._channels.update_collection_task(
                                task.id,
                                CollectionTaskStatus.FAILED,
                                messages_collected=fresh.messages_collected,
                                error="Stats task failed with unexpected dispatcher error",
                            )
                    except Exception:
                        logger.exception("Failed to mark broken stats task as failed")
                await asyncio.sleep(self._poll_interval_sec)

    async def _run_stats_task(self, task: CollectionTask) -> None:
        if task.id is None:
            return

        payload = task.payload
        if not isinstance(payload, StatsAllTaskPayload):
            await self._channels.update_collection_task(
                task.id,
                CollectionTaskStatus.FAILED,
                error="Unsupported stats task payload",
            )
            return

        channel_ids = payload.channel_ids
        next_index = payload.next_index
        batch_size = max(1, payload.batch_size or self._default_batch_size)
        channels_ok = payload.channels_ok or (task.messages_collected or 0)
        channels_err = payload.channels_err

        logger.info(
            "Running stats task #%s: next_index=%d batch_size=%d total=%d",
            task.id,
            next_index,
            batch_size,
            len(channel_ids),
        )

        if next_index >= len(channel_ids):
            await self._channels.update_collection_task(
                task.id,
                CollectionTaskStatus.COMPLETED,
                messages_collected=channels_ok,
            )
            return

        batch_end = min(next_index + batch_size, len(channel_ids))
        cursor = next_index

        while cursor < batch_end:
            channel_id = channel_ids[cursor]
            logger.info(
                "Stats task #%s: processing channel %d/%d (channel_id=%s)",
                task.id,
                cursor + 1,
                len(channel_ids),
                channel_id,
            )
            channel = await self._channels.get_by_channel_id(channel_id)
            if channel is None:
                logger.warning(
                    "Stats task #%s: channel_id=%s not found, skipping",
                    task.id,
                    channel_id,
                )
                channels_err += 1
                cursor += 1
                continue

            try:
                result = await asyncio.wait_for(
                    self._collector.collect_channel_stats(channel),
                    timeout=self._channel_timeout_sec,
                )
            except asyncio.TimeoutError:
                logger.error(
                    "Stats timeout for channel %s in task #%s",
                    channel.channel_id,
                    task.id,
                )
                channels_err += 1
                cursor += 1
            except Exception as exc:
                logger.error("Stats error for channel %s: %s", channel.channel_id, exc)
                channels_err += 1
                cursor += 1
            else:
                if result is None:
                    availability = await self._collector.get_stats_availability()
                    if (
                        availability.state == "all_flooded"
                        and availability.next_available_at_utc is not None
                    ):
                        logger.warning(
                            "Stats task #%s deferred: all clients flood-waited until %s",
                            task.id,
                            availability.next_available_at_utc.isoformat(),
                        )
                        continuation_payload = self._build_payload(
                            channel_ids=channel_ids,
                            next_index=cursor,
                            batch_size=batch_size,
                            channels_ok=channels_ok,
                            channels_err=channels_err,
                        )
                        continuation_id = await self._channels.create_stats_continuation_task(
                            payload=continuation_payload,
                            run_after=availability.next_available_at_utc,
                            parent_task_id=task.id,
                        )
                        await self._channels.update_collection_task(
                            task.id,
                            CollectionTaskStatus.FAILED,
                            messages_collected=channels_ok,
                            error=(
                                "Deferred to task "
                                f"#{continuation_id} until "
                                f"{availability.next_available_at_utc.isoformat()} "
                                "(all clients flood-waited)"
                            ),
                        )
                        return

                    logger.error(
                        "Stats task #%s failed: no active connected Telegram accounts",
                        task.id,
                    )
                    await self._channels.update_collection_task(
                        task.id,
                        CollectionTaskStatus.FAILED,
                        messages_collected=channels_ok,
                        error="No active connected Telegram accounts",
                    )
                    return

                channels_ok += 1
                cursor += 1
                await self._channels.update_collection_task_progress(task.id, channels_ok)
                logger.info(
                    "Stats task #%s: channel_id=%s done (ok=%d, err=%d)",
                    task.id,
                    channel.channel_id,
                    channels_ok,
                    channels_err,
                )

            if cursor < batch_end:
                await asyncio.sleep(self._collector.delay_between_channels_sec)

        if cursor < len(channel_ids):
            continuation_payload = self._build_payload(
                channel_ids=channel_ids,
                next_index=cursor,
                batch_size=batch_size,
                channels_ok=channels_ok,
                channels_err=channels_err,
            )
            await self._channels.create_stats_continuation_task(
                payload=continuation_payload,
                run_after=datetime.now(timezone.utc),
                parent_task_id=task.id,
            )
            logger.info(
                "Stats task #%s batch complete: processed=%d/%d, continuation created",
                task.id,
                cursor,
                len(channel_ids),
            )
            await self._channels.update_collection_task(
                task.id,
                CollectionTaskStatus.COMPLETED,
                messages_collected=channels_ok,
            )
            return

        logger.info("Stats task #%s finished: processed all %d channels", task.id, len(channel_ids))
        await self._channels.update_collection_task(
            task.id,
            CollectionTaskStatus.COMPLETED,
            messages_collected=channels_ok,
        )

    @staticmethod
    def _build_payload(
        *,
        channel_ids: list[int],
        next_index: int,
        batch_size: int,
        channels_ok: int,
        channels_err: int,
    ) -> StatsAllTaskPayload:
        return StatsAllTaskPayload(
            channel_ids=channel_ids,
            next_index=next_index,
            batch_size=batch_size,
            channels_ok=channels_ok,
            channels_err=channels_err,
        )
