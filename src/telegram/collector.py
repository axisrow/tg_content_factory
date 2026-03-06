from __future__ import annotations

import asyncio
import logging
import re
from collections.abc import Awaitable, Callable
from datetime import datetime, timezone

from telethon.errors import FloodWaitError, UsernameInvalidError, UsernameNotOccupiedError
from telethon.tl.functions.channels import GetFullChannelRequest
from telethon.tl.types import (
    DocumentAttributeAnimated,
    DocumentAttributeAudio,
    DocumentAttributeSticker,
    DocumentAttributeVideo,
    MessageMediaContact,
    MessageMediaDice,
    MessageMediaDocument,
    MessageMediaGame,
    MessageMediaGeo,
    MessageMediaGeoLive,
    MessageMediaPhoto,
    MessageMediaPoll,
    MessageMediaWebPage,
    PeerChannel,
)

from src.config import SchedulerConfig
from src.database import Database
from src.filters.criteria import (
    LOW_SUBSCRIBER_RATIO_CHAT_THRESHOLD,
    LOW_SUBSCRIBER_RATIO_THRESHOLD,
    LOW_UNIQUENESS_THRESHOLD,
    PRECHECK_CROSS_DUPE_MIN_SAMPLE,
    PRECHECK_CROSS_DUPE_RATIO,
    PRECHECK_CROSS_DUPE_SAMPLE,
)
from src.models import Channel, ChannelStats, Message
from src.telegram.client_pool import ClientPool
from src.telegram.notifier import Notifier

logger = logging.getLogger(__name__)


class NoActiveStatsClientsError(RuntimeError):
    """Raised when there are no active connected clients for stats collection."""


class AllStatsClientsFloodedError(RuntimeError):
    """Raised when all active connected clients are in flood-wait."""

    def __init__(self, retry_after_sec: int, next_available_at: datetime):
        super().__init__(
            "All active clients are flood-waited until "
            f"{next_available_at.isoformat()} (retry in {retry_after_sec}s)"
        )
        self.retry_after_sec = retry_after_sec
        self.next_available_at = next_available_at


class Collector:
    def __init__(
        self,
        pool: ClientPool,
        db: Database,
        config: SchedulerConfig,
        notifier: Notifier | None = None,
    ):
        self._pool = pool
        self._db = db
        self._config = config
        self._notifier = notifier
        self._running = False
        self._stats_running = False
        self._cancel_event = asyncio.Event()
        self._lock = asyncio.Lock()
        self._stats_lock = asyncio.Lock()

    @property
    def is_running(self) -> bool:
        return self._running or self._stats_running

    @property
    def is_stats_running(self) -> bool:
        return self._stats_running

    @property
    def delay_between_channels_sec(self) -> int:
        return self._config.delay_between_channels_sec

    async def get_stats_availability(self):
        return await self._pool.get_stats_availability()

    async def cancel(self) -> None:
        self._cancel_event.set()

    @property
    def is_cancelled(self) -> bool:
        return self._cancel_event.is_set()

    async def collect_single_channel(
        self,
        channel: Channel,
        *,
        full: bool = False,
        progress_callback: Callable[[int], Awaitable[None]] | None = None,
        force: bool = False,
    ) -> int:
        """Collect messages from a single channel. If full=True, reset last_collected_id to 0.

        This is the canonical entry point for is_filtered checks in the
        collection path.  Other callers (CollectionQueue, CLI, web routes)
        may also guard against filtered channels earlier for better UX,
        but this check is the authoritative gate.
        """
        if channel.is_filtered and not force:
            logger.info(
                "Skipping collection for channel %d: channel is filtered",
                channel.channel_id,
            )
            return 0
        async with self._lock:
            self._running = True
            self._cancel_event.clear()
            try:
                result = await self._pool.get_available_client()
                if result:
                    client, phone = result
                    try:
                        await asyncio.wait_for(client.get_dialogs(), timeout=30)
                    except Exception:
                        pass
                    finally:
                        await self._pool.release_client(phone)

                if full:
                    channel = Channel(**{**channel.model_dump(), "last_collected_id": 0})

                min_subs_raw = await self._db.get_setting("min_subscribers_filter")
                min_subs = int(min_subs_raw) if min_subs_raw else 0
                return await self._collect_channel(
                    channel, progress_callback=progress_callback, force=force, min_subs=min_subs
                )
            finally:
                self._running = False

    async def collect_all_channels(self) -> dict:
        """Collect messages from all active channels. Returns stats."""
        async with self._lock:
            self._running = True
            self._cancel_event.clear()
            stats = {"channels": 0, "messages": 0, "errors": 0}

            try:
                channels = await self._db.get_channels(
                    active_only=True, include_filtered=False
                )
                if not channels:
                    logger.info("No active unfiltered channels to collect")
                    return stats
                logger.info("Found %d active unfiltered channels to collect", len(channels))

                # Pre-fetch dialogs to populate Telethon entity cache.
                # StringSession loses entity cache between restarts, so
                # get_dialogs() is needed for PeerChannel lookups to work.
                result = await self._pool.get_available_client()
                if result:
                    client, phone = result
                    try:
                        logger.info("Pre-fetching dialogs...")
                        await asyncio.wait_for(client.get_dialogs(), timeout=30)
                        logger.info("Dialogs pre-fetched successfully")
                    except Exception as e:
                        logger.warning("Failed to pre-fetch dialogs: %s", e)
                    finally:
                        await self._pool.release_client(phone)

                min_subs_raw = await self._db.get_setting("min_subscribers_filter")
                min_subs = int(min_subs_raw) if min_subs_raw else 0

                for channel in channels:
                    if self._cancel_event.is_set():
                        logger.info("Collection cancelled")
                        break
                    try:
                        collected = await self._collect_channel(channel, min_subs=min_subs)
                        stats["channels"] += 1
                        stats["messages"] += collected
                        await asyncio.sleep(self._config.delay_between_channels_sec)
                    except Exception as e:
                        logger.error("Error collecting channel %s: %s", channel.channel_id, e)
                        stats["errors"] += 1
            finally:
                self._running = False

        logger.info(
            "Collection done: %d channels, %d messages, %d errors",
            stats["channels"],
            stats["messages"],
            stats["errors"],
        )
        return stats

    @staticmethod
    def _get_media_type(msg) -> str | None:
        """Determine media type from a Telethon message."""
        media = msg.media
        if media is None:
            return None
        if isinstance(media, MessageMediaPhoto):
            return "photo"
        if isinstance(media, MessageMediaDocument):
            doc = media.document
            if doc and hasattr(doc, "attributes"):
                for attr in doc.attributes:
                    if isinstance(attr, DocumentAttributeSticker):
                        return "sticker"
                    if isinstance(attr, DocumentAttributeVideo):
                        return "video_note" if getattr(attr, "round_message", False) else "video"
                    if isinstance(attr, DocumentAttributeAudio):
                        return "voice" if getattr(attr, "voice", False) else "audio"
                    if isinstance(attr, DocumentAttributeAnimated):
                        return "gif"
            return "document"
        if isinstance(media, MessageMediaWebPage):
            return "web_page"
        if isinstance(media, MessageMediaGeo):
            return "location"
        if isinstance(media, MessageMediaGeoLive):
            return "geo_live"
        if isinstance(media, MessageMediaContact):
            return "contact"
        if isinstance(media, MessageMediaPoll):
            return "poll"
        if isinstance(media, MessageMediaDice):
            return "dice"
        if isinstance(media, MessageMediaGame):
            return "game"
        return "unknown"

    async def _collect_channel(
        self,
        channel: Channel,
        progress_callback: Callable[[int], Awaitable[None]] | None = None,
        force: bool = False,
        min_subs: int = 0,
    ) -> int:
        """Collect new messages from a single channel. Returns count."""
        channel_id = channel.channel_id
        min_id = channel.last_collected_id

        result = await self._pool.get_available_client()
        if result is None:
            logger.error("No available clients for collection")
            return 0

        client, phone = result
        messages_batch: list[Message] = []
        all_messages: list[Message] = []
        # Tracks the highest message_id seen in this run.
        # Initialized to min_id so the guard `max_msg_id > min_id`
        # prevents a spurious DB update when no messages are collected.
        max_msg_id = min_id
        flood_wait_sec: int | None = None

        is_first_run = channel.last_collected_id == 0
        limit = None if is_first_run else self._config.messages_per_channel
        logger.info(
            "Collecting channel %d (%s), first_run=%s, min_id=%d, limit=%s",
            channel_id, channel.username or channel.title, is_first_run, min_id, limit,
        )

        try:
            if channel.username:
                try:
                    entity = await asyncio.wait_for(
                        client.get_entity(channel.username), timeout=30.0
                    )
                except asyncio.TimeoutError:
                    logger.warning("get_entity timed out for channel %d, skipping", channel_id)
                    return 0
                except (ValueError, UsernameNotOccupiedError, UsernameInvalidError):
                    logger.warning(
                        "Channel %d (%s): username not found, trying numeric ID fallback",
                        channel_id, channel.username,
                    )
                    try:
                        fallback_entity = await asyncio.wait_for(
                            client.get_entity(PeerChannel(channel_id)), timeout=30.0
                        )
                    except Exception:
                        logger.warning(
                            "Channel %d: all entity lookups failed, deactivating", channel_id
                        )
                        if channel.id:
                            await self._db.set_channel_active(channel.id, False)
                        return 0
                    new_username = getattr(fallback_entity, "username", None)
                    new_title = getattr(fallback_entity, "title", channel.title)
                    await self._db.update_channel_meta(
                        channel_id, username=new_username, title=new_title
                    )
                    logger.warning(
                        "Channel %d: username changed %s → %s, marking filtered",
                        channel_id, channel.username, new_username,
                    )
                    await self._db.set_channels_filtered_bulk(
                        [(channel_id, "username_changed")]
                    )
                    return 0
            else:
                try:
                    entity = await asyncio.wait_for(
                        client.get_entity(PeerChannel(channel_id)), timeout=30.0
                    )
                except asyncio.TimeoutError:
                    logger.warning("get_entity timed out for channel %d, skipping", channel_id)
                    return 0

            # Превентивная фильтрация по subscriber_ratio до загрузки сообщений
            # Пропускается при force=True (ручной запуск не должен менять фильтр-статус)
            if not force:
                stats_list = await self._db.get_channel_stats(channel_id, limit=1)
                subscriber_count = stats_list[0].subscriber_count if stats_list else None
                if subscriber_count is not None:
                    if min_subs > 0 and subscriber_count < min_subs:
                        await self._db.set_channels_filtered_bulk(
                            [(channel_id, "low_subscriber_manual")]
                        )
                        logger.info(
                            "Pre-filter: channel %d subscribers %d < %d, skipping",
                            channel_id, subscriber_count, min_subs,
                        )
                        return 0
                    cur = await self._db.execute(
                        "SELECT COUNT(*) FROM messages WHERE channel_id = ?",
                        (channel_id,),
                    )
                    row = await cur.fetchone()
                    message_count = row[0] if row else 0
                    if message_count > 0:
                        is_broadcast = channel.channel_type in ("channel", "monoforum")
                        threshold = (
                            LOW_SUBSCRIBER_RATIO_THRESHOLD
                            if is_broadcast
                            else LOW_SUBSCRIBER_RATIO_CHAT_THRESHOLD
                        )
                        ratio = subscriber_count / message_count
                        if ratio < threshold:
                            await self._db.set_channels_filtered_bulk(
                                [(channel_id, "low_subscriber_ratio")]
                            )
                            logger.info(
                                "Pre-filter: channel %d ratio %.4f < %.2f, skipping",
                                channel_id, ratio, threshold,
                            )
                            return 0

            # Pre-check: sample 10 posts to detect cross-channel duplicates
            if is_first_run:
                try:
                    sample_prefixes = await asyncio.wait_for(
                        self._precheck_sample(client, entity, PRECHECK_CROSS_DUPE_SAMPLE),
                        timeout=60.0,
                    )
                except asyncio.TimeoutError:
                    logger.warning(
                        "Precheck timed out for channel %d, skipping precheck", channel_id
                    )
                    sample_prefixes = []
                if len(sample_prefixes) >= PRECHECK_CROSS_DUPE_MIN_SAMPLE:
                    matches = await self._db.filter_repo.count_matching_prefixes_in_other_channels(
                        channel_id, sample_prefixes
                    )
                    if matches / len(sample_prefixes) >= PRECHECK_CROSS_DUPE_RATIO:
                        await self._db.set_channels_filtered_bulk(
                            [(channel_id, "cross_channel_spam")]
                        )
                        logger.info(
                            "Pre-filter: channel %d has %d/%d cross-dupe messages, skipping",
                            channel_id, matches, len(sample_prefixes),
                        )
                        return 0

            async for msg in client.iter_messages(
                entity,
                min_id=min_id,
                limit=limit,
                reverse=True,
                wait_time=self._config.delay_between_requests_sec,
            ):
                message = Message(
                    channel_id=channel_id,
                    message_id=msg.id,
                    sender_id=msg.sender_id,
                    sender_name=self._get_sender_name(msg),
                    text=msg.text,
                    media_type=self._get_media_type(msg),
                    date=msg.date.replace(tzinfo=timezone.utc)
                    if msg.date and msg.date.tzinfo is None
                    else msg.date,
                )
                messages_batch.append(message)
                max_msg_id = max(max_msg_id, msg.id)

                if len(messages_batch) % 10 == 0 and self._cancel_event.is_set():
                    logger.info("Channel %d collection interrupted", channel_id)
                    break

                if is_first_run and len(messages_batch) >= 500:
                    await self._db.insert_messages_batch(messages_batch)
                    all_messages.extend(messages_batch)
                    logger.info(
                        "Channel %d: flushed %d, total %d",
                        channel_id, len(messages_batch), len(all_messages),
                    )
                    messages_batch = []
                    if progress_callback:
                        await progress_callback(len(all_messages))
                    if self._cancel_event.is_set():
                        break

        except (UsernameNotOccupiedError, UsernameInvalidError):
            logger.warning(
                "Channel %d (%s): username not found, deactivating",
                channel_id, channel.username,
            )
            if channel.id:
                await self._db.set_channel_active(channel.id, False)
            raise
        except FloodWaitError as e:
            flood_wait_sec = e.seconds
            logger.warning("FloodWait %ds for %s on channel %d", flood_wait_sec, phone, channel_id)
        finally:
            # Flush remaining messages — each operation is protected independently
            # so a failure in one doesn't prevent the other from executing.
            try:
                if messages_batch:
                    await self._db.insert_messages_batch(messages_batch)
                    all_messages.extend(messages_batch)
                    logger.info(
                        "Channel %d: saved %d remaining messages on exit",
                        channel_id, len(messages_batch),
                    )
                    if progress_callback:
                        await progress_callback(len(all_messages))
            except Exception as flush_err:
                logger.error(
                    "Failed to flush %d messages for channel %d: %s",
                    len(messages_batch), channel_id, flush_err,
                )
            try:
                if max_msg_id > min_id:
                    await self._db.update_channel_last_id(channel_id, max_msg_id)
            except Exception as update_err:
                logger.error(
                    "Failed to update last_collected_id for channel %d: %s",
                    channel_id, update_err,
                )
            await self._pool.release_client(phone)

        # Handle FloodWait AFTER finally has flushed progress
        if flood_wait_sec is not None:
            await self._pool.report_flood(phone, flood_wait_sec)
            if flood_wait_sec <= self._config.max_flood_wait_sec:
                # Re-read channel from DB to get updated last_collected_id.
                # Use get_channel_by_pk (no filtering) — collection already
                # started, so we must finish even if the channel was filtered
                # in the meantime.
                updated = None
                if channel.id is not None:
                    updated = await self._db.get_channel_by_pk(channel.id)
                if updated:
                    return len(all_messages) + await self._collect_channel(
                        updated, progress_callback=progress_callback, force=force
                    )
            else:
                if self._notifier:
                    await self._notifier.notify(
                        f"FloodWait {flood_wait_sec}s on {phone}, "
                        f"channel {channel_id} skipped"
                    )
            return len(all_messages)

        if all_messages and self._notifier:
            await self._check_keywords(all_messages)

        if is_first_run and len(all_messages) >= 50:
            cur = await self._db.execute(
                "SELECT COUNT(*) as total, COUNT(DISTINCT substr(text,1,100)) as uniq"
                " FROM messages WHERE channel_id = ? AND text IS NOT NULL AND length(text) > 10",
                (channel_id,),
            )
            row = await cur.fetchone()
            if row and row["total"] >= 50:
                ratio = row["uniq"] / row["total"] * 100
                if ratio < LOW_UNIQUENESS_THRESHOLD:
                    await self._db.set_channels_filtered_bulk([(channel_id, "low_uniqueness")])
                    logger.warning(
                        "Post-collection: channel %d low_uniqueness %.1f%%, marked filtered",
                        channel_id,
                        ratio,
                    )

        return len(all_messages)

    async def _precheck_sample(self, client, entity, limit: int) -> list[str]:
        """Sample up to `limit` messages for cross-channel precheck."""
        prefixes: list[str] = []
        async for msg in client.iter_messages(entity, limit=limit):
            if self._cancel_event.is_set():
                break
            if msg.text and len(msg.text) > 10:
                prefixes.append(msg.text[:100])
        return prefixes

    async def _check_keywords(self, messages: list[Message]) -> None:
        """Check messages against active keywords and notify."""
        if not self._notifier:
            return

        keywords = await self._db.get_keywords(active_only=True)
        if not keywords:
            return

        for msg in messages:
            if not msg.text:
                continue
            for kw in keywords:
                matched = False
                if kw.is_regex:
                    try:
                        matched = bool(re.search(kw.pattern, msg.text, re.IGNORECASE))
                    except re.error:
                        pass
                else:
                    matched = kw.pattern.lower() in msg.text.lower()

                if matched:
                    await self._notifier.notify(
                        f"Keyword '{kw.pattern}' found in channel {msg.channel_id}:\n"
                        f"{msg.text[:200]}"
                    )

    async def collect_channel_stats(self, channel: Channel) -> ChannelStats | None:
        async with self._stats_lock:
            self._stats_running = True
            try:
                return await self._collect_channel_stats(channel)
            except (AllStatsClientsFloodedError, NoActiveStatsClientsError):
                logger.error("No available clients for stats collection")
                return None
            finally:
                self._stats_running = False

    async def _collect_channel_stats(self, channel: Channel) -> ChannelStats | None:
        while True:
            result = await self._pool.get_available_client()
            if result is None:
                availability_fn = getattr(self._pool, "get_stats_availability", None)
                if not callable(availability_fn):
                    raise NoActiveStatsClientsError("No active connected clients")
                availability_result = availability_fn()
                if not asyncio.iscoroutine(availability_result):
                    raise NoActiveStatsClientsError("No active connected clients")
                availability = await availability_result
                if (
                    availability.state == "all_flooded"
                    and availability.retry_after_sec is not None
                    and availability.next_available_at_utc is not None
                ):
                    raise AllStatsClientsFloodedError(
                        retry_after_sec=availability.retry_after_sec,
                        next_available_at=availability.next_available_at_utc,
                    )
                raise NoActiveStatsClientsError("No active connected clients")

            client, phone = result
            try:
                if channel.username:
                    entity = await client.get_entity(channel.username)
                else:
                    entity = await client.get_entity(PeerChannel(channel.channel_id))

                full = await client(GetFullChannelRequest(entity))
                subscriber_count = getattr(full.full_chat, "participants_count", None)

                views_list, reactions_list, forwards_list = [], [], []
                async for msg in client.iter_messages(
                    entity,
                    limit=50,
                    wait_time=self._config.delay_between_requests_sec,
                ):
                    if getattr(msg, "views", None) is not None:
                        views_list.append(msg.views)
                    if getattr(msg, "forwards", None) is not None:
                        forwards_list.append(msg.forwards)
                    reactions = getattr(msg, "reactions", None)
                    if reactions:
                        total = sum(
                            getattr(r, "count", 0)
                            for r in getattr(reactions, "results", [])
                        )
                        reactions_list.append(total)

                stats = ChannelStats(
                    channel_id=channel.channel_id,
                    subscriber_count=subscriber_count,
                    avg_views=sum(views_list) / len(views_list) if views_list else None,
                    avg_reactions=(
                        sum(reactions_list) / len(reactions_list)
                        if reactions_list else None
                    ),
                    avg_forwards=(
                        sum(forwards_list) / len(forwards_list) if forwards_list else None
                    ),
                )
                await self._db.save_channel_stats(stats)
                return stats
            except FloodWaitError as e:
                logger.warning(
                    "Flood wait %ds for stats on %s via %s",
                    e.seconds, channel.channel_id, phone,
                )
                await self._pool.report_flood(phone, e.seconds)
            finally:
                await self._pool.release_client(phone)

    async def collect_all_stats(self) -> dict:
        async with self._stats_lock:
            self._stats_running = True
            try:
                channels = await self._db.get_channels(
                    active_only=True, include_filtered=False
                )
                stats = {"channels": 0, "errors": 0}
                for idx, channel in enumerate(channels):
                    while True:
                        try:
                            await self._collect_channel_stats(channel)
                            stats["channels"] += 1
                            break
                        except AllStatsClientsFloodedError as e:
                            logger.warning(
                                "All clients are flood-waited for stats. "
                                "Waiting %ds until %s",
                                e.retry_after_sec,
                                e.next_available_at.isoformat(),
                            )
                            await asyncio.sleep(e.retry_after_sec)
                        except NoActiveStatsClientsError:
                            logger.error("No active connected clients for stats collection")
                            stats["errors"] += len(channels) - idx
                            return stats
                        except Exception as e:
                            logger.error("Stats error for %s: %s", channel.channel_id, e)
                            stats["errors"] += 1
                            break
                    if idx < len(channels) - 1:
                        await asyncio.sleep(self._config.delay_between_channels_sec)
                return stats
            finally:
                self._stats_running = False

    @staticmethod
    def _get_sender_name(msg) -> str | None:
        if msg.sender:
            if hasattr(msg.sender, "first_name"):
                parts = [msg.sender.first_name or "", msg.sender.last_name or ""]
                return " ".join(p for p in parts if p) or None
            if hasattr(msg.sender, "title"):
                return msg.sender.title
        return None
