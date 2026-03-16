from __future__ import annotations

import asyncio
import json
import logging
from collections.abc import Awaitable, Callable
from datetime import datetime, timezone

from telethon.errors import FloodWaitError, UsernameInvalidError, UsernameNotOccupiedError
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
from src.settings_utils import parse_int_setting
from src.telegram.backends import adapt_transport_session
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
        self._stats_all_running = False
        self._cancel_event = asyncio.Event()
        self._lock = asyncio.Lock()
        self._stats_lock = asyncio.Lock()
        self._stats_all_lock = asyncio.Lock()

    @property
    def is_running(self) -> bool:
        return self._running or self._stats_running or self._stats_all_running

    @property
    def is_stats_running(self) -> bool:
        return self._stats_running or self._stats_all_running

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

    async def _load_min_subscribers_filter(self) -> int:
        return parse_int_setting(
            await self._db.get_setting("min_subscribers_filter"),
            setting_name="min_subscribers_filter",
            default=0,
            logger=logger,
        )

    async def _is_auto_delete_enabled(self) -> bool:
        """Check if auto_delete_on_collect is enabled (cached per collection run)."""
        cached = getattr(self, "_auto_delete_cached", None)
        if cached is not None:
            return cached
        setting = await self._db.get_setting("auto_delete_on_collect")
        result = setting == "1"
        self._auto_delete_cached = result
        return result

    async def _maybe_auto_delete(self, channel_id: int) -> bool:
        """Purge messages from filtered channel if auto_delete_on_collect is enabled."""
        if not await self._is_auto_delete_enabled():
            return False
        try:
            deleted = await self._db.delete_messages_for_channel(channel_id)
            logger.info(
                "Auto-purged %d messages from filtered channel %d during collection",
                deleted, channel_id,
            )
            return True
        except Exception:
            logger.exception("Failed to auto-purge channel %d", channel_id)
            return False

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
            self._auto_delete_cached = None
            try:
                if full:
                    channel = Channel(**{**channel.model_dump(), "last_collected_id": 0})

                min_subs = await self._load_min_subscribers_filter()
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
            self._auto_delete_cached = None
            stats = {"channels": 0, "messages": 0, "errors": 0}

            try:
                channels = await self._db.get_channels(
                    active_only=True, include_filtered=False
                )
                if not channels:
                    logger.info("No active unfiltered channels to collect")
                    return stats
                logger.info("Found %d active unfiltered channels to collect", len(channels))

                min_subs = await self._load_min_subscribers_filter()

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
        progress_offset: int = 0,
    ) -> int:
        """Collect new messages from a single channel. Returns count."""
        total_collected = 0

        while True:
            channel_id = channel.channel_id
            min_id = channel.last_collected_id

            result = await self._pool.get_available_client()
            if result is None:
                logger.error("No available clients for collection")
                return total_collected

            session, phone = result
            session = adapt_transport_session(session, disconnect_on_close=False)
            # Populate entity cache when using PeerChannel
            # (StringSession loses cache between restarts).
            # Only needed once per process lifetime per phone —
            # the in-memory cache persists.
            if not channel.username and not self._pool.is_dialogs_fetched(phone):
                try:
                    await asyncio.wait_for(session.warm_dialog_cache(), timeout=30)
                    self._pool.mark_dialogs_fetched(phone)
                except Exception as e:
                    logger.warning("Failed to prefetch dialogs for %s: %s", phone, e)
            messages_batch: list[Message] = []
            all_messages: list[Message] = []
            persisted_max_msg_id = min_id
            flood_wait_sec: int | None = None
            stop_due_to_persistence_error = False

            is_first_run = channel.last_collected_id == 0
            should_notify = self._notifier is not None and not is_first_run
            limit = None
            logger.info(
                "Collecting channel %d (%s), first_run=%s, min_id=%d, limit=%s",
                channel_id,
                channel.username or channel.title,
                is_first_run,
                min_id,
                limit,
            )

            async def _flush_batch(batch: list[Message]) -> bool:
                nonlocal persisted_max_msg_id
                if not batch:
                    return True

                await self._db.insert_messages_batch(batch)
                expected_ids = {message.message_id for message in batch}
                placeholders = ",".join("?" for _ in expected_ids)
                cur = await self._db.execute(
                    f"SELECT message_id FROM messages WHERE channel_id = ? "
                    f"AND message_id IN ({placeholders})",
                    (channel_id, *expected_ids),
                )
                rows = await cur.fetchall()
                persisted_ids = {row["message_id"] for row in rows}
                missing_ids = expected_ids - persisted_ids
                if missing_ids:
                    logger.error(
                        "Failed to persist %d/%d messages for channel %d; "
                        "last persisted id remains %d",
                        len(missing_ids),
                        len(expected_ids),
                        channel_id,
                        persisted_max_msg_id,
                    )
                    return False

                persisted_max_msg_id = max(persisted_max_msg_id, max(expected_ids))
                all_messages.extend(batch)
                logger.info(
                    "Channel %d: persisted %d messages, total %d",
                    channel_id,
                    len(batch),
                    len(all_messages),
                )
                if progress_callback:
                    await progress_callback(
                        total_collected + len(all_messages)
                    )
                return True

            try:
                if channel.username:
                    try:
                        entity = await asyncio.wait_for(
                            session.resolve_entity(channel.username), timeout=30.0
                        )
                    except asyncio.TimeoutError:
                        logger.warning(
                            "get_entity timed out for channel %d, skipping",
                            channel_id,
                        )
                        return total_collected
                    except (ValueError, UsernameNotOccupiedError, UsernameInvalidError):
                        logger.warning(
                            "Channel %d (%s): username not found, "
                            "trying numeric ID fallback",
                            channel_id, channel.username,
                        )
                        try:
                            fallback_entity = await asyncio.wait_for(
                                session.resolve_entity(PeerChannel(channel_id)),
                                timeout=30.0,
                            )
                        except Exception:
                            logger.warning(
                                "Channel %d: all entity lookups failed, "
                                "deactivating",
                                channel_id,
                            )
                            if channel.id:
                                await self._db.set_channel_active(channel.id, False)
                            return total_collected
                        new_username = getattr(fallback_entity, "username", None)
                        new_title = (
                            getattr(fallback_entity, "title", None)
                            or channel.title
                            or channel.username
                            or str(channel_id)
                        )
                        await self._db.update_channel_meta(
                            channel_id, username=new_username, title=new_title
                        )
                        logger.warning(
                            "Channel %d: username changed %s → %s, "
                            "marking filtered",
                            channel_id, channel.username, new_username,
                        )
                        await self._db.set_channels_filtered_bulk(
                            [(channel_id, "username_changed")]
                        )
                        await self._maybe_auto_delete(channel_id)
                        return total_collected
                else:
                    try:
                        entity = await asyncio.wait_for(
                            session.resolve_entity(PeerChannel(channel_id)),
                            timeout=30.0,
                        )
                    except asyncio.TimeoutError:
                        logger.warning(
                            "get_entity timed out for channel %d, skipping",
                            channel_id,
                        )
                        return total_collected

                # Превентивная фильтрация по subscriber_ratio до загрузки
                # сообщений.
                # Пропускается при force=True (ручной запуск не должен менять
                # фильтр-статус)
                if not force:
                    stats_list = await self._db.get_channel_stats(
                        channel_id, limit=1
                    )
                    subscriber_count = (
                        stats_list[0].subscriber_count if stats_list else None
                    )
                    if subscriber_count is not None:
                        if min_subs > 0 and subscriber_count < min_subs:
                            await self._db.set_channels_filtered_bulk(
                                [(channel_id, "low_subscriber_manual")]
                            )
                            logger.info(
                                "Pre-filter: channel %d subscribers %d < %d,"
                                " skipping",
                                channel_id, subscriber_count, min_subs,
                            )
                            await self._maybe_auto_delete(channel_id)
                            return total_collected
                        cur = await self._db.execute(
                            "SELECT COUNT(*) FROM messages"
                            " WHERE channel_id = ?",
                            (channel_id,),
                        )
                        row = await cur.fetchone()
                        message_count = row[0] if row else 0
                        if message_count > 0:
                            is_broadcast = channel.channel_type in (
                                "channel", "monoforum",
                            )
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
                                    "Pre-filter: channel %d ratio %.4f"
                                    " < %.2f, skipping",
                                    channel_id, ratio, threshold,
                                )
                                await self._maybe_auto_delete(channel_id)
                                return total_collected

                # Pre-check: sample 10 posts to detect cross-channel duplicates
                if is_first_run and not force:
                    try:
                        sample_prefixes = await asyncio.wait_for(
                            self._precheck_sample(
                                session, entity, PRECHECK_CROSS_DUPE_SAMPLE
                            ),
                            timeout=60.0,
                        )
                    except asyncio.TimeoutError:
                        logger.warning(
                            "Precheck timed out for channel %d, "
                            "skipping precheck",
                            channel_id,
                        )
                        sample_prefixes = []
                    unique_prefixes = list(dict.fromkeys(sample_prefixes))
                    if len(unique_prefixes) >= PRECHECK_CROSS_DUPE_MIN_SAMPLE:
                        repo = self._db.filter_repo
                        matches = await repo.count_matching_prefixes_in_other_channels(
                            channel_id, unique_prefixes
                        )
                        if (
                            matches / len(unique_prefixes)
                            >= PRECHECK_CROSS_DUPE_RATIO
                        ):
                            await self._db.set_channels_filtered_bulk(
                                [(channel_id, "cross_channel_spam")]
                            )
                            logger.info(
                                "Pre-filter: channel %d has %d/%d "
                                "cross-dupe messages, skipping",
                                channel_id,
                                matches,
                                len(unique_prefixes),
                            )
                            await self._maybe_auto_delete(channel_id)
                            return total_collected

                async for msg in session.stream_messages(
                    entity,
                    min_id=min_id,
                    limit=limit,
                    reverse=True,
                    wait_time=self._config.delay_between_requests_sec,
                ):
                    topic_id = None
                    reply_to = getattr(msg, "reply_to", None)
                    if reply_to and getattr(reply_to, "forum_topic", False):
                        topic_id = (
                            getattr(reply_to, "reply_to_top_id", None)
                            or getattr(reply_to, "reply_to_msg_id", None)
                            or 1
                        )
                    elif reply_to:
                        # Обычный reply (не форум) — topic_id не нужен
                        pass
                    message = Message(
                        channel_id=channel_id,
                        message_id=msg.id,
                        sender_id=msg.sender_id,
                        sender_name=self._get_sender_name(msg),
                        text=msg.text,
                        media_type=self._get_media_type(msg),
                        topic_id=topic_id,
                        reactions_json=self._extract_reactions(msg),
                        views=getattr(msg, "views", None),
                        forwards=getattr(msg, "forwards", None),
                        reply_count=getattr(getattr(msg, "replies", None), "replies", None),
                        date=msg.date.replace(tzinfo=timezone.utc)
                        if msg.date and msg.date.tzinfo is None
                        else msg.date,
                    )
                    messages_batch.append(message)

                    if (
                        len(messages_batch) % 10 == 0
                        and self._cancel_event.is_set()
                    ):
                        logger.info(
                            "Channel %d collection interrupted", channel_id
                        )
                        break

                    if is_first_run and len(messages_batch) >= 500:
                        if not await self._channel_still_exists(channel_id):
                            messages_batch = []
                            break
                        if not await _flush_batch(messages_batch):
                            stop_due_to_persistence_error = True
                            break
                        messages_batch = []
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
                logger.warning(
                    "FloodWait %ds for %s on channel %d",
                    flood_wait_sec, phone, channel_id,
                )
            finally:
                # Flush remaining messages — each operation is protected
                # independently so a failure in one doesn't prevent the
                # other from executing.
                try:
                    if messages_batch:
                        if not await self._channel_still_exists(channel_id):
                            messages_batch = []
                        else:
                            stop_due_to_persistence_error = (
                                not await _flush_batch(messages_batch)
                            )
                except Exception as flush_err:
                    logger.error(
                        "Failed to flush %d messages for channel %d: %s",
                        len(messages_batch), channel_id, flush_err,
                    )
                    stop_due_to_persistence_error = True
                try:
                    if (
                        persisted_max_msg_id > min_id
                        and await self._channel_still_exists(channel_id)
                    ):
                        await self._db.update_channel_last_id(
                            channel_id, persisted_max_msg_id
                        )
                except Exception as update_err:
                    logger.error(
                        "Failed to update last_collected_id for "
                        "channel %d: %s",
                        channel_id, update_err,
                    )
                await self._pool.release_client(phone)

            if stop_due_to_persistence_error:
                return total_collected + len(all_messages)

            # Handle FloodWait AFTER finally has flushed progress
            if flood_wait_sec is not None:
                await self._pool.report_flood(phone, flood_wait_sec)
                if flood_wait_sec <= self._config.max_flood_wait_sec:
                    # Re-read channel from DB to get updated
                    # last_collected_id.
                    # Use get_channel_by_pk (no filtering) — collection
                    # already started, so we must finish even if the
                    # channel was filtered in the meantime.
                    updated = None
                    if channel.id is not None:
                        updated = await self._db.get_channel_by_pk(
                            channel.id
                        )
                    if updated:
                        total_collected += len(all_messages)
                        progress_offset += len(all_messages)
                        channel = updated
                        continue
                else:
                    if self._notifier:
                        await self._notifier.notify(
                            f"FloodWait {flood_wait_sec}s on {phone}, "
                            f"channel {channel_id} skipped"
                        )
                return total_collected + len(all_messages)

            if should_notify and all_messages:
                await self._check_notification_queries(all_messages)

            # Update forum topics in DB if messages with topic_id
            # were collected
            if all_messages and any(
                m.topic_id is not None for m in all_messages
            ):
                cached = await self._db.get_forum_topics(channel_id)
                if not cached:
                    try:
                        topics = await self._pool.get_forum_topics(
                            channel_id
                        )
                        if topics:
                            await self._db.upsert_forum_topics(
                                channel_id, topics
                            )
                    except Exception as e:
                        logger.warning(
                            "Failed to update forum topics for %d: %s",
                            channel_id, e,
                        )

            if is_first_run and not force and len(all_messages) >= 50:
                cur = await self._db.execute(
                    "SELECT COUNT(*) as total,"
                    " COUNT(DISTINCT substr(text,1,100)) as uniq"
                    " FROM messages WHERE channel_id = ?"
                    " AND text IS NOT NULL AND length(text) > 10",
                    (channel_id,),
                )
                row = await cur.fetchone()
                if row and row["total"] >= 50:
                    ratio = row["uniq"] / row["total"] * 100
                    if ratio < LOW_UNIQUENESS_THRESHOLD:
                        await self._db.set_channels_filtered_bulk(
                            [(channel_id, "low_uniqueness")]
                        )
                        logger.warning(
                            "Post-collection: channel %d low_uniqueness"
                            " %.1f%%, marked filtered",
                            channel_id,
                            ratio,
                        )
                        # Not auto-deleting here: messages were just collected,
                        # channel will be deleted on the next collection run.

            return total_collected + len(all_messages)

    async def _precheck_sample(self, session, entity, limit: int) -> list[str]:
        """Sample up to `limit` messages for cross-channel precheck."""
        prefixes: list[str] = []
        async for msg in session.stream_messages(
            entity,
            limit=limit,
            wait_time=self._config.delay_between_requests_sec,
        ):
            if self._cancel_event.is_set():
                break
            if msg.text and len(msg.text) > 10:
                prefixes.append(msg.text[:100])
        return prefixes

    async def _check_notification_queries(self, messages: list[Message]) -> None:
        """Check messages against active notification queries and send batched notifications."""
        if not self._notifier:
            return

        queries = await self._db.get_notification_queries(active_only=True)
        if not queries:
            return

        from src.services.notification_matcher import NotificationMatcher

        matcher = NotificationMatcher(self._notifier)
        await matcher.match_and_notify(messages, queries)

    async def _channel_still_exists(self, channel_id: int) -> bool:
        return await self._db.get_channel_by_channel_id(channel_id) is not None

    async def sample_channel(self, channel_id: int, limit: int = 10) -> list[dict]:
        """Fetch the last `limit` messages from a channel for preview. No DB writes."""
        result = await self._pool.get_available_client()
        if result is None:
            logger.error("No available clients for sample collection")
            return []

        session, phone = result
        session = adapt_transport_session(session, disconnect_on_close=False)
        try:
            if not self._pool.is_dialogs_fetched(phone):
                try:
                    await asyncio.wait_for(session.warm_dialog_cache(), timeout=30)
                    self._pool.mark_dialogs_fetched(phone)
                except Exception as e:
                    logger.warning("Failed to prefetch dialogs for %s: %s", phone, e)

            try:
                entity = await asyncio.wait_for(
                    session.resolve_entity(PeerChannel(channel_id)),
                    timeout=30.0,
                )
            except (asyncio.TimeoutError, ValueError, LookupError):
                logger.warning(
                    "Could not resolve entity for channel %d", channel_id
                )
                return []

            previews: list[dict] = []
            async for msg in session.stream_messages(
                entity,
                limit=limit,
                wait_time=self._config.delay_between_requests_sec,
            ):
                if self._cancel_event.is_set():
                    break
                text = msg.text or ""
                previews.append({
                    "message_id": msg.id,
                    "date": msg.date,
                    "text_preview": text[:100] if text else None,
                    "media_type": self._get_media_type(msg),
                })

            return previews
        except FloodWaitError as e:
            logger.warning(
                "Flood wait %ds for sample on channel %d via %s",
                e.seconds, channel_id, phone,
            )
            await self._pool.report_flood(phone, e.seconds)
            return []
        finally:
            await self._pool.release_client(phone)

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

            session, phone = result
            session = adapt_transport_session(session, disconnect_on_close=False)
            try:
                if channel.username:
                    entity = await asyncio.wait_for(
                        session.resolve_entity(channel.username), timeout=30.0
                    )
                else:
                    entity = await asyncio.wait_for(
                        session.resolve_entity(PeerChannel(channel.channel_id)),
                        timeout=30.0,
                    )

                full = await asyncio.wait_for(
                    session.fetch_full_channel(entity),
                    timeout=30.0,
                )
                subscriber_count = getattr(full.full_chat, "participants_count", None)

                views_list, reactions_list, forwards_list = [], [], []

                async def _collect_stats_messages() -> None:
                    async for msg in session.stream_messages(
                        entity,
                        limit=50,
                        wait_time=self._config.delay_between_requests_sec,
                    ):
                        if self._cancel_event.is_set():
                            break
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

                try:
                    await asyncio.wait_for(_collect_stats_messages(), timeout=90.0)
                except asyncio.TimeoutError:
                    logger.warning(
                        "iter_messages timed out for stats on channel %d", channel.channel_id
                    )

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

                # Update channel_type if missing
                if channel.channel_type is None:
                    channel_type, _deactivate = self._pool._classify_entity(entity)
                    await self._db.set_channel_type(channel.channel_id, channel_type)

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
        async with self._stats_all_lock:
            self._stats_all_running = True
            try:
                channels = await self._db.get_channels(
                    active_only=True, include_filtered=False
                )
                stats = {"channels": 0, "errors": 0}
                for idx, channel in enumerate(channels):
                    while True:
                        try:
                            async with self._stats_lock:
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
                            logger.error(
                                "No active connected clients for stats collection"
                            )
                            stats["errors"] += len(channels) - idx
                            return stats
                        except Exception as e:
                            logger.error(
                                "Stats error for %s: %s", channel.channel_id, e
                            )
                            stats["errors"] += 1
                            break
                    if idx < len(channels) - 1:
                        await asyncio.sleep(self._config.delay_between_channels_sec)
                return stats
            finally:
                self._stats_all_running = False

    @staticmethod
    def _extract_reactions(msg) -> str | None:
        """Extract reactions from a Telethon message as JSON string."""
        reactions = getattr(msg, "reactions", None)
        if not reactions:
            return None
        results = getattr(reactions, "results", None)
        if not results:
            return None
        items = []
        for r in results:
            reaction = getattr(r, "reaction", None)
            if reaction is None:
                continue
            emoticon = getattr(reaction, "emoticon", None)
            if emoticon:
                emoji = emoticon
            else:
                document_id = getattr(reaction, "document_id", None)
                if document_id is not None:
                    emoji = f"custom:{document_id}"
                else:
                    continue
            items.append({"emoji": emoji, "count": getattr(r, "count", 0)})
        return json.dumps(items, ensure_ascii=False) if items else None

    @staticmethod
    def _get_sender_name(msg) -> str | None:
        if msg.sender:
            if hasattr(msg.sender, "first_name"):
                parts = [msg.sender.first_name or "", msg.sender.last_name or ""]
                return " ".join(p for p in parts if p) or None
            if hasattr(msg.sender, "title"):
                return msg.sender.title
        return None
