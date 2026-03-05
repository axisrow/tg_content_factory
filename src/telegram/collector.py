from __future__ import annotations

import asyncio
import logging
import re
from collections.abc import Awaitable, Callable
from datetime import timezone

from telethon.errors import FloodWaitError
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
from src.models import Channel, Message
from src.telegram.client_pool import ClientPool
from src.telegram.notifier import Notifier

logger = logging.getLogger(__name__)


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
        self._cancel_event = asyncio.Event()
        self._lock = asyncio.Lock()

    @property
    def is_running(self) -> bool:
        return self._running

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
    ) -> int:
        """Collect messages from a single channel. If full=True, reset last_collected_id to 0."""
        async with self._lock:
            self._running = True
            self._cancel_event.clear()
            try:
                result = await self._pool.get_available_client()
                if result:
                    client, _ = result
                    try:
                        await asyncio.wait_for(client.get_dialogs(), timeout=30)
                    except Exception:
                        pass

                if full:
                    channel = Channel(**{**channel.model_dump(), "last_collected_id": 0})

                return await self._collect_channel(
                    channel, progress_callback=progress_callback
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
                channels = await self._db.get_channels(active_only=True)
                if not channels:
                    logger.info("No active channels to collect")
                    return stats
                logger.info("Found %d active channels to collect", len(channels))

                # Pre-fetch dialogs to populate Telethon entity cache.
                # StringSession loses entity cache between restarts, so
                # get_dialogs() is needed for PeerChannel lookups to work.
                result = await self._pool.get_available_client()
                if result:
                    client, _ = result
                    try:
                        logger.info("Pre-fetching dialogs...")
                        await asyncio.wait_for(client.get_dialogs(), timeout=30)
                        logger.info("Dialogs pre-fetched successfully")
                    except Exception as e:
                        logger.warning("Failed to pre-fetch dialogs: %s", e)

                for channel in channels:
                    if self._cancel_event.is_set():
                        logger.info("Collection cancelled")
                        break
                    try:
                        collected = await self._collect_channel(channel)
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
                entity = await client.get_entity(channel.username)
            else:
                entity = await client.get_entity(PeerChannel(channel_id))

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
                # Re-read channel from DB to get updated last_collected_id
                channels = await self._db.get_channels()
                updated = next(
                    (c for c in channels if c.channel_id == channel_id), None
                )
                if updated:
                    return len(all_messages) + await self._collect_channel(
                        updated, progress_callback=progress_callback
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

        return len(all_messages)

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

    @staticmethod
    def _get_sender_name(msg) -> str | None:
        if msg.sender:
            if hasattr(msg.sender, "first_name"):
                parts = [msg.sender.first_name or "", msg.sender.last_name or ""]
                return " ".join(p for p in parts if p) or None
            if hasattr(msg.sender, "title"):
                return msg.sender.title
        return None
