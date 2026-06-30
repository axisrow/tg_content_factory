"""Collector message stream helpers and Telethon message parsing delegates (#1137)."""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from inspect import isawaitable
from typing import TYPE_CHECKING

from src.models import Channel, Message
from src.telegram.collector_message_parse import (
    build_message_from_telethon,
    extract_reactions,
    get_message_kind,
    get_sender_kind,
    get_sender_name,
    get_service_action_payload,
    get_service_action_raw,
    get_service_action_semantic,
)
from src.telegram.collector_types import _StreamOutcome

if TYPE_CHECKING:
    from typing import Any, Protocol, TypeAlias

    from src.telegram.collector import Collector as _RuntimeCollector

    _RuntimeCollectorType: TypeAlias = type[_RuntimeCollector]

    class Collector(Protocol):
        def __getattribute__(self, name: str) -> Any: ...
        def __setattr__(self, name: str, value: Any) -> None: ...

logger = logging.getLogger("src.telegram.collector")

MESSAGE_FLUSH_BATCH_SIZE = 500
STREAM_CLEANUP_TIMEOUT_SEC = 10.0


class StreamMixin:
    async def _release_collection_client(
        self: "Collector",
        phone: str,
        session,
        *,
        retire: bool = False,
    ) -> None:
        if not retire:
            await self._pool.release_client(phone)
            return

        logger.warning(
            "Retiring Telegram client for %s because a message stream read did not finish cancellation",
            phone,
        )
        pool_dict = getattr(self._pool, "__dict__", {})
        remove_client = None
        if "remove_client" in pool_dict or hasattr(type(self._pool), "remove_client"):
            remove_client = getattr(self._pool, "remove_client", None)
        if remove_client is not None:
            try:
                result = remove_client(phone)
                if isawaitable(result):
                    await asyncio.wait_for(result, timeout=STREAM_CLEANUP_TIMEOUT_SEC)
                return
            except asyncio.TimeoutError:
                logger.warning(
                    "Timed out removing dirty Telegram client for %s after %.1fs; "
                    "disconnecting and releasing the lease",
                    phone,
                    STREAM_CLEANUP_TIMEOUT_SEC,
                )
            except Exception:
                logger.debug("Failed to remove dirty Telegram client for %s", phone, exc_info=True)

        raw_client = getattr(session, "raw_client", None)
        disconnect = getattr(raw_client, "disconnect", None) if raw_client is not None else None
        if disconnect is None:
            disconnect = getattr(session, "disconnect", None)
        if disconnect is not None:
            try:
                result = disconnect()
                if isawaitable(result):
                    await asyncio.wait_for(result, timeout=STREAM_CLEANUP_TIMEOUT_SEC)
            except Exception:
                logger.debug("Failed to disconnect dirty Telegram client for %s", phone, exc_info=True)
        await self._pool.release_client(phone)

    async def _stream_channel_messages(
        self: "Collector",
        *,
        session,
        entity,
        min_id: int,
        limit,
        channel: Channel,
        channel_id: int,
        cancel_event: asyncio.Event | None,
        messages_batch: list[Message],
        flush_batch: Callable[[list[Message]], Awaitable[bool]],
        outcome: _StreamOutcome,
    ) -> None:
        """Stream messages from `entity` into `messages_batch`, flushing at the
        batch boundary via `flush_batch`. Mutates `messages_batch` in place and
        records abort flags on `outcome` so partial progress survives a FloodWait
        or idle-timeout raised mid-stream (see _StreamOutcome)."""
        # Idle timeout caps the wait for the *next* post, not the whole
        # channel: a healthy channel that streams post-by-post is never
        # aborted, only a stream gone silent (dead socket) is. 0/negative
        # disables the cap (wait_for(timeout=None) == a bare await).
        # asyncio.TimeoutError propagates to the outer handler, which
        # releases the client.
        configured = self._config.collection_stream_timeout_sec
        idle_timeout = configured if configured and configured > 0 else None
        stream = session.stream_messages(
            entity,
            min_id=min_id,
            limit=limit,
            reverse=True,
            wait_time=self._config.delay_between_requests_sec,
        )
        agen = stream.__aiter__()
        stream_close_allowed = True

        async def _next_message():
            nonlocal stream_close_allowed
            if idle_timeout is None:
                return await agen.__anext__()

            next_task = asyncio.create_task(agen.__anext__())

            def _consume_late_next_task(task: asyncio.Task) -> None:
                try:
                    exc = task.exception()
                except asyncio.CancelledError:
                    return
                if exc is not None:
                    logger.debug(
                        "Channel %d (%s): message stream next failed after timeout",
                        channel_id,
                        channel.username or channel.title or "",
                        exc_info=(type(exc), exc, exc.__traceback__),
                    )

            async def _cancel_next_task(reason: str) -> None:
                nonlocal stream_close_allowed
                if next_task.done():
                    return
                stream_close_allowed = False
                next_task.cancel()
                next_task.add_done_callback(_consume_late_next_task)
                done, _ = await asyncio.wait(
                    {next_task}, timeout=STREAM_CLEANUP_TIMEOUT_SEC
                )
                if next_task in done:
                    stream_close_allowed = True
                else:
                    outcome.retire_client = True
                    logger.warning(
                        "Channel %d (%s): message stream %s timed out after %.1fs",
                        channel_id,
                        channel.username or channel.title or "",
                        reason,
                        STREAM_CLEANUP_TIMEOUT_SEC,
                    )

            try:
                done, _ = await asyncio.wait({next_task}, timeout=idle_timeout)
            except asyncio.CancelledError:
                await _cancel_next_task("next-cancel on collector cancellation")
                raise
            if next_task in done:
                return await next_task

            await _cancel_next_task("next-cancel")
            raise asyncio.TimeoutError

        try:
            while True:
                msg = await _next_message()

                message = build_message_from_telethon(msg, channel_id)
                messages_batch.append(message)

                if (
                    len(messages_batch) % 10 == 0
                    and self._is_collection_cancelled(cancel_event)
                ):
                    logger.info("Channel %d collection interrupted", channel_id)
                    break

                if len(messages_batch) >= MESSAGE_FLUSH_BATCH_SIZE:
                    if not await self._channel_still_exists(channel_id):
                        messages_batch.clear()
                        break
                    if not await flush_batch(messages_batch):
                        outcome.stop_due_to_persistence_error = True
                        break
                    messages_batch.clear()
                    if self._is_collection_cancelled(cancel_event):
                        break
        except StopAsyncIteration:
            pass
        finally:
            aclose = getattr(agen, "aclose", None)
            if aclose is not None and stream_close_allowed:
                try:
                    close_result = aclose()
                    if isawaitable(close_result):
                        await asyncio.wait_for(
                            close_result, timeout=STREAM_CLEANUP_TIMEOUT_SEC
                        )
                except asyncio.TimeoutError:
                    logger.warning(
                        "Channel %d (%s): message stream close timed out after %.1fs",
                        channel_id,
                        channel.username or channel.title or "",
                        STREAM_CLEANUP_TIMEOUT_SEC,
                    )
                except Exception:
                    logger.debug(
                        "Channel %d (%s): message stream close failed",
                        channel_id,
                        channel.username or channel.title or "",
                        exc_info=True,
                    )
            elif aclose is not None:
                logger.debug(
                    "Channel %d (%s): skipping message stream close because "
                    "a pending next read did not finish cancellation",
                    channel_id,
                    channel.username or channel.title or "",
                )

    @staticmethod
    def _build_message(msg, channel_id: int) -> Message:
        """Build a :class:`Message` from a Telethon message."""
        return build_message_from_telethon(msg, channel_id)

    @staticmethod
    def _extract_reactions(msg) -> str | None:
        """Extract reactions from a Telethon message as JSON string."""
        return extract_reactions(msg)

    @staticmethod
    def _get_sender_name(msg) -> str | None:
        return get_sender_name(msg)

    @classmethod
    def _get_message_kind(cls, msg) -> str:
        return get_message_kind(msg)

    @staticmethod
    def _get_service_action_raw(msg) -> str | None:
        return get_service_action_raw(msg)

    @classmethod
    def _get_service_action_semantic(cls, msg) -> str | None:
        return get_service_action_semantic(msg)

    @staticmethod
    def _get_service_action_payload(msg) -> str | None:
        return get_service_action_payload(msg)

    @staticmethod
    def _get_sender_kind(msg) -> str:
        return get_sender_kind(msg)
