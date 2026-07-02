"""Channel-entity resolution for the collection path.

Extracted from ``Collector._resolve_channel_entity`` (#1045). The resolution
logic — username→numeric fallback, resolve-rate-limit account rotation,
preferred-phone invalidation/rediscovery, and flood-wait encoding — is a
self-contained slice of the collect loop, but it leans on a lot of collector
state (``_pool``, ``_db``, ``_resolve_channel_input_entity``,
``_can_rotate_resolve``, ``_next_resolve_capable_at``,
``_discover_phone_for_channel``, ``_handle_meta_change_review``).

Per the project's "composition over inheritance" rule it lives as a free
function taking the collector explicitly rather than a mixin; ``Collector`` keeps
a thin ``_resolve_channel_entity`` delegate so existing call sites and tests are
unchanged. Behavior is preserved exactly, including the subtle
``RESOLVE_USERNAME_OPERATION`` flood label for username resolves.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from telethon.errors import UsernameInvalidError, UsernameNotOccupiedError
from telethon.tl.types import PeerChannel

from src.models import Channel
from src.telegram.flood_wait import HandledFloodWaitError
from src.telegram.rate_limiter import UsernameResolveRateLimitedError
from src.utils.safe_logging import mask_phone

if TYPE_CHECKING:
    from src.telegram.collector import Collector

logger = logging.getLogger(__name__)

RESOLVE_USERNAME_OPERATION = "collect_channel_resolve_username"


class ResolveOutcome:
    """Result of :func:`resolve_channel_entity` — exactly one outcome the
    collection loop acts on, replacing the inline resolve block's mix of
    ``continue`` / ``return`` / ``raise`` (#923).

    - ``entity`` set → proceed (run pre-filters + stream);
    - ``action == "retry"`` → ``continue`` the loop (account rotation / preferred-
      phone rediscovery), adopting ``channel`` when it was updated;
    - ``action == "stop"`` → ``return total_collected`` (resolve timeout / deactivation);
    - ``flood_wait_sec`` set → record the flood wait and fall through to the
      ``finally`` + post-collection flood handler. The operation label is
      preserved exactly: ``RESOLVE_USERNAME_OPERATION`` for the username resolve,
      otherwise ``exc.info.operation``.

    FloodWaits are encoded here rather than re-raised so the operation label is
    preserved without relying on the outer ``except HandledFloodWaitError``.
    ``UsernameResolveRateLimitedError`` propagates out of
    :func:`resolve_channel_entity` as an exception. The username-not-found errors
    (``UsernameNotOccupiedError`` / ``UsernameInvalidError``) are handled inside
    the resolve path (numeric fallback) and only ever reach ``_collect_channel``'s
    outer handler from streaming — exactly as in the pre-extraction code.
    """

    __slots__ = ("entity", "action", "channel", "flood_wait_sec", "flood_wait_operation")

    def __init__(
        self,
        *,
        entity=None,
        action: str = "proceed",
        channel: "Channel | None" = None,
        flood_wait_sec: int | None = None,
        flood_wait_operation: str | None = None,
    ) -> None:
        self.entity = entity
        self.action = action
        self.channel = channel
        self.flood_wait_sec = flood_wait_sec
        self.flood_wait_operation = flood_wait_operation


async def _resolve_by_username(
    collector: "Collector",
    channel: Channel,
    session,
    phone: str,
    channel_id: int,
    resolve_cache_only: bool,
    attempted_resolve_phones: set[str],
) -> ResolveOutcome:
    """Resolve a channel that has a username, with numeric-id fallback.

    The caller only routes here when ``channel.username`` is truthy; the assert
    documents that invariant and narrows the type for the resolve call.
    """
    assert channel.username is not None
    try:
        entity = await collector._resolve_channel_input_entity(
            session,
            channel_id=channel_id,
            username=channel.username,
            phone=phone,
            cache_only=resolve_cache_only,
        )
    except asyncio.TimeoutError:
        logger.warning(
            "get_input_entity timed out for channel %d, skipping",
            channel_id,
        )
        return ResolveOutcome(action="stop")
    except HandledFloodWaitError as exc:
        return ResolveOutcome(
            flood_wait_sec=exc.info.wait_seconds,
            flood_wait_operation=RESOLVE_USERNAME_OPERATION,
        )
    except UsernameResolveRateLimitedError as exc:
        # This phone cannot resolve live right now (backoff or limiter). Rotate
        # to a free account when one exists; defer the channel only when every
        # account is blocked (#790). The outer finally releases the client.
        attempted_resolve_phones.add(phone)
        if await collector._can_rotate_resolve(attempted_resolve_phones):
            logger.warning(
                "Channel %d (%s): live resolve unavailable on %s; "
                "rotating to another account",
                channel_id,
                channel.username,
                mask_phone(phone),
            )
            return ResolveOutcome(action="retry")
        # No account can live-resolve right now — defer the channel for the
        # moment the *first* account becomes capable again, combining per-phone
        # resolve backoff with generic flood waits; this phone's own (possibly
        # hours-long) window must not dictate the retry when another account
        # frees up sooner (#790 review P2).
        capable_at = await collector._next_resolve_capable_at()
        if capable_at is not None:
            now = datetime.now(timezone.utc)
            raise UsernameResolveRateLimitedError(
                phone,
                (capable_at - now).total_seconds(),
                now=now,
            ) from exc
        raise
    except (ValueError, UsernameNotOccupiedError, UsernameInvalidError):
        logger.warning(
            "Channel %d (%s): username not found, " "trying numeric ID fallback",
            channel_id,
            channel.username,
        )
        try:
            fallback_entity = await collector._pool.resolve_entity_with_warm(
                session,
                phone,
                PeerChannel(channel_id),
                operation="collect_channel_resolve_channel_id",
            )
        except HandledFloodWaitError as exc:
            return ResolveOutcome(
                flood_wait_sec=exc.info.wait_seconds,
                flood_wait_operation=exc.info.operation,
            )
        except Exception:
            logger.warning(
                "Channel %d: all entity lookups failed, " "deactivating",
                channel_id,
            )
            if channel.id:
                await collector._db.set_channel_active(channel.id, False)
            return ResolveOutcome(action="stop")
        new_username = getattr(fallback_entity, "username", None)
        new_title = (
            getattr(fallback_entity, "title", None)
            or channel.title
            or channel.username
            or str(channel_id)
        )
        await collector._handle_meta_change_review(
            channel,
            new_username,
            new_title,
            log_prefix="Channel",
        )
        return ResolveOutcome(action="stop")
    return ResolveOutcome(entity=entity)


async def _resolve_by_numeric(
    collector: "Collector",
    channel: Channel,
    session,
    phone: str,
    channel_id: int,
) -> ResolveOutcome:
    """Resolve a channel by numeric id, with preferred-phone rediscovery."""
    try:
        entity = await collector._pool.resolve_entity_with_warm(
            session,
            phone,
            PeerChannel(channel_id),
            operation="collect_channel_resolve_numeric",
        )
    except asyncio.TimeoutError:
        logger.warning(
            "get_entity timed out for channel %d, skipping",
            channel_id,
        )
        return ResolveOutcome(action="stop")
    except HandledFloodWaitError as exc:
        return ResolveOutcome(
            flood_wait_sec=exc.info.wait_seconds,
            flood_wait_operation=exc.info.operation,
        )
    except ValueError:
        # preferred_phone turned out to be wrong (account was kicked, or channel
        # added before warming finished). Invalidate and rediscover.
        if channel.preferred_phone or collector._pool.get_phone_for_channel(channel_id):
            channel = channel.model_copy(update={"preferred_phone": None})
            collector._pool.clear_channel_phone(channel_id)
            try:
                await collector._db.repos.channels.update_channel_preferred_phone(
                    channel_id, None
                )
            except Exception:
                # Pool is already cleared; a stale DB value just causes the same
                # rediscovery next restart. Log so the loop is visible.
                logger.warning(
                    "Channel %d: failed to clear stale preferred_phone in DB",
                    channel_id,
                    exc_info=True,
                )
        found = await collector._discover_phone_for_channel(channel_id, exclude=phone)
        if found is not None:
            collector._pool.register_channel_phone(channel_id, found)
            try:
                await collector._db.repos.channels.update_channel_preferred_phone(
                    channel_id, found
                )
            except Exception:
                # Pool already knows the right phone; a failed DB write only means
                # the rediscovery repeats next restart. Log it.
                logger.warning(
                    "Channel %d: failed to persist rediscovered preferred_phone=%s",
                    channel_id,
                    found,
                    exc_info=True,
                )
            logger.info(
                "Channel %d: rediscovered on %s, retrying",
                channel_id,
                found,
            )
            # finally releases current phone; next iter picks up found
            return ResolveOutcome(action="retry", channel=channel)
        logger.warning(
            "Channel %d (%s): no connected account can resolve entity; "
            "deactivating and skipping collection",
            channel_id,
            channel.title or channel.username or "no title",
        )
        if channel.id:
            await collector._db.set_channel_active(channel.id, False)
        return ResolveOutcome(action="stop", channel=channel)
    return ResolveOutcome(entity=entity)


async def resolve_channel_entity(
    collector: "Collector",
    channel: Channel,
    session,
    phone: str,
    channel_id: int,
    resolve_cache_only: bool,
    attempted_resolve_phones: set[str],
) -> ResolveOutcome:
    """Resolve a channel's Telegram entity for collection.

    Handles the username→numeric fallback, resolve rate-limit account rotation,
    preferred-phone invalidation/rediscovery, and flood waits. Returns a
    :class:`ResolveOutcome` the caller acts on. Behavior preserved exactly,
    including the ``RESOLVE_USERNAME_OPERATION`` flood label for username
    resolves.
    """
    if channel.username:
        return await _resolve_by_username(
            collector,
            channel,
            session,
            phone,
            channel_id,
            resolve_cache_only,
            attempted_resolve_phones,
        )
    return await _resolve_by_numeric(collector, channel, session, phone, channel_id)
