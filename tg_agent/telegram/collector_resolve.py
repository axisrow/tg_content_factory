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
from telethon.tl.types import InputPeerSelf, InputPeerUser, InputUser, PeerChannel, PeerChat, PeerUser, User
from telethon_floodgate import HandledFloodWaitError, UsernameResolveRateLimitedError

from tg_agent.database.live_accounts import account_row_exists
from tg_agent.models import Channel
from tg_agent.parsers import bare_channel_id
from tg_agent.telegram.collector_types import ChannelCollectionTerminalError
from tg_agent.utils.safe_logging import mask_phone

if TYPE_CHECKING:
    from typing import Any, Protocol

    class Collector(Protocol):
        def __getattribute__(self, name: str, /) -> Any: ...
        def __setattr__(self, name: str, value: Any, /) -> None: ...

logger = logging.getLogger(__name__)

RESOLVE_USERNAME_OPERATION = "collect_channel_resolve_username"
TRANSIENT_REVIEW_REASONS = frozenset(
    {"resolve_transient_error", "resolve_account_unavailable", "stats_entity_unresolved",
     "numeric_unresolved"}
)


def _peer_for_channel_type(channel_type: str | None, channel_id: int):
    """Build the MTProto peer matching a stored channel type."""
    if channel_type == "group":
        return PeerChat(abs(channel_id))
    return PeerChannel(channel_id)


def _channel_peer(channel: Channel, channel_id: int):
    """Build the MTProto peer matching the stored channel type.

    Legacy basic groups are represented by ``PeerChat``.  Sending their id as
    ``PeerChannel`` raises ``ValueError`` even when the account is a member,
    which incorrectly enters the preferred-phone recovery path.
    """
    return _peer_for_channel_type(channel.channel_type, channel_id)


_USER_PEER_TYPES = (InputPeerUser, InputPeerSelf, InputUser, PeerUser, User)


def _int_attr(obj: object, name: str) -> int | None:
    value = getattr(obj, name, None)
    return value if isinstance(value, int) and not isinstance(value, bool) else None


def _username_owner_mismatch(entity: object, channel_id: int) -> str | None:
    """What a username-resolved peer is when it is NOT the tracked channel, else ``None``.

    Telegram hands a vacated username to whoever claims it next, and the id
    ranges of users and channels overlap, so both the peer type and the id are
    checked (#1769, #1779). Only a real integer id counts as evidence: an object
    without one says nothing either way and is left to the stream.
    """
    if isinstance(entity, _USER_PEER_TYPES) or _int_attr(entity, "user_id") is not None:
        return "a user"
    resolved = _int_attr(entity, "channel_id")
    if resolved is None:
        resolved = _int_attr(entity, "id")
    if resolved is None or bare_channel_id(resolved) == bare_channel_id(channel_id):
        return None
    return f"channel {bare_channel_id(resolved)}"


def _dead_username_code(exc: BaseException) -> str | None:
    """Telegram's definitive "nobody owns this name" answer as a terminal code.

    Telethon reports a vacant name as ``ValueError`` chained from
    ``UsernameNotOccupiedError``; a bare ``ValueError`` (a cache miss) is not
    that answer.
    """
    for candidate in (exc, exc.__cause__):
        if isinstance(candidate, (UsernameNotOccupiedError, UsernameInvalidError)):
            return type(candidate).__name__
    return None


def _transient_miss(exc: BaseException) -> bool:
    """Telethon reports a request that kept failing (-500 and the like) as a
    ValueError too; unlike a peer miss it says nothing about the peer."""
    return isinstance(exc, ValueError) and str(exc).startswith("Request was unsuccessful")


async def _id_unknown_to_every_account(
    collector: "Collector", channel: Channel, phone: str, channel_id: int, miss: BaseException
) -> bool:
    """True only when ``miss`` on ``phone`` is a real peer miss and every other
    active account misses the id too — a miss on one account proves nothing while
    another, perhaps unconnected, may know the channel (#1245)."""
    if not isinstance(miss, (ValueError, TypeError)) or _transient_miss(miss):
        return False
    verdict: dict[str, bool] = {}
    found = await collector._discover_phone_for_channel(
        channel_id, exclude=phone, channel_type=channel.channel_type, verdict=verdict
    )
    if found is not None:
        collector._pool.register_channel_phone(channel_id, found)  # the next pass reads there
        return False
    return bool(verdict.get("conclusive"))


class ResolveOutcome:
    """Result of :func:`resolve_channel_entity` — exactly one outcome the
    collection loop acts on, replacing the inline resolve block's mix of
    ``continue`` / ``return`` / ``raise`` (#923).

    - ``entity`` set → proceed (run pre-filters + stream);
    - ``action == "retry"`` → ``continue`` the loop (account rotation / preferred-
      phone rediscovery), adopting ``channel`` when it was updated;
    - ``action == "stop"`` → ``return total_collected`` (resolve timeout / review quarantine);
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

    __slots__ = ("entity", "action", "channel", "flood_wait_sec", "flood_wait_operation", "source")

    def __init__(
        self,
        *,
        entity=None,
        action: str = "proceed",
        channel: "Channel | None" = None,
        flood_wait_sec: int | None = None,
        flood_wait_operation: str | None = None,
        source: str | None = None,
    ) -> None:
        self.entity = entity
        self.action = action
        self.channel = channel
        self.flood_wait_sec = flood_wait_sec
        self.flood_wait_operation = flood_wait_operation
        # Where a username channel's key came from: cache:id / cache:username / live (#1877).
        self.source = source


async def _fall_back_to_the_id(
    collector: "Collector",
    channel: Channel,
    session,
    phone: str,
    channel_id: int,
    *,
    terminal: tuple[str, str] | None,
    cause: BaseException | None = None,
) -> ResolveOutcome:
    """The stored username no longer leads to this channel: look the row up by id.

    Found → the operator reviews the new name. Unknown to every active account →
    ``terminal`` (code, detail) when given. Anything else is transient: review.
    """
    try:
        entity = await collector._pool.resolve_entity_with_warm(
            session,
            phone,
            _channel_peer(channel, channel_id),
            operation="collect_channel_resolve_channel_id",
        )
    except HandledFloodWaitError as exc:
        return ResolveOutcome(
            flood_wait_sec=exc.info.wait_seconds,
            flood_wait_operation=exc.info.operation,
        )
    except Exception as miss:
        if terminal is not None and await _id_unknown_to_every_account(collector, channel, phone, channel_id, miss):
            # Nothing recovers this row until an operator fixes it (#1783). The
            # queue stamps it; the bulk enqueue stops spending a resolve on it
            # every round.
            raise ChannelCollectionTerminalError(*terminal) from cause or miss
        logger.warning(
            "Channel %d: all entity lookups failed transiently, marking for review",
            channel_id,
        )
        if channel.id:
            await collector._db.repos.channels.set_channel_review(channel.id, "resolve_transient_error")
        return ResolveOutcome(action="stop")
    new_title = getattr(entity, "title", None) or channel.title or channel.username or str(channel_id)
    await collector._handle_meta_change_review(
        channel,
        getattr(entity, "username", None),
        new_title,
        log_prefix="Channel",
    )
    return ResolveOutcome(action="stop")


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
        entity, source = await collector._resolve_channel_input_entity(
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
            # INFO (#1877): rotating past a spent budget is the designed path.
            logger.info(
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
    except (ValueError, UsernameNotOccupiedError, UsernameInvalidError) as username_exc:
        logger.warning(
            "Channel %d (%s): username not found, " "trying numeric ID fallback",
            channel_id,
            channel.username,
        )
        # Telegram says nobody owns the name: terminal once no account knows the id.
        dead_code = _dead_username_code(username_exc)
        return await _fall_back_to_the_id(
            collector,
            channel,
            session,
            phone,
            channel_id,
            terminal=(dead_code, f"@{channel.username} is vacant and the stored id is unknown") if dead_code else None,
            cause=username_exc,
        )
    foreign = _username_owner_mismatch(entity, channel_id)
    if foreign is not None:
        # The stored username now leads somewhere else. Streaming it would file
        # someone else's posts under this channel's id, so nothing is read; the
        # row is looked up by its id instead (#1769, #1779).
        logger.warning(
            "Channel %d: stored username now belongs to %s; nothing collected, looking the id up",
            channel_id,
            foreign,
        )
        return await _fall_back_to_the_id(
            collector,
            channel,
            session,
            phone,
            channel_id,
            terminal=("UsernameChannelIdMismatch", f"@{channel.username} now belongs to {foreign}"),
        )
    if channel.id and channel.review_reason in TRANSIENT_REVIEW_REASONS:
        await collector._db.repos.channels.clear_channel_review(channel.id)
    return ResolveOutcome(entity=entity, source=source)


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
            _channel_peer(channel, channel_id),
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
    except ValueError as miss:
        # A numeric-PeerChannel miss means "this account cannot resolve the
        # channel" — NOT "the channel is gone". It may justify invalidating
        # the stored preferred_phone when the resolve ran on the channel's OWN
        # preferred account. When #1245's fallback
        # routed us to an arbitrary non-member account (because the real owner
        # was transiently flood-waited), a miss is expected and must not clear
        # preferred_phone or deactivate a live channel — that would turn a
        # temporary owner flood into permanent data loss (pool_dialogs.py:516
        # documents the same "miss on an arbitrary account proves nothing").
        own_preferred = channel.preferred_phone or collector._pool.get_phone_for_channel(
            channel_id
        )
        # A miss on a #1245 FALLBACK account (phone != own preferred) only means
        # "this arbitrary account is not a member" — it proves nothing about the
        # channel. Skip this pass WITHOUT clearing preferred_phone or
        # deactivating ONLY while the owner account is still alive, so a
        # transiently-unavailable owner (flooded, in-use, or briefly
        # disconnected) recovers on a later pass. "Still alive" = the owner is a
        # live/usable account in the DB (get_account(active_only=True) is not
        # None — this already covers the flood case, since a flood-waited account
        # stays active). A DEAD owner (deleted/deactivated → get_account None)
        # falls through to rediscovery + review quarantine below, so a channel whose
        # sole owner account was removed no longer sticks forever active-but-
        # uncollectable (liveness regression Fix B first introduced, dual-review).
        if own_preferred is not None and phone != own_preferred:
            owner_account = await collector._pool._lease_pool.get_account(
                own_preferred, active_only=True
            )
            if owner_account is not None:
                logger.warning(
                    "Channel %d: numeric resolve failed on fallback account %s "
                    "(owner %s alive but currently unavailable — flood/in-use/"
                    "disconnected); keeping preferred_phone and channel active, "
                    "skipping this pass",
                    channel_id,
                    mask_phone(phone),
                    mask_phone(own_preferred),
                )
                return ResolveOutcome(action="stop", channel=channel)
            if await account_row_exists(collector._db, own_preferred):
                logger.warning(
                    "Channel %d: preferred owner %s exists but is temporarily "
                    "unusable; keeping preferred_phone and channel active",
                    channel_id,
                    mask_phone(own_preferred),
                )
                return ResolveOutcome(action="stop", channel=channel)
            logger.warning(
                "Channel %d: preferred owner %s is no longer a live account "
                "(removed/deactivated); proceeding to rediscovery/review",
                channel_id,
                mask_phone(own_preferred),
            )
        # preferred_phone turned out to be wrong (account was kicked, or channel
        # added before warming finished). Invalidate and rediscover.
        #
        # Clear against own_preferred — the phone observed at the TOP of this
        # except block, BEFORE the `await get_account` liveness lookup. Do NOT
        # re-read the mapping here: a concurrent task may have persisted a new
        # valid owner during that await, and a re-read would then hand that fresh
        # owner to forget_channel_phone as if it were the stale one. Passing the
        # originally-observed phone (plus the atomic conditional UPDATE in
        # clear_preferred_phone_if_matches) guarantees we only clear the account
        # that actually failed, never a concurrently-installed replacement
        # (#1245 dual-review).
        if own_preferred is not None:
            channel = channel.model_copy(update={"preferred_phone": None})
            # TOCTOU-safe clear: forget_channel_phone drops the in-memory map
            # unconditionally but only NULLs the DB preferred_phone when it still
            # matches the account that just failed (only_if_phone). A concurrent
            # writer that has meanwhile persisted a NEW valid owner must not be
            # clobbered by this stale error-recovery task (pool_dialogs.py:239).
            await collector._pool.forget_channel_phone(
                channel_id, only_if_phone=own_preferred
            )
        verdict: dict[str, bool] = {}
        found = await collector._discover_phone_for_channel(
            channel_id, exclude=phone, channel_type=channel.channel_type, verdict=verdict
        )
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
            "marking for review and skipping this pass",
            channel_id,
            channel.title or channel.username or "no title",
        )
        if channel.id:
            await collector._db.repos.channels.set_channel_review(channel.id, "resolve_account_unavailable")
        if verdict.get("conclusive") and not _transient_miss(miss):
            # Every active account answered with a peer miss and the row has
            # no username to warm the entity from: nothing will ever recover it
            # (#1805). The queue stamps it instead of sweeping every account's
            # dialogs for it again on the next round.
            raise ChannelCollectionTerminalError(
                "HistoryNumericUnresolved", "no connected account knows this id and the row has no username"
            ) from miss
        return ResolveOutcome(action="stop", channel=channel)
    # Self-heal on success (#1327): remember which account resolved this channel
    # so the NEXT pass routes there directly instead of rolling the dice on
    # get_available_client() again. Mirrors fetch_channel_meta's same pattern
    # (pool_dialogs.py, #808) — write only if preferred_phone is unset, or force
    # an overwrite when the resolve happened on a DIFFERENT account than the
    # stored preferred (that stored value was stale/unavailable and this account
    # just confirmably worked).
    own_preferred = channel.preferred_phone or collector._pool.get_phone_for_channel(
        channel_id
    )
    if channel.id and channel.review_reason in TRANSIENT_REVIEW_REASONS:
        await collector._db.repos.channels.clear_channel_review(channel.id)
    await collector._pool.remember_channel_phone(
        channel_id,
        phone,
        known_preferred=own_preferred,
        force=phone != own_preferred,
    )
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
