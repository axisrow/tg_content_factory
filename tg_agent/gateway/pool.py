"""Project-side access to shared accounts; this module never creates Telegram clients."""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, timezone
from math import ceil
from typing import Literal
from uuid import UUID, uuid4

import httpx
from telethon_floodgate import TRANSIENT_FLOOD_WAIT_MAX_SEC

from tg_agent.gateway.catalog import ProjectAccountCatalog
from tg_agent.gateway.client import TelegramGatewayClient
from tg_agent.gateway.outbox import ProjectOutbox
from tg_agent.gateway.protocol import Dialogs, History, InspectPeer, MessagePage, Profile, is_read_operation
from tg_agent.models import Message, TelegramUserInfo
from tg_agent.telegram.collector_types import AllCollectionClientsFloodedError, NoActiveCollectionClientsError

logger = logging.getLogger(__name__)

# Cap on consecutive wait_for_limits rounds in one read_request (#1769 review):
# a gateway account persistently returning short deferred receipts must not
# spin here forever — past the cap the flood error propagates so the caller's
# stop/error path bounds the loop.
READ_WAIT_LIMIT_ROUNDS = 12


class GatewayUncertainError(TimeoutError):
    """A write may have reached Telegram; never automatically create another intent."""


class GatewayOperationError(RuntimeError):
    def __init__(self, state, result):
        self.code = (result or {}).get("error")
        super().__init__(f"Gateway operation {state}: {self.code or state}")


class GatewayReadDeferredError(AllCollectionClientsFloodedError):
    """A receipt's retry deadline, preserving local pacing versus Telegram waits."""

    def __init__(self, result):
        super().__init__(max(1, ceil(result["retry_at"] - time.time())),
                         datetime.fromtimestamp(result["retry_at"], timezone.utc))
        self.reason = result.get("reason", "account_unavailable")
        self.wait_seconds = result.get("wait_seconds", self.retry_after_sec)
        self.local = self.reason in {
            "TelegramRateLimitedError", "TelegramOperationSuspendedError", "TelegramPeerRateLimitedError",
        }
        self.args = (f"{self.reason}; retry at {self.next_available_at.isoformat()} "
                     f"(in {self.retry_after_sec}s)",)


class GatewayAuth:
    """Project workers have no local auth key or Telegram credentials."""
    is_configured = True

    async def cleanup(self):
        pass


class GatewayPool:
    _gateway_project = True

    def __init__(self, config, db, *, client=None):
        self.config = config
        self._db = db
        self.client = client
        self.clients = {}
        self.accounts = []
        self._refresh_task = None
        self._disconnecting = False
        self.gateway = self
        self.gateway_status = {"http_available": False, "dispatcher_running": False}
        self.outbox = ProjectOutbox(db)

    async def initialize(self, **kwargs):
        await self.outbox.initialize()
        await self.outbox.prune()
        try:
            if self.client is None:
                self.client = await TelegramGatewayClient.from_config(self.config)
            await ProjectAccountCatalog(self._db, self.client).refresh()
            status = await self.client.status()
            self.gateway_status = status
            self.accounts = status["accounts"]
            self.clients = {row["phone"]: row for row in self.accounts if row.get("connected", True)}
        except (OSError, RuntimeError, httpx.HTTPError) as exc:
            self.clients = {}
            self.gateway_status = {"http_available": False, "dispatcher_running": False,
                                   "reason": type(exc).__name__}
            logger.warning("Project gateway unavailable: %s", type(exc).__name__)
            if self.client is not None and isinstance(exc, (OSError, httpx.TransportError)):
                await self.client.close()
                self.client = None
        if self._refresh_task is None:
            self._refresh_task = asyncio.create_task(self._refresh_loop(), name="project-gateway-refresh")

    async def _refresh_loop(self):
        while not self._disconnecting:
            await asyncio.sleep(5)
            await self.initialize()

    async def disconnect_all(self):
        self._disconnecting = True
        if self._refresh_task is not None:
            self._refresh_task.cancel()
            await asyncio.gather(self._refresh_task, return_exceptions=True)
            self._refresh_task = None
        if self.client is not None:
            await self.client.close()
        self.clients.clear()

    async def warm_all_dialogs(self, **kwargs):
        await self.initialize()
        for phone in list(self.clients):
            await self.get_dialogs_for_phone(phone, include_dm=True, refresh=True)

    async def authenticate(self, action, payload):
        if self.client is None:
            raise NoActiveCollectionClientsError("Shared gateway is unavailable")
        allowed = {"phone", "code", "phone_code_hash", "password_2fa", "qr_id", "session_string"}
        fields = {key: value for key, value in payload.items() if key in allowed}
        response = await self.client.http.post("/v1/auth", json={"action": action, **fields})
        if response.status_code == 409 and response.json().get("detail") == "TwoFactorRequiredError":
            from tg_shared.auth import TwoFactorRequiredError

            raise TwoFactorRequiredError()
        response.raise_for_status()
        await self.initialize()
        return response.json()

    def is_warming(self):
        return any(row.get("warming", False) for row in self.accounts)

    def get_resolve_username_backoff_remaining_sec(self, phone=None):
        return max((row.get("resolve_backoff_remaining_sec", 0) for row in self.accounts
                    if phone is None or row["phone"] == phone), default=0)

    async def available_collection_client_count(self):
        return sum(row["flood_until"] <= time.time() for row in self.clients.values())

    available_stats_client_count = available_collection_client_count

    def connected_phones(self):
        return set(self.clients)

    async def get_stats_availability(self):
        from tg_agent.telegram.client_pool import StatsClientAvailability

        try:
            self.select()
        except NoActiveCollectionClientsError:
            return StatsClientAvailability("no_connected_active")
        except AllCollectionClientsFloodedError as exc:
            return StatsClientAvailability("all_flooded", exc.retry_after_sec, exc.next_available_at)
        return StatsClientAvailability("available")

    async def get_users_info(self, include_avatar=True):
        result = []
        for row in sorted(self.clients.values(), key=lambda item: not item.get("is_primary")):
            data = await self.request(Profile(kind="profile", account_id=UUID(row["id"]),
                                              include_avatar=include_avatar), f"profile:{uuid4()}")
            result.append(TelegramUserInfo(phone=row["phone"], is_primary=row.get("is_primary", False), **data))
        return result

    async def get_dialogs_for_phone(self, phone, *, include_dm=False, refresh=False, mode="full"):
        account = self.select(phone)
        if account["phone"] != phone:
            raise PermissionError("Account is not available to this project")
        result = await self.request(Dialogs(kind="dialogs", account_id=UUID(account["id"]),
                                           include_dm=include_dm, refresh=refresh, mode=mode), f"dialogs:{uuid4()}")
        return result["dialogs"]

    async def get_dialogs(self, **kwargs):
        rows = []
        for phone in list(self.clients):
            rows.extend(await self.get_dialogs_for_phone(phone, **kwargs))
        return rows

    async def inspect_peer(self, peer, *, channel_type=None, phone=None, statistics=False, topics=False,
                           wait_for_limits=False):
        account = self.select(phone)
        # Rotate across the project's connected accounts when the first pick
        # cannot resolve the peer: a private channel may be visible only to the
        # account that collected it, and preferred_phone is often unknown
        # (#1765 review). Entity-resolution/ChannelPrivate misses are retryable
        # read errors, so read_request walks the remaining usable accounts.
        peer_kind: Literal["chat", "channel"] = "chat" if channel_type == "group" else "channel"
        return await self.read_request(InspectPeer(kind="inspect_peer", account_id=UUID(account["id"]), peer=peer,
                                                   peer_type=peer_kind,
                                                   statistics=statistics, topics=topics), f"inspect:{uuid4()}",
                                       rotate=True, wait_for_limits=wait_for_limits)

    async def fetch_channel_meta(self, channel_id, channel_type=None, phone=None):
        return await self.inspect_peer(channel_id, channel_type=channel_type, phone=phone)

    async def resolve_channel(self, identifier, *, phone=None, channel_type=None, wait_for_limits=False, **kwargs):
        peer = int(identifier) if str(identifier).lstrip("-").isdigit() else identifier
        return await self.inspect_peer(peer, channel_type=channel_type, phone=phone, wait_for_limits=wait_for_limits)

    async def resolve_any_entity(self, identifier, phone=None):
        peer = int(identifier) if str(identifier).lstrip("-").isdigit() else identifier
        account = self.select(phone)
        return await self.request(InspectPeer(kind="inspect_peer", account_id=UUID(account["id"]), peer=peer,
                                             peer_type="auto"), f"resolve:{uuid4()}")

    async def get_forum_topics(self, channel_id):
        return (await self.inspect_peer(channel_id, topics=True))["topics"]

    async def own_user_id(self, phone):
        account = self.select(phone)
        if account["phone"] != phone:
            raise PermissionError("Account is not available to this project")
        result = await self.request(Profile(kind="profile", account_id=UUID(account["id"])), f"profile:{uuid4()}")
        return result["user_id"]

    async def read_messages(self, peer, *, phone=None, limit=50, offset_id: int | None = 0, topic_id=None,
                            include_reaction_users=False, reaction_users_limit=20, peer_type="auto"):
        # CLI --offset-id is None when unspecified (#1752); the MessagePage
        # contract requires a concrete int >= 0.
        offset_id = offset_id or 0
        account = self.select(phone)
        result = {"messages": [], "reaction_users": {}, "forward_ids": [], "urls": {}}
        while len(result["messages"]) < limit:
            size = min(100, limit - len(result["messages"]))
            operation = MessagePage(kind="message_page", account_id=UUID(account["id"]), peer=peer,
                peer_type=peer_type, limit=size, offset_id=offset_id, topic_id=topic_id,
                include_reaction_users=include_reaction_users, reaction_users_limit=reaction_users_limit)
            data = await self.read_request(operation, f"read-messages:{uuid4()}", rotate=phone is None)
            result["messages"].extend(data["messages"])
            result["reaction_users"].update(data["reaction_users"])
            result["forward_ids"].extend(data["forward_ids"])
            result["urls"].update(data["urls"])
            if len(data["messages"]) < size:
                break
            next_offset = min(row["message_id"] for row in data["messages"])
            if next_offset == offset_id:
                raise ValueError("Gateway returned a non-advancing message page")
            offset_id = next_offset
        return result

    def select(self, preferred_phone=None, *, exclude=()):
        available = [row for row in self.clients.values() if row["active"] and row["id"] not in exclude]
        if not available:
            raise NoActiveCollectionClientsError("Shared gateway has no connected project accounts")
        usable = [row for row in available if row["flood_until"] <= time.time()]
        if not usable:
            until = min(row["flood_until"] for row in available)
            raise AllCollectionClientsFloodedError(max(1, int(until - time.time())),
                                                   datetime.fromtimestamp(until, timezone.utc))
        return next((row for row in usable if row["phone"] == preferred_phone),
                    next((row for row in usable if row.get("is_primary")), usable[0]))

    async def request(self, operation, intent_key):
        client = self.client
        if client is None:
            raise NoActiveCollectionClientsError("Shared gateway is unavailable")
        request = await self.outbox.prepare(intent_key, operation)
        try:
            await client.submit(request, operation)
            receipt = await client.wait(request)
        except httpx.HTTPError as exc:
            raise NoActiveCollectionClientsError("Shared gateway is unavailable; request remains persisted") from exc
        except TimeoutError as exc:
            if not is_read_operation(operation):
                raise GatewayUncertainError(
                    "Gateway receipt deadline expired; original request remains persisted") from exc
            raise NoActiveCollectionClientsError("Gateway read deadline expired; retry the saved request") from exc
        except asyncio.CancelledError:
            await client.cancel(request)
            raise
        except RuntimeError as exc:
            if getattr(getattr(client, "http", None), "is_closed", False):
                raise NoActiveCollectionClientsError("Shared gateway client was reconnected; retry the read") from exc
            raise
        state = receipt["state"]
        if state in ("succeeded", "failed"):
            try:
                # #1773: a terminal receipt moves the row to the short retention
                # bucket. Audit only — a failed record never changes the
                # operation's outcome, the row just stays pending. Uncertain and
                # deferred receipts are deliberately NOT recorded: replaying an
                # intent key after prune depends on the row existing, and an
                # unconfirmed send whose row pruned after 7 days would re-send
                # as a new intent on the same deterministic key.
                await self.outbox.record(intent_key, receipt)
            except Exception:
                logger.warning("Failed to record gateway receipt for %s", intent_key, exc_info=True)
        if state == "deferred":
            raise GatewayReadDeferredError(receipt["result"])
        if state == "uncertain":
            raise GatewayUncertainError("Telegram delivery is unconfirmed; verify before creating another intent")
        if state != "succeeded":
            from tg_agent.services import telegram_actions

            error = (receipt.get("result") or {}).get("error")
            known = {name: getattr(telegram_actions, name) for name in (
                "TelegramActionClientUnavailableError", "TelegramActionEntityResolutionError",
                "TelegramActionMessageNotFoundError", "TelegramActionNoMediaError", "TelegramActionPathEscapeError")}
            if error in known:
                raise known[error](error)
            raise GatewayOperationError(state, receipt.get("result"))
        return receipt["result"]

    async def read_request(self, operation, intent, *, rotate=False, wait_for_limits=False):
        from tg_agent.services.telegram_actions import TelegramActionEntityResolutionError

        if not is_read_operation(operation):
            raise ValueError("Account rotation is only allowed for read operations")
        excluded, floods, unavailable = set(), [], set()
        wait_rounds = 0
        while True:
            account = str(operation.account_id)
            try:
                return await self.request(operation, f"{intent}:account:{account}")
            except (GatewayOperationError, TelegramActionEntityResolutionError,
                    AllCollectionClientsFloodedError) as exc:
                retryable = isinstance(exc, (TelegramActionEntityResolutionError, AllCollectionClientsFloodedError))
                retryable = retryable or getattr(exc, "code", None) in {
                    "ValueError", "ChannelPrivateError", "ChannelInvalidError", "PeerIdInvalidError",
                    "UsernameResolveRateLimitedError", "UsernameResolveFloodWaitDeferredError",
                }
                if not rotate or not retryable:
                    raise
                excluded.add(account)
                if isinstance(exc, AllCollectionClientsFloodedError):
                    floods.append((operation, exc))
                else:
                    unavailable.add(account)
                try:
                    replacement = self.select(exclude=excluded)
                except (NoActiveCollectionClientsError, AllCollectionClientsFloodedError):
                    if floods:
                        retryable_waits = [
                            item for item in floods
                            if (isinstance(item[1], GatewayReadDeferredError) and item[1].local)
                            or getattr(item[1], "wait_seconds", item[1].retry_after_sec) <= TRANSIENT_FLOOD_WAIT_MAX_SEC
                        ]
                        if wait_for_limits and retryable_waits:
                            wait_rounds += 1
                            if wait_rounds > READ_WAIT_LIMIT_ROUNDS:
                                raise min(floods, key=lambda item: item[1].next_available_at)[1] from exc
                            operation, delay = min(retryable_waits, key=lambda item: item[1].next_available_at)
                            seconds = max(0.1, delay.next_available_at.timestamp() - time.time())
                            logger.info("Gateway read waiting: kind=%s reason=%s retry_in=%.1fs",
                                        operation.kind, getattr(delay, "reason", "account_unavailable"), seconds)
                            await asyncio.sleep(seconds)
                            excluded, floods = set(unavailable), []
                            continue
                        raise min(floods, key=lambda item: item[1].next_available_at)[1] from exc
                    raise exc from None
                operation = operation.model_copy(update={"account_id": UUID(replacement["id"])})

    async def collect_channel(self, channel, *, full=False, cancel_event=None, progress_callback=None,
                              intent_key=None, page_callback=None):
        account = self.select(channel.preferred_phone)
        cursor = 0 if full else channel.last_collected_id
        key = intent_key or f"collect:{uuid4()}"
        collected = 0
        username_fallback_used = False
        while cancel_event is None or not cancel_event.is_set():
            operation = History(kind="history", account_id=UUID(account["id"]), peer=channel.channel_id,
                                peer_type="chat" if channel.channel_type == "group" else "channel", after_id=cursor)
            request = asyncio.create_task(self.read_request(operation, f"{key}:{channel.channel_id}:{cursor}",
                                                             rotate=True))
            cancelled = asyncio.create_task(cancel_event.wait()) if cancel_event is not None else None
            try:
                if cancelled is not None:
                    await asyncio.wait({request, cancelled}, return_when=asyncio.FIRST_COMPLETED)
                    if cancel_event.is_set():
                        request.cancel()
                        await asyncio.gather(request, return_exceptions=True)
                        return collected
                data = await request
            except GatewayOperationError as exc:
                # Numeric-miss on every gateway account (#1779, the collection
                # twin of the refresh-card #1769 fallback): a bare PeerChannel
                # resolve needs the entity in the account dialog cache, which
                # most tracked channels are not in. One stored-@username
                # resolve — membership-free, and it caches the entity into the
                # resolving account's session — unlocks the numeric history.
                # Once per collect call; verification guards reoccupied names.
                username = getattr(channel, "username", None)
                if exc.code != "ValueError":
                    raise
                if username_fallback_used or not username:
                    # #1805: nothing will ever recover this numeric peer-miss —
                    # no stored username to warm the entity from, or the one-shot
                    # fallback already tried. Surface the terminal code so the
                    # queue stamps the channel and defers the bulk re-enqueue
                    # (396 such peers burned every account every round).
                    raise GatewayOperationError(
                        "failed", {"error": "HistoryNumericUnresolved"}
                    ) from exc
                username_fallback_used = True
                info = await self.resolve_channel(username, channel_type=channel.channel_type)
                resolved = (abs(int(info.get("channel_id") or 0))
                            if info and not info.get("gone") and not info.get("review") else 0)
                if not resolved:
                    if info and info.get("review"):
                        # Owner unavailable/unresolved (review sentinel, #1791):
                        # transient — never a terminal stamp on a live channel.
                        # Mark for review (the collector's auto-clear heals it on
                        # the next success) and re-raise the original peer-miss.
                        if channel.id:
                            await self._db.repos.channels.set_channel_review(
                                channel.id, info.get("reason") or "numeric_unresolved")
                        raise exc from None
                    raise GatewayOperationError("failed", {"error": "UsernameResolveMiss"}) from exc
                if info.get("channel_type") in ("user", "bot"):
                    # Telegram id ranges overlap: a username reoccupied by a
                    # USER can collide with the stored channel id and pass the
                    # numeric check. Never write a person's card onto a channel
                    # row (#1769 review, mirrored from refresh-card).
                    logger.warning(
                        "collect: @%s now belongs to a %s, not a channel; row untouched",
                        username, info.get("channel_type"))
                    raise GatewayOperationError("failed", {"error": "UsernameChannelIdMismatch"}) from exc
                if resolved != abs(channel.channel_id):
                    logger.warning(
                        "collect: @%s now resolves to channel %d, not stored %d; row untouched (#1779)",
                        username, resolved, channel.channel_id)
                    raise GatewayOperationError("failed", {"error": "UsernameChannelIdMismatch"}) from exc
                logger.info("collect: numeric miss on %s recovered via stored @%s; retrying history (#1779)",
                            channel.channel_id, username)
                # One resolve, one job (#1788): the id-verified inspect card
                # carries the fresh open metadata — write it here (the same
                # update_channel_card call refresh-card makes, including the
                # terminal-stamp clear on a verified identity), so no second
                # resolve is ever spent on this channel's meta.
                await self._db.repos.channels.update_channel_card(
                    channel.channel_id,
                    username=info.get("username"),
                    title=info.get("title") or None,
                    channel_type=info.get("channel_type") or None,
                    created_at=info.get("created_at"),
                    about=info.get("about"),
                    linked_chat_id=info.get("linked_chat_id"),
                    has_comments=bool(info.get("has_comments")),
                    meta_present=True,
                )
                # Fresh intent nonce: outbox.prepare dedups by action key, so
                # re-issuing the failed key would replay the stored failed
                # receipt instead of executing the retry (#1782 review).
                key = f"{key}:username-retry"
                continue
            finally:
                if cancelled is not None:
                    cancelled.cancel()
                    await asyncio.gather(cancelled, return_exceptions=True)
                if not request.done():
                    request.cancel()
                    await asyncio.gather(request, return_exceptions=True)
            messages = [Message.model_validate(row) for row in data["messages"]]
            if not messages:
                break
            next_id = data.get("next_id", max(message.message_id for message in messages))
            if next_id <= cursor:
                raise ValueError("Gateway returned a non-advancing collection page")
            collected += await self._db.repos.messages.save_collected_page(channel.channel_id, messages, next_id)
            if page_callback is not None:
                await page_callback(messages)
            cursor = next_id
            if progress_callback is not None:
                await progress_callback(collected)
        return collected
