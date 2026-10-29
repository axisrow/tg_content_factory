"""Project-side access to shared accounts; this module never creates Telegram clients."""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, timezone
from uuid import UUID, uuid4

import httpx

from tg_agent.gateway.catalog import ProjectAccountCatalog
from tg_agent.gateway.client import TelegramGatewayClient
from tg_agent.gateway.outbox import ProjectOutbox
from tg_agent.gateway.protocol import Dialogs, History, InspectPeer, MessagePage, Profile, is_read_operation
from tg_agent.models import Message, TelegramUserInfo
from tg_agent.telegram.collector_types import AllCollectionClientsFloodedError, NoActiveCollectionClientsError

logger = logging.getLogger(__name__)


class GatewayUncertainError(TimeoutError):
    """A write may have reached Telegram; never automatically create another intent."""


class GatewayOperationError(RuntimeError):
    def __init__(self, state, result):
        self.code = (result or {}).get("error")
        super().__init__(f"Gateway operation {state}: {self.code or state}")


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

    async def inspect_peer(self, peer, *, channel_type=None, phone=None, statistics=False, topics=False):
        account = self.select(phone)
        return await self.request(InspectPeer(kind="inspect_peer", account_id=UUID(account["id"]), peer=peer,
                                             peer_type="chat" if channel_type == "group" else "channel",
                                             statistics=statistics, topics=topics), f"inspect:{uuid4()}")

    async def fetch_channel_meta(self, channel_id, channel_type=None):
        return await self.inspect_peer(channel_id, channel_type=channel_type)

    async def resolve_channel(self, identifier, **kwargs):
        peer = int(identifier) if str(identifier).lstrip("-").isdigit() else identifier
        return await self.inspect_peer(peer)

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

    async def read_messages(self, peer, *, phone=None, limit=50, offset_id=0, topic_id=None,
                            include_reaction_users=False, reaction_users_limit=20, peer_type="auto"):
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
        if self.client is None:
            raise NoActiveCollectionClientsError("Shared gateway is unavailable")
        request = await self.outbox.prepare(intent_key, operation)
        try:
            await self.client.submit(request, operation)
            receipt = await self.client.wait(request)
        except httpx.HTTPError as exc:
            raise NoActiveCollectionClientsError("Shared gateway is unavailable; request remains persisted") from exc
        except TimeoutError as exc:
            if not is_read_operation(operation):
                raise GatewayUncertainError(
                    "Gateway receipt deadline expired; original request remains persisted") from exc
            raise NoActiveCollectionClientsError("Gateway read deadline expired; retry the saved request") from exc
        except asyncio.CancelledError:
            await self.client.cancel(request)
            raise
        state = receipt["state"]
        if state == "deferred":
            until = receipt["result"]["retry_at"]
            raise AllCollectionClientsFloodedError(max(1, int(until - time.time())),
                                                   datetime.fromtimestamp(until, timezone.utc))
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

    async def read_request(self, operation, intent, *, rotate=False):
        from tg_agent.services.telegram_actions import TelegramActionEntityResolutionError

        if not is_read_operation(operation):
            raise ValueError("Account rotation is only allowed for read operations")
        excluded, floods = set(), []
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
                    floods.append(exc)
                try:
                    replacement = self.select(exclude=excluded)
                except (NoActiveCollectionClientsError, AllCollectionClientsFloodedError):
                    if floods:
                        raise min(floods, key=lambda error: error.retry_after_sec) from exc
                    raise exc from None
                operation = operation.model_copy(update={"account_id": UUID(replacement["id"])})

    async def collect_channel(self, channel, *, full=False, cancel_event=None, progress_callback=None,
                              intent_key=None, page_callback=None):
        account = self.select(channel.preferred_phone)
        cursor = 0 if full else channel.last_collected_id
        key = intent_key or f"collect:{uuid4()}"
        collected = 0
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
