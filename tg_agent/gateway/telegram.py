"""The gateway's sole owner of live Telegram sessions (served over the relay, #1868)."""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, timezone
from typing import TYPE_CHECKING, cast

from telethon import events
from telethon_floodgate import TelegramRateLimitedError

from tg_agent.config import TelegramRuntimeConfig
from tg_agent.gateway.owned_pool import GatewayClientPool
from tg_agent.gateway.pool_storage import GatewayPoolStorage
from tg_agent.gateway.store import GatewayStore
from tg_agent.telegram.backends import TelegramTransportSession
from tg_shared.auth import TelegramAuth

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable

    from tg_agent.database import Database

logger = logging.getLogger(__name__)


class LiveTelegramAccess:
    """Cache one connection per account; releasing a request never disconnects it."""

    def __init__(self, store: GatewayStore, auth: TelegramAuth):
        self.store = store
        self.auth = auth
        self.sessions: dict[str, TelegramTransportSession] = {}
        self._connect_lock = asyncio.Lock()
        self._pool = None
        self._startup: set[asyncio.Task] = set()
        self._warm_failures: dict[str, str] = {}  # account -> last connect failure
        # Set by the relay hub (``on_event(account)``): projects granted the account
        # pull its new DM events at once.
        self.on_event: Callable[[str], Awaitable[None]] | None = None

    async def start(self) -> None:
        for project, account in await self.store.subscriptions():
            name = f"gateway-account-connect:{account}"
            if self.account_status(account)["connected"] or any(task.get_name() == name for task in self._startup):
                continue
            task = asyncio.create_task(self._warm(project, account), name=name)
            self._startup.add(task)
            task.add_done_callback(self._startup.discard)

    async def _warm(self, project: str, account: str) -> None:
        try:
            await self.connection(project, account)
        except Exception as exc:
            # Housekeeping retries every half minute: a new failure is reported once,
            # with its text and traceback; its repeats stay one-line debug notes.
            failure = type(exc).__name__
            first = self._warm_failures.get(account) != failure
            self._warm_failures[account] = failure
            log = logger.warning if first else logger.debug
            log("Gateway account unavailable: account=%s error=%s: %s", account, failure, exc, exc_info=first)
        else:
            self._warm_failures.pop(account, None)

    async def _incoming(self, account: str, event) -> None:
        message = event.message
        payload = {"kind": "dm", "dialog_id": event.chat_id, "message_id": message.id,
                   "text": message.raw_text, "out": bool(message.out),
                   "date": message.date.isoformat() if message.date else None}
        identity = f"{event.chat_id}:{message.id}:{int(bool(message.out))}"
        await self.store.record_event(account, identity, payload, now=time.time())
        if self.on_event is not None:
            await self.on_event(account)

    async def report_flood(self, phone: str, wait_seconds: int) -> None:
        # The adapter's phone key is the stable gateway account UUID.
        assert self._pool is not None, "flood report before the gateway pool exists"
        await self._pool.report_flood(phone, wait_seconds)

    def account_status(self, account: str) -> dict:
        session = self.sessions.get(account)
        return {"connected": session is not None and session.raw_client.is_connected(),
                "resolve_backoff_remaining_sec": (self._pool.get_resolve_username_backoff_remaining_sec(account)
                                                   if self._pool is not None else 0),
                "warming": self._pool.is_warming() if self._pool is not None else False}

    async def persist_flood_guards(self) -> None:
        """Save the shared pool's flood-guard state and channel keys (best-effort; housekeeping calls it)."""
        if self._pool is not None:
            await self._pool.persist_flood_guard_snapshot()
            await self._pool.persist_session_entities()  # #1877

    async def remove_account(self, account: str):
        if self._pool is not None:
            await self._pool.remove_client(account)

    async def connection(self, project: str, account: str) -> TelegramTransportSession:
        """The account's live connection, connecting it on first use.

        Access is the caller's check: the relay hub verifies the project's grant
        on every request (``GatewayHub.client``) before asking for the connection.
        """
        async with self._connect_lock:
            if self._pool is None:
                storage = GatewayPoolStorage(self.store)
                await storage.initialize()
                pool_db = cast("Database", storage)  # implements the account slice of Database the pool uses
                self._pool = GatewayClientPool(self.auth, pool_db, runtime_config=TelegramRuntimeConfig(
                    backend_mode="native", session_cache_dir=str(self.store.root / "telegram_sessions")))
                await self._pool.initialize(phones=[])
                self.sessions = cast(dict[str, TelegramTransportSession], self._pool.clients)
            existing = self.sessions.get(account)
            if existing is not None and not existing.raw_client.is_connected():
                await self._pool.remove_client(account)
            if account not in self.sessions:
                accounts = await self._pool._db.get_live_usable_accounts(active_only=True)
                record = next(row for row in accounts if row.phone == account)
                # The RPC guard knows about a new wait before its SQLite write completes.
                pending = getattr(self._pool, "_rpc_pending_account_deadlines", {}).get(account)
                now = datetime.now(timezone.utc)
                flood_until = max(record.flood_wait_until or now, pending or now)
                if flood_until > now:
                    raise TelegramRateLimitedError(account, "flood_wait", (flood_until - now).total_seconds())
                await self._pool._connect_account(record)
                # Gateway accounts connect lazily, so this is the gateway-side
                # pool startup: re-verify the cached premium flag live (#1749)
                # and persist it into the gateway catalog for the project mirror.
                refresh = asyncio.create_task(self._pool._refresh_premium_flags(),
                                              name=f"gateway-premium-refresh:{account}")
                self._startup.add(refresh)
                refresh.add_done_callback(self._startup.discard)
                raw = self.sessions[account].raw_client
                async def incoming(event):
                    await self._incoming(account, event)

                raw.add_event_handler(incoming, events.NewMessage(func=lambda event: event.is_private))
            return self.sessions[account]

    async def close(self) -> None:
        for task in self._startup:
            task.cancel()
        await asyncio.gather(*self._startup, return_exceptions=True)
        self._startup.clear()
        if self._pool is not None:
            await self._pool.disconnect_all()
        await self.auth.cleanup()
