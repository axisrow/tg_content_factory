"""The gateway's sole owner of live Telegram sessions."""

from __future__ import annotations

import asyncio
import logging
import time

from telethon import events
from telethon_floodgate import FloodCircuitBreaker, TelegramRateLimitGate

from tg_agent.gateway.artifacts import ArtifactStore
from tg_agent.gateway.protocol import Delete, Edit, History, Operation, Publish, Send
from tg_agent.gateway.store import GatewayStore
from tg_agent.telegram.backends import TelegramTransportSession
from tg_agent.telegram.client_pool import HISTORY_CALIBRATED_SPEC, PROCESS_OUTGOING_SPEC
from tg_agent.telegram.collector_message_parse import build_message_from_telethon
from tg_agent.telegram.outgoing_ceiling import OutgoingRateCeiling
from tg_agent.telegram.session_lock import acquire_session_lock
from tg_shared.auth import TelegramAuth

logger = logging.getLogger(__name__)


class LiveTelegramAccess:
    """Cache one connection per account; releasing a request never disconnects it."""

    def __init__(self, store: GatewayStore, auth: TelegramAuth):
        self.store = store
        self.auth = auth
        self.sessions: dict[str, TelegramTransportSession] = {}
        self._connect_lock = asyncio.Lock()
        self._rate_limit_gate = TelegramRateLimitGate(category_limits={"history": HISTORY_CALIBRATED_SPEC})
        self._flood_breaker = FloodCircuitBreaker()
        self._outgoing_ceiling = OutgoingRateCeiling(PROCESS_OUTGOING_SPEC)
        self._startup: set[asyncio.Task] = set()

    async def start(self) -> None:
        for project, account in await self.store.subscriptions():
            task = asyncio.create_task(self._warm(project, account), name="gateway-account-connect")
            self._startup.add(task)
            task.add_done_callback(self._startup.discard)

    async def _warm(self, project: str, account: str) -> None:
        try:
            await self.connection(project, account)
        except Exception as exc:
            logger.warning("Gateway account unavailable: error=%s", type(exc).__name__)

    async def _incoming(self, account: str, event) -> None:
        message = event.message
        payload = {"kind": "dm", "dialog_id": event.chat_id, "message_id": message.id,
                   "text": message.raw_text, "out": bool(message.out),
                   "date": message.date.isoformat() if message.date else None}
        identity = f"{event.chat_id}:{message.id}:{int(bool(message.out))}"
        await self.store.record_event(account, identity, payload, now=time.time())

    async def report_flood(self, phone: str, wait_seconds: int) -> None:
        # The adapter's phone key is the stable gateway account UUID.
        await self.store.flood(phone, time.time() + wait_seconds)

    async def connection(self, project: str, account: str) -> TelegramTransportSession:
        secret = await self.store.session(project, account)
        async with self._connect_lock:
            if account not in self.sessions:
                await acquire_session_lock(secret, phone=account)
                raw = await self.auth.create_client_from_session(secret)
                raw.flood_sleep_threshold = 0
                async def incoming(event):
                    await self._incoming(account, event)

                raw.add_event_handler(incoming, events.NewMessage(func=lambda event: event.is_private))
                self.sessions[account] = TelegramTransportSession(raw, phone=account, pool=self)
            return self.sessions[account]

    async def execute(self, project: str, operation: Operation) -> dict:
        session = await self.connection(project, str(operation.account_id))
        entity = await session.resolve_entity(operation.peer)
        if isinstance(operation, History):
            # Telegram IDs and the cursor are normalized; no native object escapes.
            channel_id = abs(int(entity.id))
            messages = [build_message_from_telethon(message, channel_id).model_dump(mode="json")
                        async for message in session.stream_messages(
                            entity, min_id=operation.after_id, reverse=True, limit=operation.limit)]
            return {"messages": messages,
                    "next_id": max((message["message_id"] for message in messages), default=operation.after_id)}
        if isinstance(operation, Send):
            message = await session.send_message(entity, operation.text, reply_to=operation.reply_to)
            return {"message_id": message.id}
        if isinstance(operation, Edit):
            message = await session.edit_message(entity, operation.message_id, operation.text)
            return {"message_id": message.id if message else operation.message_id}
        if isinstance(operation, Delete):
            await session.delete_messages(entity, operation.message_ids)
            return {"deleted_ids": operation.message_ids}
        if isinstance(operation, Publish):
            artifacts = ArtifactStore(self.store.root)
            paths = [str(await asyncio.to_thread(artifacts.resolve, project, artifact))
                     for artifact in operation.artifact_ids]
            sent = await session.publish_files(entity, paths, caption=operation.caption)
            messages = sent if isinstance(sent, list) else [sent]
            return {"message_ids": [message.id for message in messages]}
        raise ValueError("Unsupported operation")

    async def close(self) -> None:
        for task in self._startup:
            task.cancel()
        await asyncio.gather(*self._startup, return_exceptions=True)
        self._startup.clear()
        await asyncio.gather(*(session.close() for session in self.sessions.values()), return_exceptions=True)
        self.sessions.clear()
        await self.auth.cleanup()
