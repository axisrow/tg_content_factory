"""The gateway's sole owner of live Telegram sessions."""

from __future__ import annotations

import asyncio
import logging
import shutil
import time

from telethon import events
from telethon.tl.types import PeerChannel, PeerChat, PeerUser

from tg_agent.config import TelegramRuntimeConfig
from tg_agent.gateway.artifacts import ArtifactStore
from tg_agent.gateway.owned_pool import GatewayClientPool
from tg_agent.gateway.pool_storage import GatewayPoolStorage
from tg_agent.gateway.protocol import (
    BusinessAction,
    Delete,
    Dialogs,
    DmHistory,
    Edit,
    History,
    InspectPeer,
    MessagePage,
    Notification,
    Operation,
    Profile,
    Publish,
    ScheduledDelete,
    Search,
    Send,
    VoiceSource,
)
from tg_agent.gateway.store import GatewayStore
from tg_agent.telegram.backends import TelegramTransportSession
from tg_agent.telegram.collector_message_parse import build_message_from_telethon
from tg_shared.auth import TelegramAuth

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
        await self._pool.report_flood(phone, wait_seconds)

    def account_status(self, account: str) -> dict:
        session = self.sessions.get(account)
        return {"connected": session is not None and session.raw_client.is_connected(),
                "resolve_backoff_remaining_sec": (self._pool.get_resolve_username_backoff_remaining_sec(account)
                                                   if self._pool is not None else 0),
                "warming": self._pool.is_warming() if self._pool is not None else False}

    async def remove_account(self, account: str):
        if self._pool is not None:
            await self._pool.remove_client(account)

    async def connection(self, project: str, account: str) -> TelegramTransportSession:
        await self.store.session(project, account)  # Authorization is checked even for an existing pooled client.
        async with self._connect_lock:
            if self._pool is None:
                storage = GatewayPoolStorage(self.store)
                await storage.initialize()
                self._pool = GatewayClientPool(self.auth, storage, runtime_config=TelegramRuntimeConfig(
                    backend_mode="native", session_cache_dir=str(self.store.root / "telegram_sessions")))
                await self._pool.initialize(phones=[])
                self.sessions = self._pool.clients
            existing = self.sessions.get(account)
            if existing is not None and not existing.raw_client.is_connected():
                await self._pool.remove_client(account)
            if account not in self.sessions:
                accounts = await self._pool._db.get_live_usable_accounts(active_only=True)
                record = next(row for row in accounts if row.phone == account)
                await self._pool._connect_account(record)
                raw = self.sessions[account].raw_client
                async def incoming(event):
                    await self._incoming(account, event)

                raw.add_event_handler(incoming, events.NewMessage(func=lambda event: event.is_private))
            return self.sessions[account]

    async def execute(self, project: str, operation: Operation) -> dict:
        if isinstance(operation, VoiceSource):
            from tg_agent.gateway.transcription import read_voice

            return await read_voice(self, project, str(operation.account_id), operation.peer, operation.message_id,
                                    metadata_only=operation.metadata_only)
        if isinstance(operation, Notification):
            from tg_agent.gateway.notifications import execute_notification

            return await execute_notification(self, project, operation)
        if isinstance(operation, (Profile, InspectPeer, Dialogs, Search, DmHistory, MessagePage)):
            from tg_agent.gateway.reads import execute_read

            return await execute_read(self, project, operation)
        if isinstance(operation, BusinessAction):
            from tg_agent.gateway.actions import execute_action

            return await execute_action(self, project, operation)
        session = await self.connection(project, str(operation.account_id))
        peer = operation.peer
        kind = getattr(operation, "peer_type", "auto")
        if isinstance(peer, int) and kind != "auto":
            peer = {"channel": PeerChannel, "chat": PeerChat, "user": PeerUser}[kind](abs(peer))
        if isinstance(peer, str):
            entity = await self._pool.resolve_entity_with_warm(
                session, str(operation.account_id), peer, operation="gateway_resolve")
        else:
            entity = await session.resolve_entity(peer)
        if isinstance(operation, History):
            # Telegram IDs and the cursor are normalized; no native object escapes.
            channel_id = abs(int(entity.id))
            messages = [build_message_from_telethon(message, channel_id).model_dump(mode="json")
                        async for message in session.stream_messages(
                            entity, min_id=operation.after_id, reverse=True, limit=operation.limit)]
            return {"messages": messages,
                    "next_id": max((message["message_id"] for message in messages), default=operation.after_id)}
        if isinstance(operation, Send):
            message = await session.send_message(entity, operation.text, reply_to=operation.reply_to,
                                                  link_preview=operation.link_preview)
            return {"message_id": message.id}
        if isinstance(operation, Edit):
            message = await session.edit_message(entity, operation.message_id, operation.text)
            return {"message_id": message.id if message else operation.message_id}
        if isinstance(operation, Delete):
            await session.delete_messages(entity, operation.message_ids)
            return {"deleted_ids": operation.message_ids}
        if isinstance(operation, ScheduledDelete):
            await session.delete_scheduled_messages(entity, operation.message_ids)
            return {"deleted_ids": operation.message_ids}
        if isinstance(operation, Publish):
            artifacts = ArtifactStore(self.store.root)
            from uuid import uuid4

            directory = artifacts.path(project, uuid4()).with_suffix(".send")
            await asyncio.to_thread(directory.mkdir, parents=True, mode=0o700)
            try:
                paths = []
                for index, artifact in enumerate(operation.artifact_ids):
                    path = await asyncio.to_thread(artifacts.resolve, project, artifact)
                    info = await asyncio.to_thread(artifacts.describe, project, artifact)
                    target = directory / f"{index}-{info['filename']}"
                    await asyncio.to_thread(shutil.copyfile, path, target)
                    paths.append(str(target))
                sent = await session.publish_files(entity, paths, caption=operation.caption,
                                                   schedule=operation.schedule, reply_to=operation.reply_to,
                                                   force_document=operation.force_document)
                messages = sent if isinstance(sent, list) else [sent]
                return {"message_ids": [message.id for message in messages]}
            finally:
                await asyncio.to_thread(shutil.rmtree, directory)
        raise ValueError("Unsupported operation")

    async def close(self) -> None:
        for task in self._startup:
            task.cancel()
        await asyncio.gather(*self._startup, return_exceptions=True)
        self._startup.clear()
        if self._pool is not None:
            await self._pool.disconnect_all()
        await self.auth.cleanup()
