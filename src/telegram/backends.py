from __future__ import annotations

import logging
from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
from dataclasses import dataclass
from types import SimpleNamespace
from typing import Any

from telethon import TelegramClient
from telethon_cli import runtime as telethon_cli_runtime
from telethon_cli.errors import CLIError

from src.models import Account
from src.telegram.auth import TelegramAuth
from src.telegram.session_materializer import SessionMaterializer

logger = logging.getLogger(__name__)


class BackendAcquireError(RuntimeError):
    """Raised when a backend cannot provide a usable authorized client."""


class UnsupportedBackendOperationError(RuntimeError):
    """Raised when a backend cannot support the requested transport/mode."""


class TelegramTransportSession:
    """Thin operation adapter that keeps raw Telethon calls inside backend code."""

    def __init__(self, client: Any, *, disconnect_on_close: bool = True):
        self._client = client
        self._disconnect_on_close = disconnect_on_close

    @property
    def raw_client(self) -> Any:
        return self._client

    async def close(self) -> None:
        if self._disconnect_on_close:
            await self._client.disconnect()

    async def fetch_me(self) -> Any:
        return await self._client.get_me()

    async def fetch_profile_photo(self, entity: Any, *, file: Any) -> Any:
        return await self._client.download_profile_photo(entity, file=file)

    async def resolve_entity(self, peer: Any) -> Any:
        return await self._client.get_entity(peer)

    async def resolve_input_entity(self, peer: Any) -> Any:
        return await self._client.get_input_entity(peer)

    async def warm_dialog_cache(self) -> Any:
        return await self._client.get_dialogs()

    def stream_dialogs(self) -> AsyncIterator[Any]:
        return self._client.iter_dialogs()

    def stream_messages(self, entity: Any, **kwargs: Any) -> AsyncIterator[Any]:
        return self._client.iter_messages(entity, **kwargs)

    async def publish_files(
        self,
        entity: Any,
        files: Any,
        *,
        caption: str | None = None,
        schedule: Any = None,
    ) -> Any:
        return await self._client.send_file(entity, files, caption=caption, schedule=schedule)

    async def send_message(self, entity: Any, message: Any, **kwargs: Any) -> Any:
        return await self._client.send_message(entity, message, **kwargs)

    async def forward_messages(self, entity: Any, messages: Any, from_peer: Any) -> Any:
        return await self._client.forward_messages(entity, messages, from_peer)

    async def edit_message(self, entity: Any, message: int, text: str, **kwargs: Any) -> Any:
        return await self._client.edit_message(entity, message, text, **kwargs)

    async def pin_message(self, entity: Any, message: Any, *, notify: bool = False) -> Any:
        return await self._client.pin_message(entity, message, notify=notify)

    async def unpin_message(self, entity: Any, message: Any = None) -> Any:
        return await self._client.unpin_message(entity, message)

    async def delete_messages(self, entity: Any, message_ids: list[int]) -> Any:
        return await self._client.delete_messages(entity, message_ids)

    async def send_reaction(self, entity: Any, message_id: int, emoji: str) -> Any:
        """Send a reaction to a message using the Telethon raw API."""
        try:
            from telethon.tl.functions.messages import SendReactionRequest
            from telethon.tl.types import ReactionEmoji

            return await self._client(
                SendReactionRequest(
                    peer=entity,
                    msg_id=message_id,
                    reaction=[ReactionEmoji(emoticon=emoji)],
                )
            )
        except ImportError:
            # Telethon not available (e.g. test env with stub backend)
            return None

    async def download_media(self, message: Any, *, file: Any = None) -> Any:
        return await self._client.download_media(message, file=file)

    async def get_participants(self, entity: Any, *, limit: int | None = None, search: str = "") -> Any:
        kwargs: dict[str, Any] = {}
        if limit is not None:
            kwargs["limit"] = limit
        if search:
            kwargs["search"] = search
        return await self._client.get_participants(entity, **kwargs)

    def stream_participants(self, entity: Any, **kwargs: Any) -> AsyncIterator[Any]:
        return self._client.iter_participants(entity, **kwargs)

    async def edit_admin(self, entity: Any, user: Any, **kwargs: Any) -> Any:
        return await self._client.edit_admin(entity, user, **kwargs)

    async def edit_permissions(self, entity: Any, user: Any, until_date: Any = None, **kwargs: Any) -> Any:
        if until_date is not None:
            kwargs["until_date"] = until_date
        return await self._client.edit_permissions(entity, user, **kwargs)

    async def kick_participant(self, entity: Any, user: Any) -> Any:
        return await self._client.kick_participant(entity, user)

    async def edit_folder(self, entity: Any, folder: int) -> Any:
        return await self._client.edit_folder(entity, folder)

    async def send_read_acknowledge(self, entity: Any, *, max_id: int | None = None) -> Any:
        kwargs: dict[str, Any] = {}
        if max_id is not None:
            kwargs["max_id"] = max_id
        return await self._client.send_read_acknowledge(entity, **kwargs)

    # --- base ---

    def set_proxy(self, proxy: Any) -> None:
        self._client.set_proxy(proxy)

    # --- uploads ---

    async def upload_file(self, file: Any, **kwargs: Any) -> Any:
        return await self._client.upload_file(file, **kwargs)

    # --- downloads ---

    async def download_file(self, input_location: Any, file: Any = None, **kwargs: Any) -> Any:
        return await self._client.download_file(input_location, file, **kwargs)

    def stream_download(self, file: Any, **kwargs: Any) -> AsyncIterator[Any]:
        return self._client.iter_download(file, **kwargs)

    # --- dialogs ---

    def stream_drafts(self) -> AsyncIterator[Any]:
        return self._client.iter_drafts()

    async def get_drafts(self) -> Any:
        return await self._client.get_drafts()

    def conversation(self, entity: Any, **kwargs: Any) -> Any:
        return self._client.conversation(entity, **kwargs)

    # --- users ---

    async def is_bot(self) -> bool:
        return await self._client.is_bot()

    async def is_user_authorized(self) -> bool:
        return await self._client.is_user_authorized()

    async def get_peer_id(self, peer: Any) -> int:
        return await self._client.get_peer_id(peer)

    # --- chats ---

    def stream_admin_log(self, entity: Any, **kwargs: Any) -> AsyncIterator[Any]:
        return self._client.iter_admin_log(entity, **kwargs)

    async def get_admin_log(self, entity: Any, **kwargs: Any) -> Any:
        return await self._client.get_admin_log(entity, **kwargs)

    def stream_profile_photos(self, entity: Any, **kwargs: Any) -> AsyncIterator[Any]:
        return self._client.iter_profile_photos(entity, **kwargs)

    async def get_profile_photos(self, entity: Any, **kwargs: Any) -> Any:
        return await self._client.get_profile_photos(entity, **kwargs)

    def action(self, entity: Any, action: str, **kwargs: Any) -> Any:
        return self._client.action(entity, action, **kwargs)

    async def get_permissions(self, entity: Any, user: Any = None) -> Any:
        return await self._client.get_permissions(entity, user)

    # --- updates ---

    async def set_receive_updates(self, enabled: bool) -> None:
        await self._client.set_receive_updates(enabled)

    async def run_until_disconnected(self) -> None:
        await self._client.run_until_disconnected()

    def on(self, event: Any) -> Any:
        return self._client.on(event)

    def add_event_handler(self, callback: Any, event: Any = None) -> None:
        self._client.add_event_handler(callback, event)

    def remove_event_handler(self, callback: Any, event: Any = None) -> bool:
        return self._client.remove_event_handler(callback, event)

    def list_event_handlers(self) -> list:
        return self._client.list_event_handlers()

    async def catch_up(self) -> None:
        await self._client.catch_up()

    # --- bots ---

    async def inline_query(self, bot: Any, query: str, **kwargs: Any) -> Any:
        return await self._client.inline_query(bot, query, **kwargs)

    # --- buttons ---

    def build_reply_markup(self, buttons: Any) -> Any:
        return self._client.build_reply_markup(buttons)

    # --- account ---

    def takeout(self, **kwargs: Any) -> Any:
        return self._client.takeout(**kwargs)

    async def end_takeout(self, success: bool) -> None:
        await self._client.end_takeout(success)

    # --- existing ---

    async def remove_dialog(self, entity: Any) -> None:
        await self._client.delete_dialog(entity)

    async def invoke_request(self, request: Any) -> Any:
        return await self._client(request)

    async def lookup_search_posts_flood(self, query: str = "") -> Any:
        from telethon.tl.functions.channels import CheckSearchPostsFloodRequest

        return await self.invoke_request(CheckSearchPostsFloodRequest(query=query))

    async def search_posts_batch(
        self,
        query: str,
        *,
        offset_rate: int,
        offset_peer: Any,
        offset_id: int,
        limit: int,
    ) -> Any:
        from telethon.tl.functions.channels import SearchPostsRequest

        return await self.invoke_request(
            SearchPostsRequest(
                query=query,
                offset_rate=offset_rate,
                offset_peer=offset_peer,
                offset_id=offset_id,
                limit=limit,
            )
        )

    async def fetch_full_channel(self, entity: Any) -> Any:
        from telethon.tl.functions.channels import GetFullChannelRequest

        return await self.invoke_request(GetFullChannelRequest(entity))

    async def fetch_full_chat(self, entity: Any) -> Any:
        from telethon.tl.functions.messages import GetFullChatRequest

        return await self.invoke_request(GetFullChatRequest(entity))

    async def get_broadcast_stats(self, entity: Any) -> Any:
        from telethon.tl.functions.stats import GetBroadcastStatsRequest

        return await self.invoke_request(GetBroadcastStatsRequest(channel=entity))

    async def fetch_forum_topics(self, entity: Any, *, limit: int = 100) -> Any:
        from telethon.tl.functions.messages import GetForumTopicsRequest

        return await self.invoke_request(
            GetForumTopicsRequest(
                peer=entity,
                offset_date=None,
                offset_id=0,
                offset_topic=0,
                limit=limit,
            )
        )


def adapt_transport_session(
    candidate: Any,
    *,
    disconnect_on_close: bool = False,
) -> TelegramTransportSession:
    if isinstance(candidate, TelegramTransportSession):
        return candidate
    return TelegramTransportSession(candidate, disconnect_on_close=disconnect_on_close)


@dataclass
class BackendClientLease:
    phone: str
    session: TelegramTransportSession
    backend_name: str
    disconnect_on_release: bool = True


class TelegramBackend(ABC):
    name: str

    @abstractmethod
    async def acquire_client(self, account: Account) -> BackendClientLease:
        raise NotImplementedError

    async def release(self, lease: BackendClientLease) -> None:
        if lease.disconnect_on_release:
            await lease.session.close()


class NativeTelethonBackend(TelegramBackend):
    name = "native"

    def __init__(self, auth: TelegramAuth):
        self._auth = auth

    async def acquire_client(self, account: Account) -> BackendClientLease:
        client = await self._auth.create_client_from_session(account.session_string)
        # Surface every FloodWaitError so run_with_flood_wait can call
        # pool.report_flood and rotate accounts (#495). Telethon's default
        # silently sleeps on waits ≤ threshold and hides the flood from us.
        client.flood_sleep_threshold = 0
        return BackendClientLease(
            phone=account.phone,
            session=TelegramTransportSession(client),
            backend_name=self.name,
        )


class TelethonCliBackend(TelegramBackend):
    name = "telethon_cli"

    def __init__(
        self,
        auth: TelegramAuth,
        materializer: SessionMaterializer,
        transport: str = "hybrid",
    ):
        self._auth = auth
        self._materializer = materializer
        self._transport = transport

    async def acquire_client(self, account: Account) -> BackendClientLease:
        if self._transport == "subprocess":
            raise UnsupportedBackendOperationError(
                "subprocess transport cannot expose a live TelegramClient"
            )

        session_path = self._materializer.materialize(account.phone, account.session_string)
        env_file = self._materializer.ensure_empty_env_file()
        namespace = SimpleNamespace(
            api_id=self._auth.api_id,
            api_hash=self._auth.api_hash,
            session=session_path,
            password=None,
            env_file=env_file,
        )
        try:
            client: TelegramClient = telethon_cli_runtime.create_client(namespace)
            client._connection_retries = None
            client._retry_delay = 2
            client.flood_sleep_threshold = 0
            await client.connect()
            if not await client.is_user_authorized():
                await client.disconnect()
                raise BackendAcquireError(f"Session is no longer valid for {account.phone}")
            return BackendClientLease(
                phone=account.phone,
                session=TelegramTransportSession(client),
                backend_name=self.name,
            )
        except CLIError as exc:
            raise BackendAcquireError(str(exc)) from exc


class BackendRouter:
    def __init__(
        self,
        *,
        mode: str,
        primary: TelegramBackend,
        native: NativeTelethonBackend,
    ):
        self._mode = mode
        self._primary = primary
        self._native = native

    async def acquire_client(
        self,
        account: Account,
        *,
        force_native: bool = False,
    ) -> BackendClientLease:
        if force_native or self._mode == "native":
            return await self._native.acquire_client(account)

        if self._mode == "auto":
            try:
                return await self._primary.acquire_client(account)
            except Exception as exc:
                logger.warning(
                    "Primary backend failed for %s, falling back to native: %s",
                    account.phone,
                    exc,
                )
                return await self._native.acquire_client(account)

        if self._mode == "telethon_cli":
            return await self._primary.acquire_client(account)

        raise ValueError(f"Unknown backend mode: {self._mode!r}")

    async def release(self, lease: BackendClientLease) -> None:
        if lease.backend_name == "direct":
            return  # persistent session, nothing to release
        backend = self._native if lease.backend_name == self._native.name else self._primary
        await backend.release(lease)
