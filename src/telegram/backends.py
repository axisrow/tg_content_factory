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
        client.flood_sleep_threshold = 60
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
            client.flood_sleep_threshold = 60
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
