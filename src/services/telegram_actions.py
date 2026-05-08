"""Shared Telegram business actions used by CLI, web, agent tools, and pipelines."""
from __future__ import annotations

import inspect
import logging
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from src.telegram.client_pool import ClientPool
from src.telegram.flood_wait import run_with_flood_wait

logger = logging.getLogger(__name__)


class TelegramActionError(RuntimeError):
    """Base exception for shared Telegram action failures."""


class TelegramActionClientUnavailableError(TelegramActionError):
    """Raised when no usable Telegram client can be acquired."""


class TelegramActionEntityResolutionError(TelegramActionError):
    """Raised when a chat/user identifier cannot be resolved for an action."""


class TelegramActionMessageNotFoundError(TelegramActionError):
    """Raised when a requested Telegram message cannot be found."""


class TelegramActionNoMediaError(TelegramActionError):
    """Raised when a requested Telegram message has no downloadable media."""


class TelegramActionPathEscapeError(TelegramActionError):
    """Raised when a media download returns a path outside the requested directory."""


@dataclass(frozen=True)
class TelegramActionResult:
    phone: str


@dataclass(frozen=True)
class SendMessageResult:
    phone: str
    message_id: int | None


@dataclass(frozen=True)
class CountActionResult:
    phone: str
    count: int


@dataclass(frozen=True)
class ParticipantsResult:
    phone: str
    participants: list[Any]


@dataclass(frozen=True)
class BroadcastStatsResult:
    phone: str
    stats: Any


@dataclass(frozen=True)
class CreateChannelResult:
    phone: str
    channel_id: int | None
    channel_title: str
    channel_username: str
    invite_link: str
    username_error: str | None = None


@dataclass(frozen=True)
class DownloadMediaResult:
    phone: str
    path: str


@dataclass(frozen=True)
class LeaveDialogsResult:
    phone: str
    results: dict[Any, bool]

    @property
    def success_count(self) -> int:
        return sum(1 for value in self.results.values() if value)

    @property
    def failed_count(self) -> int:
        return len(self.results) - self.success_count


class TelegramActionService:
    """Typed facade for Telegram-side business actions.

    Interface layers should adapt inputs/outputs and delegate Telegram execution
    here instead of directly assembling raw Telethon requests.
    """

    def __init__(self, pool: ClientPool):
        self._pool = pool

    @staticmethod
    def _require_explicit_operation(client: Any, name: str) -> Any:
        operation = getattr(client, name, None)
        if operation is None:
            raise AttributeError(f"client does not implement {name}")
        if not hasattr(type(client), name) and name not in getattr(client, "__dict__", {}):
            raise AttributeError(f"client does not implement {name}")
        return operation

    @staticmethod
    def _has_explicit_pool_operation(pool: Any, name: str) -> bool:
        return hasattr(type(pool), name) or name in getattr(pool, "__dict__", {})

    @staticmethod
    def _looks_numeric_identifier(value: Any) -> bool:
        if not isinstance(value, str):
            return isinstance(value, int)
        stripped = value.strip()
        if not stripped:
            return False
        if stripped[0] in ("+", "-"):
            return stripped[1:].isdigit()
        return stripped.isdigit()

    @asynccontextmanager
    async def _client(
        self,
        *,
        phone: str | None,
        native: bool,
        allow_any: bool = False,
    ):
        acquired_phone: str | None = None
        if phone:
            if native and self._has_explicit_pool_operation(self._pool, "get_native_client_by_phone"):
                result = await self._pool.get_native_client_by_phone(phone)
            elif self._has_explicit_pool_operation(self._pool, "get_client_by_phone"):
                result = await self._pool.get_client_by_phone(phone)
            else:
                result = None
        elif allow_any and self._has_explicit_pool_operation(self._pool, "get_available_client"):
            result = await self._pool.get_available_client()
        else:
            result = None
        if result is None:
            raise TelegramActionClientUnavailableError("client unavailable")
        session, acquired_phone = result
        try:
            yield session, acquired_phone
        finally:
            if acquired_phone is not None:
                release = getattr(self._pool, "release_client", None)
                if release is not None:
                    result = release(acquired_phone)
                    if inspect.isawaitable(result):
                        await result

    async def _resolve_entity(
        self,
        client: Any,
        *,
        phone: str,
        identifier: Any,
        is_user: bool = False,
    ) -> Any:
        """Resolve an action entity through one shared path.

        Numeric dialog IDs use ClientPool.resolve_dialog_entity when the pool
        exposes it explicitly, so CLI/web/agent behavior can share the same
        cache-warming path. Other identifiers fall back to Telethon's get_entity.
        """
        if self._looks_numeric_identifier(identifier) and self._has_explicit_pool_operation(
            self._pool, "resolve_dialog_entity"
        ):
            get_client_by_phone = getattr(self._pool, "get_client_by_phone", None)
            if callable(get_client_by_phone):
                session_result = await get_client_by_phone(phone)
                if session_result is not None:
                    session, _ = session_result
                    dialog_id = int(str(identifier).strip())
                    target_types = ("dm",) if is_user else (None, "dm")
                    for target_type in target_types:
                        try:
                            entity = await self._pool.resolve_dialog_entity(
                                session,
                                phone,
                                dialog_id,
                                target_type,
                            )
                        except (ValueError, TypeError, KeyError):
                            continue
                        except Exception as exc:
                            raise TelegramActionEntityResolutionError(
                                f"Ошибка: не удалось получить entity для {identifier}: {exc}"
                            ) from exc
                        if entity is not None:
                            return entity

        try:
            return await client.get_entity(identifier)
        except Exception as exc:
            raise TelegramActionEntityResolutionError(
                f"Ошибка: не удалось найти чат/пользователя '{identifier}': {exc}"
            ) from exc

    async def send_reaction(
        self,
        *,
        phone: str | None,
        chat_id: Any,
        message_id: int,
        emoji: str,
        native: bool = True,
        allow_any: bool = False,
        resolve_entity: bool = True,
    ) -> TelegramActionResult:
        async with self._client(phone=phone, native=native, allow_any=allow_any) as (client, acquired_phone):
            entity = (
                await self._resolve_entity(client, phone=acquired_phone, identifier=chat_id)
                if resolve_entity
                else chat_id
            )
            await client.send_reaction(entity, int(message_id), emoji)
            return TelegramActionResult(phone=acquired_phone)

    async def ensure_client(
        self,
        *,
        phone: str,
        native: bool = True,
    ) -> TelegramActionResult:
        async with self._client(phone=phone, native=native) as (_client, acquired_phone):
            return TelegramActionResult(phone=acquired_phone)

    async def send_message(
        self,
        *,
        phone: str,
        recipient: Any,
        text: str,
    ) -> SendMessageResult:
        async with self._client(phone=phone, native=True) as (client, acquired_phone):
            entity = await self._resolve_entity(client, phone=acquired_phone, identifier=recipient)
            message = await client.send_message(entity, text)
            return SendMessageResult(phone=acquired_phone, message_id=getattr(message, "id", None))

    async def edit_message(
        self,
        *,
        phone: str,
        chat_id: Any,
        message_id: int,
        text: str,
    ) -> TelegramActionResult:
        async with self._client(phone=phone, native=True) as (client, acquired_phone):
            entity = await self._resolve_entity(client, phone=acquired_phone, identifier=chat_id)
            await client.edit_message(entity, int(message_id), text)
            return TelegramActionResult(phone=acquired_phone)

    async def delete_messages(
        self,
        *,
        phone: str | None,
        chat_id: Any,
        message_ids: list[int],
        native: bool = True,
        allow_any: bool = False,
        resolve_entity: bool = True,
    ) -> CountActionResult:
        async with self._client(phone=phone, native=native, allow_any=allow_any) as (client, acquired_phone):
            entity = (
                await self._resolve_entity(client, phone=acquired_phone, identifier=chat_id)
                if resolve_entity
                else chat_id
            )
            ids = [int(value) for value in message_ids]
            await client.delete_messages(entity, ids)
            return CountActionResult(phone=acquired_phone, count=len(ids))

    async def forward_messages(
        self,
        *,
        phone: str,
        from_chat: Any,
        to_chat: Any,
        message_ids: list[int],
        native: bool = True,
        resolve_entities: bool = True,
        collapse_single_message_id: bool = False,
    ) -> CountActionResult:
        async with self._client(phone=phone, native=native) as (client, acquired_phone):
            from_entity = (
                await self._resolve_entity(client, phone=acquired_phone, identifier=from_chat)
                if resolve_entities
                else from_chat
            )
            to_entity = (
                await self._resolve_entity(client, phone=acquired_phone, identifier=to_chat)
                if resolve_entities
                else to_chat
            )
            ids = [int(value) for value in message_ids]
            messages: Any = ids[0] if collapse_single_message_id and len(ids) == 1 else ids
            await client.forward_messages(to_entity, messages, from_entity)
            return CountActionResult(phone=acquired_phone, count=len(ids))

    async def pin_message(
        self,
        *,
        phone: str,
        chat_id: Any,
        message_id: int,
        notify: bool = False,
    ) -> TelegramActionResult:
        async with self._client(phone=phone, native=True) as (client, acquired_phone):
            entity = await self._resolve_entity(client, phone=acquired_phone, identifier=chat_id)
            await client.pin_message(entity, int(message_id), notify=notify)
            return TelegramActionResult(phone=acquired_phone)

    async def unpin_message(
        self,
        *,
        phone: str,
        chat_id: Any,
        message_id: int | None = None,
    ) -> TelegramActionResult:
        async with self._client(phone=phone, native=True) as (client, acquired_phone):
            entity = await self._resolve_entity(client, phone=acquired_phone, identifier=chat_id)
            await client.unpin_message(entity, int(message_id) if message_id is not None else None)
            return TelegramActionResult(phone=acquired_phone)

    async def mark_read(
        self,
        *,
        phone: str,
        chat_id: Any,
        max_id: int | None = None,
    ) -> TelegramActionResult:
        async with self._client(phone=phone, native=True) as (client, acquired_phone):
            entity = await self._resolve_entity(client, phone=acquired_phone, identifier=chat_id)
            await client.send_read_acknowledge(entity, max_id=int(max_id) if max_id is not None else None)
            return TelegramActionResult(phone=acquired_phone)

    async def set_dialog_folder(
        self,
        *,
        phone: str,
        chat_id: Any,
        folder_id: int,
    ) -> TelegramActionResult:
        async with self._client(phone=phone, native=True) as (client, acquired_phone):
            entity = await self._resolve_entity(client, phone=acquired_phone, identifier=chat_id)
            await client.edit_folder(entity, int(folder_id))
            return TelegramActionResult(phone=acquired_phone)

    async def get_participants(
        self,
        *,
        phone: str,
        chat_id: Any,
        limit: int = 200,
        search: str = "",
    ) -> ParticipantsResult:
        async with self._client(phone=phone, native=True) as (client, acquired_phone):
            entity = await self._resolve_entity(client, phone=acquired_phone, identifier=chat_id)
            participants = await client.get_participants(entity, limit=int(limit), search=search)
            return ParticipantsResult(phone=acquired_phone, participants=list(participants))

    async def get_broadcast_stats(
        self,
        *,
        phone: str,
        chat_id: Any,
    ) -> BroadcastStatsResult:
        async with self._client(phone=phone, native=True) as (client, acquired_phone):
            entity = await self._resolve_entity(client, phone=acquired_phone, identifier=chat_id)
            stats = await client.get_broadcast_stats(entity)
            return BroadcastStatsResult(phone=acquired_phone, stats=stats)

    async def edit_admin(
        self,
        *,
        phone: str,
        chat_id: Any,
        user_id: Any,
        is_admin: bool,
        title: str | None = None,
    ) -> TelegramActionResult:
        async with self._client(phone=phone, native=True) as (client, acquired_phone):
            entity = await self._resolve_entity(client, phone=acquired_phone, identifier=chat_id)
            user = await self._resolve_entity(client, phone=acquired_phone, identifier=user_id, is_user=True)
            kwargs: dict[str, Any] = {"is_admin": bool(is_admin)}
            if title:
                kwargs["title"] = title
            await client.edit_admin(entity, user, **kwargs)
            return TelegramActionResult(phone=acquired_phone)

    async def edit_permissions(
        self,
        *,
        phone: str,
        chat_id: Any,
        user_id: Any,
        until_date: Any = None,
        send_messages: bool | None = None,
        send_media: bool | None = None,
    ) -> TelegramActionResult:
        async with self._client(phone=phone, native=True) as (client, acquired_phone):
            entity = await self._resolve_entity(client, phone=acquired_phone, identifier=chat_id)
            user = await self._resolve_entity(client, phone=acquired_phone, identifier=user_id, is_user=True)
            kwargs: dict[str, Any] = {}
            if until_date is not None:
                kwargs["until_date"] = until_date
            if send_messages is not None:
                kwargs["send_messages"] = send_messages
            if send_media is not None:
                kwargs["send_media"] = send_media
            await client.edit_permissions(entity, user, **kwargs)
            return TelegramActionResult(phone=acquired_phone)

    async def kick_participant(
        self,
        *,
        phone: str,
        chat_id: Any,
        user_id: Any,
    ) -> TelegramActionResult:
        async with self._client(phone=phone, native=True) as (client, acquired_phone):
            entity = await self._resolve_entity(client, phone=acquired_phone, identifier=chat_id)
            user = await self._resolve_entity(client, phone=acquired_phone, identifier=user_id, is_user=True)
            await client.kick_participant(entity, user)
            return TelegramActionResult(phone=acquired_phone)

    async def download_media(
        self,
        *,
        phone: str,
        chat_id: Any,
        message_id: int,
        output_dir: str | Path,
        operation_prefix: str = "telegram_action_download_media",
    ) -> DownloadMediaResult:
        output_path = Path(output_dir)
        output_path.mkdir(parents=True, exist_ok=True)
        output_resolved = output_path.resolve()
        async with self._client(phone=phone, native=True) as (client, acquired_phone):
            entity = await self._resolve_entity(client, phone=acquired_phone, identifier=chat_id)
            message = None

            async def _lookup_message() -> None:
                nonlocal message
                async for current_message in client.iter_messages(entity, ids=int(message_id)):
                    message = current_message
                    break

            await run_with_flood_wait(
                _lookup_message(),
                operation=f"{operation_prefix}_lookup",
                phone=acquired_phone,
                pool=self._pool,
            )
            if message is None:
                raise TelegramActionMessageNotFoundError("message_not_found")
            path = await run_with_flood_wait(
                client.download_media(message, file=str(output_resolved)),
                operation=operation_prefix,
                phone=acquired_phone,
                pool=self._pool,
            )
            if not path:
                raise TelegramActionNoMediaError("no_media")
            resolved = Path(path).resolve()
            if resolved != output_resolved and output_resolved not in resolved.parents:
                raise TelegramActionPathEscapeError("path_escape")
            return DownloadMediaResult(phone=acquired_phone, path=str(path))

    async def leave_dialogs(
        self,
        *,
        phone: str,
        dialogs: list[tuple[int, str]],
    ) -> LeaveDialogsResult:
        leave_channels = getattr(self._pool, "leave_channels")
        results = leave_channels(phone, dialogs)
        if inspect.isawaitable(results):
            results = await results
        return LeaveDialogsResult(phone=phone, results=dict(results))

    async def create_channel(
        self,
        *,
        phone: str,
        title: str,
        about: str = "",
        username: str = "",
    ) -> CreateChannelResult:
        async with self._client(phone=phone, native=True) as (client, acquired_phone):
            create_channel = self._require_explicit_operation(client, "create_channel")
            result = await create_channel(
                title=title,
                about=about or "",
                broadcast=True,
                megagroup=False,
            )
            channel = result.chats[0] if getattr(result, "chats", None) else None
            if channel is None:
                raise RuntimeError("Telegram returned empty response")
            channel_id = getattr(channel, "id", None)
            channel_username = getattr(channel, "username", None) or ""
            requested_username = (username or "").strip()
            username_error: str | None = None
            if requested_username and channel_id:
                try:
                    update_channel_username = self._require_explicit_operation(client, "update_channel_username")
                    await update_channel_username(channel, requested_username)
                    channel_username = requested_username
                except Exception as exc:
                    username_error = str(exc)
                    logger.warning(
                        "Could not set username %r for new channel id=%s",
                        requested_username,
                        channel_id,
                    )
            return CreateChannelResult(
                phone=acquired_phone,
                channel_id=channel_id,
                channel_title=title,
                channel_username=channel_username,
                invite_link=f"https://t.me/{channel_username}" if channel_username else "",
                username_error=username_error,
            )
