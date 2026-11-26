"""Shared Telegram business actions used by CLI, web, agent tools, and pipelines."""
from __future__ import annotations

import asyncio
import inspect
import logging
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import parse_qs, urlparse

from telethon_floodgate import (
    HandledFloodWaitError,
    TelegramRpcDeferredError,
    run_with_flood_wait,
)

from tg_agent.database import Database
from tg_agent.models import MessagesResult
from tg_agent.services.sent_registry import record_send
from tg_agent.telegram.client_pool import ClientPool
from tg_agent.telegram.dm_history import read_dialog_history
from tg_agent.telegram.outgoing_text import parse_outgoing_text, unparse_outgoing_html
from tg_agent.telegram.transcription import (
    ENGINES,
    engine_config_error,
    is_transcribable,
    marked_chat_id,
    transcribe_cached,
)
from tg_agent.utils.introspection import explicit_pool_method
from tg_agent.utils.sanitize import sanitize_user_content

logger = logging.getLogger(__name__)


def _ensure_resolved_dir(path: str | Path) -> Path:
    output_path = Path(path)
    output_path.mkdir(parents=True, exist_ok=True)
    return output_path.resolve()


def _resolve_path(path: str) -> Path:
    return Path(path).resolve()

# Broadcast-stats per-post metric attributes, shared so the CLI and the agent
# tool always render the same set of fields (parity — issue #567).
BROADCAST_STAT_FIELDS: tuple[str, ...] = (
    "followers",
    "views_per_post",
    "shares_per_post",
    "reactions_per_post",
    "forwards_per_post",
)


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
class DraftListResult:
    phone: str
    drafts: list[dict[str, Any]]


@dataclass(frozen=True)
class CountActionResult:
    phone: str
    count: int


@dataclass(frozen=True)
class ForwardMessagesResult:
    phone: str
    count: int
    message_ids: tuple[int, ...]


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
class JoinDialogResult:
    phone: str
    target: str
    via_invite: bool


@dataclass(frozen=True)
class DownloadMediaResult:
    phone: str
    path: str


@dataclass(frozen=True)
class MediaDownloadOutcome:
    """Result of a size-aware media download (issue #834).

    ``skipped=True`` means the file was left undownloaded (over the size
    threshold); ``path`` is then None and ``reason``/``size_bytes`` explain it.
    ``rel_path`` is relative to the export root so JSON/HTML can link it.
    """

    phone: str
    kind: str
    subdir: str
    path: str | None = None
    rel_path: str | None = None
    size_bytes: int | None = None
    skipped: bool = False
    reason: str | None = None


# media kind → Telegram Desktop subdirectory name. Round-video "video notes" go
# to video_messages/ (distinct from regular video_files/) to match TG Desktop.
_MEDIA_SUBDIRS = {
    "photo": "photos",
    "video": "video_files",
    "video_note": "video_messages",
    "voice": "voice_messages",
    "file": "files",
}


def classify_media_kind(message: Any) -> str:
    """Best-effort Telegram-Desktop media class for a message."""
    if getattr(message, "photo", None):
        return "photo"
    if getattr(message, "voice", None):
        return "voice"
    # Check video_note before video — TG Desktop files round videos separately.
    if getattr(message, "video_note", None):
        return "video_note"
    if getattr(message, "video", None):
        return "video"
    mime = getattr(getattr(message, "file", None), "mime_type", None) or ""
    if mime.startswith("image/"):
        return "photo"
    if mime.startswith("video/"):
        return "video"
    return "file"


def media_subdir(kind: str) -> str:
    return _MEDIA_SUBDIRS.get(kind, "files")


def _serialize_drafts(updates: Any) -> list[dict[str, Any]]:
    """Flatten a messages.GetAllDrafts Updates reply into plain dicts (#1854).

    Follows axisrow/telegram-mcp#364's get_drafts: a formatted draft also gets
    ``message_html`` (html.unparse of the draft's raw entities) that round-trips
    through save_draft(parse_mode="html"); ``message`` stays the plain text.
    Entity title/username come from the same reply's users/chats lists, so a
    listing costs no per-peer resolves.
    """
    from telethon import types, utils

    users = getattr(updates, "users", None) or []
    chats = getattr(updates, "chats", None) or []
    peers: dict[int, Any] = {}
    for entity in [*users, *chats]:
        try:
            peers[utils.get_peer_id(entity)] = entity
        except (ValueError, TypeError):
            continue

    drafts: list[dict[str, Any]] = []
    for update in getattr(updates, "updates", None) or []:
        if not isinstance(update, types.UpdateDraftMessage) or update.draft is None:
            continue
        draft = update.draft
        message = getattr(draft, "message", "") or ""
        entities = getattr(draft, "entities", None) or []
        if not message and not entities:
            continue
        peer_id = utils.get_peer_id(update.peer)
        entry: dict[str, Any] = {
            "peer_id": peer_id,
            "message": sanitize_user_content(message, preserve_emoji=True),
        }
        entity = peers.get(peer_id)
        if entity is not None:
            for field in ("title", "username", "first_name"):
                value = getattr(entity, field, None)
                if value:
                    entry[field] = value
        if entities:
            entry["message_html"] = sanitize_user_content(
                unparse_outgoing_html(message, entities), max_length=16384, preserve_emoji=True
            )
        date = getattr(draft, "date", None)
        if date:
            entry["date"] = date.isoformat()
        entry["no_webpage"] = bool(getattr(draft, "no_webpage", False))
        reply = getattr(draft, "reply_to", None)
        reply_id = getattr(reply, "reply_to_msg_id", None) if reply is not None else None
        if reply_id:
            entry["reply_to_msg_id"] = reply_id
        drafts.append(entry)
    return drafts


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


@dataclass(frozen=True)
class DeleteDialogsResult:
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

    def __init__(self, pool: ClientPool, db: Database | None = None):
        self._pool = pool
        # When present, every successful send/forward lands one row in the
        # sent-messages registry (#1893); None keeps bare constructions
        # (tests, ad-hoc use) legal without an account.
        self._db = db

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
        return explicit_pool_method(pool, name) is not None

    @staticmethod
    def _looks_numeric_identifier(value: Any) -> bool:
        if not isinstance(value, str):
            return isinstance(value, int)
        stripped = value.strip()
        if not stripped:
            return False
        if stripped[0] == "-":
            return stripped[1:].isdigit()
        # A leading '+' is a PHONE number, not a Telegram id (#1830 review):
        # numeric-id attempts can never resolve it, so it must reach the
        # string path where Telethon resolves contacts.
        return stripped.isdigit()

    @staticmethod
    def _strip_tg_url_noise(value: str) -> str:
        return value.strip().split("?", 1)[0].split("#", 1)[0].rstrip("/")

    @classmethod
    def _extract_invite_hash(cls, value: Any) -> str | None:
        if not isinstance(value, str):
            return None
        raw = value.strip()
        if not raw:
            return None

        if raw.startswith("tg://join"):
            parsed = urlparse(raw)
            invite = parse_qs(parsed.query).get("invite", [""])[0].strip()
            return invite or None

        cleaned = cls._strip_tg_url_noise(raw)
        parsed = urlparse(cleaned if "://" in cleaned else f"https://{cleaned}")
        if parsed.netloc.lower() in {"t.me", "www.t.me", "telegram.me", "www.telegram.me"}:
            parts = [part for part in parsed.path.split("/") if part]
            if not parts:
                return None
            if parts[0] == "joinchat" and len(parts) >= 2:
                return parts[1] or None
            if parts[0].startswith("+"):
                return parts[0].lstrip("+") or None

        if raw.startswith("+") and len(raw) > 1 and "/" not in raw:
            return raw.lstrip("+") or None
        return None

    @classmethod
    def _normalize_public_join_target(cls, value: Any) -> str:
        raw = str(value or "").strip()
        cleaned = cls._strip_tg_url_noise(raw)
        if "://" in cleaned:
            parse_value = cleaned
        elif cleaned.startswith(("t.me/", "www.t.me/", "telegram.me/", "www.telegram.me/")):
            parse_value = f"https://{cleaned}"
        else:
            parse_value = ""
        parsed = urlparse(parse_value)
        if parsed.netloc.lower() in {"t.me", "www.t.me", "telegram.me", "www.telegram.me"}:
            parts = [part for part in parsed.path.split("/") if part]
            if parts and parts[0] == "s":
                parts = parts[1:]
            if parts:
                username = parts[0].strip()
                if username and username not in {"c", "joinchat"} and not username.startswith("+"):
                    return username if username.startswith("@") else f"@{username}"
        return raw

    async def _invalidate_dialog_cache(self, phone: str) -> None:
        invalidate = getattr(self._pool, "invalidate_dialogs_cache", None)
        if callable(invalidate):
            result = invalidate(phone)
            if inspect.isawaitable(result):
                await result
        db = getattr(self._pool, "_db", None)
        repo = getattr(getattr(db, "repos", None), "dialog_cache", None)
        clear_dialogs = getattr(repo, "clear_dialogs", None)
        if callable(clear_dialogs):
            result = clear_dialogs(phone)
            if inspect.isawaitable(result):
                await result

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
                result = await self._pool.get_native_client_by_phone(phone, wait_for_flood=True)
            elif self._has_explicit_pool_operation(self._pool, "get_client_by_phone"):
                result = await self._pool.get_client_by_phone(phone, wait_for_flood=True)
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
            dialog_id = int(str(identifier).strip())
            target_types = ("dm",) if is_user else (None, "dm")
            # Pass 1 — cache-only (#1830): every candidate kind is checked
            # against the session store BEFORE any candidate may warm, so a
            # numeric user id known to the session answers with zero RPCs
            # and zero sweeps.
            for target_type in target_types:
                try:
                    entity = await self._pool.resolve_dialog_entity(
                        client,
                        phone,
                        dialog_id,
                        target_type,
                        allow_warm=False,
                    )
                except HandledFloodWaitError:
                    raise
                # A curated local refusal (#1828) is diagnostic, not a resolver
                # failure: wrap only genuine misses below. Typed resolver
                # refusals (UsernameResolveRateLimitedError, the gateway's
                # flood pre-check) keep their TelegramActionEntityResolutionError
                # wrap — their str() already carries the deadline.
                except TelegramRpcDeferredError:
                    raise
                except (ValueError, TypeError, KeyError):
                    continue
                except Exception as exc:
                    raise TelegramActionEntityResolutionError(
                        f"Ошибка: не удалось получить entity для {identifier}: {exc}"
                    ) from exc
                if entity is not None:
                    return entity
            # Pass 2 — at most ONE warm attempt, with the preferred candidate;
            # remaining candidates stay cache-only (#1830): one action never
            # pays more than one dialog sweep. A rate-limited warm refusal
            # cannot skip cache candidates — they already ran in pass 1.
            for index, target_type in enumerate(target_types):
                try:
                    entity = await self._pool.resolve_dialog_entity(
                        client,
                        phone,
                        dialog_id,
                        target_type,
                        allow_warm=index == 0,
                    )
                except HandledFloodWaitError:
                    raise
                except TelegramRpcDeferredError:
                    raise
                except (ValueError, TypeError, KeyError):
                    continue
                except Exception as exc:
                    raise TelegramActionEntityResolutionError(
                        f"Ошибка: не удалось получить entity для {identifier}: {exc}"
                    ) from exc
                if entity is not None:
                    return entity
            # Both candidates failed: the string fallback below would repeat
            # the same misses and pay another sweep for it (#1830).
            raise TelegramActionEntityResolutionError(
                f"Ошибка: не удалось получить entity для {identifier}"
            )

        try:
            if self._has_explicit_pool_operation(self._pool, "resolve_entity_with_warm"):
                # Centralized cache-warming resolve so a freshly created chat
                # (cold entity cache) resolves on retry instead of failing.
                # Writes only need an InputPeer (#1831): get_input_entity
                # answers from the session store without a request, while the
                # full get_entity pays a network resolve on every call — one
                # unit of the 60/h username budget per write.
                return await self._pool.resolve_entity_with_warm(
                    client,
                    phone,
                    identifier,
                    operation="telegram_actions_resolve",
                    use_input_entity=True,
                )
            return await client.get_input_entity(identifier)
        except HandledFloodWaitError:
            raise
        except TelegramRpcDeferredError:
            raise
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
        emoji: str | None,
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
        link_preview: bool = False,
        reply_to: int | None = None,
        is_user: bool = False,
        silent: bool = False,
        comment_to: int | None = None,
        parse_mode: str | None = None,
        source: str = "unknown",
        ref: str | None = None,
    ) -> SendMessageResult:
        # Operator channels never render web previews on links (#1584-adjacent
        # digest flow: a bare URL silently produced one); opt in explicitly.
        # reply_to (seeding publisher #1757): message id of the replica this
        # one answers; omitted for a top-level message so existing callers'
        # RPC shape is unchanged. is_user (#1830): callers addressing a DM
        # peer by numeric id must say so — otherwise the resolver tries a
        # channel first and pays a dialog sweep for it. silent (upstream #395
        # port, #1856): send without a notification sound. comment_to
        # (#1903): the recipient is a channel and the text becomes a comment
        # under its post — Telethon resolves the linked discussion group
        # (GetDiscussionMessageRequest) and replies to the top message.
        # parse_mode (#1937): omitted, the client's default Markdown parses
        # the text; given, the text is parsed here and the client is told not
        # to — `plain` sends it verbatim. Parsed before any Telegram request:
        # an unknown mode raises here and costs no resolve.
        entities: list | None = None
        if parse_mode is not None:
            text, entities = parse_outgoing_text(text, parse_mode)
        async with self._client(phone=phone, native=True) as (client, acquired_phone):
            entity = await self._resolve_entity(
                client, phone=acquired_phone, identifier=recipient, is_user=is_user
            )
            options: dict[str, Any] = {"link_preview": link_preview}
            if reply_to is not None:
                options["reply_to"] = reply_to
            if silent:
                options["silent"] = True
            if comment_to is not None:
                options["comment_to"] = comment_to
            if parse_mode is not None:
                # An empty list is "provided": Telethon skips its own parsing.
                options["formatting_entities"] = entities or []
            message = await client.send_message(entity, text, **options)
            message_id = getattr(message, "id", None)
            # A comment physically lands in the linked discussion group, so the
            # registry row must carry the group's chat: its id space owns the
            # message id and the deletion joins match on it (#1903 review).
            # recipient/text keep the channel context the caller asked for.
            registry_entity = (
                message.peer_id
                if comment_to is not None and getattr(message, "peer_id", None) is not None
                else entity
            )
            await record_send(
                self._db,
                phone=acquired_phone,
                entity=registry_entity,
                recipient=recipient,
                message_ids=[message_id] if message_id is not None else [],
                text=text,
                source=source,
                ref=ref,
            )
            return SendMessageResult(phone=acquired_phone, message_id=message_id)

    async def edit_message(
        self,
        *,
        phone: str,
        chat_id: Any,
        message_id: int,
        text: str,
        link_preview: bool = False,
    ) -> TelegramActionResult:
        async with self._client(phone=phone, native=True) as (client, acquired_phone):
            entity = await self._resolve_entity(client, phone=acquired_phone, identifier=chat_id)
            await client.edit_message(entity, int(message_id), text, link_preview=link_preview)
            return TelegramActionResult(phone=acquired_phone)

    async def edit_message_media(
        self,
        *,
        phone: str,
        chat_id: Any,
        message_id: int,
        file: str,
        text: str | None = None,
        parse_mode: str | None = None,
    ) -> TelegramActionResult:
        # Ported from axisrow/telegram-mcp#398 (edit_message_media, merged
        # 2026-11-10): replace the media attachment of a sent message without
        # reposting. An omitted ``text`` keeps the current caption and its
        # formatting entities (a read-then-edit — a concurrent caption change
        # from another client can be overwritten; pass ``text`` explicitly to
        # pin its value); an empty ``text`` clears the caption. Upstream's MP4
        # video-attribute parser is deliberately not ported yet. See
        # THIRD_PARTY_NOTICES.md.
        def _resolve_media_file(raw: str) -> Path:
            return Path(raw).expanduser().resolve()

        resolved_path = await asyncio.to_thread(_resolve_media_file, file)
        if not resolved_path.is_file():
            raise FileNotFoundError(f"Файл для замены вложения не найден: {file}")
        mode = (parse_mode or "").strip().lower() or None
        if mode is not None and mode not in {"plain", "md", "markdown", "html"}:
            raise ValueError(
                f"parse_mode='{parse_mode}' не поддерживается. Используйте 'md', 'html' или 'plain'."
            )
        async with self._client(phone=phone, native=True) as (client, acquired_phone):
            entity = await self._resolve_entity(client, phone=acquired_phone, identifier=chat_id)
            kwargs: dict[str, Any] = {"file": str(resolved_path)}
            if text is None:
                # Caption preservation needs the current message's text +
                # entities; an explicit text (including "") skips the read.
                current = await self._fetch_message_with_flood_wait(
                    client,
                    acquired_phone,
                    entity,
                    int(message_id),
                    operation_prefix="telegram_action_edit_message_media",
                )
                new_text = getattr(current, "message", "") or ""
                kwargs["parse_mode"] = None
                kwargs["formatting_entities"] = list(getattr(current, "entities", None) or [])
            else:
                new_text = text
                if mode == "plain":
                    kwargs["parse_mode"] = None
                elif mode is not None:
                    kwargs["parse_mode"] = mode
                # mode None (not given) passes nothing: Telethon keeps its
                # default (Markdown) parsing, same as upstream captions.
            await client.edit_message(entity, int(message_id), new_text, **kwargs)
            return TelegramActionResult(phone=acquired_phone)

    async def save_draft(
        self,
        *,
        phone: str,
        chat_id: Any,
        text: str,
        parse_mode: str | None = None,
        reply_to: int | None = None,
        no_webpage: bool = False,
    ) -> TelegramActionResult:
        # Ported from axisrow/telegram-mcp#364 (save_draft parse_mode): the
        # markup pre-parses into real Telegram entities so a draft keeps its
        # formatting in the chat's input box; a bad parse_mode raises before
        # any client work, never producing a request. See
        # THIRD_PARTY_NOTICES.md.
        parsed, entities = parse_outgoing_text(text, parse_mode)
        async with self._client(phone=phone, native=True) as (client, acquired_phone):
            entity = await self._resolve_entity(client, phone=acquired_phone, identifier=chat_id)
            await client.save_draft(
                entity, parsed, entities=entities, reply_to=reply_to, no_webpage=no_webpage
            )
            return TelegramActionResult(phone=acquired_phone)

    async def list_drafts(self, *, phone: str) -> DraftListResult:
        async with self._client(phone=phone, native=True) as (client, acquired_phone):
            updates = await client.get_all_drafts()
            return DraftListResult(phone=acquired_phone, drafts=_serialize_drafts(updates))

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
        source: str = "unknown",
        ref: str | None = None,
    ) -> ForwardMessagesResult:
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
            forwarded = await client.forward_messages(to_entity, messages, from_entity)
            if not isinstance(forwarded, (list, tuple)):
                forwarded = [forwarded] if forwarded is not None else []
            forwarded_ids = tuple(int(m.id) for m in forwarded if m is not None and hasattr(m, "id"))
            await record_send(
                self._db,
                phone=acquired_phone,
                entity=to_entity,
                recipient=to_chat,
                message_ids=list(forwarded_ids),
                text=None,
                source=source,
                ref=ref,
            )
            return ForwardMessagesResult(phone=acquired_phone, count=len(ids), message_ids=forwarded_ids)

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

    async def read_history(
        self,
        *,
        phone: str,
        chat_id: Any,
        limit: int = 50,
        offset_id: int = 0,
    ) -> MessagesResult:
        """Read live dialog history via tg_messenger, reusing the pool's own client.

        See `tg_agent/telegram/dm_history.py` for why this never opens a second
        Telethon connection on the same session.
        """
        async with self._client(phone=phone, native=True) as (client, acquired_phone):
            entity = await self._resolve_entity(client, phone=acquired_phone, identifier=chat_id)
            peer = await client.get_peer_id(entity)
            # ClientPool.__init__ always sets _auth, and api_id/api_hash are
            # public properties on it — a getattr chain with 0/"" fallbacks would
            # only defer the failure into tg_messenger's credential validation.
            auth = self._pool._auth
            api_id = auth.api_id
            api_hash = auth.api_hash
            messages = await read_dialog_history(
                client,
                api_id=api_id,
                api_hash=api_hash,
                peer=peer,
                limit=limit,
                offset_id=offset_id,
            )
            return MessagesResult(phone=acquired_phone, dialog_id=peer, messages=messages)

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

    async def _fetch_message_with_flood_wait(
        self,
        client,
        acquired_phone: str,
        entity,
        message_id: int,
        *,
        operation_prefix: str,
    ):
        """Resolve a single message by id under ``run_with_flood_wait``, or raise.

        Shared body of :meth:`download_media` / :meth:`download_media_sized`:
        fetch the one message via ``iter_messages(ids=...)`` under the flood-wait
        wrapper, then raise :class:`TelegramActionMessageNotFoundError` when the
        id no longer exists. Returns the resolved message object.
        """
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
        return message

    async def download_media(
        self,
        *,
        phone: str,
        chat_id: Any,
        message_id: int,
        output_dir: str | Path,
        operation_prefix: str = "telegram_action_download_media",
    ) -> DownloadMediaResult:
        output_resolved = await asyncio.to_thread(_ensure_resolved_dir, output_dir)
        async with self._client(phone=phone, native=True) as (client, acquired_phone):
            entity = await self._resolve_entity(client, phone=acquired_phone, identifier=chat_id)
            message = await self._fetch_message_with_flood_wait(
                client, acquired_phone, entity, message_id, operation_prefix=operation_prefix
            )
            path = await run_with_flood_wait(
                client.download_media(message, file=str(output_resolved)),
                operation=operation_prefix,
                phone=acquired_phone,
                pool=self._pool,
            )
            if not path:
                raise TelegramActionNoMediaError("no_media")
            resolved = await asyncio.to_thread(_resolve_path, path)
            if resolved != output_resolved and output_resolved not in resolved.parents:
                raise TelegramActionPathEscapeError("path_escape")
            return DownloadMediaResult(phone=acquired_phone, path=str(resolved))

    async def download_media_sized(
        self,
        *,
        phone: str,
        chat_id: Any,
        message_id: int,
        output_dir: str | Path,
        max_size_bytes: int | None = None,
        operation_prefix: str = "telegram_action_download_media_sized",
    ) -> MediaDownloadOutcome:
        """Download a message's media into a typed subdir, skipping oversized files.

        Files larger than ``max_size_bytes`` (when set) are NOT downloaded; the
        returned outcome records the reason and original size so the export can
        show a Telegram-style "(File exceeds maximum size…)" placeholder (#834).
        """
        root_resolved = await asyncio.to_thread(_ensure_resolved_dir, output_dir)
        async with self._client(phone=phone, native=True) as (client, acquired_phone):
            entity = await self._resolve_entity(client, phone=acquired_phone, identifier=chat_id)
            message = await self._fetch_message_with_flood_wait(
                client, acquired_phone, entity, message_id, operation_prefix=operation_prefix
            )
            if getattr(message, "media", None) is None:
                raise TelegramActionNoMediaError("no_media")

            kind = classify_media_kind(message)
            subdir = media_subdir(kind)
            size_bytes = getattr(getattr(message, "file", None), "size", None)

            # When a limit is set, skip files we can't size-check (Telegram omits
            # size for some documents/older media) rather than downloading them
            # unconditionally and risking disk exhaustion (Claude review on #938).
            if max_size_bytes is not None and size_bytes is None:
                return MediaDownloadOutcome(
                    phone=acquired_phone,
                    kind=kind,
                    subdir=subdir,
                    size_bytes=None,
                    skipped=True,
                    reason="size_unknown",
                )
            if max_size_bytes is not None and size_bytes is not None and size_bytes > max_size_bytes:
                return MediaDownloadOutcome(
                    phone=acquired_phone,
                    kind=kind,
                    subdir=subdir,
                    size_bytes=size_bytes,
                    skipped=True,
                    reason="exceeds_max_size",
                )

            dest = root_resolved / subdir
            dest.mkdir(parents=True, exist_ok=True)
            path = await run_with_flood_wait(
                client.download_media(message, file=str(dest)),
                operation=operation_prefix,
                phone=acquired_phone,
                pool=self._pool,
            )
            if not path:
                raise TelegramActionNoMediaError("no_media")
            resolved = await asyncio.to_thread(_resolve_path, path)
            if resolved != root_resolved and root_resolved not in resolved.parents:
                raise TelegramActionPathEscapeError("path_escape")
            return MediaDownloadOutcome(
                phone=acquired_phone,
                kind=kind,
                subdir=subdir,
                path=str(resolved),
                rel_path=str(resolved.relative_to(root_resolved)),
                size_bytes=size_bytes,
            )

    async def transcribe_voice(
        self,
        *,
        phone: str | None,
        chat_id: Any,
        message_id: int,
        engine: str,
        db: Database,
    ) -> dict:
        """Transcribe a voice note / video note (#1545).

        Thin orchestration around :mod:`tg_agent.telegram.transcription`: resolve the
        chat + message, guard the media type, then run the cache-first engine
        call (cache lives in the project ``transcripts`` table). ``engine=
        'telegram'`` needs a Premium account, so without an explicit phone the
        existing premium lease picks one. Misconfigured engines fail here,
        before any download. Returns the engine status dict.
        """
        if engine not in ENGINES:
            return {
                "status": "error",
                "error": f"Unknown engine '{engine}'. Expected one of: {', '.join(sorted(ENGINES))}.",
            }
        misconfig = engine_config_error(engine)
        if misconfig:
            return {"status": "error", "error": misconfig}

        resolved_phone = phone
        if engine == "telegram" and not resolved_phone:
            # Premium-lease pick: native transcription requires Premium.
            result = await self._pool.get_premium_client()
            if result is None:
                return {
                    "status": "premium_required",
                    "error": await self._pool.get_premium_unavailability_reason(),
                }
            _, resolved_phone = result
            # The phone is all we needed from the lease; the transcription itself
            # re-acquires the client through the standard ``_client`` path below.
            await self._pool.release_client(resolved_phone)

        async with self._client(phone=resolved_phone, native=True) as (client, acquired_phone):
            entity = await self._resolve_entity(client, phone=acquired_phone, identifier=chat_id)
            message = await self._fetch_message_with_flood_wait(
                client,
                acquired_phone,
                entity,
                message_id,
                operation_prefix="telegram_action_transcribe",
            )
            if not is_transcribable(message):
                return {
                    "status": "error",
                    "error": "В сообщении нет голосового или видеокружка для транскрипции.",
                }
            # Under the project-wide flood-wait wrapper: a FloodWaitError from the
            # native transcribe RPC (or the download) is reported to the pool's
            # flood tracking and re-raised as HandledFloodWaitError for the
            # caller, instead of drowning in a generic status-error dict.
            return await run_with_flood_wait(
                transcribe_cached(client, entity, message, engine, db, marked_chat_id(entity)),
                operation="telegram_action_transcribe",
                phone=acquired_phone,
                pool=self._pool,
            )

    async def leave_dialogs(
        self,
        *,
        phone: str,
        dialogs: list[tuple[int, str]],
    ) -> LeaveDialogsResult:
        leave_channels = getattr(self._pool, "leave_channels", None)
        if leave_channels is None:
            raise TelegramActionClientUnavailableError(
                "client pool does not implement leave_channels"
            )
        results = leave_channels(phone, dialogs)
        if inspect.isawaitable(results):
            results = await results
        return LeaveDialogsResult(phone=phone, results=dict(results))

    async def delete_dialogs(
        self,
        *,
        phone: str,
        dialogs: list[tuple[int, str]],
    ) -> DeleteDialogsResult:
        delete_dialogs = getattr(self._pool, "delete_dialogs", None)
        if delete_dialogs is None:
            raise TelegramActionClientUnavailableError(
                "client pool does not implement delete_dialogs"
            )
        results = delete_dialogs(phone, dialogs)
        if inspect.isawaitable(results):
            results = await results
        return DeleteDialogsResult(phone=phone, results=dict(results))

    async def create_channel(
        self,
        *,
        phone: str,
        title: str,
        about: str = "",
        username: str = "",
        broadcast: bool = True,
        megagroup: bool = False,
    ) -> CreateChannelResult:
        async with self._client(phone=phone, native=True) as (client, acquired_phone):
            requested_username = (username or "").strip()
            slots_reserved = False
            reserve_slots = inspect.getattr_static(client, "reserve_rate_limit_slots", None)
            if requested_username and callable(reserve_slots):
                # Creating a public channel is one logical action backed by two
                # irreversible Telegram writes. Reserve both before creating so
                # a rate-limit boundary cannot leave an unintended private channel.
                client.reserve_rate_limit_slots("telegram_create_channel", slots=2)
                slots_reserved = True

            create_channel = self._require_explicit_operation(client, "create_channel")
            create_kwargs: dict[str, Any] = {
                "title": title,
                "about": about or "",
                "broadcast": broadcast,
                "megagroup": megagroup,
            }
            if slots_reserved:
                create_kwargs["_rate_limit_reserved"] = True
            result = await create_channel(
                **create_kwargs,
            )
            channel = result.chats[0] if getattr(result, "chats", None) else None
            if channel is None:
                raise RuntimeError("Telegram returned empty response")
            channel_id = getattr(channel, "id", None)
            channel_username = getattr(channel, "username", None) or ""
            username_error: str | None = None
            if requested_username and channel_id:
                try:
                    update_channel_username = self._require_explicit_operation(client, "update_channel_username")
                    update_kwargs = {"_rate_limit_reserved": True} if slots_reserved else {}
                    await update_channel_username(channel, requested_username, **update_kwargs)
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

    async def join_dialog(
        self,
        *,
        phone: str,
        target: Any,
    ) -> JoinDialogResult:
        invite_hash = self._extract_invite_hash(target)
        async with self._client(phone=phone, native=True) as (client, acquired_phone):
            if invite_hash:
                import_chat_invite = self._require_explicit_operation(client, "import_chat_invite")
                await import_chat_invite(invite_hash)
                await self._invalidate_dialog_cache(acquired_phone)
                return JoinDialogResult(phone=acquired_phone, target=str(target), via_invite=True)

            public_target = self._normalize_public_join_target(target)
            entity = await self._resolve_entity(client, phone=acquired_phone, identifier=public_target)
            join_channel = self._require_explicit_operation(client, "join_channel")
            await join_channel(entity)
            await self._invalidate_dialog_cache(acquired_phone)
            return JoinDialogResult(phone=acquired_phone, target=public_target, via_invite=False)
