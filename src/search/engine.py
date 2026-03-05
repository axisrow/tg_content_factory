from __future__ import annotations

import logging
from datetime import timezone

from src.database import Database
from src.models import Channel, Message, SearchResult
from src.telegram.client_pool import ClientPool
from src.telegram.collector import Collector

try:
    from telethon.tl.types import PeerChannel
except ImportError:  # pragma: no cover
    PeerChannel = None  # type: ignore[assignment,misc]

logger = logging.getLogger(__name__)


class SearchEngine:
    def __init__(self, db: Database, pool: ClientPool | None = None):
        self._db = db
        self._pool = pool

    async def search_local(
        self,
        query: str,
        channel_id: int | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
        limit: int = 50,
        offset: int = 0,
    ) -> SearchResult:
        """Search messages in local database."""
        messages, total = await self._db.search_messages(
            query=query,
            channel_id=channel_id,
            date_from=date_from,
            date_to=date_to,
            limit=limit,
            offset=offset,
        )
        return SearchResult(messages=messages, total=total, query=query)

    async def check_search_quota(self, query: str = "") -> dict | None:
        """Call checkSearchPostsFlood and return quota info, or None if unavailable."""
        if not self._pool:
            return None

        result = await self._pool.get_available_client()
        if result is None:
            return None

        client, _phone = result

        try:
            from telethon.tl.functions.channels import CheckSearchPostsFloodRequest

            r = await client(CheckSearchPostsFloodRequest(query=query))
            return {
                "total_daily": getattr(r, "total_daily", None),
                "remains": getattr(r, "remains", None),
                "wait_till": getattr(r, "wait_till", None),
                "query_is_free": getattr(r, "query_is_free", False),
                "stars_amount": getattr(r, "stars_amount", None),
            }
        except Exception as exc:
            logger.debug("checkSearchPostsFlood unavailable: %s", exc)
            return None

    async def search_telegram(
        self,
        query: str,
        limit: int = 50,
    ) -> SearchResult:
        """Global search across all public Telegram channels (requires Premium)."""
        if not self._pool:
            return SearchResult(
                messages=[], total=0, query=query,
                error="Нет подключённых Telegram-аккаунтов.",
            )

        result = await self._pool.get_available_client()
        if result is None:
            return SearchResult(
                messages=[], total=0, query=query,
                error="Нет доступных Telegram-аккаунтов. Проверьте подключение.",
            )

        client, _phone = result

        # Premium check — channels.searchPosts requires it
        me = await client.get_me()
        if not getattr(me, "premium", False):
            return SearchResult(
                messages=[], total=0, query=query,
                error=(
                    "Глобальный поиск по публичным каналам требует Telegram Premium. "
                    f"Аккаунт {_phone} не имеет подписки Premium."
                ),
            )

        # Quota check
        quota = await self.check_search_quota(query)
        if quota and quota.get("remains") == 0 and not quota.get("query_is_free"):
            return SearchResult(
                messages=[], total=0, query=query,
                error=(
                    "Лимит Premium-поиска исчерпан на сегодня. "
                    "Попробуйте позже или используйте другой режим поиска."
                ),
            )

        messages, seen_channels = await self._search_posts_global(
            client, query, limit
        )

        for ch in seen_channels.values():
            await self._db.add_channel(ch)

        if messages:
            await self._db.insert_messages_batch(messages)

        await self._db.log_search(_phone, query, len(messages))

        return SearchResult(messages=messages, total=len(messages), query=query)

    async def _search_posts_global(
        self,
        client,
        query: str,
        limit: int,
    ) -> tuple[list[Message], dict[int, Channel]]:
        """Search via channels.searchPosts (all public channels, requires Premium)."""
        from telethon.tl.functions.channels import SearchPostsRequest
        from telethon.tl.types import InputPeerEmpty, PeerChannel
        from telethon.utils import get_input_peer

        messages: list[Message] = []
        seen_channels: dict[int, Channel] = {}

        offset_rate = 0
        offset_peer = InputPeerEmpty()
        offset_id = 0

        while len(messages) < limit:
            batch_limit = min(limit - len(messages), 100)
            r = await client(SearchPostsRequest(
                query=query,
                offset_rate=offset_rate,
                offset_peer=offset_peer,
                offset_id=offset_id,
                limit=batch_limit,
            ))

            if not r.messages:
                break

            chats_map = {c.id: c for c in getattr(r, "chats", [])}
            users_map = {u.id: u for u in getattr(r, "users", [])}

            for msg in r.messages:
                if not isinstance(getattr(msg, "peer_id", None), PeerChannel):
                    continue
                chat_id = msg.peer_id.channel_id

                chat = chats_map.get(chat_id)
                chat_title = getattr(chat, "title", None) if chat else None
                chat_username = getattr(chat, "username", None) if chat else None

                if chat_id not in seen_channels:
                    seen_channels[chat_id] = Channel(
                        channel_id=chat_id,
                        title=chat_title,
                        username=chat_username,
                    )

                sender_id, sender_name = self._resolve_sender(
                    msg, chats_map, users_map
                )

                messages.append(
                    Message(
                        channel_id=chat_id,
                        message_id=msg.id,
                        sender_id=sender_id,
                        sender_name=sender_name,
                        text=getattr(msg, "message", None),
                        media_type=Collector._get_media_type(msg),
                        date=msg.date.replace(tzinfo=timezone.utc)
                        if msg.date and msg.date.tzinfo is None
                        else msg.date,
                        channel_title=chat_title,
                        channel_username=chat_username,
                    )
                )

            # Pagination
            next_rate = getattr(r, "next_rate", None)
            if next_rate and len(r.messages) == batch_limit:
                offset_rate = next_rate
                last_msg = r.messages[-1]
                offset_id = last_msg.id
                if isinstance(last_msg.peer_id, PeerChannel):
                    last_chat = chats_map.get(last_msg.peer_id.channel_id)
                    if last_chat:
                        offset_peer = get_input_peer(last_chat)
                    else:
                        break
                else:
                    break
            else:
                break

        return messages, seen_channels

    # ------------------------------------------------------------------
    # New search modes
    # ------------------------------------------------------------------

    def _convert_telethon_message(self, msg) -> Message | None:
        """Convert a resolved Telethon message (from iter_messages) to Message model."""
        chat = getattr(msg, "chat", None)
        if chat is None:
            return None

        chat_id = getattr(chat, "id", 0)
        chat_title = getattr(chat, "title", None)
        chat_username = getattr(chat, "username", None)

        sender = getattr(msg, "sender", None)
        sender_id = getattr(sender, "id", None) if sender else None
        sender_name = None
        if sender:
            first = getattr(sender, "first_name", "") or ""
            last = getattr(sender, "last_name", "") or ""
            title = getattr(sender, "title", "") or ""
            sender_name = (
                " ".join(p for p in (first, last) if p) or title or None
            )

        date = msg.date
        if date and date.tzinfo is None:
            date = date.replace(tzinfo=timezone.utc)

        return Message(
            channel_id=chat_id,
            message_id=msg.id,
            sender_id=sender_id,
            sender_name=sender_name,
            text=getattr(msg, "message", None) or getattr(msg, "text", None),
            media_type=Collector._get_media_type(msg),
            date=date,
            channel_title=chat_title,
            channel_username=chat_username,
        )

    async def search_my_chats(
        self,
        query: str,
        limit: int = 50,
    ) -> SearchResult:
        """Search across all user's chats via messages.searchGlobal (no Premium)."""
        if not self._pool:
            return SearchResult(
                messages=[], total=0, query=query,
                error="Нет подключённых Telegram-аккаунтов.",
            )

        result = await self._pool.get_available_client()
        if result is None:
            return SearchResult(
                messages=[], total=0, query=query,
                error="Нет доступных Telegram-аккаунтов. Проверьте подключение.",
            )

        client, _phone = result

        # Pre-fetch entity cache (needed for StringSession clients)
        await client.get_dialogs()

        messages: list[Message] = []
        seen_channels: dict[int, Channel] = {}

        async for msg in client.iter_messages(None, search=query, limit=limit):
            converted = self._convert_telethon_message(msg)
            if converted is None:
                continue
            messages.append(converted)
            if converted.channel_id not in seen_channels:
                seen_channels[converted.channel_id] = Channel(
                    channel_id=converted.channel_id,
                    title=converted.channel_title,
                    username=converted.channel_username,
                )

        for ch in seen_channels.values():
            await self._db.add_channel(ch)
        if messages:
            await self._db.insert_messages_batch(messages)

        return SearchResult(messages=messages, total=len(messages), query=query)

    async def search_in_channel(
        self,
        channel_id: int | None,
        query: str,
        limit: int = 50,
    ) -> SearchResult:
        """Search within a specific channel or all channels (no Premium).

        If channel_id is None — searches across all user's channels/chats
        (equivalent to search_my_chats). If channel_id is set — searches
        within that specific channel via messages.search.
        """
        if not self._pool:
            return SearchResult(
                messages=[], total=0, query=query,
                error="Нет подключённых Telegram-аккаунтов.",
            )

        result = await self._pool.get_available_client()
        if result is None:
            return SearchResult(
                messages=[], total=0, query=query,
                error="Нет доступных Telegram-аккаунтов. Проверьте подключение.",
            )

        client, _phone = result

        # Resolve entity: specific channel or None (all chats)
        entity = None
        if channel_id:
            try:
                entity = await client.get_entity(PeerChannel(channel_id))
            except Exception as exc:
                logger.warning("Cannot resolve channel %s: %s", channel_id, exc)
                return SearchResult(
                    messages=[], total=0, query=query,
                    error=f"Не удалось найти канал {channel_id}: {exc}",
                )
        else:
            # Pre-fetch entity cache for global search across own chats
            await client.get_dialogs()

        messages: list[Message] = []
        seen_channels: dict[int, Channel] = {}

        async for msg in client.iter_messages(entity, search=query, limit=limit):
            converted = self._convert_telethon_message(msg)
            if converted is None:
                continue
            messages.append(converted)
            if converted.channel_id not in seen_channels:
                seen_channels[converted.channel_id] = Channel(
                    channel_id=converted.channel_id,
                    title=converted.channel_title,
                    username=converted.channel_username,
                )

        for ch in seen_channels.values():
            await self._db.add_channel(ch)
        if messages:
            await self._db.insert_messages_batch(messages)

        return SearchResult(messages=messages, total=len(messages), query=query)

    @staticmethod
    def _resolve_sender(msg, chats_map, users_map) -> tuple[int | None, str | None]:
        """Extract sender_id and sender_name from raw API message."""
        from telethon.tl.types import PeerChannel, PeerUser

        sender_id = None
        sender_name = None
        from_id = getattr(msg, "from_id", None)

        if isinstance(from_id, PeerUser):
            sender_id = from_id.user_id
            user = users_map.get(sender_id)
            if user:
                parts = [
                    getattr(user, "first_name", "") or "",
                    getattr(user, "last_name", "") or "",
                ]
                sender_name = " ".join(p for p in parts if p) or None
        elif isinstance(from_id, PeerChannel):
            sender_id = from_id.channel_id
            ch = chats_map.get(sender_id)
            sender_name = getattr(ch, "title", None) if ch else None

        return sender_id, sender_name

