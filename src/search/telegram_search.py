from __future__ import annotations

import asyncio
import inspect
import logging
from datetime import timezone

from src.models import Channel, Message, SearchResult
from src.search.persistence import SearchPersistence
from src.search.transformers import TelegramMessageTransformer
from src.telegram.backends import adapt_transport_session
from src.telegram.client_pool import ClientPool

try:
    from telethon.tl.types import PeerChannel
except ImportError:  # pragma: no cover
    PeerChannel = None  # type: ignore[assignment,misc]

logger = logging.getLogger(__name__)


class TelegramSearch:
    def __init__(self, pool: ClientPool | None, persistence: SearchPersistence):
        self._pool = pool
        self._persistence = persistence

    async def check_search_quota(self, query: str = "") -> dict | None:
        if not self._pool:
            return None

        result = await self._pool.get_premium_client()
        if result is None:
            return None

        session, phone = result
        session = adapt_transport_session(session, disconnect_on_close=False)
        try:
            return await self._check_search_quota_with_client(session, query)
        except Exception as exc:
            logger.debug("checkSearchPostsFlood unavailable: %s", exc)
            return None
        finally:
            await self._pool.release_client(phone)

    async def _check_search_quota_with_client(self, session, query: str = "") -> dict | None:
        try:
            quota_response = await session.lookup_search_posts_flood(query)
            return {
                "total_daily": getattr(quota_response, "total_daily", None),
                "remains": getattr(quota_response, "remains", None),
                "wait_till": getattr(quota_response, "wait_till", None),
                "query_is_free": getattr(quota_response, "query_is_free", False),
                "stars_amount": getattr(quota_response, "stars_amount", None),
            }
        except Exception as exc:
            logger.debug("checkSearchPostsFlood unavailable: %s", exc)
            return None

    async def _get_premium_unavailability_reason(self) -> str:
        if not self._pool:
            return "Нет подключённых Telegram-аккаунтов."

        reason_getter = getattr(self._pool, "get_premium_unavailability_reason", None)
        if not callable(reason_getter):
            return "Нет аккаунтов с Telegram Premium. Добавьте Premium-аккаунт в настройках."

        try:
            reason = reason_getter()
            if inspect.isawaitable(reason):
                reason = await reason
        except Exception as exc:
            logger.warning("Failed to resolve premium unavailability reason: %s", exc)
            return "Нет аккаунтов с Telegram Premium. Добавьте Premium-аккаунт в настройках."

        if isinstance(reason, str) and reason:
            return reason
        return "Нет аккаунтов с Telegram Premium. Добавьте Premium-аккаунт в настройках."

    async def search_telegram(self, query: str, limit: int = 50) -> SearchResult:
        if not self._pool:
            return SearchResult(
                messages=[],
                total=0,
                query=query,
                error="Нет подключённых Telegram-аккаунтов.",
            )

        result = await self._pool.get_premium_client()
        if result is None:
            reason = await self._get_premium_unavailability_reason()
            logger.warning("search_telegram: no premium client for query=%r: %s", query, reason)
            return SearchResult(messages=[], total=0, query=query, error=reason)

        session, phone = result
        session = adapt_transport_session(session, disconnect_on_close=False)
        try:
            quota = await self._check_search_quota_with_client(session, query)
            if quota and quota.get("remains") == 0 and not quota.get("query_is_free"):
                return SearchResult(
                    messages=[],
                    total=0,
                    query=query,
                    error=(
                        "Лимит Premium-поиска исчерпан на сегодня. "
                        "Попробуйте позже или используйте другой режим поиска."
                    ),
                )

            messages, seen_channels = await self._search_posts_global(session, query, limit)
            await self._persistence.cache_search_results(seen_channels, messages, phone, query)
            return SearchResult(messages=messages, total=len(messages), query=query)
        except Exception as exc:
            logger.exception("Telegram global search failed for query=%r", query)
            return SearchResult(
                messages=[],
                total=0,
                query=query,
                error=f"Ошибка поиска в Telegram: {exc}",
            )
        finally:
            await self._pool.release_client(phone)

    async def _search_posts_global(
        self,
        session,
        query: str,
        limit: int,
    ) -> tuple[list[Message], dict[int, Channel]]:
        from telethon.tl.types import InputPeerEmpty, PeerChannel
        from telethon.utils import get_input_peer

        messages: list[Message] = []
        seen_channels: dict[int, Channel] = {}

        offset_rate = 0
        offset_peer = InputPeerEmpty()
        offset_id = 0

        while len(messages) < limit:
            batch_limit = min(limit - len(messages), 100)
            search_response = await session.search_posts_batch(
                query,
                offset_rate=offset_rate,
                offset_peer=offset_peer,
                offset_id=offset_id,
                limit=batch_limit,
            )

            if not search_response.messages:
                break

            chats_map = {c.id: c for c in getattr(search_response, "chats", [])}
            users_map = {u.id: u for u in getattr(search_response, "users", [])}

            for msg in search_response.messages:
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

                sender_id, sender_name = TelegramMessageTransformer.resolve_sender(
                    msg,
                    chats_map,
                    users_map,
                )

                messages.append(
                    Message(
                        channel_id=chat_id,
                        message_id=msg.id,
                        sender_id=sender_id,
                        sender_name=sender_name,
                        text=getattr(msg, "message", None),
                        media_type=TelegramMessageTransformer.media_type_from_message(msg),
                        date=(
                            msg.date.replace(tzinfo=timezone.utc)
                            if msg.date and msg.date.tzinfo is None
                            else msg.date
                        ),
                        channel_title=chat_title,
                        channel_username=chat_username,
                    )
                )

            next_rate = getattr(search_response, "next_rate", None)
            if next_rate and len(search_response.messages) == batch_limit:
                offset_rate = next_rate
                last_msg = search_response.messages[-1]
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

    async def search_my_chats(self, query: str, limit: int = 50) -> SearchResult:
        if not self._pool:
            return SearchResult(
                messages=[],
                total=0,
                query=query,
                error="Нет подключённых Telegram-аккаунтов.",
            )

        result = await self._pool.get_available_client()
        if result is None:
            return SearchResult(
                messages=[],
                total=0,
                query=query,
                error="Нет доступных Telegram-аккаунтов. Проверьте подключение.",
            )

        session, phone = result
        session = adapt_transport_session(session, disconnect_on_close=False)
        try:
            await asyncio.wait_for(session.warm_dialog_cache(), timeout=30.0)

            async def _collect_my_chats() -> tuple[list[Message], dict[int, Channel]]:
                collected: list[Message] = []
                seen: dict[int, Channel] = {}
                async for msg in session.stream_messages(None, search=query, limit=limit):
                    converted = TelegramMessageTransformer.convert_telethon_message(msg)
                    if converted is None:
                        logger.debug(
                            "Skipping message in search_my_chats: id=%s has no chat context",
                            getattr(msg, "id", None),
                        )
                        continue
                    collected.append(converted)
                    if converted.channel_id not in seen:
                        seen[converted.channel_id] = Channel(
                            channel_id=converted.channel_id,
                            title=converted.channel_title,
                            username=converted.channel_username,
                        )
                return collected, seen

            messages, seen_channels = await asyncio.wait_for(_collect_my_chats(), timeout=90.0)

            await self._persistence.cache_messages_and_channels(seen_channels, messages)
            return SearchResult(messages=messages, total=len(messages), query=query)
        except Exception as exc:
            logger.exception("Telegram my_chats search failed for query=%r", query)
            return SearchResult(
                messages=[],
                total=0,
                query=query,
                error=f"Ошибка поиска в Telegram: {exc}",
            )
        finally:
            await self._pool.release_client(phone)

    async def search_in_channel(
        self,
        channel_id: int | None,
        query: str,
        limit: int = 50,
    ) -> SearchResult:
        if not self._pool:
            return SearchResult(
                messages=[],
                total=0,
                query=query,
                error="Нет подключённых Telegram-аккаунтов.",
            )

        result = await self._pool.get_available_client()
        if result is None:
            return SearchResult(
                messages=[],
                total=0,
                query=query,
                error="Нет доступных Telegram-аккаунтов. Проверьте подключение.",
            )

        session, phone = result
        session = adapt_transport_session(session, disconnect_on_close=False)
        try:
            await asyncio.wait_for(session.warm_dialog_cache(), timeout=30.0)

            entity = None
            if channel_id:
                try:
                    entity = await asyncio.wait_for(
                        session.resolve_entity(PeerChannel(channel_id)),
                        timeout=30.0,
                    )
                except Exception:
                    logger.debug(
                        "PeerChannel(%s) not in cache, trying username fallback",
                        channel_id,
                    )
                    ch_record = await self._persistence._search.channels.get_channel_by_channel_id(
                        channel_id
                    )
                    username = ch_record.username if ch_record else None
                    if username:
                        try:
                            entity = await asyncio.wait_for(
                                session.resolve_entity(username),
                                timeout=30.0,
                            )
                        except Exception as exc2:
                            logger.warning(
                                "Cannot resolve channel %s (@%s): %s",
                                channel_id,
                                username,
                                exc2,
                            )
                            return SearchResult(
                                messages=[],
                                total=0,
                                query=query,
                                error=f"Не удалось найти канал {channel_id}: {exc2}",
                            )
                    else:
                        return SearchResult(
                            messages=[],
                            total=0,
                            query=query,
                            error=(
                                f"Не удалось найти канал {channel_id}"
                                " (нет username для fallback)"
                            ),
                        )

            async def _collect_in_channel() -> tuple[list[Message], dict[int, Channel]]:
                collected: list[Message] = []
                seen: dict[int, Channel] = {}
                async for msg in session.stream_messages(entity, search=query, limit=limit):
                    converted = TelegramMessageTransformer.convert_telethon_message(msg)
                    if converted is None:
                        logger.debug(
                            "Skipping message in search_in_channel: id=%s has no chat context",
                            getattr(msg, "id", None),
                        )
                        continue
                    collected.append(converted)
                    if converted.channel_id not in seen:
                        seen[converted.channel_id] = Channel(
                            channel_id=converted.channel_id,
                            title=converted.channel_title,
                            username=converted.channel_username,
                        )
                return collected, seen

            messages, seen_channels = await asyncio.wait_for(_collect_in_channel(), timeout=90.0)

            await self._persistence.cache_messages_and_channels(seen_channels, messages)
            return SearchResult(messages=messages, total=len(messages), query=query)
        except Exception as exc:
            logger.exception(
                "Telegram channel search failed for channel_id=%s query=%r",
                channel_id,
                query,
            )
            return SearchResult(
                messages=[],
                total=0,
                query=query,
                error=f"Ошибка поиска в Telegram: {exc}",
            )
        finally:
            await self._pool.release_client(phone)
