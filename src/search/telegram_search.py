from __future__ import annotations

import asyncio
import inspect
import logging
import time
from datetime import timezone

from src.models import Channel, Message, SearchResult
from src.search.persistence import SearchPersistence
from src.search.transformers import TelegramMessageTransformer
from src.telegram.backends import adapt_transport_session
from src.telegram.client_pool import ClientPool
from src.telegram.flood_wait import HandledFloodWaitError, run_with_flood_wait
from src.utils.safe_logging import mask_phone, query_log_fields

try:
    from telethon.tl.types import PeerChannel
except ImportError:  # pragma: no cover
    PeerChannel = None  # type: ignore[assignment,misc]

logger = logging.getLogger(__name__)
PREMIUM_SEARCH_RPC_TIMEOUT_SEC = 60.0


def _elapsed_ms(started_at: float) -> int:
    return int((time.monotonic() - started_at) * 1000)


def _timeout_error(stage: str) -> str:
    return (
        f"Telegram Premium search timed out after {PREMIUM_SEARCH_RPC_TIMEOUT_SEC:.0f}s "
        f"(stage={stage}). Попробуйте позже или используйте другой режим поиска."
    )


class TelegramSearch:
    def __init__(self, pool: ClientPool | None, persistence: SearchPersistence):
        self._pool = pool
        self._persistence = persistence

    async def _warm_dialog_cache_if_needed(self, session, phone: str) -> None:
        cache_ready = False
        cache_checker = getattr(self._pool, "is_dialogs_fetched", None)
        if callable(cache_checker):
            cache_ready = bool(cache_checker(phone))
        if cache_ready:
            return

        await run_with_flood_wait(
            session.warm_dialog_cache(),
            operation="search_warm_dialog_cache",
            phone=phone,
            pool=self._pool,
            logger_=logger,
            timeout=30.0,
        )

        marker = getattr(self._pool, "mark_dialogs_fetched", None)
        if callable(marker):
            result = marker(phone)
            if inspect.isawaitable(result):
                await result

    async def _load_search_quota_with_flood_handling(
        self,
        session,
        phone: str,
        *,
        query: str,
        operation: str,
    ) -> dict | None:
        try:
            return await run_with_flood_wait(
                self._check_search_quota_with_client(session, query),
                operation=operation,
                phone=mask_phone(phone),
                pool=None,
                logger_=logger,
                timeout=PREMIUM_SEARCH_RPC_TIMEOUT_SEC,
            )
        except HandledFloodWaitError:
            raise
        except asyncio.TimeoutError:
            raise
        except Exception as exc:
            logger.debug("checkSearchPostsFlood unavailable: %s", exc)
            return None

    async def check_search_quota(self, query: str = "") -> dict | None:
        if not self._pool:
            return None

        result = await self._pool.get_premium_client()
        if result is None:
            return None

        session, phone = result
        session = adapt_transport_session(session, disconnect_on_close=False)
        try:
            return await self._load_search_quota_with_flood_handling(
                session,
                phone,
                query=query,
                operation="check_search_quota",
            )
        except HandledFloodWaitError as exc:
            reporter = getattr(self._pool, "report_premium_flood", None)
            if callable(reporter):
                await reporter(phone, exc.info.wait_seconds)
            logger.debug("checkSearchPostsFlood flood-waited for %s: %s", mask_phone(phone), exc.info.detail)
            return None
        except asyncio.TimeoutError:
            fields = query_log_fields(query)
            logger.warning(
                "premium_search_quota timeout stage=quota phone=%s timeout_sec=%.0f "
                "query_hash=%s query_len=%d query_preview=%r",
                mask_phone(phone),
                PREMIUM_SEARCH_RPC_TIMEOUT_SEC,
                fields["query_hash"],
                fields["query_len"],
                fields["query_preview"],
            )
            return None
        finally:
            await self._pool.release_client(phone)

    async def _check_search_quota_with_client(self, session, query: str = "") -> dict | None:
        quota_response = await session.lookup_search_posts_flood(query)
        return {
            "total_daily": getattr(quota_response, "total_daily", None),
            "remains": getattr(quota_response, "remains", None),
            "wait_till": getattr(quota_response, "wait_till", None),
            "query_is_free": getattr(quota_response, "query_is_free", False),
            "stars_amount": getattr(quota_response, "stars_amount", None),
        }

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
        search_started_at = time.monotonic()
        fields = query_log_fields(query)
        logger.info(
            "premium_search start mode=telegram limit=%d query_hash=%s query_len=%d query_preview=%r",
            limit,
            fields["query_hash"],
            fields["query_len"],
            fields["query_preview"],
        )
        if not self._pool:
            logger.warning(
                "premium_search unavailable reason=no_pool limit=%d query_hash=%s",
                limit,
                fields["query_hash"],
            )
            return SearchResult(
                messages=[],
                total=0,
                query=query,
                error="Нет подключённых Telegram-аккаунтов.",
            )

        result = await self._pool.get_premium_client()
        if result is None:
            reason = await self._get_premium_unavailability_reason()
            logger.warning(
                "premium_search unavailable reason=no_premium_client detail=%s limit=%d "
                "query_hash=%s query_len=%d query_preview=%r",
                reason,
                limit,
                fields["query_hash"],
                fields["query_len"],
                fields["query_preview"],
            )
            return SearchResult(messages=[], total=0, query=query, error=reason)

        session, phone = result
        session = adapt_transport_session(session, disconnect_on_close=False)
        masked_phone = mask_phone(phone)
        logger.info(
            "premium_search acquired phone=%s limit=%d query_hash=%s",
            masked_phone,
            limit,
            fields["query_hash"],
        )
        try:
            quota_started_at = time.monotonic()
            try:
                quota = await self._load_search_quota_with_flood_handling(
                    session,
                    phone,
                    query=query,
                    operation="search_telegram_check_quota",
                )
            except asyncio.TimeoutError:
                logger.warning(
                    "premium_search timeout stage=quota phone=%s elapsed_ms=%d timeout_sec=%.0f "
                    "limit=%d query_hash=%s query_len=%d query_preview=%r",
                    masked_phone,
                    _elapsed_ms(quota_started_at),
                    PREMIUM_SEARCH_RPC_TIMEOUT_SEC,
                    limit,
                    fields["query_hash"],
                    fields["query_len"],
                    fields["query_preview"],
                )
                return SearchResult(
                    messages=[],
                    total=0,
                    query=query,
                    error=_timeout_error("quota"),
                )
            logger.info(
                "premium_search quota_ok phone=%s elapsed_ms=%d remains=%s query_is_free=%s "
                "query_hash=%s",
                masked_phone,
                _elapsed_ms(quota_started_at),
                quota.get("remains") if quota else None,
                quota.get("query_is_free") if quota else None,
                fields["query_hash"],
            )
            if quota and quota.get("remains") == 0 and not quota.get("query_is_free"):
                logger.warning(
                    "premium_search quota_exhausted phone=%s elapsed_ms=%d query_hash=%s",
                    masked_phone,
                    _elapsed_ms(search_started_at),
                    fields["query_hash"],
                )
                return SearchResult(
                    messages=[],
                    total=0,
                    query=query,
                    error=(
                        "Лимит Premium-поиска исчерпан на сегодня. "
                        "Попробуйте позже или используйте другой режим поиска."
                    ),
                )

            rpc_started_at = time.monotonic()
            messages, seen_channels = await run_with_flood_wait(
                self._search_posts_global(session, query, limit),
                operation="search_telegram",
                phone=masked_phone,
                pool=None,
                logger_=logger,
                timeout=PREMIUM_SEARCH_RPC_TIMEOUT_SEC,
            )
            logger.info(
                "premium_search telegram_rpc_ok phone=%s elapsed_ms=%d raw_messages=%d "
                "channels=%d query_hash=%s",
                masked_phone,
                _elapsed_ms(rpc_started_at),
                len(messages),
                len(seen_channels),
                fields["query_hash"],
            )
            clearer = getattr(self._pool, "clear_premium_flood", None)
            if callable(clearer):
                result = clearer(phone)
                if inspect.isawaitable(result):
                    await result
            messages = await self._persistence.cache_search_results(seen_channels, messages, phone, query)
            logger.info(
                "premium_search success phone=%s elapsed_ms=%d total=%d query_hash=%s",
                masked_phone,
                _elapsed_ms(search_started_at),
                len(messages),
                fields["query_hash"],
            )
            return SearchResult(messages=messages, total=len(messages), query=query)
        except asyncio.TimeoutError:
            logger.warning(
                "premium_search timeout stage=telegram_rpc phone=%s elapsed_ms=%d timeout_sec=%.0f "
                "limit=%d query_hash=%s query_len=%d query_preview=%r",
                masked_phone,
                _elapsed_ms(search_started_at),
                PREMIUM_SEARCH_RPC_TIMEOUT_SEC,
                limit,
                fields["query_hash"],
                fields["query_len"],
                fields["query_preview"],
            )
            return SearchResult(
                messages=[],
                total=0,
                query=query,
                error=_timeout_error("telegram_rpc"),
            )
        except HandledFloodWaitError as exc:
            reporter = getattr(self._pool, "report_premium_flood", None)
            if callable(reporter):
                await reporter(phone, exc.info.wait_seconds)
            logger.warning(
                "premium_search flood_wait phone=%s wait_seconds=%d operation=%s query_hash=%s",
                masked_phone,
                exc.info.wait_seconds,
                exc.info.operation,
                fields["query_hash"],
            )
            return SearchResult(
                messages=[],
                total=0,
                query=query,
                error=exc.info.detail,
                flood_wait=exc.info,
            )
        except Exception as exc:
            logger.exception(
                "premium_search error phone=%s elapsed_ms=%d query_hash=%s query_len=%d "
                "query_preview=%r",
                masked_phone,
                _elapsed_ms(search_started_at),
                fields["query_hash"],
                fields["query_len"],
                fields["query_preview"],
            )
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

                sender_identity = TelegramMessageTransformer.resolve_sender_identity(
                    msg,
                    chats_map,
                    users_map,
                )

                messages.append(
                    Message(
                        channel_id=chat_id,
                        message_id=msg.id,
                        sender_id=sender_identity.sender_id,
                        sender_name=sender_identity.sender_name,
                        sender_first_name=sender_identity.sender_first_name,
                        sender_last_name=sender_identity.sender_last_name,
                        sender_username=sender_identity.sender_username,
                        text=getattr(msg, "message", None),
                        media_type=TelegramMessageTransformer.media_type_from_message(msg),
                        date=(
                            msg.date.replace(tzinfo=timezone.utc)
                            if msg.date and msg.date.tzinfo is None
                            else msg.date
                        ),
                        channel_title=chat_title,
                        channel_username=chat_username,
                        **TelegramMessageTransformer.engagement_fields_from_message(msg),
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
            await self._warm_dialog_cache_if_needed(session, phone)

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

            messages, seen_channels = await run_with_flood_wait(
                _collect_my_chats(),
                operation="search_my_chats",
                phone=phone,
                pool=self._pool,
                logger_=logger,
                timeout=90.0,
            )

            messages = await self._persistence.cache_messages_and_channels(seen_channels, messages)
            return SearchResult(messages=messages, total=len(messages), query=query)
        except HandledFloodWaitError as exc:
            return SearchResult(
                messages=[],
                total=0,
                query=query,
                error=exc.info.detail,
                flood_wait=exc.info,
            )
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
            await self._warm_dialog_cache_if_needed(session, phone)

            entity = None
            if channel_id:
                try:
                    entity = await run_with_flood_wait(
                        session.resolve_entity(PeerChannel(channel_id)),
                        operation="search_in_channel_resolve_entity",
                        phone=phone,
                        pool=self._pool,
                        logger_=logger,
                        timeout=30.0,
                    )
                except HandledFloodWaitError:
                    raise
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
                            entity = await run_with_flood_wait(
                                session.resolve_entity(username),
                                operation="search_in_channel_resolve_username",
                                phone=phone,
                                pool=self._pool,
                                logger_=logger,
                                timeout=30.0,
                            )
                        except HandledFloodWaitError:
                            raise
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

            messages, seen_channels = await run_with_flood_wait(
                _collect_in_channel(),
                operation="search_in_channel",
                phone=phone,
                pool=self._pool,
                logger_=logger,
                timeout=90.0,
            )

            messages = await self._persistence.cache_messages_and_channels(seen_channels, messages)
            return SearchResult(messages=messages, total=len(messages), query=query)
        except HandledFloodWaitError as exc:
            return SearchResult(
                messages=[],
                total=0,
                query=query,
                error=exc.info.detail,
                flood_wait=exc.info,
            )
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
