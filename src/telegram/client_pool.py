from __future__ import annotations

import asyncio
import base64
import io
import logging
import re
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from telethon import TelegramClient
from telethon.errors import FloodWaitError, UsernameInvalidError, UsernameNotOccupiedError
from telethon.tl.types import ChannelForbidden, PeerChannel

from src.database import Database
from src.models import TelegramUserInfo
from src.telegram.auth import TelegramAuth

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class StatsClientAvailability:
    state: str  # "available" | "all_flooded" | "no_connected_active"
    retry_after_sec: int | None = None
    next_available_at_utc: datetime | None = None


class ClientPool:
    """Pool of Telegram clients with fallback rotation on flood waits."""

    def __init__(self, auth: TelegramAuth, db: Database, max_flood_wait_sec: int = 300):
        self._auth = auth
        self._db = db
        self._max_flood_wait_sec = max_flood_wait_sec
        self.clients: dict[str, TelegramClient] = {}
        self._lock = asyncio.Lock()
        self._in_use: set[str] = set()
        self._dialogs_fetched: set[str] = set()

    def is_dialogs_fetched(self, phone: str) -> bool:
        """Return True if get_dialogs() was already called for this phone in this process."""
        return phone in self._dialogs_fetched

    def mark_dialogs_fetched(self, phone: str) -> None:
        """Mark that get_dialogs() has been called for this phone."""
        self._dialogs_fetched.add(phone)

    async def initialize(self) -> None:
        """Connect all active accounts from DB."""
        accounts = await self._db.get_accounts(active_only=True)
        for acc in accounts:
            try:
                client = await self._auth.create_client_from_session(acc.session_string)
                client.flood_sleep_threshold = 60
                self.clients[acc.phone] = client
                logger.info("Connected account: %s (primary=%s)", acc.phone, acc.is_primary)
                try:
                    me = await asyncio.wait_for(client.get_me(), timeout=15.0)
                    is_premium = bool(getattr(me, "premium", False))
                    if is_premium != acc.is_premium:
                        await self._db.update_account_premium(acc.phone, is_premium)
                except Exception as e:
                    logger.warning("Failed to fetch premium status for %s: %s", acc.phone, e)
            except Exception as e:
                logger.error("Failed to connect %s: %s", acc.phone, e)

    async def get_available_client(self) -> tuple[TelegramClient, str] | None:
        """Get first available client not in flood wait. Returns (client, phone) or None."""
        async with self._lock:
            now = datetime.now(timezone.utc)
            accounts = await self._db.get_accounts(active_only=True)

            for acc in accounts:
                if acc.phone in self._in_use:
                    continue
                flood_until = self._normalize_utc(acc.flood_wait_until)
                if flood_until and flood_until > now:
                    continue
                if acc.phone in self.clients:
                    self._in_use.add(acc.phone)
                    return self.clients[acc.phone], acc.phone

            # Fallback: if all clients are in use, return any non-flood-waited client
            # (allows the same client to be shared when there's only one account)
            for acc in accounts:
                flood_until = self._normalize_utc(acc.flood_wait_until)
                if flood_until and flood_until > now:
                    continue
                if acc.phone in self.clients:
                    return self.clients[acc.phone], acc.phone

            return None

    async def get_client_by_phone(self, phone: str) -> tuple[TelegramClient, str] | None:
        """Get a specific active connected client when it is not flood-waited."""
        async with self._lock:
            now = datetime.now(timezone.utc)
            accounts = await self._db.get_accounts(active_only=True)
            account = next((acc for acc in accounts if acc.phone == phone), None)
            if account is None:
                return None

            flood_until = self._normalize_utc(account.flood_wait_until)
            if flood_until and flood_until > now:
                return None

            client = self.clients.get(phone)
            if client is None:
                return None

            if phone not in self._in_use:
                self._in_use.add(phone)
            return client, phone

    async def get_premium_client(self) -> tuple[TelegramClient, str] | None:
        """Get first available premium client.

        Flood wait is ignored because premium search uses a different API method.
        """
        async with self._lock:
            accounts = await self._db.get_accounts(active_only=True)
            for acc in accounts:
                if not acc.is_premium:
                    continue
                if acc.phone in self._in_use:
                    continue
                if acc.phone in self.clients:
                    self._in_use.add(acc.phone)
                    return self.clients[acc.phone], acc.phone

            # Fallback: share client if all in use
            for acc in accounts:
                if not acc.is_premium:
                    continue
                if acc.phone in self.clients:
                    return self.clients[acc.phone], acc.phone

            return None

    async def get_premium_unavailability_reason(self) -> str:
        accounts = await self._db.get_accounts(active_only=True)
        premium = [acc for acc in accounts if acc.is_premium]
        if not premium:
            return "Нет аккаунтов с Telegram Premium. Добавьте Premium-аккаунт в настройках."
        connected = [acc for acc in premium if acc.phone in self.clients]
        if not connected:
            return "Premium-аккаунт не подключён. Перезапустите сервер."
        return "Premium-аккаунт недоступен."

    async def get_stats_availability(self) -> StatsClientAvailability:
        """Describe stats client availability for batch scheduling decisions."""
        async with self._lock:
            now = datetime.now(timezone.utc)
            accounts = await self._db.get_accounts(active_only=True)
            connected = [acc for acc in accounts if acc.phone in self.clients]
            if not connected:
                return StatsClientAvailability(state="no_connected_active")

            earliest: datetime | None = None
            for acc in connected:
                flood_until = self._normalize_utc(acc.flood_wait_until)
                if flood_until is None or flood_until <= now:
                    return StatsClientAvailability(state="available")
                if earliest is None or flood_until < earliest:
                    earliest = flood_until

            if earliest is None:
                return StatsClientAvailability(state="no_connected_active")

            retry_after_sec = max(1, int((earliest - now).total_seconds()))
            return StatsClientAvailability(
                state="all_flooded",
                retry_after_sec=retry_after_sec,
                next_available_at_utc=earliest,
            )

    async def release_client(self, phone: str) -> None:
        """Mark client as no longer in active use."""
        async with self._lock:
            self._in_use.discard(phone)

    async def report_flood(self, phone: str, wait_seconds: int) -> None:
        """Mark account as flood-waited."""
        until = datetime.now(timezone.utc) + timedelta(seconds=wait_seconds)
        await self._db.update_account_flood(phone, until)
        logger.warning("Flood wait for %s: %d seconds (until %s)", phone, wait_seconds, until)

    async def clear_flood(self, phone: str) -> None:
        await self._db.update_account_flood(phone, None)

    async def add_client(self, phone: str, session_string: str) -> None:
        """Add and connect a new client."""
        client = await self._auth.create_client_from_session(session_string)
        client.flood_sleep_threshold = 60
        self.clients[phone] = client

    async def remove_client(self, phone: str) -> None:
        if phone in self.clients:
            try:
                await self.clients[phone].disconnect()
            except Exception:
                pass
            self._dialogs_fetched.discard(phone)
            del self.clients[phone]

    async def disconnect_all(self) -> None:
        for phone in list(self.clients):
            await self.remove_client(phone)

    @staticmethod
    def _normalize_utc(value: datetime | None) -> datetime | None:
        if value is None:
            return None
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    async def get_users_info(self) -> list[TelegramUserInfo]:
        """Get info about all connected Telegram accounts."""
        accounts = await self._db.get_accounts(active_only=True)
        primary_phones = {a.phone for a in accounts if a.is_primary}
        result: list[TelegramUserInfo] = []

        for phone, client in self.clients.items():
            try:
                me = await asyncio.wait_for(client.get_me(), timeout=15.0)
                avatar_base64 = None
                try:
                    buf = io.BytesIO()
                    downloaded = await asyncio.wait_for(
                        client.download_profile_photo("me", file=buf), timeout=15.0
                    )
                    if downloaded:
                        buf.seek(0)
                        encoded = base64.b64encode(buf.read()).decode()
                        avatar_base64 = f"data:image/jpeg;base64,{encoded}"
                except Exception:
                    logger.debug("Failed to download avatar for %s", phone)

                result.append(TelegramUserInfo(
                    phone=phone,
                    first_name=me.first_name or "",
                    last_name=me.last_name or "",
                    username=me.username,
                    is_primary=phone in primary_phones,
                    avatar_base64=avatar_base64,
                ))
            except Exception as e:
                logger.error("Failed to get info for %s: %s", phone, e)

        result.sort(key=lambda u: (not u.is_primary, u.phone))
        return result

    async def resolve_channel(self, identifier: str) -> dict | None:
        """Resolve channel by @username or t.me/ link. Returns dict with channel info.

        Raises:
            RuntimeError("no_client") — no connected/available Telegram accounts.
        """
        # Normalize post links: https://t.me/channel/123 → https://t.me/channel
        identifier = re.sub(r"(t\.me/[^/\s]+)/\d+$", r"\1", identifier)

        # Use PeerChannel for numeric IDs so Telethon treats them as channels, not users
        if identifier.lstrip("-").isdigit():
            peer: str | PeerChannel = PeerChannel(abs(int(identifier)))
        else:
            peer = identifier

        for _attempt in range(3):
            result = await self.get_available_client()
            if not result:
                logger.warning("resolve_channel: no available client for '%s'", identifier)
                raise RuntimeError("no_client")
            client, phone = result
            try:
                entity = await asyncio.wait_for(client.get_entity(peer), timeout=30.0)
                if not hasattr(entity, "title"):
                    logger.info(
                        "resolve_channel: '%s' is a user, not a channel/group", identifier
                    )
                    return None
                if isinstance(entity, ChannelForbidden):
                    return None
                channel_type, deactivate = self._classify_entity(entity)
                return {
                    "channel_id": entity.id,
                    "title": entity.title,
                    "username": getattr(entity, "username", None),
                    "channel_type": channel_type,
                    "deactivate": deactivate,
                }
            except asyncio.TimeoutError:
                logger.warning("resolve_channel: get_entity timed out for '%s'", identifier)
                return None
            except FloodWaitError as e:
                await self.release_client(phone)
                await self.report_flood(phone, e.seconds)
                logger.warning(
                    "resolve_channel: flood wait %ds for '%s', rotating client",
                    e.seconds, identifier,
                )
                continue
            except (UsernameNotOccupiedError, UsernameInvalidError) as e:
                logger.warning("resolve_channel: username not found '%s': %s", identifier, e)
                return None
            except Exception as e:
                logger.warning("resolve_channel: failed to resolve '%s': %s", identifier, e)
                return None
            finally:
                await self.release_client(phone)
        logger.warning("resolve_channel: all clients flood-waited for '%s'", identifier)
        return None

    @staticmethod
    def _classify_entity(entity) -> tuple[str, bool]:
        """Return (channel_type, deactivate) for a Telegram channel/group entity."""
        if getattr(entity, "scam", False):
            channel_type = "scam"
        elif getattr(entity, "fake", False):
            channel_type = "fake"
        elif getattr(entity, "restricted", False):
            channel_type = "restricted"
        elif getattr(entity, "monoforum", False):
            channel_type = "monoforum"
        elif getattr(entity, "forum", False):
            channel_type = "forum"
        elif getattr(entity, "gigagroup", False):
            channel_type = "gigagroup"
        elif getattr(entity, "megagroup", False):
            channel_type = "supergroup"
        elif getattr(entity, "broadcast", False):
            channel_type = "channel"
        else:
            channel_type = "group"
        return channel_type, channel_type in ("scam", "fake", "restricted")

    async def get_dialogs_for_phone(self, phone: str, include_dm: bool = False) -> list[dict]:
        """Get all dialogs for a specific connected account."""
        result = await self.get_client_by_phone(phone)
        if not result:
            return []
        client, phone = result
        try:
            items: list[dict] = []

            async def _iter() -> None:
                async for dialog in client.iter_dialogs():
                    entity = dialog.entity
                    if dialog.is_channel or dialog.is_group:
                        channel_type, deactivate = self._classify_entity(entity)
                        items.append({
                            "channel_id": entity.id,
                            "title": dialog.title,
                            "username": getattr(entity, "username", None),
                            "channel_type": channel_type,
                            "deactivate": deactivate,
                        })
                    elif include_dm:
                        is_bot = getattr(entity, "bot", False)
                        items.append({
                            "channel_id": entity.id,
                            "title": dialog.title,
                            "username": getattr(entity, "username", None),
                            "channel_type": "bot" if is_bot else "dm",
                            "deactivate": False,
                        })

            try:
                await asyncio.wait_for(_iter(), timeout=60.0)
            except asyncio.TimeoutError:
                logger.warning(
                    "get_dialogs_for_phone: timed out for %s, returning %d partial results",
                    phone,
                    len(items),
                )
            return items
        finally:
            await self.release_client(phone)

    async def leave_channels(self, phone: str, channel_ids: list[int]) -> dict[int, bool]:
        """Leave/unsubscribe from a list of dialogs for the given account."""
        result = await self.get_client_by_phone(phone)
        if not result:
            return {cid: False for cid in channel_ids}
        client, phone = result
        outcomes: dict[int, bool] = {}
        try:
            for cid in channel_ids:
                try:
                    entity = await client.get_entity(cid)
                    await client.delete_dialog(entity)
                    outcomes[cid] = True
                    await asyncio.sleep(0.3)
                except Exception as e:
                    logger.warning("leave_channels: failed for %d: %s", cid, e)
                    outcomes[cid] = False
        finally:
            await self.release_client(phone)
        return outcomes

    async def get_dialogs(self) -> list[dict]:
        """Get list of subscribed channels and groups."""
        result = await self.get_available_client()
        if not result:
            return []
        client, phone = result
        try:
            async def _iter_dialogs() -> list[dict]:
                result: list[dict] = []
                async for dialog in client.iter_dialogs():
                    if dialog.is_channel or dialog.is_group:
                        entity = dialog.entity
                        channel_type, deactivate = self._classify_entity(entity)
                        result.append({
                            "channel_id": entity.id,
                            "title": dialog.title,
                            "username": getattr(entity, "username", None),
                            "channel_type": channel_type,
                            "deactivate": deactivate,
                        })
                return result

            try:
                dialogs = await asyncio.wait_for(_iter_dialogs(), timeout=60.0)
            except asyncio.TimeoutError:
                logger.warning("get_dialogs: iter_dialogs timed out for %s", phone)
                dialogs = []
            return dialogs
        finally:
            await self.release_client(phone)
