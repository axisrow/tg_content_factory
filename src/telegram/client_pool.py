from __future__ import annotations

import asyncio
import base64
import io
import logging
from datetime import datetime, timedelta, timezone

from telethon import TelegramClient

from src.database import Database
from src.models import TelegramUserInfo
from src.telegram.auth import TelegramAuth

logger = logging.getLogger(__name__)


class ClientPool:
    """Pool of Telegram clients with fallback rotation on flood waits."""

    def __init__(self, auth: TelegramAuth, db: Database, max_flood_wait_sec: int = 300):
        self._auth = auth
        self._db = db
        self._max_flood_wait_sec = max_flood_wait_sec
        self.clients: dict[str, TelegramClient] = {}
        self._lock = asyncio.Lock()
        self._in_use: set[str] = set()

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
                    me = await client.get_me()
                    is_premium = bool(getattr(me, "premium", False))
                    if is_premium != acc.is_premium:
                        await self._db.update_account_premium(acc.phone, is_premium)
                except Exception:
                    pass
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
                if (
                    acc.flood_wait_until
                    and acc.flood_wait_until.replace(tzinfo=timezone.utc) > now
                ):
                    continue
                if acc.phone in self.clients:
                    self._in_use.add(acc.phone)
                    return self.clients[acc.phone], acc.phone

            # Fallback: if all clients are in use, return any non-flood-waited client
            # (allows the same client to be shared when there's only one account)
            for acc in accounts:
                if (
                    acc.flood_wait_until
                    and acc.flood_wait_until.replace(tzinfo=timezone.utc) > now
                ):
                    continue
                if acc.phone in self.clients:
                    return self.clients[acc.phone], acc.phone

            return None

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
            del self.clients[phone]

    async def disconnect_all(self) -> None:
        for phone in list(self.clients):
            await self.remove_client(phone)

    async def get_users_info(self) -> list[TelegramUserInfo]:
        """Get info about all connected Telegram accounts."""
        accounts = await self._db.get_accounts(active_only=True)
        primary_phones = {a.phone for a in accounts if a.is_primary}
        result: list[TelegramUserInfo] = []

        for phone, client in self.clients.items():
            try:
                me = await client.get_me()
                avatar_base64 = None
                try:
                    buf = io.BytesIO()
                    downloaded = await client.download_profile_photo("me", file=buf)
                    if downloaded:
                        buf.seek(0)
                        encoded = base64.b64encode(buf.read()).decode()
                        avatar_base64 = f"data:image/jpeg;base64,{encoded}"
                except Exception:
                    pass

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
        """Resolve channel by @username or t.me/ link. Returns dict with channel info."""
        result = await self.get_available_client()
        if not result:
            return None
        client, phone = result
        try:
            entity = await client.get_entity(identifier)
            if getattr(entity, "broadcast", False):
                channel_type = "channel"
            elif getattr(entity, "megagroup", False):
                channel_type = "group"
            else:
                channel_type = None
            return {
                "channel_id": entity.id,
                "title": entity.title,
                "username": getattr(entity, "username", None),
                "channel_type": channel_type,
            }
        finally:
            await self.release_client(phone)

    async def get_dialogs(self) -> list[dict]:
        """Get list of subscribed channels and groups."""
        result = await self.get_available_client()
        if not result:
            return []
        client, phone = result
        try:
            dialogs = []
            async for dialog in client.iter_dialogs():
                if dialog.is_channel or dialog.is_group:
                    entity = dialog.entity
                    if getattr(entity, "broadcast", False):
                        channel_type = "channel"
                    elif getattr(entity, "megagroup", False):
                        channel_type = "group"
                    else:
                        channel_type = None
                    dialogs.append({
                        "channel_id": entity.id,
                        "title": dialog.title,
                        "username": getattr(entity, "username", None),
                        "channel_type": channel_type,
                    })
            return dialogs
        finally:
            await self.release_client(phone)

