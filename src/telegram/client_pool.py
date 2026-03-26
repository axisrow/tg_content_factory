from __future__ import annotations

import asyncio
import base64
import io
import logging
import re
import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

from telethon.errors import (
    ChannelPrivateError,
    ChatAdminRequiredError,
    UsernameInvalidError,
    UsernameNotOccupiedError,
)
from telethon.tl.types import ChannelForbidden, PeerChannel, PeerUser

from src.config import TelegramRuntimeConfig
from src.database import Database
from src.models import Account, TelegramUserInfo
from src.telegram.account_lease_pool import AccountLease, AccountLeasePool
from src.telegram.auth import TelegramAuth
from src.telegram.backends import (
    BackendClientLease,
    BackendRouter,
    NativeTelethonBackend,
    TelegramTransportSession,
    TelethonCliBackend,
    adapt_transport_session,
)
from src.telegram.flood_wait import HandledFloodWaitError, run_with_flood_wait
from src.telegram.session_materializer import SessionMaterializer
from src.telegram.utils import normalize_utc

logger = logging.getLogger(__name__)


@dataclass
class DialogFetchStats:
    raw_dialogs: int = 0
    channels: int = 0
    groups: int = 0
    dms: int = 0
    bots: int = 0
    partial: bool = False


@dataclass
class DialogCacheEntry:
    fetched_at_monotonic: float
    dialogs: list[dict]


@dataclass(frozen=True)
class StatsClientAvailability:
    state: str  # "available" | "all_flooded" | "no_connected_active"
    retry_after_sec: int | None = None
    next_available_at_utc: datetime | None = None


class ClientPool:
    """Pool of Telegram clients with fallback rotation on flood waits."""

    def __init__(
        self,
        auth: TelegramAuth,
        db: Database,
        max_flood_wait_sec: int = 300,
        runtime_config: TelegramRuntimeConfig | None = None,
    ):
        self._auth = auth
        self._db = db
        self._max_flood_wait_sec = max_flood_wait_sec
        self._runtime_config = self._normalize_runtime_config(runtime_config)
        self.clients: dict[str, object] = {}
        self.init_timeout: float = 15.0
        self._lock = asyncio.Lock()
        self._in_use: set[str] = set()
        self._lease_pool = AccountLeasePool(db, self._in_use)
        self._session_overrides: dict[str, str] = {}
        self._active_leases: dict[str, list[BackendClientLease]] = defaultdict(list)
        self._materializer = SessionMaterializer(self._runtime_config.session_cache_dir)
        self._native_backend = NativeTelethonBackend(auth)
        self._primary_backend = TelethonCliBackend(
            auth,
            self._materializer,
            transport=self._runtime_config.cli_transport,
        )
        self._backend_router = BackendRouter(
            mode=self._runtime_config.backend_mode,
            primary=self._primary_backend,
            native=self._native_backend,
        )
        self._dialogs_fetched: set[str] = set()
        self._dialogs_cache: dict[tuple[str, str], DialogCacheEntry] = {}
        self._dialogs_cache_ttl_sec = 60.0
        self._premium_flood_wait_until: dict[str, datetime] = {}

    def is_dialogs_fetched(self, phone: str) -> bool:
        """Return True if get_dialogs() was already called for this phone in this process."""
        return phone in self._dialogs_fetched

    def mark_dialogs_fetched(self, phone: str) -> None:
        """Mark that get_dialogs() has been called for this phone."""
        self._dialogs_fetched.add(phone)

    def invalidate_dialogs_cache(self, phone: str | None = None) -> None:
        if phone is None:
            self._dialogs_cache.clear()
            return
        keys = [key for key in self._dialogs_cache if key[0] == phone]
        for key in keys:
            del self._dialogs_cache[key]

    async def _get_db_cached_dialogs(self, phone: str, mode: str) -> list[dict] | None:
        full_dialogs = await self._db.repos.dialog_cache.list_dialogs(phone)
        if not full_dialogs:
            return None
        if mode == "channels_only":
            filtered = [
                dict(dialog)
                for dialog in full_dialogs
                if dialog.get("channel_type") not in ("dm", "bot", "saved")
            ]
            self._store_cached_dialogs(phone, mode, filtered)
            self._store_cached_dialogs(phone, "full", full_dialogs)
            return filtered
        self._store_cached_dialogs(phone, "full", full_dialogs)
        return [dict(dialog) for dialog in full_dialogs]

    def _get_cached_dialogs(self, phone: str, mode: str) -> list[dict] | None:
        entry = self._dialogs_cache.get((phone, mode))
        if entry is not None:
            age = time.monotonic() - entry.fetched_at_monotonic
            if age <= self._dialogs_cache_ttl_sec:
                return [dict(dialog) for dialog in entry.dialogs]
            self._dialogs_cache.pop((phone, mode), None)

        if mode == "channels_only":
            full_entry = self._dialogs_cache.get((phone, "full"))
            if full_entry is not None:
                age = time.monotonic() - full_entry.fetched_at_monotonic
                if age > self._dialogs_cache_ttl_sec:
                    self._dialogs_cache.pop((phone, "full"), None)
                    return None
                filtered = [
                    dict(dialog)
                    for dialog in full_entry.dialogs
                    if dialog.get("channel_type") not in ("dm", "bot", "saved")
                ]
                self._store_cached_dialogs(phone, mode, filtered)
                return filtered
            return None
        return None

    def _store_cached_dialogs(self, phone: str, mode: str, dialogs: list[dict]) -> None:
        self._dialogs_cache[(phone, mode)] = DialogCacheEntry(
            fetched_at_monotonic=time.monotonic(),
            dialogs=[dict(dialog) for dialog in dialogs],
        )

    async def _get_cached_dialog(self, phone: str, dialog_id: int) -> dict | None:
        full_entry = self._dialogs_cache.get((phone, "full"))
        if full_entry is not None:
            age = time.monotonic() - full_entry.fetched_at_monotonic
            if age <= self._dialogs_cache_ttl_sec:
                for dialog in full_entry.dialogs:
                    if int(dialog.get("channel_id", 0)) == dialog_id:
                        return dict(dialog)
            else:
                self._dialogs_cache.pop((phone, "full"), None)
        return await self._db.repos.dialog_cache.get_dialog(phone, dialog_id)

    async def resolve_dialog_entity(
        self,
        session: TelegramTransportSession | object,
        phone: str,
        dialog_id: int,
        target_type: str | None = None,
    ):
        session = adapt_transport_session(session, disconnect_on_close=False)
        peer = PeerUser(dialog_id) if target_type in ("dm", "bot", "saved") else PeerChannel(abs(dialog_id))
        try:
            return await run_with_flood_wait(
                session.resolve_input_entity(peer),
                operation="resolve_dialog_entity_peer",
                phone=phone,
                pool=self,
                logger_=logger,
                timeout=30.0,
            )
        except (ValueError, TypeError):
            pass

        try:
            await run_with_flood_wait(
                session.warm_dialog_cache(),
                operation="resolve_dialog_entity_warm_dialog_cache",
                phone=phone,
                pool=self,
                logger_=logger,
                timeout=60.0,
            )
            self.mark_dialogs_fetched(phone)
            return await run_with_flood_wait(
                session.resolve_input_entity(peer),
                operation="resolve_dialog_entity_peer_after_warm",
                phone=phone,
                pool=self,
                logger_=logger,
                timeout=30.0,
            )
        except (ValueError, TypeError):
            pass

        dialog = await self._get_cached_dialog(phone, dialog_id)
        username = dialog.get("username") if dialog else None
        if username:
            return await run_with_flood_wait(
                session.resolve_input_entity(username),
                operation="resolve_dialog_entity_username",
                phone=phone,
                pool=self,
                logger_=logger,
                timeout=30.0,
            )

        return await run_with_flood_wait(
            session.resolve_input_entity(peer),
            operation="resolve_dialog_entity_peer_fallback",
            phone=phone,
            pool=self,
            logger_=logger,
            timeout=30.0,
        )

    async def initialize(self) -> None:
        """Load active accounts and validate that their sessions are usable."""
        accounts = await self._db.get_accounts(active_only=True)
        new_accounts = [acc for acc in accounts if acc.phone not in self.clients]
        if not new_accounts:
            return

        async def _init_one(acc: Account) -> None:
            lease: BackendClientLease | None = None
            try:
                lease = await self._connect_account(acc)
                session = lease.session
                logger.info("Connected account: %s (primary=%s)", acc.phone, acc.is_primary)
                try:
                    me = await asyncio.wait_for(session.fetch_me(), timeout=10.0)
                    is_premium = bool(getattr(me, "premium", False))
                    if is_premium != acc.is_premium:
                        await self._db.update_account_premium(acc.phone, is_premium)
                except Exception as e:
                    logger.warning("Failed to fetch premium status for %s: %s", acc.phone, e)
                finally:
                    if lease is not None:
                        await self._backend_router.release(lease)
            except Exception as e:
                logger.error("Failed to connect %s: %s", acc.phone, e)

        tasks = {asyncio.create_task(_init_one(acc)): acc for acc in new_accounts}
        done, pending = await asyncio.wait(tasks.keys(), timeout=self.init_timeout)
        if pending:
            phones = []
            for task in pending:
                acc = tasks[task]
                phones.append(acc.phone)
                logger.warning("Account %s init timed out — skipping", acc.phone)
                task.cancel()
            await asyncio.wait(pending, timeout=3.0)
            for phone in phones:
                if phone in self.clients:
                    try:
                        await asyncio.wait_for(self.clients[phone].close(), timeout=2.0)
                    except Exception:
                        pass
                    del self.clients[phone]

    async def get_available_client(self) -> tuple[TelegramTransportSession, str] | None:
        """Get first available client not in flood wait. Returns (client, phone) or None."""
        for _ in range(max(1, len(self.clients))):
            lease = await self._lease_pool.acquire_available(self._connected_phones())
            if lease is None:
                return None
            result = await self._acquire_from_lease(lease)
            if result is not None:
                return result
        return None

    async def get_client_by_phone(
        self,
        phone: str,
    ) -> tuple[TelegramTransportSession, str] | None:
        """Get a specific active connected client when it is not flood-waited."""
        lease = await self._acquire_phone_lease(phone)
        if lease is None:
            return None
        return await self._acquire_from_lease(lease)

    async def get_native_client_by_phone(
        self,
        phone: str,
    ) -> tuple[object, str] | None:
        """Get a specific client through the native backend for stateful flows."""
        lease = await self._acquire_phone_lease(phone)
        if lease is None:
            return None
        result = await self._acquire_from_lease(lease, force_native=True)
        if result is None:
            return None
        session, acquired_phone = result
        return session.raw_client, acquired_phone

    async def get_premium_client(self) -> tuple[TelegramTransportSession, str] | None:
        """Get first available premium client.

        Premium-only flood waits are tracked separately from generic account flood waits.
        """
        blocked_phones = self._premium_flooded_phones()
        for _ in range(max(1, len(self.clients))):
            lease = await self._lease_pool.acquire_premium(
                self._connected_phones(),
                blocked_phones=blocked_phones,
            )
            if lease is None:
                return None
            result = await self._acquire_from_lease(lease)
            if result is not None:
                return result
        return None

    async def get_premium_unavailability_reason(self) -> str:
        accounts = await self._db.get_accounts(active_only=True)
        premium = [acc for acc in accounts if acc.is_premium]
        if not premium:
            return "Нет аккаунтов с Telegram Premium. Добавьте Premium-аккаунт в настройках."
        connected = [acc for acc in premium if acc.phone in self.clients]
        if not connected:
            return "Premium-аккаунт не подключён. Перезапустите сервер."
        now = datetime.now(timezone.utc)
        blocked = [
            phone
            for phone, until in self._premium_flood_wait_until.items()
            if phone in {acc.phone for acc in connected} and until > now
        ]
        if blocked and len(blocked) == len(connected):
            return "Premium-аккаунты временно недоступны из-за Flood Wait."
        return "Premium-аккаунт недоступен."

    async def get_stats_availability(self) -> StatsClientAvailability:
        """Describe stats client availability for batch scheduling decisions."""
        state, retry_after_sec, next_available_at_utc = (
            await self._lease_pool.snapshot_stats_availability(self._connected_phones())
        )
        return StatsClientAvailability(
            state=state,
            retry_after_sec=retry_after_sec,
            next_available_at_utc=next_available_at_utc,
        )

    async def get_premium_stats_availability(self) -> StatsClientAvailability:
        """Describe premium client availability including premium-only flood waits."""
        accounts = await self._db.get_accounts(active_only=True)
        now = datetime.now(timezone.utc)
        connected_premium = [
            acc
            for acc in accounts
            if acc.is_premium and acc.phone in self.clients
        ]
        if not connected_premium:
            return StatsClientAvailability(state="no_connected_active")

        earliest: datetime | None = None
        for account in connected_premium:
            premium_until = self._premium_flood_wait_until.get(account.phone)
            if premium_until is None or premium_until <= now:
                return StatsClientAvailability(state="available")
            if earliest is None or premium_until < earliest:
                earliest = premium_until

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
            lease = None
            stack = self._active_leases.get(phone)
            if stack:
                lease = stack.pop()
                if not stack:
                    self._active_leases.pop(phone, None)
            should_release = not self._active_leases.get(phone)
        if should_release:
            await self._lease_pool.release(phone)
        if lease is not None and lease.disconnect_on_release:
            await self._backend_router.release(lease)

    async def report_flood(self, phone: str, wait_seconds: int) -> None:
        """Mark account as flood-waited."""
        until = datetime.now(timezone.utc) + timedelta(seconds=wait_seconds)
        await self._db.update_account_flood(phone, until)
        logger.warning("Flood wait for %s: %d seconds (until %s)", phone, wait_seconds, until)

    async def report_premium_flood(self, phone: str, wait_seconds: int) -> None:
        """Mark account as premium-search flood-waited without touching generic account state."""
        now = datetime.now(timezone.utc)
        until = now + timedelta(seconds=wait_seconds)
        self._premium_flood_wait_until[phone] = until
        # Eager cleanup of expired entries
        expired = [p for p, u in self._premium_flood_wait_until.items() if u <= now and p != phone]
        for p in expired:
            del self._premium_flood_wait_until[p]
        logger.warning(
            "Premium flood wait for %s: %d seconds (until %s)",
            phone,
            wait_seconds,
            until,
        )

    async def clear_flood(self, phone: str) -> None:
        await self._db.update_account_flood(phone, None)

    def clear_premium_flood(self, phone: str) -> None:
        self._premium_flood_wait_until.pop(phone, None)

    async def add_client(self, phone: str, session_string: str) -> None:
        """Register a new account as connected and validate its stored session."""
        self._session_overrides[phone] = session_string
        account = Account(phone=phone, session_string=session_string, is_active=True)
        lease = await self._connect_account(account)
        await self._backend_router.release(lease)

    async def reconnect_phone(self, phone: str) -> bool:
        """Attempt to reconnect a disconnected client. Returns True on success."""
        session = self.clients.get(phone)
        if session is None:
            return False
        try:
            client = session.raw_client
            if not client.is_connected():
                logger.info("Reconnecting client for %s", phone)
                await client.connect()
            return client.is_connected()
        except Exception:
            logger.exception("Failed to reconnect client for %s", phone)
            return False

    async def remove_client(self, phone: str) -> None:
        self._session_overrides.pop(phone, None)
        async with self._lock:
            leases = list(self._active_leases.pop(phone, []))
        for lease in reversed(leases):
            if lease.disconnect_on_release:
                try:
                    await self._backend_router.release(lease)
                except Exception:
                    logger.debug("Failed to release live lease for %s", phone, exc_info=True)
        client = self.clients.pop(phone, None)
        if isinstance(client, TelegramTransportSession):
            try:
                await client.raw_client.disconnect()
            except Exception:
                logger.debug("Failed to disconnect session for %s", phone, exc_info=True)
        self._in_use.discard(phone)
        self._dialogs_fetched.discard(phone)
        self.invalidate_dialogs_cache(phone)
        self.clear_premium_flood(phone)

    def _premium_flooded_phones(self) -> set[str]:
        now = datetime.now(timezone.utc)
        expired = [
            phone
            for phone, until in self._premium_flood_wait_until.items()
            if until <= now
        ]
        for phone in expired:
            self._premium_flood_wait_until.pop(phone, None)
        return set(self._premium_flood_wait_until)

    async def disconnect_all(self) -> None:
        for phone in list(self.clients):
            await self.remove_client(phone)

    @staticmethod
    def _normalize_runtime_config(
        runtime_config: TelegramRuntimeConfig | None,
    ) -> TelegramRuntimeConfig:
        if runtime_config is None:
            return TelegramRuntimeConfig(
                backend_mode="auto",
                cli_transport="hybrid",
            )
        if runtime_config.backend_mode not in {"auto", "telethon_cli", "native"}:
            logger.warning(
                "Unknown backend_mode %r, falling back to 'auto'", runtime_config.backend_mode
            )
            runtime_config.backend_mode = "auto"
        if runtime_config.cli_transport not in {"in_process", "subprocess", "hybrid"}:
            logger.warning(
                "Unknown cli_transport %r, falling back to 'hybrid'", runtime_config.cli_transport
            )
            runtime_config.cli_transport = "hybrid"
        return runtime_config

    def _connected_phones(self) -> set[str]:
        return set(self.clients.keys())

    def _direct_session(self, phone: str) -> TelegramTransportSession | None:
        candidate = self.clients.get(phone)
        if isinstance(candidate, TelegramTransportSession):
            return candidate
        return None

    async def _get_account_for_phone(
        self,
        phone: str,
        *,
        active_only: bool = True,
    ) -> Account | None:
        account = await self._lease_pool.get_account(phone, active_only=active_only)
        if account is not None:
            return account
        session_string = self._session_overrides.get(phone)
        if session_string is None:
            return None
        return Account(phone=phone, session_string=session_string, is_active=True)

    async def _acquire_phone_lease(self, phone: str) -> AccountLease | None:
        lease = await self._lease_pool.acquire_by_phone(phone, self._connected_phones())
        if lease is not None:
            return lease

        if phone not in self.clients:
            return None
        account = await self._get_account_for_phone(phone)
        if account is None:
            return None
        flood_until = normalize_utc(account.flood_wait_until)
        if flood_until and flood_until > datetime.now(timezone.utc):
            return None
        async with self._lock:
            if phone not in self._in_use:
                self._in_use.add(phone)
                return AccountLease(account=account, shared=False)
        return AccountLease(account=account, shared=True)

    async def _acquire_from_lease(
        self,
        account_lease: AccountLease,
        *,
        force_native: bool = False,
    ) -> tuple[TelegramTransportSession, str] | None:
        phone = account_lease.account.phone
        # force_native bypasses the persistent pool session — callers need a raw native client
        direct_session = None if force_native else self._direct_session(phone)
        lease: BackendClientLease | None = None
        try:
            if direct_session is not None:
                lease = BackendClientLease(
                    phone=phone,
                    session=direct_session,
                    backend_name="direct",
                    disconnect_on_release=False,
                )
            else:
                lease = await self._backend_router.acquire_client(
                    account_lease.account,
                    force_native=force_native,
                )
                if not force_native:
                    # Store persistent session for future direct reuse.
                    # force_native sessions are short-lived and must not replace the pool session.
                    self.clients[phone] = TelegramTransportSession(
                        lease.session.raw_client, disconnect_on_close=False
                    )
                    lease.disconnect_on_release = False

            async with self._lock:
                self._active_leases[phone].append(lease)
            return lease.session, phone
        except Exception as exc:
            logger.error("Failed to acquire client for %s: %s", phone, exc)
            if not account_lease.shared:
                await self._lease_pool.release(phone)
            if lease is not None and lease.disconnect_on_release:
                try:
                    await self._backend_router.release(lease)
                except Exception:
                    logger.debug("Failed to release broken lease for %s", phone, exc_info=True)
            if direct_session is None:
                self.clients.pop(phone, None)
            return None

    async def _connect_account(self, account: Account) -> BackendClientLease:
        lease = await self._backend_router.acquire_client(account)
        # Store persistent transport session so _direct_session() can reuse the connection
        self.clients[account.phone] = TelegramTransportSession(
            lease.session.raw_client, disconnect_on_close=False
        )
        lease.disconnect_on_release = False
        return lease

    async def get_users_info(self, include_avatar: bool = True) -> list[TelegramUserInfo]:
        """Get info about all connected Telegram accounts.

        Direct sessions (self.clients) are borrowed refs with disconnect_on_close=False —
        they must NOT be released here; the pool owns their lifetime.
        Only the fallback lease (when no direct session is available) requires explicit release.

        Args:
            include_avatar: If True (default), download and encode profile photos as base64.
                           Set to False for CLI usage to skip unnecessary 15s I/O per account.
        """
        accounts = await self._db.get_accounts(active_only=True)
        primary_phones = {a.phone for a in accounts if a.is_primary}
        result: list[TelegramUserInfo] = []

        for phone in sorted(self.clients):
            session = self._direct_session(phone)
            lease: BackendClientLease | None = None
            try:
                if session is None:
                    account = await self._get_account_for_phone(phone, active_only=False)
                    if account is None:
                        continue
                    lease = await self._backend_router.acquire_client(account)
                    session = lease.session

                me = await run_with_flood_wait(
                    session.fetch_me(),
                    operation="get_users_info_fetch_me",
                    phone=phone,
                    pool=self,
                    logger_=logger,
                    timeout=15.0,
                )
                avatar_base64 = None
                if include_avatar:
                    try:
                        buf = io.BytesIO()
                        downloaded = await run_with_flood_wait(
                            session.fetch_profile_photo("me", file=buf),
                            operation="get_users_info_fetch_profile_photo",
                            phone=phone,
                            pool=self,
                            logger_=logger,
                            timeout=15.0,
                        )
                        if downloaded:
                            buf.seek(0)
                            encoded = base64.b64encode(buf.read()).decode()
                            avatar_base64 = f"data:image/jpeg;base64,{encoded}"
                    except Exception:
                        logger.debug("Failed to download avatar for %s", phone)

                result.append(
                    TelegramUserInfo(
                        phone=phone,
                        first_name=me.first_name or "",
                        last_name=me.last_name or "",
                        username=me.username,
                        is_primary=phone in primary_phones,
                        is_premium=getattr(me, "premium", False) or False,
                        avatar_base64=avatar_base64,
                    )
                )
            except Exception as e:
                logger.error("Failed to get info for %s: %s", phone, e)
            finally:
                if lease is not None:
                    await self._backend_router.release(lease)

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

        last_flood_error: HandledFloodWaitError | None = None
        for _attempt in range(3):
            result = await self.get_available_client()
            if not result:
                if last_flood_error is not None:
                    raise last_flood_error
                logger.warning("resolve_channel: no available client for '%s'", identifier)
                raise RuntimeError("no_client")
            session, phone = result
            session = adapt_transport_session(session, disconnect_on_close=False)
            try:
                entity = await run_with_flood_wait(
                    session.resolve_entity(peer),
                    operation="resolve_channel",
                    phone=phone,
                    pool=self,
                    logger_=logger,
                    timeout=30.0,
                )
                if not hasattr(entity, "title"):
                    logger.info("resolve_channel: '%s' is a user, not a channel/group", identifier)
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
            except HandledFloodWaitError as exc:
                last_flood_error = exc
                logger.info(
                    "resolve_channel: rotating client after flood wait for '%s': %s",
                    identifier,
                    exc.info.detail,
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
        if last_flood_error is not None:
            logger.warning("resolve_channel: all clients flood-waited for '%s'", identifier)
            raise last_flood_error
        logger.warning("resolve_channel: all attempts failed for '%s' (no available clients)", identifier)
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

    async def fetch_channel_meta(
        self, channel_id: int, channel_type: str | None
    ) -> dict | None:
        """Fetch about, linked_chat_id, has_comments for a channel.

        Only works for channel/supergroup/gigagroup/forum/monoforum types.
        Returns dict with keys: about, linked_chat_id, has_comments.
        Returns None on failure.
        """
        # Group type has no linked_chat_id, so skip the expensive API call
        if channel_type == "group":
            return None

        result = await self.get_available_client()
        if not result:
            logger.warning("fetch_channel_meta: no available client for channel_id %s", channel_id)
            return None

        session, phone = result
        session = adapt_transport_session(session, disconnect_on_close=False)
        try:
            entity = await run_with_flood_wait(
                session.resolve_entity(PeerChannel(channel_id)),
                operation="fetch_channel_meta",
                phone=phone,
                pool=self,
                logger_=logger,
                timeout=30.0,
            )
            full = await run_with_flood_wait(
                session.fetch_full_channel(entity),
                operation="fetch_channel_meta_full",
                phone=phone,
                pool=self,
                logger_=logger,
                timeout=30.0,
            )
            about = full.full_chat.about if full and full.full_chat else None
            linked_chat_id = (
                full.full_chat.linked_chat_id if full and full.full_chat else None
            )
            has_comments = linked_chat_id is not None
            return {
                "about": about,
                "linked_chat_id": linked_chat_id,
                "has_comments": has_comments,
            }
        except asyncio.TimeoutError:
            logger.warning(
                "fetch_channel_meta: get_entity timed out for channel_id %s", channel_id
            )
            return None
        except HandledFloodWaitError as exc:
            logger.info(
                "fetch_channel_meta: flood wait for channel_id %s: %s",
                channel_id,
                exc.info.detail,
            )
            return None
        except (ChannelPrivateError, ChatAdminRequiredError):
            logger.debug(
                "fetch_channel_meta: access denied for channel_id %s (expected for private channels)",
                channel_id,
            )
            return None
        except Exception as e:
            logger.warning(
                "fetch_channel_meta: failed to fetch full meta for channel_id %s: %s",
                channel_id,
                e,
            )
            return None
        finally:
            await self.release_client(phone)

    async def get_dialogs_for_phone(
        self,
        phone: str,
        include_dm: bool = False,
        mode: str = "channels_only",
        refresh: bool = False,
    ) -> list[dict]:
        """Get all dialogs for a specific connected account."""
        cache_mode = "full" if include_dm or mode == "full" else "channels_only"
        if refresh:
            logger.info(
                "get_dialogs_for_phone: manual refresh for %s mode=%s",
                phone,
                cache_mode,
            )
        else:
            cached = self._get_cached_dialogs(phone, cache_mode)
            if cached is not None:
                logger.info(
                    "get_dialogs_for_phone: cache hit for %s mode=%s count=%d",
                    phone,
                    cache_mode,
                    len(cached),
                )
                return cached
            db_cached = await self._get_db_cached_dialogs(phone, cache_mode)
            if db_cached is not None:
                logger.info(
                    "get_dialogs_for_phone: db cache hit for %s mode=%s count=%d",
                    phone,
                    cache_mode,
                    len(db_cached),
                )
                return db_cached
            logger.info(
                "get_dialogs_for_phone: cache miss for %s mode=%s source=telegram",
                phone,
                cache_mode,
            )

        result = await self.get_client_by_phone(phone)
        if not result:
            return []
        session, phone = result
        session = adapt_transport_session(session, disconnect_on_close=False)
        started_at = time.perf_counter()
        try:
            items: list[dict] = []
            stats = DialogFetchStats()
            try:
                me = await session.fetch_me()
                my_id: int | None = me.id
            except Exception:
                my_id = None

            async def _iter() -> None:
                async for dialog in session.stream_dialogs():
                    stats.raw_dialogs += 1
                    entity = dialog.entity
                    if dialog.is_channel or dialog.is_group:
                        channel_type, deactivate = self._classify_entity(entity)
                        if channel_type in (
                            "channel",
                            "monoforum",
                            "scam",
                            "fake",
                            "restricted",
                        ):
                            stats.channels += 1
                        if channel_type in ("supergroup", "group", "gigagroup", "forum"):
                            stats.groups += 1
                        items.append(
                            {
                                "channel_id": entity.id,
                                "title": dialog.title,
                                "username": getattr(entity, "username", None),
                                "channel_type": channel_type,
                                "deactivate": deactivate,
                                "is_own": getattr(entity, "creator", False),
                            }
                        )
                    elif include_dm or mode == "full":
                        is_bot = getattr(entity, "bot", False)
                        is_saved = my_id is not None and entity.id == my_id
                        if is_bot:
                            stats.bots += 1
                        else:
                            stats.dms += 1
                        items.append(
                            {
                                "channel_id": entity.id,
                                "title": "Избранное (Saved Messages)" if is_saved else dialog.title,
                                "username": getattr(entity, "username", None),
                                "channel_type": "saved" if is_saved else ("bot" if is_bot else "dm"),
                                "deactivate": False,
                                "is_own": False,
                            }
                        )

            iter_coro = _iter()
            try:
                await run_with_flood_wait(
                    iter_coro,
                    operation="get_dialogs_for_phone",
                    phone=phone,
                    pool=self,
                    logger_=logger,
                    timeout=60.0,
                )
            except asyncio.TimeoutError:
                iter_coro.close()
                stats.partial = True
                logger.warning(
                    "get_dialogs_for_phone: timed out for %s mode=%s, returning %d partial results",
                    phone,
                    cache_mode,
                    len(items),
                )
            elapsed_ms = int((time.perf_counter() - started_at) * 1000)
            logger.info(
                "get_dialogs_for_phone: phone=%s mode=%s duration_ms=%d "
                "raw=%d channels=%d groups=%d dms=%d bots=%d "
                "partial=%s result=%d",
                phone,
                cache_mode,
                elapsed_ms,
                stats.raw_dialogs,
                stats.channels,
                stats.groups,
                stats.dms,
                stats.bots,
                stats.partial,
                len(items),
            )
            if not stats.partial:
                self.invalidate_dialogs_cache(phone)
                self._store_cached_dialogs(phone, cache_mode, items)
                if cache_mode == "full":
                    self.mark_dialogs_fetched(phone)
                    await self._db.repos.dialog_cache.replace_dialogs(phone, items)
            return items
        finally:
            await self.release_client(phone)

    async def leave_channels(self, phone: str, dialogs: list[tuple[int, str]]) -> dict[int, bool]:
        """Leave/unsubscribe from a list of dialogs for the given account.

        dialogs: list of (channel_id, channel_type) where channel_type comes from
        get_dialogs_for_phone (e.g. "channel", "supergroup", "dm", "bot").
        """
        result = await self.get_client_by_phone(phone)
        if not result:
            return {cid: False for cid, _ in dialogs}
        session, phone = result
        session = adapt_transport_session(session, disconnect_on_close=False)
        outcomes: dict[int, bool] = {}
        try:
            for cid, ctype in dialogs:
                try:
                    peer = PeerUser(cid) if ctype in ("dm", "bot", "saved") else PeerChannel(abs(cid))
                    async def _remove_dialog() -> None:
                        entity = await session.resolve_entity(peer)
                        await session.remove_dialog(entity)

                    await run_with_flood_wait(
                        _remove_dialog(),
                        operation=f"leave_channels:{cid}",
                        phone=phone,
                        pool=self,
                        logger_=logger,
                    )
                    outcomes[cid] = True
                    await asyncio.sleep(0.3)
                except HandledFloodWaitError:
                    outcomes[cid] = False
                    for remaining_cid, _ in dialogs:
                        if remaining_cid not in outcomes:
                            outcomes[remaining_cid] = False
                    break
                except Exception as e:
                    logger.warning("leave_channels: failed for %d: %s", cid, e)
                    outcomes[cid] = False
        finally:
            await self.release_client(phone)
        self.invalidate_dialogs_cache(phone)
        await self._db.repos.dialog_cache.clear_dialogs(phone)
        return outcomes

    async def get_forum_topics(self, channel_id: int) -> list[dict]:
        """Fetch forum topics for a forum-type channel.

        Uses entity-first approach: tries get_entity(PeerChannel) directly (fast, 10s timeout).
        Falls back to get_dialogs() only if entity lookup fails with ValueError (cache miss).
        """
        result = await self.get_available_client()
        if result is None:
            logger.warning("get_forum_topics: no available client for channel %d", channel_id)
            return []
        session, phone = result
        session = adapt_transport_session(session, disconnect_on_close=False)
        try:
            # Try direct entity lookup first (works when entity is already cached)
            try:
                entity = await run_with_flood_wait(
                    session.resolve_entity(PeerChannel(channel_id)),
                    operation="get_forum_topics_resolve_channel_id",
                    phone=phone,
                    pool=self,
                    logger_=logger,
                    timeout=10.0,
                )
            except (ValueError, asyncio.TimeoutError):
                # Cache miss — populate cache via get_dialogs and retry
                if not self.is_dialogs_fetched(phone):
                    await run_with_flood_wait(
                        session.warm_dialog_cache(),
                        operation="get_forum_topics_warm_dialog_cache",
                        phone=phone,
                        pool=self,
                        logger_=logger,
                        timeout=60.0,
                    )
                    self.mark_dialogs_fetched(phone)
                    try:
                        entity = await run_with_flood_wait(
                            session.resolve_entity(PeerChannel(channel_id)),
                            operation="get_forum_topics_resolve_channel_id_after_warm",
                            phone=phone,
                            pool=self,
                            logger_=logger,
                            timeout=10.0,
                        )
                    except (ValueError, asyncio.TimeoutError):
                        entity = None
                else:
                    entity = None

                # Last resort — resolve by username from DB
                if entity is None:
                    channel = await self._db.get_channel_by_channel_id(channel_id)
                    if channel and channel.username:
                        entity = await run_with_flood_wait(
                            session.resolve_entity(channel.username),
                            operation="get_forum_topics_resolve_username",
                            phone=phone,
                            pool=self,
                            logger_=logger,
                            timeout=10.0,
                        )
                    else:
                        raise LookupError(
                            f"Cannot resolve entity for channel {channel_id}: "
                            "not in cache and no username in DB"
                        )
            response = await run_with_flood_wait(
                session.fetch_forum_topics(entity, limit=100),
                operation="get_forum_topics_fetch_topics",
                phone=phone,
                pool=self,
                logger_=logger,
                timeout=30.0,
            )
            return [
                {
                    "id": t.id,
                    "title": t.title,
                    "icon_emoji_id": getattr(t, "icon_emoji_id", None),
                    "date": (t.date.isoformat() if getattr(t, "date", None) else None),
                }
                for t in response.topics
                if hasattr(t, "title")
            ]
        except Exception as e:
            logger.warning(
                "get_forum_topics failed for channel %d: %s", channel_id, e, exc_info=True
            )
            return []
        finally:
            await self.release_client(phone)

    async def get_dialogs(self) -> list[dict]:
        """Get list of subscribed channels and groups."""
        accounts = await self._db.get_accounts(active_only=True)
        now = datetime.now(timezone.utc)
        for acc in accounts:
            flood_until = normalize_utc(acc.flood_wait_until)
            if flood_until and flood_until > now:
                continue
            if acc.phone in self.clients:
                return await self.get_dialogs_for_phone(acc.phone, mode="channels_only")
        return []
