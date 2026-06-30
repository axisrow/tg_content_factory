"""Client lifecycle, acquisition, and auth/StringSession wiring (#1046).

Extracted from the ``ClientPool`` monolith as a composition mixin. Covers the
connection lifecycle of per-account Telegram sessions and the account-leasing
machinery the rest of the pool builds on:

* **lifecycle** — ``initialize`` / ``add_client`` / ``remove_client`` /
  ``disconnect_all`` / ``reconnect_phone`` / ``force_reconnect_phone`` and the
  MTProto security-watchdog reconnect callback (#556).
* **acquisition** — ``get_available_client`` (flood-wait rotation entry point),
  ``get_client_by_phone`` / ``get_native_client_by_phone``, the lease-stack
  ``release_client`` (LIFO teardown, #838/#868), and the internal
  ``_acquire_from_lease`` / ``_acquire_phone_lease`` helpers.
* **auth / StringSession** — ``_connect_account`` materialises a stored session
  through the backend router; ``add_client`` registers a freshly authed
  StringSession (``_session_overrides``). The pool↔DB ordering invariant
  (``db.add_account`` before ``pool.add_client``, #449) lives in the callers;
  this mixin keeps ``add_client`` side-effect-symmetric with ``remove_client``.
* **primary selection** — ``get_users_info`` reports the primary account first,
  tied to the is-primary partial unique index (#733).

Behaviour is unchanged by the split: the same methods run on the same single
``self`` and mutate the same per-instance attributes the test harnesses poke at
(``clients``, ``_in_use``, ``_active_leases``, ``_session_overrides`` …).
"""

from __future__ import annotations

import asyncio
import base64
import io
import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING, cast

from src.database.live_accounts import load_live_usable_accounts
from src.models import Account, TelegramUserInfo
from src.telegram.account_lease_pool import AccountLease
from src.telegram.backends import BackendClientLease, TelegramTransportSession
from src.telegram.flood_wait import run_with_flood_wait
from src.telegram.utils import normalize_utc
from src.utils.safe_logging import mask_phone

if TYPE_CHECKING:
    from collections.abc import Iterable

    from src.database import Database
    from src.telegram.account_lease_pool import AccountLeasePool
    from src.telegram.backends import BackendRouter
    from src.telegram.mtproto_watchdog import MTProtoSecurityWatchdog

logger = logging.getLogger(__name__)

REMOVE_CLIENT_DISCONNECT_TIMEOUT_SEC = 5.0


class ClientLifecycleMixin:
    """Connection lifecycle, lease acquisition/release, auth, primary selection.

    A composition mixin for ``ClientPool``; relies on state the concrete pool
    initialises in ``__init__``. The annotations below declare that contract for
    the type checker (mirrors ``ResolveGuardMixin``).
    """

    # Provided by ClientPool.__init__.
    _db: Database
    _lock: asyncio.Lock
    _in_use: set[str]
    _lease_pool: AccountLeasePool
    _backend_router: BackendRouter
    _session_overrides: dict[str, str]
    _active_leases: dict[str, list[BackendClientLease]]
    _dialogs_fetched: set[str]
    clients: dict[str, object]
    init_timeout: float

    if TYPE_CHECKING:
        # Optional / lazily-present attributes (test doubles may build the pool
        # via __new__); accessed through getattr() at runtime but declared here
        # for the type checker.
        _mtproto_watchdog: MTProtoSecurityWatchdog
        _warming_task: asyncio.Task | None
        _dialog_refresh_tasks: dict[tuple[str, str], asyncio.Task[list[dict]]]

        def invalidate_dialogs_cache(self, phone: str | None = None) -> None: ...

        def reset_dialogs_warm(self, phone: str) -> None: ...

        def clear_premium_flood(self, phone: str) -> None: ...

        async def _await_transient_flood(self, phone: str) -> None: ...

        # NB: force_reconnect_phone is a *real* method of this mixin (defined
        # below), so it must NOT be re-declared as a stub here — a stub plus the
        # real def is a mypy no-redef error within the same class (#1046).

        # Signature mirrors ResolveGuardMixin exactly: ClientPool inherits all
        # four mixins, so a divergent stub here would make mypy flag the method
        # as incompatibly redefined across base classes (#1046).
        async def restore_resolve_username_backoff(
            self, db: object, *, phones: Iterable[str] | None = None
        ) -> None: ...

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

    async def initialize(self, *, phones: Iterable[str] | None = None) -> None:
        """Load active accounts and validate that their sessions are usable."""
        watchdog = getattr(self, "_mtproto_watchdog", None)
        if watchdog is not None:
            watchdog.install(asyncio.get_running_loop())
        accounts = await load_live_usable_accounts(self._db, active_only=True)
        # Restore after loading accounts: the legacy single-deadline migration
        # needs the known phones to apply the old global backoff per-phone.
        await self.restore_resolve_username_backoff(
            self._db, phones=[acc.phone for acc in accounts]
        )
        if phones is not None:
            allowed_phones = {str(phone) for phone in phones if str(phone)}
            accounts = [acc for acc in accounts if acc.phone in allowed_phones]
        new_accounts = [acc for acc in accounts if acc.phone not in self.clients]
        if not new_accounts:
            return

        async def _connect_one(acc: Account) -> BackendClientLease | None:
            try:
                lease = await self._connect_account(acc)
                logger.info("Connected account: %s (primary=%s)", acc.phone, acc.is_primary)
                return lease
            except Exception as e:
                logger.error("Failed to connect %s: %s", acc.phone, e)
                return None

        tasks = {asyncio.create_task(_connect_one(acc)): acc for acc in new_accounts}
        done, pending = await asyncio.wait(tasks.keys(), timeout=self.init_timeout)
        for task in done:
            acc = tasks[task]
            lease = task.result()
            if lease is None:
                continue
            try:
                me = await asyncio.wait_for(lease.session.fetch_me(), timeout=5.0)
                is_premium = bool(getattr(me, "premium", False))
                if is_premium != acc.is_premium:
                    await self._db.update_account_premium(acc.phone, is_premium)
            except Exception as e:
                logger.warning("Failed to fetch premium status for %s: %s", acc.phone, e)
            finally:
                await self._backend_router.release(lease)
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
                        session = cast(TelegramTransportSession, self.clients[phone])
                        # close() is a no-op (disconnect_on_close=False), so
                        # disconnect the underlying Telethon client directly to
                        # cancel its background tasks (_send_loop, _recv_loop).
                        await asyncio.wait_for(
                            session.raw_client.disconnect(), timeout=3.0,
                        )
                    except Exception:
                        pass
                    del self.clients[phone]

    async def get_available_client(
        self, *, exclude_phones: set[str] | frozenset[str] = frozenset()
    ) -> tuple[TelegramTransportSession, str] | None:
        """Get first available client not in flood wait. Returns (client, phone) or None.

        ``exclude_phones`` skips specific accounts — used by the collector to
        rotate a username resolve away from accounts already in resolve
        backoff (#790).
        """
        for _ in range(max(1, len(self.clients))):
            candidates = self._connected_phones() - set(exclude_phones)
            if not candidates:
                return None
            lease = await self._lease_pool.acquire_available(candidates)
            if lease is None:
                return None
            result = await self._acquire_from_lease(lease)
            if result is not None:
                return result
        return None

    async def get_client_by_phone(
        self,
        phone: str,
        *,
        wait_for_flood: bool = False,
    ) -> tuple[TelegramTransportSession, str] | None:
        """Get a specific active connected client when it is not flood-waited.

        wait_for_flood=True sleeps out a transient (<=60s) flood-wait on the phone
        instead of returning None immediately — for write callers that pin a phone.
        """
        lease = await self._acquire_phone_lease(phone, wait_for_flood=wait_for_flood)
        if lease is None:
            return None
        return await self._acquire_from_lease(lease)

    async def get_native_client_by_phone(
        self,
        phone: str,
        *,
        wait_for_flood: bool = False,
    ) -> tuple[TelegramTransportSession, str] | None:
        """Get a specific flood-aware client through the native backend for stateful flows."""
        lease = await self._acquire_phone_lease(phone, wait_for_flood=wait_for_flood)
        if lease is None:
            return None
        result = await self._acquire_from_lease(lease, force_native=True)
        if result is None:
            return None
        return result

    async def release_client(self, phone: str) -> None:
        """Mark client as no longer in active use.

        When the lease stack mixes an ephemeral native lease
        (disconnect_on_release=True) and a direct-pool lease, prefer releasing the
        disconnect-on-release one so the native client/connection is torn down
        promptly instead of lingering until an unrelated caller releases (#838/8).
        """
        lease = None
        async with self._lock:
            stack = self._active_leases.get(phone)
            if stack:
                # Strict LIFO: release the most-recently-acquired lease for this phone.
                # release_client takes only a phone (not a lease handle), so it cannot know
                # WHICH caller is finishing. Scanning the stack for any disconnect_on_release
                # lease could pop a native lease still in use by another caller while a live
                # direct lease sits on top — closing that session mid-operation (#868 review).
                # The leak this scan targeted (native acquired LAST) is already covered by
                # LIFO: a native lease acquired last is on top, so pop() tears it down promptly.
                lease = stack.pop()
                if not stack:
                    self._active_leases.pop(phone, None)
            # Drop the exclusive _in_use marker in the SAME critical section that
            # pops the lease stack, holding ClientPool._lock across the nested
            # _lease_pool.release (which takes AccountLeasePool._lock). With both
            # locks held there is no window in which a concurrent acquirer — the
            # fallback in _acquire_phone_lease (ClientPool._lock) OR
            # AccountLeasePool.acquire_by_phone (AccountLeasePool._lock) — can
            # observe a stale exclusive marker, grab a shared lease, and then let
            # a later caller take exclusive once this discard completes: that
            # sequence puts shared+exclusive on one session concurrently (#1181).
            # Lock order ClientPool._lock -> AccountLeasePool._lock has no reverse
            # holder (AccountLeasePool never calls back into ClientPool), so the
            # nesting cannot deadlock.
            if not self._active_leases.get(phone):
                await self._lease_pool.release(phone)
        if lease is not None and lease.disconnect_on_release:
            await self._backend_router.release(lease)

    async def add_client(self, phone: str, session_string: str) -> None:
        """Register a new account as connected and validate its stored session."""
        self._session_overrides[phone] = session_string
        # Re-auth with a (possibly new) StringSession starts from an empty
        # in-memory entity cache, so any prior warm flag is stale (#1043).
        self.reset_dialogs_warm(phone)
        self.invalidate_dialogs_cache(phone)
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
            # NB: no warm-flag reset here. A reconnect reuses the *same*
            # session object, and the StringSession entity cache
            # (``session._entities``, the fallback that resolves numeric
            # PeerChannels) lives in that object and survives disconnect +
            # connect. Re-warming would be a needless round-trip and a
            # FloodWait risk (#1043). Long-lived drift is covered by the
            # warm-flag TTL instead.
            return client.is_connected()
        except Exception:
            logger.exception("Failed to reconnect client for %s", phone)
            return False

    async def force_reconnect_phone(self, phone: str) -> bool:
        """Disconnect and reconnect a client even when it looks connected.

        Needed for the MTProto security brick (#556): the transport stays
        formally connected while every incoming message is dropped, so
        :meth:`reconnect_phone`'s ``is_connected()`` guard never fires.
        """
        session = self.clients.get(phone)
        if session is None:
            return False
        try:
            client = session.raw_client
            try:
                await asyncio.wait_for(
                    client.disconnect(), timeout=REMOVE_CLIENT_DISCONNECT_TIMEOUT_SEC
                )
            except asyncio.TimeoutError:
                logger.warning("Timeout disconnecting %s during force reconnect", mask_phone(phone))
            await client.connect()
            # NB: no warm-flag reset here either. force_reconnect (the #556
            # MTProto-brick recovery) tears down and re-establishes the
            # transport on the *same* session object, so the StringSession
            # entity cache (``session._entities``) survives — numeric
            # PeerChannel resolves keep working without a re-warm (#1043).
            if not await client.is_user_authorized():
                logger.error("Force reconnect of %s: session no longer authorized", mask_phone(phone))
                return False
            return bool(client.is_connected())
        except Exception:
            logger.exception("Force reconnect failed for %s", mask_phone(phone))
            return False

    async def _on_mtproto_security_brick(self, phone: str) -> None:
        """Watchdog callback: the client is dropping every incoming message."""
        logger.warning(
            "MTProto security errors detected on %s — forcing reconnect",
            mask_phone(phone),
        )
        ok = await self.force_reconnect_phone(phone)
        logger.warning(
            "MTProto watchdog reconnect of %s %s",
            mask_phone(phone),
            "succeeded" if ok else "FAILED",
        )

    def get_mtproto_watchdog_stats(self) -> dict[str, int]:
        """Reconnects the watchdog has triggered, keyed by phone."""
        return self._mtproto_watchdog.get_stats()

    async def remove_client(self, phone: str) -> None:
        # getattr: test doubles build the pool via __new__ without __init__.
        watchdog = getattr(self, "_mtproto_watchdog", None)
        if watchdog is not None:
            watchdog.unregister_phone(phone)
        self._session_overrides.pop(phone, None)
        async with self._lock:
            leases = list(self._active_leases.pop(phone, []))
            client = self.clients.pop(phone, None)
            await self._lease_pool.release(phone)
        self.reset_dialogs_warm(phone)
        self.invalidate_dialogs_cache(phone)
        self.clear_premium_flood(phone)
        for lease in reversed(leases):
            if lease.disconnect_on_release:
                try:
                    await asyncio.wait_for(
                        self._backend_router.release(lease),
                        timeout=REMOVE_CLIENT_DISCONNECT_TIMEOUT_SEC,
                    )
                except asyncio.TimeoutError:
                    logger.warning("Timeout releasing live lease for %s", phone)
                except Exception:
                    logger.debug("Failed to release live lease for %s", phone, exc_info=True)
        if isinstance(client, TelegramTransportSession):
            try:
                await asyncio.wait_for(
                    client.raw_client.disconnect(),
                    timeout=REMOVE_CLIENT_DISCONNECT_TIMEOUT_SEC,
                )
            except asyncio.TimeoutError:
                logger.warning("Timeout disconnecting session for %s", phone)
            except Exception:
                logger.debug("Failed to disconnect session for %s", phone, exc_info=True)

    async def disconnect_all(self) -> None:
        # Detach the watchdog handler from the global telethon.tgcf logger so
        # a torn-down pool is not kept alive by it (#817 review F1); a later
        # initialize() re-installs it. getattr: doubles built via __new__.
        watchdog = getattr(self, "_mtproto_watchdog", None)
        if watchdog is not None:
            watchdog.uninstall()
        # Cancel long-lived background tasks the pool spawned (warm_all_dialogs,
        # per-(phone,mode) dialog refresh) so they don't keep running — and
        # operating on a disconnected client — after teardown (audit #836/11).
        bg_tasks: list[asyncio.Task] = []
        warming = getattr(self, "_warming_task", None)
        if warming is not None and not warming.done() and warming is not asyncio.current_task():
            warming.cancel()
            bg_tasks.append(warming)
        refresh_tasks = getattr(self, "_dialog_refresh_tasks", None)
        if isinstance(refresh_tasks, dict):
            for task in list(refresh_tasks.values()):
                if not task.done():
                    task.cancel()
                    bg_tasks.append(task)
            refresh_tasks.clear()
        if bg_tasks:
            await asyncio.gather(*bg_tasks, return_exceptions=True)
        had_clients = bool(self.clients)
        for phone in list(self.clients):
            try:
                await asyncio.wait_for(self.remove_client(phone), timeout=5.0)
            except asyncio.TimeoutError:
                logger.warning("Timeout disconnecting %s, forcing cleanup", phone)
                async with self._lock:
                    self.clients.pop(phone, None)
                    self._active_leases.pop(phone, None)
                    await self._lease_pool.release(phone)
                self.reset_dialogs_warm(phone)
            except Exception:
                logger.debug("Error disconnecting %s", phone, exc_info=True)
        # Allow Telethon internal tasks (_send_loop, _recv_loop) to finish
        if had_clients:
            await asyncio.sleep(0.25)

    async def _acquire_phone_lease(
        self, phone: str, *, wait_for_flood: bool = False
    ) -> AccountLease | None:
        if wait_for_flood:
            await self._await_transient_flood(phone)

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
        report_generic_flood: bool = True,
    ) -> tuple[TelegramTransportSession, str] | None:
        phone = account_lease.account.phone
        # force_native bypasses the persistent pool session — callers need a raw native client
        direct_session = None if force_native else self._direct_session(phone)

        # Auto-reconnect: if the cached session's connection dropped (Stream closed),
        # attempt to reconnect before using it; fall back to backend on failure.
        if direct_session is not None and not direct_session.raw_client.is_connected():
            logger.warning("Client for %s disconnected, attempting auto-reconnect", phone)
            try:
                await direct_session.raw_client.connect()
            except Exception:
                logger.exception("Auto-reconnect failed for %s, falling back to backend", phone)
                self.clients.pop(phone, None)
                direct_session = None

        lease: BackendClientLease | None = None
        try:
            if direct_session is not None:
                context_session = direct_session.with_flood_context(
                    phone=phone,
                    pool=self,
                    logger_=logger,
                    report_flood_wait=report_generic_flood,
                )
                lease = BackendClientLease(
                    phone=phone,
                    session=context_session,
                    backend_name="direct",
                    disconnect_on_release=False,
                )
            else:
                lease = await self._backend_router.acquire_client(
                    account_lease.account,
                    force_native=force_native,
                )
                lease.session = lease.session.with_flood_context(
                    phone=phone,
                    pool=self,
                    logger_=logger,
                    report_flood_wait=report_generic_flood,
                )
                if not force_native:
                    # Store persistent session for future direct reuse.
                    # force_native sessions are short-lived and must not replace the pool session.
                    #
                    # This branch installs a *fresh* backend client (the cached
                    # direct session was absent or its auto-reconnect failed
                    # above): a new session object with an empty in-memory entity
                    # cache. Drop the warm flag so the next numeric-PeerChannel
                    # collection re-warms instead of skipping against the empty
                    # cache (#1043) — same hazard as add_client's re-auth.
                    self.reset_dialogs_warm(phone)
                    self.clients[phone] = TelegramTransportSession(
                        lease.session.raw_client,
                        disconnect_on_close=False,
                        phone=phone,
                        pool=self,
                        logger_=logger,
                    )
                    lease.disconnect_on_release = False

            async with self._lock:
                self._active_leases[phone].append(lease)
            return lease.session, phone
        except Exception as exc:
            logger.error("Failed to acquire client for %s: %s", phone, exc)
            if not account_lease.shared:
                async with self._lock:
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
        lease.session = lease.session.with_flood_context(
            phone=account.phone,
            pool=self,
            logger_=logger,
        )
        # Store persistent transport session so _direct_session() can reuse the connection
        self.clients[account.phone] = TelegramTransportSession(
            lease.session.raw_client,
            disconnect_on_close=False,
            phone=account.phone,
            pool=self,
            logger_=logger,
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
        accounts = await load_live_usable_accounts(self._db, active_only=True)
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
