"""Telegram client pool — composition of the #1046 decomposition mixins.

``ClientPool`` was a tier-1 hot-zone monolith (flood-wait rotation, primary-race
#733, StringSession auth, pool↔DB race ordering #449, the entity-cache machinery).
Issue #1046 split its responsibilities into three composition mixins, each owning
one cohesive cluster of the old class:

* :class:`~src.telegram.pool_dialogs.DialogsMixin` — dialog cache, entity
  resolution, channel↔phone routing, dialog fetch (``resolve_channel`` /
  ``fetch_channel_meta`` / ``get_dialogs_for_phone`` / ``warm_all_dialogs`` …).
* :class:`~src.telegram.pool_lifecycle.ClientLifecycleMixin` — connection
  lifecycle, lease acquisition/release, auth/StringSession wiring, primary
  selection (``initialize`` / ``add_client`` / ``get_available_client`` …).
* :class:`~src.telegram.pool_flood.FloodRotationMixin` — generic + premium
  flood-wait reporting and availability queries (``report_flood`` /
  ``get_premium_client`` / ``next_resolve_capable_at`` …).

plus the pre-existing :class:`~src.telegram.resolve_guard.ResolveGuardMixin`
(live-username-resolve FloodWait guard, #785/#790).

The decomposition is behaviour-preserving: every method runs on the same single
``self`` and mutates the same per-instance attributes the test harnesses poke at
(``_in_use``, ``_active_leases``, ``_lease_pool``, ``_session_overrides`` …).
This module keeps only what is NOT a behaviour cluster: the dataclasses, the
module-level timeout constants, ``__init__`` (the single source of all instance
state the mixins read), and ``_normalize_runtime_config``.

The mixin modules import their own collaborators (``run_with_flood_wait``,
``adapt_transport_session``, ``load_live_usable_accounts`` …) and own the
timeout constants (``WARM_*`` / ``REMOVE_CLIENT_DISCONNECT_TIMEOUT_SEC``) in
their own namespaces; tests that ``patch``/``monkeypatch`` such a name must
target the module that owns the call site (``pool_dialogs`` / ``pool_lifecycle``
/ ``pool_flood``), not ``client_pool``. The few re-exports below
(``adapt_transport_session``,
``load_live_usable_accounts``, ``ChannelForbidden``) keep the historical
``from src.telegram.client_pool import <name>`` imports working.

Lease lifecycle contract: a phone has at most one exclusive reservation;
shared leases may coexist with that reservation for pinned read paths. Popping
``_active_leases`` and clearing ``_in_use`` is one critical section with lock
order ``ClientPool._lock`` then ``AccountLeasePool._lock``; every successful
acquire is released by its owner; and no client operation may start after
``disconnect_all`` begins teardown.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime

# Re-exported for backwards-compatible ``from src.telegram.client_pool import …``
# and ``patch("src.telegram.client_pool.<name>")`` call sites that predate the
# #1046 split. The live call sites now live in the mixin modules, which import
# these names into their own namespaces.
from telethon.tl.types import ChannelForbidden  # noqa: F401
from telethon_floodgate import (
    FloodCircuitBreaker,
    RateLimitSpec,
    ResolveRateLimiter,
    TelegramRateLimitGate,
)

from src.config import TelegramRuntimeConfig
from src.database import Database
from src.database.live_accounts import load_live_usable_accounts  # noqa: F401
from src.models import Account, TelegramUserInfo  # noqa: F401
from src.telegram.account_lease_pool import AccountLeasePool
from src.telegram.auth import TelegramAuth
from src.telegram.backends import (
    BackendClientLease,
    BackendRouter,
    NativeTelethonBackend,
    TelethonCliBackend,
    adapt_transport_session,  # noqa: F401
)
from src.telegram.mtproto_watchdog import MTProtoSecurityWatchdog
from src.telegram.outgoing_ceiling import OutgoingRateCeiling
from src.telegram.pool_dialogs import (
    DialogCacheEntry,
    DialogFetchStats,  # noqa: F401
    DialogsMixin,
)
from src.telegram.pool_flood import FloodRotationMixin
from src.telegram.pool_lifecycle import ClientLifecycleMixin
from src.telegram.resolve_guard import ResolveGuardMixin
from src.telegram.session_materializer import SessionMaterializer

logger = logging.getLogger(__name__)

# The dialog/lifecycle timeout constants (WARM_* / REMOVE_CLIENT_DISCONNECT_TIMEOUT_SEC)
# now live only in the module that uses them (``pool_dialogs`` / ``pool_lifecycle``);
# a test that needs to shrink a timeout patches that owning module. They are NOT
# duplicated here — nothing reads ``client_pool.<CONST>`` any more (#1046 cleanup).


# Phase 2 calibration of the gate's ``history`` category (#1418, epic #1331).
# The released telethon-floodgate 0.1.0 default (600/min) never bound: the
# production app.log shows 209 FLOOD_WAITs on messages.getHistory with
# collector peaks of 115 channel fetches per minute, far below that guard.
# 24/30s is the empirically measured Telegram boundary (30 requests in ~30s,
# the 31st returned FLOOD_WAIT_3) with a 20% margin.  Peak collector bursts
# (p95 74-101 fetches/min) stretch instead of flooding; incremental min_id
# collection catches deferred channels on the next pass.  Other categories
# had no flood signal in the logs and keep the package defaults pending a
# larger sample.  Keep in sync with the package default; drop the override
# once a released telethon-floodgate ships this value (#1418).
HISTORY_CALIBRATED_SPEC = RateLimitSpec(max_calls=24, window_sec=30.0)

# Process-wide outgoing ceiling (#1417).  The per-account
# gate buckets above are independent, so N accounts can fire N synchronized
# bursts in the same second with every account inside its own limit — the
# 2026-08-26 production log shows four accounts flood-waited together at
# 21:00:15.  One shared window paces ALL outgoing transport calls in this
# process and bound sessions WAIT on it instead of refusing.  PLACEHOLDER
# pending calibration (epic #1331 step 0.2, not a final value): 120/30s
# leaves a single account ~2.5x headroom over its calibrated history pace
# (24/30s), so legit stream_messages pagination never defers, while the
# observed restart burst (656 calls in second 0, #1419) stretches over
# ~2.7 min and a ten-account 300/min send volley is capped at ~4 calls/s.
PROCESS_OUTGOING_SPEC = RateLimitSpec(max_calls=120, window_sec=30.0)


@dataclass(frozen=True)
class StatsClientAvailability:
    state: str  # "available" | "all_flooded" | "no_connected_active"
    retry_after_sec: int | None = None
    next_available_at_utc: datetime | None = None


class ClientPool(
    DialogsMixin,
    ClientLifecycleMixin,
    FloodRotationMixin,
    ResolveGuardMixin,
):
    """Pool of Telegram clients with fallback rotation on flood waits.

    A thin composition of the #1046 mixins: this class owns only the construction
    of the shared per-instance state below; every public/private behaviour method
    is provided by one of the mixins and operates on this single ``self``.
    """

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
        self.init_timeout: float = 45.0
        self._lock = asyncio.Lock()
        self._disconnecting = False
        self._in_use: set[str] = set()
        self._lease_pool = AccountLeasePool(db, self._in_use)
        self._session_overrides: dict[str, str] = {}
        self._active_leases: dict[str, list[BackendClientLease]] = defaultdict(list)
        self._materializer = SessionMaterializer(self._runtime_config.session_cache_dir)
        # MTProto security watchdog (#556): per-phone Telethon loggers let it
        # attribute "Security error while unpacking" warnings and force a
        # reconnect of the silently-bricked client.
        self._mtproto_watchdog = MTProtoSecurityWatchdog(self._on_mtproto_security_brick)
        self._native_backend = NativeTelethonBackend(
            auth, client_logger_provider=self._mtproto_watchdog.register_phone
        )
        self._primary_backend = TelethonCliBackend(
            auth,
            self._materializer,
            transport=self._runtime_config.cli_transport,
            client_logger_provider=self._mtproto_watchdog.register_phone,
        )
        self._backend_router = BackendRouter(
            mode=self._runtime_config.backend_mode,
            primary=self._primary_backend,
            native=self._native_backend,
        )
        self._dialogs_fetched: set[str] = set()
        # phone → monotonic timestamp of its last warm; gives the warm flag a
        # TTL so a long-lived worker re-warms a stale entity cache instead of
        # treating it as "warm forever" (#1043). Membership in
        # ``_dialogs_fetched`` stays the source of truth for "was warmed";
        # this map only carries the age.
        self._dialogs_fetched_at_monotonic: dict[str, float] = {}
        # Warm-flag TTL. Distinct from ``_dialogs_cache_ttl_sec`` (60s, for the
        # in-process list-of-dialogs cache below): the warm flag only gates the
        # one-shot entity-cache prefetch, so it tolerates a much longer window
        # before a defensive re-warm.
        self._dialogs_warm_ttl_sec = 1800.0  # 30 min
        # Injectable monotonic clock (tests advance it without sleeping).
        self._monotonic = time.monotonic
        self._channel_phone_map: dict[int, str] = {}
        # channel_id (positive MTProto) → phone that has it in dialogs
        self._warming_task: asyncio.Task | None = None
        self._dialogs_cache: dict[tuple[str, str], DialogCacheEntry] = {}
        self._dialogs_cache_ttl_sec = 60.0
        self._dialogs_db_cache_ttl_sec = 3600.0  # 1 hour; stale DB cache triggers fresh Telegram fetch
        self._dialog_refresh_tasks: dict[tuple[str, str], asyncio.Task[list[dict]]] = {}
        self._premium_flood_wait_until: dict[str, datetime] = {}
        self._resolve_rate_limiter = ResolveRateLimiter()
        # Central proactive gate.  ``history`` is calibrated from the
        # production log sample (#1418); the remaining categories are the
        # package's conservative operating defaults pending production
        # signals — keep the registry injectable for future recalibration
        # (#1331).
        self._rate_limit_gate = TelegramRateLimitGate(
            category_limits={"history": HISTORY_CALIBRATED_SPEC},
        )
        # Reactive counterpart to the gate (#1330/#1368): the gate paces calls
        # against guessed limits, the breaker stops an (operation, phone) pair
        # that Telegram is already flood-waiting instead of hammering on.
        self._flood_breaker = FloodCircuitBreaker()
        # Process-wide ceiling over ALL outgoing calls (#1417): the per-account
        # gate buckets above cannot see an aggregate volley, this one can.
        self._outgoing_ceiling = OutgoingRateCeiling(PROCESS_OUTGOING_SPEC)
        self._resolve_username_backoff_until_utc: dict[str, datetime] = {}
        self._resolve_ramp_up_until_utc: dict[str, datetime] = {}
        self._resolve_ramp_up_last_call_utc: dict[str, datetime] = {}
        self._resolve_ramp_up_min_interval_sec: float = 5.0

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
