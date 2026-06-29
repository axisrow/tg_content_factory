"""Telegram Collector facade composed from focused mixins (#1137).

The public import path stays stable: ``from src.telegram.collector import Collector``.
The hot collection, stream, stats, and cancellation behaviours live in
``src.telegram.collector_mixins`` and continue to operate on this single
``Collector`` instance/state.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime

from src.config import SchedulerConfig
from src.database import Database
from src.live_runtime_pause import LiveRuntimePauseGate
from src.telegram.client_pool import ClientPool
from src.telegram.collector_message_parse import SERVICE_ACTION_SEMANTICS
from src.telegram.collector_mixins.cancellation import CancellationMixin
from src.telegram.collector_mixins.collection import (
    NOTIFICATION_BACKLOG_LOOKBACK_HOURS,
    PERSISTED_ID_VERIFY_CHUNK_SIZE,
    CollectionMixin,
)
from src.telegram.collector_mixins.stats import StatsMixin
from src.telegram.collector_mixins.stream import (
    MESSAGE_FLUSH_BATCH_SIZE,
    STREAM_CLEANUP_TIMEOUT_SEC,
    StreamMixin,
)
from src.telegram.collector_resolve import (
    RESOLVE_USERNAME_OPERATION as RESOLVE_USERNAME_OPERATION,
)
from src.telegram.collector_resolve import ResolveOutcome
from src.telegram.collector_types import (
    _ACQUIRE_RETRY,
    AllCollectionClientsFloodedError,
    AllStatsClientsFloodedError,
    NoActiveCollectionClientsError,
    NoActiveStatsClientsError,
    _format_channel_log_name,
    _StreamOutcome,
)
from src.telegram.notifier import Notifier
from src.telegram.rate_limiter import (
    RESOLVE_USERNAME_BACKOFF_BUFFER_SEC as RESOLVE_USERNAME_BACKOFF_BUFFER_SEC,
)
from src.telegram.rate_limiter import (
    UsernameResolveFloodWaitDeferredError,
    UsernameResolveRateLimitedError,
)

logger = logging.getLogger(__name__)

# Backward-compatible alias: the resolve outcome and its logic now live in
# ``collector_resolve`` (#1045). Kept under the historical private name so
# existing references and the rich docstring there remain the single source.
_ResolveOutcome = ResolveOutcome

__all__ = [
    "AllCollectionClientsFloodedError",
    "AllStatsClientsFloodedError",
    "Collector",
    "MESSAGE_FLUSH_BATCH_SIZE",
    "NOTIFICATION_BACKLOG_LOOKBACK_HOURS",
    "NoActiveCollectionClientsError",
    "NoActiveStatsClientsError",
    "PERSISTED_ID_VERIFY_CHUNK_SIZE",
    "RESOLVE_USERNAME_BACKOFF_BUFFER_SEC",
    "RESOLVE_USERNAME_OPERATION",
    "STREAM_CLEANUP_TIMEOUT_SEC",
    "UsernameResolveFloodWaitDeferredError",
    "UsernameResolveRateLimitedError",
    "_ACQUIRE_RETRY",
    "_ResolveOutcome",
    "_StreamOutcome",
    "_format_channel_log_name",
]


class Collector(CollectionMixin, StatsMixin, StreamMixin, CancellationMixin):
    # Kept as a class attribute for backward compatibility; the canonical
    # mapping now lives in ``collector_message_parse`` (#1045).
    _SERVICE_ACTION_SEMANTICS = SERVICE_ACTION_SEMANTICS

    def __init__(
        self,
        pool: ClientPool,
        db: Database,
        config: SchedulerConfig,
        notifier: Notifier | None = None,
        *,
        live_runtime_pause_gate: LiveRuntimePauseGate | None = None,
    ):
        self._pool = pool
        self._db = db
        self._config = config
        self._notifier = notifier
        self._live_runtime_pause_gate = live_runtime_pause_gate
        self._active_collection_count = 0
        self._stats_running = False
        self._stats_all_running = False
        self._cancel_event = asyncio.Event()
        # Stats-only stop signal. STATS_ALL cancellation must NOT use the global
        # _cancel_event, which channel-collect workers also watch — sharing it let
        # a STATS_ALL cancel abort unrelated in-flight collection (audit #835/6).
        self._stats_cancel_event = asyncio.Event()
        self._lock = asyncio.Lock()
        self._stats_lock = asyncio.Lock()
        self._stats_all_lock = asyncio.Lock()
        self._last_unavailability_log: tuple[str, str | int | None, datetime | None] | None = None
