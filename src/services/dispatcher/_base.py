"""Shared attribute/method surface for the dispatcher mixins (#1047).

The per-domain mixins read collaborators (``self._db``, ``self._pool``, …) that
are assigned in :class:`TelegramCommandDispatcher.__init__`, and a few call
handlers that live on a *different* mixin (e.g. auth.verify_code chains into
accounts.connect). To keep type-checkers honest without a runtime base class,
each mixin inherits from :class:`_DispatcherProtocol` only under
``TYPE_CHECKING``; at runtime the mixins are plain classes composed by the
facade, exactly as before.
"""

from __future__ import annotations

from datetime import datetime
from typing import TYPE_CHECKING, Any, Protocol

if TYPE_CHECKING:
    from src.config import AppConfig
    from src.database import Database
    from src.live_runtime_pause import LiveRuntimePauseGate
    from src.scheduler.service import SchedulerManager
    from src.search.engine import SearchEngine
    from src.telegram.auth import TelegramAuth
    from src.telegram.client_pool import ClientPool
    from src.telegram.collector import Collector
    from src.telegram.notifier import Notifier


class _DispatcherProtocol(Protocol):
    """The collaborator surface every mixin can assume the dispatcher provides.

    Used only for static typing — never instantiated, never inherited at
    runtime. Mirrors :meth:`TelegramCommandDispatcher.__init__`.
    """

    _db: "Database"
    _pool: "ClientPool"
    _config: "AppConfig | None"
    _collector: "Collector | None"
    _scheduler: "SchedulerManager | None"
    _auth: "TelegramAuth | None"
    _search_engine: "SearchEngine | None"
    _collection_queue: Any
    _live_runtime_pause_gate: "LiveRuntimePauseGate | None"
    _notifier: "Notifier | None"
    _last_reaction_at_monotonic: dict[str, float]

    # Cross-mixin / facade helpers a handler may delegate to.
    async def _handle_accounts_connect(self, payload: dict[str, Any]) -> dict[str, Any]: ...

    def _pool_method(self, name: str) -> Any | None: ...

    async def _account_flood_until(self, phone: str) -> datetime | None: ...

    async def _reaction_min_interval(self) -> float: ...

    async def _ensure_reaction_can_run(self, phone: str) -> None: ...

    def _notification_service(self) -> Any: ...

    def _notification_target_service(self) -> Any: ...

    def _photo_task_service(self) -> Any: ...

    def _photo_auto_upload_service(self) -> Any: ...
