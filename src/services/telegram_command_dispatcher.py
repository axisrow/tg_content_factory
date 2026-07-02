from __future__ import annotations

import asyncio
import logging
import time
from datetime import timedelta
from pathlib import Path
from typing import TYPE_CHECKING, Any

from src.config import AppConfig
from src.database import Database, DatabaseBusyError
from src.live_runtime_pause import LiveRuntimePauseGate
from src.models import RuntimeSnapshot, TelegramCommandStatus  # noqa: F401  (RuntimeSnapshot re-exported for tests)
from src.scheduler.service import SchedulerManager
from src.services.dispatcher._constants import (
    COMMAND_STATUS_UPDATE_BUSY_RETRY_INITIAL_SEC,
    COMMAND_STATUS_UPDATE_BUSY_RETRY_MAX_SEC,
    DEFAULT_REACTION_MIN_INTERVAL_SEC,
    REACTION_MIN_INTERVAL_CEILING_SEC,
    REACTION_MIN_INTERVAL_FLOOR_SEC,
    REACTION_MIN_INTERVAL_SETTING,
)
from src.services.dispatcher._errors import TelegramCommandRetryLaterError
from src.services.dispatcher.accounts_mixin import AccountsCommandsMixin
from src.services.dispatcher.auth_mixin import AuthCommandsMixin
from src.services.dispatcher.channels_mixin import ChannelsCommandsMixin
from src.services.dispatcher.dialogs_mixin import DialogsCommandsMixin
from src.services.dispatcher.moderation_mixin import ModerationCommandsMixin
from src.services.dispatcher.notifications_mixin import NotificationsCommandsMixin
from src.services.dispatcher.photo_mixin import PhotoCommandsMixin
from src.services.dispatcher.scheduler_mixin import SchedulerCommandsMixin
from src.services.dispatcher.search_mixin import SearchCommandsMixin
from src.services.notification_target_service import NotificationTargetService
from src.services.telegram_actions import (
    TelegramActionMessageNotFoundError,
    TelegramActionNoMediaError,
    TelegramActionPathEscapeError,
    TelegramActionService,
)
from src.telegram.auth import TelegramAuth
from src.telegram.client_pool import ClientPool
from src.telegram.collector import Collector
from src.telegram.flood_wait import HandledFloodWaitError
from src.telegram.notifier import Notifier
from src.telegram.reactions import TelegramReactionInvalidError
from src.utils.safe_logging import elapsed_ms, query_log_fields

try:  # telethon is an optional dependency at test-time
    from telethon.errors import ReactionInvalidError
except ImportError:  # pragma: no cover
    class ReactionInvalidError(Exception):  # type: ignore[no-redef]
        pass

if TYPE_CHECKING:
    from src.collection_queue import CollectionQueue
    from src.search.engine import SearchEngine

logger = logging.getLogger(__name__)


def _downloads_dir() -> Path:
    return Path(__file__).resolve().parents[2] / "data" / "downloads"


# Re-exported so existing patch points keep working:
#   - constants accessed/patched as ``mod.<NAME>`` by the test suite;
#   - TelegramCommandRetryLaterError imported from this module by external code.
__all__ = [
    "TelegramCommandDispatcher",
    "TelegramCommandRetryLaterError",
    "REACTION_MIN_INTERVAL_SETTING",
    "DEFAULT_REACTION_MIN_INTERVAL_SEC",
    "REACTION_MIN_INTERVAL_FLOOR_SEC",
    "REACTION_MIN_INTERVAL_CEILING_SEC",
    "COMMAND_STATUS_UPDATE_BUSY_RETRY_INITIAL_SEC",
    "COMMAND_STATUS_UPDATE_BUSY_RETRY_MAX_SEC",
]


class TelegramCommandDispatcher(
    AuthCommandsMixin,
    AccountsCommandsMixin,
    SchedulerCommandsMixin,
    DialogsCommandsMixin,
    ChannelsCommandsMixin,
    NotificationsCommandsMixin,
    PhotoCommandsMixin,
    SearchCommandsMixin,
    ModerationCommandsMixin,
):
    """Worker-side command queue dispatcher.

    Owns the claim/dispatch/update loop and the command-status machinery; the
    per-domain ``_handle_*`` handlers live in the mixins above. A handful of
    handlers stay on this class because the test suite patches symbols
    (``TelegramActionService``, ``Notifier``, ``NotificationTargetService``) or
    ``__file__`` through *this* module's namespace, so their call sites must
    resolve those names here (#1047).
    """

    def __init__(
        self,
        db: Database,
        pool: ClientPool,
        config: AppConfig | None = None,
        collector: Collector | None = None,
        *,
        scheduler: SchedulerManager | None = None,
        auth: TelegramAuth | None = None,
        search_engine: "SearchEngine | None" = None,
        collection_queue: "CollectionQueue | None" = None,
        live_runtime_pause_gate: LiveRuntimePauseGate | None = None,
        notifier: "Notifier | None" = None,
    ):
        self._db = db
        self._pool = pool
        self._config = config
        self._collector = collector
        self._scheduler = scheduler
        self._auth = auth
        self._search_engine = search_engine
        self._collection_queue = collection_queue
        self._live_runtime_pause_gate = live_runtime_pause_gate
        # The shared worker Notifier (same instance the collector / unified
        # dispatcher hold) so notifications.invalidate_cache can clear its
        # me-cache when the notification account changes (#832).
        self._notifier = notifier
        self._task: asyncio.Task | None = None
        self._stop_event = asyncio.Event()
        self._last_reaction_at_monotonic: dict[str, float] = {}

    async def start(self) -> None:
        if self._task and not self._task.done():
            return
        self._stop_event.clear()
        self._task = asyncio.create_task(self._run_loop(), name="telegram_command_dispatcher")

    async def stop(self) -> None:
        self._stop_event.set()
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None

    async def _run_loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                if self._live_runtime_pause_gate is not None:
                    resumed = await self._live_runtime_pause_gate.wait_if_paused(
                        stop_event=self._stop_event,
                    )
                    if not resumed:
                        break
                command = await self._db.repos.telegram_commands.claim_next_command()
            except DatabaseBusyError:
                # Transient lock while claiming — never let it kill the loop
                # ("Task exception was never retrieved"). Back off and retry.
                logger.warning("telegram_command_dispatcher: DB busy while claiming command; retrying")
                await asyncio.sleep(1.0)
                continue
            if command is None:
                await asyncio.sleep(1.0)
                continue
            started_at = time.monotonic()
            is_auth_command = command.command_type.startswith("auth.")
            is_search_command = command.command_type == "search.telegram"
            phone = str(command.payload.get("phone", "")).strip()
            if is_auth_command:
                logger.info(
                    "telegram_auth_command start command_id=%s command_type=%s phone=%s",
                    command.id,
                    command.command_type,
                    phone,
                )
            elif is_search_command:
                search_fields = query_log_fields(str(command.payload.get("query", "")))
                logger.info(
                    "telegram_search_command start command_id=%s mode=%s limit=%s channel_id=%s "
                    "query_hash=%s query_len=%d",
                    command.id,
                    command.payload.get("mode", "telegram"),
                    command.payload.get("limit", 50),
                    command.payload.get("channel_id"),
                    search_fields["query_hash"],
                    search_fields["query_len"],
                )
            try:
                result = await self._dispatch(command.command_type, command.payload)
            except asyncio.CancelledError:
                await self._update_command_safely(
                    command.id,
                    status=TelegramCommandStatus.PENDING,
                    error="cancelled while running; reset for retry",
                    log_action="pending after cancellation",
                    retry_busy=False,
                )
                raise
            except TelegramCommandRetryLaterError as exc:
                logger.info(
                    "Telegram command delayed: id=%s type=%s run_after=%s reason=%s",
                    command.id,
                    command.command_type,
                    exc.run_after.isoformat(),
                    exc.reason,
                )
                await self._update_command_safely(
                    command.id,
                    status=TelegramCommandStatus.PENDING,
                    error=exc.reason,
                    result_payload=exc.result_payload or {},
                    payload=command.payload,
                    run_after=exc.run_after,
                    log_action="pending for retry",
                )
            except HandledFloodWaitError as exc:
                run_after = exc.info.next_available_at_utc + timedelta(seconds=1)
                logger.info(
                    "Telegram command delayed by flood-wait: id=%s type=%s run_after=%s reason=%s",
                    command.id,
                    command.command_type,
                    run_after.isoformat(),
                    exc.info.detail,
                )
                await self._update_command_safely(
                    command.id,
                    status=TelegramCommandStatus.PENDING,
                    error=exc.info.detail,
                    result_payload={
                        "state": "waiting_flood_wait",
                        "operation": exc.info.operation,
                        "phone": exc.info.phone,
                        "wait_seconds": exc.info.wait_seconds,
                        "next_available_at_utc": exc.info.next_available_at_utc.isoformat(),
                    },
                    payload=command.payload,
                    run_after=run_after,
                    log_action="pending after flood-wait",
                )
            except (TelegramReactionInvalidError, ReactionInvalidError) as exc:
                logger.info(
                    "Telegram command rejected invalid reaction: id=%s type=%s error=%s",
                    command.id,
                    command.command_type,
                    str(exc),
                )
                await self._update_command_safely(
                    command.id,
                    status=TelegramCommandStatus.FAILED,
                    error=str(exc),
                    result_payload={
                        "state": "invalid_reaction",
                        "emoji": command.payload.get("emoji"),
                    },
                    payload=command.payload,
                    log_action="failed after invalid reaction",
                )
            except Exception as exc:
                duration_ms = elapsed_ms(started_at)
                if is_auth_command:
                    logger.exception(
                        "telegram_auth_command error command_id=%s command_type=%s phone=%s duration_ms=%d error=%s",
                        command.id,
                        command.command_type,
                        phone,
                        duration_ms,
                        str(exc),
                    )
                elif is_search_command:
                    search_fields = query_log_fields(str(command.payload.get("query", "")))
                    logger.exception(
                        "telegram_search_command error command_id=%s mode=%s duration_ms=%d "
                        "error=%s query_hash=%s",
                        command.id,
                        command.payload.get("mode", "telegram"),
                        duration_ms,
                        str(exc),
                        search_fields["query_hash"],
                    )
                else:
                    logger.exception("Telegram command failed: id=%s type=%s", command.id, command.command_type)
                await self._update_command_safely(
                    command.id,
                    status=TelegramCommandStatus.FAILED,
                    error=str(exc),
                    payload=command.payload,
                    log_action="failed after dispatch error",
                )
            else:
                if is_auth_command:
                    duration_ms = elapsed_ms(started_at)
                    logger.info(
                        "telegram_auth_command success command_id=%s command_type=%s phone=%s duration_ms=%d",
                        command.id,
                        command.command_type,
                        phone,
                        duration_ms,
                    )
                elif is_search_command:
                    duration_ms = elapsed_ms(started_at)
                    result_payload = result.get("result") or {}
                    logger.info(
                        "telegram_search_command success command_id=%s mode=%s duration_ms=%d "
                        "total=%s result_error=%s",
                        command.id,
                        command.payload.get("mode", "telegram"),
                        duration_ms,
                        result_payload.get("total"),
                        bool(result_payload.get("error")),
                    )
                await self._update_command_safely(
                    command.id,
                    status=TelegramCommandStatus.SUCCEEDED,
                    result_payload=self._unwrap_result_payload(result),
                    payload=result.get("payload_update"),
                    log_action="succeeded",
                )

    @staticmethod
    def _unwrap_result_payload(result: object) -> dict:
        """Normalize a handler return into the persisted result_payload.

        Only a few handlers wrap their output in {"result": {...}}; ~40 others
        return a flat dict. Persisting result.get("result") alone dropped every
        flat result to {} (audit #838/9, and #838/4 for search participants).
        Use the inner envelope when present, otherwise the whole dict minus the
        reserved payload_update key.
        """
        if not isinstance(result, dict):
            return {}
        if "result" in result:
            inner = result.get("result")
            return inner if isinstance(inner, dict) else {}
        return {k: v for k, v in result.items() if k != "payload_update"}

    async def _update_command_safely(
        self,
        command_id: int | None,
        *,
        status: TelegramCommandStatus,
        log_action: str,
        retry_busy: bool = True,
        **kwargs: Any,
    ) -> None:
        assert command_id is not None
        delay = COMMAND_STATUS_UPDATE_BUSY_RETRY_INITIAL_SEC
        while True:
            try:
                await self._db.repos.telegram_commands.update_command(
                    command_id,
                    status=status,
                    **kwargs,
                )
                return
            except DatabaseBusyError as exc:
                logger.warning(
                    "telegram_command_dispatcher: DB busy while marking command %s %s: %s",
                    command_id,
                    log_action,
                    exc,
                )
                if not retry_busy:
                    return
                await asyncio.sleep(delay)
                delay = min(delay * 2, COMMAND_STATUS_UPDATE_BUSY_RETRY_MAX_SEC)
            except Exception as exc:
                logger.warning(
                    "telegram_command_dispatcher: failed to mark command %s %s: %s",
                    command_id,
                    log_action,
                    exc,
                )
                return

    async def _dispatch(self, command_type: str, payload: dict[str, Any]) -> dict[str, Any]:
        handler_name = f"_handle_{command_type.replace('.', '_')}"
        handler = getattr(self, handler_name, None)
        if not callable(handler):
            raise RuntimeError(f"Unsupported telegram command: {command_type}")
        return await handler(payload)

    async def _get_client(self, phone: str):
        result = await self._pool.get_native_client_by_phone(phone)
        if result is None:
            raise RuntimeError("client unavailable")
        return result

    def _pool_method(self, name: str) -> Any | None:
        instance_attrs = getattr(self._pool, "__dict__", {})
        if isinstance(instance_attrs, dict) and name in instance_attrs:
            candidate = instance_attrs[name]
        elif callable(getattr(type(self._pool), name, None)):
            candidate = getattr(self._pool, name)
        else:
            return None
        return candidate if callable(candidate) else None

    def _notification_target_service(self) -> NotificationTargetService:
        from src.database.bundles import NotificationBundle

        return NotificationTargetService(NotificationBundle.from_database(self._db), self._pool)

    async def _handle_dialogs_join(self, payload: dict[str, Any]) -> dict[str, Any]:
        result = await TelegramActionService(self._pool).join_dialog(
            phone=str(payload["phone"]),
            target=payload["target"],
        )
        return {"phone": result.phone, "target": result.target, "via_invite": result.via_invite}

    async def _handle_dialogs_download_media(self, payload: dict[str, Any]) -> dict[str, Any]:
        phone = str(payload["phone"])
        try:
            output_dir = await asyncio.to_thread(_downloads_dir)
            result = await TelegramActionService(self._pool).download_media(
                phone=phone,
                chat_id=payload["chat_id"],
                message_id=int(payload["message_id"]),
                output_dir=output_dir,
                operation_prefix="dispatcher_dialogs_download_media",
            )
            return {"phone": result.phone, "path": result.path}
        except HandledFloodWaitError as exc:
            raise RuntimeError(f"flood_wait:{exc.info.wait_seconds}") from exc
        except TelegramActionMessageNotFoundError as exc:
            raise RuntimeError("message_not_found") from exc
        except TelegramActionNoMediaError as exc:
            raise RuntimeError("no_media") from exc
        except TelegramActionPathEscapeError as exc:
            raise RuntimeError("path_escape") from exc

    async def _handle_notifications_test(self, payload: dict[str, Any]) -> dict[str, Any]:
        from src.database.bundles import NotificationBundle

        target_service = self._notification_target_service()
        notifier = Notifier(target_service, None, NotificationBundle.from_database(self._db))
        text = str(payload.get("text") or "").strip() or "✅ Тест уведомлений: соединение установлено"
        ok = await notifier.notify(text)
        if not ok:
            raise RuntimeError("notification_test_failed")
        return {"sent": True}
