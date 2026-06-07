from __future__ import annotations

import asyncio
import logging
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from inspect import isawaitable
from pathlib import Path
from typing import TYPE_CHECKING, Any

from src.config import AppConfig
from src.database import Database, DatabaseBusyError
from src.database.live_accounts import load_live_usable_accounts
from src.live_runtime_pause import LiveRuntimePauseGate
from src.models import Account, RuntimeSnapshot, TelegramCommandStatus
from src.scheduler.service import SchedulerManager
from src.services.channel_onboarding import (
    channel_from_resolved_info,
    enqueue_stats_for_new_channels,
    fetch_channel_meta,
    get_existing_channel,
)
from src.services.notification_service import NotificationService
from src.services.notification_target_service import NotificationTargetService
from src.services.photo_auto_upload_service import PhotoAutoUploadService
from src.services.photo_task_service import PhotoTarget, PhotoTaskService
from src.services.telegram_actions import (
    TelegramActionMessageNotFoundError,
    TelegramActionNoMediaError,
    TelegramActionPathEscapeError,
    TelegramActionService,
)
from src.settings_utils import parse_float_setting
from src.telegram.auth import TelegramAuth
from src.telegram.client_pool import ClientPool
from src.telegram.collector import Collector
from src.telegram.flood_wait import HandledFloodWaitError
from src.telegram.notifier import Notifier
from src.telegram.reactions import TelegramReactionInvalidError, normalize_outgoing_reaction_emoji
from src.telegram.utils import normalize_utc
from src.utils.datetime import parse_required_datetime, parse_required_schedule_datetime
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
COMMAND_STATUS_UPDATE_BUSY_RETRY_INITIAL_SEC = 0.1
COMMAND_STATUS_UPDATE_BUSY_RETRY_MAX_SEC = 1.0

# Minimum spacing between reactions on the same phone. Configurable live via the
# DB setting below; a non-zero floor is enforced because Telegram rate-limits
# reactions server-side and zero spacing risks FLOOD_WAIT / account limiting.
REACTION_MIN_INTERVAL_SETTING = "reaction_min_interval_sec"
DEFAULT_REACTION_MIN_INTERVAL_SEC = 30.0
REACTION_MIN_INTERVAL_FLOOR_SEC = 1.0
REACTION_MIN_INTERVAL_CEILING_SEC = 300.0


@dataclass(slots=True)
class TelegramCommandRetryLaterError(RuntimeError):
    run_after: datetime
    reason: str
    result_payload: dict[str, Any] | None = None

    def __str__(self) -> str:
        return self.reason


class TelegramCommandDispatcher:
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
                    result_payload=result.get("result") or {},
                    payload=result.get("payload_update"),
                    log_action="succeeded",
                )

    async def _update_command_safely(
        self,
        command_id: int | None,
        *,
        status: TelegramCommandStatus,
        log_action: str,
        retry_busy: bool = True,
        **kwargs: Any,
    ) -> None:
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

    def _photo_task_service(self) -> PhotoTaskService:
        from src.database.bundles import PhotoLoaderBundle
        from src.services.photo_publish_service import PhotoPublishService

        return PhotoTaskService(PhotoLoaderBundle.from_database(self._db), PhotoPublishService(self._pool))

    def _photo_auto_upload_service(self) -> PhotoAutoUploadService:
        from src.database.bundles import PhotoLoaderBundle
        from src.services.photo_publish_service import PhotoPublishService

        return PhotoAutoUploadService(PhotoLoaderBundle.from_database(self._db), PhotoPublishService(self._pool))

    def _notification_service(self) -> NotificationService:
        from src.database.bundles import NotificationBundle

        target_service = NotificationTargetService(NotificationBundle.from_database(self._db), self._pool)
        kwargs: dict[str, Any] = {}
        if self._config is not None:
            kwargs["bot_name_prefix"] = self._config.notifications.bot_name_prefix
            kwargs["bot_username_prefix"] = self._config.notifications.bot_username_prefix
        return NotificationService(self._db, target_service, **kwargs)

    def _notification_target_service(self) -> NotificationTargetService:
        from src.database.bundles import NotificationBundle

        return NotificationTargetService(NotificationBundle.from_database(self._db), self._pool)

    def _pool_method(self, name: str) -> Any | None:
        instance_attrs = getattr(self._pool, "__dict__", {})
        if isinstance(instance_attrs, dict) and name in instance_attrs:
            candidate = instance_attrs[name]
        elif callable(getattr(type(self._pool), name, None)):
            candidate = getattr(self._pool, name)
        else:
            return None
        return candidate if callable(candidate) else None

    async def _handle_auth_send_code(self, payload: dict[str, Any]) -> dict[str, Any]:
        if self._auth is None or not self._auth.is_configured:
            raise RuntimeError("auth_not_configured")
        phone = str(payload["phone"]).strip()
        result = await self._auth.send_code(phone)
        return {"result": {"phone": phone, **result}}

    async def _handle_auth_resend_code(self, payload: dict[str, Any]) -> dict[str, Any]:
        if self._auth is None or not self._auth.is_configured:
            raise RuntimeError("auth_not_configured")
        phone = str(payload["phone"]).strip()
        result = await self._auth.resend_code(phone)
        return {"result": {"phone": phone, **result}}

    async def _handle_auth_verify_code(self, payload: dict[str, Any]) -> dict[str, Any]:
        if self._auth is None or not self._auth.is_configured:
            raise RuntimeError("auth_not_configured")
        phone = str(payload["phone"]).strip()
        password_2fa = str(payload.get("password_2fa", "")).strip() or None
        payload["password_2fa"] = ""
        session_string = await self._auth.verify_code(
            phone,
            str(payload["code"]),
            str(payload["phone_code_hash"]),
            password_2fa,
        )
        existing = await self._db.get_account_summaries(active_only=False)
        account = Account(
            phone=phone,
            session_string=session_string,
            is_primary=not any(acc.phone == phone for acc in existing) and len(existing) == 0,
            is_premium=False,
        )
        await self._db.add_account(account)
        connect_result = await self._handle_accounts_connect({"phone": phone})
        return {"result": {"phone": phone, **connect_result}, "payload_update": {**payload}}

    async def _handle_scheduler_reconcile(self, payload: dict[str, Any]) -> dict[str, Any]:
        if self._scheduler is None:
            raise RuntimeError("scheduler_unavailable")
        autostart = await self._db.get_setting("scheduler_autostart")
        desired_running = autostart == "1"
        if not desired_running:
            await self._scheduler.stop()
            await self._scheduler.load_settings()
            return {"running": False}
        if self._scheduler.is_running:
            await self._scheduler.stop()
        await self._scheduler.load_settings()
        await self._scheduler.start()
        return {"running": True, "interval_minutes": self._scheduler.interval_minutes}

    async def _handle_scheduler_trigger_warm(self, payload: dict[str, Any]) -> dict[str, Any]:
        if self._scheduler is None:
            raise RuntimeError("scheduler_unavailable")
        await self._scheduler.trigger_warm_background()
        return {"started": True}

    async def _handle_collection_pause(self, payload: dict[str, Any]) -> dict[str, Any]:
        await self._db.set_setting("collection_queue_paused", "1")
        if self._collection_queue is not None:
            self._collection_queue.pause()
        return {"paused": True}

    async def _handle_collection_resume(self, payload: dict[str, Any]) -> dict[str, Any]:
        await self._db.set_setting("collection_queue_paused", "0")
        if self._collection_queue is not None:
            self._collection_queue.resume()
        return {"paused": False}

    async def _handle_dialogs_refresh(self, payload: dict[str, Any]) -> dict[str, Any]:
        phone = str(payload["phone"])
        dialogs = await self._pool.get_dialogs_for_phone(phone, include_dm=True, mode="full", refresh=True)
        await self._db.repos.dialog_cache.replace_dialogs(phone, dialogs)
        return {"phone": phone, "dialogs_count": len(dialogs)}

    async def _handle_dialogs_cache_clear(self, payload: dict[str, Any]) -> dict[str, Any]:
        phone = str(payload.get("phone") or "").strip()
        invalidate = getattr(self._pool, "invalidate_dialogs_cache", None)
        if phone:
            if callable(invalidate):
                invalidate(phone)
            await self._db.repos.dialog_cache.clear_dialogs(phone)
        else:
            if callable(invalidate):
                invalidate()
            await self._db.repos.dialog_cache.clear_all_dialogs()
        return {"phone": phone}

    async def _handle_dialogs_leave(self, payload: dict[str, Any]) -> dict[str, Any]:
        phone = str(payload["phone"])
        dialogs = [(int(item["dialog_id"]), str(item["title"])) for item in payload.get("dialogs", [])]
        result = await TelegramActionService(self._pool).leave_dialogs(phone=phone, dialogs=dialogs)
        return {"left": result.success_count, "failed": result.failed_count}

    async def _handle_dialogs_send(self, payload: dict[str, Any]) -> dict[str, Any]:
        result = await TelegramActionService(self._pool).send_message(
            phone=str(payload["phone"]),
            recipient=payload["recipient"],
            text=payload["text"],
        )
        return {"phone": result.phone, "message_id": result.message_id}

    async def _handle_dialogs_join(self, payload: dict[str, Any]) -> dict[str, Any]:
        result = await TelegramActionService(self._pool).join_dialog(
            phone=str(payload["phone"]),
            target=payload["target"],
        )
        return {"phone": result.phone, "target": result.target, "via_invite": result.via_invite}

    async def _handle_dialogs_resolve(self, payload: dict[str, Any]) -> dict[str, Any]:
        identifier = str(payload["identifier"])
        entity = await self._pool.resolve_any_entity(
            identifier, phone=str(payload.get("phone") or "") or None
        )
        if not entity:
            raise RuntimeError(f"resolve failed: {identifier!r} not found")
        return {"entity": entity}

    async def _handle_dialogs_edit_message(self, payload: dict[str, Any]) -> dict[str, Any]:
        result = await TelegramActionService(self._pool).edit_message(
            phone=str(payload["phone"]),
            chat_id=payload["chat_id"],
            message_id=int(payload["message_id"]),
            text=payload["text"],
        )
        return {"phone": result.phone}

    async def _handle_dialogs_delete_message(self, payload: dict[str, Any]) -> dict[str, Any]:
        result = await TelegramActionService(self._pool).delete_messages(
            phone=str(payload["phone"]),
            chat_id=payload["chat_id"],
            message_ids=[int(value) for value in payload["message_ids"]],
        )
        return {"phone": result.phone, "deleted": result.count}

    async def _handle_dialogs_forward_messages(self, payload: dict[str, Any]) -> dict[str, Any]:
        result = await TelegramActionService(self._pool).forward_messages(
            phone=str(payload["phone"]),
            from_chat=payload["from_chat"],
            to_chat=payload["to_chat"],
            message_ids=[int(value) for value in payload["message_ids"]],
        )
        return {"phone": result.phone, "forwarded": result.count}

    async def _account_flood_until(self, phone: str) -> datetime | None:
        accounts: list[Any] = []
        for getter_name in ("get_account_summaries", "get_accounts"):
            getter = getattr(self._db, getter_name, None)
            if not callable(getter):
                continue
            try:
                result = getter(active_only=True)
            except TypeError:
                result = getter()
            if isawaitable(result):
                result = await result
            if isinstance(result, (list, tuple)):
                accounts = list(result)
                break
        now = datetime.now(timezone.utc)
        for account in accounts:
            if str(getattr(account, "phone", "")) != phone:
                continue
            flood_until = normalize_utc(getattr(account, "flood_wait_until", None))
            if flood_until is not None and flood_until > now:
                return flood_until
        return None

    async def _reaction_min_interval(self) -> float:
        """Per-phone minimum seconds between reactions, read live from DB settings.

        Clamped to a non-zero floor because Telegram rate-limits reactions
        server-side; values outside the range or unparseable fall back to the
        default.
        """
        raw = await self._db.get_setting(REACTION_MIN_INTERVAL_SETTING)
        value = parse_float_setting(
            raw,
            setting_name=REACTION_MIN_INTERVAL_SETTING,
            default=DEFAULT_REACTION_MIN_INTERVAL_SEC,
            logger=logger,
        )
        return max(REACTION_MIN_INTERVAL_FLOOR_SEC, min(REACTION_MIN_INTERVAL_CEILING_SEC, value))

    async def _ensure_reaction_can_run(self, phone: str) -> None:
        is_warming = self._pool_method("is_warming")
        if callable(is_warming):
            try:
                warming = bool(is_warming())
            except Exception:
                warming = False
            if warming:
                run_after = datetime.now(timezone.utc) + timedelta(seconds=5)
                raise TelegramCommandRetryLaterError(
                    run_after=run_after,
                    reason="account dialog warm-up is still running",
                    result_payload={
                        "state": "waiting_warmup",
                        "phone": phone,
                        "next_available_at_utc": run_after.isoformat(),
                    },
                )

        flood_until = await self._account_flood_until(phone)
        if flood_until is not None:
            raise TelegramCommandRetryLaterError(
                run_after=flood_until + timedelta(seconds=1),
                reason=f"account {phone} is flood-waited until {flood_until.isoformat()}",
                result_payload={
                    "state": "waiting_flood_wait",
                    "phone": phone,
                    "next_available_at_utc": flood_until.isoformat(),
                },
            )

        last = self._last_reaction_at_monotonic.get(phone)
        if last is None:
            return
        min_interval = await self._reaction_min_interval()
        elapsed = time.monotonic() - last
        remaining = min_interval - elapsed
        if remaining > 0:
            run_after = datetime.now(timezone.utc) + timedelta(seconds=remaining)
            raise TelegramCommandRetryLaterError(
                run_after=run_after,
                reason=f"reaction rate limit for {phone}; waiting {int(remaining) + 1}s",
                result_payload={
                    "state": "waiting_rate_limit",
                    "phone": phone,
                    "retry_after_sec": int(remaining) + 1,
                    "next_available_at_utc": run_after.isoformat(),
                },
            )

    async def _handle_dialogs_pin_message(self, payload: dict[str, Any]) -> dict[str, Any]:
        result = await TelegramActionService(self._pool).pin_message(
            phone=str(payload["phone"]),
            chat_id=payload["chat_id"],
            message_id=int(payload["message_id"]),
            notify=bool(payload.get("notify", False)),
        )
        return {"phone": result.phone}

    async def _handle_dialogs_react(self, payload: dict[str, Any]) -> dict[str, Any]:
        phone = str(payload["phone"])
        await self._ensure_reaction_can_run(phone)
        emoji = normalize_outgoing_reaction_emoji(str(payload.get("emoji") or ""))
        result = await TelegramActionService(self._pool).send_reaction(
            phone=phone,
            chat_id=payload["chat_id"],
            message_id=int(payload["message_id"]),
            emoji=emoji,
            native=True,
            resolve_entity=True,
        )
        self._last_reaction_at_monotonic[result.phone] = time.monotonic()
        return {"phone": result.phone}

    async def _handle_dialogs_unpin_message(self, payload: dict[str, Any]) -> dict[str, Any]:
        message_id = payload.get("message_id")
        result = await TelegramActionService(self._pool).unpin_message(
            phone=str(payload["phone"]),
            chat_id=payload["chat_id"],
            message_id=int(message_id) if message_id is not None else None,
        )
        return {"phone": result.phone}

    async def _handle_dialogs_participants(self, payload: dict[str, Any]) -> dict[str, Any]:
        action_result = await TelegramActionService(self._pool).get_participants(
            phone=str(payload["phone"]),
            chat_id=payload["chat_id"],
            limit=int(payload.get("limit", 200)),
            search=str(payload.get("search", "")),
        )
        data = [
            {
                "id": p.id,
                "first_name": getattr(p, "first_name", None) or "",
                "last_name": getattr(p, "last_name", None) or "",
                "username": getattr(p, "username", None) or "",
            }
            for p in action_result.participants
        ]
        scope = f"dialogs_participants:{action_result.phone}:{payload['chat_id']}"
        search_value = str(payload.get("search", ""))
        # Only cache unfiltered (full) participant lists. A search-filtered
        # result would otherwise overwrite the shared snapshot, so later
        # no-search GETs would return only the filtered subset.
        if not search_value:
            await self._db.repos.runtime_snapshots.upsert_snapshot(
                RuntimeSnapshot(
                    snapshot_type="dialogs_participants",
                    scope=scope,
                    payload={"participants": data, "total": len(data)},
                )
            )
        result = {"phone": action_result.phone, "scope": scope, "total": len(data)}
        if search_value:
            # Search results are intentionally not cached; return them
            # inline so the client can read them via GET /telegram-commands/{id}.
            result["participants"] = data
        return result

    async def _handle_dialogs_broadcast_stats(self, payload: dict[str, Any]) -> dict[str, Any]:
        action_result = await TelegramActionService(self._pool).get_broadcast_stats(
            phone=str(payload["phone"]),
            chat_id=payload["chat_id"],
        )
        stats = action_result.stats
        fields: dict[str, Any] = {}
        for attr in ("followers", "views_per_post", "shares_per_post", "reactions_per_post", "forwards_per_post"):
            val = getattr(stats, attr, None)
            if val is not None:
                current = getattr(val, "current", None)
                previous = getattr(val, "previous", None)
                if current is not None:
                    fields[attr] = {"current": current, "previous": previous}
                else:
                    fields[attr] = str(val)
        period = getattr(stats, "period", None)
        if period is not None:
            fields["period"] = {
                "min_date": period.min_date.isoformat() if getattr(period, "min_date", None) else None,
                "max_date": period.max_date.isoformat() if getattr(period, "max_date", None) else None,
            }
        enabled_notifications = getattr(stats, "enabled_notifications", None)
        if enabled_notifications is not None:
            fields["enabled_notifications"] = enabled_notifications
        if not fields:
            fields["raw"] = str(stats)
        scope = f"dialogs_broadcast_stats:{action_result.phone}:{payload['chat_id']}"
        await self._db.repos.runtime_snapshots.upsert_snapshot(
            RuntimeSnapshot(
                snapshot_type="dialogs_broadcast_stats",
                scope=scope,
                payload={"stats": fields},
            )
        )
        return {"phone": action_result.phone, "scope": scope}

    async def _handle_dialogs_archive(self, payload: dict[str, Any]) -> dict[str, Any]:
        return await self._set_dialog_folder(payload, folder_id=1)

    async def _handle_dialogs_unarchive(self, payload: dict[str, Any]) -> dict[str, Any]:
        return await self._set_dialog_folder(payload, folder_id=0)

    async def _set_dialog_folder(self, payload: dict[str, Any], *, folder_id: int) -> dict[str, Any]:
        result = await TelegramActionService(self._pool).set_dialog_folder(
            phone=str(payload["phone"]),
            chat_id=payload["chat_id"],
            folder_id=folder_id,
        )
        return {"phone": result.phone, "folder_id": folder_id}

    async def _handle_dialogs_mark_read(self, payload: dict[str, Any]) -> dict[str, Any]:
        max_id = payload.get("max_id")
        result = await TelegramActionService(self._pool).mark_read(
            phone=str(payload["phone"]),
            chat_id=payload["chat_id"],
            max_id=int(max_id) if max_id is not None else None,
        )
        return {"phone": result.phone}

    async def _handle_dialogs_edit_admin(self, payload: dict[str, Any]) -> dict[str, Any]:
        result = await TelegramActionService(self._pool).edit_admin(
            phone=str(payload["phone"]),
            chat_id=payload["chat_id"],
            user_id=payload["user_id"],
            is_admin=bool(payload.get("is_admin", False)),
            title=payload.get("title") or None,
        )
        return {"phone": result.phone}

    async def _handle_dialogs_edit_permissions(self, payload: dict[str, Any]) -> dict[str, Any]:
        result = await TelegramActionService(self._pool).edit_permissions(
            phone=str(payload["phone"]),
            chat_id=payload["chat_id"],
            user_id=payload["user_id"],
            until_date=parse_required_datetime(str(payload["until_date"])) if payload.get("until_date") else None,
            send_messages=bool(payload["send_messages"]) if "send_messages" in payload else None,
            send_media=bool(payload["send_media"]) if "send_media" in payload else None,
        )
        return {"phone": result.phone}

    async def _handle_dialogs_kick(self, payload: dict[str, Any]) -> dict[str, Any]:
        result = await TelegramActionService(self._pool).kick_participant(
            phone=str(payload["phone"]),
            chat_id=payload["chat_id"],
            user_id=payload["user_id"],
        )
        return {"phone": result.phone}

    async def _handle_dialogs_create_channel(self, payload: dict[str, Any]) -> dict[str, Any]:
        result = await TelegramActionService(self._pool).create_channel(
            phone=str(payload["phone"]),
            title=str(payload["title"]).strip(),
            about=str(payload.get("about", "")).strip(),
            username=str(payload.get("username", "")).strip(),
        )
        return {
            "phone": result.phone,
            "channel_id": result.channel_id,
            "channel_title": result.channel_title,
            "channel_username": result.channel_username,
            "invite_link": result.invite_link,
        }

    async def _handle_search_telegram(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Run a live Telegram-backed search on the worker's real pool.

        The web container has no live ClientPool (runtime_mode="web"), so it
        proxies premium/my_chats/in-channel search here and reads the serialized
        SearchResult back from ``result_payload`` (#643).
        """
        if self._search_engine is None:
            raise RuntimeError("Search engine unavailable in worker")
        query = str(payload.get("query", ""))
        mode = str(payload.get("mode", "telegram"))
        limit = int(payload.get("limit", 50))
        if mode == "my_chats":
            result = await self._search_engine.search_my_chats(query, limit=limit)
        elif mode == "channel":
            channel_id = payload.get("channel_id")
            result = await self._search_engine.search_in_channel(
                int(channel_id) if channel_id is not None else None, query, limit=limit
            )
        else:
            result = await self._search_engine.search_telegram(query, limit=limit)
        return {"result": result.model_dump(mode="json")}

    async def _handle_agent_forum_topics_refresh(self, payload: dict[str, Any]) -> dict[str, Any]:
        channel_id = int(payload["channel_id"])
        topics = await self._pool.get_forum_topics(channel_id)
        if topics:
            await self._db.upsert_forum_topics(channel_id, topics)
            await self._db.set_channel_type(channel_id, "forum")
        return {"channel_id": channel_id, "count": len(topics)}

    async def _handle_channels_add_identifier(self, payload: dict[str, Any]) -> dict[str, Any]:
        identifier = str(payload["identifier"]).strip()
        info = await self._pool.resolve_channel(identifier)
        if not info:
            raise RuntimeError(f"resolve failed: {identifier!r} not found")
        existing = await get_existing_channel(self._db, int(info["channel_id"]))
        meta = await fetch_channel_meta(
            self._pool, int(info["channel_id"]), info.get("channel_type")
        )
        channel = channel_from_resolved_info(info, meta)
        await self._db.add_channel(channel)
        stats_task_id = None
        if existing is None and channel.is_active:
            stats_task_id = await enqueue_stats_for_new_channels(
                self._db.create_stats_task,
                [channel.channel_id],
                context="channels.add_identifier",
            )
        return {"channel_id": info["channel_id"], "stats_task_id": stats_task_id}

    async def _handle_channels_collect_stats(self, payload: dict[str, Any]) -> dict[str, Any]:
        if self._collector is None:
            raise RuntimeError("collector_unavailable")
        channel_pk = int(payload["channel_pk"])
        channel = await self._db.get_channel_by_pk(channel_pk)
        if channel is None:
            raise RuntimeError("channel_not_found")
        result = await self._collector.collect_channel_stats(channel)
        return {"channel_id": channel.channel_id, "collected": bool(result)}

    async def _handle_channels_refresh_types(self, payload: dict[str, Any]) -> dict[str, Any]:
        channels = await self._db.get_channels(active_only=True)
        updated = 0
        failed = 0
        for ch in channels:
            identifier = ch.username or str(ch.channel_id)
            try:
                info = await self._pool.resolve_channel(identifier)
            except Exception:
                info = None
            if info is False:
                await self._db.set_channel_active(ch.id, False)
                await self._db.set_channel_type(ch.channel_id, "unavailable")
                failed += 1
                continue
            if not info or info.get("channel_type") is None:
                failed += 1
                continue
            await self._db.set_channel_type(ch.channel_id, info["channel_type"])
            updated += 1
        return {"updated": updated, "failed": failed}

    async def _handle_channels_refresh_meta(self, payload: dict[str, Any]) -> dict[str, Any]:
        channels = await self._db.get_channels(active_only=True)
        updated = 0
        failed = 0
        for ch in channels:
            try:
                meta = await self._pool.fetch_channel_meta(ch.channel_id, ch.channel_type)
            except Exception:
                meta = None
            if not meta:
                failed += 1
                continue
            await self._db.update_channel_full_meta(
                ch.channel_id,
                about=meta["about"],
                linked_chat_id=meta["linked_chat_id"],
                has_comments=meta["has_comments"],
            )
            updated += 1
        return {"updated": updated, "failed": failed}

    async def _handle_channels_import_batch(self, payload: dict[str, Any]) -> dict[str, Any]:
        identifiers = [str(item).strip() for item in payload.get("identifiers", []) if str(item).strip()]
        existing = await self._db.get_channels()
        existing_ids = {channel.channel_id for channel in existing}
        added = 0
        skipped = 0
        failed = 0
        stats_channel_ids: list[int] = []
        details: list[dict[str, Any]] = []
        for ident in identifiers:
            try:
                info = await self._pool.resolve_channel(ident)
            except Exception:
                info = None
            if not info:
                failed += 1
                details.append({"identifier": ident, "status": "failed"})
                continue
            if info["channel_id"] in existing_ids:
                skipped += 1
                details.append({"identifier": ident, "status": "skipped"})
                continue
            meta = await fetch_channel_meta(
                self._pool, int(info["channel_id"]), info.get("channel_type")
            )
            channel = channel_from_resolved_info(info, meta)
            await self._db.add_channel(channel)
            existing_ids.add(info["channel_id"])
            if channel.is_active:
                stats_channel_ids.append(channel.channel_id)
            added += 1
            details.append({"identifier": ident, "status": "added"})
        stats_task_id = await enqueue_stats_for_new_channels(
            self._db.create_stats_task,
            stats_channel_ids,
            context="channels.import_batch",
        )
        return {
            "added": added,
            "skipped": skipped,
            "failed": failed,
            "details": details,
            "stats_task_id": stats_task_id,
        }

    async def _handle_dialogs_download_media(self, payload: dict[str, Any]) -> dict[str, Any]:
        phone = str(payload["phone"])
        try:
            output_dir = Path(__file__).resolve().parents[2] / "data" / "downloads"
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

    async def _handle_accounts_connect(self, payload: dict[str, Any]) -> dict[str, Any]:
        phone = str(payload["phone"])
        accounts = await load_live_usable_accounts(self._db, active_only=False)
        account = next((a for a in accounts if a.phone == phone), None)
        if account is None:
            summaries = await self._db.get_account_summaries(active_only=False)
            summary = next((a for a in summaries if a.phone == phone), None)
            if summary is not None:
                status = getattr(summary, "session_status", "unavailable")
                raise RuntimeError(f"account_session_unavailable:{phone}:{status}")
            raise RuntimeError(f"account_not_found:{phone}")
        await self._pool.add_client(phone, account.session_string)
        result = await self._pool.get_client_by_phone(phone)
        is_premium = False
        if result is not None:
            session, acquired_phone = result
            try:
                me = await session.fetch_me()
                is_premium = bool(getattr(me, "premium", False))
            finally:
                await self._pool.release_client(acquired_phone)
        await self._db.update_account_premium(phone, is_premium)
        return {"phone": phone, "is_premium": is_premium}

    async def _handle_accounts_toggle(self, payload: dict[str, Any]) -> dict[str, Any]:
        account_id = int(payload["account_id"])
        summaries = await self._db.get_account_summaries(active_only=False)
        account_summary = next((a for a in summaries if a.id == account_id), None)
        live_account: Account | None = None
        if account_summary is None:
            accounts = await load_live_usable_accounts(self._db, active_only=False)
            live_account = next((a for a in accounts if a.id == account_id), None)
            account_summary = live_account
        if account_summary is None:
            raise RuntimeError(f"account_not_found:{account_id}")
        new_active = not account_summary.is_active
        await self._db.set_account_active(account_id, new_active)
        if new_active:
            try:
                if live_account is None:
                    accounts = await load_live_usable_accounts(self._db, active_only=False)
                    live_account = next((a for a in accounts if a.id == account_id), None)
                if live_account is None:
                    logger.warning(
                        "accounts.toggle: account session unavailable for %s",
                        account_summary.phone,
                    )
                else:
                    await self._pool.add_client(live_account.phone, live_account.session_string)
            except Exception as exc:
                logger.warning("accounts.toggle: failed to add client %s: %s", account_summary.phone, exc)
        else:
            try:
                await self._pool.remove_client(account_summary.phone)
            except Exception as exc:
                logger.warning("accounts.toggle: failed to remove client %s: %s", account_summary.phone, exc)
        return {"account_id": account_id, "is_active": new_active}

    async def _handle_accounts_delete(self, payload: dict[str, Any]) -> dict[str, Any]:
        account_id = int(payload["account_id"])
        phone = str(payload.get("phone") or "").strip()
        delete_from_db = not phone
        if not phone:
            accounts = await self._db.get_account_summaries(active_only=False)
            account = next((a for a in accounts if a.id == account_id), None)
            phone = account.phone if account is not None else ""
        if phone:
            try:
                await self._pool.remove_client(phone)
            except Exception as exc:
                logger.warning("accounts.delete: failed to remove client %s: %s", phone, exc)
        if delete_from_db:
            await self._db.delete_account(account_id)
        return {
            "account_id": account_id,
            "deleted": delete_from_db,
            "client_removed": bool(phone),
        }

    async def _handle_notifications_setup_bot(self, payload: dict[str, Any]) -> dict[str, Any]:
        bot = await self._notification_service().setup_bot()
        return {"bot_username": bot.bot_username, "bot_id": bot.bot_id}

    async def _handle_notifications_delete_bot(self, payload: dict[str, Any]) -> dict[str, Any]:
        await self._notification_service().teardown_bot()
        return {"deleted": True}

    async def _handle_notifications_test(self, payload: dict[str, Any]) -> dict[str, Any]:
        from src.database.bundles import NotificationBundle

        target_service = self._notification_target_service()
        notifier = Notifier(target_service, None, NotificationBundle.from_database(self._db))
        text = str(payload.get("text") or "").strip() or "✅ Тест уведомлений: соединение установлено"
        ok = await notifier.notify(text)
        if not ok:
            raise RuntimeError("notification_test_failed")
        return {"sent": True}

    async def _handle_photo_send_now(self, payload: dict[str, Any]) -> dict[str, Any]:
        item = await self._photo_task_service().send_now(
            phone=str(payload["phone"]),
            target=PhotoTarget(
                dialog_id=int(payload["target_dialog_id"]),
                title=payload.get("target_title"),
                target_type=payload.get("target_type"),
            ),
            file_paths=[str(path) for path in payload.get("file_paths", [])],
            mode=str(payload.get("mode", "separate")),
            caption=payload.get("caption"),
        )
        return {"item_id": item.id, "batch_id": item.batch_id}

    async def _handle_photo_schedule_send(self, payload: dict[str, Any]) -> dict[str, Any]:
        item = await self._photo_task_service().schedule_send(
            phone=str(payload["phone"]),
            target=PhotoTarget(
                dialog_id=int(payload["target_dialog_id"]),
                title=payload.get("target_title"),
                target_type=payload.get("target_type"),
            ),
            file_paths=[str(path) for path in payload.get("file_paths", [])],
            mode=str(payload.get("mode", "separate")),
            schedule_at=parse_required_schedule_datetime(str(payload["schedule_at"])),
            caption=payload.get("caption"),
        )
        return {"item_id": item.id, "batch_id": item.batch_id}

    async def _handle_photo_run_due(self, payload: dict[str, Any]) -> dict[str, Any]:
        items = await self._photo_task_service().run_due()
        jobs = await self._photo_auto_upload_service().run_due()
        return {"processed_items": items, "processed_jobs": jobs}

    async def _handle_moderation_publish_run(self, payload: dict[str, Any]) -> dict[str, Any]:
        from src.services.pipeline_service import PipelineService
        from src.services.publish_service import PublishService

        run_id = int(payload["run_id"])
        run = await self._db.repos.generation_runs.get(run_id)
        if run is None:
            raise RuntimeError("run_not_found")
        pipeline = await PipelineService(self._db).get(int(payload["pipeline_id"]))
        if pipeline is None:
            raise RuntimeError("pipeline_invalid")
        results = await PublishService(self._db, self._pool).publish_run(run, pipeline)
        if not results or not all(result.success for result in results):
            raise RuntimeError("pipeline_run_failed")
        return {"run_id": run_id, "published": len(results)}
