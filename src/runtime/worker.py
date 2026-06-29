from __future__ import annotations

import asyncio
import logging
import signal
from dataclasses import asdict
from datetime import datetime, timezone
from inspect import isawaitable

from src.config import AppConfig
from src.database.repositories.accounts import AccountSessionDecryptError
from src.models import RuntimeSnapshot
from src.services.notification_service import NotificationService
from src.services.runtime_diagnostics import HEARTBEAT_INTERVAL_SEC, resolve_snapshot_publish_timeout
from src.telegram.utils import normalize_utc
from src.web.bootstrap import build_worker_container, start_container, stop_container
from src.web.log_handler import LogBuffer

logger = logging.getLogger(__name__)
# HEARTBEAT_INTERVAL_SEC is the single source of truth in runtime_diagnostics.
# The snapshot-publish timeout comes from config
# (scheduler.snapshot_publish_timeout_sec).


def _current_task_is_cancelling() -> bool:
    task = asyncio.current_task()
    return task is not None and task.cancelling() > 0


def _is_expected_shutdown_cancellation(stop_event: asyncio.Event | None = None) -> bool:
    return _current_task_is_cancelling() or bool(stop_event is not None and stop_event.is_set())


def _worker_decrypt_failure_payload(exc: AccountSessionDecryptError) -> dict[str, str]:
    return {
        "status": "worker_down",
        "reason": "telegram_session_decrypt_failed",
        "resource": exc.resource,
        "identifier": exc.identifier,
        "decrypt_status": exc.status,
        "action": exc.action,
        "detail": str(exc),
    }


async def _publish_worker_down_snapshot(container, exc: AccountSessionDecryptError) -> None:
    await container.db.repos.runtime_snapshots.upsert_snapshot(
        RuntimeSnapshot(
            snapshot_type="worker_heartbeat",
            payload=_worker_decrypt_failure_payload(exc),
        )
    )


async def _load_active_accounts(container) -> list:
    active_accounts = []
    for getter_name in ("get_account_summaries", "get_accounts"):
        getter = getattr(container.db, getter_name, None)
        if not callable(getter):
            continue
        try:
            result = getter(active_only=True)
            if isawaitable(result):
                result = await result
            if isinstance(result, (list, tuple)):
                active_accounts = list(result)
                break
        except Exception:
            logger.debug("Failed to load accounts while publishing account status snapshot", exc_info=True)
    return active_accounts


def _resolve_available_phones(active_accounts: list, connected_phones: list[str], now: datetime) -> tuple[dict, list]:
    flood_waits = {}
    available_phones = []
    if active_accounts:
        connected_set = set(connected_phones)
        for account in active_accounts:
            phone = getattr(account, "phone", "")
            if str(getattr(account, "session_status", "ok")) != "ok":
                continue
            flood_until = normalize_utc(getattr(account, "flood_wait_until", None))
            if flood_until is not None and flood_until > now:
                flood_waits[phone] = flood_until.isoformat()
                continue
            if phone in connected_set:
                available_phones.append(phone)
    else:
        available_phones = list(connected_phones)
    return flood_waits, available_phones


def _resolve_pool_warming(pool) -> bool:
    is_warming_method = None
    pool_attrs = getattr(pool, "__dict__", {})
    if isinstance(pool_attrs, dict) and "is_warming" in pool_attrs:
        is_warming_method = pool_attrs["is_warming"]
    elif callable(getattr(type(pool), "is_warming", None)):
        is_warming_method = getattr(pool, "is_warming")
    return bool(is_warming_method()) if callable(is_warming_method) else False


def _resolve_backoffs(pool, connected_phones: list[str]) -> dict[str, str]:
    # Per-phone live-resolve backoff deadlines (#790) — surfaced for web parity.
    resolve_backoffs: dict[str, str] = {}
    get_backoff_until = getattr(pool, "get_resolve_username_backoff_until", None)
    if callable(get_backoff_until):
        for phone in connected_phones:
            try:
                until = get_backoff_until(phone)
            except TypeError:
                break
            if isinstance(until, datetime):
                resolve_backoffs[phone] = until.isoformat()
    return resolve_backoffs


async def _publish_worker_heartbeat_snapshot(container, now: datetime) -> None:
    await container.db.repos.runtime_snapshots.upsert_snapshot(
        RuntimeSnapshot(
            snapshot_type="worker_heartbeat",
            payload={"status": "alive", "timestamp": now.isoformat()},
        )
    )


async def _publish_accounts_status_snapshot(
    container,
    *,
    connected_phones: list[str],
    available_phones: list,
    flood_waits: dict,
    resolve_backoffs: dict[str, str],
    is_warming: bool,
    now: datetime,
) -> None:
    await container.db.repos.runtime_snapshots.upsert_snapshot(
        RuntimeSnapshot(
            snapshot_type="accounts_status",
            payload={
                "connected_phones": connected_phones,
                "connected_count": len(connected_phones),
                "available_phones": sorted(available_phones),
                "flood_waits": flood_waits,
                "resolve_backoffs": resolve_backoffs,
                "is_warming": is_warming,
                "timestamp": now.isoformat(),
            },
        )
    )


def _pool_counters_payload(pool) -> dict:
    dialogs_cache = getattr(pool, "_dialogs_cache", {})
    active_leases = getattr(pool, "_active_leases", {})
    premium_flood_waits = getattr(pool, "_premium_flood_wait_until", {})
    session_overrides = getattr(pool, "_session_overrides", {})
    return {
        "dialogs_cache_entries": (
            len(dialogs_cache) if hasattr(dialogs_cache, "__len__") else 0
        ),
        "active_leases": (
            {k: len(v) for k, v in active_leases.items()}
            if isinstance(active_leases, dict)
            else {}
        ),
        "premium_flood_waits": (
            len(premium_flood_waits)
            if hasattr(premium_flood_waits, "__len__")
            else 0
        ),
        "session_overrides": (
            len(session_overrides)
            if hasattr(session_overrides, "__len__")
            else 0
        ),
    }


async def _publish_pool_counters_snapshot(container) -> None:
    await container.db.repos.runtime_snapshots.upsert_snapshot(
        RuntimeSnapshot(
            snapshot_type="pool_counters",
            payload=_pool_counters_payload(container.pool),
        )
    )


async def _publish_collector_status_snapshot(container, connected_phones: list[str]) -> None:
    await container.db.repos.runtime_snapshots.upsert_snapshot(
        RuntimeSnapshot(
            snapshot_type="collector_status",
            payload={
                "is_running": bool(getattr(container.collector, "is_running", False)),
                "state": "healthy" if connected_phones else "no_connected_active",
                "retry_after_sec": None,
                "next_available_at_utc": None,
            },
        )
    )


async def _publish_scheduler_status_snapshot(container) -> None:
    await container.db.repos.runtime_snapshots.upsert_snapshot(
        RuntimeSnapshot(
            snapshot_type="scheduler_status",
            payload={
                "is_running": bool(getattr(container.scheduler, "is_running", False)),
                "interval_minutes": getattr(container.scheduler, "interval_minutes", 60),
            },
        )
    )


async def _publish_scheduler_jobs_snapshot(container) -> None:
    jobs = []
    if hasattr(container.scheduler, "get_potential_jobs"):
        jobs = await container.scheduler.get_potential_jobs()
    await container.db.repos.runtime_snapshots.upsert_snapshot(
        RuntimeSnapshot(snapshot_type="scheduler_jobs", payload={"jobs": jobs})
    )


def _collection_queue_status_payload(container, now: datetime) -> dict:
    queue = getattr(container, "collection_queue", None)
    active_task_ids = list(getattr(queue, "_active_task_ids", {}).keys()) if queue is not None else []
    target_worker_count = 0
    if queue is not None:
        getter = getattr(queue, "_target_worker_count", None)
        if callable(getter):
            target_worker_count = getter()
    return {
        "paused": bool(getattr(queue, "is_paused", False)) if queue is not None else False,
        "current_task_id": active_task_ids[0] if active_task_ids else None,
        "active_task_ids": active_task_ids,
        "running_count": len(active_task_ids),
        "target_worker_count": target_worker_count,
        "timestamp": now.isoformat(),
    }


async def _publish_collection_queue_status_snapshot(container, now: datetime) -> None:
    await container.db.repos.runtime_snapshots.upsert_snapshot(
        RuntimeSnapshot(
            snapshot_type="collection_queue_status",
            payload=_collection_queue_status_payload(container, now),
        )
    )


async def _notification_target_status_payload(container, stop_event: asyncio.Event | None) -> dict:
    target_status = await container.notification_target_service.describe_target()
    bot_payload = {"configured": False}
    if target_status.state == "available":
        try:
            bot = await NotificationService(
                container.db,
                container.notification_target_service,
                container.config.notifications.bot_name_prefix,
                container.config.notifications.bot_username_prefix,
            ).get_status()
        except asyncio.CancelledError:
            if _is_expected_shutdown_cancellation(stop_event):
                raise
            logger.warning("Notification bot snapshot refresh was cancelled; continuing worker")
        except asyncio.TimeoutError:
            logger.warning("Notification bot snapshot timed out (network); continuing")
        except Exception:
            logger.warning("Failed to refresh notification bot snapshot", exc_info=True)
        else:
            if bot is not None:
                bot_payload = {
                    "configured": True,
                    "bot_username": bot.bot_username,
                    "bot_id": bot.bot_id,
                    "created_at": bot.created_at.isoformat() if bot.created_at else None,
                }
    return {
        "target": asdict(target_status),
        "bot": bot_payload,
    }


async def _publish_notification_target_status_snapshot(
    container,
    stop_event: asyncio.Event | None,
) -> None:
    await container.db.repos.runtime_snapshots.upsert_snapshot(
        RuntimeSnapshot(
            snapshot_type="notification_target_status",
            payload=await _notification_target_status_payload(container, stop_event),
        )
    )


async def _publish_snapshots(container, *, stop_event: asyncio.Event | None = None) -> None:
    now = datetime.now(timezone.utc)
    connected_phones = sorted(getattr(container.pool, "clients", {}).keys())
    active_accounts = await _load_active_accounts(container)
    flood_waits, available_phones = _resolve_available_phones(active_accounts, connected_phones, now)
    is_warming = _resolve_pool_warming(container.pool)
    resolve_backoffs = _resolve_backoffs(container.pool, connected_phones)
    await _publish_worker_heartbeat_snapshot(container, now)
    await _publish_accounts_status_snapshot(
        container,
        connected_phones=connected_phones,
        available_phones=available_phones,
        flood_waits=flood_waits,
        resolve_backoffs=resolve_backoffs,
        is_warming=is_warming,
        now=now,
    )
    await _publish_pool_counters_snapshot(container)
    await _publish_collector_status_snapshot(container, connected_phones)
    await _publish_scheduler_status_snapshot(container)
    await _publish_scheduler_jobs_snapshot(container)
    await _publish_collection_queue_status_snapshot(container, now)
    await _publish_notification_target_status_snapshot(container, stop_event)


async def _run_worker_async(config: AppConfig) -> None:
    container = await build_worker_container(config, log_buffer=LogBuffer(maxlen=500))
    stop_event = asyncio.Event()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, stop_event.set)
        except NotImplementedError:
            pass

    try:
        await start_container(container)
    except AccountSessionDecryptError as exc:
        logger.error(
            "worker startup blocked: resource=%s identifier=%s status=%s action=%s",
            exc.resource,
            exc.identifier,
            exc.status,
            exc.action,
        )
        await _publish_worker_down_snapshot(container, exc)
        await stop_container(container)
        return
    publish_timeout = resolve_snapshot_publish_timeout(config.scheduler.snapshot_publish_timeout_sec)
    try:
        while not stop_event.is_set():
            try:
                await asyncio.wait_for(
                    _publish_snapshots(container, stop_event=stop_event),
                    timeout=publish_timeout,
                )
            except asyncio.CancelledError:
                if _current_task_is_cancelling():
                    raise
                if stop_event.is_set():
                    logger.info("Worker stopping during snapshot publish")
                    break
                logger.warning("Worker snapshot publish was cancelled; continuing worker loop")
            except asyncio.TimeoutError:
                logger.warning(
                    "Worker snapshot publish timed out after %.1fs; continuing worker loop",
                    publish_timeout,
                )
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=HEARTBEAT_INTERVAL_SEC)
            except asyncio.TimeoutError:
                continue
    finally:
        await stop_container(container)


def run_worker(config: AppConfig) -> None:
    asyncio.run(_run_worker_async(config))
