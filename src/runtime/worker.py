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
from src.telegram.utils import normalize_utc
from src.web.bootstrap import build_worker_container, start_container, stop_container
from src.web.log_handler import LogBuffer

logger = logging.getLogger(__name__)


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


async def _publish_snapshots(container) -> None:
    now = datetime.now(timezone.utc)
    connected_phones = sorted(getattr(container.pool, "clients", {}).keys())
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
    await container.db.repos.runtime_snapshots.upsert_snapshot(
        RuntimeSnapshot(
            snapshot_type="worker_heartbeat",
            payload={"status": "alive", "timestamp": now.isoformat()},
        )
    )
    await container.db.repos.runtime_snapshots.upsert_snapshot(
        RuntimeSnapshot(
            snapshot_type="accounts_status",
            payload={
                "connected_phones": connected_phones,
                "connected_count": len(connected_phones),
                "available_phones": sorted(available_phones),
                "flood_waits": flood_waits,
                "timestamp": now.isoformat(),
            },
        )
    )
    pool = container.pool
    dialogs_cache = getattr(pool, "_dialogs_cache", {})
    active_leases = getattr(pool, "_active_leases", {})
    premium_flood_waits = getattr(pool, "_premium_flood_wait_until", {})
    session_overrides = getattr(pool, "_session_overrides", {})
    await container.db.repos.runtime_snapshots.upsert_snapshot(
        RuntimeSnapshot(
            snapshot_type="pool_counters",
            payload={
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
            },
        )
    )
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
    await container.db.repos.runtime_snapshots.upsert_snapshot(
        RuntimeSnapshot(
            snapshot_type="scheduler_status",
            payload={
                "is_running": bool(getattr(container.scheduler, "is_running", False)),
                "interval_minutes": getattr(container.scheduler, "interval_minutes", 60),
            },
        )
    )
    jobs = []
    if hasattr(container.scheduler, "get_potential_jobs"):
        jobs = await container.scheduler.get_potential_jobs()
    await container.db.repos.runtime_snapshots.upsert_snapshot(
        RuntimeSnapshot(snapshot_type="scheduler_jobs", payload={"jobs": jobs})
    )
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
    await container.db.repos.runtime_snapshots.upsert_snapshot(
        RuntimeSnapshot(
            snapshot_type="notification_target_status",
            payload={
                "target": asdict(target_status),
                "bot": bot_payload,
            },
        )
    )


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
    try:
        while not stop_event.is_set():
            await _publish_snapshots(container)
            try:
                await asyncio.wait_for(stop_event.wait(), timeout=5.0)
            except asyncio.TimeoutError:
                continue
    finally:
        await stop_container(container)


def run_worker(config: AppConfig) -> None:
    asyncio.run(_run_worker_async(config))
