from __future__ import annotations

import asyncio
import logging
import signal
from datetime import datetime, timezone
from typing import Any

from tg_agent.config import AppConfig
from tg_agent.database import DatabaseBusyError
from tg_agent.database.repositories.accounts import AccountSessionDecryptError
from tg_agent.models import RuntimeSnapshot
from tg_agent.runtime.bootstrap import build_worker_container, start_container, stop_container
from tg_agent.services.runtime_diagnostics import HEARTBEAT_INTERVAL_SEC, resolve_snapshot_publish_timeout
from tg_agent.telegram.flood_snapshot import FLOOD_GUARD_PERSIST_INTERVAL_SEC

logger = logging.getLogger(__name__)
# HEARTBEAT_INTERVAL_SEC is the single source of truth in runtime_diagnostics.
# The snapshot-publish timeout comes from config
# (scheduler.snapshot_publish_timeout_sec).


def _current_task_is_cancelling() -> bool:
    task = asyncio.current_task()
    return task is not None and task.cancelling() > 0


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


async def _publish_worker_heartbeat_snapshot(container, now: datetime) -> None:
    await container.db.repos.runtime_snapshots.upsert_snapshot(
        RuntimeSnapshot(
            snapshot_type="worker_heartbeat",
            payload={"status": "alive", "timestamp": now.isoformat()},
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
    getter = getattr(container.scheduler, "get_all_jobs_next_run", None)
    next_runs = getter() if callable(getter) else {}
    for job in jobs:
        next_run = next_runs.get(job.get("job_id"))
        job["next_run"] = next_run.isoformat() if next_run else None
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


async def _publish_dm_listener_status_snapshot(
    container, dm_listener, now: datetime
) -> None:
    """Статус слушателя входящих DM (#1427) — через runtime_snapshots, по назначению.

    Сам журнал сообщений живёт в таблице `incoming_dms` (порядок и история),
    сюда попадает только живость/счётчик неразобранного. Публикация заодно
    дочищает протухший журнал: prune-on-write работает, только пока приходят
    новые записи, а heartbeat (каждые ~5с при живом воркере) снимает протухшее
    и из затихших диалогов — гарант «приватный текст не дольше TTL, пока жив
    воркер» (ревью #1440).
    """
    await container.db.repos.incoming_dms.prune_expired()
    unprocessed = await container.db.repos.incoming_dms.count_unprocessed()
    payload: dict[str, Any] = {
        **dm_listener.status(),
        "unprocessed": unprocessed,
        "timestamp": now.isoformat(),
    }
    catchup = getattr(container, "dm_catchup", None)
    if catchup is not None:
        payload["catchup"] = catchup.status()
    await container.db.repos.runtime_snapshots.upsert_snapshot(
        RuntimeSnapshot(
            snapshot_type="dm_listener_status",
            payload=payload,
        )
    )


async def _publish_snapshots(container, *, stop_event: asyncio.Event | None = None) -> None:
    """Publish the snapshots CLI readers use (``scheduler status``, ``dm``, jobs).

    Only snapshots someone reads are written (#1868): the web panel's
    accounts/pool/collector/notification-target snapshots are gone — the last
    one called ``get_me`` on the notification account every heartbeat, i.e.
    ~720 Telegram requests an hour that nothing read.
    """
    now = datetime.now(timezone.utc)
    await _publish_worker_heartbeat_snapshot(container, now)
    await _publish_scheduler_status_snapshot(container)
    await _publish_scheduler_jobs_snapshot(container)
    await _publish_collection_queue_status_snapshot(container, now)
    # Last: предыдущие снапшоты к этому моменту уже опубликованы, так что
    # сбой здесь их не отсекает; сам вылет уходит в heartbeat-цикл, где
    # обрабатывается (DatabaseBusyError/Exception) и републикуется через 5с.
    dm_listener = getattr(container, "dm_listener", None)
    if dm_listener is not None:
        await _publish_dm_listener_status_snapshot(container, dm_listener, now)


async def _run_worker_async(config: AppConfig) -> None:
    container = await build_worker_container(config)
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
    relay = await _start_owner_relay(container, config)
    publish_timeout = resolve_snapshot_publish_timeout(config.scheduler.snapshot_publish_timeout_sec)
    flood_guards_saved_at = loop.time()
    try:
        while not stop_event.is_set():
            if loop.time() - flood_guards_saved_at >= FLOOD_GUARD_PERSIST_INTERVAL_SEC:
                flood_guards_saved_at = loop.time()
                await _persist_flood_guards(container)
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
            except DatabaseBusyError:
                # Transient lock — back off quietly. A full traceback every
                # heartbeat (~5s) floods the log while contended; the next
                # heartbeat republishes the snapshot. Matches embedded_worker.
                logger.warning("Worker DB busy publishing snapshots; will retry next heartbeat")
            except Exception:
                # Snapshot publishing must never crash the loop — the worker
                # is still processing queued tasks even if snapshots fail.
                logger.exception("Worker _publish_snapshots failed")
            try:
                # asyncio.timeout, not wait_for: the 3.11 wait_for can leak an
                # unreleased waiter when the event fires as the timeout lands
                # (fixed in 3.12) — the heartbeat loop would sleep forever.
                async with asyncio.timeout(HEARTBEAT_INTERVAL_SEC):
                    await stop_event.wait()
            except TimeoutError:
                continue
    finally:
        if relay is not None:
            await relay.stop()
        await stop_container(container)


async def _persist_flood_guards(container) -> None:
    """Save the pool's flood-guard state, so a restart keeps the spent budget."""
    persist = getattr(container.pool, "persist_flood_guard_snapshot", None)
    if callable(persist):
        await persist()  # best-effort by contract: it never raises


async def _start_owner_relay(container, config: AppConfig):
    """Serve this worker's pool and queue wake-ups to the project's CLI processes (#1868).

    In gateway mode the pool itself runs on the gateway's relay; the CLI goes to
    the gateway directly and uses this relay only to wake the worker's queues.
    """
    from tg_agent.telegram.client_pool import ClientPool
    from tg_agent.telegram.relay import relay_endpoint_path, serve_pool_relay

    if not isinstance(container.pool, ClientPool):
        return None
    try:
        return await serve_pool_relay(
            container.pool, relay_endpoint_path(config.database.path), ops={"wake": _wake_op}
        )
    except OSError:
        logger.exception("session owner relay failed to start; CLI live commands will be refused")
        return None


async def _wake_op(_grant, message: dict, _push) -> bool:
    """Relay op: a CLI process enqueued work for one of this worker's queues."""
    from tg_agent.runtime import wake

    queue = str(message.get("queue") or "")
    if queue not in wake.QUEUES:
        raise ValueError(f"unknown queue {queue!r}")
    wake.notify_local(queue)
    return True


def run_worker(config: AppConfig) -> None:
    asyncio.run(_run_worker_async(config))
