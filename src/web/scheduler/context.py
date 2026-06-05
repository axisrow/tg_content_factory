"""Context construction for the scheduler web page.

Pure presentation helpers, worker-health probing, and the heavy collector-health /
jobs / pipeline-result context builders that the scheduler page renders. Extracted
from ``src/web/routes/scheduler.py`` so route functions no longer own heavy context
construction (#654).
"""

from __future__ import annotations

import asyncio
import logging
import re
from datetime import datetime, timezone

from fastapi import Request

from src.models import AccountSessionStatus
from src.services.pipeline_result import result_kind_label
from src.services.runtime_diagnostics import (
    WORKER_HEARTBEAT_STALE_AFTER_SEC as WORKER_HEARTBEAT_STALE_AFTER_SEC_SVC,
)
from src.services.runtime_diagnostics import (
    evaluate_worker_heartbeat,
    running_task_stale_after,
)
from src.telegram.flood_wait import (
    is_blocking_flood_wait_until,
    is_transient_flood_wait_seconds,
)
from src.web import deps

logger = logging.getLogger(__name__)
_PIPELINE_RUN_NOTE_RE = re.compile(r"Pipeline run id=(\d+)")

JOB_LABELS = {
    "collect_all": "Сбор всех каналов",
    "photo_due": "Фото по расписанию",
    "photo_auto": "Автозагрузка фото",
    "warm_all_dialogs": "Прогрев кэша диалогов",
}

# Worker publishes `worker_heartbeat` every ~5s (src/runtime/worker.py:_publish_snapshots).
# 60s gives us 12 missed publishes before we conclude the worker is down. The
# staleness window + classification now live in src.services.runtime_diagnostics
# so the agent's get_runtime_diagnostics tool stays in lock-step with this banner.
WORKER_HEARTBEAT_STALE_AFTER_SEC = WORKER_HEARTBEAT_STALE_AFTER_SEC_SVC


def _is_running_task_stale(
    running_task,
    *,
    idle_timeout_sec: float | None = None,
    now: datetime | None = None,
) -> bool:
    """True when a RUNNING task's progress hasn't advanced for too long.

    'Stuck' is decided by stalled progress, not by the mere existence of a
    RUNNING row: a worker that simply crashed leaves an orphaned RUNNING row
    whose progress clock is recent, and that must read as `worker_down`, not
    `collector_stuck`. Falls back to `started_at` when no batch has flushed yet,
    and treats an unknown timestamp as NOT stale (we can't prove it's stuck).

    The threshold is derived from ``idle_timeout_sec`` (the per-channel
    collection_stream_timeout_sec) so it tracks how long the collector is
    actually allowed to wait for the next post — a large idle timeout pushes the
    'stuck' verdict out instead of firing while the worker still legally waits.
    """
    if running_task is None:
        return False
    marker = running_task.last_progress_at or running_task.started_at
    if marker is None:
        return False
    now = now or datetime.now(timezone.utc)
    if marker.tzinfo is None:
        marker = marker.replace(tzinfo=timezone.utc)
    return (now - marker).total_seconds() > running_task_stale_after(idle_timeout_sec)


def _job_label(job_id: str) -> str:
    if job_id in JOB_LABELS:
        return JOB_LABELS[job_id]
    if job_id.startswith("sq_"):
        return f"Стат. запроса #{job_id.removeprefix('sq_')}"
    if job_id.startswith("pipeline_run_"):
        return f"Пайплайн #{job_id.removeprefix('pipeline_run_')}"
    if job_id.startswith("content_generate_"):
        return f"Генерация #{job_id.removeprefix('content_generate_')}"
    return job_id


def format_task_result(task) -> str:
    """Compact display for the scheduler task result column."""
    collected = int(task.messages_collected or 0)
    if task.task_type.value == "channel_collect":
        payload = task.payload if isinstance(task.payload, dict) else {}
        raw_total = payload.get("messages_total")
        try:
            total = int(raw_total) if raw_total is not None else None
        except (TypeError, ValueError):
            total = None
        if total is not None and total >= collected:
            return f"{collected}/{total}"
    return str(collected)


def _format_retry_hint(run_after) -> str:
    if run_after is None:
        return ""
    return run_after.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def _compute_load_level(
    *,
    interval_minutes: int,
    active_unfiltered_channels: int,
    available_accounts_now: int,
    state: str,
) -> str:
    if state in {"worker_down", "collector_stuck", "all_flooded", "no_clients", "session_degraded"}:
        return "overload"
    capacity_accounts = max(1, available_accounts_now)
    pressure = active_unfiltered_channels / capacity_accounts
    if interval_minutes <= 15 and pressure >= 60:
        return "overload"
    if interval_minutes <= 30 and pressure >= 40:
        return "high"
    if pressure >= 75:
        return "high"
    return "ok"


def _current_status_presentation(state: str, *, is_running: bool) -> tuple[str, str, str]:
    if state == "worker_down":
        return "Telegram-воркер не запущен", "Задачи копятся в БД, но воркер их не исполняет.", "danger"
    if state == "collector_stuck":
        return "Сбор завис", "Текущая задача не завершилась, остальные задачи ждут.", "danger"
    if state == "all_flooded":
        return "Все аккаунты во Flood Wait", "Сбор заблокирован до ближайшего окна доступности.", "danger"
    if state == "no_clients":
        return "Нет доступных клиентов", "Нет подключенного активного Telegram-аккаунта для сбора.", "danger"
    if state == "session_degraded":
        return "SESSION_ENCRYPTION_KEY не подходит", "Активные сессии не расшифровываются текущим ключом.", "danger"
    if state == "degraded":
        return "Частичная деградация", "Часть аккаунтов временно ограничена, но сбор может продолжаться.", "warning"
    if is_running:
        return "Сбор идёт", "Коллектор выполняет текущую задачу.", "success"
    return "Коллектор ждёт", "Блокеров сбора сейчас нет.", "success"


def _load_presentation(load_level: str) -> tuple[str, str]:
    if load_level == "overload":
        return "Риск перегрузки", "warning"
    if load_level == "high":
        return "Высокая нагрузка", "warning"
    return "Нагрузка в норме", "success"


def _collector_health_border_severity(*, state: str, load_level: str) -> str:
    if state in {"worker_down", "collector_stuck", "all_flooded", "no_clients", "session_degraded"}:
        return "danger"
    if state == "degraded" or load_level in {"high", "overload"}:
        return "warning"
    return "success"


def _collector_health_recommendations(
    *,
    state: str,
    load_level: str,
    interval_minutes: int,
    active_unfiltered_channels: int,
    available_accounts_now: int,
    is_running: bool = False,
    pending_count: int = 0,
) -> list[str]:
    recommendations: list[str] = []
    if state == "worker_down":
        recommendations.append(
            "Telegram-воркер не запущен. Если используете `serve` — перезапустите его; "
            "если `serve --no-worker` — запустите воркер отдельно: `python -m src.main worker`. "
            "Без воркера задачи сбора копятся в БД, но не исполняются."
        )
    if state == "collector_stuck":
        recommendations.append(
            "Текущая задача сбора зависла. Перезапустите `serve` или отдельный воркер, "
            "затем проверьте, что очередь снова разбирается."
        )
    if state == "all_flooded":
        recommendations.append("Дождаться ближайшего окна после Flood Wait и не запускать ручной collect-all повторно.")
    if state == "no_clients":
        recommendations.append("Проверить активность аккаунтов и переподключить хотя бы один рабочий клиент.")
    if state == "session_degraded":
        recommendations.append(
            "Восстановить прежний SESSION_ENCRYPTION_KEY или повторно войти в Telegram-аккаунты."
        )
    if load_level in {"high", "overload"}:
        recommendations.append(
            f"Поднять интервал автосбора выше текущих {interval_minutes} мин, чтобы снизить частоту обращений."
        )
        recommendations.append(
            "Сократить число активных отслеживаемых каналов: "
            f"сейчас активных неотфильтрованных {active_unfiltered_channels}."
        )
    if available_accounts_now <= 1:
        recommendations.append("Добавить ещё Telegram-аккаунты, чтобы распределить нагрузку по чтению.")
    if is_running and pending_count > 0:
        recommendations.append(
            "Дождаться завершения текущего первичного сбора; "
            "не запускать collect-all вручную, пока очередь не разгребётся."
        )
    return recommendations


def _dedupe_recent_unavailability_events(recent_tasks) -> list[dict[str, object]]:
    events: dict[str, dict[str, object]] = {}
    for task in recent_tasks:
        message = ""
        if task.note and "Flood Wait" in task.note:
            message = task.note
        elif task.note and "нет подключённых активных аккаунтов" in task.note:
            message = task.note
        elif task.error and "No active connected clients" in task.error:
            message = task.error
        if not message:
            continue
        item = events.setdefault(message, {"message": message, "count": 0, "latest_at": None})
        item["count"] = int(item["count"]) + 1
        occurred_at = _as_utc_datetime(task.completed_at or task.started_at or task.created_at)
        if occurred_at is not None:
            latest_at = item["latest_at"]
            if latest_at is None or occurred_at > latest_at:
                item["latest_at"] = occurred_at
    return sorted(
        events.values(),
        key=lambda item: item["latest_at"] or datetime.min.replace(tzinfo=timezone.utc),
        reverse=True,
    )[:5]


def _as_utc_datetime(value) -> datetime | None:
    if not isinstance(value, datetime):
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


async def _worker_status(db) -> tuple[bool, str]:
    """Return True when the worker-process heartbeat snapshot is fresh.

    The worker publishes `worker_heartbeat` every ~5s
    (`src/runtime/worker.py:_publish_snapshots`). We treat anything older than
    `WORKER_HEARTBEAT_STALE_AFTER_SEC` as the worker being down — that turns the
    silent failure from #444 (serve running alone, collection tasks piling up
    with no executor) into an explicit banner.
    """
    try:
        snapshot = await db.repos.runtime_snapshots.get_snapshot("worker_heartbeat")
    except Exception as exc:  # noqa: BLE001
        # Fail closed (#530): a snapshot read failure means we cannot prove the
        # worker is alive, so report it as down — matching the agent tool's
        # get_runtime_diagnostics, which passes snapshot=None to
        # evaluate_worker_heartbeat and gets status="missing". Returning True
        # here (fail-open) would let the two surfaces drift: the banner would say
        # "alive" while the agent says "missing" from the same DB error, defeating
        # the single-source-of-truth goal and suppressing the very #444 banner.
        logger.warning("Failed to read worker_heartbeat snapshot: %s", exc)
        return False, ""
    health = evaluate_worker_heartbeat(snapshot, stale_after_sec=WORKER_HEARTBEAT_STALE_AFTER_SEC)
    return health.alive, health.reason


async def _is_worker_alive(db) -> bool:
    alive, _ = await _worker_status(db)
    return alive


async def _build_collector_health_context(request: Request) -> dict[str, object]:
    db = deps.get_db(request)
    pool = deps.get_pool(request)
    collector = deps.get_collector(request)
    accounts = await db.get_account_summaries(active_only=False)
    connected_phones = set(pool.clients.keys())
    degraded_session_accounts = [
        acc for acc in accounts if acc.is_active and acc.session_status != AccountSessionStatus.OK
    ]
    active_accounts = [
        acc for acc in accounts if acc.is_active and acc.session_status == AccountSessionStatus.OK
    ]
    connected_active_accounts = [acc for acc in active_accounts if acc.phone in connected_phones]
    now = datetime.now(timezone.utc)
    worker_alive, worker_reason = await _worker_status(db)

    flooded_accounts = []
    next_available_at = None
    for acc in connected_active_accounts:
        flood_until = acc.flood_wait_until
        if flood_until is None:
            continue
        if flood_until.tzinfo is None:
            flood_until = flood_until.replace(tzinfo=timezone.utc)
        if flood_until <= now or not is_blocking_flood_wait_until(flood_until, now=now):
            continue
        flooded_accounts.append({"phone": acc.phone, "until": flood_until})
        if next_available_at is None or flood_until < next_available_at:
            next_available_at = flood_until

    try:
        availability = await asyncio.wait_for(collector.get_collection_availability(), timeout=1.0)
    except (asyncio.TimeoutError, Exception) as exc:  # noqa: BLE001
        logger.warning("get_collection_availability timed out or failed: %s", exc)
        availability = None
    availability_state = getattr(availability, "state", "no_connected_active")
    availability_retry_after = getattr(availability, "retry_after_sec", None)
    availability_next = getattr(availability, "next_available_at_utc", None)
    if not isinstance(availability_next, datetime):
        availability_next = None
    if not isinstance(availability_retry_after, int):
        availability_retry_after = None
    availability_all_flooded_blocking = availability_state == "all_flooded" and not (
        is_transient_flood_wait_seconds(availability_retry_after)
    )
    available_accounts_now = max(0, len(connected_active_accounts) - len(flooded_accounts))
    active_unfiltered_channels = len(await db.repos.channels.get_channels(active_only=True, include_filtered=False))
    recent_tasks = await db.get_collection_tasks(limit=200)
    recent_zero_collect_count = sum(
        1
        for task in recent_tasks
        if task.task_type.value == "channel_collect"
        and task.status == "completed"
        and task.messages_collected == 0
    )
    recent_unavailability_events = _dedupe_recent_unavailability_events(recent_tasks)
    pending_channel_tasks = await db.get_pending_channel_tasks()
    running_tasks = [
        task
        for task in recent_tasks
        if task.task_type.value == "channel_collect" and task.status.value == "running"
    ]
    running_task = running_tasks[0] if running_tasks else None
    running_count = len(running_tasks)

    # Derive the "stuck" threshold from the configured per-channel idle timeout
    # so the two stay in lock-step (a large idle timeout must not trip a false
    # "stuck"). running_task_stale_after() coerces None/garbage to the floor.
    idle_timeout_sec = deps.get_container(request).config.scheduler.collection_stream_timeout_sec
    running_task_stale = _is_running_task_stale(running_task, idle_timeout_sec=idle_timeout_sec)
    # A task that is RUNNING and actually making progress (recent
    # last_progress_at). Used instead of "any RUNNING row exists" so the UI
    # never claims a collection is in flight when the row is an orphan left by
    # a crashed worker.
    task_is_progressing = worker_alive and running_task is not None and not running_task_stale
    collector_is_running = worker_alive and (collector.is_running or task_is_progressing)
    state = "healthy"
    if not worker_alive:
        # Worker-process absent dominates: without it `no_clients` /
        # `all_flooded` are symptoms, not the root cause.
        state = "worker_down"
    elif running_task_stale:
        # Genuinely stuck: a live worker has a RUNNING task whose progress
        # hasn't advanced for a long time.
        state = "collector_stuck"
    elif degraded_session_accounts and not active_accounts:
        state = "session_degraded"
    elif not connected_active_accounts:
        state = "no_clients"
    elif availability_all_flooded_blocking or available_accounts_now == 0:
        state = "all_flooded"
    elif flooded_accounts:
        state = "degraded"

    interval_minutes = max(1, getattr(deps.get_scheduler(request), "interval_minutes", 60))
    load_level = _compute_load_level(
        interval_minutes=interval_minutes,
        active_unfiltered_channels=active_unfiltered_channels,
        available_accounts_now=available_accounts_now,
        state=state,
    )
    current_status_label, current_status_detail, current_status_severity = _current_status_presentation(
        state, is_running=collector_is_running
    )
    load_label, load_severity = _load_presentation(load_level)
    capacity_accounts = max(1, available_accounts_now)
    channels_per_account = active_unfiltered_channels // capacity_accounts
    capacity_label = (
        f"{active_unfiltered_channels} каналов на {capacity_accounts} "
        f"{'аккаунт' if capacity_accounts == 1 else 'аккаунта'}, интервал {interval_minutes} мин"
    )
    capacity_detail = (
        f"Около {channels_per_account} каналов на доступный аккаунт; "
        f"в очереди {len(pending_channel_tasks)} задач."
    )
    # Compute retry_after_sec from next_available_at if available
    computed_retry_after_sec = availability_retry_after
    if computed_retry_after_sec is None and (next_available_at or availability_next):
        effective_next = next_available_at or availability_next
        delta = effective_next - now
        computed_retry_after_sec = max(0, int(delta.total_seconds()))
    return {
        "state": state,
        "connected_accounts": len(connected_phones),
        "active_accounts": len(active_accounts),
        "session_degraded_accounts": len(degraded_session_accounts),
        "worker_reason": worker_reason,
        "available_accounts_now": available_accounts_now,
        "flooded_accounts": flooded_accounts,
        "flooded_accounts_count": len(flooded_accounts),
        "next_available_at": next_available_at or availability_next,
        "retry_after_sec": computed_retry_after_sec,
        "active_unfiltered_channels": active_unfiltered_channels,
        "collect_interval_minutes": interval_minutes,
        "load_level": load_level,
        "recommendations": _collector_health_recommendations(
            state=state,
            load_level=load_level,
            interval_minutes=interval_minutes,
            active_unfiltered_channels=active_unfiltered_channels,
            available_accounts_now=available_accounts_now,
            is_running=collector_is_running,
            pending_count=len(pending_channel_tasks),
        ),
        "current_status_label": current_status_label,
        "current_status_detail": current_status_detail,
        "current_status_severity": current_status_severity,
        "health_border_severity": _collector_health_border_severity(state=state, load_level=load_level),
        "load_label": load_label,
        "load_severity": load_severity,
        "capacity_label": capacity_label,
        "capacity_detail": capacity_detail,
        "queue_pending_count": len(pending_channel_tasks),
        "running_task_title": running_task.channel_title if running_task else "",
        "running_task_messages_collected": running_task.messages_collected if running_task else 0,
        "running_count": running_count,
        "recent_zero_collect_count": recent_zero_collect_count,
        "recent_unavailability_events": recent_unavailability_events,
        "is_running": collector_is_running,
        "next_available_label": _format_retry_hint(next_available_at or availability_next),
    }


async def _build_jobs_context(sched, db) -> list[dict]:
    """Build a list of job dicts for the scheduler page template."""
    # Always fetch potential jobs to get DB-sourced interval_minutes
    potential = await sched.get_potential_jobs()
    potential_map = {j["job_id"]: j for j in potential}

    if sched.is_running:
        raw = sched.get_all_jobs_next_run()
        jobs = []
        for job_id, next_run in raw.items():
            db_interval = potential_map.get(job_id, {}).get("interval_minutes")
            jobs.append({
                "job_id": job_id,
                "label": _job_label(job_id),
                "next_run": next_run,
                "interval_minutes": db_interval,
            })
        # Also include disabled jobs (not in APScheduler but exist in potential_map)
        running_ids = set(raw.keys())
        for j in potential:
            if j["job_id"] not in running_ids:
                jobs.append({
                    "job_id": j["job_id"],
                    "label": _job_label(j["job_id"]),
                    "next_run": None,
                    "interval_minutes": j["interval_minutes"],
                })
    else:
        jobs = [
            {
                "job_id": j["job_id"],
                "label": _job_label(j["job_id"]),
                "next_run": None,
                "interval_minutes": j["interval_minutes"],
            }
            for j in potential
        ]

    disabled_map = await db.repos.settings.get_settings_by_prefix("scheduler_job_disabled:")
    for j in jobs:
        val = disabled_map.get(f"scheduler_job_disabled:{j['job_id']}")
        j["enabled"] = val != "1"
        j["interval_editable"] = j["job_id"] not in ("photo_due", "photo_auto")
    return jobs


async def _notification_snapshot_payload(request: Request) -> dict[str, object]:
    snapshot = await deps.get_db(request).repos.runtime_snapshots.get_snapshot("notification_target_status")
    payload = snapshot.payload if snapshot is not None else {}
    return payload if isinstance(payload, dict) else {}


async def _load_pipeline_run_result_meta(db, tasks) -> dict[int, dict[str, object]]:
    run_ids_by_task_id: dict[int, int] = {}
    for task in tasks:
        if task.id is None or task.task_type.value != "pipeline_run" or not task.note:
            continue
        match = _PIPELINE_RUN_NOTE_RE.search(task.note)
        if match is None:
            continue
        run_ids_by_task_id[task.id] = int(match.group(1))
    if not run_ids_by_task_id:
        return {}

    runs = await asyncio.gather(*(db.repos.generation_runs.get(run_id) for run_id in run_ids_by_task_id.values()))
    result: dict[int, dict[str, object]] = {}
    for task_id, run in zip(run_ids_by_task_id.keys(), runs, strict=False):
        if run is None:
            continue
        metadata = run.metadata if isinstance(run.metadata, dict) else {}
        raw_errors = metadata.get("node_errors")
        node_errors = raw_errors if isinstance(raw_errors, list) else []
        errors_count = len(node_errors)
        first_error_detail: str | None = None
        if node_errors and isinstance(node_errors[0], dict):
            detail = node_errors[0].get("detail")
            if isinstance(detail, str):
                first_error_detail = detail
        result[task_id] = {
            "kind": run.result_kind,
            "count": run.result_count,
            "label": result_kind_label(run.result_kind),
            "errors_count": errors_count,
            "first_error_detail": first_error_detail,
        }
    return result
