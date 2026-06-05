"""Grounded runtime diagnostics shared by the web worker banner and agent tools
(#530).

The recurring failure mode is treating a *snapshot* of the worker (or a stale
heartbeat) as proof that a Telegram account is reachable. This module is the one
place that evaluates worker-heartbeat freshness, so the Settings banner and the
agent's `get_runtime_diagnostics` tool report the same thing and cannot drift.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

# How often the worker (both the standalone `src/runtime/worker.py` and the
# embedded `src/web/embedded_worker.py`) republishes `worker_heartbeat` and the
# other runtime snapshots. Single source of truth so the two worker flavours
# cannot drift apart.
HEARTBEAT_INTERVAL_SEC = 5.0

# The worker publishes `worker_heartbeat` every ~5s
# (src/runtime/worker.py:_publish_snapshots). Anything older than this means the
# worker process is effectively down. 60s = 12 missed beats of slack.
WORKER_HEARTBEAT_STALE_AFTER_SEC = 60.0

# Floor for "a RUNNING channel-collect task whose progress hasn't advanced is
# stuck". The effective threshold is derived per-request as
# max(RUNNING_TASK_STALE_AFTER_SEC, collection_stream_timeout_sec + grace) — see
# running_task_stale_after() — so raising the per-channel idle timeout in config
# automatically pushes the "stuck" verdict out instead of the UI crying stuck
# while the collector is still legally waiting for the next post.
RUNNING_TASK_STALE_AFTER_SEC = 300.0

# Extra slack on top of the per-channel idle timeout before a stalled task is
# called stuck — covers between-channel pauses and one in-flight DB flush.
RUNNING_TASK_STALE_GRACE_SEC = 60.0

# Default snapshot-publish timeout (asyncio.wait_for around _publish_snapshots).
# A configured 0/negative/garbage value is coerced back to this by
# resolve_snapshot_publish_timeout(), because wait_for(timeout<=0) would abort
# every heartbeat on its first pass and silently flip the UI to worker_down.
DEFAULT_SNAPSHOT_PUBLISH_TIMEOUT_SEC = 30.0


def running_task_stale_after(idle_timeout_sec: float | None) -> float:
    """Threshold (seconds) past which a non-progressing RUNNING task is stuck.

    Derived from the per-channel idle timeout so the two stay in lock-step: the
    collector may legitimately wait up to ``collection_stream_timeout_sec`` for
    the next post, so "stuck" must be at least that plus grace. A disabled
    (0/None) idle timeout leaves only the static floor.
    """
    try:
        idle = float(idle_timeout_sec) if idle_timeout_sec else 0.0
    except (TypeError, ValueError):
        idle = 0.0
    if idle <= 0:
        return RUNNING_TASK_STALE_AFTER_SEC
    return max(RUNNING_TASK_STALE_AFTER_SEC, idle + RUNNING_TASK_STALE_GRACE_SEC)


def resolve_snapshot_publish_timeout(value: object) -> float:
    """Coerce a configured snapshot-publish timeout to a safe positive float.

    0, negative, None or non-numeric all fall back to
    DEFAULT_SNAPSHOT_PUBLISH_TIMEOUT_SEC so a misconfiguration can never make
    asyncio.wait_for abort every heartbeat instantly.
    """
    try:
        timeout = float(value)  # type: ignore[arg-type]
    except (TypeError, ValueError):
        return DEFAULT_SNAPSHOT_PUBLISH_TIMEOUT_SEC
    if timeout <= 0:
        return DEFAULT_SNAPSHOT_PUBLISH_TIMEOUT_SEC
    return timeout


@dataclass(frozen=True)
class WorkerHeartbeatHealth:
    alive: bool
    status: str  # "alive"/"ok"/"missing"/<worker-reported status>
    reason: str  # explicit worker-reported failure detail, else ""
    age_sec: float | None  # seconds since last heartbeat, None if unknown
    stale: bool  # heartbeat present but older than the staleness window


def evaluate_worker_heartbeat(
    snapshot: object,
    *,
    now: datetime | None = None,
    stale_after_sec: float = WORKER_HEARTBEAT_STALE_AFTER_SEC,
) -> WorkerHeartbeatHealth:
    """Classify a ``worker_heartbeat`` runtime snapshot into health state.

    ``reason`` is only populated for an explicit worker-reported failure status
    (so callers that surface a banner reason keep their prior behaviour); a
    missing or merely stale heartbeat carries its signal in ``status``/``stale``.
    """
    now = now or datetime.now(timezone.utc)

    updated_at = getattr(snapshot, "updated_at", None)
    if snapshot is None or updated_at is None:
        return WorkerHeartbeatHealth(False, "missing", "", None, False)

    payload = getattr(snapshot, "payload", None)
    payload = payload if isinstance(payload, dict) else {}
    status = str(payload.get("status", "alive"))
    if status not in {"alive", "ok"}:
        reason = str(payload.get("detail", "") or payload.get("reason", ""))
        return WorkerHeartbeatHealth(False, status, reason, None, False)

    if updated_at.tzinfo is None:
        updated_at = updated_at.replace(tzinfo=timezone.utc)
    age = (now - updated_at).total_seconds()
    if age > stale_after_sec:
        return WorkerHeartbeatHealth(False, status, "", age, True)
    return WorkerHeartbeatHealth(True, status, "", age, False)
