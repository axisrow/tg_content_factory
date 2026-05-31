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

# The worker publishes `worker_heartbeat` every ~5s
# (src/runtime/worker.py:_publish_snapshots). Anything older than this means the
# worker process is effectively down.
WORKER_HEARTBEAT_STALE_AFTER_SEC = 60.0


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
