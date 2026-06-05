"""Unit tests for the shared worker-heartbeat health evaluator (#530)."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from src.services.runtime_diagnostics import (
    DEFAULT_SNAPSHOT_PUBLISH_TIMEOUT_SEC,
    WORKER_HEARTBEAT_STALE_AFTER_SEC,
    evaluate_worker_heartbeat,
    resolve_snapshot_publish_timeout,
)


def test_resolve_snapshot_publish_timeout_passes_through_positive():
    assert resolve_snapshot_publish_timeout(30.0) == 30.0
    assert resolve_snapshot_publish_timeout(5) == 5.0


def test_resolve_snapshot_publish_timeout_rejects_zero_and_negative():
    """A misconfigured 0/negative must not make every heartbeat time out instantly.

    asyncio.wait_for(timeout<=0) aborts the publish on the first pass, so a bad
    value would silently stop heartbeats and flip the UI to worker_down. Fall
    back to the safe default instead.
    """
    assert resolve_snapshot_publish_timeout(0) == DEFAULT_SNAPSHOT_PUBLISH_TIMEOUT_SEC
    assert resolve_snapshot_publish_timeout(-1) == DEFAULT_SNAPSHOT_PUBLISH_TIMEOUT_SEC


def test_resolve_snapshot_publish_timeout_handles_none_and_garbage():
    assert resolve_snapshot_publish_timeout(None) == DEFAULT_SNAPSHOT_PUBLISH_TIMEOUT_SEC
    assert resolve_snapshot_publish_timeout("nonsense") == DEFAULT_SNAPSHOT_PUBLISH_TIMEOUT_SEC


def _snapshot(*, updated_at, payload=None):
    return SimpleNamespace(updated_at=updated_at, payload=payload or {"status": "alive"})


def test_missing_snapshot_is_not_alive():
    health = evaluate_worker_heartbeat(None)
    assert health.alive is False
    assert health.status == "missing"
    assert health.age_sec is None


def test_fresh_heartbeat_is_alive():
    now = datetime.now(timezone.utc)
    health = evaluate_worker_heartbeat(_snapshot(updated_at=now - timedelta(seconds=5)), now=now)
    assert health.alive is True
    assert health.stale is False
    assert 0 <= health.age_sec < WORKER_HEARTBEAT_STALE_AFTER_SEC


def test_stale_heartbeat_not_alive_with_age():
    now = datetime.now(timezone.utc)
    old = now - timedelta(seconds=WORKER_HEARTBEAT_STALE_AFTER_SEC + 30)
    health = evaluate_worker_heartbeat(_snapshot(updated_at=old), now=now)
    assert health.alive is False
    assert health.stale is True
    assert health.reason == ""  # stale carries its signal via .stale, not .reason
    assert health.age_sec is not None


def test_explicit_failure_status_carries_reason():
    now = datetime.now(timezone.utc)
    snap = _snapshot(
        updated_at=now,
        payload={"status": "degraded", "detail": "session decrypt failed"},
    )
    health = evaluate_worker_heartbeat(snap, now=now)
    assert health.alive is False
    assert health.status == "degraded"
    assert health.reason == "session decrypt failed"


def test_naive_updated_at_treated_as_utc():
    now = datetime.now(timezone.utc)
    naive = (now - timedelta(seconds=5)).replace(tzinfo=None)
    health = evaluate_worker_heartbeat(_snapshot(updated_at=naive), now=now)
    assert health.alive is True
