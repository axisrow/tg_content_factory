"""Unit tests for the shared worker-heartbeat health evaluator (#530)."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

from src.services.runtime_diagnostics import (
    WORKER_HEARTBEAT_STALE_AFTER_SEC,
    evaluate_worker_heartbeat,
)


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
