"""Tests for scheduler route helper functions."""
from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace

from src.web.routes.scheduler import (
    _collector_health_border_severity,
    _collector_health_recommendations,
    _compute_load_level,
    _dedupe_recent_unavailability_events,
    _format_retry_hint,
    _job_label,
)

# --- _job_label ---


def test_job_label_known():
    assert _job_label("collect_all") == "Сбор всех каналов"


def test_job_label_search_query():
    assert _job_label("sq_42") == "Стат. запроса #42"


def test_job_label_pipeline_run():
    assert _job_label("pipeline_run_7") == "Пайплайн #7"


def test_job_label_content_generate():
    assert _job_label("content_generate_3") == "Генерация #3"


def test_job_label_unknown():
    assert _job_label("custom_job") == "custom_job"


def test_job_label_photo():
    assert _job_label("photo_due") == "Фото по расписанию"


# --- _format_retry_hint ---


def test_format_retry_hint_none():
    assert _format_retry_hint(None) == ""


def test_format_retry_hint_datetime():
    dt = datetime(2026, 6, 15, 12, 30, 0, tzinfo=timezone.utc)
    result = _format_retry_hint(dt)
    assert "2026-06-15" in result
    assert "UTC" in result


def test_dedupe_recent_unavailability_events_handles_mixed_timezone_datetimes():
    events = _dedupe_recent_unavailability_events(
        [
            SimpleNamespace(
                note="Отложено: нет подключённых активных аккаунтов для сбора.",
                error=None,
                completed_at=None,
                started_at=None,
                created_at=datetime(2026, 5, 2, 17, 10, 24),
            ),
            SimpleNamespace(
                note="Отложено: все аккаунты во Flood Wait до 2026-05-02T17:12:00+00:00",
                error=None,
                completed_at=datetime(2026, 5, 2, 17, 10, 25, tzinfo=timezone.utc),
                started_at=None,
                created_at=datetime(2026, 5, 2, 17, 10, 24),
            ),
        ]
    )

    assert len(events) == 2
    assert events[0]["latest_at"].tzinfo is not None


# --- _compute_load_level ---


def test_compute_load_overload_flooded():
    assert _compute_load_level(
        interval_minutes=30, active_unfiltered_channels=100,
        available_accounts_now=2, state="all_flooded",
    ) == "overload"


def test_compute_load_overload_no_clients():
    assert _compute_load_level(
        interval_minutes=60, active_unfiltered_channels=10,
        available_accounts_now=0, state="no_clients",
    ) == "overload"


def test_compute_load_overload_high_pressure_short_interval():
    assert _compute_load_level(
        interval_minutes=15, active_unfiltered_channels=120,
        available_accounts_now=2, state="healthy",
    ) == "overload"


def test_compute_load_high_medium_interval():
    assert _compute_load_level(
        interval_minutes=30, active_unfiltered_channels=80,
        available_accounts_now=2, state="healthy",
    ) == "high"


def test_compute_load_high_extreme_pressure():
    assert _compute_load_level(
        interval_minutes=60, active_unfiltered_channels=150,
        available_accounts_now=2, state="healthy",
    ) == "high"


def test_compute_load_ok():
    assert _compute_load_level(
        interval_minutes=60, active_unfiltered_channels=10,
        available_accounts_now=5, state="healthy",
    ) == "ok"


def test_collector_health_overload_while_healthy_is_warning():
    assert _compute_load_level(
        interval_minutes=15,
        active_unfiltered_channels=216,
        available_accounts_now=1,
        state="healthy",
    ) == "overload"
    assert _collector_health_border_severity(state="healthy", load_level="overload") == "warning"


def test_collector_health_all_flooded_is_danger():
    assert _collector_health_border_severity(state="all_flooded", load_level="overload") == "danger"


def test_collector_health_worker_down_is_danger():
    assert _collector_health_border_severity(state="worker_down", load_level="overload") == "danger"


# --- _collector_health_recommendations ---


def test_recommendations_flooded():
    recs = _collector_health_recommendations(
        state="all_flooded", load_level="ok",
        interval_minutes=30, active_unfiltered_channels=10,
        available_accounts_now=3,
    )
    assert any("Flood Wait" in r for r in recs)


def test_recommendations_no_clients():
    recs = _collector_health_recommendations(
        state="no_clients", load_level="ok",
        interval_minutes=30, active_unfiltered_channels=10,
        available_accounts_now=0,
    )
    assert any("аккаунт" in r.lower() for r in recs)


def test_recommendations_high_load():
    recs = _collector_health_recommendations(
        state="healthy", load_level="high",
        interval_minutes=15, active_unfiltered_channels=100,
        available_accounts_now=2,
    )
    assert len(recs) >= 2


def test_recommendations_single_account():
    recs = _collector_health_recommendations(
        state="healthy", load_level="ok",
        interval_minutes=60, active_unfiltered_channels=10,
        available_accounts_now=1,
    )
    assert any("аккаунт" in r.lower() for r in recs)


def test_recommendations_healthy():
    recs = _collector_health_recommendations(
        state="healthy", load_level="ok",
        interval_minutes=60, active_unfiltered_channels=10,
        available_accounts_now=5,
    )
    assert recs == []
