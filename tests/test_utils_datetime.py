"""Unit tests for the UTC datetime helpers (src/utils/datetime.py).

Restores the coverage these helpers lost when the flood-wait tests moved to
the telethon-floodgate package: those tests were the ones exercising the
parsing edge cases via is_blocking_flood_wait_until → try_parse_utc_datetime.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from src.utils.datetime import (
    normalize_utc,
    parse_datetime,
    parse_required_datetime,
    parse_required_schedule_datetime,
    parse_required_utc_datetime,
    parse_utc_datetime,
    try_parse_datetime,
    try_parse_utc_datetime,
    utc_isoformat,
)

_NAIVE = "2026-01-02T03:04:05"


def _naive() -> datetime:
    # Naive via fromisoformat: constructing it with datetime(...) directly
    # trips the repo-wide DTZ001 lint rule.
    return datetime.fromisoformat(_NAIVE)


def test_normalize_utc_keeps_none_and_converts_naive_and_aware():
    assert normalize_utc(None) is None
    assert normalize_utc(_naive()).utcoffset() == timedelta(0)
    tokyo = try_parse_datetime("2026-01-02T03:04:05+09:00")
    assert tokyo is not None
    shifted = normalize_utc(tokyo)
    assert shifted is not None
    assert shifted.utcoffset() == timedelta(0)
    assert shifted == tokyo.astimezone(timezone.utc)


def test_parse_datetime_passthrough_and_none_and_z_suffix():
    assert parse_datetime(None) is None
    assert parse_datetime("") is None
    value = try_parse_datetime(_NAIVE)
    assert value is not None
    assert parse_datetime(value) is value
    parsed = parse_datetime("2026-01-02T03:04:05Z")
    assert parsed is not None
    assert parsed.utcoffset() == timedelta(0)
    assert parsed.replace(tzinfo=None).isoformat() == _NAIVE


def test_try_parse_datetime_returns_none_on_garbage():
    assert try_parse_datetime("not-a-date") is None
    parsed = try_parse_datetime(_NAIVE)
    assert parsed is not None
    assert parsed.utcoffset() is None  # naive stays naive in try_parse_


def test_parse_required_variants():
    with pytest.raises(ValueError):
        parse_required_datetime(None)  # type: ignore[arg-type]
    assert parse_required_datetime("2026-01-02") == try_parse_datetime("2026-01-02")
    with pytest.raises(ValueError):
        parse_required_utc_datetime(None)  # type: ignore[arg-type]


def test_utc_variants_normalize_and_format():
    assert parse_utc_datetime(_naive()) == try_parse_datetime("2026-01-02T03:04:05+00:00")
    assert try_parse_utc_datetime("garbage") is None
    assert try_parse_utc_datetime("2026-01-02T03:04:05Z") == try_parse_datetime(
        "2026-01-02T03:04:05+00:00"
    )
    tokyo = try_parse_datetime("2026-01-02T03:04:00+09:00")
    assert tokyo is not None
    assert parse_required_schedule_datetime(tokyo) == tokyo.astimezone(timezone.utc)
    assert parse_required_schedule_datetime(_naive()) == try_parse_datetime(
        "2026-01-02T03:04:05+00:00"
    )
    assert utc_isoformat(None) is None
    assert utc_isoformat(_naive()) == "2026-01-02T03:04:05+00:00"
