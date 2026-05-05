from datetime import datetime, timedelta, timezone

from src.utils.datetime import (
    normalize_utc,
    parse_datetime,
    parse_required_schedule_datetime,
    parse_utc_datetime,
    try_parse_datetime,
    try_parse_utc_datetime,
    utc_isoformat,
)


def test_parse_datetime_accepts_empty_values():
    assert parse_datetime(None) is None
    assert parse_datetime("") is None


def test_parse_datetime_accepts_z_suffix():
    result = parse_datetime("2026-05-05T12:00:00Z")
    assert result == datetime(2026, 5, 5, 12, 0, tzinfo=timezone.utc)


def test_parse_utc_datetime_treats_naive_as_utc():
    result = parse_utc_datetime("2026-05-05T12:00:00")
    assert result == datetime(2026, 5, 5, 12, 0, tzinfo=timezone.utc)


def test_parse_required_schedule_datetime_returns_utc():
    result = parse_required_schedule_datetime("2026-05-05T19:00:00+07:00")
    assert result == datetime(2026, 5, 5, 12, 0, tzinfo=timezone.utc)


def test_parse_required_schedule_datetime_treats_naive_as_local():
    source = datetime(2026, 5, 5, 12, 0)
    assert parse_required_schedule_datetime(source) == source.astimezone(timezone.utc)


def test_try_parse_datetime_returns_none_for_invalid_values():
    assert try_parse_datetime("not-a-date") is None
    assert try_parse_utc_datetime("not-a-date") is None


def test_normalize_utc_converts_aware_datetime():
    source = datetime(2026, 5, 5, 19, 0, tzinfo=timezone(timedelta(hours=7)))
    assert normalize_utc(source) == datetime(2026, 5, 5, 12, 0, tzinfo=timezone.utc)


def test_utc_isoformat_returns_none_or_utc_string():
    assert utc_isoformat(None) is None
    assert utc_isoformat(datetime(2026, 5, 5, 12, 0)) == "2026-05-05T12:00:00+00:00"
