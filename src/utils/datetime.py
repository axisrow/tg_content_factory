from __future__ import annotations

from datetime import datetime, timezone


def normalize_utc(value: datetime | None) -> datetime | None:
    """Ensure a datetime is UTC-aware; treat naive values as UTC."""
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def parse_datetime(value: str | datetime | None) -> datetime | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def try_parse_datetime(value: str | datetime | None) -> datetime | None:
    try:
        return parse_datetime(value)
    except ValueError:
        return None


def parse_required_datetime(value: str | datetime) -> datetime:
    parsed = parse_datetime(value)
    if parsed is None:
        raise ValueError("datetime value is required")
    return parsed


def parse_utc_datetime(value: str | datetime | None) -> datetime | None:
    return normalize_utc(parse_datetime(value))


def try_parse_utc_datetime(value: str | datetime | None) -> datetime | None:
    return normalize_utc(try_parse_datetime(value))


def parse_required_utc_datetime(value: str | datetime) -> datetime:
    return normalize_utc(parse_required_datetime(value))


def parse_required_schedule_datetime(value: str | datetime) -> datetime:
    parsed = parse_required_datetime(value)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def utc_isoformat(value: datetime | None) -> str | None:
    dt = normalize_utc(value)
    return dt.isoformat() if dt else None
