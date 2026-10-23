from __future__ import annotations

from datetime import date, datetime, timezone
from typing import overload


@overload
def normalize_utc(value: datetime) -> datetime: ...
@overload
def normalize_utc(value: None) -> None: ...
@overload
def normalize_utc(value: datetime | None) -> datetime | None: ...


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


def parse_date_bound(value: str) -> date:
    """Parse a date-bound string: ``YYYY-MM-DD`` (relaxed ``YYYY-M-D`` accepted).

    Raises ValueError for anything else — callers must reject such input instead
    of letting a non-canonical string reach a lexicographic SQL comparison
    against ISO dates and silently mis-window the query (#1593).
    """
    parts = value.split("-")
    if len(parts) == 3:
        try:
            return date(int(parts[0]), int(parts[1]), int(parts[2]))
        except ValueError:
            pass
    raise ValueError(f"invalid date {value!r}: expected YYYY-MM-DD")


def parse_datetime_bound(value: str) -> datetime:
    """Parse a full ISO datetime bound (e.g. ``2025-03-06T12:00:00+00:00``).

    Separated from :func:`parse_date_bound` so callers can keep the exact
    precision datetime bounds carry while date-only bounds normalize to whole
    days (#1593). The result is normalized to UTC (naive input treated as UTC):
    bounds reach SQL as ISO TEXT against stored ``m.date`` values, so
    equivalent instants with different offsets (``10:00Z`` vs ``13:00+03:00``)
    must render on one timescale or the lexicographic comparison silently
    mis-windows the query (#1666). Raises ValueError on anything that is
    neither.
    """
    try:
        return normalize_utc(datetime.fromisoformat(value.replace("Z", "+00:00")))
    except ValueError:
        raise ValueError(
            f"invalid date {value!r}: expected YYYY-MM-DD or an ISO datetime"
        ) from None


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
