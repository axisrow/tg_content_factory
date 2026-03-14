from __future__ import annotations

from datetime import datetime, timezone


def normalize_utc(value: datetime | None) -> datetime | None:
    """Ensure a datetime is UTC-aware; treat naive values as UTC."""
    if value is None:
        return None
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)
