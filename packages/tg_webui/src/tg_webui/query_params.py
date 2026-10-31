"""Lenient parsing for optional query parameters.

HTML GET forms submit empty selects/inputs as empty strings (``?pipeline_id=``),
and Jinja renders ``None`` into handcrafted pagination links. FastAPI rejects
both with 422 when the parameter is annotated as ``int | None``, so routes that
back such forms accept ``str | None`` and parse with these helpers (#779).
"""

from __future__ import annotations


def parse_optional_int(value: str | None, default: int | None = None) -> int | None:
    """Return ``int(value)`` or *default* when the value is empty or not a number."""
    if value is None or value.strip() == "":
        return default
    try:
        return int(value)
    except ValueError:
        return default


def parse_clamped_int(value: str | None, default: int, minimum: int, maximum: int) -> int:
    """Parse like :func:`parse_optional_int` and clamp the result to [minimum, maximum]."""
    parsed = parse_optional_int(value, default)
    if parsed is None:
        parsed = default
    return max(minimum, min(parsed, maximum))
