"""Request parsing/validation for the search web domain."""

from __future__ import annotations

import re

_LEN_RE = re.compile(r"\blen\s*(<|>)\s*(\d+)[,;]?")


def extract_length(q: str) -> tuple[str, int | None, int | None]:
    """Extract ``len<N`` / ``len>N`` tokens from *q*, return cleaned query."""
    min_length: int | None = None
    max_length: int | None = None
    for m in _LEN_RE.finditer(q):
        op, val = m.group(1), int(m.group(2))
        if op == "<":
            max_length = val
        else:
            min_length = val
    cleaned = re.sub(r"\s+", " ", _LEN_RE.sub("", q)).strip()
    return cleaned, min_length, max_length


def parse_channel_id(raw: str) -> tuple[int | None, str | None]:
    """Parse the ``channel_id`` query param: ``(value, error_message)``."""
    if not raw:
        return None, None
    try:
        return int(raw), None
    except ValueError:
        return None, f"Некорректный ID канала: {raw}"
