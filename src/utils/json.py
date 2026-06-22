"""JSON utilities with safe serialization for external data."""

from datetime import date, datetime
from typing import Any, Callable

import orjson


def _default(o):
    if isinstance(o, (datetime, date)):
        return o.isoformat()
    if isinstance(o, bytes):
        return o.hex()
    # Pydantic v2
    if hasattr(o, "model_dump"):
        return o.model_dump()
    raise TypeError(f"Object of type {type(o).__name__} is not JSON serializable")


def safe_json_dumps(
    obj,
    *,
    ensure_ascii: bool = False,
    indent: int | None = None,
    sort_keys: bool = False,
    default: Callable[[Any], Any] | None = None,
) -> str:
    """
    JSON dumps (orjson-backed) with fallback for non-serializable types.

    Handles:
    - datetime, date → .isoformat()
    - bytes → .hex()
    - Pydantic v2 models → .model_dump()
    - Unknown → raises TypeError (fail-fast)

    Use for serializing external/untyped data (Telegram objects, DB payloads, etc.).

    Notes on the orjson migration (#956):
    - Output is compact (no spaces after ``:``/``,``) and always UTF-8, so
      ``ensure_ascii`` is accepted for call-site compatibility but has no effect
      (orjson never escapes non-ASCII).
    - Any truthy ``indent`` pretty-prints with 2 spaces (orjson's sole indent mode).
    - Returns ``str`` (orjson emits ``bytes``; decoded here so callers are unchanged).
    """
    option = 0
    if default is None:
        # The built-in _default renders datetime/date via .isoformat(); route them
        # to it for exact parity. With a *custom* default we must NOT force
        # passthrough — the caller's default may not handle datetime, so let orjson
        # serialize it natively (RFC 3339) instead (review on #956).
        option |= orjson.OPT_PASSTHROUGH_DATETIME
    if sort_keys:
        option |= orjson.OPT_SORT_KEYS
    if indent is not None and indent > 0:  # orjson's only indent mode is 2-space
        option |= orjson.OPT_INDENT_2
    return orjson.dumps(obj, default=default or _default, option=option).decode()


def safe_json_loads(raw: str | bytes | bytearray | None, default: Any = None) -> Any:
    if not raw:
        return default
    try:
        return orjson.loads(raw)
    except (TypeError, ValueError):
        return default


def safe_json_loads_dict(raw: str | bytes | bytearray | None) -> dict | None:
    parsed = safe_json_loads(raw)
    return parsed if isinstance(parsed, dict) else None


def safe_json_loads_list(raw: str | bytes | bytearray | None) -> list:
    parsed = safe_json_loads(raw)
    return parsed if isinstance(parsed, list) else []
