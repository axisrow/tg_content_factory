"""JSON utilities with safe serialization for external data."""

import json
from datetime import date, datetime
from typing import Any


def safe_json_dumps(obj, **kwargs) -> str:
    """
    JSON dumps with fallback for non-serializable types.

    Handles:
    - datetime, date → .isoformat()
    - bytes → .hex()
    - Pydantic v2 models → .model_dump()
    - Unknown → raises TypeError (fail-fast)

    Use for serializing external/untyped data (Telegram objects, DB payloads, etc.).
    """
    def _default(o):
        if isinstance(o, (datetime, date)):
            return o.isoformat()
        if isinstance(o, bytes):
            return o.hex()
        # Pydantic v2
        if hasattr(o, "model_dump"):
            return o.model_dump()
        raise TypeError(f"Object of type {type(o).__name__} is not JSON serializable")

    return json.dumps(obj, default=_default, **kwargs)


def safe_json_loads(raw: str | bytes | bytearray | None, default: Any = None) -> Any:
    if not raw:
        return default
    try:
        return json.loads(raw)
    except (TypeError, json.JSONDecodeError):
        return default


def safe_json_loads_dict(raw: str | bytes | bytearray | None) -> dict | None:
    parsed = safe_json_loads(raw)
    return parsed if isinstance(parsed, dict) else None


def safe_json_loads_list(raw: str | bytes | bytearray | None) -> list:
    parsed = safe_json_loads(raw)
    return parsed if isinstance(parsed, list) else []
