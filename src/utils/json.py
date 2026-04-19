"""JSON utilities with safe serialization for external data."""

import json
from datetime import date, datetime


def safe_json_dumps(obj, **kwargs) -> str:
    """
    JSON dumps with fallback for non-serializable types.

    Handles:
    - datetime, date → .isoformat()
    - bytes → .hex()
    - Pydantic v2 models → .model_dump()
    - Unknown → repr()

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
        return repr(o)

    return json.dumps(obj, default=_default, **kwargs)
