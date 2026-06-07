from __future__ import annotations

import hashlib


def mask_phone(phone: str | None) -> str:
    if not phone:
        return ""
    value = str(phone)
    if len(value) <= 7:
        return f"{value[:2]}..."
    return f"{value[:3]}...{value[-4:]}"


def text_hash(value: str) -> str:
    return hashlib.sha256(str(value).encode("utf-8")).hexdigest()[:12]


def query_log_fields(query: str) -> dict[str, object]:
    return {
        "query_hash": text_hash(query),
        "query_len": len(query),
    }
