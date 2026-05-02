from __future__ import annotations

from dataclasses import dataclass
from unittest.mock import Mock


@dataclass(frozen=True)
class SenderIdentity:
    sender_id: int | None = None
    sender_name: str | None = None
    sender_first_name: str | None = None
    sender_last_name: str | None = None
    sender_username: str | None = None


def normalize_username(username: object) -> str | None:
    if username is None or isinstance(username, Mock):
        return None
    cleaned = str(username).strip().lstrip("@")
    return cleaned or None


def _text_attr(obj: object, name: str) -> str | None:
    value = getattr(obj, name, None)
    if value is None or isinstance(value, Mock):
        return None
    if isinstance(value, str):
        cleaned = value.strip()
        return cleaned or None
    return None


def _int_value(value: object) -> int | None:
    if value is None or isinstance(value, Mock) or isinstance(value, bool):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def extract_sender_identity(sender: object | None, *, fallback_sender_id: object = None) -> SenderIdentity:
    sender_id = _int_value(getattr(sender, "id", None)) if sender is not None else None
    sender_id = sender_id if sender_id is not None else _int_value(fallback_sender_id)

    if sender is None:
        return SenderIdentity(sender_id=sender_id)

    first_name = _text_attr(sender, "first_name")
    last_name = _text_attr(sender, "last_name")
    username = normalize_username(getattr(sender, "username", None))
    title = _text_attr(sender, "title")
    full_name = " ".join(part for part in (first_name, last_name) if part) or None
    sender_name = full_name or title or username

    return SenderIdentity(
        sender_id=sender_id,
        sender_name=sender_name,
        sender_first_name=first_name,
        sender_last_name=last_name,
        sender_username=username,
    )


def extract_message_sender_identity(msg: object, *, sender: object | None = None) -> SenderIdentity:
    if sender is None:
        sender = getattr(msg, "sender", None)
    return extract_sender_identity(sender, fallback_sender_id=getattr(msg, "sender_id", None))
