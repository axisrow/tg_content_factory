from __future__ import annotations

import re
from collections.abc import Iterable
from typing import NamedTuple


def _empty_message_filter() -> dict:
    return {
        "type": "message_filter",
        "message_kinds": [],
        "service_actions": [],
        "media_types": [],
        "sender_kinds": [],
        "forwarded": None,
        "has_text": None,
        "keywords": [],
        "regex": "",
        "match_links": False,
    }


def normalize_filter_config(raw_config: dict | None) -> dict:
    config = dict(raw_config or {})
    filter_type = config.get("type", "keywords")

    if filter_type == "message_filter":
        normalized = _empty_message_filter()
        normalized.update(
            {
                "message_kinds": list(config.get("message_kinds", []) or []),
                "service_actions": list(config.get("service_actions", []) or []),
                "media_types": list(config.get("media_types", []) or []),
                "sender_kinds": list(config.get("sender_kinds", []) or []),
                "forwarded": config.get("forwarded"),
                "has_text": config.get("has_text"),
                "keywords": list(config.get("keywords", []) or []),
                "regex": config.get("regex", "") or "",
                "match_links": bool(config.get("match_links", False)),
            }
        )
        return normalized

    if filter_type == "keywords":
        normalized = _empty_message_filter()
        normalized["keywords"] = list(config.get("keywords", []) or [])
        normalized["match_links"] = bool(config.get("match_links", False))
        normalized["_filter_type"] = "keywords"
        return normalized

    if filter_type == "regex":
        normalized = _empty_message_filter()
        normalized["regex"] = config.get("pattern", "") or ""
        normalized["_filter_type"] = "regex"
        return normalized

    if filter_type == "anonymous_sender":
        normalized = _empty_message_filter()
        normalized["sender_kinds"] = ["anonymous_admin"]
        return normalized

    if filter_type == "service_message":
        mapping = {
            "user_joined": "join",
            "user_left": "leave",
            "joined": "join",
            "left": "leave",
            "pinned": "pin",
            "pin": "pin",
            "title_changed": "title_changed",
            "photo_changed": "photo_changed",
            "migrate": "migrate",
            "created": "created",
        }
        normalized = _empty_message_filter()
        normalized["message_kinds"] = ["service"]
        normalized["service_actions"] = [
            mapping.get(item, item) for item in list(config.get("service_types", []) or [])
        ]
        return normalized

    return _empty_message_filter()


def filter_messages(messages: Iterable, raw_config: dict | None) -> list:
    if not raw_config:
        return list(messages)
    return [message for message in messages if match_message_filter(message, raw_config)]


class _MessageAttrs(NamedTuple):
    """Pre-extracted message fields shared across the per-criterion checks."""

    text: str
    text_lower: str
    message_kind: str | int | bool | None
    service_action: str | int | bool | None
    sender_kind: str | int | bool | None
    sender_id: str | int | bool | None
    sender_name: str | int | bool | None
    media_type: str | int | bool | None
    forward_from_channel_id: str | int | bool | None


def _extract_message_attributes(message) -> _MessageAttrs:
    text = getattr(message, "text", None) or ""
    return _MessageAttrs(
        text=text,
        text_lower=text.lower(),
        message_kind=_safe_scalar(getattr(message, "message_kind", None)),
        service_action=_safe_scalar(getattr(message, "service_action_semantic", None)),
        sender_kind=_safe_scalar(getattr(message, "sender_kind", None)),
        sender_id=_safe_scalar(getattr(message, "sender_id", None)),
        sender_name=_safe_scalar(getattr(message, "sender_name", None)),
        media_type=_safe_scalar(getattr(message, "media_type", None)),
        forward_from_channel_id=_safe_scalar(getattr(message, "forward_from_channel_id", None)),
    )


def _check_message_kinds(attrs: _MessageAttrs, config: dict) -> bool:
    if config["message_kinds"] and attrs.message_kind not in config["message_kinds"]:
        if not (
            config["message_kinds"] == ["service"]
            and attrs.service_action is None
            and any(
                item in attrs.text_lower
                for item in ("join", "left", "pinned", "title", "photo", "migrat", "create")
            )
        ):
            return False
    return True


def _check_service_actions(attrs: _MessageAttrs, config: dict) -> bool:
    if config["service_actions"] and attrs.service_action not in config["service_actions"]:
        legacy_aliases = {
            "join": ("joined", "join"),
            "leave": ("left", "leave"),
            "pin": ("pinned", "pin"),
            "title_changed": ("title",),
            "photo_changed": ("photo",),
            "migrate": ("migrat",),
            "created": ("create", "created"),
        }
        if attrs.service_action is None:
            matched = any(
                any(alias in attrs.text_lower for alias in legacy_aliases.get(expected, (expected,)))
                for expected in config["service_actions"]
            )
            if not matched:
                return False
        else:
            return False
    return True


def _check_media_types(attrs: _MessageAttrs, config: dict) -> bool:
    if config["media_types"] and attrs.media_type not in config["media_types"]:
        return False
    return True


def _check_sender_kinds(attrs: _MessageAttrs, config: dict) -> bool:
    if config["sender_kinds"] and attrs.sender_kind not in config["sender_kinds"]:
        if not (
            attrs.sender_kind is None
            and "anonymous_admin" in config["sender_kinds"]
            and attrs.sender_id is None
            and attrs.sender_name is None
        ):
            return False
    return True


def _check_forwarded(attrs: _MessageAttrs, config: dict) -> bool:
    expected_forwarded = config.get("forwarded")
    if expected_forwarded is not None:
        is_forwarded = attrs.forward_from_channel_id is not None
        if is_forwarded is not bool(expected_forwarded):
            return False
    return True


def _check_has_text(attrs: _MessageAttrs, config: dict) -> bool:
    expected_has_text = config.get("has_text")
    if expected_has_text is not None:
        has_text = bool(attrs.text.strip())
        if has_text is not bool(expected_has_text):
            return False
    return True


def _check_keywords(attrs: _MessageAttrs, config: dict) -> bool:
    keywords = [item.lower() for item in config["keywords"] if item]
    has_keyword_filter = bool(keywords)
    has_link_filter = config["match_links"]
    if not has_keyword_filter and not has_link_filter and config.get("_filter_type") == "keywords":
        return False
    if keywords and not any(item in attrs.text_lower for item in keywords):
        return False
    return True


def _check_match_links(attrs: _MessageAttrs, config: dict) -> bool:
    if config["match_links"] and not re.search(r"https?://\S+|t\.me/\S+", attrs.text):
        return False
    return True


def _check_regex(attrs: _MessageAttrs, config: dict) -> bool:
    pattern = config["regex"]
    if config.get("_filter_type") == "regex" and not pattern:
        return False
    if pattern:
        try:
            if not re.search(pattern, attrs.text, re.IGNORECASE):
                return False
        except re.error:
            return False
    return True


# Evaluated in order with short-circuit AND semantics — the same precedence as
# the original linear if-chain. Each predicate returns False to reject.
_MESSAGE_FILTER_CHECKS = (
    _check_message_kinds,
    _check_service_actions,
    _check_media_types,
    _check_sender_kinds,
    _check_forwarded,
    _check_has_text,
    _check_keywords,
    _check_match_links,
    _check_regex,
)


def match_message_filter(message, raw_config: dict | None) -> bool:
    config = normalize_filter_config(raw_config)
    if config.get("type") != "message_filter":
        return True
    attrs = _extract_message_attributes(message)
    return all(check(attrs, config) for check in _MESSAGE_FILTER_CHECKS)


def _safe_scalar(value):
    return value if isinstance(value, (str, int, bool)) or value is None else None
