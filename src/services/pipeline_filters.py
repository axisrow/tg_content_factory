from __future__ import annotations

import re
from collections.abc import Iterable


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
    return [message for message in messages if match_message_filter(message, raw_config)]


def match_message_filter(message, raw_config: dict | None) -> bool:
    config = normalize_filter_config(raw_config)
    if config.get("type") != "message_filter":
        return True

    text = getattr(message, "text", None) or ""
    text_lower = text.lower()
    message_kind = _safe_scalar(getattr(message, "message_kind", None))
    service_action = _safe_scalar(getattr(message, "service_action_semantic", None))
    sender_kind = _safe_scalar(getattr(message, "sender_kind", None))
    sender_id = _safe_scalar(getattr(message, "sender_id", None))
    sender_name = _safe_scalar(getattr(message, "sender_name", None))

    if config["message_kinds"] and message_kind not in config["message_kinds"]:
        if not (
            config["message_kinds"] == ["service"]
            and service_action is None
            and any(item in text_lower for item in ("join", "left", "pinned", "title", "photo", "migrat", "create"))
        ):
            return False
    if config["service_actions"] and service_action not in config["service_actions"]:
        legacy_aliases = {
            "join": ("joined", "join"),
            "leave": ("left", "leave"),
            "pin": ("pinned", "pin"),
            "title_changed": ("title",),
            "photo_changed": ("photo",),
            "migrate": ("migrat",),
            "created": ("create", "created"),
        }
        if service_action is None:
            matched = any(
                any(alias in text_lower for alias in legacy_aliases.get(expected, (expected,)))
                for expected in config["service_actions"]
            )
            if not matched:
                return False
        else:
            return False
    if config["media_types"] and getattr(message, "media_type", None) not in config["media_types"]:
        return False
    if config["sender_kinds"] and sender_kind not in config["sender_kinds"]:
        if not (
            sender_kind is None
            and "anonymous_admin" in config["sender_kinds"]
            and sender_id is None
            and sender_name is None
        ):
            return False

    expected_forwarded = config.get("forwarded")
    if expected_forwarded is not None:
        is_forwarded = getattr(message, "forward_from_channel_id", None) is not None
        if is_forwarded is not bool(expected_forwarded):
            return False

    expected_has_text = config.get("has_text")
    if expected_has_text is not None:
        has_text = bool(text.strip())
        if has_text is not bool(expected_has_text):
            return False

    keywords = [item.lower() for item in config["keywords"] if item]
    has_keyword_filter = bool(keywords)
    has_link_filter = config["match_links"]
    if not has_keyword_filter and not has_link_filter and config.get("_filter_type") == "keywords":
        return False
    if keywords and not any(item in text_lower for item in keywords):
        return False

    if config["match_links"] and not re.search(r"https?://\S+|t\.me/\S+", text):
        return False

    pattern = config["regex"]
    if config.get("_filter_type") == "regex" and not pattern:
        return False
    if pattern:
        try:
            if not re.search(pattern, text, re.IGNORECASE):
                return False
        except re.error:
            return False

    return True


def _safe_scalar(value):
    return value if isinstance(value, (str, int, bool)) or value is None else None
