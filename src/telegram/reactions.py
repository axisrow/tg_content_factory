from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from typing import Any

ReactionItem = dict[str, str | int]


def _coerce_count(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def _reaction_label(reaction: Any) -> str | None:
    if reaction is None:
        return None

    emoticon = getattr(reaction, "emoticon", None)
    if emoticon:
        return str(emoticon)

    document_id = getattr(reaction, "document_id", None)
    if document_id is not None:
        return f"custom:{document_id}"

    if reaction.__class__.__name__ == "ReactionPaid":
        return "paid"

    return None


def extract_message_reactions(message: Any) -> list[ReactionItem]:
    reactions = getattr(message, "reactions", None)
    results = getattr(reactions, "results", None)
    if not results:
        return []

    items: list[ReactionItem] = []
    for result in results:
        label = _reaction_label(getattr(result, "reaction", None))
        if label is None:
            continue
        items.append({"emoji": label, "count": _coerce_count(getattr(result, "count", 0))})
    return items


def extract_message_reactions_json(message: Any) -> str | None:
    items = extract_message_reactions(message)
    return json.dumps(items, ensure_ascii=False) if items else None


def parse_reactions_json(reactions_json: str | None) -> list[ReactionItem]:
    try:
        raw_items = json.loads(reactions_json or "")
    except (json.JSONDecodeError, TypeError):
        return []
    if not isinstance(raw_items, list):
        return []

    items: list[ReactionItem] = []
    for item in raw_items:
        if not isinstance(item, Mapping):
            continue
        emoji = item.get("emoji")
        if not emoji:
            continue
        items.append({"emoji": str(emoji), "count": _coerce_count(item.get("count", 0))})
    return items


def format_reaction_counts(items: Iterable[Mapping[str, Any]]) -> str:
    parts: list[str] = []
    for item in items:
        emoji = item.get("emoji")
        if not emoji:
            continue
        parts.append(f"{emoji} {_coerce_count(item.get('count', 0))}")
    return " ".join(parts)


def format_message_reactions(message: Any) -> str:
    return format_reaction_counts(extract_message_reactions(message))


def format_reactions_json(reactions_json: str | None) -> str:
    return format_reaction_counts(parse_reactions_json(reactions_json))
