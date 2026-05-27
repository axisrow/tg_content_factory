from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any

ReactionItem = dict[str, str | int]
ReactionUserItem = dict[str, str | int]
DEFAULT_REACTION_USERS_LIMIT = 20
MAX_REACTION_USERS_LIMIT = 100
SUPPORTED_REACTION_EMOJIS: tuple[str, ...] = (
    "👍",
    "👎",
    "❤",
    "🔥",
    "🥰",
    "👏",
    "😁",
    "🤔",
    "🤯",
    "😱",
    "🤬",
    "😢",
    "🎉",
    "🤩",
    "🤮",
    "💩",
    "🙏",
    "👌",
    "🕊",
    "🤡",
    "🥱",
    "🥴",
    "😍",
    "🐳",
    "❤‍🔥",
    "🌚",
    "🌭",
    "💯",
    "🤣",
    "⚡",
    "🍌",
    "🏆",
    "💔",
    "🤨",
    "😐",
    "🍓",
    "🍾",
    "💋",
    "🖕",
    "😈",
    "😴",
    "😭",
    "🤓",
    "👻",
    "👨‍💻",
    "👀",
    "🎃",
    "🙈",
    "😇",
    "😨",
    "🤝",
    "✍",
    "🤗",
    "🫡",
    "🎅",
    "🎄",
    "☃",
    "💅",
    "🤪",
    "🗿",
    "🆒",
    "💘",
    "🙉",
    "🦄",
    "😘",
    "💊",
    "🙊",
    "😎",
    "👾",
    "🤷‍♂",
    "🤷",
    "🤷‍♀",
    "😡",
)
SUPPORTED_REACTION_EMOJIS_SET = frozenset(SUPPORTED_REACTION_EMOJIS)
SUPPORTED_REACTION_EMOJIS_DISPLAY = " ".join(SUPPORTED_REACTION_EMOJIS)
_VARIATION_SELECTOR_16 = "\ufe0f"


@dataclass(frozen=True)
class ReactionUsersResult:
    items: list[ReactionUserItem]
    unavailable: str | None = None
    limited: bool = False


class TelegramReactionInvalidError(ValueError):
    """Raised when an outgoing reaction is not supported by Telegram."""


def normalize_outgoing_reaction_emoji(emoji: str | None, *, allow_clear: bool = False) -> str | None:
    """Normalize and validate a Telegram built-in reaction emoji before sending."""
    if emoji is None:
        if allow_clear:
            return None
        raise TelegramReactionInvalidError(_invalid_reaction_message(emoji))

    normalized = str(emoji).strip().replace(_VARIATION_SELECTOR_16, "")
    if not normalized or normalized not in SUPPORTED_REACTION_EMOJIS_SET:
        raise TelegramReactionInvalidError(_invalid_reaction_message(emoji))
    return normalized


def _invalid_reaction_message(emoji: object) -> str:
    return (
        f"Unsupported Telegram reaction emoji {emoji!r}. "
        f"Supported reactions: {SUPPORTED_REACTION_EMOJIS_DISPLAY}"
    )


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


def _coerce_int(value: Any) -> int | None:
    if value is None or isinstance(value, bool):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _peer_key(peer_id: Any) -> tuple[str | None, int | None]:
    for kind, attr in (("user", "user_id"), ("channel", "channel_id"), ("chat", "chat_id"), (None, "id")):
        value = _coerce_int(getattr(peer_id, attr, None))
        if value is not None:
            return kind, value
    return None, None


def _index_by_id(items: Iterable[Any]) -> dict[int, Any]:
    indexed: dict[int, Any] = {}
    for item in items:
        item_id = _coerce_int(getattr(item, "id", None))
        if item_id is not None:
            indexed[item_id] = item
    return indexed


def _display_peer(entity: Any, peer_id: int | None) -> str:
    if entity is not None:
        username = getattr(entity, "username", None)
        if username:
            return f"@{str(username).strip().lstrip('@')}"

        first_name = str(getattr(entity, "first_name", "") or "").strip()
        last_name = str(getattr(entity, "last_name", "") or "").strip()
        full_name = " ".join(part for part in (first_name, last_name) if part)
        if full_name:
            return full_name

        title = str(getattr(entity, "title", "") or "").strip()
        if title:
            return title

    if peer_id is not None:
        return f"id={peer_id}"
    return "unknown"


def normalize_reaction_users_limit(value: Any, *, default: int = DEFAULT_REACTION_USERS_LIMIT) -> int:
    limit = _coerce_int(value)
    if limit is None:
        limit = default
    return max(1, min(limit, MAX_REACTION_USERS_LIMIT))


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


def extract_reaction_users(result: Any) -> list[ReactionUserItem]:
    users_by_id = _index_by_id(getattr(result, "users", None) or [])
    chats_by_id = _index_by_id(getattr(result, "chats", None) or [])

    items: list[ReactionUserItem] = []
    for peer_reaction in getattr(result, "reactions", None) or []:
        label = _reaction_label(getattr(peer_reaction, "reaction", None))
        if label is None:
            continue

        kind, peer_id = _peer_key(getattr(peer_reaction, "peer_id", None))
        if kind == "user":
            entity = users_by_id.get(peer_id) if peer_id is not None else None
        elif kind in {"chat", "channel"}:
            entity = chats_by_id.get(peer_id) if peer_id is not None else None
        else:
            entity = (users_by_id.get(peer_id) or chats_by_id.get(peer_id)) if peer_id is not None else None

        item: ReactionUserItem = {
            "emoji": label,
            "display": _display_peer(entity, peer_id),
        }
        if peer_id is not None:
            item["peer_id"] = peer_id
        items.append(item)
    return items


async def fetch_message_reaction_users(
    client: Any,
    peer: Any,
    message_id: int,
    *,
    limit: Any = DEFAULT_REACTION_USERS_LIMIT,
) -> ReactionUsersResult:
    safe_limit = normalize_reaction_users_limit(limit)
    try:
        from src.telegram.backends import fetch_message_reaction_users_raw
    except ImportError as exc:
        return ReactionUsersResult([], unavailable=str(exc))

    try:
        result = await fetch_message_reaction_users_raw(client, peer, int(message_id), limit=safe_limit)
    except Exception as exc:
        if exc.__class__.__name__ == "FloodWaitError":
            raise
        return ReactionUsersResult([], unavailable=str(exc))

    items = extract_reaction_users(result)
    total = _coerce_count(getattr(result, "count", len(items)))
    limited = bool(getattr(result, "next_offset", None)) or total > len(items)
    return ReactionUsersResult(items, limited=limited)


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


def format_reaction_users(
    items: Iterable[Mapping[str, Any]],
    *,
    unavailable: bool = False,
    limited: bool = False,
) -> str:
    if unavailable:
        return "пользователи реакций недоступны"

    grouped: dict[str, list[str]] = {}
    for item in items:
        emoji = item.get("emoji")
        display = item.get("display")
        if not emoji or not display:
            continue
        grouped.setdefault(str(emoji), []).append(str(display))

    parts = [f"{emoji} {', '.join(users)}" for emoji, users in grouped.items()]
    text = "; ".join(parts)
    if text and limited:
        text += " ..."
    return text


def format_reaction_users_result(result: ReactionUsersResult) -> str:
    return format_reaction_users(
        result.items,
        unavailable=bool(result.unavailable),
        limited=result.limited,
    )
