from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Literal
from urllib.parse import parse_qs, urlsplit

from src.models import Channel, Message

ChatFilterTokenKind = Literal["numeric", "username", "invalid"]

_USERNAME_RE = re.compile(r"^[A-Za-z0-9_]+$")
_SPLIT_RE = re.compile(r"[\s,]+")


@dataclass(frozen=True)
class ChatFilterToken:
    raw: str
    kind: ChatFilterTokenKind
    value: int | str | None = None


@dataclass(frozen=True)
class ParsedChatFilter:
    entries: tuple[ChatFilterToken, ...]

    @property
    def has_filter(self) -> bool:
        return bool(self.entries)

    @property
    def numeric_values(self) -> tuple[int, ...]:
        return tuple(entry.value for entry in self.entries if entry.kind == "numeric" and isinstance(entry.value, int))

    @property
    def usernames(self) -> tuple[str, ...]:
        return tuple(entry.value for entry in self.entries if entry.kind == "username" and isinstance(entry.value, str))

    @property
    def invalid_tokens(self) -> tuple[str, ...]:
        return tuple(entry.raw for entry in self.entries if entry.kind == "invalid")

    @property
    def has_valid_tokens(self) -> bool:
        return bool(self.numeric_values or self.usernames)


@dataclass(frozen=True)
class ChatFilterValidation:
    invalid_tokens: tuple[str, ...] = ()
    unknown_tokens: tuple[str, ...] = ()
    matched_channel_ids: tuple[int, ...] = ()

    @property
    def has_warnings(self) -> bool:
        return bool(self.invalid_tokens or self.unknown_tokens)

    def warning_text(self) -> str:
        parts = []
        if self.invalid_tokens:
            parts.append("некорректные: " + ", ".join(self.invalid_tokens))
        if self.unknown_tokens:
            parts.append("не найдены: " + ", ".join(self.unknown_tokens))
        return "Чаты в фильтре сохранены, но есть предупреждения: " + "; ".join(parts) if parts else ""


def parse_chat_filter(raw_filter: str | None) -> ParsedChatFilter:
    entries: list[ChatFilterToken] = []
    seen: set[tuple[ChatFilterTokenKind, int | str | None]] = set()
    for raw in _SPLIT_RE.split((raw_filter or "").strip()):
        token = raw.strip().strip(",;")
        if not token:
            continue
        entry = _parse_token(token)
        key = (entry.kind, entry.value if entry.kind != "invalid" else entry.raw)
        if key in seen:
            continue
        seen.add(key)
        entries.append(entry)
    return ParsedChatFilter(tuple(entries))


def validate_chat_filter(raw_filter: str | None, channels: list[Channel]) -> ChatFilterValidation:
    parsed = parse_chat_filter(raw_filter)
    if not parsed.has_filter:
        return ChatFilterValidation()

    matched_ids: set[int] = set()
    unknown: list[str] = []
    invalid = list(parsed.invalid_tokens)

    channels_by_username = {
        (ch.username or "").lower(): ch
        for ch in channels
        if ch.username
    }
    for entry in parsed.entries:
        if entry.kind == "invalid":
            continue
        if entry.kind == "numeric" and isinstance(entry.value, int):
            matches = [
                ch
                for ch in channels
                if ch.channel_id == entry.value or ch.id == entry.value
            ]
        elif entry.kind == "username" and isinstance(entry.value, str):
            match = channels_by_username.get(entry.value)
            matches = [match] if match else []
        else:
            matches = []

        if matches:
            matched_ids.update(ch.channel_id for ch in matches)
        else:
            unknown.append(entry.raw)

    return ChatFilterValidation(
        invalid_tokens=tuple(invalid),
        unknown_tokens=tuple(unknown),
        matched_channel_ids=tuple(sorted(matched_ids)),
    )


def chat_filter_matches_message(
    raw_filter: str | None,
    msg: Message,
    *,
    channels: list[Channel] | None = None,
) -> bool:
    parsed = parse_chat_filter(raw_filter)
    if not parsed.has_filter:
        return True
    if not parsed.has_valid_tokens:
        return False

    numeric_values = set(parsed.numeric_values)
    usernames = set(parsed.usernames)
    if msg.channel_id in numeric_values:
        return True
    if msg.channel_username and msg.channel_username.lower() in usernames:
        return True

    for ch in channels or []:
        if ch.channel_id != msg.channel_id:
            continue
        if ch.id in numeric_values or ch.channel_id in numeric_values:
            return True
        if ch.username and ch.username.lower() in usernames:
            return True
    return False


def single_resolved_channel_id(raw_filter: str | None, channels: list[Channel]) -> int | None:
    validation = validate_chat_filter(raw_filter, channels)
    if validation.invalid_tokens or validation.unknown_tokens:
        return None
    if len(validation.matched_channel_ids) == 1:
        return validation.matched_channel_ids[0]
    return None


def _parse_token(token: str) -> ChatFilterToken:
    normalized = _normalize_token(token)
    if not normalized:
        return ChatFilterToken(raw=token, kind="invalid")
    try:
        return ChatFilterToken(raw=token, kind="numeric", value=int(normalized))
    except ValueError:
        pass
    if _USERNAME_RE.match(normalized):
        return ChatFilterToken(raw=token, kind="username", value=normalized.lower())
    return ChatFilterToken(raw=token, kind="invalid")


def _normalize_token(token: str) -> str | None:
    token = token.strip()
    if not token:
        return None
    if token.startswith("@"):
        return token[1:].strip()
    if token.startswith(("https://", "http://", "t.me/", "telegram.me/")):
        url = token if token.startswith(("https://", "http://")) else f"https://{token}"
        parsed = urlsplit(url)
        host = parsed.netloc.lower()
        if host not in {"t.me", "telegram.me", "www.t.me", "www.telegram.me"}:
            return None
        query_domain = parse_qs(parsed.query).get("domain")
        if query_domain:
            return query_domain[0].strip().lstrip("@")
        parts = [part for part in parsed.path.split("/") if part]
        if not parts:
            return None
        if parts[0] == "s":
            parts = parts[1:]
            if not parts:
                return None
        if parts[0] == "c" and len(parts) > 1 and parts[1].isdigit():
            return f"-100{parts[1]}"
        return parts[0].strip().lstrip("@")
    return token
