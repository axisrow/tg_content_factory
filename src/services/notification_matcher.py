from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from typing import Protocol

from src.models import Channel, Message, SearchQuery
from src.parsers import bare_channel_id
from src.telegram.notifier import Notifier
from src.utils.search_query_chat_filter import chat_filter_matches_message

logger = logging.getLogger(__name__)


class NotifiedStore(Protocol):
    """Persistence port for notification dedup (decouples delivery from the
    collection cursor — audit #838/1)."""

    async def filter_unnotified(
        self, query_id: int, channel_id: int, message_ids: list[int]
    ) -> set[int]: ...

    async def record(self, query_id: int, channel_id: int, message_ids: list[int]) -> None: ...


def message_matches_query(sq: SearchQuery, msg: Message, channels: list[Channel] | None = None) -> bool:
    """True if *msg* matches notification query *sq*.

    Shared by the live matcher and the dry-run preview so both use the exact same
    semantics (regex via re.search, plain via substring) instead of diverging
    (audit #838/3 keys off this predicate).
    """
    if not msg.text:
        return False
    if not chat_filter_matches_message(sq.chat_filter, msg, channels=channels or []):
        return False
    if sq.max_length is not None and len(msg.text) >= sq.max_length:
        return False
    if any(p.lower() in msg.text.lower() for p in sq.exclude_patterns_list):
        return False
    if sq.is_regex:
        try:
            return bool(re.search(sq.query, msg.text, re.IGNORECASE))
        except re.error:
            return False
    if sq.is_fts:
        return _fts_query_matches(sq.query, msg.text)
    return sq.query.lower() in msg.text.lower()


def dry_run_matches(
    messages: list[Message], sq: SearchQuery, channels: list[Channel] | None = None
) -> tuple[list[Message], int]:
    """Preview matches for *sq* using the production predicate (not FTS).

    Returns (matched_messages, count) so the dry-run preview agrees with what the
    live NotificationMatcher would actually fire on (audit #838/3).
    """
    matched = [m for m in messages if message_matches_query(sq, m, channels)]
    return matched, len(matched)


@dataclass
class _QueryMatch:
    name: str
    messages: list[Message] = field(default_factory=list)


class NotificationMatcher:
    """Match messages against notification queries and send batched notifications."""

    def __init__(
        self,
        notifier: Notifier,
        *,
        channels: list[Channel] | None = None,
        notified_store: NotifiedStore | None = None,
    ):
        self._notifier = notifier
        self._channels = channels or []
        self._notified_store = notified_store

    async def match_and_notify(
        self,
        messages: list[Message],
        queries: list[SearchQuery],
    ) -> dict[int, int]:
        """Match messages against queries and notify. Returns {sq_id: notified_count}.

        Only matches not already recorded in the dedup store fire, and a query is
        recorded as notified solely after its notification is sent successfully —
        so a transient send failure leaves the matches eligible for retry on the
        next pass (the collector re-presents recent messages via a backlog scan).
        """
        if not messages or not queries:
            return {}

        matched: dict[int, _QueryMatch] = {}
        for msg in messages:
            if not msg.text:
                continue
            for sq in queries:
                if sq.id is None:
                    continue
                if not message_matches_query(sq, msg, self._channels):
                    continue
                qm = matched.get(sq.id)
                if qm is None:
                    matched[sq.id] = _QueryMatch(name=sq.name or sq.query, messages=[msg])
                else:
                    qm.messages.append(msg)

        result: dict[int, int] = {}
        for sq_id, qm in matched.items():
            fresh = await self._filter_fresh(sq_id, qm.messages)
            if not fresh:
                continue
            count = len(fresh)
            first = fresh[0]
            preview = (first.text or "")[:200]
            link = _make_message_link(first)
            if count == 1:
                text = f"Query '{qm.name}' matched in channel:\n{preview}\n{link}"
            else:
                text = f"Query '{qm.name}' matched {count} times. First:\n{preview}\n{link}"

            sent = await self._notifier.notify(text)
            if sent:
                result[sq_id] = count
                await self._record(sq_id, fresh)
            else:
                logger.warning(
                    "Notification send failed for query id=%s (%d matches); will retry next pass",
                    sq_id,
                    count,
                )

        return result

    async def _filter_fresh(self, sq_id: int, messages: list[Message]) -> list[Message]:
        if self._notified_store is None:
            return messages
        by_channel: dict[int, list[Message]] = {}
        for m in messages:
            by_channel.setdefault(m.channel_id, []).append(m)
        keep_keys: set[tuple[int, int]] = set()
        for channel_id, msgs in by_channel.items():
            new_ids = await self._notified_store.filter_unnotified(
                sq_id, channel_id, [m.message_id for m in msgs]
            )
            keep_keys.update((channel_id, mid) for mid in new_ids)
        return [m for m in messages if (m.channel_id, m.message_id) in keep_keys]

    async def _record(self, sq_id: int, messages: list[Message]) -> None:
        if self._notified_store is None:
            return
        by_channel: dict[int, list[int]] = {}
        for m in messages:
            by_channel.setdefault(m.channel_id, []).append(m.message_id)
        for channel_id, ids in by_channel.items():
            await self._notified_store.record(sq_id, channel_id, ids)


def _make_message_link(msg: Message) -> str:
    """Build a t.me link for the message."""
    if msg.channel_username:
        return f"https://t.me/{msg.channel_username}/{msg.message_id}"
    return f"https://t.me/c/{bare_channel_id(msg.channel_id)}/{msg.message_id}"


def _fts_query_matches(fts_query: str, text: str) -> bool:
    """Approximate FTS5 boolean query matching against plain text."""
    text_lower = text.lower()
    parts = re.split(r"\bAND\b", fts_query, flags=re.IGNORECASE)
    for part in parts:
        part = part.strip().strip("()")
        alternatives = re.split(r"\bOR\b", part, flags=re.IGNORECASE)
        terms = [alt.strip().strip('"').rstrip("*").lower() for alt in alternatives]
        if not any(t and t in text_lower for t in terms):
            return False
    return True
