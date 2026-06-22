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


async def dry_run_counts(db, queries: list[SearchQuery], since: str | None) -> dict[int | None, int]:
    """Exact dry-run match counts per query over the whole window collected since *since*.

    Single shared engine for the CLI, web and agent dry-run surfaces (CLI/Web/agent parity):
    pages through ALL messages in the window via iter_messages_collected_since and counts via
    the production predicate, so the preview is uncapped and uses the same regex/substring
    semantics the live NotificationMatcher would (#838/3 + its 5000-cap review). Returns
    {sq.id: total}; an empty/absent window yields 0 for every query.
    """
    totals: dict[int | None, int] = {sq.id: 0 for sq in queries}
    if not since or not queries:
        return totals
    channels = await db.get_channels()
    async for page in db.repos.messages.iter_messages_collected_since(since):
        for sq in queries:
            for m in page:
                if message_matches_query(sq, m, channels):
                    totals[sq.id] += 1
    return totals


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


# FTS5's default tokenizer splits text on non-alphanumeric runs; approximate it
# with a Unicode word-token regex so matching respects token boundaries instead
# of raw substrings (a bare `cat` must not match inside `concatenate`).
_FTS_TOKEN_RE = re.compile(r"\w+", re.UNICODE)


def _contains_consecutive(tokens: list[str], phrase: list[str]) -> bool:
    """True if ``phrase`` appears as a run of consecutive entries in ``tokens``."""
    n, m = len(tokens), len(phrase)
    if m == 0 or m > n:
        return False
    return any(tokens[i : i + m] == phrase for i in range(n - m + 1))


def _fts_alt_matches(alt: str, tokens: list[str], token_set: frozenset[str]) -> bool:
    """Match a single OR-alternative against the message's token list.

    Mirrors FTS5 token semantics rather than substring containment:
    - a quoted phrase ("apple banana") matches a run of consecutive tokens;
    - bare terms are implicit-AND — each must equal a whole token (any position);
    - a trailing ``*`` makes a term a token prefix (``app*`` matches ``apple``).
    """
    alt = alt.strip()
    if not alt:
        return False
    if alt.startswith('"') and alt.endswith('"'):
        phrase = _FTS_TOKEN_RE.findall(alt.strip('"').lower())
        return bool(phrase) and _contains_consecutive(tokens, phrase)
    saw_term = False
    for raw in alt.split():
        is_prefix = raw.endswith("*")
        words = _FTS_TOKEN_RE.findall(raw.lower())
        if not words:
            continue
        saw_term = True
        term = words[0]
        if is_prefix:
            if not any(t.startswith(term) for t in token_set):
                return False
        elif term not in token_set:
            return False
    return saw_term


def _fts_query_matches(fts_query: str, text: str) -> bool:
    """Approximate FTS5 boolean query matching against plain text."""
    tokens = _FTS_TOKEN_RE.findall(text.lower())
    token_set = frozenset(tokens)
    parts = re.split(r"\bAND\b", fts_query, flags=re.IGNORECASE)
    for part in parts:
        part = part.strip().strip("()")
        alternatives = re.split(r"\bOR\b", part, flags=re.IGNORECASE)
        if not any(_fts_alt_matches(alt, tokens, token_set) for alt in alternatives):
            return False
    return True
