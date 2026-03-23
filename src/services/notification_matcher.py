from __future__ import annotations

import logging
import re

from src.models import Message, SearchQuery
from src.telegram.notifier import Notifier

logger = logging.getLogger(__name__)


class NotificationMatcher:
    """Match messages against notification queries and send batched notifications."""

    def __init__(self, notifier: Notifier):
        self._notifier = notifier

    async def match_and_notify(
        self,
        messages: list[Message],
        queries: list[SearchQuery],
    ) -> dict[int, int]:
        """Match messages against queries and notify. Returns {sq_id: match_count}."""
        if not messages or not queries:
            return {}

        # Collect matches per query: {sq_id: (query_name, count, first_preview)}
        matches: dict[int, tuple[str, int, str]] = {}
        for msg in messages:
            if not msg.text:
                continue
            for sq in queries:
                if sq.max_length is not None and len(msg.text) >= sq.max_length:
                    continue
                if any(p.lower() in msg.text.lower() for p in sq.exclude_patterns_list):
                    continue

                matched = False
                if sq.is_regex:
                    try:
                        matched = bool(re.search(sq.query, msg.text, re.IGNORECASE))
                    except re.error:
                        pass
                elif sq.is_fts:
                    matched = _fts_query_matches(sq.query, msg.text)
                else:
                    matched = sq.query.lower() in msg.text.lower()

                if matched:
                    if sq.id is None:
                        continue
                    key = sq.id
                    if key in matches:
                        name, count, preview = matches[key]
                        matches[key] = (name, count + 1, preview)
                    else:
                        matches[key] = (sq.query, 1, msg.text[:200])

        result: dict[int, int] = {}
        for sq_id, (name, count, preview) in matches.items():
            result[sq_id] = count
            if count == 1:
                await self._notifier.notify(f"Query '{name}' matched in channel:\n{preview}")
            else:
                await self._notifier.notify(
                    f"Query '{name}' matched {count} times. First:\n{preview}"
                )

        return result


def _fts_query_matches(fts_query: str, text: str) -> bool:
    """Approximate FTS5 boolean query matching against plain text."""
    text_lower = text.lower()
    parts = re.split(r"\bAND\b", fts_query, flags=re.IGNORECASE)
    for part in parts:
        part = part.strip().strip("()")
        alternatives = re.split(r"\bOR\b", part, flags=re.IGNORECASE)
        if not any(alt.strip().strip('"').rstrip("*").lower() in text_lower for alt in alternatives):
            return False
    return True
