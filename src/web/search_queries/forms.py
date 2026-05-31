"""Form DTO for the search-queries web domain."""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SearchQueryForm:
    query: str = ""
    interval_minutes: int = 60
    is_regex: bool = False
    is_fts: bool = False
    notify_on_collect: bool = False
    track_stats: bool = False
    exclude_patterns: str = ""
    max_length: int | None = None
    chat_filter: str = ""
