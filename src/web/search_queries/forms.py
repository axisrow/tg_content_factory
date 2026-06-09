"""Form DTO for the search-queries web domain."""

from __future__ import annotations

from dataclasses import dataclass

from fastapi import Form


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


def search_query_form(
    query: str = Form(""),
    interval_minutes: int = Form(60),
    is_regex: bool = Form(False),
    is_fts: bool = Form(False),
    notify_on_collect: bool = Form(False),
    track_stats: bool = Form(False),
    exclude_patterns: str = Form(""),
    max_length: int | None = Form(None),
    chat_filter: str = Form(""),
) -> SearchQueryForm:
    """FastAPI dependency assembling a SearchQueryForm from posted form fields.

    Shared by the add and edit routes so the nine field declarations live once.
    """
    return SearchQueryForm(
        query=query,
        interval_minutes=interval_minutes,
        is_regex=is_regex,
        is_fts=is_fts,
        notify_on_collect=notify_on_collect,
        track_stats=track_stats,
        exclude_patterns=exclude_patterns,
        max_length=max_length,
        chat_filter=chat_filter,
    )
