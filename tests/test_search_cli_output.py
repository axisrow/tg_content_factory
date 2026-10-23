"""CLI search output must expose channel username/title, not bare numeric id.

Bug (2026-10-03): premium global search returns messages carrying
``channel_username``/``channel_title`` (src/search/telegram_search.py), but the
CLI printed only ``Channel <numeric_id>`` — channels absent from the accounts'
dialogs could not be re-resolved and added to collection.
"""

from __future__ import annotations

import csv
from datetime import datetime, timezone

from tg_agent.cli.commands.search import (
    _format_search_result_line,
    _print_search_result,
    _search_envelope,
)
from tg_agent.models import Message, SearchResult


def _msg(
    *,
    channel_username: str | None = None,
    channel_title: str | None = None,
) -> Message:
    return Message(
        channel_id=3928033429,
        message_id=42,
        date=datetime(2026, 10, 3, 12, 0, tzinfo=timezone.utc),
        text="пост про харнессы",
        channel_username=channel_username,
        channel_title=channel_title,
    )


def test_line_prefers_username_over_numeric_id() -> None:
    line = _format_search_result_line(_msg(channel_username="techs_dump", channel_title="техно-свалка"))
    assert "@techs_dump" in line
    assert "3928033429" in line
    assert "пост про харнессы" in line


def test_line_falls_back_to_title_when_no_username() -> None:
    line = _format_search_result_line(_msg(channel_title="Нейроканал"))
    assert "Нейроканал" in line
    assert "3928033429" in line


def test_line_falls_back_to_numeric_id_when_no_meta() -> None:
    line = _format_search_result_line(_msg())
    assert "Channel 3928033429" in line


# ---------------------------------------------------------------------------
# #1617 — `search --format csv` must keep text over 500 chars whole
# ---------------------------------------------------------------------------


def test_csv_keeps_full_text_over_500_chars(capsys) -> None:
    long_text = "START|" + "0123456789" * 55 + "|END: this part must survive CSV export"
    msg = _msg()
    msg.text = long_text
    result = SearchResult(messages=[msg], total=1, query="START")
    _print_search_result(result, "csv")
    out = capsys.readouterr().out
    row = next(csv.reader(out.splitlines()[1:]))
    assert row[4] == long_text


def test_envelope_exposes_truncation_contract() -> None:
    # The per-query envelope is the digest sweep's completeness signal:
    # total + has_more must survive into batch JSON, not just the message list.
    result = SearchResult(messages=[_msg()], total=7, has_more=True, query="харнесс")
    envelope = _search_envelope(result)
    assert envelope["query"] == "харнесс"
    assert envelope["total"] == 7
    assert envelope["has_more"] is True
    assert envelope["error"] is None
    assert envelope["messages"][0]["message_id"] == 42
