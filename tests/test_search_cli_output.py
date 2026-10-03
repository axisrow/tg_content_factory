"""CLI search output must expose channel username/title, not bare numeric id.

Bug (2026-10-03): premium global search returns messages carrying
``channel_username``/``channel_title`` (src/search/telegram_search.py), but the
CLI printed only ``Channel <numeric_id>`` — channels absent from the accounts'
dialogs could not be re-resolved and added to collection.
"""

from datetime import datetime, timezone

from src.cli.commands.search import _format_search_result_line
from src.models import Message


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
