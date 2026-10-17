"""Tests for CLI search: date-window filters and json/text output formats.

The digest workflow (skills/tg-agent/digest.md) pulls its 24h window through
``search --date-from --date-to --format json`` — these tests pin that contract.
"""
from __future__ import annotations

import asyncio
import json
from datetime import datetime, timezone

import pytest

from tests.helpers import cli_add_channel as _add_channel
from tests.helpers import cli_ns as _ns
from tg_agent.models import Message

FRESH = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
OLD = datetime(2024, 12, 30, 12, 0, 0, tzinfo=timezone.utc)
pytestmark = pytest.mark.aiosqlite_serial


def _add_message(db, message_id: int, text: str, date: datetime) -> None:
    asyncio.run(
        db.insert_message(
            Message(channel_id=100, message_id=message_id, text=text, date=date)
        )
    )


def _seed(db) -> None:
    _add_channel(db, channel_id=100, title="DigestCh")
    _add_message(db, 1, "новый релиз нейросети от OpenAI", FRESH)
    _add_message(db, 2, "старая новость про нейросети", OLD)


def _run_search(**overrides) -> None:
    """Invoke the argparse-path ``run()``; unset attrs fall back to run()'s defaults."""
    from tg_agent.cli.commands.search import run

    kwargs = {"query": "нейросети", "mode": "local"}
    kwargs.update(overrides)
    run(_ns(**kwargs))


def test_search_json_respects_date_window(cli_env, capsys):
    _seed(cli_env)

    _run_search(date_from="2025-01-01", date_to="2025-01-01", output_format="json")

    data = json.loads(capsys.readouterr().out)
    assert data["error"] is None
    assert data["total"] == 1
    assert data["has_more"] is False
    assert data["query"] == "нейросети"
    assert len(data["messages"]) == 1
    hit = data["messages"][0]
    assert hit["message_id"] == 1
    assert hit["date"].startswith("2025-01-01")
    assert hit["channel_title"] == "DigestCh"
    assert "нейросети от OpenAI" in hit["text"]


def test_search_json_without_window_returns_all(cli_env, capsys):
    _seed(cli_env)

    _run_search(output_format="json")

    data = json.loads(capsys.readouterr().out)
    assert {item["message_id"] for item in data["messages"]} == {1, 2}


def test_search_text_format_unchanged(cli_env, capsys):
    _seed(cli_env)

    _run_search(date_from="2025-01-01", date_to="2025-01-01")

    out = capsys.readouterr().out
    assert "Found" in out
    assert "нейросети от OpenAI" in out
    assert "старая новость" not in out
    assert '"message_id"' not in out  # stays text, not json


def test_search_date_window_rejected_for_live_modes(cli_env, caplog):
    # telegram mode takes no date bounds — must fail loudly BEFORE any pool
    # init (no accounts here), not silently ignore the window (#1350 class).
    _run_search(mode="telegram", date_from="2025-01-01")

    assert "only supported" in caplog.text
