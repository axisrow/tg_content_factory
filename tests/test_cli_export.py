"""Tests for CLI export commands."""
from __future__ import annotations

import json

import pytest

from src.models import Message
from tests.helpers import cli_ns as _ns

NOW = __import__("datetime").datetime(
    2025, 1, 1, 12, 0, 0, tzinfo=__import__("datetime").timezone.utc
)
pytestmark = pytest.mark.aiosqlite_serial


def _add_message(db, channel_id=100, message_id=1, text="hello"):
    __import__("asyncio").run(
        db.insert_message(
            Message(channel_id=channel_id, message_id=message_id, text=text, date=NOW)
        )
    )


def _add_channel(db, channel_id=100, title="TestCh"):
    from src.models import Channel

    return __import__("asyncio").run(
        db.add_channel(Channel(channel_id=channel_id, title=title))
    )


def test_export_json(cli_env, capsys):
    _add_channel(cli_env, channel_id=100)
    _add_message(cli_env, channel_id=100, message_id=1, text="test message")

    from src.cli.commands.export import run

    run(_ns(export_action="json", limit=10, channel_id=None, output=None))
    out = capsys.readouterr().out
    data = json.loads(out)
    assert isinstance(data, list)
    assert len(data) >= 1
    assert data[0]["text"] == "test message"


def test_export_csv(cli_env, capsys):
    _add_channel(cli_env, channel_id=100)
    _add_message(cli_env, channel_id=100, message_id=1, text="csv test")

    from src.cli.commands.export import run

    run(_ns(export_action="csv", limit=10, channel_id=None, output=None))
    out = capsys.readouterr().out
    assert "channel_id" in out
    assert "csv test" in out


def test_export_rss(cli_env, capsys):
    _add_channel(cli_env, channel_id=100)
    _add_message(cli_env, channel_id=100, message_id=1, text="rss test content")

    from src.cli.commands.export import run

    run(_ns(export_action="rss", limit=10, channel_id=None, output=None))
    out = capsys.readouterr().out
    assert "<rss" in out
    assert "rss test content" in out


def test_export_empty(cli_env, capsys):
    from src.cli.commands.export import run

    run(_ns(export_action="json", limit=10, channel_id=None, output=None))
    err = capsys.readouterr().err
    assert "No messages found" in err


def test_export_to_file(cli_env, tmp_path, capsys):
    _add_channel(cli_env, channel_id=100)
    _add_message(cli_env, channel_id=100, message_id=1, text="file test")

    output_path = str(tmp_path / "export.json")
    from src.cli.commands.export import run

    run(_ns(export_action="json", limit=10, channel_id=None, output=output_path))
    err = capsys.readouterr().err
    assert "Exported" in err

    with open(output_path) as f:
        data = json.load(f)
    assert len(data) >= 1


def test_export_with_channel_filter(cli_env, capsys):
    _add_channel(cli_env, channel_id=100)
    _add_channel(cli_env, channel_id=200)
    _add_message(cli_env, channel_id=100, message_id=1, text="from 100")
    _add_message(cli_env, channel_id=200, message_id=2, text="from 200")

    from src.cli.commands.export import run

    run(_ns(export_action="json", limit=10, channel_id=100, output=None))
    out = capsys.readouterr().out
    data = json.loads(out)
    assert all(m["channel_id"] == 100 for m in data)
    assert any(m["text"] == "from 100" for m in data)
