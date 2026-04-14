"""Tests for src/cli/commands/export.py — CLI export subcommands."""
from __future__ import annotations

import argparse
import asyncio
import json
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

from src.cli.commands.export import _export_csv, _export_json, _export_rss, _rfc822, run


def _fake_asyncio_run(coro):
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


def _args(**overrides):
    defaults = {"config": "config.yaml", "export_action": "json", "limit": 100,
                "output": None, "channel_id": None}
    defaults.update(overrides)
    return argparse.Namespace(**defaults)


def _make_db():
    db = MagicMock()
    db.close = AsyncMock()
    return db


def _make_msg(**overrides):
    msg = MagicMock()
    msg.id = 1
    msg.channel_id = 100
    msg.message_id = 42
    msg.date = datetime(2024, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
    msg.text = "Hello world"
    msg.views = 100
    msg.forwards = 5
    for k, v in overrides.items():
        setattr(msg, k, v)
    return msg


# ---------------------------------------------------------------------------
# _rfc822
# ---------------------------------------------------------------------------


def test_rfc822_none():
    result = _rfc822(None)
    assert str(datetime.now(timezone.utc).year) in result


def test_rfc822_with_datetime():
    dt = datetime(2024, 1, 15, 12, 0, 0, tzinfo=timezone.utc)
    result = _rfc822(dt)
    assert "Jan" in result or "2024" in result


def test_rfc822_naive_datetime():
    dt = datetime(2024, 6, 1, 0, 0, 0)
    result = _rfc822(dt)
    assert "2024" in result


# ---------------------------------------------------------------------------
# _export_json
# ---------------------------------------------------------------------------


def test_export_json():
    msgs = [_make_msg()]
    result = _export_json(msgs)
    data = json.loads(result)
    assert len(data) == 1
    assert data[0]["text"] == "Hello world"
    assert data[0]["channel_id"] == 100


# ---------------------------------------------------------------------------
# _export_csv
# ---------------------------------------------------------------------------


def test_export_csv():
    msgs = [_make_msg()]
    result = _export_csv(msgs)
    assert "Hello world" in result
    assert "channel_id" in result


# ---------------------------------------------------------------------------
# _export_rss
# ---------------------------------------------------------------------------


def test_export_rss():
    msgs = [_make_msg()]
    result = _export_rss(msgs)
    assert "<?xml" in result
    assert "<title>" in result
    assert "Hello world" in result


def test_export_rss_skips_empty_text():
    msgs = [_make_msg(text="")]
    result = _export_rss(msgs)
    assert "<item>" not in result


# ---------------------------------------------------------------------------
# run() integration
# ---------------------------------------------------------------------------


def test_run_json_no_messages(capsys):
    db = _make_db()
    db.search_messages = AsyncMock(return_value=([], 0))
    with patch("src.cli.commands.export.runtime.init_db", AsyncMock(return_value=(MagicMock(), db))), \
         patch("asyncio.run", _fake_asyncio_run):
        run(_args(export_action="json"))
    assert "No messages" in capsys.readouterr().err


def test_run_json_with_messages(capsys):
    db = _make_db()
    msgs = [_make_msg()]
    db.search_messages = AsyncMock(return_value=(msgs, 1))
    with patch("src.cli.commands.export.runtime.init_db", AsyncMock(return_value=(MagicMock(), db))), \
         patch("asyncio.run", _fake_asyncio_run):
        run(_args(export_action="json"))
    out = capsys.readouterr().out
    data = json.loads(out)
    assert len(data) == 1


def test_run_csv_with_messages(capsys):
    db = _make_db()
    msgs = [_make_msg()]
    db.search_messages = AsyncMock(return_value=(msgs, 1))
    with patch("src.cli.commands.export.runtime.init_db", AsyncMock(return_value=(MagicMock(), db))), \
         patch("asyncio.run", _fake_asyncio_run):
        run(_args(export_action="csv"))
    out = capsys.readouterr().out
    assert "Hello world" in out


def test_run_rss_with_messages(capsys):
    db = _make_db()
    msgs = [_make_msg()]
    db.search_messages = AsyncMock(return_value=(msgs, 1))
    with patch("src.cli.commands.export.runtime.init_db", AsyncMock(return_value=(MagicMock(), db))), \
         patch("asyncio.run", _fake_asyncio_run):
        run(_args(export_action="rss"))
    assert "<?xml" in capsys.readouterr().out


def test_run_to_file(capsys, tmp_path):
    db = _make_db()
    msgs = [_make_msg()]
    db.search_messages = AsyncMock(return_value=(msgs, 1))
    outfile = str(tmp_path / "out.json")
    with patch("src.cli.commands.export.runtime.init_db", AsyncMock(return_value=(MagicMock(), db))), \
         patch("asyncio.run", _fake_asyncio_run):
        run(_args(export_action="json", output=outfile))
    with open(outfile) as f:
        data = json.load(f)
    assert len(data) == 1


def test_run_unknown_format(capsys):
    db = _make_db()
    msgs = [_make_msg()]
    db.search_messages = AsyncMock(return_value=(msgs, 1))
    with patch("src.cli.commands.export.runtime.init_db", AsyncMock(return_value=(MagicMock(), db))), \
         patch("asyncio.run", _fake_asyncio_run):
        run(_args(export_action="xml"))
    assert "Unknown" in capsys.readouterr().err


def test_run_with_channel_filter(capsys):
    db = _make_db()
    msgs = [_make_msg()]
    db.search_messages = AsyncMock(return_value=(msgs, 1))
    with patch("src.cli.commands.export.runtime.init_db", AsyncMock(return_value=(MagicMock(), db))), \
         patch("asyncio.run", _fake_asyncio_run):
        run(_args(export_action="json", channel_id=100))
    db.search_messages.assert_called_once_with(channel_id=100, limit=100)
