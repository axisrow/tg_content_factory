"""Tests for CLI messages read command."""
from __future__ import annotations

import json

import pytest

from src.models import Message
from tests.helpers import cli_add_channel as _add_channel
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


def test_messages_read_text_format(cli_env, capsys):
    _add_channel(cli_env, channel_id=100, title="MsgCh")
    _add_message(cli_env, channel_id=100, message_id=1, text="test message")

    from src.cli.commands.messages import run

    run(_ns(
        messages_action="read",
        identifier="100",
        limit=50,
        live=False,
        phone=None,
        query="",
        date_from=None,
        date_to=None,
        topic_id=None,
        offset_id=None,
        output_format="text",
    ))
    out = capsys.readouterr().out
    assert "test message" in out
    assert "Total:" in out


def test_messages_read_json_format(cli_env, capsys):
    _add_channel(cli_env, channel_id=100, title="MsgCh")
    _add_message(cli_env, channel_id=100, message_id=1, text="json msg")

    from src.cli.commands.messages import run

    run(_ns(
        messages_action="read",
        identifier="100",
        limit=50,
        live=False,
        phone=None,
        query="",
        date_from=None,
        date_to=None,
        topic_id=None,
        offset_id=None,
        output_format="json",
    ))
    out = capsys.readouterr().out
    data = json.loads(out)
    assert isinstance(data, list)
    assert any(m["text"] == "json msg" for m in data)


def test_messages_read_csv_format(cli_env, capsys):
    _add_channel(cli_env, channel_id=100, title="MsgCh")
    _add_message(cli_env, channel_id=100, message_id=1, text="csv msg")

    from src.cli.commands.messages import run

    run(_ns(
        messages_action="read",
        identifier="100",
        limit=50,
        live=False,
        phone=None,
        query="",
        date_from=None,
        date_to=None,
        topic_id=None,
        offset_id=None,
        output_format="csv",
    ))
    out = capsys.readouterr().out
    assert "csv msg" in out
    assert "channel_id" in out


def test_messages_read_with_query_filter(cli_env, capsys):
    _add_channel(cli_env, channel_id=100, title="MsgCh")
    _add_message(cli_env, channel_id=100, message_id=1, text="important alert")
    _add_message(cli_env, channel_id=100, message_id=2, text="boring stuff")

    from src.cli.commands.messages import run

    run(_ns(
        messages_action="read",
        identifier="100",
        limit=50,
        live=False,
        phone=None,
        query="important",
        date_from=None,
        date_to=None,
        topic_id=None,
        offset_id=None,
        output_format="json",
    ))
    out = capsys.readouterr().out
    data = json.loads(out)
    assert len(data) >= 1
    assert any("important" in m["text"] for m in data)


def test_messages_read_channel_not_found(cli_env, capsys):
    from src.cli.commands.messages import run

    run(_ns(
        messages_action="read",
        identifier="99999",
        limit=50,
        live=False,
        phone=None,
        query="",
        date_from=None,
        date_to=None,
        topic_id=None,
        offset_id=None,
        output_format="text",
    ))
    out = capsys.readouterr().out
    assert "not found" in out.lower() or "No messages found" in out


def test_messages_read_no_messages(cli_env, capsys):
    _add_channel(cli_env, channel_id=100, title="EmptyCh")

    from src.cli.commands.messages import run

    run(_ns(
        messages_action="read",
        identifier="100",
        limit=50,
        live=False,
        phone=None,
        query="",
        date_from=None,
        date_to=None,
        topic_id=None,
        offset_id=None,
        output_format="text",
    ))
    out = capsys.readouterr().out
    assert "No messages found" in out


def test_messages_read_by_pk(cli_env, capsys):
    pk = _add_channel(cli_env, channel_id=500, title="PkCh")
    _add_message(cli_env, channel_id=500, message_id=1, text="found by pk")

    from src.cli.commands.messages import run

    run(_ns(
        messages_action="read",
        identifier=str(pk),
        limit=50,
        live=False,
        phone=None,
        query="",
        date_from=None,
        date_to=None,
        topic_id=None,
        offset_id=None,
        output_format="json",
    ))
    out = capsys.readouterr().out
    data = json.loads(out)
    assert any(m["text"] == "found by pk" for m in data)
