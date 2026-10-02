"""Tests for CLI messages read command."""
from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.config import AppConfig
from src.models import Account, Message
from tests.helpers import cli_add_channel as _add_channel
from tests.helpers import cli_ns as _ns

NOW = __import__("datetime").datetime(
    2025, 1, 1, 12, 0, 0, tzinfo=__import__("datetime").timezone.utc
)
pytestmark = pytest.mark.aiosqlite_serial


def _add_message(db, channel_id=100, message_id=1, text="hello", reactions_json=None):
    __import__("asyncio").run(
        db.insert_message(
            Message(
                channel_id=channel_id,
                message_id=message_id,
                text=text,
                date=NOW,
                reactions_json=reactions_json,
            )
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
    assert "reactions:" not in out


def test_messages_read_text_format_with_reactions(cli_env, capsys):
    _add_channel(cli_env, channel_id=100, title="MsgCh")
    _add_message(
        cli_env,
        channel_id=100,
        message_id=1,
        text="reacted message",
        reactions_json='[{"emoji": "👍", "count": 5}, {"emoji": "❤️", "count": 2}]',
    )

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
    assert "reacted message" in out
    assert "reactions: 👍 5 ❤️ 2" in out


def test_messages_read_json_format(cli_env, capsys):
    _add_channel(cli_env, channel_id=100, title="MsgCh")
    _add_message(
        cli_env,
        channel_id=100,
        message_id=1,
        text="json msg",
        reactions_json='[{"emoji": "🔥", "count": 7}]',
    )

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
    assert any(m["reactions"] == [{"emoji": "🔥", "count": 7}] for m in data)


def test_messages_read_csv_format(cli_env, capsys):
    _add_channel(cli_env, channel_id=100, title="MsgCh")
    _add_message(
        cli_env,
        channel_id=100,
        message_id=1,
        text="csv msg",
        reactions_json='[{"emoji": "custom:42", "count": 3}]',
    )

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
    assert "reactions" in out.splitlines()[0]
    assert "custom:42 3" in out


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


# --------------------------------------------------------------------------- #
# --live mode: default account is the DB primary (#1480)
# --------------------------------------------------------------------------- #

NOW_ISO = "2026-10-02T12:00:00+00:00"


async def _fake_iter(items):
    for item in items:
        yield item


def _run_live(cli_db, identifier, *, capsys, resolve_side_effect=None):
    from src.cli.commands.messages import messages_read_impl

    pool = MagicMock()
    pool.clients = {"+10000000001": object(), "+90000000009": object()}
    fake_client = MagicMock()
    fake_client.iter_messages = MagicMock(
        return_value=_fake_iter([SimpleNamespace(id=7, date=NOW_ISO, sender=None,
                                                 text="live hello", media=None)])
    )
    pool.get_native_client_by_phone = AsyncMock(return_value=(fake_client, "+90000000009"))
    if resolve_side_effect is not None:
        pool.resolve_entity_with_warm = AsyncMock(side_effect=resolve_side_effect)
    else:
        pool.resolve_entity_with_warm = AsyncMock(return_value=SimpleNamespace(id=1))
    pool.disconnect_all = AsyncMock()

    async def fake_init_db(_):
        return AppConfig(), cli_db

    async def fake_init_pool(_, __):
        return MagicMock(), pool

    with patch("src.cli.runtime.init_db", side_effect=fake_init_db), \
         patch("src.cli.runtime.init_pool", side_effect=fake_init_pool):
        asyncio.run(messages_read_impl(
            "config.yaml", identifier=identifier, limit=10, live=True, phone=None,
        ))
    return pool, capsys.readouterr().out


def test_messages_read_live_defaults_to_db_primary(cli_db, capsys):
    """Sorted-first must NOT win: without --phone the DB primary is queried (#1480)."""
    asyncio.run(cli_db.add_account(Account(phone="+10000000001", session_string="a")))
    asyncio.run(cli_db.add_account(Account(phone="+90000000009", session_string="b", is_primary=True)))

    pool, out = _run_live(cli_db, "@somedialog", capsys=capsys)

    assert "live hello" in out
    assert pool.resolve_entity_with_warm.await_args.args[1] == "+90000000009"


def test_messages_read_live_resolve_failure_names_account_and_hints_phone(cli_db, capsys):
    """A resolve failure says which account was queried and hints --phone (#1480)."""
    asyncio.run(cli_db.add_account(Account(phone="+90000000009", session_string="b", is_primary=True)))

    pool, out = _run_live(
        cli_db, "@somedialog", capsys=capsys,
        resolve_side_effect=ValueError("Cannot find any entity"),
    )

    assert "Cannot resolve '@somedialog' via account +90000000009" in out
    assert "--phone" in out
