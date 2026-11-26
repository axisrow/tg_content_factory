"""Tests for CLI messages read command."""
from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import typer
from telethon.tl.types import Channel as TLChannel
from telethon.tl.types import ChatPhotoEmpty
from telethon.tl.types import User as TLUser

from tests.helpers import cli_add_channel as _add_channel
from tests.helpers import cli_ns as _ns
from tg_agent.config import AppConfig
from tg_agent.models import Account, Message

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

    from tg_agent.cli.commands.messages import run

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

    from tg_agent.cli.commands.messages import run

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

    from tg_agent.cli.commands.messages import run

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

    from tg_agent.cli.commands.messages import run

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

    from tg_agent.cli.commands.messages import run

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
    from tg_agent.cli.commands.messages import run

    with pytest.raises(typer.Exit) as exc_info:
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
    captured = capsys.readouterr()
    assert "not found" in captured.err.lower()
    assert captured.out == ""
    assert exc_info.value.exit_code == 1


def test_messages_read_no_messages(cli_env, capsys):
    _add_channel(cli_env, channel_id=100, title="EmptyCh")

    from tg_agent.cli.commands.messages import run

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

    from tg_agent.cli.commands.messages import run

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


def test_messages_read_names_a_filtered_channels_history(cli_env, capsys):
    """#1917: the filter flag hides a channel from cross-channel search, not from its own read."""
    pk = _add_channel(cli_env, channel_id=600, title="FilteredCh")
    _add_message(cli_env, channel_id=600, message_id=1, text="still stored")
    asyncio.run(cli_env.repos.channels.set_channel_filtered(pk, True, origin="auto"))

    from tg_agent.cli.commands.messages import run

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
    data = json.loads(capsys.readouterr().out)
    assert [m["text"] for m in data] == ["still stored"]


# --------------------------------------------------------------------------- #
# --live mode: default account is the DB primary (#1480)
# --------------------------------------------------------------------------- #

NOW_ISO = "2026-10-02T12:00:00+00:00"


async def _fake_iter(items):
    for item in items:
        yield item


def _run_live(cli_db, identifier, *, capsys, resolve_side_effect=None):
    from tg_agent.cli.commands.messages import messages_read_impl

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

    with patch("tg_agent.cli.runtime.init_db", side_effect=fake_init_db), \
         patch("tg_agent.cli.runtime.init_pool", side_effect=fake_init_pool):
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
    """A resolve failure says which account was queried and hints --phone (#1480).

    Since #1486 it also fails the process: stderr + non-zero exit.
    """
    asyncio.run(cli_db.add_account(Account(phone="+90000000009", session_string="b", is_primary=True)))

    with pytest.raises(typer.Exit) as excinfo:
        _run_live(
            cli_db, "@somedialog", capsys=capsys,
            resolve_side_effect=ValueError("Cannot find any entity"),
        )

    assert excinfo.value.exit_code == 1
    err = capsys.readouterr().err
    assert "Cannot resolve '@somedialog' via account +90000000009" in err
    assert "--phone" in err


def test_messages_read_live_empty_phone_falls_back_to_default(cli_db, capsys):
    """`--phone ''` (templating agents pass empty strings) must fall back to the
    default account, not hard-error 'Account  not connected.' (#1480 review M1)."""
    asyncio.run(cli_db.add_account(Account(phone="+90000000009", session_string="b", is_primary=True)))

    from tg_agent.cli.commands.messages import messages_read_impl

    pool = MagicMock()
    pool.clients = {"+90000000009": object()}
    fake_client = MagicMock()
    fake_client.iter_messages = MagicMock(
        return_value=_fake_iter([SimpleNamespace(id=7, date=NOW_ISO, sender=None,
                                                 text="empty phone ok", media=None)])
    )
    pool.get_native_client_by_phone = AsyncMock(return_value=(fake_client, "+90000000009"))
    pool.resolve_entity_with_warm = AsyncMock(return_value=SimpleNamespace(id=1))
    pool.disconnect_all = AsyncMock()

    async def fake_init_db(_):
        return AppConfig(), cli_db

    async def fake_init_pool(_, __):
        return MagicMock(), pool

    with patch("tg_agent.cli.runtime.init_db", side_effect=fake_init_db), \
         patch("tg_agent.cli.runtime.init_pool", side_effect=fake_init_pool):
        asyncio.run(messages_read_impl(
            "config.yaml", identifier="@somedialog", limit=10, live=True, phone="",
        ))

    out = capsys.readouterr().out
    assert "empty phone ok" in out
    assert "not connected" not in out


# ---------------------------------------------------------------------------
# issue #1598: --format json/csv must honor the declared format on empty
# results; errors go to stderr with a non-zero exit code; --live rejects
# non-text formats explicitly instead of silently ignoring --format.
# ---------------------------------------------------------------------------


def test_messages_read_json_empty_result_is_valid_json(cli_env, capsys):
    _add_channel(cli_env, channel_id=100, title="EmptyCh")

    from tg_agent.cli.commands.messages import run

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
    assert json.loads(out) == []


def test_messages_read_csv_empty_result_has_header(cli_env, capsys):
    _add_channel(cli_env, channel_id=100, title="EmptyCh")

    from tg_agent.cli.commands.messages import run

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
    lines = out.splitlines()
    assert lines, "CSV output must not be empty"
    assert lines[0].startswith("id,")


def test_messages_read_channel_not_found_json_stderr_and_exit(cli_env, capsys):
    from tg_agent.cli.commands.messages import run

    with pytest.raises(typer.Exit) as exc_info:
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
            output_format="json",
        ))
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "not found" in captured.err.lower()
    assert exc_info.value.exit_code == 1


def test_messages_read_live_rejects_csv_format(cli_env, capsys):
    """Live CSV stays refused (#1598); live JSON became supported in #1872."""
    from tg_agent.cli.commands.messages import run

    with pytest.raises(typer.Exit) as exc_info:
        run(_ns(
            messages_action="read",
            identifier="100",
            limit=50,
            live=True,
            phone=None,
            query="",
            date_from=None,
            date_to=None,
            topic_id=None,
            offset_id=None,
            output_format="csv",
        ))
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "csv" in captured.err.lower()
    assert exc_info.value.exit_code == 1


# ---------------------------------------------------------------------------
# issue #1872: --after-id forward cursor + live JSON envelope
# ---------------------------------------------------------------------------


def _live_msg(i, *, text="m", out=False, mentioned=False, sender=None, reply_to=None):
    return SimpleNamespace(
        id=i, date=NOW, sender=sender, text=text, media=None, out=out, mentioned=mentioned,
        reply_to=reply_to,
    )


def _run_live_read(cli_db, identifier, *, capsys, messages, entity, output_format="json",
                   after_id=None, offset_id=None, limit=10):
    from tg_agent.cli.commands.messages import messages_read_impl

    pool = MagicMock()
    pool.clients = {"+90000000009": object()}
    fake_client = MagicMock()
    fake_client.iter_messages = MagicMock(return_value=_fake_iter(messages))
    pool.get_native_client_by_phone = AsyncMock(return_value=(fake_client, "+90000000009"))
    pool.resolve_entity_with_warm = AsyncMock(return_value=entity)
    pool.disconnect_all = AsyncMock()

    async def fake_init_db(_):
        return AppConfig(), cli_db

    async def fake_init_pool(_, __):
        return MagicMock(), pool

    with patch("tg_agent.cli.runtime.init_db", side_effect=fake_init_db), \
         patch("tg_agent.cli.runtime.init_pool", side_effect=fake_init_pool):
        asyncio.run(messages_read_impl(
            "config.yaml", identifier=identifier, limit=limit, live=True,
            phone="+90000000009", after_id=after_id, offset_id=offset_id,
            output_format=output_format,
        ))
    return fake_client, capsys.readouterr()


def test_messages_read_live_json_after_id_forward_cursor(cli_db, capsys):
    """--after-id fetches forward (min_id+reverse) and emits the cursor envelope."""
    msgs = [
        _live_msg(5, text="first", sender=TLUser(id=4242, first_name="Ann")),
        _live_msg(6, text="second"), _live_msg(7, text="third"),
    ]
    client, captured = _run_live_read(
        cli_db, "@friend", capsys=capsys, messages=msgs,
        entity=TLUser(id=4242, first_name="Ann"), after_id=4,
    )

    kwargs = client.iter_messages.call_args.kwargs
    assert kwargs["min_id"] == 4
    assert kwargs["reverse"] is True
    assert kwargs["limit"] == 11  # limit+1 powers the exact has_more (#1872)
    envelope = json.loads(captured.out)
    assert [m["id"] for m in envelope["messages"]] == [5, 6, 7]
    assert envelope["newest_id"] == 7
    assert envelope["has_more"] is False
    first = envelope["messages"][0]
    assert first["out"] is False
    assert first["addressed"] is True  # private chat: incoming is always addressed
    assert first["sender"] == "Ann"
    assert first["text"] == "first"
    assert first["date"] == NOW.isoformat()
    assert first["reply_to"] is None  # a top-level message


def test_messages_read_live_json_names_the_replied_message(cli_db, capsys):
    """#1938: a reply carries its target; a forum topic's root is a location, not a reply."""
    reply = SimpleNamespace(reply_to_msg_id=41, forum_topic=False)
    in_topic = SimpleNamespace(reply_to_msg_id=900, forum_topic=True)
    # A reply inside a topic keeps forum_topic; the topic's root moves to top_id.
    in_topic_reply = SimpleNamespace(reply_to_msg_id=41, reply_to_top_id=900, forum_topic=True)
    msgs = [_live_msg(5, text="question"), _live_msg(6, text="answer", reply_to=reply),
            _live_msg(7, text="in topic", reply_to=in_topic),
            _live_msg(8, text="answer in topic", reply_to=in_topic_reply)]
    _, captured = _run_live_read(
        cli_db, "@room", capsys=capsys, messages=msgs,
        entity=TLUser(id=4242, first_name="Ann"),
    )

    recs = {m["id"]: m["reply_to"] for m in json.loads(captured.out)["messages"]}
    assert recs == {5: None, 6: 41, 7: None, 8: 41}


def test_messages_read_live_text_marks_the_replied_message(cli_db, capsys):
    """#1938: the text header names the replied message, a top-level one stays as is."""
    reply = SimpleNamespace(reply_to_msg_id=41, forum_topic=False)
    msgs = [_live_msg(5, text="question"), _live_msg(6, text="answer", reply_to=reply)]
    _, captured = _run_live_read(
        cli_db, "@room", capsys=capsys, messages=msgs,
        entity=TLUser(id=4242, first_name="Ann"), output_format="text",
    )

    headers = [line for line in captured.out.splitlines() if line.startswith("[")]
    assert len(headers) == 2
    assert headers[0].endswith("#5")
    assert headers[1].endswith("#6 reply to #41")


def test_messages_read_live_json_after_id_truncates_with_has_more(cli_db, capsys):
    msgs = [_live_msg(i, text=f"m{i}") for i in range(5, 9)]
    _, captured = _run_live_read(
        cli_db, "@friend", capsys=capsys, messages=msgs,
        entity=TLUser(id=4242, first_name="Ann"), after_id=4, limit=2,
    )
    envelope = json.loads(captured.out)
    assert [m["id"] for m in envelope["messages"]] == [5, 6]
    assert envelope["newest_id"] == 6
    assert envelope["has_more"] is True


def test_messages_read_live_json_empty_is_clean_envelope(cli_db, capsys):
    _, captured = _run_live_read(
        cli_db, "@friend", capsys=capsys, messages=[],
        entity=TLUser(id=4242, first_name="Ann"), after_id=99,
    )
    assert json.loads(captured.out) == {"messages": [], "newest_id": None, "has_more": False}


def test_messages_read_live_json_group_addressed_by_mention(cli_db, capsys):
    msgs = [
        _live_msg(5, text="plain"),
        _live_msg(6, text="mention", mentioned=True),
        _live_msg(7, text="mine", out=True),
    ]
    _, captured = _run_live_read(
        cli_db, "@room", capsys=capsys, messages=msgs,
        entity=TLChannel(id=777, title="Room", photo=ChatPhotoEmpty(), date=NOW),
    )
    recs = {m["id"]: m for m in json.loads(captured.out)["messages"]}
    assert recs[5]["addressed"] is False
    assert recs[6]["addressed"] is True
    assert recs[7]["out"] is True
    assert recs[7]["addressed"] is False


def test_messages_read_live_json_sender_username_and_channel_title(cli_db, capsys):
    msgs = [
        _live_msg(5, sender=TLUser(id=1, first_name="A", username="ann")),
        _live_msg(6, sender=TLChannel(id=2, title="News", photo=ChatPhotoEmpty(), date=NOW)),
    ]
    _, captured = _run_live_read(
        cli_db, "@room", capsys=capsys, messages=msgs,
        entity=TLChannel(id=777, title="Room", photo=ChatPhotoEmpty(), date=NOW),
    )
    recs = {m["id"]: m for m in json.loads(captured.out)["messages"]}
    assert recs[5]["sender"] == "@ann"
    assert recs[6]["sender"] == "News"


def test_messages_read_live_text_after_id_stays_text(cli_db, capsys):
    """Text live output keeps its shape and chronological order (#1872, criterion 3)."""
    msgs = [_live_msg(6, text="t6"), _live_msg(5, text="t5")]
    _, captured = _run_live_read(
        cli_db, "@friend", capsys=capsys, messages=msgs,
        entity=TLUser(id=4242, first_name="Ann"), after_id=4, output_format="text",
    )
    out = captured.out
    assert "t5" in out and "t6" in out
    assert out.index("t5") < out.index("t6")
    with pytest.raises(ValueError):
        json.loads(out)


def test_messages_read_after_id_requires_live(cli_env, capsys):
    """--after-id is a live cursor — refused in DB mode (#1872, non-goal)."""
    from tg_agent.cli.commands.messages import run

    with pytest.raises(typer.Exit) as exc_info:
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
            after_id=5,
            output_format="text",
        ))
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "--after-id" in captured.err
    assert exc_info.value.exit_code == 1


def test_messages_read_live_after_id_offset_id_conflict(cli_env, capsys):
    from tg_agent.cli.commands.messages import run

    with pytest.raises(typer.Exit) as exc_info:
        run(_ns(
            messages_action="read",
            identifier="100",
            limit=50,
            live=True,
            phone=None,
            query="",
            date_from=None,
            date_to=None,
            topic_id=None,
            offset_id=9,
            after_id=5,
            output_format="text",
        ))
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "mutually exclusive" in captured.err
    assert exc_info.value.exit_code == 1


def test_messages_read_live_json_rejects_include_reaction_users(cli_env, capsys):
    from tg_agent.cli.commands.messages import run

    with pytest.raises(typer.Exit) as exc_info:
        run(_ns(
            messages_action="read",
            identifier="100",
            limit=50,
            live=True,
            phone=None,
            query="",
            date_from=None,
            date_to=None,
            topic_id=None,
            offset_id=None,
            after_id=None,
            include_reaction_users=True,
            output_format="json",
        ))
    captured = capsys.readouterr()
    assert captured.out == ""
    assert "--include-reaction-users" in captured.err
    assert exc_info.value.exit_code == 1
