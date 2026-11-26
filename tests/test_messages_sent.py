"""Tests for `messages sent`: the outgoing-send registry report (#1893)."""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta, timezone


def _seed(db, phone, chat_id, message_id, *, chat_type="channel", source="dialogs_send", sent_at=None):
    asyncio.run(
        db.repos.sent_messages.record_many(
            phone, chat_id, [message_id], chat_type=chat_type, source=source, sent_at=sent_at
        )
    )


def _sent(**kwargs):
    from tg_agent.cli.commands.messages import messages_sent_impl

    asyncio.run(messages_sent_impl("config.yaml", **kwargs))


def _row(capsys):
    return capsys.readouterr().out


def test_registry_rows_are_listed_newest_first(cli_env, cli_db, capsys):
    _seed(cli_db, "+7001", -100123, 41, sent_at=datetime.now(timezone.utc) - timedelta(hours=2))
    _seed(cli_db, "+7002", None, 7, chat_type="dm", source="dm_draft")

    _sent()

    out = _row(capsys)
    assert "message_id=7" in out
    assert "via=+7002" in out
    assert "source=dm_draft" in out
    assert out.index("message_id=7") < out.index("message_id=41")


def test_deletion_status_is_shown_per_row(cli_env, cli_db, capsys):
    _seed(cli_db, "+7001", -100123, 41)
    asyncio.run(cli_db.repos.observed_deletions.record_many("+7001", -100123, [41]))

    _sent()

    out = _row(capsys)
    assert "status=deleted" in out


def test_a_phone_filter_narrows_the_report(cli_env, cli_db, capsys):
    _seed(cli_db, "+7001", -100123, 41, source="dialogs_send")
    _seed(cli_db, "+7002", None, 7, chat_type="dm", source="dm_draft")

    _sent(phone="+7002")

    out = _row(capsys)
    assert "message_id=7" in out
    assert "message_id=41" not in out


def test_a_source_filter_narrows_the_report(cli_env, cli_db, capsys):
    _seed(cli_db, "+7001", -100123, 41, source="dialogs_send")
    _seed(cli_db, "+7002", None, 7, chat_type="dm", source="dm_draft")

    _sent(source="dialogs_send")

    out = _row(capsys)
    assert "message_id=41" in out
    assert "message_id=7" not in out


def test_json_output_is_a_list_of_dicts(cli_env, cli_db, capsys):
    _seed(cli_db, "+7001", -100123, 41)

    _sent(output_format="json")

    rows = json.loads(_row(capsys))
    assert rows[0]["message_id"] == 41
    assert rows[0]["phone"] == "+7001"


def test_a_bare_channel_id_matches_a_marked_registry_row(cli_env, cli_db, capsys):
    """#1926: the bare id `messages read` prints must find the -100… store row."""
    _seed(cli_db, "+7001", -1001525693788, 41)

    _sent(chat=1525693788)
    assert "message_id=41" in _row(capsys)


def test_a_marked_channel_id_still_matches_its_row(cli_env, cli_db, capsys):
    """#1926 keeps the original form working: the store's own -100… shape."""
    _seed(cli_db, "+7001", -1001525693788, 41)

    _sent(chat=-1001525693788)
    assert "message_id=41" in _row(capsys)


def test_a_comment_row_names_the_post_it_answers(cli_env, cli_db, capsys):
    """#1946: a comment lives in the channel's discussion group; the row says so."""
    asyncio.run(
        cli_db.repos.sent_messages.record_many(
            "+7001", -100123, [777], chat_type="comment", source="dialogs_send",
            recipient="-100555", ref="42",
        )
    )

    _sent()

    out = _row(capsys)
    assert "chat=-100123 type=comment" in out
    assert "to=-100555" in out
    assert "comment_on=42" in out


def test_empty_registry_prints_placeholder(cli_env, cli_db, capsys):
    _sent()

    assert "No sent messages" in _row(capsys)
