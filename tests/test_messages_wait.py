"""Tests for `messages wait`: the deletion verdict over the local store (step 2)."""

from __future__ import annotations

import asyncio

import pytest
import typer


def _seed(db, phone: str, chat_id: int | None, message_id: int):
    asyncio.run(db.repos.observed_deletions.record_many(phone, chat_id, [message_id]))


def _wait(message_id: int, *, phone: str | None = None, chat: int | None = None, timeout: int = 0):
    from tg_agent.cli.commands.messages import messages_wait_impl

    asyncio.run(
        messages_wait_impl(
            "config.yaml",
            message_id=message_id,
            phone=phone,
            chat=chat,
            timeout=timeout,
        )
    )


def test_a_recorded_deletion_prints_the_verdict_and_exits_zero(cli_env, cli_db, capsys):
    _seed(cli_db, "+7001", None, 41)

    _wait(41, phone="+7001")

    out = capsys.readouterr().out
    assert "Deleted: message_id=41" in out
    assert "via=+7001" in out


def test_an_unrecorded_message_stays_alive_and_exits_one(cli_env, cli_db, capsys):
    with pytest.raises(typer.Exit) as excinfo:
        _wait(41, phone="+7001")
    assert excinfo.value.exit_code == 1
    # Exit 1 reports on stderr, per the #1486 exit-code contract.
    assert "Alive: no deletion recorded for message_id=41" in capsys.readouterr().err


def test_a_chat_filter_matches_that_chat(cli_env, cli_db, capsys):
    _seed(cli_db, "+7001", -1001525693788, 5)

    # The chat id arrives in the form Telegram reports (marked -100…).
    _wait(5, chat=-1001525693788)
    assert "Deleted: message_id=5" in capsys.readouterr().out


def test_a_wrong_chat_never_matches(cli_env, cli_db, capsys):
    _seed(cli_db, "+7001", -1001525693788, 5)

    with pytest.raises(typer.Exit) as excinfo:
        _wait(5, chat=-1009999999999)
    assert excinfo.value.exit_code == 1


def test_a_phone_without_a_chat_demands_the_dm_shape(cli_env, cli_db, capsys):
    # Only a channel row exists; a DM-shaped lookup (phone, no --chat) must not see it.
    _seed(cli_db, "+7001", -1001525693788, 5)

    with pytest.raises(typer.Exit) as excinfo:
        _wait(5, phone="+7001")
    assert excinfo.value.exit_code == 1


def test_without_phone_or_chat_any_row_matches(cli_env, cli_db, capsys):
    _seed(cli_db, "+7002", -1001525693788, 7)

    _wait(7)
    out = capsys.readouterr().out
    assert "Deleted: message_id=7" in out
    assert "via=+7002" in out
