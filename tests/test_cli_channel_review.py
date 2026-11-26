"""`channel review-confirm` / `review-keep` answer a flag that is there (#1979)."""
from __future__ import annotations

import asyncio
from datetime import datetime, timezone

import pytest
import typer

from tests.helpers import cli_add_channel
from tg_agent.cli.commands import channel


def _row(db, pk: int):
    """A command closes its db on the way out; reopen the fixture instance to look."""
    if db._connection.db is None:
        asyncio.run(db.initialize())
    return asyncio.run(db.get_channel_by_pk(pk))


def _flag_for_review(db, pk: int) -> None:
    asyncio.run(db.repos.channels.set_channel_review(pk, "numeric_unresolved"))


def test_review_confirm_leaves_a_channel_that_was_never_flagged_alone(cli_env, capsys):
    pk = cli_add_channel(cli_env, channel_id=300, title="Healthy")

    with pytest.raises(typer.Exit) as excinfo:
        asyncio.run(channel.review_confirm_impl("config.yaml", identifier="300"))

    assert excinfo.value.exit_code == 1
    captured = capsys.readouterr()
    assert "not under review" in captured.err
    assert captured.out == ""
    row = _row(cli_env, pk)
    assert row is not None and row.is_active is True
    assert row.channel_type != "unavailable"


def test_review_confirm_deactivates_a_channel_under_review(cli_env, capsys):
    pk = cli_add_channel(cli_env, channel_id=301, title="Quarantined")
    _flag_for_review(cli_env, pk)

    asyncio.run(channel.review_confirm_impl("config.yaml", identifier="301"))

    row = _row(cli_env, pk)
    assert row is not None and row.is_active is False
    assert row.channel_type == "unavailable"
    assert row.needs_review is False
    assert "DEACTIVATED" in capsys.readouterr().out


def test_review_confirm_deactivates_a_terminal_stamped_row(cli_env, capsys):
    """#1783: the documented way to deactivate a dead or reoccupied username."""
    pk = cli_add_channel(cli_env, channel_id=302, title="Dead")
    asyncio.run(
        cli_env.repos.channels.set_channel_terminal(
            pk, "UsernameChannelIdMismatch", datetime.now(timezone.utc)
        )
    )

    asyncio.run(channel.review_confirm_impl("config.yaml", identifier="302"))

    row = _row(cli_env, pk)
    assert row is not None and row.is_active is False


def test_review_keep_leaves_a_channel_that_was_never_flagged_alone(cli_env, capsys):
    pk = cli_add_channel(cli_env, channel_id=303, title="Healthy")

    with pytest.raises(typer.Exit) as excinfo:
        asyncio.run(channel.review_keep_impl("config.yaml", identifier="303"))

    assert excinfo.value.exit_code == 1
    captured = capsys.readouterr()
    assert "not under review" in captured.err
    assert captured.out == ""
    row = _row(cli_env, pk)
    assert row is not None and row.is_active is True


def test_review_keep_clears_the_flag_and_keeps_an_active_channel_active(cli_env, capsys):
    pk = cli_add_channel(cli_env, channel_id=304, title="Quarantined")
    _flag_for_review(cli_env, pk)

    asyncio.run(channel.review_keep_impl("config.yaml", identifier="304"))

    row = _row(cli_env, pk)
    assert row is not None and row.needs_review is False and row.is_active is True
    assert "KEPT ACTIVE" in capsys.readouterr().out


def test_review_keep_does_not_call_an_inactive_channel_active(cli_env, capsys):
    """The command does not touch `is_active`: an inactive channel stays inactive."""
    pk = cli_add_channel(cli_env, channel_id=305, title="Quarantined", is_active=False)
    _flag_for_review(cli_env, pk)

    asyncio.run(channel.review_keep_impl("config.yaml", identifier="305"))

    row = _row(cli_env, pk)
    assert row is not None and row.needs_review is False and row.is_active is False
    out = capsys.readouterr().out
    assert "KEPT ACTIVE" not in out
    assert "stays inactive" in out
