"""Pending-approval channel workflow (#1701, #1700) — CLI visibility and honesty.

The approval queue (#1313) makes pending rows invisible to every standard
reader, which is correct for collection but left the manual workflow blind:

- ``channel list`` had no way to show the pending queue;
- ``channel add`` on an already-pending discovery row upserted metadata,
  kept ``approval_state='pending'`` and still printed "Added channel";
- ``channel collect`` on a pending channel said "not found" instead of
  pointing at ``channel toggle`` / ``channel reject``;
- ``channel toggle --help`` did not mention that it approves pending rows.

Each defect here is proven red on clean main first (skeptic protocol).
Fake/harness only: no live Telegram, tmp-dir DBs via ``cli_env``.
"""

from __future__ import annotations

import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from typer.testing import CliRunner

from tests.helpers import cli_add_channel, cli_ns
from tg_agent.cli.commands.channel import run
from tg_agent.config import AppConfig

pytestmark = pytest.mark.aiosqlite_serial


def _ensure_db_open(db):
    """Re-open the CLI database if the command closed it in its ``finally``."""
    if db._connection.db is None:
        asyncio.run(db.initialize())


def _fake_pool(resolve_result):
    """Pool double: one connected client, controllable resolve_channel."""
    pool = MagicMock()
    pool.clients = {"+70000000000": MagicMock()}
    pool.resolve_channel = AsyncMock(return_value=resolve_result)
    pool.fetch_channel_meta = AsyncMock(return_value=None)
    pool.disconnect_all = AsyncMock()
    return pool


def _auth_pool_pair(pool):
    async def fake_init_pool(config, db_arg, **_kwargs):
        return AppConfig(), pool

    return fake_init_pool


# ---------------------------------------------------------------------------
# channel list — pending visibility (#1701 item 1)
# ---------------------------------------------------------------------------


class TestChannelListPending:
    def test_pending_flag_shows_only_the_queue(self, cli_env, capsys):
        pk = cli_add_channel(cli_env, channel_id=100, title="PendMe", approval_state="pending")
        cli_add_channel(cli_env, channel_id=200, title="KnownCh")

        run(cli_ns(channel_action="list", pending=True))

        out = capsys.readouterr().out
        assert "PendMe" in out, f"pending channel invisible to `channel list --pending`: {out}"
        assert "KnownCh" not in out
        assert pk

    def test_all_flag_includes_pending_rows_marked(self, cli_env, capsys):
        cli_add_channel(cli_env, channel_id=100, title="PendMe", approval_state="pending")
        cli_add_channel(cli_env, channel_id=200, title="KnownCh")

        run(cli_ns(channel_action="list", all=True))

        out = capsys.readouterr().out
        assert "PendMe" in out, f"pending channel invisible to `channel list --all`: {out}"
        assert "KnownCh" in out
        assert "pending" in out

    def test_default_list_still_hides_pending(self, cli_env, capsys):
        """Regression guard: without flags the #1313 gate keeps pending invisible."""
        cli_add_channel(cli_env, channel_id=100, title="PendMe", approval_state="pending")
        cli_add_channel(cli_env, channel_id=200, title="KnownCh")

        run(cli_ns(channel_action="list"))

        out = capsys.readouterr().out
        assert "KnownCh" in out
        assert "PendMe" not in out


# ---------------------------------------------------------------------------
# channel add — no silent pending (#1701 item 2, #1700)
# ---------------------------------------------------------------------------


class TestChannelAddPending:
    def test_add_on_pending_channel_reports_and_approves(self, cli_env, capsys):
        """Re-adding a discovery-pending channel is an explicit operator decision:
        it must lift the approval gate (journaled, human origin) instead of
        silently keeping the row invisible."""
        pk = cli_add_channel(cli_env, channel_id=2629366839, title="PendMe", approval_state="pending")
        pool = _fake_pool(
            {"channel_id": 2629366839, "title": "PendMe", "username": None, "channel_type": "channel"}
        )

        with (
            patch("tg_agent.cli.commands.channel.runtime.init_pool", side_effect=_auth_pool_pair(pool)),
            patch("tg_agent.cli.commands.channel.worker_handoff.serve_is_running", return_value=False),
        ):
            run(cli_ns(channel_action="add", identifier="2629366839"))

        out = capsys.readouterr().out
        assert "pending approval" in out, f"add stayed silent about the pending row: {out}"

        _ensure_db_open(cli_env)
        row = asyncio.run(cli_env.get_channel_by_pk(pk))
        assert row.approval_state == "approved", "add left the channel pending"
        assert row.active_origin == "human", "approval by add must carry human origin"
        decisions = asyncio.run(
            cli_env.repos.decisions.history("channel", pk, field="approval_state")
        )
        assert decisions, "approval by add must journal the decision"
        assert decisions[0].old_value == "pending" and decisions[0].new_value == "approved"
        assert decisions[0].origin == "human"

    def test_add_new_channel_still_creates_approved(self, cli_env, capsys):
        """Guard: a fresh add keeps its pre-existing behavior (approved, 'Added')."""
        pool = _fake_pool(
            {"channel_id": 555, "title": "FreshCh", "username": None, "channel_type": "channel"}
        )

        with (
            patch("tg_agent.cli.commands.channel.runtime.init_pool", side_effect=_auth_pool_pair(pool)),
            patch("tg_agent.cli.commands.channel.worker_handoff.serve_is_running", return_value=False),
        ):
            run(cli_ns(channel_action="add", identifier="555"))

        out = capsys.readouterr().out
        assert "Added channel" in out
        _ensure_db_open(cli_env)
        row = asyncio.run(cli_env.get_channel_by_channel_id(555))
        assert row is not None
        assert row.approval_state == "approved"


# ---------------------------------------------------------------------------
# channel collect — pending guidance instead of "not found" (#1701 item 3)
# ---------------------------------------------------------------------------


class TestChannelCollectPending:
    def test_direct_collect_on_pending_says_pending(self, cli_env, capsys):
        pk = cli_add_channel(cli_env, channel_id=777, title="PendMe", approval_state="pending")
        pool = _fake_pool({})

        with (
            patch("tg_agent.cli.commands.channel.worker_handoff.serve_is_running", return_value=False),
            patch("tg_agent.cli.commands.channel.runtime.init_pool", side_effect=_auth_pool_pair(pool)),
            patch("tg_agent.cli.commands.channel.Collector") as collector_cls,
        ):
            collector_cls.return_value.collect_single_channel = AsyncMock(return_value=0)
            run(cli_ns(channel_action="collect", identifier="777", full=False, direct=False, wait=False))

        out = capsys.readouterr().out
        assert "pending approval" in out, f"collect misreported a pending channel: {out}"
        assert "channel toggle" in out, "must tell the operator how to approve"

        _ensure_db_open(cli_env)
        assert asyncio.run(cli_env.get_collection_tasks(limit=10)) == [], (
            "a pending channel must not be queued for collection"
        )

    def test_handoff_collect_on_pending_says_pending(self, cli_env, capsys):
        cli_add_channel(cli_env, channel_id=778, title="PendMe", approval_state="pending")

        with patch(
            "tg_agent.cli.commands.channel.worker_handoff.serve_is_running", return_value=True
        ):
            run(cli_ns(channel_action="collect", identifier="778", full=False, direct=False, wait=False))

        out = capsys.readouterr().out
        assert "pending approval" in out, f"handoff misreported a pending channel: {out}"
        assert "channel toggle" in out


# ---------------------------------------------------------------------------
# channel toggle — help documents the approval gesture (#1701 item 4)
# ---------------------------------------------------------------------------


def test_channel_toggle_help_documents_approval():
    from tg_agent.cli.typer_app import app

    result = CliRunner().invoke(app, ["channel", "toggle", "--help"])
    assert result.exit_code == 0, result.output
    assert "approval" in result.output.lower(), (
        "channel toggle --help must document that it approves pending channels"
    )
