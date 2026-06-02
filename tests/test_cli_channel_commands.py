"""Tests for src/cli/commands/channel.py — CLI channel subcommands."""

from __future__ import annotations

import argparse
import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.cli.commands.channel import _handle_tag, run
from tests.helpers import cli_add_channel as _add_channel
from tests.helpers import cli_ns as _ns
from tests.helpers import fake_asyncio_run, make_cli_config, make_cli_db

# ---------------------------------------------------------------------------
# _handle_tag (pure logic + db mock)
# ---------------------------------------------------------------------------


class TestHandleTag:
    @pytest.mark.anyio
    async def test_no_tag_action_prints_usage(self, capsys):
        args = argparse.Namespace(tag_action=None)
        db = MagicMock()
        await _handle_tag(args, db)
        assert "Usage: channel tag" in capsys.readouterr().out

    @pytest.mark.anyio
    async def test_list_tags_empty(self, capsys):
        args = argparse.Namespace(tag_action="list")
        db = MagicMock()
        db.repos.channels.list_all_tags = AsyncMock(return_value=[])
        await _handle_tag(args, db)
        assert "No tags found." in capsys.readouterr().out

    @pytest.mark.anyio
    async def test_list_tags(self, capsys):
        args = argparse.Namespace(tag_action="list")
        db = MagicMock()
        db.repos.channels.list_all_tags = AsyncMock(return_value=["news", "tech"])
        await _handle_tag(args, db)
        out = capsys.readouterr().out
        assert "news" in out
        assert "tech" in out

    @pytest.mark.anyio
    async def test_add_tag(self, capsys):
        args = argparse.Namespace(tag_action="add", name="sports")
        db = MagicMock()
        db.repos.channels.create_tag = AsyncMock()
        await _handle_tag(args, db)
        assert "Tag 'sports' created." in capsys.readouterr().out
        db.repos.channels.create_tag.assert_awaited_once_with("sports")

    @pytest.mark.anyio
    async def test_delete_tag(self, capsys):
        args = argparse.Namespace(tag_action="delete", name="old")
        db = MagicMock()
        db.repos.channels.delete_tag = AsyncMock()
        await _handle_tag(args, db)
        assert "Tag 'old' deleted." in capsys.readouterr().out
        db.repos.channels.delete_tag.assert_awaited_once_with("old")

    @pytest.mark.anyio
    async def test_set_channel_tags(self, capsys):
        args = argparse.Namespace(tag_action="set", pk=5, tags="a, b, c")
        db = MagicMock()
        db.repos.channels.set_channel_tags = AsyncMock()
        await _handle_tag(args, db)
        out = capsys.readouterr().out
        assert "pk=5" in out
        db.repos.channels.set_channel_tags.assert_awaited_once_with(5, ["a", "b", "c"])

    @pytest.mark.anyio
    async def test_get_channel_tags_empty(self, capsys):
        args = argparse.Namespace(tag_action="get", pk=10)
        db = MagicMock()
        db.repos.channels.get_channel_tags = AsyncMock(return_value=[])
        await _handle_tag(args, db)
        assert "No tags for channel pk=10" in capsys.readouterr().out

    @pytest.mark.anyio
    async def test_get_channel_tags(self, capsys):
        args = argparse.Namespace(tag_action="get", pk=10)
        db = MagicMock()
        db.repos.channels.get_channel_tags = AsyncMock(return_value=["x", "y"])
        await _handle_tag(args, db)
        out = capsys.readouterr().out
        assert "x, y" in out
        assert "pk=10" in out


# ---------------------------------------------------------------------------
# channel list — each test calls run() exactly once
# ---------------------------------------------------------------------------


class TestChannelList:
    def test_empty(self, cli_env, capsys):

        run(_ns(channel_action="list"))
        assert "No channels found." in capsys.readouterr().out

    def test_with_channels(self, cli_env, capsys):

        _add_channel(cli_env, channel_id=100, title="TestCh")
        run(_ns(channel_action="list"))
        out = capsys.readouterr().out
        assert "100" in out
        assert "TestCh" in out


# ---------------------------------------------------------------------------
# channel delete — each test calls run() exactly once
# ---------------------------------------------------------------------------


class TestChannelDelete:
    def test_delete_not_found(self, cli_env, capsys):

        run(_ns(channel_action="delete", identifier="99999"))
        assert "not found" in capsys.readouterr().out

    def test_delete_by_pk(self, cli_env, capsys):

        pk = _add_channel(cli_env, channel_id=200, title="DelMe")
        run(_ns(channel_action="delete", identifier=str(pk)))
        out = capsys.readouterr().out
        assert "Deleted channel" in out
        assert "DelMe" in out

    def test_delete_by_channel_id(self, cli_env, capsys):

        _add_channel(cli_env, channel_id=300, title="DelByID")
        run(_ns(channel_action="delete", identifier="300"))
        out = capsys.readouterr().out
        assert "Deleted channel" in out


# ---------------------------------------------------------------------------
# channel toggle — each test calls run() exactly once
# ---------------------------------------------------------------------------


class TestChannelToggle:
    def test_toggle_not_found(self, cli_env, capsys):

        run(_ns(channel_action="toggle", identifier="99999"))
        assert "not found" in capsys.readouterr().out

    def test_toggle_deactivates(self, cli_env, capsys):

        pk = _add_channel(cli_env, channel_id=500, title="ToggleMe")
        run(_ns(channel_action="toggle", identifier=str(pk)))
        out = capsys.readouterr().out
        assert "active=False" in out


# ---------------------------------------------------------------------------
# channel tag (via run) — each test calls run() exactly once
# ---------------------------------------------------------------------------


class TestChannelTagViaRun:
    def test_tag_list_empty(self, cli_env, capsys):

        run(_ns(channel_action="tag", tag_action="list"))
        assert "No tags found." in capsys.readouterr().out

    def test_tag_add(self, cli_env, capsys):

        run(_ns(channel_action="tag", tag_action="add", name="testtag"))
        assert "Tag 'testtag' created." in capsys.readouterr().out

    def test_tag_delete(self, cli_env, capsys):

        # Can't chain run() calls since each closes the DB.
        # Just test that delete doesn't crash with a non-existent tag.
        run(_ns(channel_action="tag", tag_action="delete", name="nonexistent"))
        assert "deleted" in capsys.readouterr().out


# ---------------------------------------------------------------------------
# channel import — identifier parsing (no Telegram pool needed)
# ---------------------------------------------------------------------------


class TestChannelImportNoIdentifiers:
    def test_import_empty_source(self, cli_env, capsys):
        """Import with no identifiers prints 'No identifiers found'."""

        run(_ns(channel_action="import", source=""))
        assert "No identifiers found" in capsys.readouterr().out


# ---------------------------------------------------------------------------
# channel stats — no accounts
# ---------------------------------------------------------------------------


class TestChannelStatsNoAccounts:
    def test_stats_all_no_accounts(self, cli_env, capsys):

        run(_ns(channel_action="stats", all=True))
        # The command will try to init_pool, which fails gracefully
        # Just ensure no crash
        _ = capsys.readouterr().out


# ---------------------------------------------------------------------------
# channel list-for-import — mirrors web GET /channels/dialogs (mocked service)
# ---------------------------------------------------------------------------


class TestChannelListForImport:
    _DIALOGS = [
        {
            "channel_id": 111,
            "title": "Added Ch",
            "username": "added",
            "channel_type": "channel",
            "already_added": True,
        },
        {
            "channel_id": 222,
            "title": "New Ch",
            "username": None,
            "channel_type": "supergroup",
            "already_added": False,
        },
    ]

    def _run(self, args, dialogs=None):
        db = make_cli_db()
        pool = MagicMock()
        pool.disconnect_all = AsyncMock()
        config = make_cli_config()
        svc = MagicMock()
        svc.get_dialogs_with_added_flags = AsyncMock(
            return_value=self._DIALOGS if dialogs is None else dialogs
        )
        with patch(
            "src.cli.commands.channel.runtime.init_db",
            AsyncMock(return_value=(config, db)),
        ), patch(
            "src.cli.commands.channel.runtime.init_pool",
            AsyncMock(return_value=(MagicMock(), pool)),
        ), patch(
            "src.cli.commands.channel.ChannelService", return_value=svc
        ), patch("asyncio.run", fake_asyncio_run):
            run(args)
        return svc

    def test_table_output(self, capsys):
        svc = self._run(_ns(channel_action="list-for-import", json=False))
        svc.get_dialogs_with_added_flags.assert_awaited_once_with()
        out = capsys.readouterr().out
        assert "Added Ch" in out
        assert "New Ch" in out
        assert "111" in out
        assert "222" in out
        # already_added column rendered as Yes/No
        assert "Yes" in out
        assert "No" in out

    def test_json_output(self, capsys):
        self._run(_ns(channel_action="list-for-import", json=True))
        payload = json.loads(capsys.readouterr().out.strip())
        assert {d["channel_id"] for d in payload} == {111, 222}

    def test_empty(self, capsys):
        self._run(_ns(channel_action="list-for-import", json=False), dialogs=[])
        assert "No dialogs found." in capsys.readouterr().out
