"""Tests for src/cli/commands/channel.py — CLI channel subcommands."""

from __future__ import annotations

import argparse
from unittest.mock import AsyncMock, MagicMock

import pytest

from src.cli.commands.channel import _handle_tag, run
from tests.helpers import cli_add_channel as _add_channel
from tests.helpers import cli_ns as _ns

# ---------------------------------------------------------------------------
# _handle_tag (pure logic + db mock)
# ---------------------------------------------------------------------------


class TestHandleTag:
    @pytest.mark.asyncio
    async def test_no_tag_action_prints_usage(self, capsys):
        args = argparse.Namespace(tag_action=None)
        db = MagicMock()
        await _handle_tag(args, db)
        assert "Usage: channel tag" in capsys.readouterr().out

    @pytest.mark.asyncio
    async def test_list_tags_empty(self, capsys):
        args = argparse.Namespace(tag_action="list")
        db = MagicMock()
        db.repos.channels.list_all_tags = AsyncMock(return_value=[])
        await _handle_tag(args, db)
        assert "No tags found." in capsys.readouterr().out

    @pytest.mark.asyncio
    async def test_list_tags(self, capsys):
        args = argparse.Namespace(tag_action="list")
        db = MagicMock()
        db.repos.channels.list_all_tags = AsyncMock(return_value=["news", "tech"])
        await _handle_tag(args, db)
        out = capsys.readouterr().out
        assert "news" in out
        assert "tech" in out

    @pytest.mark.asyncio
    async def test_add_tag(self, capsys):
        args = argparse.Namespace(tag_action="add", name="sports")
        db = MagicMock()
        db.repos.channels.create_tag = AsyncMock()
        await _handle_tag(args, db)
        assert "Tag 'sports' created." in capsys.readouterr().out
        db.repos.channels.create_tag.assert_awaited_once_with("sports")

    @pytest.mark.asyncio
    async def test_delete_tag(self, capsys):
        args = argparse.Namespace(tag_action="delete", name="old")
        db = MagicMock()
        db.repos.channels.delete_tag = AsyncMock()
        await _handle_tag(args, db)
        assert "Tag 'old' deleted." in capsys.readouterr().out
        db.repos.channels.delete_tag.assert_awaited_once_with("old")

    @pytest.mark.asyncio
    async def test_set_channel_tags(self, capsys):
        args = argparse.Namespace(tag_action="set", pk=5, tags="a, b, c")
        db = MagicMock()
        db.repos.channels.set_channel_tags = AsyncMock()
        await _handle_tag(args, db)
        out = capsys.readouterr().out
        assert "pk=5" in out
        db.repos.channels.set_channel_tags.assert_awaited_once_with(5, ["a", "b", "c"])

    @pytest.mark.asyncio
    async def test_get_channel_tags_empty(self, capsys):
        args = argparse.Namespace(tag_action="get", pk=10)
        db = MagicMock()
        db.repos.channels.get_channel_tags = AsyncMock(return_value=[])
        await _handle_tag(args, db)
        assert "No tags for channel pk=10" in capsys.readouterr().out

    @pytest.mark.asyncio
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
