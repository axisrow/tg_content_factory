"""Tests for CLI dialogs commands."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.cli.commands.dialogs import run_with_dependencies
from src.config import AppConfig
from tests.helpers import cli_ns as _ns

pytestmark = pytest.mark.aiosqlite_serial


def _mock_pool():
    pool = MagicMock()
    pool.clients = {"+1234567890": MagicMock()}
    pool.disconnect_all = AsyncMock()
    pool.get_forum_topics = AsyncMock(return_value=[])
    pool.get_native_client_by_phone = AsyncMock(return_value=None)
    pool.get_dialogs_for_phone = AsyncMock(return_value=[
        {"channel_id": 100111, "title": "My Channel", "username": "mychan",
         "channel_type": "channel", "already_added": False},
    ])
    pool.leave_channels = AsyncMock(return_value={100111: True})
    return pool


def _run(args, pool, cli_db):
    """Run CLI command with mocked pool."""
    config = AppConfig()

    async def fake_init_db(_):
        return config, cli_db

    async def fake_init_pool(_, __):
        from src.telegram.auth import TelegramAuth
        return TelegramAuth(0, ""), pool

    with patch("src.cli.commands.dialogs.runtime.init_db", side_effect=fake_init_db), \
         patch("src.cli.commands.dialogs.runtime.init_pool", side_effect=fake_init_pool):
        run_with_dependencies(args)


def test_cli_dialogs_list(cli_db, capsys):
    """Test `dialogs list` command prints dialog table."""
    import asyncio

    pool = _mock_pool()

    # channel_service.get_my_dialogs() now reads from dialog_cache by default
    # (live pool calls only happen on --refresh, owned by the worker).
    async def _seed():
        await cli_db.repos.dialog_cache.replace_dialogs(
            "+1234567890",
            [
                {
                    "channel_id": 100111,
                    "title": "My Channel",
                    "username": "mychan",
                    "channel_type": "channel",
                    "is_dm": False,
                }
            ],
        )

    asyncio.run(_seed())

    _run(_ns(dialogs_action="list", phone="+1234567890"), pool, cli_db)
    out = capsys.readouterr().out
    assert "My Channel" in out
    assert "mychan" in out


def test_cli_dialogs_list_no_accounts(cli_db, capsys):
    """Test `dialogs list` with no connected accounts."""
    pool = _mock_pool()
    pool.clients = {}
    _run(_ns(dialogs_action="list", phone=None), pool, cli_db)
    out = capsys.readouterr().out
    assert "No connected accounts" in out


def test_cli_dialogs_list_phone_not_connected(cli_db, capsys):
    """Test `dialogs list` with phone that is not connected."""
    pool = _mock_pool()
    _run(_ns(dialogs_action="list", phone="+9999999999"), pool, cli_db)
    out = capsys.readouterr().out
    assert "not connected" in out


def test_cli_dialogs_refresh(cli_db, capsys):
    """Test `dialogs refresh` command."""
    pool = _mock_pool()
    _run(_ns(dialogs_action="refresh", phone="+1234567890"), pool, cli_db)
    out = capsys.readouterr().out
    assert "refreshed" in out.lower()


def test_cli_dialogs_refresh_no_accounts(cli_db, capsys):
    """Test `dialogs refresh` with no connected accounts."""
    pool = _mock_pool()
    pool.clients = {}
    _run(_ns(dialogs_action="refresh", phone=None), pool, cli_db)
    out = capsys.readouterr().out
    assert "No connected accounts" in out


def test_cli_dialogs_leave(cli_db, capsys):
    """Test `dialogs leave` with auto-confirm."""
    pool = _mock_pool()
    _run(
        _ns(dialogs_action="leave", phone="+1234567890",
            dialog_ids=["100111"], yes=True),
        pool, cli_db,
    )
    out = capsys.readouterr().out
    assert "left" in out


def test_cli_dialogs_topics(cli_db, capsys):
    """Test `dialogs topics` with no topics."""
    pool = _mock_pool()
    pool.get_forum_topics = AsyncMock(return_value=[])
    _run(_ns(dialogs_action="topics", channel_id=100111), pool, cli_db)
    out = capsys.readouterr().out
    assert "No forum topics" in out


def test_cli_dialogs_topics_with_data(cli_db, capsys):
    """Test `dialogs topics` returns topic list."""
    pool = _mock_pool()
    pool.get_forum_topics = AsyncMock(return_value=[
        {"id": 1, "title": "General", "icon_emoji_id": None, "date": "2025-01-01"},
    ])
    _run(_ns(dialogs_action="topics", channel_id=100111), pool, cli_db)
    out = capsys.readouterr().out
    assert "General" in out


def test_cli_dialogs_send_no_client(cli_db, capsys):
    """Test `dialogs send` when client unavailable."""
    pool = _mock_pool()
    pool.get_native_client_by_phone = AsyncMock(return_value=None)
    _run(
        _ns(dialogs_action="send", phone="+1234567890",
            recipient="100111", text="hello", yes=True),
        pool, cli_db,
    )
    out = capsys.readouterr().out
    assert "unavailable" in out.lower()
