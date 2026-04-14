"""Tests for src/cli/commands/notification.py — CLI notification subcommands."""
from __future__ import annotations

import argparse
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

from src.cli.commands.notification import run


def _fake_asyncio_run(coro):
    loop = asyncio.new_event_loop()
    try:
        return loop.run_until_complete(coro)
    finally:
        loop.close()


def _args(**overrides):
    defaults = {"config": "config.yaml"}
    defaults.update(overrides)
    return argparse.Namespace(**defaults)


def _make_db(**overrides):
    db = MagicMock()
    db.close = AsyncMock()
    db.get_notification_queries = AsyncMock(return_value=[])
    db.repos.settings.get_setting = AsyncMock(return_value=None)
    for k, v in overrides.items():
        setattr(db, k, v)
    return db


def _make_config():
    cfg = MagicMock()
    cfg.notifications.bot_name_prefix = "tgcf_"
    cfg.notifications.bot_username_prefix = "tgcf_"
    return cfg


def _make_pool():
    pool = MagicMock()
    pool.disconnect_all = AsyncMock()
    return pool


# ---------------------------------------------------------------------------
# setup
# ---------------------------------------------------------------------------


def test_setup(capsys):
    db = _make_db()
    pool = _make_pool()
    config = _make_config()
    mock_svc = MagicMock()
    mock_svc.setup_bot = AsyncMock(return_value=MagicMock(bot_username="test_bot", bot_token="123:abc"))
    mock_ts = MagicMock()
    with patch("src.cli.commands.notification.runtime.init_db", AsyncMock(return_value=(config, db))), \
         patch("src.cli.commands.notification.runtime.init_pool", AsyncMock(return_value=(MagicMock(), pool))), \
         patch("src.cli.commands.notification.NotificationService", return_value=mock_svc), \
         patch("src.cli.commands.notification.NotificationTargetService", return_value=mock_ts), \
         patch("asyncio.run", _fake_asyncio_run):
        run(_args(notification_action="setup"))
    out = capsys.readouterr().out
    assert "test_bot" in out


# ---------------------------------------------------------------------------
# status
# ---------------------------------------------------------------------------


def test_status_no_bot(capsys):
    db = _make_db()
    pool = _make_pool()
    config = _make_config()
    mock_svc = MagicMock()
    mock_svc.get_status = AsyncMock(return_value=None)
    mock_ts = MagicMock()
    with patch("src.cli.commands.notification.runtime.init_db", AsyncMock(return_value=(config, db))), \
         patch("src.cli.commands.notification.runtime.init_pool", AsyncMock(return_value=(MagicMock(), pool))), \
         patch("src.cli.commands.notification.NotificationService", return_value=mock_svc), \
         patch("src.cli.commands.notification.NotificationTargetService", return_value=mock_ts), \
         patch("asyncio.run", _fake_asyncio_run):
        run(_args(notification_action="status"))
    assert "No notification bot" in capsys.readouterr().out


def test_status_with_bot(capsys):
    db = _make_db()
    pool = _make_pool()
    config = _make_config()
    mock_svc = MagicMock()
    mock_svc.get_status = AsyncMock(return_value=MagicMock(bot_username="test_bot", bot_id=42, created_at="2024-01-01"))
    mock_ts = MagicMock()
    with patch("src.cli.commands.notification.runtime.init_db", AsyncMock(return_value=(config, db))), \
         patch("src.cli.commands.notification.runtime.init_pool", AsyncMock(return_value=(MagicMock(), pool))), \
         patch("src.cli.commands.notification.NotificationService", return_value=mock_svc), \
         patch("src.cli.commands.notification.NotificationTargetService", return_value=mock_ts), \
         patch("asyncio.run", _fake_asyncio_run):
        run(_args(notification_action="status"))
    assert "test_bot" in capsys.readouterr().out


# ---------------------------------------------------------------------------
# delete
# ---------------------------------------------------------------------------


def test_delete(capsys):
    db = _make_db()
    pool = _make_pool()
    config = _make_config()
    mock_svc = MagicMock()
    mock_svc.teardown_bot = AsyncMock()
    mock_ts = MagicMock()
    with patch("src.cli.commands.notification.runtime.init_db", AsyncMock(return_value=(config, db))), \
         patch("src.cli.commands.notification.runtime.init_pool", AsyncMock(return_value=(MagicMock(), pool))), \
         patch("src.cli.commands.notification.NotificationService", return_value=mock_svc), \
         patch("src.cli.commands.notification.NotificationTargetService", return_value=mock_ts), \
         patch("asyncio.run", _fake_asyncio_run):
        run(_args(notification_action="delete"))
    assert "deleted" in capsys.readouterr().out.lower()


# ---------------------------------------------------------------------------
# test
# ---------------------------------------------------------------------------


def test_test_notification(capsys):
    db = _make_db()
    pool = _make_pool()
    config = _make_config()
    mock_svc = MagicMock()
    mock_svc.send_notification = AsyncMock()
    mock_ts = MagicMock()
    with patch("src.cli.commands.notification.runtime.init_db", AsyncMock(return_value=(config, db))), \
         patch("src.cli.commands.notification.runtime.init_pool", AsyncMock(return_value=(MagicMock(), pool))), \
         patch("src.cli.commands.notification.NotificationService", return_value=mock_svc), \
         patch("src.cli.commands.notification.NotificationTargetService", return_value=mock_ts), \
         patch("asyncio.run", _fake_asyncio_run):
        run(_args(notification_action="test", message="Custom test"))
    mock_svc.send_notification.assert_called_with("Custom test")


# ---------------------------------------------------------------------------
# set-account
# ---------------------------------------------------------------------------


def test_set_account(capsys):
    db = _make_db()
    pool = _make_pool()
    config = _make_config()
    mock_svc = MagicMock()
    mock_ts = MagicMock()
    mock_ts.set_configured_phone = AsyncMock()
    with patch("src.cli.commands.notification.runtime.init_db", AsyncMock(return_value=(config, db))), \
         patch("src.cli.commands.notification.runtime.init_pool", AsyncMock(return_value=(MagicMock(), pool))), \
         patch("src.cli.commands.notification.NotificationService", return_value=mock_svc), \
         patch("src.cli.commands.notification.NotificationTargetService", return_value=mock_ts), \
         patch("asyncio.run", _fake_asyncio_run):
        run(_args(notification_action="set-account", phone="+1234567890"))
    assert "+1234567890" in capsys.readouterr().out


# ---------------------------------------------------------------------------
# dry-run
# ---------------------------------------------------------------------------


def test_dry_run_no_queries(capsys):
    db = _make_db()
    pool = _make_pool()
    config = _make_config()
    db.repos.tasks = MagicMock()
    db.repos.tasks.get_last_completed_collect_task = AsyncMock(return_value=None)
    mock_svc = MagicMock()
    mock_ts = MagicMock()
    with patch("src.cli.commands.notification.runtime.init_db", AsyncMock(return_value=(config, db))), \
         patch("src.cli.commands.notification.runtime.init_pool", AsyncMock(return_value=(MagicMock(), pool))), \
         patch("src.cli.commands.notification.NotificationService", return_value=mock_svc), \
         patch("src.cli.commands.notification.NotificationTargetService", return_value=mock_ts), \
         patch("asyncio.run", _fake_asyncio_run):
        run(_args(notification_action="dry-run"))
    assert "No active" in capsys.readouterr().out


def test_dry_run_with_queries(capsys):
    sq = MagicMock(id=1, name="TestQ", query="test")
    db = _make_db(
        get_notification_queries=AsyncMock(return_value=[sq]),
        search_messages_for_query_since=AsyncMock(return_value=([], 5)),
    )
    pool = _make_pool()
    config = _make_config()
    db.repos.tasks = MagicMock()
    db.repos.tasks.get_last_completed_collect_task = AsyncMock(return_value=None)
    db.repos.settings.get_setting = AsyncMock(return_value=None)
    mock_svc = MagicMock()
    mock_ts = MagicMock()
    with patch("src.cli.commands.notification.runtime.init_db", AsyncMock(return_value=(config, db))), \
         patch("src.cli.commands.notification.runtime.init_pool", AsyncMock(return_value=(MagicMock(), pool))), \
         patch("src.cli.commands.notification.NotificationService", return_value=mock_svc), \
         patch("src.cli.commands.notification.NotificationTargetService", return_value=mock_ts), \
         patch("asyncio.run", _fake_asyncio_run):
        run(_args(notification_action="dry-run"))
    out = capsys.readouterr().out
    assert "TestQ" in out or "test" in out
