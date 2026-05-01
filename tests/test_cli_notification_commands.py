"""Tests for src/cli/commands/notification.py — CLI notification subcommands."""
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

from src.cli.commands.notification import run
from tests.helpers import cli_ns, fake_asyncio_run, make_cli_db


def _args(**overrides):
    defaults = {"config": "config.yaml"}
    defaults.update(overrides)
    return cli_ns(**defaults)


def make_notification_config():
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
    db = make_cli_db()
    pool = _make_pool()
    config = make_notification_config()
    mock_svc = MagicMock()
    mock_svc.setup_bot = AsyncMock(return_value=MagicMock(bot_username="test_bot", bot_token="123:abc"))
    mock_ts = MagicMock()
    with patch("src.cli.commands.notification.runtime.init_db", AsyncMock(return_value=(config, db))), \
         patch("src.cli.commands.notification.runtime.init_pool", AsyncMock(return_value=(MagicMock(), pool))), \
         patch("src.cli.commands.notification.NotificationService", return_value=mock_svc), \
         patch("src.cli.commands.notification.NotificationTargetService", return_value=mock_ts), \
         patch("asyncio.run", fake_asyncio_run):
        run(_args(notification_action="setup"))
    out = capsys.readouterr().out
    assert "test_bot" in out


# ---------------------------------------------------------------------------
# status
# ---------------------------------------------------------------------------


def test_status_no_bot(capsys):
    db = make_cli_db()
    pool = _make_pool()
    config = make_notification_config()
    mock_svc = MagicMock()
    mock_svc.get_status = AsyncMock(return_value=None)
    mock_ts = MagicMock()
    with patch("src.cli.commands.notification.runtime.init_db", AsyncMock(return_value=(config, db))), \
         patch("src.cli.commands.notification.runtime.init_pool", AsyncMock(return_value=(MagicMock(), pool))), \
         patch("src.cli.commands.notification.NotificationService", return_value=mock_svc), \
         patch("src.cli.commands.notification.NotificationTargetService", return_value=mock_ts), \
         patch("asyncio.run", fake_asyncio_run):
        run(_args(notification_action="status"))
    assert "No notification bot" in capsys.readouterr().out


def test_status_with_bot(capsys):
    db = make_cli_db()
    pool = _make_pool()
    config = make_notification_config()
    mock_svc = MagicMock()
    mock_svc.get_status = AsyncMock(return_value=MagicMock(bot_username="test_bot", bot_id=42, created_at="2024-01-01"))
    mock_ts = MagicMock()
    with patch("src.cli.commands.notification.runtime.init_db", AsyncMock(return_value=(config, db))), \
         patch("src.cli.commands.notification.runtime.init_pool", AsyncMock(return_value=(MagicMock(), pool))), \
         patch("src.cli.commands.notification.NotificationService", return_value=mock_svc), \
         patch("src.cli.commands.notification.NotificationTargetService", return_value=mock_ts), \
         patch("asyncio.run", fake_asyncio_run):
        run(_args(notification_action="status"))
    assert "test_bot" in capsys.readouterr().out


def test_status_target_unavailable_prints_diagnostic(capsys):
    db = make_cli_db()
    pool = _make_pool()
    config = make_notification_config()
    target_status = SimpleNamespace(
        mode="primary",
        state="disconnected",
        message="Аккаунт +123 не подключён.",
        configured_phone=None,
        effective_phone="+123",
    )
    mock_svc = MagicMock()
    mock_svc.get_status = AsyncMock(side_effect=AssertionError("get_status should not be called"))
    mock_ts = MagicMock()
    mock_ts.describe_target = AsyncMock(return_value=target_status)
    with patch("src.cli.commands.notification.runtime.init_db", AsyncMock(return_value=(config, db))), \
         patch("src.cli.commands.notification.runtime.init_pool", AsyncMock(return_value=(MagicMock(), pool))), \
         patch("src.cli.commands.notification.NotificationService", return_value=mock_svc), \
         patch("src.cli.commands.notification.NotificationTargetService", return_value=mock_ts), \
         patch("asyncio.run", fake_asyncio_run):
        run(_args(notification_action="status"))

    out = capsys.readouterr().out
    assert "Notification target unavailable" in out
    assert "disconnected" in out
    assert "+123" in out
    assert "Аккаунт +123 не подключён." in out
    mock_svc.get_status.assert_not_awaited()


# ---------------------------------------------------------------------------
# delete
# ---------------------------------------------------------------------------


def test_delete(capsys):
    db = make_cli_db()
    pool = _make_pool()
    config = make_notification_config()
    mock_svc = MagicMock()
    mock_svc.teardown_bot = AsyncMock()
    mock_ts = MagicMock()
    with patch("src.cli.commands.notification.runtime.init_db", AsyncMock(return_value=(config, db))), \
         patch("src.cli.commands.notification.runtime.init_pool", AsyncMock(return_value=(MagicMock(), pool))), \
         patch("src.cli.commands.notification.NotificationService", return_value=mock_svc), \
         patch("src.cli.commands.notification.NotificationTargetService", return_value=mock_ts), \
         patch("asyncio.run", fake_asyncio_run):
        run(_args(notification_action="delete"))
    assert "deleted" in capsys.readouterr().out.lower()


# ---------------------------------------------------------------------------
# test
# ---------------------------------------------------------------------------


def test_test_notification(capsys):
    db = make_cli_db()
    pool = _make_pool()
    config = make_notification_config()
    mock_svc = MagicMock()
    mock_svc.send_notification = AsyncMock()
    mock_ts = MagicMock()
    with patch("src.cli.commands.notification.runtime.init_db", AsyncMock(return_value=(config, db))), \
         patch("src.cli.commands.notification.runtime.init_pool", AsyncMock(return_value=(MagicMock(), pool))), \
         patch("src.cli.commands.notification.NotificationService", return_value=mock_svc), \
         patch("src.cli.commands.notification.NotificationTargetService", return_value=mock_ts), \
         patch("asyncio.run", fake_asyncio_run):
        run(_args(notification_action="test", message="Custom test"))
    mock_svc.send_notification.assert_called_with("Custom test")


# ---------------------------------------------------------------------------
# set-account
# ---------------------------------------------------------------------------


def test_set_account(capsys):
    db = make_cli_db()
    pool = _make_pool()
    config = make_notification_config()
    mock_svc = MagicMock()
    mock_ts = MagicMock()
    mock_ts.set_configured_phone = AsyncMock()
    with patch("src.cli.commands.notification.runtime.init_db", AsyncMock(return_value=(config, db))), \
         patch("src.cli.commands.notification.runtime.init_pool", AsyncMock(return_value=(MagicMock(), pool))), \
         patch("src.cli.commands.notification.NotificationService", return_value=mock_svc), \
         patch("src.cli.commands.notification.NotificationTargetService", return_value=mock_ts), \
         patch("asyncio.run", fake_asyncio_run):
        run(_args(notification_action="set-account", phone="+1234567890"))
    assert "+1234567890" in capsys.readouterr().out


# ---------------------------------------------------------------------------
# dry-run
# ---------------------------------------------------------------------------


def test_dry_run_no_queries(capsys):
    db = make_cli_db()
    pool = _make_pool()
    config = make_notification_config()
    db.repos.tasks = MagicMock()
    db.repos.tasks.get_last_completed_collect_task = AsyncMock(return_value=None)
    mock_svc = MagicMock()
    mock_ts = MagicMock()
    with patch("src.cli.commands.notification.runtime.init_db", AsyncMock(return_value=(config, db))), \
         patch("src.cli.commands.notification.runtime.init_pool", AsyncMock(return_value=(MagicMock(), pool))), \
         patch("src.cli.commands.notification.NotificationService", return_value=mock_svc), \
         patch("src.cli.commands.notification.NotificationTargetService", return_value=mock_ts), \
         patch("asyncio.run", fake_asyncio_run):
        run(_args(notification_action="dry-run"))
    assert "No active" in capsys.readouterr().out


def test_dry_run_with_queries(capsys):
    sq = MagicMock(id=1, name="TestQ", query="test")
    db = make_cli_db(
        get_notification_queries=AsyncMock(return_value=[sq]),
        search_messages_for_query_since=AsyncMock(return_value=([], 5)),
    )
    pool = _make_pool()
    config = make_notification_config()
    db.repos.tasks = MagicMock()
    db.repos.tasks.get_last_completed_collect_task = AsyncMock(return_value=None)
    db.repos.settings.get_setting = AsyncMock(return_value=None)
    mock_svc = MagicMock()
    mock_ts = MagicMock()
    with patch("src.cli.commands.notification.runtime.init_db", AsyncMock(return_value=(config, db))), \
         patch("src.cli.commands.notification.runtime.init_pool", AsyncMock(return_value=(MagicMock(), pool))), \
         patch("src.cli.commands.notification.NotificationService", return_value=mock_svc), \
         patch("src.cli.commands.notification.NotificationTargetService", return_value=mock_ts), \
         patch("asyncio.run", fake_asyncio_run):
        run(_args(notification_action="dry-run"))
    out = capsys.readouterr().out
    assert "TestQ" in out or "test" in out
