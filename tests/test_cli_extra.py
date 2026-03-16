"""Extra CLI tests for commands with low coverage."""
from __future__ import annotations

import argparse
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.config import AppConfig
from src.database import Database
from src.models import Channel, NotificationBot


@pytest.fixture
def cli_db(tmp_path):
    """Sync fixture: real SQLite for CLI tests."""
    db_path = str(tmp_path / "cli_test.db")
    database = Database(db_path)
    asyncio.run(database.initialize())
    yield database
    asyncio.run(database.close())


@pytest.fixture
def cli_env(cli_db):
    """Patch runtime.init_db to return real db without loading config.yaml."""
    config = AppConfig()

    async def fake_init_db(config_path: str):
        return config, cli_db

    with patch("src.cli.runtime.init_db", side_effect=fake_init_db):
        yield cli_db


@pytest.fixture
def cli_env_with_pool(cli_env):
    """Additionally patch runtime.init_pool to return a pool with clients."""
    fake_pool = AsyncMock()
    fake_pool.clients = {}
    fake_pool.disconnect_all = AsyncMock()

    async def fake_init_pool(config, db):
        from src.telegram.auth import TelegramAuth
        return TelegramAuth(0, ""), fake_pool

    with patch("src.cli.runtime.init_pool", side_effect=fake_init_pool):
        yield cli_env, fake_pool


def _ns(**kwargs) -> argparse.Namespace:
    """Build Namespace with defaults."""
    defaults = {"config": "config.yaml"}
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


# ---------------------------------------------------------------------------
# my_telegram command
# ---------------------------------------------------------------------------


class TestMyTelegramCommand:
    """Tests for my-telegram CLI command."""

    def test_list_no_accounts(self, cli_env_with_pool, capsys):
        """Test my-telegram list when no accounts connected."""
        cli_env, fake_pool = cli_env_with_pool
        fake_pool.clients = {}

        from src.cli.commands.my_telegram import run

        run(_ns(my_telegram_action="list", phone=None))
        out = capsys.readouterr().out
        assert "No connected accounts" in out

    def test_list_account_not_connected(self, cli_env_with_pool, capsys):
        """Test my-telegram list when specified account not connected."""
        cli_env, fake_pool = cli_env_with_pool
        fake_pool.clients = {"+70001112233": AsyncMock()}

        from src.cli.commands.my_telegram import run

        run(_ns(my_telegram_action="list", phone="+70009999999"))
        out = capsys.readouterr().out
        assert "not connected" in out

    def test_list_success(self, cli_env_with_pool, capsys):
        """Test my-telegram list with dialogs."""
        cli_env, fake_pool = cli_env_with_pool
        fake_pool.clients = {"+70001112233": AsyncMock()}

        from src.services.channel_service import ChannelService

        mock_dialogs = [
            {
                "channel_type": "channel",
                "title": "Test Channel",
                "username": "test_ch",
                "already_added": True,
            },
            {
                "channel_type": "group",
                "title": "Test Group",
                "username": None,
                "already_added": False,
            },
        ]

        with patch.object(
            ChannelService, "get_my_dialogs", new_callable=AsyncMock, return_value=mock_dialogs
        ):
            from src.cli.commands.my_telegram import run

            run(_ns(my_telegram_action="list", phone="+70001112233"))

        out = capsys.readouterr().out
        assert "Test Channel" in out
        assert "@test_ch" in out
        assert "Test Group" in out
        assert "Yes" in out

    def test_list_no_dialogs(self, cli_env_with_pool, capsys):
        """Test my-telegram list when no dialogs found."""
        cli_env, fake_pool = cli_env_with_pool
        fake_pool.clients = {"+70001112233": AsyncMock()}

        from src.services.channel_service import ChannelService

        with patch.object(
            ChannelService, "get_my_dialogs", new_callable=AsyncMock, return_value=[]
        ):
            from src.cli.commands.my_telegram import run

            run(_ns(my_telegram_action="list", phone="+70001112233"))

        out = capsys.readouterr().out
        assert "No dialogs found" in out

    def test_list_uses_first_account_by_default(self, cli_env_with_pool, capsys):
        """Test my-telegram list uses first available account when phone not specified."""
        cli_env, fake_pool = cli_env_with_pool
        fake_pool.clients = {
            "+70001112233": AsyncMock(),
            "+70002223344": AsyncMock(),
        }

        from src.services.channel_service import ChannelService

        with patch.object(
            ChannelService, "get_my_dialogs", new_callable=AsyncMock, return_value=[]
        ) as mock_get:
            from src.cli.commands.my_telegram import run

            run(_ns(my_telegram_action="list", phone=None))

        # Should use first account alphabetically
        mock_get.assert_awaited_once_with("+70001112233")

    def test_leave_no_accounts(self, cli_env_with_pool, capsys):
        """Test my-telegram leave when no accounts connected."""
        cli_env, fake_pool = cli_env_with_pool
        fake_pool.clients = {}

        from src.cli.commands.my_telegram import run

        run(_ns(my_telegram_action="leave", phone=None, dialog_ids=["-100123"], yes=True))
        out = capsys.readouterr().out
        assert "No connected accounts" in out

    def test_leave_account_not_connected(self, cli_env_with_pool, capsys):
        """Test my-telegram leave when specified account not connected."""
        cli_env, fake_pool = cli_env_with_pool
        fake_pool.clients = {"+10001112233": AsyncMock()}

        from src.cli.commands.my_telegram import run

        run(_ns(my_telegram_action="leave", phone="+10009999999", dialog_ids=["-100123"], yes=True))
        out = capsys.readouterr().out
        assert "not connected" in out

    def test_leave_invalid_ids(self, cli_env_with_pool, capsys):
        """Test my-telegram leave with non-numeric dialog IDs."""
        cli_env, fake_pool = cli_env_with_pool
        fake_pool.clients = {"+10001112233": AsyncMock()}

        from src.services.channel_service import ChannelService

        with patch.object(
            ChannelService, "get_my_dialogs", new_callable=AsyncMock, return_value=[]
        ):
            from src.cli.commands.my_telegram import run

            run(_ns(my_telegram_action="leave", phone="+10001112233",
                    dialog_ids=["notanid"], yes=True))

        out = capsys.readouterr().out
        assert "Invalid dialog ID" in out
        assert "No valid dialog IDs" in out

    def test_leave_success_with_yes_flag(self, cli_env_with_pool, capsys):
        """Test my-telegram leave succeeds when --yes skips confirmation."""
        cli_env, fake_pool = cli_env_with_pool
        fake_pool.clients = {"+10001112233": AsyncMock()}

        from src.services.channel_service import ChannelService

        dialogs_info = [
            {
                "channel_id": -100123456, "channel_type": "channel",
                "title": "Chan", "username": None,
            },
        ]

        with (
            patch.object(
                ChannelService, "get_my_dialogs", new_callable=AsyncMock, return_value=dialogs_info
            ),
            patch.object(
                ChannelService,
                "leave_dialogs",
                new_callable=AsyncMock,
                return_value={-100123456: True},
            ),
        ):
            from src.cli.commands.my_telegram import run

            run(_ns(my_telegram_action="leave", phone="+10001112233",
                    dialog_ids=["-100123456"], yes=True))

        out = capsys.readouterr().out
        assert "left" in out
        assert "Done:" in out

    def test_leave_comma_separated_ids(self, cli_env_with_pool, capsys):
        """Test my-telegram leave accepts comma-separated dialog IDs in a single arg."""
        cli_env, fake_pool = cli_env_with_pool
        fake_pool.clients = {"+10001112233": AsyncMock()}

        from src.services.channel_service import ChannelService

        with (
            patch.object(
                ChannelService, "get_my_dialogs", new_callable=AsyncMock, return_value=[]
            ),
            patch.object(
                ChannelService,
                "leave_dialogs",
                new_callable=AsyncMock,
                return_value={-100111: True, -100222: False},
            ) as mock_leave,
        ):
            from src.cli.commands.my_telegram import run

            run(_ns(my_telegram_action="leave", phone="+10001112233",
                    dialog_ids=["-100111,-100222"], yes=True))

        called_dialogs = mock_leave.call_args[0][1]
        assert (-100111, "channel") in called_dialogs
        assert (-100222, "channel") in called_dialogs

    def test_leave_aborted_on_no_confirmation(self, cli_env_with_pool, capsys):
        """Test my-telegram leave aborts when user answers 'n' to confirmation."""
        cli_env, fake_pool = cli_env_with_pool
        fake_pool.clients = {"+10001112233": AsyncMock()}

        from src.services.channel_service import ChannelService

        dialogs_info = [
            {
                "channel_id": -100123456, "channel_type": "channel",
                "title": "Chan", "username": None,
            },
        ]

        with (
            patch.object(
                ChannelService, "get_my_dialogs", new_callable=AsyncMock, return_value=dialogs_info
            ),
            patch("builtins.input", return_value="n"),
        ):
            from src.cli.commands.my_telegram import run

            run(_ns(my_telegram_action="leave", phone="+10001112233",
                    dialog_ids=["-100123456"], yes=False))

        out = capsys.readouterr().out
        assert "Aborted" in out

    def test_leave_confirms_on_yes(self, cli_env_with_pool, capsys):
        """Test my-telegram leave proceeds when user answers 'y' to confirmation."""
        cli_env, fake_pool = cli_env_with_pool
        fake_pool.clients = {"+10001112233": AsyncMock()}

        from src.services.channel_service import ChannelService

        dialogs_info = [
            {
                "channel_id": -100123456, "channel_type": "channel",
                "title": "Chan", "username": None,
            },
        ]

        with (
            patch.object(
                ChannelService, "get_my_dialogs", new_callable=AsyncMock, return_value=dialogs_info
            ),
            patch.object(
                ChannelService,
                "leave_dialogs",
                new_callable=AsyncMock,
                return_value={-100123456: True},
            ),
            patch("builtins.input", return_value="y"),
        ):
            from src.cli.commands.my_telegram import run

            run(_ns(my_telegram_action="leave", phone="+10001112233",
                    dialog_ids=["-100123456"], yes=False))

        out = capsys.readouterr().out
        assert "Done:" in out

    def test_leave_uses_first_account_by_default(self, cli_env_with_pool, capsys):
        """Test my-telegram leave uses first available account when phone not specified."""
        cli_env, fake_pool = cli_env_with_pool
        fake_pool.clients = {
            "+10001112233": AsyncMock(),
            "+10002223344": AsyncMock(),
        }

        from src.services.channel_service import ChannelService

        with (
            patch.object(
                ChannelService, "get_my_dialogs", new_callable=AsyncMock, return_value=[]
            ),
            patch.object(
                ChannelService,
                "leave_dialogs",
                new_callable=AsyncMock,
                return_value={-100123456: True},
            ) as mock_leave,
        ):
            from src.cli.commands.my_telegram import run

            run(_ns(my_telegram_action="leave", phone=None, dialog_ids=["-100123456"], yes=True))

        # First account alphabetically should be used
        mock_leave.assert_awaited_once()
        called_phone = mock_leave.call_args[0][0]
        assert called_phone == "+10001112233"

    def test_leave_dm_uses_dm_type(self, cli_env_with_pool, capsys):
        """Test my-telegram leave infers 'dm' type for positive IDs not in cache."""
        cli_env, fake_pool = cli_env_with_pool
        fake_pool.clients = {"+10001112233": AsyncMock()}

        from src.services.channel_service import ChannelService

        with (
            patch.object(
                ChannelService, "get_my_dialogs", new_callable=AsyncMock, return_value=[]
            ),
            patch.object(
                ChannelService,
                "leave_dialogs",
                new_callable=AsyncMock,
                return_value={123456: True},
            ) as mock_leave,
        ):
            from src.cli.commands.my_telegram import run

            run(_ns(my_telegram_action="leave", phone="+10001112233",
                    dialog_ids=["123456"], yes=True))

        called_dialogs = mock_leave.call_args[0][1]
        assert (123456, "dm") in called_dialogs


# ---------------------------------------------------------------------------
# notification command
# ---------------------------------------------------------------------------


class TestNotificationCommand:
    """Tests for notification CLI command."""

    def test_setup_success(self, cli_env_with_pool, capsys):
        """Test notification setup creates a bot."""
        cli_env, fake_pool = cli_env_with_pool
        fake_pool.clients = {"+70001112233": AsyncMock()}

        from src.models import NotificationBot
        from src.services.notification_service import NotificationService

        mock_bot = NotificationBot(
            tg_user_id=123,
            tg_username="test_user",
            bot_id=456,
            bot_username="testbot_bot",
            bot_token="123456:ABC-DEF",
        )

        with patch.object(
            NotificationService, "setup_bot", new_callable=AsyncMock, return_value=mock_bot
        ):
            from src.cli.commands.notification import run

            run(_ns(notification_action="setup"))

        out = capsys.readouterr().out
        assert "@testbot_bot" in out
        assert "123456:ABC-DEF" in out

    def test_status_no_bot(self, cli_env_with_pool, capsys):
        """Test notification status when no bot configured."""
        cli_env, fake_pool = cli_env_with_pool
        fake_pool.clients = {"+70001112233": AsyncMock()}

        from src.services.notification_service import NotificationService

        with patch.object(
            NotificationService, "get_status", new_callable=AsyncMock, return_value=None
        ):
            from src.cli.commands.notification import run

            run(_ns(notification_action="status"))

        out = capsys.readouterr().out
        assert "No notification bot" in out

    def test_status_with_bot(self, cli_env_with_pool, capsys):
        """Test notification status with configured bot."""
        cli_env, fake_pool = cli_env_with_pool
        fake_pool.clients = {"+70001112233": AsyncMock()}

        from src.services.notification_service import NotificationService

        mock_bot = NotificationBot(
            tg_user_id=123,
            tg_username="test_user",
            bot_id=456,
            bot_username="testbot_bot",
            bot_token="123456:ABC-DEF",
        )

        with patch.object(
            NotificationService, "get_status", new_callable=AsyncMock, return_value=mock_bot
        ):
            from src.cli.commands.notification import run

            run(_ns(notification_action="status"))

        out = capsys.readouterr().out
        assert "@testbot_bot" in out
        assert "456" in out

    def test_delete_success(self, cli_env_with_pool, capsys):
        """Test notification delete removes the bot."""
        cli_env, fake_pool = cli_env_with_pool
        fake_pool.clients = {"+70001112233": AsyncMock()}

        from src.services.notification_service import NotificationService

        with patch.object(
            NotificationService, "teardown_bot", new_callable=AsyncMock
        ):
            from src.cli.commands.notification import run

            run(_ns(notification_action="delete"))

        out = capsys.readouterr().out
        assert "deleted" in out.lower()


# ---------------------------------------------------------------------------
# scheduler command
# ---------------------------------------------------------------------------


class TestSchedulerCommand:
    """Tests for scheduler CLI command."""

    def test_start_no_clients(self, cli_env_with_pool, capsys, caplog):
        """Test scheduler start when no accounts connected."""
        cli_env, fake_pool = cli_env_with_pool
        fake_pool.clients = {}

        from src.cli.commands.scheduler import run

        run(_ns(scheduler_action="start"))
        assert "No connected accounts" in caplog.text

    def test_trigger_no_clients(self, cli_env_with_pool, capsys, caplog):
        """Test scheduler trigger when no accounts connected."""
        cli_env, fake_pool = cli_env_with_pool
        fake_pool.clients = {}

        from src.cli.commands.scheduler import run

        run(_ns(scheduler_action="trigger"))
        assert "No connected accounts" in caplog.text

    def test_trigger_success(self, cli_env_with_pool, capsys):
        """Test scheduler trigger enqueues channels."""
        cli_env, fake_pool = cli_env_with_pool
        fake_pool.clients = {"+70001112233": AsyncMock()}

        from src.cli.commands.scheduler import run

        run(_ns(scheduler_action="trigger"))

        out = capsys.readouterr().out
        assert "enqueued" in out.lower()



# ---------------------------------------------------------------------------
# serve command
# ---------------------------------------------------------------------------


class TestServeCommand:
    """Tests for serve CLI command."""

    def test_serve_exits_without_password(self, capsys):
        """Test serve exits when no password is configured."""
        from src.cli.commands.serve import run

        with patch("src.cli.commands.serve.load_config") as mock_load:
            config = AppConfig()
            config.web.password = ""
            mock_load.return_value = config

            with pytest.raises(SystemExit, match="1"):
                run(_ns(config="config.yaml", web_pass=None))

    def test_serve_uses_web_pass_arg(self):
        """Test serve uses web_pass from command line."""
        from src.cli.commands.serve import run

        with patch("src.cli.commands.serve.load_config") as mock_load, patch(
            "src.cli.commands.serve.create_app"
        ) as mock_create, patch(
            "src.cli.commands.serve.pid_file_path"
        ) as mock_pid_path, patch(
            "src.cli.commands.serve.register_current_process"
        ), patch("src.cli.commands.serve.unregister_current_process"), patch(
            "src.cli.commands.serve.uvicorn.run"
        ) as mock_uvicorn:

            config = AppConfig()
            config.web.password = ""  # Empty by default
            mock_load.return_value = config
            mock_create.return_value = MagicMock()
            mock_pid_path.return_value = "/tmp/test.pid"

            run(_ns(config="config.yaml", web_pass="cli_password"))

            # Password should be updated from CLI arg
            assert config.web.password == "cli_password"
            mock_uvicorn.assert_called_once()

    def test_serve_registers_and_unregisters_pid(self):
        """Test serve registers and unregisters PID file."""
        from src.cli.commands.serve import run

        with patch("src.cli.commands.serve.load_config") as mock_load, patch(
            "src.cli.commands.serve.create_app"
        ) as mock_create, patch(
            "src.cli.commands.serve.pid_file_path"
        ) as mock_pid_path, patch(
            "src.cli.commands.serve.register_current_process"
        ) as mock_register, patch(
            "src.cli.commands.serve.unregister_current_process"
        ) as mock_unregister, patch("src.cli.commands.serve.uvicorn.run"):

            config = AppConfig()
            config.web.password = "testpass"
            mock_load.return_value = config
            mock_create.return_value = MagicMock()
            mock_pid_path.return_value = "/tmp/test.pid"

            run(_ns(config="config.yaml", web_pass=None))

            mock_register.assert_called_once_with("/tmp/test.pid")
            mock_unregister.assert_called_once_with("/tmp/test.pid")

    def test_serve_exits_on_pid_registration_error(self, caplog):
        """Test serve exits when PID registration fails."""
        from src.cli.commands.serve import run

        with patch("src.cli.commands.serve.load_config") as mock_load, patch(
            "src.cli.commands.serve.create_app"
        ), patch("src.cli.commands.serve.pid_file_path") as mock_pid_path, patch(
            "src.cli.commands.serve.register_current_process"
        ) as mock_register:

            config = AppConfig()
            config.web.password = "testpass"
            mock_load.return_value = config
            mock_pid_path.return_value = "/tmp/test.pid"
            mock_register.side_effect = RuntimeError("PID file exists")

            with pytest.raises(SystemExit, match="1"):
                run(_ns(config="config.yaml", web_pass=None))


# ---------------------------------------------------------------------------
# collect command
# ---------------------------------------------------------------------------


class TestCollectCommand:
    """Tests for collect CLI command."""

    def test_collect_no_clients(self, cli_env_with_pool, caplog):
        """Test collect when no accounts connected."""
        cli_env, fake_pool = cli_env_with_pool
        fake_pool.clients = {}

        from src.cli.commands.collect import run

        run(_ns(channel_id=None, full=False))
        assert "No connected accounts" in caplog.text

    def test_collect_success(self, cli_env_with_pool, capsys):
        """Test collect enqueues channels."""
        cli_env, fake_pool = cli_env_with_pool
        fake_pool.clients = {"+70001112233": AsyncMock()}

        from src.cli.commands.collect import run

        run(_ns(channel_id=None, full=False))

        out = capsys.readouterr().out
        assert "enqueued" in out.lower()

    def test_collect_single_channel(self, cli_env_with_pool, capsys):
        """Test collect for a single channel."""
        cli_env, fake_pool = cli_env_with_pool
        fake_pool.clients = {"+70001112233": AsyncMock()}

        # Add a channel to collect
        _add_channel(cli_env, channel_id=100, title="Test Channel")

        from src.telegram.collector import Collector

        mock_stats = {"collected": 10, "errors": 0}

        with patch.object(
            Collector, "collect_single_channel", new_callable=AsyncMock, return_value=mock_stats
        ):
            from src.cli.commands.collect import run

            run(_ns(channel_id=100, full=False))

        out = capsys.readouterr().out
        assert "Collected" in out and "100" in out

    def test_collect_full_mode(self, cli_env_with_pool, capsys):
        """Test collect with full flag for single channel."""
        cli_env, fake_pool = cli_env_with_pool
        fake_pool.clients = {"+70001112233": AsyncMock()}

        # Add a channel
        _add_channel(cli_env, channel_id=100, title="Test Channel")

        from src.telegram.collector import Collector

        mock_count = 100

        with patch.object(
            Collector, "collect_single_channel", new_callable=AsyncMock, return_value=mock_count
        ) as mock_collect:
            from src.cli.commands.collect import run

            # Pass channel_id as int since argparse may convert it
            run(_ns(channel_id=100, full=True))

            # collect_single_channel should be called with full=True
            mock_collect.assert_awaited_once()
            # Check the kwargs - full is a keyword-only argument
            call_kwargs = mock_collect.call_args.kwargs
            assert call_kwargs.get("full") is True

        out = capsys.readouterr().out
        assert "100" in out or "complete" in out.lower()


def _add_channel(db: Database, channel_id: int = 100, title: str = "TestCh") -> int:
    return asyncio.run(db.add_channel(Channel(channel_id=channel_id, title=title)))
