"""Tests for CLI dialogs, notification, scheduler, serve, and collect paths."""

from __future__ import annotations

import argparse
import asyncio
import json
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import typer

from tests.helpers import patch_serve_create_app
from tg_agent.config import AppConfig
from tg_agent.database import Database
from tg_agent.models import Account, Channel, CollectionTaskStatus, NotificationBot

pytestmark = pytest.mark.aiosqlite_serial


@pytest.fixture
def cli_env_with_pool(cli_env):
    """Additionally patch runtime.init_pool to return a pool with clients."""
    fake_pool = AsyncMock()
    fake_pool.clients = {}
    fake_pool.disconnect_all = AsyncMock()
    fake_pool.get_resolve_username_backoff_remaining_sec = MagicMock(return_value=0)

    async def fake_init_pool(config, db, **_kwargs):
        from tg_shared.auth import TelegramAuth

        return TelegramAuth(0, ""), fake_pool

    with patch("tg_agent.cli.runtime.init_pool", side_effect=fake_init_pool):
        yield cli_env, fake_pool


def _ns(**kwargs) -> argparse.Namespace:
    """Build Namespace with defaults."""
    defaults = {"config": "config.yaml"}
    defaults.update(kwargs)
    return argparse.Namespace(**defaults)


# ---------------------------------------------------------------------------
# dialogs command
# ---------------------------------------------------------------------------


class TestMyTelegramCommand:
    """Tests for dialogs CLI command."""

    def test_list_no_accounts(self, cli_env_with_pool, capsys):
        """Test dialogs list when no accounts connected."""
        cli_env, fake_pool = cli_env_with_pool
        fake_pool.clients = {}

        from tg_agent.cli.commands.dialogs import run

        with pytest.raises(typer.Exit):
            run(_ns(dialogs_action="list", phone=None))
        err = capsys.readouterr().err

        assert "No connected accounts" in err

    def test_list_account_not_connected(self, cli_env_with_pool, capsys):
        """Test dialogs list when specified account not connected."""
        cli_env, fake_pool = cli_env_with_pool
        fake_pool.clients = {"+70001112233": AsyncMock()}

        from tg_agent.cli.commands.dialogs import run

        with pytest.raises(typer.Exit):
            run(_ns(dialogs_action="list", phone="+70009999999"))
        err = capsys.readouterr().err

        assert "not connected" in err

    def test_list_success(self, cli_env_with_pool, capsys):
        """Test dialogs list with dialogs."""
        cli_env, fake_pool = cli_env_with_pool
        fake_pool.clients = {"+70001112233": AsyncMock()}

        from tg_agent.services.channel_service import ChannelService

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
            from tg_agent.cli.commands.dialogs import run

            run(_ns(dialogs_action="list", phone="+70001112233"))

        out = capsys.readouterr().out
        assert "Test Channel" in out
        assert "@test_ch" in out
        assert "Test Group" in out
        assert "Yes" in out

    def test_list_no_dialogs(self, cli_env_with_pool, capsys):
        """Test dialogs list when no dialogs found."""
        cli_env, fake_pool = cli_env_with_pool
        fake_pool.clients = {"+70001112233": AsyncMock()}

        from tg_agent.services.channel_service import ChannelService

        with patch.object(
            ChannelService, "get_my_dialogs", new_callable=AsyncMock, return_value=[]
        ):
            from tg_agent.cli.commands.dialogs import run

            run(_ns(dialogs_action="list", phone="+70001112233"))

        out = capsys.readouterr().out
        assert "No dialogs found" in out

    def test_list_uses_first_account_by_default(self, cli_env_with_pool, capsys):
        """Test dialogs list uses first available account when phone not specified."""
        cli_env, fake_pool = cli_env_with_pool
        fake_pool.clients = {
            "+70001112233": AsyncMock(),
            "+70002223344": AsyncMock(),
        }

        from tg_agent.services.channel_service import ChannelService

        with patch.object(
            ChannelService, "get_my_dialogs", new_callable=AsyncMock, return_value=[]
        ) as mock_get:
            from tg_agent.cli.commands.dialogs import run

            run(_ns(dialogs_action="list", phone=None))

        # Should use first account alphabetically
        mock_get.assert_awaited_once_with("+70001112233")

    def test_leave_no_accounts(self, cli_env_with_pool, capsys):
        """Test dialogs leave when no accounts connected."""
        cli_env, fake_pool = cli_env_with_pool
        fake_pool.clients = {}

        from tg_agent.cli.commands.dialogs import run

        with pytest.raises(typer.Exit):
            run(_ns(dialogs_action="leave", phone=None, dialog_ids=["-100123"], yes=True))
        err = capsys.readouterr().err

        assert "No connected accounts" in err

    def test_leave_account_not_connected(self, cli_env_with_pool, capsys):
        """Test dialogs leave when specified account not connected."""
        cli_env, fake_pool = cli_env_with_pool
        fake_pool.clients = {"+10001112233": AsyncMock()}

        from tg_agent.cli.commands.dialogs import run

        with pytest.raises(typer.Exit):
            run(_ns(dialogs_action="leave", phone="+10009999999", dialog_ids=["-100123"], yes=True))
        err = capsys.readouterr().err

        assert "not connected" in err

    def test_leave_invalid_ids(self, cli_env_with_pool, capsys):
        """Test dialogs leave with non-numeric dialog IDs."""
        cli_env, fake_pool = cli_env_with_pool
        fake_pool.clients = {"+10001112233": AsyncMock()}

        from tg_agent.services.channel_service import ChannelService

        with patch.object(
            ChannelService, "get_my_dialogs", new_callable=AsyncMock, return_value=[]
        ):
            from tg_agent.cli.commands.dialogs import run

            run(
                _ns(
                    dialogs_action="leave",
                    phone="+10001112233",
                    dialog_ids=["notanid"],
                    yes=True,
                )
            )

        out = capsys.readouterr().out
        assert "Invalid dialog ID" in out
        assert "No valid dialog IDs" in out

    def test_leave_success_with_yes_flag(self, cli_env_with_pool, capsys):
        """Test dialogs leave succeeds when --yes skips confirmation."""
        cli_env, fake_pool = cli_env_with_pool
        fake_pool.clients = {"+10001112233": AsyncMock()}

        from tg_agent.services.channel_service import ChannelService

        dialogs_info = [
            {
                "channel_id": -100123456,
                "channel_type": "channel",
                "title": "Chan",
                "username": None,
            },
        ]
        fake_pool.leave_channels = AsyncMock(return_value={-100123456: True})

        with (
            patch.object(
                ChannelService, "get_my_dialogs", new_callable=AsyncMock, return_value=dialogs_info
            ),
        ):
            from tg_agent.cli.commands.dialogs import run

            run(
                _ns(
                    dialogs_action="leave",
                    phone="+10001112233",
                    dialog_ids=["-100123456"],
                    yes=True,
                )
            )

        out = capsys.readouterr().out
        assert "left" in out
        assert "Done:" in out

    def test_leave_comma_separated_ids(self, cli_env_with_pool, capsys):
        """Test dialogs leave accepts comma-separated dialog IDs in a single arg."""
        cli_env, fake_pool = cli_env_with_pool
        fake_pool.clients = {"+10001112233": AsyncMock()}

        from tg_agent.services.channel_service import ChannelService
        fake_pool.leave_channels = AsyncMock(return_value={-100111: True, -100222: False})

        with (
            patch.object(ChannelService, "get_my_dialogs", new_callable=AsyncMock, return_value=[]),
        ):
            from tg_agent.cli.commands.dialogs import run

            run(
                _ns(
                    dialogs_action="leave",
                    phone="+10001112233",
                    dialog_ids=["-100111,-100222"],
                    yes=True,
                )
            )

        called_dialogs = fake_pool.leave_channels.call_args[0][1]
        assert (-100111, "channel") in called_dialogs
        assert (-100222, "channel") in called_dialogs

    def test_leave_aborted_on_no_confirmation(self, cli_env_with_pool, capsys):
        """Test dialogs leave aborts when user answers 'n' to confirmation."""
        cli_env, fake_pool = cli_env_with_pool
        fake_pool.clients = {"+10001112233": AsyncMock()}

        from tg_agent.services.channel_service import ChannelService

        dialogs_info = [
            {
                "channel_id": -100123456,
                "channel_type": "channel",
                "title": "Chan",
                "username": None,
            },
        ]

        with (
            patch.object(
                ChannelService, "get_my_dialogs", new_callable=AsyncMock, return_value=dialogs_info
            ),
            patch("builtins.input", return_value="n"),
        ):
            from tg_agent.cli.commands.dialogs import run

            run(
                _ns(
                    dialogs_action="leave",
                    phone="+10001112233",
                    dialog_ids=["-100123456"],
                    yes=False,
                )
            )

        out = capsys.readouterr().out
        assert "Aborted" in out

    def test_leave_confirms_on_yes(self, cli_env_with_pool, capsys):
        """Test dialogs leave proceeds when user answers 'y' to confirmation."""
        cli_env, fake_pool = cli_env_with_pool
        fake_pool.clients = {"+10001112233": AsyncMock()}

        from tg_agent.services.channel_service import ChannelService

        dialogs_info = [
            {
                "channel_id": -100123456,
                "channel_type": "channel",
                "title": "Chan",
                "username": None,
            },
        ]
        fake_pool.leave_channels = AsyncMock(return_value={-100123456: True})

        with (
            patch.object(
                ChannelService, "get_my_dialogs", new_callable=AsyncMock, return_value=dialogs_info
            ),
            patch("builtins.input", return_value="y"),
        ):
            from tg_agent.cli.commands.dialogs import run

            run(
                _ns(
                    dialogs_action="leave",
                    phone="+10001112233",
                    dialog_ids=["-100123456"],
                    yes=False,
                )
            )

        out = capsys.readouterr().out
        assert "Done:" in out

    def test_leave_uses_first_account_by_default(self, cli_env_with_pool, capsys):
        """Test dialogs leave uses first available account when phone not specified."""
        cli_env, fake_pool = cli_env_with_pool
        fake_pool.clients = {
            "+10001112233": AsyncMock(),
            "+10002223344": AsyncMock(),
        }

        from tg_agent.services.channel_service import ChannelService
        fake_pool.leave_channels = AsyncMock(return_value={-100123456: True})

        with (
            patch.object(ChannelService, "get_my_dialogs", new_callable=AsyncMock, return_value=[]),
        ):
            from tg_agent.cli.commands.dialogs import run

            run(_ns(dialogs_action="leave", phone=None, dialog_ids=["-100123456"], yes=True))

        # First account alphabetically should be used
        fake_pool.leave_channels.assert_awaited_once()
        called_phone = fake_pool.leave_channels.call_args[0][0]
        assert called_phone == "+10001112233"

    def test_leave_unknown_positive_id_uses_channel_type(self, cli_env_with_pool, capsys):
        """A positive ID not in the dialog cache must fall back to 'channel' -> PeerChannel,
        NOT a guessed 'dm' -> PeerUser. Channel ids here are bare-positive, so guessing 'dm'
        for an unknown positive id would target the wrong peer (#842 review)."""
        cli_env, fake_pool = cli_env_with_pool
        fake_pool.clients = {"+10001112233": AsyncMock()}

        from tg_agent.services.channel_service import ChannelService
        fake_pool.leave_channels = AsyncMock(return_value={123456: True})

        with (
            patch.object(ChannelService, "get_my_dialogs", new_callable=AsyncMock, return_value=[]),
        ):
            from tg_agent.cli.commands.dialogs import run

            run(
                _ns(
                    dialogs_action="leave",
                    phone="+10001112233",
                    dialog_ids=["123456"],
                    yes=True,
                )
            )

        called_dialogs = fake_pool.leave_channels.call_args[0][1]
        assert (123456, "channel") in called_dialogs

    def test_cache_clear_all(self, cli_env_with_pool, capsys):
        """Test cache-clear without phone clears all accounts."""
        cli_env, fake_pool = cli_env_with_pool
        fake_pool.invalidate_dialogs_cache = MagicMock()
        fake_pool._dialogs_cache = {}

        from tg_agent.cli.commands.dialogs import run

        run(_ns(dialogs_action="cache-clear", phone=None))
        out = capsys.readouterr().out
        fake_pool.invalidate_dialogs_cache.assert_called_once_with()
        assert "all accounts" in out

    def test_cache_clear_specific_phone(self, cli_env_with_pool, capsys):
        """Test cache-clear with --phone clears only that account."""
        cli_env, fake_pool = cli_env_with_pool
        fake_pool.invalidate_dialogs_cache = MagicMock()
        fake_pool._dialogs_cache = {}

        from tg_agent.cli.commands.dialogs import run

        run(_ns(dialogs_action="cache-clear", phone="+10001112233"))
        out = capsys.readouterr().out
        fake_pool.invalidate_dialogs_cache.assert_called_once_with("+10001112233")
        assert "+10001112233" in out

    def test_cache_status_no_cache(self, cli_env_with_pool, capsys):
        """Test cache-status when no dialogs cached."""
        cli_env, fake_pool = cli_env_with_pool
        fake_pool._dialogs_cache = {}
        fake_pool._dialogs_cache_ttl_sec = 60.0

        from tg_agent.cli.commands.dialogs import run

        run(_ns(dialogs_action="cache-status"))
        out = capsys.readouterr().out
        assert "No cached dialogs" in out

    def test_cache_status_with_db_entries(self, cli_env_with_pool, capsys):
        """Test cache-status shows entries from DB."""
        import asyncio

        cli_env, fake_pool = cli_env_with_pool
        fake_pool._dialogs_cache = {}
        fake_pool._dialogs_cache_ttl_sec = 60.0
        db = cli_env

        # Seed some dialog cache rows directly via the DB
        dialogs = [
            {
                "channel_id": 100,
                "title": "Chan A",
                "username": "chana",
                "channel_type": "channel",
                "deactivate": False,
                "is_own": False,
            }
        ]
        asyncio.run(db.repos.dialog_cache.replace_dialogs("+10001112233", dialogs))

        from tg_agent.cli.commands.dialogs import run

        run(_ns(dialogs_action="cache-status"))
        out = capsys.readouterr().out
        assert "+10001112233" in out
        assert "1" in out  # 1 DB entry


# ---------------------------------------------------------------------------
# notification command
# ---------------------------------------------------------------------------


class TestNotificationCommand:
    """Tests for notification CLI command."""

    def test_setup_success(self, cli_env_with_pool, capsys):
        """Test notification setup creates a bot."""
        cli_env, fake_pool = cli_env_with_pool
        fake_pool.clients = {"+70001112233": AsyncMock()}

        from tg_agent.models import NotificationBot
        from tg_agent.services.notification_service import NotificationService

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
            from tg_agent.cli.commands.notification import run

            run(_ns(notification_action="setup"))

        out = capsys.readouterr().out
        assert "@testbot_bot" in out
        assert "123456:ABC-DEF" in out

    def test_status_no_bot(self, cli_env_with_pool, capsys):
        """Test notification status when no bot configured."""
        cli_env, fake_pool = cli_env_with_pool
        asyncio.run(
            cli_env.add_account(Account(phone="+70001112233", session_string="sess", is_primary=True))
        )
        fake_pool.clients = {"+70001112233": AsyncMock()}

        from tg_agent.services.notification_service import NotificationService

        with patch.object(
            NotificationService, "get_status", new_callable=AsyncMock, return_value=None
        ):
            from tg_agent.cli.commands.notification import run

            run(_ns(notification_action="status"))

        out = capsys.readouterr().out
        assert "No notification bot" in out

    def test_status_with_bot(self, cli_env_with_pool, capsys):
        """Test notification status with configured bot."""
        cli_env, fake_pool = cli_env_with_pool
        asyncio.run(
            cli_env.add_account(Account(phone="+70001112233", session_string="sess", is_primary=True))
        )
        fake_pool.clients = {"+70001112233": AsyncMock()}

        from tg_agent.services.notification_service import NotificationService

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
            from tg_agent.cli.commands.notification import run

            run(_ns(notification_action="status"))

        out = capsys.readouterr().out
        assert "@testbot_bot" in out
        assert "456" in out

    def test_delete_success(self, cli_env_with_pool, capsys):
        """Test notification delete removes the bot."""
        cli_env, fake_pool = cli_env_with_pool
        fake_pool.clients = {"+70001112233": AsyncMock()}

        from tg_agent.services.notification_service import NotificationService

        with patch.object(NotificationService, "teardown_bot", new_callable=AsyncMock):
            from tg_agent.cli.commands.notification import run

            run(_ns(notification_action="delete"))

        out = capsys.readouterr().out
        assert "deleted" in out.lower()


# ---------------------------------------------------------------------------
# scheduler command
# ---------------------------------------------------------------------------


class TestSchedulerCommand:
    """Tests for scheduler CLI command."""

    def test_start_writes_autostart_and_exits(self, cli_env_with_pool, capsys):
        """`scheduler start` no longer runs a foreground pool (#1569) — it writes
        the autostart setting, enqueues reconcile and exits."""
        cli_env, fake_pool = cli_env_with_pool

        from tg_agent.cli.commands.scheduler import run

        run(_ns(scheduler_action="start"))

        out = capsys.readouterr().out
        assert "autostart enabled" in out.lower()

def _sched_count_trigger_commands(db_path: str) -> list[int]:
    import sqlite3

    conn = sqlite3.connect(db_path)
    try:
        rows = conn.execute(
            "SELECT id FROM telegram_commands WHERE command_type = ? AND status = ?",
            ("scheduler.trigger_collection", "pending"),
        ).fetchall()
        return [int(r[0]) for r in rows]
    finally:
        conn.close()


def test_trigger_hands_off_to_worker(tmp_path, cli_init_patch, capsys):
    """Test scheduler trigger enqueues the worker command instead of a pool run (#1568)."""
    db_path = _sched_init_empty_db(tmp_path, "sched_trigger.db")

    _sched_run_with_db(db_path, cli_init_patch, _ns(scheduler_action="trigger"))

    out = capsys.readouterr().out
    assert "scheduler.trigger_collection" in out
    command_ids = _sched_count_trigger_commands(db_path)
    assert len(command_ids) == 1
    assert f"#{command_ids[0]}" in out
    assert "Enqueued" not in out and "serve" not in out


def _sched_read_setting(db_path: str, key: str) -> str | None:
    """Read a settings row using a fresh sqlite3 connection.

    Needed because scheduler.run() calls db.close() in finally, leaving the
    aiosqlite-backed cli_env Database unusable.
    """
    import sqlite3

    conn = sqlite3.connect(db_path)
    try:
        cur = conn.execute("SELECT value FROM settings WHERE key = ?", (key,))
        row = cur.fetchone()
        return row[0] if row else None
    finally:
        conn.close()


def _sched_seed_collection_task(db: Database, channel_id: int = 100) -> int:
    """Insert one pending collection task; return its id."""
    return asyncio.run(
        db.repos.tasks.create_collection_task(
            channel_id=channel_id,
            channel_title="seed",
        )
    )


def _sched_read_task_status(db_path: str, task_id: int) -> str | None:
    import sqlite3

    conn = sqlite3.connect(db_path)
    try:
        cur = conn.execute("SELECT status FROM collection_tasks WHERE id = ?", (task_id,))
        row = cur.fetchone()
        return row[0] if row else None
    finally:
        conn.close()


def _sched_count_pending_tasks(db_path: str) -> int:
    import sqlite3

    conn = sqlite3.connect(db_path)
    try:
        cur = conn.execute(
            "SELECT COUNT(*) FROM collection_tasks WHERE status = ?",
            ("pending",),
        )
        return cur.fetchone()[0]
    finally:
        conn.close()


_SCHED_INIT_DB_TARGET = "tg_agent.cli.commands.scheduler.runtime.init_db"


@pytest.fixture
def sched_pool_patch():
    """Patch src.cli.runtime.init_pool to return a pool with one fake client.

    Bypasses scheduler.py's `if not pool.clients: return` guard so the command
    reaches its DB write. The pool itself is just an AsyncMock with disconnect.
    """
    fake_pool = AsyncMock()
    fake_pool.clients = {"+70001112233": AsyncMock()}
    fake_pool.disconnect_all = AsyncMock()

    async def fake_init_pool(config, db, **_kwargs):
        from tg_shared.auth import TelegramAuth

        return TelegramAuth(0, ""), fake_pool

    with patch("tg_agent.cli.runtime.init_pool", side_effect=fake_init_pool):
        yield fake_pool


def _sched_init_empty_db(tmp_path, name: str) -> str:
    """Create a fresh DB file and return its path. Closes its own connection."""
    db_path = str(tmp_path / name)
    db = Database(db_path)
    asyncio.run(db.initialize())
    asyncio.run(db.close())
    return db_path


def _sched_run_with_db(db_path: str, cli_init_patch, ns):
    """Open a fresh Database, run scheduler command via fresh_database=True, then close."""
    db = Database(db_path)
    asyncio.run(db.initialize())
    try:
        with cli_init_patch(db, _SCHED_INIT_DB_TARGET, fresh_database=True):
            from tg_agent.cli.commands.scheduler import run

            run(ns)
    finally:
        asyncio.run(db.close())


class TestSchedulerCommandRW:
    """RW-DB happy-path tests for scheduler stop/job-toggle/set-interval/task-cancel/clear-pending."""

    def test_stop_sets_autostart_off(self, tmp_path, cli_init_patch, sched_pool_patch, capsys):
        db_path = _sched_init_empty_db(tmp_path, "sched_stop.db")

        _sched_run_with_db(db_path, cli_init_patch, _ns(scheduler_action="stop"))

        assert _sched_read_setting(db_path, "scheduler_autostart") == "0"
        assert "Scheduler autostart disabled" in capsys.readouterr().out

    def test_job_toggle_disables_then_enables(
        self, tmp_path, cli_init_patch, sched_pool_patch, capsys
    ):
        db_path = _sched_init_empty_db(tmp_path, "sched_toggle.db")

        _sched_run_with_db(
            db_path,
            cli_init_patch,
            _ns(scheduler_action="job-toggle", job_id="collect_all"),
        )
        assert _sched_read_setting(db_path, "scheduler_job_disabled:collect_all") == "1"
        assert "disabled" in capsys.readouterr().out

        _sched_run_with_db(
            db_path,
            cli_init_patch,
            _ns(scheduler_action="job-toggle", job_id="collect_all"),
        )
        assert _sched_read_setting(db_path, "scheduler_job_disabled:collect_all") == "0"
        assert "enabled" in capsys.readouterr().out

    def test_job_toggle_journals_operator_decision(
        self, tmp_path, cli_init_patch, sched_pool_patch, capsys
    ):
        """#1315: the toggle is journaled, so startup canonicalization keeps it."""
        import sqlite3

        db_path = _sched_init_empty_db(tmp_path, "sched_toggle_journal.db")

        _sched_run_with_db(
            db_path,
            cli_init_patch,
            _ns(scheduler_action="job-toggle", job_id="content_generate_5"),
        )

        conn = sqlite3.connect(db_path)
        try:
            rows = conn.execute(
                "SELECT entity, entity_name, new_value, origin, actor FROM decisions"
            ).fetchall()
        finally:
            conn.close()
        assert rows == [
            ("setting", "scheduler_job_disabled:content_generate_5", "1", "human", "cli")
        ]

    def test_set_interval_writes_setting(
        self, tmp_path, cli_init_patch, sched_pool_patch, capsys
    ):
        db_path = _sched_init_empty_db(tmp_path, "sched_interval.db")

        _sched_run_with_db(
            db_path,
            cli_init_patch,
            _ns(scheduler_action="set-interval", job_id="collect_all", minutes=42),
        )

        assert _sched_read_setting(db_path, "collect_interval_minutes") == "42"
        assert "42 min" in capsys.readouterr().out

    def test_task_cancel_marks_task_cancelled(
        self, tmp_path, cli_init_patch, sched_pool_patch, capsys
    ):
        db_path = str(tmp_path / "sched_taskcancel.db")
        seed = Database(db_path)
        asyncio.run(seed.initialize())
        try:
            task_id = _sched_seed_collection_task(seed, channel_id=222_001)
        finally:
            asyncio.run(seed.close())

        _sched_run_with_db(
            db_path,
            cli_init_patch,
            _ns(scheduler_action="task-cancel", task_id=task_id),
        )

        assert _sched_read_task_status(db_path, task_id) == "cancelled"
        assert f"Task {task_id} cancelled" in capsys.readouterr().out

    def test_clear_pending_removes_all(
        self, tmp_path, cli_init_patch, sched_pool_patch, capsys
    ):
        db_path = str(tmp_path / "sched_clear.db")
        seed = Database(db_path)
        asyncio.run(seed.initialize())
        try:
            _sched_seed_collection_task(seed, channel_id=222_010)
            _sched_seed_collection_task(seed, channel_id=222_011)
        finally:
            asyncio.run(seed.close())
        assert _sched_count_pending_tasks(db_path) == 2

        _sched_run_with_db(
            db_path,
            cli_init_patch,
            _ns(scheduler_action="clear-pending"),
        )

        assert _sched_count_pending_tasks(db_path) == 0
        assert "Cleared 2 pending" in capsys.readouterr().out

    def test_queue_pause_sets_setting(self, tmp_path, cli_init_patch, sched_pool_patch, capsys):
        db_path = _sched_init_empty_db(tmp_path, "sched_qpause.db")

        _sched_run_with_db(db_path, cli_init_patch, _ns(scheduler_action="queue-pause"))

        assert _sched_read_setting(db_path, "collection_queue_paused") == "1"
        assert "Collection queue paused" in capsys.readouterr().out

    def test_queue_resume_clears_setting(self, tmp_path, cli_init_patch, sched_pool_patch, capsys):
        db_path = _sched_init_empty_db(tmp_path, "sched_qresume.db")

        _sched_run_with_db(db_path, cli_init_patch, _ns(scheduler_action="queue-pause"))
        capsys.readouterr()
        _sched_run_with_db(db_path, cli_init_patch, _ns(scheduler_action="queue-resume"))

        assert _sched_read_setting(db_path, "collection_queue_paused") == "0"
        assert "Collection queue resumed" in capsys.readouterr().out

    def test_queue_status_empty(self, tmp_path, cli_init_patch, sched_pool_patch, capsys):
        db_path = _sched_init_empty_db(tmp_path, "sched_qstatus_empty.db")

        _sched_run_with_db(db_path, cli_init_patch, _ns(scheduler_action="queue-status"))

        assert "empty" in capsys.readouterr().out

    def test_queue_status_remaining_without_throughput(
        self, tmp_path, cli_init_patch, sched_pool_patch, capsys
    ):
        db_path = str(tmp_path / "sched_qstatus_pending.db")
        seed = Database(db_path)
        asyncio.run(seed.initialize())
        try:
            _sched_seed_collection_task(seed, channel_id=222_030)
            _sched_seed_collection_task(seed, channel_id=222_031)
        finally:
            asyncio.run(seed.close())

        _sched_run_with_db(db_path, cli_init_patch, _ns(scheduler_action="queue-status"))

        out = capsys.readouterr().out
        assert "2 remaining (2 pending, 0 running)" in out
        # Нет завершений за окно — ETA из данных не построить, называем unknown.
        assert "ETA: unknown" in out

    def test_queue_status_eta_from_window_rate(
        self, tmp_path, cli_init_patch, sched_pool_patch, capsys
    ):
        db_path = str(tmp_path / "sched_qstatus_eta.db")
        seed = Database(db_path)
        asyncio.run(seed.initialize())
        try:
            ids = [_sched_seed_collection_task(seed, channel_id=222_040 + i) for i in range(6)]
            for task_id in ids[:4]:
                asyncio.run(
                    seed.repos.tasks.update_collection_task(
                        task_id, CollectionTaskStatus.COMPLETED
                    )
                )
        finally:
            asyncio.run(seed.close())

        _sched_run_with_db(db_path, cli_init_patch, _ns(scheduler_action="queue-status"))

        out = capsys.readouterr().out
        # Завершения «сейчас» → наблюдение зажато в 0.5 мин: rate = 4/0.5 = 8/мин,
        # ETA = ceil(2/8) = 1 (деление на полное окно занизило бы rate ~в 10 раз).
        assert "Last 0.5 min: 4 completed" in out
        assert "ETA: ~1 min" in out

    def test_queue_status_rate_uses_observed_span(
        self, tmp_path, cli_init_patch, sched_pool_patch, capsys
    ):
        db_path = str(tmp_path / "sched_qstatus_span.db")
        seed = Database(db_path)
        asyncio.run(seed.initialize())
        try:
            ids = [_sched_seed_collection_task(seed, channel_id=222_070 + i) for i in range(6)]
            for task_id in ids[:4]:
                asyncio.run(
                    seed.repos.tasks.update_collection_task(
                        task_id, CollectionTaskStatus.COMPLETED
                    )
                )
            # Завершения 3 минуты назад: наблюдение = 3 мин, rate = 4/3,
            # ETA = ceil(2 / (4/3)) = 2.
            old_ts = (datetime.now(timezone.utc) - timedelta(minutes=3)).isoformat()
            asyncio.run(
                seed.execute_write(
                    "UPDATE collection_tasks SET completed_at = ? WHERE status = ?",
                    (old_ts, CollectionTaskStatus.COMPLETED.value),
                )
            )
        finally:
            asyncio.run(seed.close())

        _sched_run_with_db(db_path, cli_init_patch, _ns(scheduler_action="queue-status"))

        out = capsys.readouterr().out
        assert "min: 4 completed" in out
        assert "ETA: ~2 min" in out

    def test_queue_status_paused_marker(
        self, tmp_path, cli_init_patch, sched_pool_patch, capsys
    ):
        db_path = str(tmp_path / "sched_qstatus_paused.db")
        seed = Database(db_path)
        asyncio.run(seed.initialize())
        try:
            _sched_seed_collection_task(seed, channel_id=222_050)
        finally:
            asyncio.run(seed.close())

        _sched_run_with_db(db_path, cli_init_patch, _ns(scheduler_action="queue-pause"))
        capsys.readouterr()
        _sched_run_with_db(db_path, cli_init_patch, _ns(scheduler_action="queue-status"))

        out = capsys.readouterr().out
        assert "PAUSED" in out
        assert "1 remaining (1 pending, 0 running)" in out

    def test_queue_status_json(self, tmp_path, cli_init_patch, sched_pool_patch, capsys):
        db_path = str(tmp_path / "sched_qstatus_json.db")
        seed = Database(db_path)
        asyncio.run(seed.initialize())
        try:
            _sched_seed_collection_task(seed, channel_id=222_060)
        finally:
            asyncio.run(seed.close())

        _sched_run_with_db(
            db_path,
            cli_init_patch,
            _ns(scheduler_action="queue-status", scheduler_format="json"),
        )

        data = json.loads(capsys.readouterr().out)
        assert data["remaining"] == 1
        assert data["pending"] == 1
        assert data["running"] == 0
        assert data["observed_min"] == 5  # завершений нет — наблюдение = полное окно
        assert data["paused"] is False
        assert data["rate_per_min"] == 0.0
        assert data["eta_min"] is None

    def test_db_only_actions_do_not_init_pool(self, tmp_path, cli_init_patch, capsys):
        """DB-only scheduler subcommands must not connect the Telegram pool.

        Each `scheduler task-cancel` used to spin up a full ClientPool (every
        account connecting to Telegram) just to flip one DB row — which made
        per-task cleanup in the live heavy suite take 30s+ per call and blow
        the test timeout, and needlessly hammered Telegram with connects.
        """

        async def _explode(config, db):
            raise AssertionError(
                "init_pool must not be called for DB-only scheduler actions"
            )

        db_path = str(tmp_path / "sched_no_pool.db")
        seed = Database(db_path)
        asyncio.run(seed.initialize())
        try:
            task_id = _sched_seed_collection_task(seed, channel_id=222_020)
        finally:
            asyncio.run(seed.close())

        with patch("tg_agent.cli.runtime.init_pool", side_effect=_explode):
            for ns in (
                _ns(scheduler_action="task-cancel", task_id=task_id),
                _ns(scheduler_action="clear-pending"),
                _ns(scheduler_action="status"),
                _ns(scheduler_action="stop"),
                _ns(scheduler_action="job-toggle", job_id="collect_all"),
                _ns(scheduler_action="set-interval", job_id="collect_all", minutes=30),
                _ns(scheduler_action="queue-pause"),
                _ns(scheduler_action="queue-resume"),
            ):
                _sched_run_with_db(db_path, cli_init_patch, ns)

        assert _sched_read_task_status(db_path, task_id) == "cancelled"


# ---------------------------------------------------------------------------
# serve command
# ---------------------------------------------------------------------------


class TestServeCommand:
    """Tests for serve CLI command."""

    def test_serve_exits_without_password(self, capsys):
        """Test serve exits when no password is configured."""
        from tg_agent.cli.commands.serve import run

        with patch("tg_agent.cli.commands.serve.load_config") as mock_load:
            config = AppConfig()
            config.web.password = ""
            mock_load.return_value = config

            with pytest.raises(SystemExit, match="1"):
                run(_ns(config="config.yaml", web_pass=None))

    def test_serve_uses_web_pass_arg(self):
        """Test serve uses web_pass from command line."""
        from tg_agent.cli.commands.serve import run

        with (
            patch("tg_agent.cli.commands.serve.load_config") as mock_load,
            patch_serve_create_app() as mock_create,
            patch("tg_agent.cli.commands.serve.pid_file_path") as mock_pid_path,
            patch("tg_agent.cli.commands.serve.register_current_process"),
            patch("tg_agent.cli.commands.serve.unregister_current_process"),
            patch("tg_agent.cli.commands.serve.uvicorn.run") as mock_uvicorn,
        ):

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
        from tg_agent.cli.commands.serve import run

        with (
            patch("tg_agent.cli.commands.serve.load_config") as mock_load,
            patch_serve_create_app() as mock_create,
            patch("tg_agent.cli.commands.serve.pid_file_path") as mock_pid_path,
            patch("tg_agent.cli.commands.serve.register_current_process") as mock_register,
            patch("tg_agent.cli.commands.serve.unregister_current_process") as mock_unregister,
            patch("tg_agent.cli.commands.serve.uvicorn.run"),
        ):

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
        from tg_agent.cli.commands.serve import run

        with (
            patch("tg_agent.cli.commands.serve.load_config") as mock_load,
            patch_serve_create_app(),
            patch("tg_agent.cli.commands.serve.pid_file_path") as mock_pid_path,
            patch("tg_agent.cli.commands.serve.register_current_process") as mock_register,
        ):

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

        from tg_agent.cli.commands.collect import run

        run(_ns(channel_id=None, full=False))
        assert "No connected accounts" in caplog.text

    def test_collect_success(self, cli_env_with_pool, capsys):
        """Test collect enqueues channels."""
        cli_env, fake_pool = cli_env_with_pool
        fake_pool.clients = {"+70001112233": AsyncMock()}

        from tg_agent.cli.commands.collect import run

        run(_ns(channel_id=None, full=False))

        out = capsys.readouterr().out
        assert "enqueued" in out.lower()

    def test_collect_single_channel(self, cli_env_with_pool, capsys):
        """Test collect for a single channel."""
        cli_env, fake_pool = cli_env_with_pool
        fake_pool.clients = {"+70001112233": AsyncMock()}

        # Add a channel to collect
        _add_channel(cli_env, channel_id=100, title="Test Channel")

        from tg_agent.telegram.collector import Collector

        mock_stats = {"collected": 10, "errors": 0}

        with patch.object(
            Collector, "collect_single_channel", new_callable=AsyncMock, return_value=mock_stats
        ) as mock_collect:
            from tg_agent.cli.commands.collect import run

            run(_ns(channel_id=100, full=False))

            mock_collect.assert_awaited_once()
            assert mock_collect.call_args.kwargs.get("full") is False

        out = capsys.readouterr().out
        assert "Collected" in out and "100" in out

    def test_collect_full_mode(self, cli_env_with_pool, capsys):
        """Test collect with full flag for single channel."""
        cli_env, fake_pool = cli_env_with_pool
        fake_pool.clients = {"+70001112233": AsyncMock()}

        # Add a channel
        _add_channel(cli_env, channel_id=100, title="Test Channel")

        from tg_agent.telegram.collector import Collector

        mock_count = 100

        with patch.object(
            Collector, "collect_single_channel", new_callable=AsyncMock, return_value=mock_count
        ) as mock_collect:
            from tg_agent.cli.commands.collect import run

            # Pass channel_id as int since argparse may convert it
            run(_ns(channel_id=100, full=True))

            # collect_single_channel should be called with full=True
            mock_collect.assert_awaited_once()
            # Check the kwargs - full is a keyword-only argument
            call_kwargs = mock_collect.call_args.kwargs
            assert call_kwargs.get("full") is True

        out = capsys.readouterr().out
        assert "100" in out or "complete" in out.lower()

    def test_collect_single_channel_username_resolve_deferred(self, cli_env_with_pool, capsys):
        """Username resolve backoff should print a CLI message, not a traceback."""
        from datetime import datetime, timedelta, timezone

        from tg_agent.telegram.collector import Collector, UsernameResolveFloodWaitDeferredError

        cli_env, fake_pool = cli_env_with_pool
        fake_pool.clients = {"+70001112233": AsyncMock()}
        _add_channel(cli_env, channel_id=100, title="Test Channel")

        exc = UsernameResolveFloodWaitDeferredError(
            wait_seconds=60,
            next_available_at=datetime.now(timezone.utc) + timedelta(seconds=60),
        )
        with patch.object(Collector, "collect_single_channel", new_callable=AsyncMock, side_effect=exc):
            from tg_agent.cli.commands.collect import run

            run(_ns(channel_id=100, full=True))

        out = capsys.readouterr().out
        assert "Username resolve Flood Wait active until" in out
        assert "try again later" in out


    def test_collect_single_channel_username_rate_limited(self, cli_env_with_pool, capsys):
        from tg_agent.telegram.collector import Collector, UsernameResolveRateLimitedError

        cli_env, fake_pool = cli_env_with_pool
        fake_pool.clients = {"+70001112233": AsyncMock()}
        _add_channel(cli_env, channel_id=101, title="Rate Limited Channel")

        exc = UsernameResolveRateLimitedError("+7001", 28)
        with patch.object(Collector, "collect_single_channel", new_callable=AsyncMock, side_effect=exc):
            from tg_agent.cli.commands.collect import run

            run(_ns(channel_id=101, full=True))

        out = capsys.readouterr().out
        assert "resolve_username rate-limited" in out
        assert "Collected" not in out

    def test_collect_single_channel_all_clients_flooded(self, cli_env_with_pool, capsys):
        """A pool-wide collection FloodWait must print a CLI message with the
        retry deadline, not dump a traceback."""
        from datetime import datetime, timedelta, timezone

        from tg_agent.telegram.collector import AllCollectionClientsFloodedError, Collector

        cli_env, fake_pool = cli_env_with_pool
        fake_pool.clients = {"+70001112233": AsyncMock()}
        _add_channel(cli_env, channel_id=102, title="Flooded Channel")

        next_at = datetime.now(timezone.utc) + timedelta(seconds=120)
        exc = AllCollectionClientsFloodedError(120, next_at)
        with patch.object(Collector, "collect_single_channel", new_callable=AsyncMock, side_effect=exc):
            from tg_agent.cli.commands.collect import run

            run(_ns(channel_id=102, full=False))

        out = capsys.readouterr().out
        assert "flood-waited until" in out
        assert "120s" in out
        assert "Collected" not in out

    def test_collect_enqueue_warns_about_active_resolve_backoff(
        self, cli_env_with_pool, capsys
    ):
        """#790: bare `collect` must surface active per-account resolve
        backoffs instead of silently enqueueing as if nothing is wrong."""
        cli_env, fake_pool = cli_env_with_pool
        fake_pool.clients = {"+7001": AsyncMock(), "+7002": AsyncMock()}
        fake_pool.get_resolve_username_backoff_remaining_sec = MagicMock(
            side_effect=lambda phone=None: 600 if phone == "+7001" else 0
        )

        from tg_agent.cli.commands.collect import run

        run(_ns(channel_id=None, full=False))

        out = capsys.readouterr().out
        assert "enqueued" in out.lower()
        assert "resolve_username Flood Wait active" in out
        assert "+7001" in out
        assert "+7002" not in out.split("Flood Wait active")[-1]

    def test_collect_enqueue_no_backoff_no_warning(self, cli_env_with_pool, capsys):
        cli_env, fake_pool = cli_env_with_pool
        fake_pool.clients = {"+7001": AsyncMock()}

        from tg_agent.cli.commands.collect import run

        run(_ns(channel_id=None, full=False))

        out = capsys.readouterr().out
        assert "enqueued" in out.lower()
        assert "Flood Wait active" not in out


def _add_channel(db: Database, channel_id: int = 100, title: str = "TestCh") -> int:
    return asyncio.run(db.add_channel(Channel(channel_id=channel_id, title=title)))
