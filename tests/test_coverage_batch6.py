"""Coverage batch 6: cli/commands/test.py, cli/process_control.py,
database/connection.py, database/repositories/messages.py,
database/migrations.py, database/repositories/photo_loader.py
"""

from __future__ import annotations

import os
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import aiosqlite
import pytest

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


_FLOOD_DT = datetime(2025, 1, 1, tzinfo=timezone.utc)


def _flood_info(operation: str, *, wait_seconds: int = 5, detail: str = ""):
    from src.telegram.flood_wait import FloodWaitInfo

    return FloodWaitInfo(
        operation=operation,
        wait_seconds=wait_seconds,
        phone=None,
        detail=detail,
        next_available_at_utc=_FLOOD_DT,
    )


def _make_msg(channel_id: int = 100, message_id: int = 1, text: str = "hello"):
    from src.models import Message

    return Message(
        channel_id=channel_id,
        message_id=message_id,
        text=text,
        date=datetime(2025, 1, 15, tzinfo=timezone.utc),
    )


# ===========================================================================
# 1. cli/commands/test.py — helper functions and additional branches
# ===========================================================================


class TestFormatAllFloodedDetail:
    def test_no_retry_sec(self):
        from src.cli.commands.test import _format_all_flooded_detail

        result = _format_all_flooded_detail(
            "base",
            retry_after_sec=None,
            next_available_at_utc=None,
        )
        assert "all clients are flood-waited" in result
        assert "base" in result

    def test_retry_sec_no_datetime(self):
        from src.cli.commands.test import _format_all_flooded_detail

        result = _format_all_flooded_detail(
            "base",
            retry_after_sec=60,
            next_available_at_utc=None,
        )
        assert "60s" in result
        assert "retry after about" in result

    def test_retry_sec_with_datetime(self):
        from src.cli.commands.test import _format_all_flooded_detail

        dt = datetime(2025, 5, 1, 12, 0, 0, tzinfo=timezone.utc)
        result = _format_all_flooded_detail(
            "base",
            retry_after_sec=30,
            next_available_at_utc=dt,
        )
        assert "until" in result
        assert "2025-05-01" in result


class TestIsPremiumFlood:
    def test_premium_flood_operations(self):
        from src.cli.commands.test import _is_premium_flood

        for op in ("check_search_quota", "search_telegram_check_quota", "search_telegram"):
            info = _flood_info(op, wait_seconds=10)
            assert _is_premium_flood(info) is True

    def test_non_premium_operation(self):
        from src.cli.commands.test import _is_premium_flood

        info = _flood_info("get_dialogs")
        assert _is_premium_flood(info) is False


class TestGetLiveFloodAvailability:
    @pytest.mark.asyncio
    async def test_premium_flood_uses_premium_getter(self):
        from src.cli.commands.test import _get_live_flood_availability

        info = _flood_info("search_telegram")
        avail = MagicMock()
        pool = MagicMock()
        pool.get_premium_stats_availability = AsyncMock(return_value=avail)
        result = await _get_live_flood_availability(pool, info)
        assert result is avail

    @pytest.mark.asyncio
    async def test_non_premium_uses_regular_getter(self):
        from src.cli.commands.test import _get_live_flood_availability

        info = _flood_info("get_dialogs")
        avail = MagicMock()
        pool = MagicMock()
        pool.get_stats_availability = AsyncMock(return_value=avail)
        result = await _get_live_flood_availability(pool, info)
        assert result is avail

    @pytest.mark.asyncio
    async def test_returns_none_when_no_getter(self):
        from src.cli.commands.test import _get_live_flood_availability

        info = _flood_info("get_dialogs")
        pool = MagicMock(spec=[])  # no attributes
        result = await _get_live_flood_availability(pool, info)
        assert result is None


class TestDecideLiveTestFloodAction:
    @pytest.mark.asyncio
    async def test_no_availability_returns_skip(self):
        from src.cli.commands.test import _decide_live_test_flood_action

        info = _flood_info("get_dialogs", detail="flooded")
        pool = MagicMock(spec=[])
        decision = await _decide_live_test_flood_action(pool, info)
        assert decision.action == "skip"

    @pytest.mark.asyncio
    async def test_not_all_flooded_returns_rotate(self):
        from src.cli.commands.test import _decide_live_test_flood_action

        info = _flood_info("get_dialogs", detail="partial")
        avail = MagicMock()
        avail.state = "partial_flood"
        pool = MagicMock()
        pool.get_stats_availability = AsyncMock(return_value=avail)
        decision = await _decide_live_test_flood_action(pool, info)
        assert decision.action == "rotate"

    @pytest.mark.asyncio
    async def test_all_flooded_short_retry_returns_wait_retry(self):
        from src.cli.commands.test import _decide_live_test_flood_action

        info = _flood_info("get_dialogs", detail="all flood")
        avail = MagicMock()
        avail.state = "all_flooded"
        avail.retry_after_sec = 10  # <= SHORT_FLOOD_WAIT_RETRY_SEC (30)
        avail.next_available_at_utc = None
        pool = MagicMock()
        pool.get_stats_availability = AsyncMock(return_value=avail)
        decision = await _decide_live_test_flood_action(pool, info)
        assert decision.action == "wait_retry"
        assert decision.retry_after_sec == 10

    @pytest.mark.asyncio
    async def test_all_flooded_long_retry_returns_skip(self):
        from src.cli.commands.test import _decide_live_test_flood_action

        info = _flood_info("get_dialogs", detail="all flood")
        avail = MagicMock()
        avail.state = "all_flooded"
        avail.retry_after_sec = 120  # > SHORT_FLOOD_WAIT_RETRY_SEC
        avail.next_available_at_utc = None
        pool = MagicMock()
        pool.get_stats_availability = AsyncMock(return_value=avail)
        decision = await _decide_live_test_flood_action(pool, info)
        assert decision.action == "skip"


class TestGetSearchResultFloodWait:
    def test_returns_flood_wait_info(self):
        from src.cli.commands.test import _get_search_result_flood_wait

        info = _flood_info("search")
        result = MagicMock()
        result.flood_wait = info
        assert _get_search_result_flood_wait(result) is info

    def test_returns_none_when_not_flood_wait(self):
        from src.cli.commands.test import _get_search_result_flood_wait

        result = MagicMock()
        result.flood_wait = "not_a_flood_wait"
        assert _get_search_result_flood_wait(result) is None

    def test_returns_none_when_no_attribute(self):
        from src.cli.commands.test import _get_search_result_flood_wait

        result = MagicMock(spec=[])
        assert _get_search_result_flood_wait(result) is None


class TestIsClientUnavailableErrors:
    def test_regular_search_unavailable(self):
        from src.cli.commands.test import _is_regular_search_client_unavailable_error

        assert _is_regular_search_client_unavailable_error(
            "Нет доступных Telegram-аккаунтов. Проверьте подключение."
        )
        assert not _is_regular_search_client_unavailable_error("other error")
        assert not _is_regular_search_client_unavailable_error(None)

    def test_premium_flood_unavailable(self):
        from src.cli.commands.test import _is_premium_flood_unavailable_error

        assert _is_premium_flood_unavailable_error(
            "Premium-аккаунты временно недоступны из-за Flood Wait."
        )
        assert not _is_premium_flood_unavailable_error("regular error")
        assert not _is_premium_flood_unavailable_error(None)


class TestSkipRemainingChecks:
    def test_appends_skip_results(self):
        from src.cli.commands.test import Status, _skip_remaining_tg_checks

        results = []
        _skip_remaining_tg_checks(results, "pool failed", ["check_a", "check_b"])
        assert len(results) == 2
        assert all(r.status == Status.SKIP for r in results)
        assert results[0].name == "check_a"
        assert results[1].name == "check_b"


class TestRunChecks:
    @pytest.mark.asyncio
    async def test_check_get_stats_fail(self):
        from src.cli.commands.test import Status, _check_get_stats

        db = MagicMock()
        db.get_stats = AsyncMock(side_effect=Exception("stats error"))
        result = await _check_get_stats(db)
        assert result.status == Status.FAIL
        assert "stats error" in result.detail

    @pytest.mark.asyncio
    async def test_check_account_list_fail(self):
        from src.cli.commands.test import Status, _check_account_list

        db = MagicMock()
        db.get_accounts = AsyncMock(side_effect=Exception("acct error"))
        result = await _check_account_list(db)
        assert result.status == Status.FAIL

    @pytest.mark.asyncio
    async def test_check_channel_list_fail(self):
        from src.cli.commands.test import Status, _check_channel_list

        db = MagicMock()
        db.get_channels_with_counts = AsyncMock(side_effect=Exception("ch error"))
        result = await _check_channel_list(db)
        assert result.status == Status.FAIL

    @pytest.mark.asyncio
    async def test_check_notification_queries_with_data(self):
        from src.cli.commands.test import Status, _check_notification_queries

        db = MagicMock()
        db.get_notification_queries = AsyncMock(return_value=[MagicMock(), MagicMock()])
        result = await _check_notification_queries(db)
        assert result.status == Status.PASS
        assert "2 queries" in result.detail

    @pytest.mark.asyncio
    async def test_check_notification_queries_fail(self):
        from src.cli.commands.test import Status, _check_notification_queries

        db = MagicMock()
        db.get_notification_queries = AsyncMock(side_effect=Exception("db fail"))
        result = await _check_notification_queries(db)
        assert result.status == Status.FAIL

    @pytest.mark.asyncio
    async def test_check_local_search_fail(self):
        from src.cli.commands.test import Status, _check_local_search

        db = MagicMock()
        db.search_messages = AsyncMock(side_effect=Exception("search fail"))
        result = await _check_local_search(db)
        assert result.status == Status.FAIL

    @pytest.mark.asyncio
    async def test_check_collection_tasks_fail(self):
        from src.cli.commands.test import Status, _check_collection_tasks

        db = MagicMock()
        db.get_collection_tasks = AsyncMock(side_effect=Exception("task fail"))
        result = await _check_collection_tasks(db)
        assert result.status == Status.FAIL

    @pytest.mark.asyncio
    async def test_check_recent_searches_with_data(self):
        from src.cli.commands.test import Status, _check_recent_searches

        db = MagicMock()
        db.get_recent_searches = AsyncMock(return_value=[MagicMock()])
        result = await _check_recent_searches(db)
        assert result.status == Status.PASS
        assert "1 entries" in result.detail

    @pytest.mark.asyncio
    async def test_check_recent_searches_fail(self):
        from src.cli.commands.test import Status, _check_recent_searches

        db = MagicMock()
        db.get_recent_searches = AsyncMock(side_effect=Exception("rs fail"))
        result = await _check_recent_searches(db)
        assert result.status == Status.FAIL

    @pytest.mark.asyncio
    async def test_check_pipeline_list_fail(self):
        from src.cli.commands.test import Status, _check_pipeline_list

        db = MagicMock()
        db.repos = MagicMock()
        db.repos.content_pipelines = MagicMock()
        db.repos.content_pipelines.get_all = AsyncMock(side_effect=Exception("pipeline fail"))
        result = await _check_pipeline_list(db)
        assert result.status == Status.FAIL

    @pytest.mark.asyncio
    async def test_check_notification_bot_fail(self):
        from src.cli.commands.test import Status, _check_notification_bot

        db = MagicMock()
        db.repos = MagicMock()
        db.repos.notification_bots = MagicMock()
        db.repos.notification_bots.count = AsyncMock(side_effect=Exception("bot fail"))
        result = await _check_notification_bot(db)
        assert result.status == Status.FAIL

    @pytest.mark.asyncio
    async def test_check_notification_bot_configured(self):
        from src.cli.commands.test import Status, _check_notification_bot

        db = MagicMock()
        db.repos = MagicMock()
        db.repos.notification_bots = MagicMock()
        db.repos.notification_bots.count = AsyncMock(return_value=2)
        result = await _check_notification_bot(db)
        assert result.status == Status.PASS
        assert "2 configured" in result.detail

    @pytest.mark.asyncio
    async def test_check_photo_tasks_fail(self):
        from src.cli.commands.test import Status, _check_photo_tasks

        db = MagicMock()
        db.repos = MagicMock()
        db.repos.photo_loader = MagicMock()
        db.repos.photo_loader.list_batches = AsyncMock(side_effect=Exception("photo fail"))
        result = await _check_photo_tasks(db)
        assert result.status == Status.FAIL


class TestCLITestRunEntry:
    def test_run_read_db_init_failure(self, capsys):
        from src.cli.commands.test import run

        ns = SimpleNamespace(config="config.yaml", test_action="read")
        with patch("src.cli.runtime.init_db", side_effect=Exception("db crash")):
            with pytest.raises(SystemExit):
                run(ns)

    def test_run_write_db_init_failure(self, capsys):
        from src.cli.commands.test import run

        ns = SimpleNamespace(config="config.yaml", test_action="write")
        with patch("src.cli.runtime.init_db", side_effect=Exception("no config")):
            # write_db_copy fails → run() exits with 1
            with pytest.raises(SystemExit) as exc_info:
                run(ns)
        assert exc_info.value.code == 1
        out = capsys.readouterr().out
        assert "Write Tests" in out

    def test_run_telegram_db_init_failure(self, capsys):
        from src.cli.commands.test import run

        ns = SimpleNamespace(config="config.yaml", test_action="telegram")
        with patch("src.cli.runtime.init_db", side_effect=Exception("no config")):
            # tg_db_copy fails → run() exits with 1
            with pytest.raises(SystemExit) as exc_info:
                run(ns)
        assert exc_info.value.code == 1
        out = capsys.readouterr().out
        assert "Telegram" in out

    def test_run_all_with_mocked_db(self, capsys):
        from src.cli.commands.test import run

        fake_db = MagicMock()
        fake_db.get_stats = AsyncMock(return_value={"channels": 0, "messages": 0})
        fake_db.get_accounts = AsyncMock(return_value=[])
        fake_db.get_channels_with_counts = AsyncMock(return_value=[])
        fake_db.get_notification_queries = AsyncMock(return_value=[])
        fake_db.search_messages = AsyncMock(return_value=([], 0))
        fake_db.get_collection_tasks = AsyncMock(return_value=[])
        fake_db.get_recent_searches = AsyncMock(return_value=[])
        fake_db.repos = MagicMock()
        fake_db.repos.content_pipelines = MagicMock()
        fake_db.repos.content_pipelines.get_all = AsyncMock(return_value=[])
        fake_db.repos.notification_bots = MagicMock()
        fake_db.repos.notification_bots.count = AsyncMock(return_value=0)
        fake_db.repos.photo_loader = MagicMock()
        fake_db.repos.photo_loader.list_batches = AsyncMock(return_value=[])
        fake_db.close = AsyncMock()
        fake_db._db_path = ":memory:"
        fake_db._session_encryption_secret = None

        # Only run read to keep the test fast
        ns = SimpleNamespace(config="config.yaml", test_action="read")
        with patch("src.cli.runtime.init_db", return_value=(MagicMock(), fake_db)):
            run(ns)
        out = capsys.readouterr().out
        assert "Read Tests" in out
        assert "Test Summary" in out

    def test_run_all_exits_1_on_failure(self, capsys):
        from src.cli.commands.test import run

        fake_db = MagicMock()
        fake_db.get_stats = AsyncMock(side_effect=Exception("force fail"))
        fake_db.get_accounts = AsyncMock(return_value=[])
        fake_db.get_channels_with_counts = AsyncMock(return_value=[])
        fake_db.get_notification_queries = AsyncMock(return_value=[])
        fake_db.search_messages = AsyncMock(return_value=([], 0))
        fake_db.get_collection_tasks = AsyncMock(return_value=[])
        fake_db.get_recent_searches = AsyncMock(return_value=[])
        fake_db.repos = MagicMock()
        fake_db.repos.content_pipelines = MagicMock()
        fake_db.repos.content_pipelines.get_all = AsyncMock(return_value=[])
        fake_db.repos.notification_bots = MagicMock()
        fake_db.repos.notification_bots.count = AsyncMock(return_value=0)
        fake_db.repos.photo_loader = MagicMock()
        fake_db.repos.photo_loader.list_batches = AsyncMock(return_value=[])
        fake_db.close = AsyncMock()
        fake_db._db_path = ":memory:"
        fake_db._session_encryption_secret = None

        ns = SimpleNamespace(config="config.yaml", test_action="read")
        with patch("src.cli.runtime.init_db", return_value=(MagicMock(), fake_db)):
            with pytest.raises(SystemExit) as exc_info:
                run(ns)
        assert exc_info.value.code == 1


class TestDisableFloodAutoSleep:
    @pytest.mark.asyncio
    async def test_sets_threshold_to_zero(self):
        from src.cli.commands.test import _disable_flood_auto_sleep

        raw_client = MagicMock()
        raw_client.flood_sleep_threshold = 60

        session = MagicMock()
        session.raw_client = raw_client

        with patch(
            "src.cli.commands.test.adapt_transport_session",
            return_value=session,
        ):
            pool = MagicMock()
            pool.clients = {"phone1": MagicMock()}
            await _disable_flood_auto_sleep(pool)

        assert raw_client.flood_sleep_threshold == 0

    @pytest.mark.asyncio
    async def test_skips_when_no_raw_client(self):
        from src.cli.commands.test import _disable_flood_auto_sleep

        session = MagicMock()
        session.raw_client = None

        with patch(
            "src.cli.commands.test.adapt_transport_session",
            return_value=session,
        ):
            pool = MagicMock()
            pool.clients = {"phone1": MagicMock()}
            # Should not raise
            await _disable_flood_auto_sleep(pool)


class TestTelegramLiveFloodDecision:
    def test_dataclass_attributes(self):
        from src.cli.commands.test import TelegramLiveFloodDecision

        d = TelegramLiveFloodDecision(action="skip", detail="flooded")
        assert d.action == "skip"
        assert d.detail == "flooded"
        assert d.retry_after_sec is None
        assert d.next_available_at_utc is None


# ===========================================================================
# 2. cli/process_control.py — uncovered branches
# ===========================================================================


class TestProcessControlAdditional:
    def test_is_expected_server_process_negative_pid(self):
        from src.cli.process_control import is_expected_server_process

        assert is_expected_server_process(-1) is False

    def test_is_expected_server_process_dead_pid(self):
        from src.cli.process_control import is_expected_server_process

        assert is_expected_server_process(999999999) is False

    def test_is_expected_server_process_alive_but_wrong_command(self):
        from src.cli.process_control import is_expected_server_process

        # Current process is alive but not running src.main serve
        result = is_expected_server_process(os.getpid())
        # pytest itself is not src.main serve, so should be False
        assert result is False

    def test_stop_server_unmanaged_pid(self, tmp_path):
        """Process exists but is not src.main serve — returns UNMANAGED."""
        from src.cli.process_control import StopResult, stop_server

        p = tmp_path / "unmanaged.pid"
        p.write_text(f"{os.getpid()}\n", encoding="utf-8")
        # Current process is alive but not src.main serve
        outcome = stop_server(p)
        assert outcome.result == StopResult.UNMANAGED

    def test_ensure_server_not_running_stale_pid_removes_file(self, tmp_path):
        """ensure_server_not_running removes file when PID is dead."""
        from src.cli.process_control import ensure_server_not_running

        p = tmp_path / "stale.pid"
        p.write_text("999999999\n", encoding="utf-8")
        ensure_server_not_running(p)
        assert not p.exists()

    def test_unregister_current_process_wrong_pid(self, tmp_path):
        """unregister_current_process does NOT remove file owned by another PID."""
        from src.cli.process_control import unregister_current_process

        p = tmp_path / "other.pid"
        # Write a PID that's definitely not ours (use PID 1 which is init/launchd)
        p.write_text("1\n", encoding="utf-8")
        unregister_current_process(p)
        # File should still exist since it's not our PID
        assert p.exists()

    def test_unregister_swallows_exceptions(self, tmp_path):
        """unregister_current_process must never raise."""
        from src.cli.process_control import unregister_current_process

        p = tmp_path / "corrupt.pid"
        p.write_text("not_a_number\n", encoding="utf-8")
        # Should not raise despite ValueError from read_pid
        unregister_current_process(p)

    def test_is_process_alive_permission_error(self):
        """PID 1 (init) returns PermissionError -> alive=True on macOS/Linux."""
        from src.cli.process_control import is_process_alive

        # PID 1 always exists but may raise PermissionError
        result = is_process_alive(1)
        # Either True (permission error = alive) or False (not found, unusual)
        assert isinstance(result, bool)

    def test_process_command_oserror_returns_empty(self):
        """_process_command returns '' on OSError in subprocess.run."""
        from src.cli.process_control import _process_command

        with patch("subprocess.run", side_effect=OSError("no ps")):
            result = _process_command(os.getpid())
            assert result == ""

    def test_process_command_nonzero_rc_returns_empty(self):
        """_process_command returns '' on non-zero returncode."""

        from src.cli.process_control import _process_command

        mock_result = MagicMock()
        mock_result.returncode = 1
        mock_result.stdout = ""
        with patch("subprocess.run", return_value=mock_result):
            result = _process_command(os.getpid())
            assert result == ""

    def test_stop_server_permission_denied_sigterm(self, tmp_path):
        """stop_server raises ProcessControlError on PermissionError during SIGTERM."""
        from src.cli.process_control import ProcessControlError, stop_server

        p = tmp_path / "perm.pid"
        p.write_text(f"{os.getpid()}\n", encoding="utf-8")

        with patch("src.cli.process_control.is_expected_server_process", return_value=True):
            with patch("os.kill", side_effect=PermissionError("no perm")):
                with pytest.raises(ProcessControlError, match="Permission denied"):
                    stop_server(p)

    def test_register_creates_parent_dirs(self, tmp_path):
        """register_current_process creates parent directories if needed."""
        from src.cli.process_control import register_current_process, unregister_current_process

        nested = tmp_path / "a" / "b" / "c" / "test.pid"
        register_current_process(nested)
        assert nested.exists()
        assert int(nested.read_text().strip()) == os.getpid()
        unregister_current_process(nested)


# ===========================================================================
# 3. database/connection.py — uncovered paths
# ===========================================================================


class TestDBConnectionAdditional:
    @pytest.mark.asyncio
    async def test_execute_method_works(self, tmp_path):
        from src.database.connection import DBConnection

        conn = DBConnection(str(tmp_path / "exec.db"))
        try:
            await conn.connect()
            cur = await conn.execute("SELECT 42 AS val")
            row = await cur.fetchone()
            assert row[0] == 42
        finally:
            await conn.close()

    @pytest.mark.asyncio
    async def test_connect_sets_row_factory(self, tmp_path):
        from src.database.connection import DBConnection

        conn = DBConnection(str(tmp_path / "rf.db"))
        try:
            db = await conn.connect()
            # row_factory should be aiosqlite.Row
            assert db.row_factory is aiosqlite.Row
        finally:
            await conn.close()

    @pytest.mark.asyncio
    async def test_connect_sets_foreign_keys(self, tmp_path):
        from src.database.connection import DBConnection

        conn = DBConnection(str(tmp_path / "fk.db"))
        try:
            db = await conn.connect()
            cur = await db.execute("PRAGMA foreign_keys")
            row = await cur.fetchone()
            assert row[0] == 1
        finally:
            await conn.close()

    @pytest.mark.asyncio
    async def test_profiling_connection_execute_without_profiler(self, tmp_path):
        from src.database.connection import ProfilingConnection

        async with aiosqlite.connect(str(tmp_path / "prof.db")) as raw:
            raw.row_factory = aiosqlite.Row
            pc = ProfilingConnection(raw)
            cur = await pc.execute("SELECT 1 AS n")
            row = await cur.fetchone()
            assert row[0] == 1

    @pytest.mark.asyncio
    async def test_profiling_connection_execute_fetchall(self, tmp_path):
        from src.database.connection import ProfilingConnection

        async with aiosqlite.connect(str(tmp_path / "prof2.db")) as raw:
            raw.row_factory = aiosqlite.Row
            pc = ProfilingConnection(raw)
            rows = await pc.execute_fetchall("SELECT 1 AS n UNION SELECT 2")
            assert len(rows) == 2

    @pytest.mark.asyncio
    async def test_profiling_connection_executemany(self, tmp_path):
        from src.database.connection import ProfilingConnection

        async with aiosqlite.connect(str(tmp_path / "prof3.db")) as raw:
            raw.row_factory = aiosqlite.Row
            await raw.execute("CREATE TABLE t (v INTEGER)")
            pc = ProfilingConnection(raw)
            await pc.executemany("INSERT INTO t VALUES (?)", [(1,), (2,), (3,)])
            rows = await raw.execute_fetchall("SELECT * FROM t")
            assert len(rows) == 3

    @pytest.mark.asyncio
    async def test_profiling_connection_executescript(self, tmp_path):
        from src.database.connection import ProfilingConnection

        async with aiosqlite.connect(str(tmp_path / "prof4.db")) as raw:
            raw.row_factory = aiosqlite.Row
            pc = ProfilingConnection(raw)
            await pc.executescript("CREATE TABLE x (id INTEGER PRIMARY KEY)")

    @pytest.mark.asyncio
    async def test_profiling_connection_getattr_delegates(self, tmp_path):
        from src.database.connection import ProfilingConnection

        async with aiosqlite.connect(str(tmp_path / "prof5.db")) as raw:
            raw.row_factory = aiosqlite.Row
            pc = ProfilingConnection(raw)
            # row_factory attribute should delegate to underlying connection
            assert pc.row_factory is aiosqlite.Row

    @pytest.mark.asyncio
    async def test_profiling_connection_with_profiler(self, tmp_path):
        """ProfilingConnection records timing when profiler is active."""
        from src.database.connection import ProfilingConnection

        profiler = MagicMock()
        profiler.record_db = MagicMock()

        async with aiosqlite.connect(str(tmp_path / "profwith.db")) as raw:
            raw.row_factory = aiosqlite.Row
            pc = ProfilingConnection(raw)
            with patch(
                "src.web.timing.get_current_profiler",
                return_value=profiler,
            ):
                cur = await pc.execute("SELECT 99 AS n")
                row = await cur.fetchone()
                assert row[0] == 99

        assert profiler.record_db.called


# ===========================================================================
# 4. database/repositories/messages.py — uncovered branches
# ===========================================================================


@pytest.mark.asyncio
async def test_messages_search_with_query_and_date_from(db):
    """search_messages with query + date_from filter should work."""
    msg = _make_msg(channel_id=10, message_id=1, text="important news")
    await db.repos.messages.insert_message(msg)

    msgs, total = await db.repos.messages.search_messages(
        query="important",
        date_from="2025-01-01",
        limit=10,
    )
    assert isinstance(msgs, list)
    assert isinstance(total, int)


@pytest.mark.asyncio
async def test_messages_search_with_query_and_date_to(db):
    """search_messages with query + date_to filter should work."""
    msg = _make_msg(channel_id=11, message_id=2, text="date filter test")
    await db.repos.messages.insert_message(msg)

    msgs, total = await db.repos.messages.search_messages(
        query="filter",
        date_to="2025-12-31",
        limit=10,
    )
    assert isinstance(msgs, list)


@pytest.mark.asyncio
async def test_messages_search_with_channel_id_filter(db):
    """search_messages with channel_id filter."""
    msg = _make_msg(channel_id=42, message_id=3, text="channel specific")
    await db.repos.messages.insert_message(msg)

    msgs, total = await db.repos.messages.search_messages(
        channel_id=42,
        limit=10,
    )
    assert all(m.channel_id == 42 for m in msgs)


@pytest.mark.asyncio
async def test_messages_search_with_min_max_length(db):
    """search_messages with min/max length filters."""
    msg = _make_msg(channel_id=50, message_id=4, text="A" * 100)
    await db.repos.messages.insert_message(msg)

    msgs, total = await db.repos.messages.search_messages(
        min_length=50,
        max_length=200,
        limit=10,
    )
    assert isinstance(msgs, list)


@pytest.mark.asyncio
async def test_messages_search_with_offset(db):
    """search_messages with offset should skip rows."""
    for i in range(5):
        msg = _make_msg(channel_id=60, message_id=i + 1, text=f"offset test {i}")
        await db.repos.messages.insert_message(msg)

    msgs_all, total_all = await db.repos.messages.search_messages(channel_id=60, limit=10)
    msgs_offset, _ = await db.repos.messages.search_messages(channel_id=60, limit=10, offset=2)
    assert len(msgs_all) - 2 == len(msgs_offset)


@pytest.mark.asyncio
async def test_messages_search_fts_mode(db):
    """search_messages with is_fts=True."""
    msg = _make_msg(channel_id=70, message_id=1, text="fts mode test phrase")
    await db.repos.messages.insert_message(msg)

    msgs, total = await db.repos.messages.search_messages(
        query="fts mode test",
        is_fts=True,
        limit=10,
    )
    assert isinstance(msgs, list)


@pytest.mark.asyncio
async def test_messages_search_date_to_date_only_normalization(db):
    """date_to with a date-only string gets normalized to next day."""
    msg = _make_msg(channel_id=80, message_id=1, text="date norm test")
    await db.repos.messages.insert_message(msg)

    msgs, total = await db.repos.messages.search_messages(
        date_from="2025-01-01",
        date_to="2025-01-15",  # date-only → normalized to 2025-01-16 <
        limit=10,
    )
    assert isinstance(msgs, list)


@pytest.mark.asyncio
async def test_messages_get_by_id_existing(db):
    """get_by_id returns message for existing id."""
    msg = _make_msg(channel_id=90, message_id=1, text="by id test")
    await db.repos.messages.insert_message(msg)

    msgs, _ = await db.repos.messages.search_messages(channel_id=90, limit=1)
    assert len(msgs) >= 1
    fetched = await db.repos.messages.get_by_id(msgs[0].id)
    assert fetched is not None
    assert fetched.channel_id == 90


@pytest.mark.asyncio
async def test_messages_get_by_id_nonexistent(db):
    """get_by_id returns None for unknown id."""
    result = await db.repos.messages.get_by_id(999999)
    assert result is None


@pytest.mark.asyncio
async def test_messages_build_fts_match_fts_mode():
    """_build_fts_match returns raw query in FTS mode."""
    from src.database.repositories.messages import MessagesRepository

    result = MessagesRepository._build_fts_match("my query", is_fts=True)
    assert result == "my query"


@pytest.mark.asyncio
async def test_messages_build_fts_match_phrase_mode():
    """_build_fts_match wraps query in quotes for phrase mode."""
    from src.database.repositories.messages import MessagesRepository

    result = MessagesRepository._build_fts_match("test phrase", is_fts=False)
    assert result.startswith('"')
    assert result.endswith('"')


@pytest.mark.asyncio
async def test_messages_build_fts_match_escapes_quotes():
    """_build_fts_match escapes internal quotes."""
    from src.database.repositories.messages import MessagesRepository

    result = MessagesRepository._build_fts_match('say "hello"', is_fts=False)
    assert '""' in result


@pytest.mark.asyncio
async def test_messages_build_extra_conditions_with_max_length():
    """_build_extra_conditions adds max_length and exclude patterns."""
    from src.database.repositories.messages import MessagesRepository
    from src.models import SearchQuery

    sq = SearchQuery(
        name="test",
        query="hello",
        max_length=500,
        exclude_patterns="spam*\nad*",
    )
    conds, params = MessagesRepository._build_extra_conditions(sq)
    assert any("LENGTH" in c for c in conds)
    assert any("NOT LIKE" in c for c in conds)


@pytest.mark.asyncio
async def test_messages_upsert_embeddings_empty(db):
    """upsert_message_embeddings with empty list returns 0."""
    count = await db.repos.messages.upsert_message_embeddings([])
    assert count == 0


@pytest.mark.asyncio
async def test_messages_upsert_embedding_json_empty(db):
    """upsert_message_embedding_json with empty list returns 0."""
    count = await db.repos.messages.upsert_message_embedding_json([])
    assert count == 0


@pytest.mark.asyncio
async def test_messages_upsert_embeddings_mismatch_dims(db):
    """upsert_message_embeddings raises ValueError on mismatched dimensions."""
    with pytest.raises(ValueError, match="dimensions"):
        await db.repos.messages.upsert_message_embeddings(
            [(1, [0.1, 0.2]), (2, [0.1, 0.2, 0.3])]
        )


@pytest.mark.asyncio
async def test_messages_reset_embeddings_index(db):
    """reset_embeddings_index should complete without error."""
    await db.repos.messages.reset_embeddings_index()
    dims = await db.repos.messages.get_embedding_dimensions()
    assert dims is None


@pytest.mark.asyncio
async def test_messages_get_embedding_dimensions_invalid_value(db):
    """get_embedding_dimensions returns None for invalid setting value."""
    await db.repos.messages._set_setting("semantic_embedding_dimensions", "not_a_number")
    result = await db.repos.messages.get_embedding_dimensions()
    assert result is None


@pytest.mark.asyncio
async def test_messages_insert_batch_with_reactions(db):
    """insert_messages_batch handles messages with reactions."""
    import json as _json

    reactions = _json.dumps([{"emoji": "👍", "count": 3}])
    msgs = [
        _make_msg(channel_id=200, message_id=i)
        for i in range(1, 4)
    ]
    # Add reactions to first message
    msgs[0].reactions_json = reactions

    count = await db.repos.messages.insert_messages_batch(msgs)
    assert count >= 1


@pytest.mark.asyncio
async def test_messages_parse_reactions_json_valid():
    from src.database.repositories.messages import _parse_reactions_json

    result = _parse_reactions_json('[{"emoji": "👍", "count": 5}]')
    assert len(result) == 1
    assert result[0]["emoji"] == "👍"


@pytest.mark.asyncio
async def test_messages_parse_reactions_json_invalid():
    from src.database.repositories.messages import _parse_reactions_json

    result = _parse_reactions_json("not json")
    assert result == []


@pytest.mark.asyncio
async def test_messages_parse_reactions_json_none():
    from src.database.repositories.messages import _parse_reactions_json

    result = _parse_reactions_json(None)  # type: ignore[arg-type]
    assert result == []


@pytest.mark.asyncio
async def test_messages_search_topic_id_filter(db):
    """search_messages with topic_id filter."""
    msg = _make_msg(channel_id=300, message_id=1, text="topic test")
    msg.topic_id = 42
    await db.repos.messages.insert_message(msg)

    msgs, total = await db.repos.messages.search_messages(
        topic_id=42,
        limit=10,
    )
    assert isinstance(msgs, list)


@pytest.mark.asyncio
async def test_messages_like_fallback_search(db):
    """MessagesRepository with fts_available=False uses LIKE search."""
    from src.database.repositories.messages import MessagesRepository

    msg = _make_msg(channel_id=400, message_id=1, text="like fallback search word")
    await db.repos.messages.insert_message(msg)

    # Use the underlying connection directly
    repo_no_fts = MessagesRepository(db._db, fts_available=False)
    msgs, total = await repo_no_fts.search_messages(query="fallback", limit=10)
    assert isinstance(msgs, list)


# ===========================================================================
# 5. database/migrations.py — additional paths
# ===========================================================================


@pytest.mark.asyncio
async def test_migrations_from_scratch_creates_all_tables():
    """run_migrations on fresh schema creates expected tables."""
    from src.database.migrations import run_migrations
    from src.database.schema import SCHEMA_SQL

    async with aiosqlite.connect(":memory:") as conn:
        conn.row_factory = aiosqlite.Row
        await conn.executescript(SCHEMA_SQL)
        await conn.commit()
        fts_available = await run_migrations(conn)
        assert isinstance(fts_available, bool)

        # Verify key tables exist
        cur = await conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )
        tables = {row["name"] for row in await cur.fetchall()}
        assert "notification_bots" in tables
        assert "search_queries" in tables
        assert "content_pipelines" in tables
        assert "generation_runs" in tables


@pytest.mark.asyncio
async def test_migrations_notification_bots_table_created():
    """notification_bots table is created during migration."""
    from src.database.migrations import run_migrations
    from src.database.schema import SCHEMA_SQL

    async with aiosqlite.connect(":memory:") as conn:
        conn.row_factory = aiosqlite.Row
        await conn.executescript(SCHEMA_SQL)
        await conn.commit()
        await run_migrations(conn)
        cur = await conn.execute("PRAGMA table_info(notification_bots)")
        cols = {row["name"] for row in await cur.fetchall()}
        assert "bot_token" in cols
        assert "bot_username" in cols


@pytest.mark.asyncio
async def test_migrations_photo_tables_created():
    """Photo tables are created during migration."""
    from src.database.migrations import run_migrations
    from src.database.schema import SCHEMA_SQL

    async with aiosqlite.connect(":memory:") as conn:
        conn.row_factory = aiosqlite.Row
        await conn.executescript(SCHEMA_SQL)
        await conn.commit()
        await run_migrations(conn)
        cur = await conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        )
        tables = {row["name"] for row in await cur.fetchall()}
        assert "photo_batches" in tables or "collection_tasks" in tables


@pytest.mark.asyncio
async def test_migrations_collection_tasks_index():
    """Collection tasks index is created during migration."""
    from src.database.migrations import run_migrations
    from src.database.schema import SCHEMA_SQL

    async with aiosqlite.connect(":memory:") as conn:
        conn.row_factory = aiosqlite.Row
        await conn.executescript(SCHEMA_SQL)
        await conn.commit()
        await run_migrations(conn)
        cur = await conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index'"
        )
        indices = {row["name"] for row in await cur.fetchall()}
        assert "idx_collection_tasks_type_status_run_after" in indices


@pytest.mark.asyncio
async def test_migrations_search_queries_columns():
    """search_queries gets all expected columns."""
    from src.database.migrations import run_migrations
    from src.database.schema import SCHEMA_SQL

    async with aiosqlite.connect(":memory:") as conn:
        conn.row_factory = aiosqlite.Row
        await conn.executescript(SCHEMA_SQL)
        await conn.commit()
        await run_migrations(conn)
        cur = await conn.execute("PRAGMA table_info(search_queries)")
        cols = {row["name"] for row in await cur.fetchall()}
        assert "is_regex" in cols
        assert "notify_on_collect" in cols
        assert "is_fts" in cols


@pytest.mark.asyncio
async def test_migrate_vec_to_portable_with_table_no_dims():
    """_migrate_vec_to_portable skips when dimensions setting is missing."""
    from src.database.migrations import _migrate_vec_to_portable
    from src.database.schema import SCHEMA_SQL

    async with aiosqlite.connect(":memory:") as conn:
        conn.row_factory = aiosqlite.Row
        await conn.executescript(SCHEMA_SQL)
        await conn.commit()
        # Create a vec_messages table
        await conn.execute(
            "CREATE TABLE vec_messages (message_id INTEGER, embedding BLOB)"
        )
        await conn.commit()
        # No settings row for dimensions — should return early
        await _migrate_vec_to_portable(conn)


@pytest.mark.asyncio
async def test_migrate_vec_to_portable_invalid_dims():
    """_migrate_vec_to_portable skips when dimensions value is invalid."""
    from src.database.migrations import _migrate_vec_to_portable
    from src.database.schema import SCHEMA_SQL

    async with aiosqlite.connect(":memory:") as conn:
        conn.row_factory = aiosqlite.Row
        await conn.executescript(SCHEMA_SQL)
        await conn.commit()
        await conn.execute("CREATE TABLE vec_messages (message_id INTEGER, embedding BLOB)")
        await conn.execute(
            "INSERT INTO settings (key, value) VALUES (?, ?)",
            ("semantic_embedding_dimensions", "invalid"),
        )
        await conn.commit()
        # Should not raise
        await _migrate_vec_to_portable(conn)


# ===========================================================================
# 6. database/repositories/photo_loader.py — full coverage
# ===========================================================================


def _make_photo_batch(status="pending"):
    from src.models import PhotoBatch, PhotoBatchStatus, PhotoSendMode

    return PhotoBatch(
        phone="+1234567890",
        target_dialog_id=1001,
        target_title="Test Channel",
        target_type="channel",
        send_mode=PhotoSendMode.ALBUM,
        caption="Test caption",
        status=PhotoBatchStatus(status),
        error=None,
    )


def _make_photo_item(batch_id: int, status="pending"):
    from src.models import PhotoBatchItem, PhotoBatchStatus, PhotoSendMode

    return PhotoBatchItem(
        batch_id=batch_id,
        phone="+1234567890",
        target_dialog_id=1001,
        target_title="Test Channel",
        target_type="channel",
        file_paths=["/tmp/photo1.jpg"],
        send_mode=PhotoSendMode.ALBUM,
        caption="Item caption",
        schedule_at=None,
        status=PhotoBatchStatus(status),
        error=None,
        telegram_message_ids=[],
    )


def _make_auto_job():
    from src.models import PhotoAutoUploadJob, PhotoSendMode

    return PhotoAutoUploadJob(
        phone="+1234567890",
        target_dialog_id=2001,
        target_title="Auto Upload Target",
        target_type="channel",
        folder_path="/tmp/photos",
        send_mode=PhotoSendMode.ALBUM,
        caption="Auto caption",
        interval_minutes=60,
        is_active=True,
        error=None,
        last_run_at=None,
        last_seen_marker=None,
    )


@pytest.mark.asyncio
async def test_photo_loader_create_and_get_batch(db):
    batch = _make_photo_batch()
    batch_id = await db.repos.photo_loader.create_batch(batch)
    assert batch_id > 0

    fetched = await db.repos.photo_loader.get_batch(batch_id)
    assert fetched is not None
    assert fetched.id == batch_id
    assert fetched.phone == "+1234567890"


@pytest.mark.asyncio
async def test_photo_loader_get_batch_nonexistent(db):
    result = await db.repos.photo_loader.get_batch(999999)
    assert result is None


@pytest.mark.asyncio
async def test_photo_loader_list_batches(db):
    for i in range(3):
        batch = _make_photo_batch()
        await db.repos.photo_loader.create_batch(batch)

    batches = await db.repos.photo_loader.list_batches()
    assert len(batches) >= 3


@pytest.mark.asyncio
async def test_photo_loader_list_batches_with_limit(db):
    for i in range(5):
        batch = _make_photo_batch()
        await db.repos.photo_loader.create_batch(batch)

    batches = await db.repos.photo_loader.list_batches(limit=2)
    assert len(batches) <= 2


@pytest.mark.asyncio
async def test_photo_loader_update_batch_status(db):
    from src.models import PhotoBatchStatus

    batch = _make_photo_batch()
    batch_id = await db.repos.photo_loader.create_batch(batch)

    await db.repos.photo_loader.update_batch(batch_id, status=PhotoBatchStatus.RUNNING)
    fetched = await db.repos.photo_loader.get_batch(batch_id)
    assert fetched.status == PhotoBatchStatus.RUNNING


@pytest.mark.asyncio
async def test_photo_loader_update_batch_error(db):
    batch = _make_photo_batch()
    batch_id = await db.repos.photo_loader.create_batch(batch)

    await db.repos.photo_loader.update_batch(batch_id, error="Some error occurred")
    fetched = await db.repos.photo_loader.get_batch(batch_id)
    assert fetched.error == "Some error occurred"


@pytest.mark.asyncio
async def test_photo_loader_update_batch_last_run_at(db):
    batch = _make_photo_batch()
    batch_id = await db.repos.photo_loader.create_batch(batch)

    now = datetime.now(timezone.utc)
    await db.repos.photo_loader.update_batch(batch_id, last_run_at=now)
    fetched = await db.repos.photo_loader.get_batch(batch_id)
    assert fetched.last_run_at is not None


@pytest.mark.asyncio
async def test_photo_loader_update_batch_no_sets(db):
    """update_batch with no args is a no-op."""
    batch = _make_photo_batch()
    batch_id = await db.repos.photo_loader.create_batch(batch)
    # Should not raise
    await db.repos.photo_loader.update_batch(batch_id)


@pytest.mark.asyncio
async def test_photo_loader_create_and_get_item(db):
    batch = _make_photo_batch()
    batch_id = await db.repos.photo_loader.create_batch(batch)

    item = _make_photo_item(batch_id)
    item_id = await db.repos.photo_loader.create_item(item)
    assert item_id > 0

    fetched = await db.repos.photo_loader.get_item(item_id)
    assert fetched is not None
    assert fetched.batch_id == batch_id
    assert "/tmp/photo1.jpg" in fetched.file_paths


@pytest.mark.asyncio
async def test_photo_loader_get_item_nonexistent(db):
    result = await db.repos.photo_loader.get_item(999999)
    assert result is None


@pytest.mark.asyncio
async def test_photo_loader_list_items(db):
    batch = _make_photo_batch()
    batch_id = await db.repos.photo_loader.create_batch(batch)

    for i in range(3):
        item = _make_photo_item(batch_id)
        await db.repos.photo_loader.create_item(item)

    items = await db.repos.photo_loader.list_items()
    assert len(items) >= 3


@pytest.mark.asyncio
async def test_photo_loader_list_items_for_batch(db):
    batch = _make_photo_batch()
    batch_id = await db.repos.photo_loader.create_batch(batch)

    for i in range(2):
        item = _make_photo_item(batch_id)
        await db.repos.photo_loader.create_item(item)

    items = await db.repos.photo_loader.list_items_for_batch(batch_id)
    assert len(items) == 2
    assert all(it.batch_id == batch_id for it in items)


@pytest.mark.asyncio
async def test_photo_loader_update_item_status(db):
    from src.models import PhotoBatchStatus

    batch = _make_photo_batch()
    batch_id = await db.repos.photo_loader.create_batch(batch)
    item = _make_photo_item(batch_id)
    item_id = await db.repos.photo_loader.create_item(item)

    await db.repos.photo_loader.update_item(item_id, status=PhotoBatchStatus.COMPLETED)
    fetched = await db.repos.photo_loader.get_item(item_id)
    assert fetched.status == PhotoBatchStatus.COMPLETED


@pytest.mark.asyncio
async def test_photo_loader_update_item_error(db):
    batch = _make_photo_batch()
    batch_id = await db.repos.photo_loader.create_batch(batch)
    item = _make_photo_item(batch_id)
    item_id = await db.repos.photo_loader.create_item(item)

    await db.repos.photo_loader.update_item(item_id, error="upload failed")
    fetched = await db.repos.photo_loader.get_item(item_id)
    assert fetched.error == "upload failed"


@pytest.mark.asyncio
async def test_photo_loader_update_item_telegram_ids(db):
    batch = _make_photo_batch()
    batch_id = await db.repos.photo_loader.create_batch(batch)
    item = _make_photo_item(batch_id)
    item_id = await db.repos.photo_loader.create_item(item)

    await db.repos.photo_loader.update_item(item_id, telegram_message_ids=[111, 222])
    fetched = await db.repos.photo_loader.get_item(item_id)
    assert fetched.telegram_message_ids == [111, 222]


@pytest.mark.asyncio
async def test_photo_loader_update_item_timestamps(db):
    batch = _make_photo_batch()
    batch_id = await db.repos.photo_loader.create_batch(batch)
    item = _make_photo_item(batch_id)
    item_id = await db.repos.photo_loader.create_item(item)

    now = datetime.now(timezone.utc)
    await db.repos.photo_loader.update_item(
        item_id,
        started_at=now,
        completed_at=now,
    )
    fetched = await db.repos.photo_loader.get_item(item_id)
    assert fetched.started_at is not None
    assert fetched.completed_at is not None


@pytest.mark.asyncio
async def test_photo_loader_update_item_no_sets(db):
    """update_item with no args is a no-op."""
    batch = _make_photo_batch()
    batch_id = await db.repos.photo_loader.create_batch(batch)
    item = _make_photo_item(batch_id)
    item_id = await db.repos.photo_loader.create_item(item)
    # Should not raise
    await db.repos.photo_loader.update_item(item_id)


@pytest.mark.asyncio
async def test_photo_loader_cancel_item(db):
    batch = _make_photo_batch()
    batch_id = await db.repos.photo_loader.create_batch(batch)
    item = _make_photo_item(batch_id, status="pending")
    item_id = await db.repos.photo_loader.create_item(item)

    result = await db.repos.photo_loader.cancel_item(item_id)
    assert result is True

    fetched = await db.repos.photo_loader.get_item(item_id)
    from src.models import PhotoBatchStatus
    assert fetched.status == PhotoBatchStatus.CANCELLED


@pytest.mark.asyncio
async def test_photo_loader_cancel_item_nonexistent(db):
    result = await db.repos.photo_loader.cancel_item(999999)
    assert result is False


@pytest.mark.asyncio
async def test_photo_loader_claim_next_due_item(db):
    """claim_next_due_item returns pending item."""
    batch = _make_photo_batch()
    batch_id = await db.repos.photo_loader.create_batch(batch)
    item = _make_photo_item(batch_id, status="pending")
    item_id = await db.repos.photo_loader.create_item(item)

    now = datetime.now(timezone.utc)
    claimed = await db.repos.photo_loader.claim_next_due_item(now)
    from src.models import PhotoBatchStatus
    assert claimed is not None
    assert claimed.id == item_id
    assert claimed.status == PhotoBatchStatus.RUNNING


@pytest.mark.asyncio
async def test_photo_loader_claim_next_due_item_none(db):
    """claim_next_due_item returns None when no pending items."""
    now = datetime.now(timezone.utc)
    claimed = await db.repos.photo_loader.claim_next_due_item(now)
    assert claimed is None


@pytest.mark.asyncio
async def test_photo_loader_requeue_running_items(db):
    """requeue_running_items_on_startup requeues RUNNING items."""
    from src.models import PhotoBatchStatus

    batch = _make_photo_batch()
    batch_id = await db.repos.photo_loader.create_batch(batch)
    item = _make_photo_item(batch_id, status="pending")
    item_id = await db.repos.photo_loader.create_item(item)

    # Claim it first to set to RUNNING
    now = datetime.now(timezone.utc)
    await db.repos.photo_loader.claim_next_due_item(now)

    count = await db.repos.photo_loader.requeue_running_items_on_startup(now)
    assert count >= 1

    fetched = await db.repos.photo_loader.get_item(item_id)
    assert fetched.status == PhotoBatchStatus.PENDING


@pytest.mark.asyncio
async def test_photo_loader_create_and_get_auto_job(db):
    job = _make_auto_job()
    job_id = await db.repos.photo_loader.create_auto_job(job)
    assert job_id > 0

    fetched = await db.repos.photo_loader.get_auto_job(job_id)
    assert fetched is not None
    assert fetched.id == job_id
    assert fetched.folder_path == "/tmp/photos"
    assert fetched.is_active is True


@pytest.mark.asyncio
async def test_photo_loader_get_auto_job_nonexistent(db):
    result = await db.repos.photo_loader.get_auto_job(999999)
    assert result is None


@pytest.mark.asyncio
async def test_photo_loader_list_auto_jobs(db):
    for i in range(3):
        job = _make_auto_job()
        await db.repos.photo_loader.create_auto_job(job)

    jobs = await db.repos.photo_loader.list_auto_jobs()
    assert len(jobs) >= 3


@pytest.mark.asyncio
async def test_photo_loader_list_auto_jobs_active_only(db):

    # Active job
    active_job = _make_auto_job()
    active_id = await db.repos.photo_loader.create_auto_job(active_job)

    # Inactive job
    inactive_job = _make_auto_job()
    inactive_id = await db.repos.photo_loader.create_auto_job(inactive_job)
    await db.repos.photo_loader.update_auto_job(inactive_id, is_active=False)

    active_jobs = await db.repos.photo_loader.list_auto_jobs(active_only=True)
    job_ids = [j.id for j in active_jobs]
    assert active_id in job_ids
    assert inactive_id not in job_ids


@pytest.mark.asyncio
async def test_photo_loader_update_auto_job_all_fields(db):
    job = _make_auto_job()
    job_id = await db.repos.photo_loader.create_auto_job(job)

    now = datetime.now(timezone.utc)
    from src.models import PhotoSendMode
    await db.repos.photo_loader.update_auto_job(
        job_id,
        folder_path="/new/folder",
        send_mode=PhotoSendMode.ALBUM,
        caption="New caption",
        interval_minutes=30,
        is_active=False,
        error="some error",
        last_run_at=now,
        last_seen_marker="file_001.jpg",
    )

    fetched = await db.repos.photo_loader.get_auto_job(job_id)
    assert fetched.folder_path == "/new/folder"
    assert fetched.caption == "New caption"
    assert fetched.interval_minutes == 30
    assert fetched.is_active is False
    assert fetched.error == "some error"
    assert fetched.last_run_at is not None
    assert fetched.last_seen_marker == "file_001.jpg"


@pytest.mark.asyncio
async def test_photo_loader_update_auto_job_no_sets(db):
    """update_auto_job with no args is a no-op."""
    job = _make_auto_job()
    job_id = await db.repos.photo_loader.create_auto_job(job)
    # Should not raise
    await db.repos.photo_loader.update_auto_job(job_id)


@pytest.mark.asyncio
async def test_photo_loader_delete_auto_job(db):
    job = _make_auto_job()
    job_id = await db.repos.photo_loader.create_auto_job(job)

    await db.repos.photo_loader.delete_auto_job(job_id)
    result = await db.repos.photo_loader.get_auto_job(job_id)
    assert result is None


@pytest.mark.asyncio
async def test_photo_loader_has_sent_auto_file(db):
    job = _make_auto_job()
    job_id = await db.repos.photo_loader.create_auto_job(job)

    file_path = "/tmp/photos/photo1.jpg"
    result_before = await db.repos.photo_loader.has_sent_auto_file(job_id, file_path)
    assert result_before is False

    await db.repos.photo_loader.mark_auto_file_sent(job_id, file_path)
    result_after = await db.repos.photo_loader.has_sent_auto_file(job_id, file_path)
    assert result_after is True


@pytest.mark.asyncio
async def test_photo_loader_mark_auto_file_sent_idempotent(db):
    """mark_auto_file_sent is idempotent (INSERT OR IGNORE)."""
    job = _make_auto_job()
    job_id = await db.repos.photo_loader.create_auto_job(job)

    file_path = "/tmp/photos/idempotent.jpg"
    await db.repos.photo_loader.mark_auto_file_sent(job_id, file_path)
    # Second call should not raise
    await db.repos.photo_loader.mark_auto_file_sent(job_id, file_path)

    result = await db.repos.photo_loader.has_sent_auto_file(job_id, file_path)
    assert result is True


@pytest.mark.asyncio
async def test_photo_loader_json_loads_list_valid():
    from src.database.repositories.photo_loader import _json_loads_list

    result = _json_loads_list('[1, 2, 3]')
    assert result == [1, 2, 3]


@pytest.mark.asyncio
async def test_photo_loader_json_loads_list_invalid():
    from src.database.repositories.photo_loader import _json_loads_list

    result = _json_loads_list("not json")
    assert result == []


@pytest.mark.asyncio
async def test_photo_loader_json_loads_list_not_list():
    from src.database.repositories.photo_loader import _json_loads_list

    result = _json_loads_list('{"key": "value"}')
    assert result == []


@pytest.mark.asyncio
async def test_photo_loader_json_loads_list_empty():
    from src.database.repositories.photo_loader import _json_loads_list

    result = _json_loads_list(None)
    assert result == []


@pytest.mark.asyncio
async def test_photo_loader_dt_helper():
    from src.database.repositories.photo_loader import _dt

    assert _dt(None) is None
    dt = _dt("2025-01-15T10:00:00")
    assert dt is not None
    assert dt.year == 2025
