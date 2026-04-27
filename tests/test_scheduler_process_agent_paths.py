"""Tests for scheduler, CLI commands, process control, database connection, and agent manager paths."""

from __future__ import annotations

import os
import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.config import AppConfig, SchedulerConfig
from src.models import SearchQuery
from src.scheduler.service import SchedulerManager
from tests.helpers import cli_ns as _ns

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_mock_bundle(setting_val=None):
    bundle = MagicMock()
    bundle.get_setting = AsyncMock(return_value=setting_val)
    bundle.set_setting = AsyncMock()
    return bundle


def _make_mock_sq_bundle(queries=None):
    bundle = MagicMock()
    bundle.get_all = AsyncMock(return_value=queries or [])
    return bundle


def _make_mock_pipeline_bundle(pipelines=None):
    bundle = MagicMock()
    bundle.get_all = AsyncMock(return_value=pipelines or [])
    return bundle


def _make_task_enqueuer():
    enqueuer = MagicMock()
    result = MagicMock()
    result.queued_count = 2
    result.skipped_existing_count = 0
    result.total_candidates = 2
    enqueuer.enqueue_all_channels = AsyncMock(return_value=result)
    enqueuer.enqueue_sq_stats = AsyncMock()
    enqueuer.enqueue_photo_due = AsyncMock()
    enqueuer.enqueue_photo_auto = AsyncMock()
    enqueuer.enqueue_pipeline_run = AsyncMock()
    enqueuer.enqueue_content_generate = AsyncMock()
    return enqueuer


async def _make_mgr(db=None, *, config=None, task_enqueuer=None, sq_bundle=None, pipeline_bundle=None):
    """Create a SchedulerManager with mock bundles (no real DB required)."""
    scheduler_bundle = _make_mock_bundle()
    sq_bundle = sq_bundle or _make_mock_sq_bundle()
    pipeline_bundle = pipeline_bundle or _make_mock_pipeline_bundle()
    cfg = config or SchedulerConfig(collect_interval_minutes=30)
    mgr = SchedulerManager(
        cfg,
        scheduler_bundle=scheduler_bundle,
        search_query_bundle=sq_bundle,
        pipeline_bundle=pipeline_bundle,
        task_enqueuer=task_enqueuer,
    )
    return mgr


# ===========================================================================
# 1. scheduler/manager.py
# ===========================================================================


class TestSchedulerUpdateInterval:
    @pytest.mark.asyncio
    async def test_update_interval_while_running(self):
        mgr = await _make_mgr()
        await mgr.start()
        try:
            mgr.update_interval(15)
            assert mgr._current_interval_minutes == 15
        finally:
            await mgr.stop()

    @pytest.mark.asyncio
    async def test_update_interval_stores_when_job_disabled(self):
        """update_interval stores value when collect_all job is not registered."""
        mock_bundle = _make_mock_bundle("1")  # job disabled
        mgr = SchedulerManager(
            SchedulerConfig(collect_interval_minutes=60),
            scheduler_bundle=mock_bundle,
        )
        await mgr.start()
        try:
            # Even if job was not registered (disabled), interval is stored
            mgr.update_interval(20)
            assert mgr._current_interval_minutes == 20
        finally:
            await mgr.stop()

    @pytest.mark.asyncio
    async def test_update_interval_when_not_running(self):
        mgr = await _make_mgr()
        mgr.update_interval(45)
        assert mgr._current_interval_minutes == 45


class TestSchedulerGetJobNextRun:
    @pytest.mark.asyncio
    async def test_get_job_next_run_scheduler_none(self):
        mgr = await _make_mgr()
        assert mgr.get_job_next_run("collect_all") is None

    @pytest.mark.asyncio
    async def test_get_job_next_run_existing_job(self):
        mgr = await _make_mgr()
        await mgr.start()
        try:
            result = mgr.get_job_next_run("collect_all")
            assert result is not None
        finally:
            await mgr.stop()

    @pytest.mark.asyncio
    async def test_get_job_next_run_nonexistent_job(self):
        mgr = await _make_mgr()
        await mgr.start()
        try:
            result = mgr.get_job_next_run("nonexistent_job_xyz")
            assert result is None
        finally:
            await mgr.stop()


class TestSchedulerGetAllJobsNextRun:
    @pytest.mark.asyncio
    async def test_returns_empty_when_scheduler_none(self):
        mgr = await _make_mgr()
        result = mgr.get_all_jobs_next_run()
        assert result == {}

    @pytest.mark.asyncio
    async def test_returns_dict_when_running(self):
        mgr = await _make_mgr()
        await mgr.start()
        try:
            result = mgr.get_all_jobs_next_run()
            assert isinstance(result, dict)
            assert "collect_all" in result
        finally:
            await mgr.stop()

    @pytest.mark.asyncio
    async def test_ttl_cache_returns_same_object_within_5s(self):
        mgr = await _make_mgr()
        await mgr.start()
        try:
            first = mgr.get_all_jobs_next_run()
            second = mgr.get_all_jobs_next_run()
            # Cache should return same object if called quickly
            assert first is second
        finally:
            await mgr.stop()

    @pytest.mark.asyncio
    async def test_ttl_cache_refreshes_after_expiry(self):
        mgr = await _make_mgr()
        await mgr.start()
        try:
            mgr.get_all_jobs_next_run()
            # Force cache expiry
            mgr._jobs_cache_ts = time.monotonic() - 10.0
            second = mgr.get_all_jobs_next_run()
            # After expiry, a new dict is built (may be equal in content but is refreshed)
            assert isinstance(second, dict)
        finally:
            await mgr.stop()


class TestSchedulerSyncSearchQueryJobs:
    @pytest.mark.asyncio
    async def test_sync_sq_jobs_adds_job_for_active_query(self):
        sq = SearchQuery(id=5, name="test", query="test", track_stats=True, interval_minutes=15)
        sq_bundle = _make_mock_sq_bundle([sq])
        mgr = await _make_mgr(sq_bundle=sq_bundle)
        await mgr.start()
        try:
            await mgr.sync_search_query_jobs()
            jobs = mgr._scheduler.get_jobs()
            assert any(j.id == "sq_5" for j in jobs)
        finally:
            await mgr.stop()

    @pytest.mark.asyncio
    async def test_sync_sq_jobs_skips_non_track_stats(self):
        sq = SearchQuery(id=6, name="no_track", query="no_track", track_stats=False, interval_minutes=15)
        sq_bundle = _make_mock_sq_bundle([sq])
        mgr = await _make_mgr(sq_bundle=sq_bundle)
        await mgr.start()
        try:
            await mgr.sync_search_query_jobs()
            jobs = mgr._scheduler.get_jobs()
            assert not any(j.id == "sq_6" for j in jobs)
        finally:
            await mgr.stop()

    @pytest.mark.asyncio
    async def test_sync_sq_jobs_removes_stale_job(self):
        sq = SearchQuery(id=7, name="q7", query="q7", track_stats=True, interval_minutes=10)
        sq_bundle = _make_mock_sq_bundle([sq])
        mgr = await _make_mgr(sq_bundle=sq_bundle)
        await mgr.start()
        try:
            await mgr.sync_search_query_jobs()
            assert any(j.id == "sq_7" for j in mgr._scheduler.get_jobs())

            sq_bundle.get_all.return_value = []
            await mgr.sync_search_query_jobs()
            assert not any(j.id == "sq_7" for j in mgr._scheduler.get_jobs())
        finally:
            await mgr.stop()

    @pytest.mark.asyncio
    async def test_sync_sq_jobs_no_bundle(self):
        """sync_search_query_jobs should return immediately when bundle is None."""
        mgr = SchedulerManager(SchedulerConfig(), scheduler_bundle=_make_mock_bundle())
        await mgr.start()
        try:
            await mgr.sync_search_query_jobs()  # no exception
        finally:
            await mgr.stop()


class TestSchedulerSyncPipelineJobs:
    @pytest.mark.asyncio
    async def test_sync_pipeline_jobs_adds_jobs(self):
        pipeline = MagicMock()
        pipeline.id = 10
        pipeline.is_active = True
        pipeline.generate_interval_minutes = 60
        pl_bundle = _make_mock_pipeline_bundle([pipeline])
        mgr = await _make_mgr(pipeline_bundle=pl_bundle)
        await mgr.start()
        try:
            await mgr.sync_pipeline_jobs()
            jobs = mgr._scheduler.get_jobs()
            job_ids = {j.id for j in jobs}
            assert "pipeline_run_10" in job_ids
            assert "content_generate_10" in job_ids
        finally:
            await mgr.stop()

    @pytest.mark.asyncio
    async def test_sync_pipeline_jobs_removes_stale(self):
        pipeline = MagicMock()
        pipeline.id = 11
        pipeline.is_active = True
        pipeline.generate_interval_minutes = 30
        pl_bundle = _make_mock_pipeline_bundle([pipeline])
        mgr = await _make_mgr(pipeline_bundle=pl_bundle)
        await mgr.start()
        try:
            await mgr.sync_pipeline_jobs()
            assert any(j.id == "pipeline_run_11" for j in mgr._scheduler.get_jobs())

            pl_bundle.get_all.return_value = []
            await mgr.sync_pipeline_jobs()
            assert not any(j.id == "pipeline_run_11" for j in mgr._scheduler.get_jobs())
        finally:
            await mgr.stop()

    @pytest.mark.asyncio
    async def test_sync_pipeline_jobs_no_bundle(self):
        mgr = SchedulerManager(SchedulerConfig(), scheduler_bundle=_make_mock_bundle())
        await mgr.start()
        try:
            await mgr.sync_pipeline_jobs()  # no exception
        finally:
            await mgr.stop()


class TestSchedulerGetPotentialJobs:
    @pytest.mark.asyncio
    async def test_get_potential_jobs_basic(self):
        mgr = await _make_mgr()
        jobs = await mgr.get_potential_jobs()
        assert any(j["job_id"] == "collect_all" for j in jobs)

    @pytest.mark.asyncio
    async def test_get_potential_jobs_with_task_enqueuer(self):
        enqueuer = _make_task_enqueuer()
        mgr = await _make_mgr(task_enqueuer=enqueuer)
        jobs = await mgr.get_potential_jobs()
        job_ids = {j["job_id"] for j in jobs}
        assert "photo_due" in job_ids
        assert "photo_auto" in job_ids

    @pytest.mark.asyncio
    async def test_get_potential_jobs_with_sq_bundle(self):
        sq = SearchQuery(id=20, name="sq20", query="sq20", track_stats=True, interval_minutes=10)
        sq_bundle = _make_mock_sq_bundle([sq])
        mgr = await _make_mgr(sq_bundle=sq_bundle)
        jobs = await mgr.get_potential_jobs()
        assert any(j["job_id"] == "sq_20" for j in jobs)

    @pytest.mark.asyncio
    async def test_get_potential_jobs_with_pipeline_bundle(self):
        pipeline = MagicMock()
        pipeline.id = 99
        pipeline.is_active = True
        pipeline.generate_interval_minutes = 60
        pl_bundle = _make_mock_pipeline_bundle([pipeline])
        mgr = await _make_mgr(pipeline_bundle=pl_bundle)
        jobs = await mgr.get_potential_jobs()
        job_ids = {j["job_id"] for j in jobs}
        assert "pipeline_run_99" in job_ids
        assert "content_generate_99" in job_ids


class TestSchedulerLoadSettings:
    @pytest.mark.asyncio
    async def test_load_settings_reads_interval(self):
        mock_bundle = _make_mock_bundle("25")
        mgr = SchedulerManager(SchedulerConfig(collect_interval_minutes=60), scheduler_bundle=mock_bundle)
        await mgr.load_settings()
        assert mgr._current_interval_minutes == 25

    @pytest.mark.asyncio
    async def test_load_settings_uses_default_when_no_bundle(self):
        mgr = SchedulerManager(SchedulerConfig(collect_interval_minutes=60))
        await mgr.load_settings()
        assert mgr._current_interval_minutes == 60

    @pytest.mark.asyncio
    async def test_load_settings_invalid_value_uses_default(self):
        mock_bundle = _make_mock_bundle("not_a_number")
        mgr = SchedulerManager(SchedulerConfig(collect_interval_minutes=45), scheduler_bundle=mock_bundle)
        await mgr.load_settings()
        assert mgr._current_interval_minutes == 45


class TestSchedulerIsJobEnabled:
    @pytest.mark.asyncio
    async def test_is_job_enabled_returns_true_when_no_bundle(self):
        mgr = SchedulerManager(SchedulerConfig())
        assert await mgr.is_job_enabled("collect_all") is True

    @pytest.mark.asyncio
    async def test_is_job_enabled_returns_true_by_default(self):
        mock_bundle = _make_mock_bundle(None)
        mgr = SchedulerManager(SchedulerConfig(), scheduler_bundle=mock_bundle)
        assert await mgr.is_job_enabled("collect_all") is True

    @pytest.mark.asyncio
    async def test_is_job_enabled_returns_false_when_disabled(self):
        mock_bundle = _make_mock_bundle("1")
        mgr = SchedulerManager(SchedulerConfig(), scheduler_bundle=mock_bundle)
        assert await mgr.is_job_enabled("collect_all") is False

    @pytest.mark.asyncio
    async def test_is_job_enabled_returns_true_when_not_one(self):
        mock_bundle = _make_mock_bundle("0")
        mgr = SchedulerManager(SchedulerConfig(), scheduler_bundle=mock_bundle)
        assert await mgr.is_job_enabled("collect_all") is True


class TestSchedulerRunPhotoDue:
    @pytest.mark.asyncio
    async def test_run_photo_due_no_enqueuer(self):
        mgr = await _make_mgr()
        result = await mgr._run_photo_due()
        assert result == {"processed": 0}

    @pytest.mark.asyncio
    async def test_run_photo_due_with_enqueuer(self):
        enqueuer = _make_task_enqueuer()
        mgr = await _make_mgr(task_enqueuer=enqueuer)
        result = await mgr._run_photo_due()
        assert result == {"enqueued": True}
        enqueuer.enqueue_photo_due.assert_called_once()

    @pytest.mark.asyncio
    async def test_run_photo_auto_no_enqueuer(self):
        mgr = await _make_mgr()
        result = await mgr._run_photo_auto()
        assert result == {"jobs": 0}

    @pytest.mark.asyncio
    async def test_run_photo_auto_with_enqueuer(self):
        enqueuer = _make_task_enqueuer()
        mgr = await _make_mgr(task_enqueuer=enqueuer)
        result = await mgr._run_photo_auto()
        assert result == {"enqueued": True}
        enqueuer.enqueue_photo_auto.assert_called_once()


# ===========================================================================
# 2. cli/commands/notification.py
# ===========================================================================


class TestCLINotification:
    def test_notification_status_none(self, cli_env, capsys):
        from src.cli.commands.notification import run

        fake_pool = AsyncMock()
        fake_pool.clients = {}
        fake_pool.disconnect_all = AsyncMock()

        async def fake_init_pool(config, db):
            from src.telegram.auth import TelegramAuth
            return TelegramAuth(0, ""), fake_pool

        with patch("src.cli.runtime.init_pool", side_effect=fake_init_pool):
            with patch(
                "src.services.notification_service.NotificationService.get_status",
                new_callable=AsyncMock,
                return_value=None,
            ):
                run(_ns(notification_action="status"))
        out = capsys.readouterr().out
        assert "No notification bot configured" in out

    def test_notification_status_with_bot(self, cli_env, capsys):
        from datetime import datetime, timezone

        from src.cli.commands.notification import run
        from src.models import NotificationBot

        fake_pool = AsyncMock()
        fake_pool.clients = {}
        fake_pool.disconnect_all = AsyncMock()

        bot = NotificationBot(
            tg_user_id=111,
            bot_id=123456,
            bot_username="testbot",
            bot_token="token123",
            created_at=datetime(2025, 1, 1, tzinfo=timezone.utc),
        )

        async def fake_init_pool(config, db):
            from src.telegram.auth import TelegramAuth
            return TelegramAuth(0, ""), fake_pool

        with patch("src.cli.runtime.init_pool", side_effect=fake_init_pool):
            with patch(
                "src.services.notification_service.NotificationService.get_status",
                new_callable=AsyncMock,
                return_value=bot,
            ):
                run(_ns(notification_action="status"))
        out = capsys.readouterr().out
        assert "testbot" in out
        assert "123456" in out

    def test_notification_delete(self, cli_env, capsys):
        from src.cli.commands.notification import run

        fake_pool = AsyncMock()
        fake_pool.clients = {}
        fake_pool.disconnect_all = AsyncMock()

        async def fake_init_pool(config, db):
            from src.telegram.auth import TelegramAuth
            return TelegramAuth(0, ""), fake_pool

        with patch("src.cli.runtime.init_pool", side_effect=fake_init_pool):
            with patch(
                "src.services.notification_service.NotificationService.teardown_bot",
                new_callable=AsyncMock,
            ):
                run(_ns(notification_action="delete"))
        out = capsys.readouterr().out
        assert "deleted" in out.lower()

    def test_notification_test_message(self, cli_env, capsys):
        from src.cli.commands.notification import run

        fake_pool = AsyncMock()
        fake_pool.clients = {}
        fake_pool.disconnect_all = AsyncMock()

        async def fake_init_pool(config, db):
            from src.telegram.auth import TelegramAuth
            return TelegramAuth(0, ""), fake_pool

        # Patch the whole NotificationService so send_notification is available
        fake_svc = MagicMock()
        fake_svc.send_notification = AsyncMock()

        with patch("src.cli.runtime.init_pool", side_effect=fake_init_pool):
            with patch(
                "src.cli.commands.notification.NotificationService",
                return_value=fake_svc,
            ):
                run(_ns(notification_action="test", message="hello"))
        out = capsys.readouterr().out
        assert "sent" in out.lower()


# ===========================================================================
# 3. cli/commands/test.py — read/write checks
# ===========================================================================


class TestCLITestReadChecks:
    @pytest.mark.asyncio
    async def test_check_get_stats_pass(self, db):
        from src.cli.commands.test import Status, _check_get_stats

        result = await _check_get_stats(db)
        assert result.status == Status.PASS

    @pytest.mark.asyncio
    async def test_check_account_list_pass(self, db):
        from src.cli.commands.test import Status, _check_account_list

        result = await _check_account_list(db)
        assert result.status == Status.PASS
        assert "accounts" in result.detail

    @pytest.mark.asyncio
    async def test_check_channel_list_pass(self, db):
        from src.cli.commands.test import Status, _check_channel_list

        result = await _check_channel_list(db)
        assert result.status == Status.PASS

    @pytest.mark.asyncio
    async def test_check_notification_queries_skip_when_empty(self, db):
        from src.cli.commands.test import Status, _check_notification_queries

        result = await _check_notification_queries(db)
        assert result.status == Status.SKIP

    @pytest.mark.asyncio
    async def test_check_local_search_pass(self, db):
        from src.cli.commands.test import Status, _check_local_search

        result = await _check_local_search(db)
        assert result.status == Status.PASS

    @pytest.mark.asyncio
    async def test_check_collection_tasks_pass(self, db):
        from src.cli.commands.test import Status, _check_collection_tasks

        result = await _check_collection_tasks(db)
        assert result.status == Status.PASS

    @pytest.mark.asyncio
    async def test_check_recent_searches_skip_when_empty(self, db):
        from src.cli.commands.test import Status, _check_recent_searches

        result = await _check_recent_searches(db)
        assert result.status == Status.SKIP

    @pytest.mark.asyncio
    async def test_check_pipeline_list_pass(self, db):
        from src.cli.commands.test import Status, _check_pipeline_list

        result = await _check_pipeline_list(db)
        assert result.status == Status.PASS

    @pytest.mark.asyncio
    async def test_check_notification_bot_pass(self, db):
        from src.cli.commands.test import Status, _check_notification_bot

        result = await _check_notification_bot(db)
        assert result.status == Status.PASS

    @pytest.mark.asyncio
    async def test_check_photo_tasks_pass(self, db):
        from src.cli.commands.test import Status, _check_photo_tasks

        result = await _check_photo_tasks(db)
        assert result.status == Status.PASS

    def test_print_result_pass(self, capsys):
        from src.cli.commands.test import CheckResult, Status, _print_result

        _print_result(CheckResult("test_check", Status.PASS, "all good"))
        out = capsys.readouterr().out
        assert "PASS" in out
        assert "test_check" in out

    def test_print_result_fail(self, capsys):
        from src.cli.commands.test import CheckResult, Status, _print_result

        _print_result(CheckResult("test_check", Status.FAIL, "broken"))
        out = capsys.readouterr().out
        assert "FAIL" in out

    def test_print_result_skip(self, capsys):
        from src.cli.commands.test import CheckResult, Status, _print_result

        _print_result(CheckResult("test_check", Status.SKIP, "skipped"))
        out = capsys.readouterr().out
        assert "SKIP" in out


class TestCLITestDataclasses:
    def test_check_result_attributes(self):
        from src.cli.commands.test import CheckResult, Status

        r = CheckResult("mycheck", Status.PASS, "detail text")
        assert r.name == "mycheck"
        assert r.status == Status.PASS
        assert r.detail == "detail text"

    def test_benchmark_step_attributes(self):
        from src.cli.commands.test import BenchmarkStep

        step = BenchmarkStep("my_step", ("python", "-m", "pytest"))
        assert step.name == "my_step"
        assert step.command == ("python", "-m", "pytest")

    def test_format_exception_with_message(self):
        from src.cli.commands.test import _format_exception

        exc = ValueError("something went wrong")
        result = _format_exception(exc)
        assert result == "something went wrong"

    def test_format_exception_empty_uses_class_name(self):
        from src.cli.commands.test import _format_exception

        exc = ValueError("")
        result = _format_exception(exc)
        assert result == "ValueError"

    def test_run_benchmark_step_records_time(self, tmp_path):
        """_run_benchmark_step returns a float elapsed time."""
        import sys

        from src.cli.commands.test import BenchmarkStep, _run_benchmark_step

        # Run a trivial command that succeeds quickly
        step = BenchmarkStep("trivial", (sys.executable, "-c", "pass"))
        elapsed = _run_benchmark_step(step)
        assert isinstance(elapsed, float)
        assert elapsed >= 0.0

    def test_run_benchmark_step_raises_on_failure(self):
        import sys

        from src.cli.commands.test import BenchmarkStep, _run_benchmark_step

        step = BenchmarkStep("failing_step", (sys.executable, "-c", "raise SystemExit(1)"))
        with pytest.raises(SystemExit):
            _run_benchmark_step(step)


class TestCLITestRun:
    def test_run_read_action_calls_db(self, cli_env, capsys):
        from src.cli.commands.test import run

        run(_ns(test_action="read"))
        out = capsys.readouterr().out
        assert "Read Tests" in out
        assert "passed" in out

    def test_run_read_action_db_fail_exits(self, cli_env, capsys):
        from src.cli.commands.test import run

        with patch("src.cli.runtime.init_db", side_effect=Exception("db crash")):
            with pytest.raises(SystemExit):
                run(_ns(test_action="read"))


# ===========================================================================
# 4. cli/process_control.py
# ===========================================================================


class TestProcessControl:
    def test_read_pid_returns_none_when_no_file(self, tmp_path):
        from src.cli.process_control import read_pid

        p = tmp_path / "test.pid"
        assert read_pid(p) is None

    def test_read_pid_returns_int(self, tmp_path):
        from src.cli.process_control import read_pid

        p = tmp_path / "test.pid"
        p.write_text("12345\n", encoding="utf-8")
        assert read_pid(p) == 12345

    def test_read_pid_empty_file_returns_none(self, tmp_path):
        from src.cli.process_control import read_pid

        p = tmp_path / "test.pid"
        p.write_text("   \n", encoding="utf-8")
        assert read_pid(p) is None

    def test_read_pid_invalid_raises(self, tmp_path):
        from src.cli.process_control import ProcessControlError, read_pid

        p = tmp_path / "test.pid"
        p.write_text("not_a_pid", encoding="utf-8")
        with pytest.raises(ProcessControlError):
            read_pid(p)

    def test_remove_pid_file_removes_existing(self, tmp_path):
        from src.cli.process_control import remove_pid_file

        p = tmp_path / "test.pid"
        p.write_text("1", encoding="utf-8")
        remove_pid_file(p)
        assert not p.exists()

    def test_remove_pid_file_ignores_missing(self, tmp_path):
        from src.cli.process_control import remove_pid_file

        p = tmp_path / "nonexistent.pid"
        remove_pid_file(p)  # should not raise

    def test_is_process_alive_current_process(self):
        from src.cli.process_control import is_process_alive

        assert is_process_alive(os.getpid()) is True

    def test_is_process_alive_nonexistent_pid(self):
        from src.cli.process_control import is_process_alive

        # PID 0 is the kernel idle process on Linux/macOS; kill -0 to 0 raises PermissionError
        # Use a very large PID that's almost certainly not running
        assert is_process_alive(999999999) is False

    def test_register_and_unregister(self, tmp_path):
        from src.cli.process_control import register_current_process, unregister_current_process

        p = tmp_path / "test.pid"
        register_current_process(p)
        assert p.exists()
        pid_read = int(p.read_text(encoding="utf-8").strip())
        assert pid_read == os.getpid()

        unregister_current_process(p)
        assert not p.exists()

    def test_ensure_server_not_running_no_pid_file(self, tmp_path):
        from src.cli.process_control import ensure_server_not_running

        p = tmp_path / "no.pid"
        ensure_server_not_running(p)  # should return silently

    def test_ensure_server_not_running_own_pid_removes_file(self, tmp_path):
        from src.cli.process_control import ensure_server_not_running

        p = tmp_path / "self.pid"
        p.write_text(f"{os.getpid()}\n", encoding="utf-8")
        ensure_server_not_running(p)
        assert not p.exists()

    def test_stop_server_no_pid_file(self, tmp_path):
        from src.cli.process_control import StopResult, stop_server

        p = tmp_path / "no.pid"
        outcome = stop_server(p)
        assert outcome.result == StopResult.NOT_RUNNING

    def test_stop_server_stale_pid(self, tmp_path):
        from src.cli.process_control import StopResult, stop_server

        p = tmp_path / "stale.pid"
        p.write_text("999999999\n", encoding="utf-8")
        outcome = stop_server(p)
        assert outcome.result == StopResult.STALE_PID
        assert not p.exists()

    def test_pid_file_path(self, tmp_path):
        from src.cli.process_control import pid_file_path

        config = AppConfig()
        config.database.path = str(tmp_path / "mydb.db")
        path = pid_file_path(config)
        assert path.suffix == ".pid"
        assert path.stem == "mydb"


# ===========================================================================
# 5. database/connection.py
# ===========================================================================


class TestDBConnection:
    @pytest.mark.asyncio
    async def test_connect_creates_wal_db(self, tmp_path):
        from src.database.connection import DBConnection

        db_path = str(tmp_path / "test_conn.db")
        conn = DBConnection(db_path)
        try:
            db = await conn.connect()
            assert db is not None
            # WAL mode should be set
            cursor = await db.execute("PRAGMA journal_mode")
            row = await cursor.fetchone()
            assert row[0] == "wal"
        finally:
            await conn.close()

    @pytest.mark.asyncio
    async def test_close_is_idempotent(self, tmp_path):
        from src.database.connection import DBConnection

        db_path = str(tmp_path / "test_close.db")
        conn = DBConnection(db_path)
        await conn.connect()
        await conn.close()
        await conn.close()  # second close should not raise

    @pytest.mark.asyncio
    async def test_close_when_not_connected(self, tmp_path):
        from src.database.connection import DBConnection

        db_path = str(tmp_path / "never_opened.db")
        conn = DBConnection(db_path)
        await conn.close()  # should not raise

    @pytest.mark.asyncio
    async def test_execute_fetchall(self, tmp_path):
        from src.database.connection import DBConnection

        db_path = str(tmp_path / "test_exec.db")
        conn = DBConnection(db_path)
        try:
            await conn.connect()
            rows = await conn.execute_fetchall("SELECT 1 as n")
            assert len(rows) == 1
        finally:
            await conn.close()

    @pytest.mark.asyncio
    async def test_creates_parent_directory(self, tmp_path):
        from src.database.connection import DBConnection

        nested = tmp_path / "nested" / "dir" / "db.sqlite"
        conn = DBConnection(str(nested))
        try:
            await conn.connect()
            assert nested.parent.exists()
        finally:
            await conn.close()


# ===========================================================================
# 6. agent/manager.py
# ===========================================================================


class TestAgentRuntimeStatus:
    def test_dataclass_fields(self):
        from src.agent.manager import AgentRuntimeStatus

        status = AgentRuntimeStatus(
            claude_available=True,
            deepagents_available=False,
            dev_mode_enabled=False,
            backend_override="auto",
            selected_backend="claude",
            fallback_model="",
            fallback_provider="",
            using_override=False,
            error=None,
        )
        assert status.claude_available is True
        assert status.selected_backend == "claude"
        assert status.error is None

    def test_dataclass_with_error(self):
        from src.agent.manager import AgentRuntimeStatus

        status = AgentRuntimeStatus(
            claude_available=False,
            deepagents_available=False,
            dev_mode_enabled=False,
            backend_override="auto",
            selected_backend=None,
            fallback_model="",
            fallback_provider="",
            using_override=False,
            error="No backend configured",
        )
        assert status.error == "No backend configured"
        assert status.selected_backend is None


class TestAgentManagerInit:
    def test_init_with_defaults(self, db):
        from src.agent.manager import AgentManager

        mgr = AgentManager(db)
        assert mgr._db is db
        assert mgr._config is not None
        assert mgr._claude_backend is not None
        assert mgr._deepagents_backend is not None
        assert mgr._active_tasks == {}

    def test_init_with_config(self, db):
        from src.agent.manager import AgentManager

        config = AppConfig()
        mgr = AgentManager(db, config=config)
        assert mgr._config is config

    def test_available_false_when_no_credentials(self, db):
        from src.agent.manager import AgentManager

        with patch.dict(os.environ, {}, clear=True):
            mgr = AgentManager(db)
            assert mgr._claude_backend.available is False


class TestAgentManagerRefreshSettingsCache:
    @pytest.mark.asyncio
    async def test_refresh_settings_cache_no_preflight(self, db):
        from src.agent.manager import AgentManager

        mgr = AgentManager(db)
        # Should not raise
        await mgr.refresh_settings_cache(preflight=False)

    @pytest.mark.asyncio
    async def test_refresh_settings_cache_with_preflight_no_config(self, db):
        from src.agent.manager import AgentManager

        mgr = AgentManager(db)
        # preflight=True with no deepagents configured; should not raise
        await mgr.refresh_settings_cache(preflight=True)


class TestAgentManagerGetRuntimeStatus:
    @pytest.mark.asyncio
    async def test_get_runtime_status_returns_status(self, db):
        from src.agent.manager import AgentManager, AgentRuntimeStatus

        mgr = AgentManager(db)
        status = await mgr.get_runtime_status()
        assert isinstance(status, AgentRuntimeStatus)
        assert isinstance(status.claude_available, bool)
        assert isinstance(status.deepagents_available, bool)

    @pytest.mark.asyncio
    async def test_get_runtime_status_no_backends(self, db):
        from src.agent.manager import AgentManager, ClaudeSdkBackend

        mgr = AgentManager(db)
        # Patch the `available` property at the class level temporarily
        with patch.object(ClaudeSdkBackend, "available", new_callable=lambda: property(lambda self: False)):
            status = await mgr.get_runtime_status()
        assert isinstance(status.selected_backend, (str, type(None)))

    @pytest.mark.asyncio
    async def test_get_runtime_status_dev_mode_override(self, db):
        from src.agent.manager import AgentManager

        mgr = AgentManager(db)
        # Set dev mode and override in DB
        await db.set_setting("agent_dev_mode_enabled", "1")
        await db.set_setting("agent_backend_override", "claude")

        status = await mgr.get_runtime_status()
        assert status.dev_mode_enabled is True
        assert status.backend_override == "claude"


class TestAgentManagerBuildPrompt:
    def test_build_prompt_no_history(self, db):
        from src.agent.manager import AgentManager

        mgr = AgentManager(db)
        prompt, stats = mgr._build_prompt([], "Hello")
        assert "Hello" in prompt
        assert stats["total_msgs"] == 0
        assert stats["kept_msgs"] == 0

    def test_build_prompt_with_history(self, db):
        from src.agent.manager import AgentManager

        mgr = AgentManager(db)
        history = [
            {"role": "user", "content": "first message"},
            {"role": "assistant", "content": "first reply"},
        ]
        prompt, stats = mgr._build_prompt(history, "second message")
        assert "second message" in prompt
        assert stats["total_msgs"] == 2
        assert stats["kept_msgs"] == 2

    def test_build_prompt_stats_only(self, db):
        from src.agent.manager import AgentManager

        mgr = AgentManager(db)
        stats = mgr._build_prompt_stats_only([], "Hello")
        assert stats["total_msgs"] == 0
        assert stats["kept_msgs"] == 0
        assert stats["prompt_chars"] > 0


class TestClaudeSdkBackendAvailable:
    def test_available_with_api_key(self, db):
        from src.agent.manager import ClaudeSdkBackend

        with patch.dict(os.environ, {"ANTHROPIC_API_KEY": "sk-test"}):
            backend = ClaudeSdkBackend(db, AppConfig())
            assert backend.available is True

    def test_available_with_oauth_token(self, db):
        from src.agent.manager import ClaudeSdkBackend

        env = {k: v for k, v in os.environ.items() if k != "ANTHROPIC_API_KEY"}
        env["CLAUDE_CODE_OAUTH_TOKEN"] = "token-test"
        with patch.dict(os.environ, env, clear=True):
            backend = ClaudeSdkBackend(db, AppConfig())
            assert backend.available is True

    def test_not_available_without_credentials(self, db):
        from src.agent.manager import ClaudeSdkBackend

        env = {
            k: v for k, v in os.environ.items()
            if k not in ("ANTHROPIC_API_KEY", "CLAUDE_CODE_OAUTH_TOKEN")
        }
        with patch.dict(os.environ, env, clear=True):
            backend = ClaudeSdkBackend(db, AppConfig())
            assert backend.available is False
