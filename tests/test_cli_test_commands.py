"""Tests for src/cli/commands/test.py — CLI test subcommands and helpers."""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.cli.commands.test import (
    BenchmarkStep,
    CheckResult,
    Status,
    TelegramLiveFloodDecision,
    TelegramLiveStepSkipError,
    _check_account_list,
    _check_channel_list,
    _check_collection_tasks,
    _check_get_stats,
    _check_local_search,
    _check_notification_bot,
    _check_notification_queries,
    _check_photo_tasks,
    _check_pipeline_list,
    _check_recent_searches,
    _format_all_flooded_detail,
    _format_exception,
    _get_search_result_flood_wait,
    _is_premium_flood,
    _is_premium_flood_unavailable_error,
    _is_regular_search_client_unavailable_error,
    _print_result,
    _run_benchmark_step,
    _skip_remaining_tg_checks,
)

# ---------------------------------------------------------------------------
# Status enum
# ---------------------------------------------------------------------------


class TestStatus:
    def test_values(self):
        assert Status.PASS.value == "PASS"
        assert Status.FAIL.value == "FAIL"
        assert Status.SKIP.value == "SKIP"


# ---------------------------------------------------------------------------
# CheckResult dataclass
# ---------------------------------------------------------------------------


class TestCheckResult:
    def test_creation(self):
        r = CheckResult("test_check", Status.PASS, "all good")
        assert r.name == "test_check"
        assert r.status == Status.PASS
        assert r.detail == "all good"


# ---------------------------------------------------------------------------
# BenchmarkStep dataclass
# ---------------------------------------------------------------------------


class TestBenchmarkStep:
    def test_creation(self):
        step = BenchmarkStep(name="serial", command=("pytest", "-q"))
        assert step.name == "serial"
        assert step.command == ("pytest", "-q")


# ---------------------------------------------------------------------------
# TelegramLiveFloodDecision
# ---------------------------------------------------------------------------


class TestTelegramLiveFloodDecision:
    def test_defaults(self):
        d = TelegramLiveFloodDecision(action="skip", detail="flooded")
        assert d.retry_after_sec is None
        assert d.next_available_at_utc is None


# ---------------------------------------------------------------------------
# _format_exception
# ---------------------------------------------------------------------------


class TestFormatException:
    def test_with_message(self):
        assert _format_exception(ValueError("bad")) == "bad"

    def test_with_empty_message(self):
        assert _format_exception(ValueError("")) == "ValueError"

    def test_with_no_args(self):
        assert _format_exception(RuntimeError()) == "RuntimeError"


# ---------------------------------------------------------------------------
# _print_result
# ---------------------------------------------------------------------------


class TestPrintResult:
    def test_pass(self, capsys):
        _print_result(CheckResult("x", Status.PASS, "ok"))
        out = capsys.readouterr().out
        assert "PASS" in out
        assert "x" in out
        assert "ok" in out

    def test_fail(self, capsys):
        _print_result(CheckResult("y", Status.FAIL, "err"))
        assert "FAIL" in capsys.readouterr().out

    def test_skip(self, capsys):
        _print_result(CheckResult("z", Status.SKIP, "skipped"))
        assert "SKIP" in capsys.readouterr().out


# ---------------------------------------------------------------------------
# _is_premium_flood
# ---------------------------------------------------------------------------


class TestIsPremiumFlood:
    def test_premium_operations(self):
        for op in ("check_search_quota", "search_telegram_check_quota", "search_telegram"):
            info = SimpleNamespace(operation=op)
            assert _is_premium_flood(info) is True

    def test_non_premium_operation(self):
        info = SimpleNamespace(operation="get_dialogs")
        assert _is_premium_flood(info) is False


# ---------------------------------------------------------------------------
# Error classification helpers
# ---------------------------------------------------------------------------


class TestErrorClassification:
    def test_regular_unavailable(self):
        assert _is_regular_search_client_unavailable_error(
            "Нет доступных Telegram-аккаунтов. Проверьте подключение."
        ) is True

    def test_regular_unavailable_wrong_text(self):
        assert _is_regular_search_client_unavailable_error("other error") is False

    def test_regular_unavailable_none(self):
        assert _is_regular_search_client_unavailable_error(None) is False

    def test_premium_unavailable(self):
        assert _is_premium_flood_unavailable_error(
            "Premium-аккаунты временно недоступны из-за Flood Wait."
        ) is True

    def test_premium_unavailable_wrong_text(self):
        assert _is_premium_flood_unavailable_error("other") is False

    def test_premium_unavailable_none(self):
        assert _is_premium_flood_unavailable_error(None) is False


# ---------------------------------------------------------------------------
# _format_all_flooded_detail
# ---------------------------------------------------------------------------


class TestFormatAllFloodedDetail:
    def test_no_retry(self):
        result = _format_all_flooded_detail("base", retry_after_sec=None, next_available_at_utc=None)
        assert "all clients are flood-waited" in result
        assert "retry after" not in result

    def test_with_retry_no_time(self):
        result = _format_all_flooded_detail("base", retry_after_sec=30, next_available_at_utc=None)
        assert "retry after about 30s" in result

    def test_with_retry_and_time(self):
        t = datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        result = _format_all_flooded_detail("base", retry_after_sec=60, next_available_at_utc=t)
        assert "2025-01-01" in result
        assert "retry after about 60s" in result


# ---------------------------------------------------------------------------
# _get_search_result_flood_wait
# ---------------------------------------------------------------------------


class TestGetSearchResultFloodWait:
    def test_with_flood_wait(self):
        from src.telegram.flood_wait import FloodWaitInfo

        fw = FloodWaitInfo(
            phone="123",
            wait_seconds=30,
            operation="test",
            next_available_at_utc=datetime(2025, 1, 1, tzinfo=timezone.utc),
            detail="flooded",
        )
        result = SimpleNamespace(flood_wait=fw)
        assert _get_search_result_flood_wait(result) is fw

    def test_without_flood_wait(self):
        result = SimpleNamespace(flood_wait=None)
        assert _get_search_result_flood_wait(result) is None

    def test_missing_attribute(self):
        result = SimpleNamespace()
        assert _get_search_result_flood_wait(result) is None


# ---------------------------------------------------------------------------
# _skip_remaining_tg_checks
# ---------------------------------------------------------------------------


class TestSkipRemainingTgChecks:
    def test_appends_skip_results(self):
        results = []
        _skip_remaining_tg_checks(results, "pool init skipped", ["check_a", "check_b"])
        assert len(results) == 2
        assert results[0].name == "check_a"
        assert results[0].status == Status.SKIP
        assert results[1].name == "check_b"
        assert results[1].status == Status.SKIP


# ---------------------------------------------------------------------------
# _run_benchmark_step
# ---------------------------------------------------------------------------


class TestRunBenchmarkStep:
    @patch("subprocess.run")
    def test_success(self, mock_run, capsys):
        mock_run.return_value = MagicMock(returncode=0)
        step = BenchmarkStep("test_step", (sys.executable, "-c", "pass"))
        elapsed = _run_benchmark_step(step)
        assert elapsed >= 0
        out = capsys.readouterr().out
        assert "test_step" in out

    @patch("subprocess.run")
    def test_failure_exits(self, mock_run):
        mock_run.return_value = MagicMock(returncode=1)
        step = BenchmarkStep("fail_step", (sys.executable, "-c", "fail"))
        with pytest.raises(SystemExit):
            _run_benchmark_step(step)


# ---------------------------------------------------------------------------
# DB check functions (with mocked db)
# ---------------------------------------------------------------------------


class TestCheckGetStats:
    @pytest.mark.anyio
    async def test_pass(self):
        db = MagicMock()
        db.get_stats = AsyncMock(return_value={"channels": 5})
        result = await _check_get_stats(db)
        assert result.status == Status.PASS
        assert "channels=5" in result.detail

    @pytest.mark.anyio
    async def test_fail(self):
        db = MagicMock()
        db.get_stats = AsyncMock(side_effect=RuntimeError("no db"))
        result = await _check_get_stats(db)
        assert result.status == Status.FAIL
        assert "no db" in result.detail


class TestCheckAccountList:
    @pytest.mark.anyio
    async def test_pass(self):
        db = MagicMock()
        db.get_accounts = AsyncMock(return_value=[MagicMock()])
        result = await _check_account_list(db)
        assert result.status == Status.PASS
        assert "1 accounts" in result.detail

    @pytest.mark.anyio
    async def test_fail(self):
        db = MagicMock()
        db.get_accounts = AsyncMock(side_effect=Exception("fail"))
        result = await _check_account_list(db)
        assert result.status == Status.FAIL


class TestCheckChannelList:
    @pytest.mark.anyio
    async def test_pass(self):
        db = MagicMock()
        db.get_channels_with_counts = AsyncMock(return_value=[MagicMock(), MagicMock()])
        result = await _check_channel_list(db)
        assert result.status == Status.PASS
        assert "2 channels" in result.detail


class TestCheckNotificationQueries:
    @pytest.mark.anyio
    async def test_skip_when_empty(self):
        db = MagicMock()
        db.get_notification_queries = AsyncMock(return_value=[])
        result = await _check_notification_queries(db)
        assert result.status == Status.SKIP

    @pytest.mark.anyio
    async def test_pass(self):
        db = MagicMock()
        db.get_notification_queries = AsyncMock(return_value=[MagicMock()])
        result = await _check_notification_queries(db)
        assert result.status == Status.PASS
        assert "1 queries" in result.detail


class TestCheckLocalSearch:
    @pytest.mark.anyio
    async def test_pass(self):
        db = MagicMock()
        db.search_messages = AsyncMock(return_value=([], 0))
        result = await _check_local_search(db)
        assert result.status == Status.PASS
        assert "0 results" in result.detail


class TestCheckCollectionTasks:
    @pytest.mark.anyio
    async def test_pass(self):
        db = MagicMock()
        db.get_collection_tasks = AsyncMock(return_value=[MagicMock()])
        result = await _check_collection_tasks(db)
        assert result.status == Status.PASS
        assert "1 tasks" in result.detail


class TestCheckRecentSearches:
    @pytest.mark.anyio
    async def test_skip_when_empty(self):
        db = MagicMock()
        db.get_recent_searches = AsyncMock(return_value=[])
        result = await _check_recent_searches(db)
        assert result.status == Status.SKIP

    @pytest.mark.anyio
    async def test_pass(self):
        db = MagicMock()
        db.get_recent_searches = AsyncMock(return_value=[MagicMock(), MagicMock()])
        result = await _check_recent_searches(db)
        assert result.status == Status.PASS
        assert "2 entries" in result.detail


class TestCheckPipelineList:
    @pytest.mark.anyio
    async def test_pass(self):
        db = MagicMock()
        db.repos.content_pipelines.get_all = AsyncMock(return_value=[MagicMock()])
        result = await _check_pipeline_list(db)
        assert result.status == Status.PASS
        assert "1 pipelines" in result.detail

    @pytest.mark.anyio
    async def test_fail(self):
        db = MagicMock()
        db.repos.content_pipelines.get_all = AsyncMock(side_effect=Exception("err"))
        result = await _check_pipeline_list(db)
        assert result.status == Status.FAIL


class TestCheckNotificationBot:
    @pytest.mark.anyio
    async def test_none_configured(self):
        db = MagicMock()
        db.repos.notification_bots.count = AsyncMock(return_value=0)
        result = await _check_notification_bot(db)
        assert result.status == Status.PASS
        assert "none configured" in result.detail

    @pytest.mark.anyio
    async def test_configured(self):
        db = MagicMock()
        db.repos.notification_bots.count = AsyncMock(return_value=2)
        result = await _check_notification_bot(db)
        assert result.status == Status.PASS
        assert "2 configured" in result.detail


class TestCheckPhotoTasks:
    @pytest.mark.anyio
    async def test_pass(self):
        db = MagicMock()
        db.repos.photo_loader.list_batches = AsyncMock(return_value=[MagicMock()])
        result = await _check_photo_tasks(db)
        assert result.status == Status.PASS
        assert "1 batches" in result.detail

    @pytest.mark.anyio
    async def test_fail(self):
        db = MagicMock()
        db.repos.photo_loader.list_batches = AsyncMock(side_effect=Exception("err"))
        result = await _check_photo_tasks(db)
        assert result.status == Status.FAIL


# ---------------------------------------------------------------------------
# TelegramLiveStepSkipError
# ---------------------------------------------------------------------------


class TestTelegramLiveStepSkipError:
    def test_is_runtime_error(self):
        exc = TelegramLiveStepSkipError("detail msg")
        assert isinstance(exc, RuntimeError)
        assert str(exc) == "detail msg"
