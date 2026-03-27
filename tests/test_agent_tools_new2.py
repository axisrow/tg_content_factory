"""Tests for agent tools: filters.py, scheduler.py, search_queries.py.

These tests call actual tool handler functions via the @tool decorator's
.handler attribute, ensuring argument parsing, formatting, and error handling
are all exercised.
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.database import Database

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_db():
    """Create a mock Database for testing tools."""
    return MagicMock(spec=Database)


def _get_tool_handlers(mock_db, client_pool=None, config=None, **kwargs):
    """Build MCP tools and return their handlers keyed by name."""
    captured_tools = []

    with patch(
        "src.agent.tools.create_sdk_mcp_server",
        side_effect=lambda **kw: captured_tools.extend(kw.get("tools", [])),
    ):
        from src.agent.tools import make_mcp_server

        make_mcp_server(mock_db, client_pool=client_pool, config=config, **kwargs)

    return {t.name: t.handler for t in captured_tools}


def _text(result: dict) -> str:
    """Extract text from tool result payload."""
    return result["content"][0]["text"]


def _make_scheduler_mgr():
    """Build a standard mock SchedulerManager."""
    mgr = MagicMock()
    mgr.is_running = True
    mgr.interval_minutes = 60
    mgr.get_potential_jobs = AsyncMock(return_value=[{"id": "collect", "enabled": True}])
    mgr.get_all_jobs_next_run = MagicMock(return_value={"collect": "2025-01-01 12:00"})
    mgr.is_job_enabled = AsyncMock(return_value=True)
    mgr.sync_job_state = AsyncMock()
    mgr.start = AsyncMock()
    mgr.stop = AsyncMock()
    mgr.trigger_now = AsyncMock(return_value="ok")
    return mgr


def _make_purge_result(purged=2, deleted=50):
    r = MagicMock()
    r.purged_count = purged
    r.total_messages_deleted = deleted
    return r


# ---------------------------------------------------------------------------
# filters.py — analyze_filters
# ---------------------------------------------------------------------------


class TestAnalyzeFiltersTool:
    @pytest.mark.asyncio
    async def test_empty_report(self, mock_db):
        """Empty report returns appropriate message."""
        report = MagicMock()
        report.results = []
        with patch("src.filters.analyzer.ChannelAnalyzer") as mock_analyzer:
            mock_analyzer.return_value.analyze_all = AsyncMock(return_value=report)
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["analyze_filters"]({})
        assert "Нет каналов для анализа" in _text(result)

    @pytest.mark.asyncio
    async def test_report_with_flagged_channels(self, mock_db):
        """Flagged channels are listed with their flags."""
        r = MagicMock()
        r.should_filter = True
        r.title = "SpamChan"
        r.channel_id = 100
        r.flags = ["low_uniqueness", "spam"]
        report = MagicMock()
        report.results = [r]
        with patch("src.filters.analyzer.ChannelAnalyzer") as mock_analyzer:
            mock_analyzer.return_value.analyze_all = AsyncMock(return_value=report)
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["analyze_filters"]({})
        text = _text(result)
        assert "SpamChan" in text
        assert "low_uniqueness" in text
        assert "spam" in text
        assert "1 рекомендовано" in text

    @pytest.mark.asyncio
    async def test_report_truncates_beyond_30(self, mock_db):
        """More than 30 flagged channels triggers truncation message."""

        def _make_result(i):
            r = MagicMock()
            r.should_filter = True
            r.title = f"Chan{i}"
            r.channel_id = i
            r.flags = ["low_uniqueness"]
            return r

        report = MagicMock()
        report.results = [_make_result(i) for i in range(35)]
        with patch("src.filters.analyzer.ChannelAnalyzer") as mock_analyzer:
            mock_analyzer.return_value.analyze_all = AsyncMock(return_value=report)
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["analyze_filters"]({})
        text = _text(result)
        assert "и ещё 5" in text

    @pytest.mark.asyncio
    async def test_report_non_flagged_not_listed(self, mock_db):
        """Non-flagged channels don't appear in the flagged list."""
        ok = MagicMock()
        ok.should_filter = False
        ok.title = "GoodChan"
        ok.channel_id = 1
        ok.flags = []
        report = MagicMock()
        report.results = [ok]
        with patch("src.filters.analyzer.ChannelAnalyzer") as mock_analyzer:
            mock_analyzer.return_value.analyze_all = AsyncMock(return_value=report)
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["analyze_filters"]({})
        text = _text(result)
        assert "GoodChan" not in text
        assert "0 рекомендовано" in text

    @pytest.mark.asyncio
    async def test_error_returns_text(self, mock_db):
        with patch("src.filters.analyzer.ChannelAnalyzer") as mock_analyzer:
            mock_analyzer.return_value.analyze_all = AsyncMock(side_effect=RuntimeError("DB fail"))
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["analyze_filters"]({})
        assert "Ошибка анализа фильтров" in _text(result)
        assert "DB fail" in _text(result)


# ---------------------------------------------------------------------------
# filters.py — apply_filters
# ---------------------------------------------------------------------------


class TestApplyFiltersTool:
    @pytest.mark.asyncio
    async def test_no_confirm_returns_gate(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["apply_filters"]({})
        assert "confirm=true" in _text(result)

    @pytest.mark.asyncio
    async def test_with_confirm_applies_and_returns_count(self, mock_db):
        report = MagicMock()
        with patch("src.filters.analyzer.ChannelAnalyzer") as mock_analyzer:
            inst = mock_analyzer.return_value
            inst.analyze_all = AsyncMock(return_value=report)
            inst.apply_filters = AsyncMock(return_value=3)
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["apply_filters"]({"confirm": True})
        text = _text(result)
        assert "3 каналов" in text
        assert "Фильтры применены" in text

    @pytest.mark.asyncio
    async def test_error_returns_text(self, mock_db):
        with patch("src.filters.analyzer.ChannelAnalyzer") as mock_analyzer:
            mock_analyzer.return_value.analyze_all = AsyncMock(side_effect=Exception("oops"))
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["apply_filters"]({"confirm": True})
        assert "Ошибка применения фильтров" in _text(result)


# ---------------------------------------------------------------------------
# filters.py — reset_filters
# ---------------------------------------------------------------------------


class TestResetFiltersTool:
    @pytest.mark.asyncio
    async def test_no_confirm_returns_gate(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["reset_filters"]({})
        assert "confirm=true" in _text(result)

    @pytest.mark.asyncio
    async def test_with_confirm_resets_and_returns_count(self, mock_db):
        with patch("src.filters.analyzer.ChannelAnalyzer") as mock_analyzer:
            mock_analyzer.return_value.reset_filters = AsyncMock(return_value=7)
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["reset_filters"]({"confirm": True})
        text = _text(result)
        assert "7 каналов" in text
        assert "разблокированы" in text

    @pytest.mark.asyncio
    async def test_error_returns_text(self, mock_db):
        with patch("src.filters.analyzer.ChannelAnalyzer") as mock_analyzer:
            mock_analyzer.return_value.reset_filters = AsyncMock(side_effect=Exception("nope"))
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["reset_filters"]({"confirm": True})
        assert "Ошибка сброса фильтров" in _text(result)


# ---------------------------------------------------------------------------
# filters.py — toggle_channel_filter
# ---------------------------------------------------------------------------


class TestToggleChannelFilterTool:
    @pytest.mark.asyncio
    async def test_missing_pk_returns_error(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["toggle_channel_filter"]({})
        assert "pk обязателен" in _text(result)

    @pytest.mark.asyncio
    async def test_channel_not_found(self, mock_db):
        mock_db.get_channel_by_pk = AsyncMock(return_value=None)
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["toggle_channel_filter"]({"pk": 999})
        assert "не найден" in _text(result)
        assert "999" in _text(result)

    @pytest.mark.asyncio
    async def test_filtered_false_becomes_filtered(self, mock_db):
        ch = MagicMock()
        ch.is_filtered = False
        ch.title = "NewsChan"
        mock_db.get_channel_by_pk = AsyncMock(return_value=ch)
        mock_db.set_channel_filtered = AsyncMock()
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["toggle_channel_filter"]({"pk": 1})
        text = _text(result)
        assert "NewsChan" in text
        assert "отфильтрован" in text
        mock_db.set_channel_filtered.assert_awaited_once_with(1, True)

    @pytest.mark.asyncio
    async def test_filtered_true_becomes_unblocked(self, mock_db):
        ch = MagicMock()
        ch.is_filtered = True
        ch.title = "SpamChan"
        mock_db.get_channel_by_pk = AsyncMock(return_value=ch)
        mock_db.set_channel_filtered = AsyncMock()
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["toggle_channel_filter"]({"pk": 2})
        text = _text(result)
        assert "SpamChan" in text
        assert "разблокирован" in text
        mock_db.set_channel_filtered.assert_awaited_once_with(2, False)

    @pytest.mark.asyncio
    async def test_error_returns_text(self, mock_db):
        mock_db.get_channel_by_pk = AsyncMock(side_effect=Exception("db error"))
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["toggle_channel_filter"]({"pk": 1})
        assert "Ошибка переключения фильтра" in _text(result)


# ---------------------------------------------------------------------------
# filters.py — purge_filtered_channels
# ---------------------------------------------------------------------------


class TestPurgeFilteredChannelsTool:
    @pytest.mark.asyncio
    async def test_no_confirm_returns_gate(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["purge_filtered_channels"]({})
        assert "confirm=true" in _text(result)

    @pytest.mark.asyncio
    async def test_with_pks_and_confirm_purges_by_pks(self, mock_db):
        purge_result = _make_purge_result(purged=2, deleted=40)
        with patch("src.services.filter_deletion_service.FilterDeletionService") as mock_svc:
            mock_svc.return_value.purge_channels_by_pks = AsyncMock(return_value=purge_result)
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["purge_filtered_channels"]({"pks": "1,2", "confirm": True})
        text = _text(result)
        assert "2 каналов" in text
        assert "40 сообщений" in text
        mock_svc.return_value.purge_channels_by_pks.assert_awaited_once_with([1, 2])

    @pytest.mark.asyncio
    async def test_empty_pks_and_confirm_purges_all(self, mock_db):
        purge_result = _make_purge_result(purged=5, deleted=200)
        with patch("src.services.filter_deletion_service.FilterDeletionService") as mock_svc:
            mock_svc.return_value.purge_all_filtered = AsyncMock(return_value=purge_result)
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["purge_filtered_channels"]({"pks": "", "confirm": True})
        text = _text(result)
        assert "5 каналов" in text
        assert "200 сообщений" in text
        mock_svc.return_value.purge_all_filtered.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_error_returns_text(self, mock_db):
        with patch("src.services.filter_deletion_service.FilterDeletionService") as mock_svc:
            mock_svc.return_value.purge_all_filtered = AsyncMock(side_effect=Exception("disk full"))
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["purge_filtered_channels"]({"confirm": True})
        assert "Ошибка очистки каналов" in _text(result)


# ---------------------------------------------------------------------------
# filters.py — hard_delete_channels
# ---------------------------------------------------------------------------


class TestHardDeleteChannelsTool:
    @pytest.mark.asyncio
    async def test_empty_pks_returns_error(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["hard_delete_channels"]({})
        assert "pks обязателен" in _text(result)

    @pytest.mark.asyncio
    async def test_no_confirm_returns_gate(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["hard_delete_channels"]({"pks": "1,2"})
        assert "confirm=true" in _text(result)

    @pytest.mark.asyncio
    async def test_with_pks_and_confirm_deletes(self, mock_db):
        del_result = _make_purge_result(purged=2, deleted=0)
        with patch("src.services.filter_deletion_service.FilterDeletionService") as mock_svc:
            mock_svc.return_value.hard_delete_channels_by_pks = AsyncMock(return_value=del_result)
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["hard_delete_channels"]({"pks": "3,4", "confirm": True})
        text = _text(result)
        assert "2 каналов" in text
        assert "безвозвратно" in text
        mock_svc.return_value.hard_delete_channels_by_pks.assert_awaited_once_with([3, 4])

    @pytest.mark.asyncio
    async def test_error_returns_text(self, mock_db):
        with patch("src.services.filter_deletion_service.FilterDeletionService") as mock_svc:
            mock_svc.return_value.hard_delete_channels_by_pks = AsyncMock(side_effect=Exception("err"))
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["hard_delete_channels"]({"pks": "1", "confirm": True})
        assert "Ошибка удаления каналов" in _text(result)


# ---------------------------------------------------------------------------
# scheduler.py — get_scheduler_status
# ---------------------------------------------------------------------------


class TestGetSchedulerStatusTool:
    @pytest.mark.asyncio
    async def test_no_manager_returns_error(self, mock_db):
        """Without a scheduler_manager the tool catches RuntimeError and returns text."""
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["get_scheduler_status"]({})
        assert "Ошибка получения статуса планировщика" in _text(result)

    @pytest.mark.asyncio
    async def test_with_manager_shows_running_state(self, mock_db):
        mgr = _make_scheduler_mgr()
        handlers = _get_tool_handlers(mock_db, scheduler_manager=mgr)
        result = await handlers["get_scheduler_status"]({})
        text = _text(result)
        assert "запущен" in text
        assert "60 мин" in text

    @pytest.mark.asyncio
    async def test_with_manager_lists_jobs(self, mock_db):
        mgr = _make_scheduler_mgr()
        handlers = _get_tool_handlers(mock_db, scheduler_manager=mgr)
        result = await handlers["get_scheduler_status"]({})
        text = _text(result)
        assert "collect" in text
        assert "вкл" in text
        assert "2025-01-01 12:00" in text

    @pytest.mark.asyncio
    async def test_with_manager_stopped(self, mock_db):
        mgr = _make_scheduler_mgr()
        mgr.is_running = False
        handlers = _get_tool_handlers(mock_db, scheduler_manager=mgr)
        result = await handlers["get_scheduler_status"]({})
        assert "остановлен" in _text(result)


# ---------------------------------------------------------------------------
# scheduler.py — start_scheduler
# ---------------------------------------------------------------------------


class TestStartSchedulerTool:
    @pytest.mark.asyncio
    async def test_no_pool_returns_pool_gate(self, mock_db):
        handlers = _get_tool_handlers(mock_db, client_pool=None)
        result = await handlers["start_scheduler"]({"confirm": True})
        assert "Telegram-клиент" in _text(result)

    @pytest.mark.asyncio
    async def test_no_confirm_returns_gate(self, mock_db):
        mock_pool = MagicMock()
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["start_scheduler"]({})
        assert "confirm=true" in _text(result)

    @pytest.mark.asyncio
    async def test_with_pool_and_confirm_starts(self, mock_db):
        mgr = _make_scheduler_mgr()
        mock_pool = MagicMock()
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool, scheduler_manager=mgr)
        result = await handlers["start_scheduler"]({"confirm": True})
        assert "Планировщик запущен" in _text(result)
        mgr.start.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_error_returns_text(self, mock_db):
        mgr = _make_scheduler_mgr()
        mgr.start = AsyncMock(side_effect=Exception("already running"))
        mock_pool = MagicMock()
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool, scheduler_manager=mgr)
        result = await handlers["start_scheduler"]({"confirm": True})
        assert "Ошибка запуска планировщика" in _text(result)


# ---------------------------------------------------------------------------
# scheduler.py — stop_scheduler
# ---------------------------------------------------------------------------


class TestStopSchedulerTool:
    @pytest.mark.asyncio
    async def test_no_confirm_returns_gate(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["stop_scheduler"]({})
        assert "confirm=true" in _text(result)

    @pytest.mark.asyncio
    async def test_with_confirm_stops(self, mock_db):
        mgr = _make_scheduler_mgr()
        handlers = _get_tool_handlers(mock_db, scheduler_manager=mgr)
        result = await handlers["stop_scheduler"]({"confirm": True})
        assert "Планировщик остановлен" in _text(result)
        mgr.stop.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_error_returns_text(self, mock_db):
        mgr = _make_scheduler_mgr()
        mgr.stop = AsyncMock(side_effect=Exception("not running"))
        handlers = _get_tool_handlers(mock_db, scheduler_manager=mgr)
        result = await handlers["stop_scheduler"]({"confirm": True})
        assert "Ошибка остановки планировщика" in _text(result)


# ---------------------------------------------------------------------------
# scheduler.py — trigger_collection
# ---------------------------------------------------------------------------


class TestTriggerCollectionTool:
    @pytest.mark.asyncio
    async def test_no_pool_returns_pool_gate(self, mock_db):
        handlers = _get_tool_handlers(mock_db, client_pool=None)
        result = await handlers["trigger_collection"]({"confirm": True})
        assert "Telegram-клиент" in _text(result)

    @pytest.mark.asyncio
    async def test_no_confirm_returns_gate(self, mock_db):
        mock_pool = MagicMock()
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["trigger_collection"]({})
        assert "confirm=true" in _text(result)

    @pytest.mark.asyncio
    async def test_with_pool_and_confirm_triggers(self, mock_db):
        mgr = _make_scheduler_mgr()
        mock_pool = MagicMock()
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool, scheduler_manager=mgr)
        result = await handlers["trigger_collection"]({"confirm": True})
        text = _text(result)
        assert "Сбор запущен" in text
        mgr.trigger_now.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_error_returns_text(self, mock_db):
        mgr = _make_scheduler_mgr()
        mgr.trigger_now = AsyncMock(side_effect=Exception("busy"))
        mock_pool = MagicMock()
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool, scheduler_manager=mgr)
        result = await handlers["trigger_collection"]({"confirm": True})
        assert "Ошибка запуска сбора" in _text(result)


# ---------------------------------------------------------------------------
# scheduler.py — toggle_scheduler_job
# ---------------------------------------------------------------------------


class TestToggleSchedulerJobTool:
    @pytest.mark.asyncio
    async def test_missing_job_id_returns_error(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["toggle_scheduler_job"]({})
        assert "job_id обязателен" in _text(result)

    @pytest.mark.asyncio
    async def test_enabled_job_becomes_disabled(self, mock_db):
        mgr = _make_scheduler_mgr()
        mgr.is_job_enabled = AsyncMock(return_value=True)
        handlers = _get_tool_handlers(mock_db, scheduler_manager=mgr)
        result = await handlers["toggle_scheduler_job"]({"job_id": "collect"})
        text = _text(result)
        assert "collect" in text
        assert "выключена" in text
        mgr.sync_job_state.assert_awaited_once_with("collect", False)

    @pytest.mark.asyncio
    async def test_disabled_job_becomes_enabled(self, mock_db):
        mgr = _make_scheduler_mgr()
        mgr.is_job_enabled = AsyncMock(return_value=False)
        handlers = _get_tool_handlers(mock_db, scheduler_manager=mgr)
        result = await handlers["toggle_scheduler_job"]({"job_id": "notify"})
        text = _text(result)
        assert "notify" in text
        assert "включена" in text
        mgr.sync_job_state.assert_awaited_once_with("notify", True)

    @pytest.mark.asyncio
    async def test_no_manager_returns_error(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["toggle_scheduler_job"]({"job_id": "collect"})
        assert "Ошибка переключения задачи" in _text(result)

    @pytest.mark.asyncio
    async def test_error_returns_text(self, mock_db):
        mgr = _make_scheduler_mgr()
        mgr.is_job_enabled = AsyncMock(side_effect=Exception("unknown job"))
        handlers = _get_tool_handlers(mock_db, scheduler_manager=mgr)
        result = await handlers["toggle_scheduler_job"]({"job_id": "bad_job"})
        assert "Ошибка переключения задачи" in _text(result)


# ---------------------------------------------------------------------------
# search_queries.py — use real DB fixture
# ---------------------------------------------------------------------------


@pytest.fixture
async def sq_handlers(db):
    """Tool handlers backed by a real in-memory DB."""
    return _get_tool_handlers(db)


async def _add_query(db, query="test query", interval_minutes=30, is_active=True):
    """Helper: add a search query via the service and return its id."""
    from src.services.search_query_service import SearchQueryService

    svc = SearchQueryService(db)
    sq_id = await svc.add(query, interval_minutes=interval_minutes)
    if not is_active:
        await svc.toggle(sq_id)  # toggle off since created as active
    return sq_id


# ---------------------------------------------------------------------------
# search_queries.py — list_search_queries
# ---------------------------------------------------------------------------


class TestListSearchQueriesTool:
    @pytest.mark.asyncio
    async def test_empty_returns_not_found(self, sq_handlers):
        result = await sq_handlers["list_search_queries"]({"active_only": False})
        assert "не найдены" in _text(result)

    @pytest.mark.asyncio
    async def test_lists_all_when_active_only_false(self, db, sq_handlers):
        await _add_query(db, "query1", is_active=True)
        await _add_query(db, "query2", is_active=False)
        result = await sq_handlers["list_search_queries"]({"active_only": False})
        text = _text(result)
        assert "query1" in text
        assert "query2" in text
        assert "Поисковые запросы (2)" in text

    @pytest.mark.asyncio
    async def test_active_only_filters_inactive(self, db, sq_handlers):
        await _add_query(db, "active_query", is_active=True)
        await _add_query(db, "inactive_query", is_active=False)
        result = await sq_handlers["list_search_queries"]({"active_only": True})
        text = _text(result)
        assert "active_query" in text
        assert "inactive_query" not in text

    @pytest.mark.asyncio
    async def test_shows_status_labels(self, db, sq_handlers):
        await _add_query(db, "myquery", is_active=True)
        result = await sq_handlers["list_search_queries"]({})
        text = _text(result)
        assert "активен" in text


# ---------------------------------------------------------------------------
# search_queries.py — get_search_query
# ---------------------------------------------------------------------------


class TestGetSearchQueryTool:
    @pytest.mark.asyncio
    async def test_missing_sq_id_returns_error(self, sq_handlers):
        result = await sq_handlers["get_search_query"]({})
        assert "sq_id обязателен" in _text(result)

    @pytest.mark.asyncio
    async def test_not_found_returns_error(self, sq_handlers):
        result = await sq_handlers["get_search_query"]({"sq_id": 9999})
        assert "не найден" in _text(result)
        assert "9999" in _text(result)

    @pytest.mark.asyncio
    async def test_found_shows_fields(self, db, sq_handlers):
        sq_id = await _add_query(db, "find this")
        result = await sq_handlers["get_search_query"]({"sq_id": sq_id})
        text = _text(result)
        assert "find this" in text
        assert f"id: {sq_id}" in text
        assert "is_active" in text
        assert "interval_minutes" in text


# ---------------------------------------------------------------------------
# search_queries.py — add_search_query
# ---------------------------------------------------------------------------


class TestAddSearchQueryTool:
    @pytest.mark.asyncio
    async def test_missing_query_returns_error(self, sq_handlers):
        result = await sq_handlers["add_search_query"]({"confirm": True})
        assert "query обязателен" in _text(result)

    @pytest.mark.asyncio
    async def test_no_confirm_returns_gate(self, sq_handlers):
        result = await sq_handlers["add_search_query"]({"query": "hello"})
        assert "confirm=true" in _text(result)

    @pytest.mark.asyncio
    async def test_with_query_and_confirm_creates(self, sq_handlers):
        result = await sq_handlers["add_search_query"]({"query": "new query", "confirm": True})
        text = _text(result)
        assert "создан" in text
        assert "id=" in text

    @pytest.mark.asyncio
    async def test_with_custom_interval_creates(self, sq_handlers):
        result = await sq_handlers["add_search_query"](
            {"query": "custom interval", "interval_minutes": 120, "confirm": True}
        )
        assert "создан" in _text(result)


# ---------------------------------------------------------------------------
# search_queries.py — edit_search_query
# ---------------------------------------------------------------------------


class TestEditSearchQueryTool:
    @pytest.mark.asyncio
    async def test_missing_sq_id_returns_error(self, sq_handlers):
        result = await sq_handlers["edit_search_query"]({"confirm": True})
        assert "sq_id обязателен" in _text(result)

    @pytest.mark.asyncio
    async def test_no_confirm_returns_gate(self, sq_handlers):
        result = await sq_handlers["edit_search_query"]({"sq_id": 1})
        assert "confirm=true" in _text(result)

    @pytest.mark.asyncio
    async def test_not_found_returns_error(self, sq_handlers):
        result = await sq_handlers["edit_search_query"]({"sq_id": 9999, "confirm": True})
        assert "не найден" in _text(result)

    @pytest.mark.asyncio
    async def test_found_updates_query(self, db, sq_handlers):
        sq_id = await _add_query(db, "original")
        result = await sq_handlers["edit_search_query"](
            {"sq_id": sq_id, "query": "updated", "confirm": True}
        )
        assert "обновлён" in _text(result)

    @pytest.mark.asyncio
    async def test_updates_interval(self, db, sq_handlers):
        sq_id = await _add_query(db, "some query")
        result = await sq_handlers["edit_search_query"](
            {"sq_id": sq_id, "interval_minutes": 90, "confirm": True}
        )
        assert "обновлён" in _text(result)


# ---------------------------------------------------------------------------
# search_queries.py — delete_search_query
# ---------------------------------------------------------------------------


class TestDeleteSearchQueryTool:
    @pytest.mark.asyncio
    async def test_missing_sq_id_returns_error(self, sq_handlers):
        result = await sq_handlers["delete_search_query"]({})
        assert "sq_id обязателен" in _text(result)

    @pytest.mark.asyncio
    async def test_no_confirm_returns_gate(self, sq_handlers):
        result = await sq_handlers["delete_search_query"]({"sq_id": 1})
        assert "confirm=true" in _text(result)

    @pytest.mark.asyncio
    async def test_with_confirm_deletes(self, db, sq_handlers):
        sq_id = await _add_query(db, "to delete")
        result = await sq_handlers["delete_search_query"]({"sq_id": sq_id, "confirm": True})
        text = _text(result)
        assert "удалён" in text
        assert str(sq_id) in text


# ---------------------------------------------------------------------------
# search_queries.py — toggle_search_query
# ---------------------------------------------------------------------------


class TestToggleSearchQueryTool:
    @pytest.mark.asyncio
    async def test_missing_sq_id_returns_error(self, sq_handlers):
        result = await sq_handlers["toggle_search_query"]({})
        assert "sq_id обязателен" in _text(result)

    @pytest.mark.asyncio
    async def test_not_found_returns_error(self, sq_handlers):
        result = await sq_handlers["toggle_search_query"]({"sq_id": 9999})
        assert "не найден" in _text(result)

    @pytest.mark.asyncio
    async def test_active_query_gets_deactivated(self, db, sq_handlers):
        sq_id = await _add_query(db, "active one", is_active=True)
        result = await sq_handlers["toggle_search_query"]({"sq_id": sq_id})
        text = _text(result)
        assert "деактивирован" in text
        assert str(sq_id) in text

    @pytest.mark.asyncio
    async def test_inactive_query_gets_activated(self, db, sq_handlers):
        sq_id = await _add_query(db, "inactive one", is_active=False)
        result = await sq_handlers["toggle_search_query"]({"sq_id": sq_id})
        text = _text(result)
        assert "активирован" in text
        assert str(sq_id) in text


# ---------------------------------------------------------------------------
# search_queries.py — run_search_query
# ---------------------------------------------------------------------------


class TestRunSearchQueryTool:
    @pytest.mark.asyncio
    async def test_missing_sq_id_returns_error(self, sq_handlers):
        result = await sq_handlers["run_search_query"]({})
        assert "sq_id обязателен" in _text(result)

    @pytest.mark.asyncio
    async def test_run_returns_count(self, db, sq_handlers):
        sq_id = await _add_query(db, "search term")
        result = await sq_handlers["run_search_query"]({"sq_id": sq_id})
        text = _text(result)
        assert "выполнен" in text
        assert "совпадений" in text
        assert str(sq_id) in text

    @pytest.mark.asyncio
    async def test_run_nonexistent_returns_zero_matches(self, sq_handlers):
        """run_search_query with a nonexistent id returns 0 matches (service is lenient)."""
        result = await sq_handlers["run_search_query"]({"sq_id": 9999})
        text = _text(result)
        assert "выполнен" in text
        assert "0 совпадений" in text
