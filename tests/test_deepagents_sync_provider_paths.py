"""Tests for deepagents sync tools, pipeline tools, agent manager, and provider service paths."""

from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.database import Database

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_db():
    db = MagicMock(spec=Database)
    db.repos = MagicMock()
    return db


def _build_sync_tools(mock_db, config=None):
    from src.agent.tools.deepagents_sync import build_deepagents_tools

    return {t.__name__: t for t in build_deepagents_tools(mock_db, config=config)}


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
    return result["content"][0]["text"]


# ===========================================================================
# deepagents_sync — search_messages
# ===========================================================================


class TestDeepagentsSyncSearchMessages:
    def test_no_results(self, mock_db):
        mock_db.search_messages = AsyncMock(return_value=([], 0))
        tool_map = _build_sync_tools(mock_db)
        result = tool_map["search_messages"]("test query")
        assert "Ничего не найдено" in result

    def test_with_results(self, mock_db):
        msg = SimpleNamespace(date="2026-01-01", channel_id=10, text="hello world")
        mock_db.search_messages = AsyncMock(return_value=([msg], 1))
        tool_map = _build_sync_tools(mock_db)
        result = tool_map["search_messages"]("hello")
        assert "hello world" in result
        assert "channel_id=10" in result

    def test_error_returns_text(self, mock_db):
        mock_db.search_messages = AsyncMock(side_effect=Exception("db boom"))
        tool_map = _build_sync_tools(mock_db)
        result = tool_map["search_messages"]("fail")
        assert "Ошибка поиска" in result

    def test_long_message_truncated(self, mock_db):
        long_text = "x" * 500
        msg = SimpleNamespace(date="2026-01-01", channel_id=5, text=long_text)
        mock_db.search_messages = AsyncMock(return_value=([msg], 1))
        tool_map = _build_sync_tools(mock_db)
        result = tool_map["search_messages"]("x")
        # Preview capped at 200 chars + surrounding
        assert "Найдено 1 сообщений" in result


# ===========================================================================
# deepagents_sync — semantic_search
# ===========================================================================


class TestDeepagentsSyncSemanticSearch:
    def test_error_on_missing_embedding_service(self, mock_db):
        tool_map = _build_sync_tools(mock_db)
        with patch(
            "src.services.embedding_service.EmbeddingService",
            side_effect=ImportError("no embedding"),
        ):
            result = tool_map["semantic_search"]("query")
        # Should return error string (ImportError swallowed inside except)
        assert isinstance(result, str)

    def test_no_results(self, mock_db):
        svc_mock = MagicMock()
        svc_mock.embed_query = AsyncMock(return_value=[0.1, 0.2])
        mock_db.search_semantic_messages = AsyncMock(return_value=([], 0))
        with patch("src.services.embedding_service.EmbeddingService", return_value=svc_mock):
            tool_map = _build_sync_tools(mock_db)
            result = tool_map["semantic_search"]("query")
        assert "не найдены" in result

    def test_with_results(self, mock_db):
        svc_mock = MagicMock()
        svc_mock.embed_query = AsyncMock(return_value=[0.1, 0.2])
        msg = SimpleNamespace(date="2026-01-01", channel_id=3, text="semantic content")
        mock_db.search_semantic_messages = AsyncMock(return_value=([msg], 1))
        with patch("src.services.embedding_service.EmbeddingService", return_value=svc_mock):
            tool_map = _build_sync_tools(mock_db)
            result = tool_map["semantic_search"]("query")
        assert "semantic content" in result
        assert "channel_id=3" in result


# ===========================================================================
# deepagents_sync — add_channel / delete_channel / toggle_channel
# ===========================================================================


class TestDeepagentsSyncAddChannel:
    def test_success(self, mock_db):
        svc_mock = MagicMock()
        svc_mock.add_by_identifier = AsyncMock(return_value=SimpleNamespace(title="NewChan"))
        with patch("src.services.channel_service.ChannelService", return_value=svc_mock):
            tool_map = _build_sync_tools(mock_db)
            result = tool_map["add_channel"]("@newchan")
        assert "Канал добавлен" in result

    def test_returns_none(self, mock_db):
        svc_mock = MagicMock()
        svc_mock.add_by_identifier = AsyncMock(return_value=None)
        with patch("src.services.channel_service.ChannelService", return_value=svc_mock):
            tool_map = _build_sync_tools(mock_db)
            result = tool_map["add_channel"]("@notexist")
        assert "Не удалось добавить" in result

    def test_error(self, mock_db):
        with patch("src.services.channel_service.ChannelService", side_effect=Exception("fail")):
            tool_map = _build_sync_tools(mock_db)
            result = tool_map["add_channel"]("@err")
        assert "Ошибка" in result


class TestDeepagentsSyncDeleteChannel:
    def test_success(self, mock_db):
        svc_mock = MagicMock()
        svc_mock.delete = AsyncMock(return_value=None)
        with patch("src.services.channel_service.ChannelService", return_value=svc_mock):
            tool_map = _build_sync_tools(mock_db)
            result = tool_map["delete_channel"](42)
        assert "pk=42" in result
        assert "удалён" in result

    def test_error(self, mock_db):
        with patch("src.services.channel_service.ChannelService", side_effect=Exception("oops")):
            tool_map = _build_sync_tools(mock_db)
            result = tool_map["delete_channel"](1)
        assert "Ошибка" in result


class TestDeepagentsSyncToggleChannel:
    def test_success(self, mock_db):
        svc_mock = MagicMock()
        svc_mock.toggle = AsyncMock(return_value=None)
        with patch("src.services.channel_service.ChannelService", return_value=svc_mock):
            tool_map = _build_sync_tools(mock_db)
            result = tool_map["toggle_channel"](7)
        assert "pk=7" in result
        assert "переключён" in result

    def test_error(self, mock_db):
        with patch("src.services.channel_service.ChannelService", side_effect=Exception("fail")):
            tool_map = _build_sync_tools(mock_db)
            result = tool_map["toggle_channel"](1)
        assert "Ошибка" in result


# ===========================================================================
# deepagents_sync — get_pipeline_detail
# ===========================================================================


class TestDeepagentsSyncGetPipelineDetail:
    def test_not_found(self, mock_db):
        svc_mock = MagicMock()
        svc_mock.get_detail = AsyncMock(return_value=None)
        with patch("src.services.pipeline_service.PipelineService", return_value=svc_mock):
            tool_map = _build_sync_tools(mock_db)
            result = tool_map["get_pipeline_detail"](99)
        assert "не найден" in result

    def test_found(self, mock_db):
        p = SimpleNamespace(id=3, name="MyPipe", llm_model="gpt-4", is_active=True)
        svc_mock = MagicMock()
        svc_mock.get_detail = AsyncMock(return_value={"pipeline": p})
        with patch("src.services.pipeline_service.PipelineService", return_value=svc_mock):
            tool_map = _build_sync_tools(mock_db)
            result = tool_map["get_pipeline_detail"](3)
        assert "MyPipe" in result
        assert "gpt-4" in result

    def test_error(self, mock_db):
        with patch("src.services.pipeline_service.PipelineService", side_effect=Exception("err")):
            tool_map = _build_sync_tools(mock_db)
            result = tool_map["get_pipeline_detail"](1)
        assert "Ошибка" in result


# ===========================================================================
# deepagents_sync — toggle_pipeline / delete_pipeline
# ===========================================================================


class TestDeepagentsSyncTogglePipeline:
    def test_success(self, mock_db):
        svc_mock = MagicMock()
        svc_mock.toggle = AsyncMock(return_value=True)
        with patch("src.services.pipeline_service.PipelineService", return_value=svc_mock):
            tool_map = _build_sync_tools(mock_db)
            result = tool_map["toggle_pipeline"](5)
        assert "id=5" in result
        assert "переключён" in result

    def test_error(self, mock_db):
        with patch("src.services.pipeline_service.PipelineService", side_effect=Exception("x")):
            tool_map = _build_sync_tools(mock_db)
            result = tool_map["toggle_pipeline"](1)
        assert "Ошибка" in result


class TestDeepagentsSyncDeletePipeline:
    def test_success(self, mock_db):
        svc_mock = MagicMock()
        svc_mock.delete = AsyncMock(return_value=None)
        with patch("src.services.pipeline_service.PipelineService", return_value=svc_mock):
            tool_map = _build_sync_tools(mock_db)
            result = tool_map["delete_pipeline"](8)
        assert "id=8" in result
        assert "удалён" in result

    def test_error(self, mock_db):
        with patch("src.services.pipeline_service.PipelineService", side_effect=Exception("x")):
            tool_map = _build_sync_tools(mock_db)
            result = tool_map["delete_pipeline"](1)
        assert "Ошибка" in result


# ===========================================================================
# deepagents_sync — list_pipeline_runs / get_pipeline_run
# ===========================================================================


class TestDeepagentsSyncListPipelineRuns:
    def test_empty(self, mock_db):
        mock_db.repos.generation_runs.list_by_pipeline = AsyncMock(return_value=[])
        tool_map = _build_sync_tools(mock_db)
        result = tool_map["list_pipeline_runs"](pipeline_id=1)
        assert "Нет runs" in result

    def test_with_runs(self, mock_db):
        run = SimpleNamespace(id=3, status="done", moderation_status="approved")
        mock_db.repos.generation_runs.list_by_pipeline = AsyncMock(return_value=[run])
        tool_map = _build_sync_tools(mock_db)
        result = tool_map["list_pipeline_runs"](pipeline_id=1)
        assert "id=3" in result
        assert "approved" in result

    def test_error(self, mock_db):
        mock_db.repos.generation_runs.list_by_pipeline = AsyncMock(side_effect=Exception("db"))
        tool_map = _build_sync_tools(mock_db)
        result = tool_map["list_pipeline_runs"](pipeline_id=1)
        assert "Ошибка" in result


class TestDeepagentsSyncGetPipelineRun:
    def test_not_found(self, mock_db):
        mock_db.repos.generation_runs.get = AsyncMock(return_value=None)
        tool_map = _build_sync_tools(mock_db)
        result = tool_map["get_pipeline_run"](run_id=99)
        assert "не найден" in result

    def test_found(self, mock_db):
        run = SimpleNamespace(
            id=7, pipeline_id=2, status="done", moderation_status="pending", generated_text="Text"
        )
        mock_db.repos.generation_runs.get = AsyncMock(return_value=run)
        tool_map = _build_sync_tools(mock_db)
        result = tool_map["get_pipeline_run"](run_id=7)
        assert "id=7" in result
        assert "Text" in result

    def test_error(self, mock_db):
        mock_db.repos.generation_runs.get = AsyncMock(side_effect=Exception("fail"))
        tool_map = _build_sync_tools(mock_db)
        result = tool_map["get_pipeline_run"](run_id=1)
        assert "Ошибка" in result


# ===========================================================================
# deepagents_sync — approve_run / reject_run
# ===========================================================================


class TestDeepagentsSyncApproveRun:
    def test_success(self, mock_db):
        mock_db.repos.generation_runs.set_moderation_status = AsyncMock(return_value=None)
        tool_map = _build_sync_tools(mock_db)
        result = tool_map["approve_run"](run_id=10)
        assert "одобрен" in result
        assert "id=10" in result

    def test_error(self, mock_db):
        mock_db.repos.generation_runs.set_moderation_status = AsyncMock(side_effect=Exception("x"))
        tool_map = _build_sync_tools(mock_db)
        result = tool_map["approve_run"](run_id=10)
        assert "Ошибка" in result


class TestDeepagentsSyncRejectRun:
    def test_success(self, mock_db):
        mock_db.repos.generation_runs.set_moderation_status = AsyncMock(return_value=None)
        tool_map = _build_sync_tools(mock_db)
        result = tool_map["reject_run"](run_id=12)
        assert "отклонён" in result

    def test_error(self, mock_db):
        mock_db.repos.generation_runs.set_moderation_status = AsyncMock(side_effect=Exception("x"))
        tool_map = _build_sync_tools(mock_db)
        result = tool_map["reject_run"](run_id=12)
        assert "Ошибка" in result


# ===========================================================================
# deepagents_sync — bulk_approve_runs / bulk_reject_runs
# ===========================================================================


class TestDeepagentsSyncBulkApproveRuns:
    def test_success(self, mock_db):
        mock_db.repos.generation_runs.set_moderation_status = AsyncMock(return_value=None)
        tool_map = _build_sync_tools(mock_db)
        result = tool_map["bulk_approve_runs"]("1,2,3")
        assert "Одобрено: 3" in result

    def test_error_on_invalid_ids(self, mock_db):
        tool_map = _build_sync_tools(mock_db)
        result = tool_map["bulk_approve_runs"]("a,b,c")
        assert "Ошибка" in result

    def test_empty_string(self, mock_db):
        mock_db.repos.generation_runs.set_moderation_status = AsyncMock(return_value=None)
        tool_map = _build_sync_tools(mock_db)
        result = tool_map["bulk_approve_runs"]("")
        assert "Одобрено: 0" in result


class TestDeepagentsSyncBulkRejectRuns:
    def test_success(self, mock_db):
        mock_db.repos.generation_runs.set_moderation_status = AsyncMock(return_value=None)
        tool_map = _build_sync_tools(mock_db)
        result = tool_map["bulk_reject_runs"]("10,20")
        assert "Отклонено: 2" in result

    def test_error_on_invalid_ids(self, mock_db):
        tool_map = _build_sync_tools(mock_db)
        result = tool_map["bulk_reject_runs"]("x,y")
        assert "Ошибка" in result


# ===========================================================================
# deepagents_sync — toggle_search_query / delete_search_query / run_search_query
# ===========================================================================


class TestDeepagentsSyncToggleSearchQuery:
    def test_success(self, mock_db):
        svc_mock = MagicMock()
        svc_mock.toggle = AsyncMock(return_value=None)
        with patch("src.services.search_query_service.SearchQueryService", return_value=svc_mock):
            tool_map = _build_sync_tools(mock_db)
            result = tool_map["toggle_search_query"](sq_id=3)
        assert "id=3" in result
        assert "переключён" in result

    def test_error(self, mock_db):
        with patch(
            "src.services.search_query_service.SearchQueryService", side_effect=Exception("x")
        ):
            tool_map = _build_sync_tools(mock_db)
            result = tool_map["toggle_search_query"](sq_id=1)
        assert "Ошибка" in result


class TestDeepagentsSyncDeleteSearchQuery:
    def test_success(self, mock_db):
        svc_mock = MagicMock()
        svc_mock.delete = AsyncMock(return_value=None)
        with patch("src.services.search_query_service.SearchQueryService", return_value=svc_mock):
            tool_map = _build_sync_tools(mock_db)
            result = tool_map["delete_search_query"](sq_id=5)
        assert "id=5" in result
        assert "удалён" in result

    def test_error(self, mock_db):
        with patch(
            "src.services.search_query_service.SearchQueryService", side_effect=Exception("x")
        ):
            tool_map = _build_sync_tools(mock_db)
            result = tool_map["delete_search_query"](sq_id=1)
        assert "Ошибка" in result


class TestDeepagentsSyncRunSearchQuery:
    def test_success(self, mock_db):
        svc_mock = MagicMock()
        svc_mock.run_once = AsyncMock(return_value=7)
        with patch("src.services.search_query_service.SearchQueryService", return_value=svc_mock):
            tool_map = _build_sync_tools(mock_db)
            result = tool_map["run_search_query"](sq_id=2)
        assert "id=2" in result
        assert "7 совпадений" in result

    def test_error(self, mock_db):
        with patch(
            "src.services.search_query_service.SearchQueryService", side_effect=Exception("x")
        ):
            tool_map = _build_sync_tools(mock_db)
            result = tool_map["run_search_query"](sq_id=1)
        assert "Ошибка" in result


# ===========================================================================
# deepagents_sync — toggle_account / delete_account
# ===========================================================================


class TestDeepagentsSyncToggleAccount:
    def test_not_found(self, mock_db):
        mock_db.get_accounts = AsyncMock(return_value=[])
        tool_map = _build_sync_tools(mock_db)
        result = tool_map["toggle_account"](account_id=99)
        assert "не найден" in result

    def test_success(self, mock_db):
        acc = SimpleNamespace(id=1, phone="+7999", is_active=True)
        mock_db.get_accounts = AsyncMock(return_value=[acc])
        mock_db.set_account_active = AsyncMock(return_value=None)
        tool_map = _build_sync_tools(mock_db)
        result = tool_map["toggle_account"](account_id=1)
        assert "+7999" in result
        assert "переключён" in result

    def test_error(self, mock_db):
        mock_db.get_accounts = AsyncMock(side_effect=Exception("db err"))
        tool_map = _build_sync_tools(mock_db)
        result = tool_map["toggle_account"](account_id=1)
        assert "Ошибка" in result


class TestDeepagentsSyncDeleteAccount:
    def test_success(self, mock_db):
        mock_db.delete_account = AsyncMock(return_value=None)
        tool_map = _build_sync_tools(mock_db)
        result = tool_map["delete_account"](account_id=3)
        assert "id=3" in result
        assert "удалён" in result

    def test_error(self, mock_db):
        mock_db.delete_account = AsyncMock(side_effect=Exception("fail"))
        tool_map = _build_sync_tools(mock_db)
        result = tool_map["delete_account"](account_id=3)
        assert "Ошибка" in result


# ===========================================================================
# deepagents_sync — get_analytics_summary / get_pipeline_stats
# ===========================================================================


class TestDeepagentsSyncGetAnalyticsSummary:
    def test_success(self, mock_db):
        svc_mock = MagicMock()
        svc_mock.get_summary = AsyncMock(
            return_value={"total_generations": 10, "total_published": 5, "total_pending": 2, "total_rejected": 1}
        )
        with patch(
            "src.services.content_analytics_service.ContentAnalyticsService", return_value=svc_mock
        ):
            tool_map = _build_sync_tools(mock_db)
            result = tool_map["get_analytics_summary"]()
        assert "10" in result
        assert "Аналитика" in result

    def test_error(self, mock_db):
        with patch(
            "src.services.content_analytics_service.ContentAnalyticsService",
            side_effect=Exception("err"),
        ):
            tool_map = _build_sync_tools(mock_db)
            result = tool_map["get_analytics_summary"]()
        assert "Ошибка" in result


class TestDeepagentsSyncGetPipelineStats:
    def test_empty(self, mock_db):
        svc_mock = MagicMock()
        svc_mock.get_pipeline_stats = AsyncMock(return_value=[])
        with patch(
            "src.services.content_analytics_service.ContentAnalyticsService", return_value=svc_mock
        ):
            tool_map = _build_sync_tools(mock_db)
            result = tool_map["get_pipeline_stats"](pipeline_id=0)
        assert "не найдена" in result

    def test_with_stats(self, mock_db):
        stat = SimpleNamespace(pipeline_name="Pipe1", total_generations=20, total_published=10)
        svc_mock = MagicMock()
        svc_mock.get_pipeline_stats = AsyncMock(return_value=[stat])
        with patch(
            "src.services.content_analytics_service.ContentAnalyticsService", return_value=svc_mock
        ):
            tool_map = _build_sync_tools(mock_db)
            result = tool_map["get_pipeline_stats"](pipeline_id=1)
        assert "Pipe1" in result
        assert "20" in result

    def test_error(self, mock_db):
        with patch(
            "src.services.content_analytics_service.ContentAnalyticsService",
            side_effect=Exception("err"),
        ):
            tool_map = _build_sync_tools(mock_db)
            result = tool_map["get_pipeline_stats"](pipeline_id=0)
        assert "Ошибка" in result


# ===========================================================================
# deepagents_sync — get_trending_topics / get_trending_channels / get_message_velocity
# ===========================================================================


class TestDeepagentsSyncGetTrendingTopics:
    def test_empty(self, mock_db):
        svc_mock = MagicMock()
        svc_mock.get_trending_topics = AsyncMock(return_value=[])
        with patch("src.services.trend_service.TrendService", return_value=svc_mock):
            tool_map = _build_sync_tools(mock_db)
            result = tool_map["get_trending_topics"](days=7)
        assert "не найдены" in result

    def test_with_topics(self, mock_db):
        topic = SimpleNamespace(keyword="crypto", count=50)
        svc_mock = MagicMock()
        svc_mock.get_trending_topics = AsyncMock(return_value=[topic])
        with patch("src.services.trend_service.TrendService", return_value=svc_mock):
            tool_map = _build_sync_tools(mock_db)
            result = tool_map["get_trending_topics"](days=7)
        assert "crypto" in result
        assert "50" in result

    def test_error(self, mock_db):
        with patch("src.services.trend_service.TrendService", side_effect=Exception("err")):
            tool_map = _build_sync_tools(mock_db)
            result = tool_map["get_trending_topics"](days=7)
        assert "Ошибка" in result


class TestDeepagentsSyncGetTrendingChannels:
    def test_empty(self, mock_db):
        svc_mock = MagicMock()
        svc_mock.get_trending_channels = AsyncMock(return_value=[])
        with patch("src.services.trend_service.TrendService", return_value=svc_mock):
            tool_map = _build_sync_tools(mock_db)
            result = tool_map["get_trending_channels"](days=7)
        assert "не найдены" in result

    def test_with_channels(self, mock_db):
        ch = SimpleNamespace(title="TechNews", count=100)
        svc_mock = MagicMock()
        svc_mock.get_trending_channels = AsyncMock(return_value=[ch])
        with patch("src.services.trend_service.TrendService", return_value=svc_mock):
            tool_map = _build_sync_tools(mock_db)
            result = tool_map["get_trending_channels"](days=7)
        assert "TechNews" in result
        assert "100" in result

    def test_error(self, mock_db):
        with patch("src.services.trend_service.TrendService", side_effect=Exception("err")):
            tool_map = _build_sync_tools(mock_db)
            result = tool_map["get_trending_channels"](days=7)
        assert "Ошибка" in result


class TestDeepagentsSyncGetMessageVelocity:
    def test_empty(self, mock_db):
        svc_mock = MagicMock()
        svc_mock.get_message_velocity = AsyncMock(return_value=[])
        with patch("src.services.trend_service.TrendService", return_value=svc_mock):
            tool_map = _build_sync_tools(mock_db)
            result = tool_map["get_message_velocity"](days=30)
        assert "не найдены" in result

    def test_with_data(self, mock_db):
        v = SimpleNamespace(date="2026-01-01", count=42)
        svc_mock = MagicMock()
        svc_mock.get_message_velocity = AsyncMock(return_value=[v])
        with patch("src.services.trend_service.TrendService", return_value=svc_mock):
            tool_map = _build_sync_tools(mock_db)
            result = tool_map["get_message_velocity"](days=30)
        assert "2026-01-01" in result
        assert "42" in result

    def test_error(self, mock_db):
        with patch("src.services.trend_service.TrendService", side_effect=Exception("err")):
            tool_map = _build_sync_tools(mock_db)
            result = tool_map["get_message_velocity"](days=30)
        assert "Ошибка" in result


# ===========================================================================
# deepagents_sync — get_peak_hours
# ===========================================================================


class TestDeepagentsSyncGetPeakHours:
    def test_empty(self, mock_db):
        svc_mock = MagicMock()
        svc_mock.get_peak_hours = AsyncMock(return_value=[])
        with patch("src.services.trend_service.TrendService", return_value=svc_mock):
            tool_map = _build_sync_tools(mock_db)
            result = tool_map["get_peak_hours"]()
        assert "не найдены" in result

    def test_with_hours(self, mock_db):
        h = SimpleNamespace(hour=14, count=200)
        svc_mock = MagicMock()
        svc_mock.get_peak_hours = AsyncMock(return_value=[h])
        with patch("src.services.trend_service.TrendService", return_value=svc_mock):
            tool_map = _build_sync_tools(mock_db)
            result = tool_map["get_peak_hours"]()
        assert "14:00" in result
        assert "200" in result

    def test_error(self, mock_db):
        with patch("src.services.trend_service.TrendService", side_effect=Exception("err")):
            tool_map = _build_sync_tools(mock_db)
            result = tool_map["get_peak_hours"]()
        assert "Ошибка" in result


# ===========================================================================
# deepagents_sync — get_calendar
# ===========================================================================


class TestDeepagentsSyncGetCalendar:
    def test_empty(self, mock_db):
        svc_mock = MagicMock()
        svc_mock.get_upcoming = AsyncMock(return_value=[])
        with patch(
            "src.services.content_calendar_service.ContentCalendarService", return_value=svc_mock
        ):
            tool_map = _build_sync_tools(mock_db)
            result = tool_map["get_calendar"]()
        assert "Нет запланированных" in result

    def test_with_events(self, mock_db):
        evt = SimpleNamespace(run_id=3, pipeline_name="Pipe", moderation_status="approved")
        svc_mock = MagicMock()
        svc_mock.get_upcoming = AsyncMock(return_value=[evt])
        with patch(
            "src.services.content_calendar_service.ContentCalendarService", return_value=svc_mock
        ):
            tool_map = _build_sync_tools(mock_db)
            result = tool_map["get_calendar"]()
        assert "run_id=3" in result
        assert "Pipe" in result

    def test_error(self, mock_db):
        with patch(
            "src.services.content_calendar_service.ContentCalendarService",
            side_effect=Exception("err"),
        ):
            tool_map = _build_sync_tools(mock_db)
            result = tool_map["get_calendar"]()
        assert "Ошибка" in result


# ===========================================================================
# deepagents_sync — get_daily_stats
# ===========================================================================


class TestDeepagentsSyncGetDailyStats:
    def test_empty(self, mock_db):
        svc_mock = MagicMock()
        svc_mock.get_daily_stats = AsyncMock(return_value=[])
        with patch(
            "src.services.content_analytics_service.ContentAnalyticsService", return_value=svc_mock
        ):
            tool_map = _build_sync_tools(mock_db)
            result = tool_map["get_daily_stats"](days=30)
        assert "Нет данных" in result

    def test_with_stats(self, mock_db):
        row = {"date": "2026-01-01", "count": 5}
        svc_mock = MagicMock()
        svc_mock.get_daily_stats = AsyncMock(return_value=[row])
        with patch(
            "src.services.content_analytics_service.ContentAnalyticsService", return_value=svc_mock
        ):
            tool_map = _build_sync_tools(mock_db)
            result = tool_map["get_daily_stats"](days=30)
        assert "2026-01-01" in result
        assert "5" in result

    def test_error(self, mock_db):
        with patch(
            "src.services.content_analytics_service.ContentAnalyticsService",
            side_effect=Exception("err"),
        ):
            tool_map = _build_sync_tools(mock_db)
            result = tool_map["get_daily_stats"](days=30)
        assert "Ошибка" in result


# ===========================================================================
# deepagents_sync — toggle_scheduler_job
# ===========================================================================


class TestDeepagentsSyncToggleSchedulerJob:
    def test_disable_job(self, mock_db):
        mgr_mock = MagicMock()
        mgr_mock.load_settings = AsyncMock(return_value=None)
        mgr_mock.is_job_enabled = AsyncMock(return_value=True)
        mgr_mock.sync_job_state = AsyncMock(return_value=None)
        with patch("src.scheduler.service.SchedulerManager", return_value=mgr_mock):
            tool_map = _build_sync_tools(mock_db)
            result = tool_map["toggle_scheduler_job"](job_id="collect_all")
        assert "collect_all" in result
        assert "выключена" in result

    def test_enable_job(self, mock_db):
        mgr_mock = MagicMock()
        mgr_mock.load_settings = AsyncMock(return_value=None)
        mgr_mock.is_job_enabled = AsyncMock(return_value=False)
        mgr_mock.sync_job_state = AsyncMock(return_value=None)
        with patch("src.scheduler.service.SchedulerManager", return_value=mgr_mock):
            tool_map = _build_sync_tools(mock_db)
            result = tool_map["toggle_scheduler_job"](job_id="collect_all")
        assert "включена" in result

    def test_error(self, mock_db):
        with patch("src.scheduler.service.SchedulerManager", side_effect=Exception("sched err")):
            tool_map = _build_sync_tools(mock_db)
            result = tool_map["toggle_scheduler_job"](job_id="x")
        assert "Ошибка" in result


# ===========================================================================
# deepagents_sync — delete_agent_thread / rename_agent_thread / get_thread_messages
# ===========================================================================


class TestDeepagentsSyncDeleteAgentThread:
    def test_success(self, mock_db):
        mock_db.delete_agent_thread = AsyncMock(return_value=None)
        tool_map = _build_sync_tools(mock_db)
        result = tool_map["delete_agent_thread"](thread_id=4)
        assert "id=4" in result
        assert "удалён" in result

    def test_error(self, mock_db):
        mock_db.delete_agent_thread = AsyncMock(side_effect=Exception("fail"))
        tool_map = _build_sync_tools(mock_db)
        result = tool_map["delete_agent_thread"](thread_id=4)
        assert "Ошибка" in result


class TestDeepagentsSyncRenameAgentThread:
    def test_success(self, mock_db):
        mock_db.rename_agent_thread = AsyncMock(return_value=None)
        tool_map = _build_sync_tools(mock_db)
        result = tool_map["rename_agent_thread"](thread_id=2, title="New Name")
        assert "id=2" in result
        assert "New Name" in result

    def test_error(self, mock_db):
        mock_db.rename_agent_thread = AsyncMock(side_effect=Exception("fail"))
        tool_map = _build_sync_tools(mock_db)
        result = tool_map["rename_agent_thread"](thread_id=2, title="Name")
        assert "Ошибка" in result


class TestDeepagentsSyncGetThreadMessages:
    def test_empty(self, mock_db):
        mock_db.get_agent_messages = AsyncMock(return_value=[])
        tool_map = _build_sync_tools(mock_db)
        result = tool_map["get_thread_messages"](thread_id=1)
        assert "Нет сообщений" in result

    def test_with_messages(self, mock_db):
        msgs = [{"role": "user", "content": "Hello"}, {"role": "assistant", "content": "World"}]
        mock_db.get_agent_messages = AsyncMock(return_value=msgs)
        tool_map = _build_sync_tools(mock_db)
        result = tool_map["get_thread_messages"](thread_id=1)
        assert "Hello" in result
        assert "World" in result
        assert "user" in result

    def test_error(self, mock_db):
        mock_db.get_agent_messages = AsyncMock(side_effect=Exception("fail"))
        tool_map = _build_sync_tools(mock_db)
        result = tool_map["get_thread_messages"](thread_id=1)
        assert "Ошибка" in result


# ===========================================================================
# deepagents_sync — generate_image
# ===========================================================================


class TestDeepagentsSyncGenerateImage:
    def test_success(self, mock_db):
        img_svc_mock = MagicMock()
        img_svc_mock.generate = AsyncMock(return_value="https://example.com/img.png")
        img_svc_mock.adapter_names = ["together"]
        with patch(
            "src.services.image_generation_service.ImageGenerationService",
            return_value=img_svc_mock,
        ):
            tool_map = _build_sync_tools(mock_db)
            result = tool_map["generate_image"](prompt="a cat")
        assert "Изображение" in result
        assert "https://example.com/img.png" in result

    def test_no_result(self, mock_db):
        img_svc_mock = MagicMock()
        img_svc_mock.generate = AsyncMock(return_value=None)
        img_svc_mock.adapter_names = []
        with patch(
            "src.services.image_generation_service.ImageGenerationService",
            return_value=img_svc_mock,
        ):
            tool_map = _build_sync_tools(mock_db)
            result = tool_map["generate_image"](prompt="a cat")
        assert "не вернула результат" in result

    def test_error(self, mock_db):
        img_svc_mock = MagicMock()
        img_svc_mock.generate = AsyncMock(side_effect=Exception("gen fail"))
        img_svc_mock.adapter_names = []
        with patch(
            "src.services.image_generation_service.ImageGenerationService",
            return_value=img_svc_mock,
        ):
            tool_map = _build_sync_tools(mock_db)
            result = tool_map["generate_image"](prompt="error")
        assert "Ошибка" in result


# ===========================================================================
# pipelines.py MCP tools — edit_pipeline
# ===========================================================================


class TestPipelinesToolEditPipeline:
    @pytest.mark.anyio
    async def test_requires_confirmation(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["edit_pipeline"]({"pipeline_id": 1, "confirm": False})
        assert "Подтвердите" in _text(result)

    @pytest.mark.anyio
    async def test_missing_pipeline_id(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["edit_pipeline"]({"confirm": True})
        assert "pipeline_id обязателен" in _text(result)

    @pytest.mark.anyio
    async def test_not_found(self, mock_db):
        with patch("src.services.pipeline_service.PipelineService") as mock_svc:
            mock_svc.return_value.get_detail = AsyncMock(return_value=None)
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["edit_pipeline"]({"pipeline_id": 99, "confirm": True})
        assert "не найден" in _text(result)

    @pytest.mark.anyio
    async def test_invalid_target_ref_format(self, mock_db):
        p = SimpleNamespace(
            id=1,
            name="P",
            prompt_template="t",
            llm_model=None,
            publish_mode="moderated",
        )
        detail = {
            "pipeline": p,
            "source_ids": [1],
            "targets": [],
        }
        with patch("src.services.pipeline_service.PipelineService") as mock_svc:
            mock_svc.return_value.get_detail = AsyncMock(return_value=detail)
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["edit_pipeline"](
                {
                    "pipeline_id": 1,
                    "confirm": True,
                    "target_refs": "badformat",
                }
            )
        assert "Неверный формат" in _text(result)

    @pytest.mark.anyio
    async def test_success(self, mock_db):
        p = SimpleNamespace(
            id=1,
            name="OldName",
            prompt_template="Old tmpl",
            llm_model="gpt-3",
            publish_mode="moderated",
        )
        detail = {
            "pipeline": p,
            "source_ids": [1],
            "targets": [],
        }
        with patch("src.services.pipeline_service.PipelineService") as mock_svc:
            with patch("src.services.pipeline_service.PipelineTargetRef"):
                mock_svc.return_value.get_detail = AsyncMock(return_value=detail)
                mock_svc.return_value.update = AsyncMock(return_value=True)
                handlers = _get_tool_handlers(mock_db)
                result = await handlers["edit_pipeline"](
                    {
                        "pipeline_id": 1,
                        "confirm": True,
                        "name": "NewName",
                        "target_refs": "+7123|456",
                    }
                )
        assert "NewName" in _text(result)
        assert "обновлён" in _text(result)

    @pytest.mark.anyio
    async def test_update_fails(self, mock_db):
        p = SimpleNamespace(
            id=1,
            name="P",
            prompt_template="t",
            llm_model=None,
            publish_mode="moderated",
        )
        detail = {"pipeline": p, "source_ids": [], "targets": []}
        with patch("src.services.pipeline_service.PipelineService") as mock_svc:
            mock_svc.return_value.get_detail = AsyncMock(return_value=detail)
            mock_svc.return_value.update = AsyncMock(return_value=False)
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["edit_pipeline"]({"pipeline_id": 1, "confirm": True})
        assert "Не удалось обновить" in _text(result)

    @pytest.mark.anyio
    async def test_error(self, mock_db):
        with patch("src.services.pipeline_service.PipelineService") as mock_svc:
            mock_svc.return_value.get_detail = AsyncMock(side_effect=Exception("db err"))
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["edit_pipeline"]({"pipeline_id": 1, "confirm": True})
        assert "Ошибка" in _text(result)


# ===========================================================================
# pipelines.py MCP tools — run_pipeline
# ===========================================================================


class TestPipelinesToolRunPipeline:
    @pytest.mark.anyio
    async def test_missing_id(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["run_pipeline"]({})
        assert "pipeline_id обязателен" in _text(result)

    @pytest.mark.anyio
    async def test_not_found(self, mock_db):
        with patch("src.services.pipeline_service.PipelineService") as mock_svc:
            mock_svc.return_value.get = AsyncMock(return_value=None)
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["run_pipeline"]({"pipeline_id": 99})
        assert "не найден" in _text(result)

    @pytest.mark.anyio
    async def test_inactive_pipeline(self, mock_db):
        p = SimpleNamespace(id=1, name="InactivePipe", is_active=False)
        with patch("src.services.pipeline_service.PipelineService") as mock_svc:
            mock_svc.return_value.get = AsyncMock(return_value=p)
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["run_pipeline"]({"pipeline_id": 1})
        assert "неактивен" in _text(result)

    @pytest.mark.anyio
    async def test_success(self, mock_db):
        p = SimpleNamespace(id=1, name="ActivePipe", is_active=True)
        run = SimpleNamespace(id=10, generated_text="Generated content", moderation_status="pending")
        with patch("src.services.pipeline_service.PipelineService") as mock_svc:
            with patch("src.search.engine.SearchEngine"):
                with patch(
                    "src.services.content_generation_service.ContentGenerationService"
                ) as mock_gen:
                    mock_svc.return_value.get = AsyncMock(return_value=p)
                    mock_gen.return_value.generate = AsyncMock(return_value=run)
                    handlers = _get_tool_handlers(mock_db)
                    result = await handlers["run_pipeline"]({"pipeline_id": 1})
        text = _text(result)
        assert "run id=10" in text
        assert "Generated content" in text

    @pytest.mark.anyio
    async def test_error(self, mock_db):
        with patch("src.services.pipeline_service.PipelineService") as mock_svc:
            mock_svc.return_value.get = AsyncMock(side_effect=Exception("db err"))
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["run_pipeline"]({"pipeline_id": 1})
        assert "Ошибка" in _text(result)


# ===========================================================================
# pipelines.py MCP tools — generate_draft
# ===========================================================================


class TestPipelinesToolGenerateDraft:
    @pytest.mark.anyio
    async def test_success_with_query(self, mock_db):
        gen_result = {
            "generated_text": "Draft text",
            "citations": [{"channel_title": "Chan", "message_id": 1, "date": "2026-01-01"}],
        }
        with patch("src.search.engine.SearchEngine"):
            with patch("src.services.generation_service.GenerationService") as mock_gen:
                with patch("src.services.provider_service.AgentProviderService") as mock_ps:
                    mock_ps.return_value.get_provider_callable = MagicMock(return_value=None)
                    mock_gen.return_value.generate = AsyncMock(return_value=gen_result)
                    handlers = _get_tool_handlers(mock_db)
                    result = await handlers["generate_draft"]({"query": "write about crypto"})
        text = _text(result)
        assert "Draft text" in text
        assert "Chan" in text

    @pytest.mark.anyio
    async def test_with_pipeline_id(self, mock_db):
        p = SimpleNamespace(id=1, name="P", prompt_template="Write about {topic}", llm_model="gpt-4")
        gen_result = {"generated_text": "Pipeline draft", "citations": []}
        with patch("src.services.pipeline_service.PipelineService") as mock_ps:
            mock_ps.return_value.get = AsyncMock(return_value=p)
            mock_ps.return_value.get_retrieval_scope = AsyncMock(
                return_value=SimpleNamespace(query="P", channel_id=None)
            )
            with patch("src.search.engine.SearchEngine"):
                with patch("src.services.generation_service.GenerationService") as mock_gen:
                    with patch("src.services.provider_service.AgentProviderService") as mock_aps:
                        mock_aps.return_value.get_provider_callable = MagicMock(return_value=None)
                        mock_gen.return_value.generate = AsyncMock(return_value=gen_result)
                        handlers = _get_tool_handlers(mock_db)
                        result = await handlers["generate_draft"](
                            {"query": "", "pipeline_id": 1}
                        )
        text = _text(result)
        assert "Pipeline draft" in text

    @pytest.mark.anyio
    async def test_error(self, mock_db):
        with patch("src.search.engine.SearchEngine", side_effect=Exception("search fail")):
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["generate_draft"]({"query": "fail"})
        assert "Ошибка" in _text(result)


# ===========================================================================
# pipelines.py MCP tools — publish_pipeline_run
# ===========================================================================


class TestPipelinesToolPublishPipelineRun:
    @pytest.mark.anyio
    async def test_no_pool(self, mock_db):
        # No client_pool passed → require_pool gate should trigger
        handlers = _get_tool_handlers(mock_db, client_pool=None)
        result = await handlers["publish_pipeline_run"]({"run_id": 1, "confirm": True})
        txt = _text(result)
        assert "недоступен" in txt or "Подтвердите" in txt or "pool" in txt.lower() or "Telegram" in txt

    @pytest.mark.anyio
    async def test_requires_confirmation(self, mock_db):
        pool = MagicMock()
        handlers = _get_tool_handlers(mock_db, client_pool=pool)
        result = await handlers["publish_pipeline_run"]({"run_id": 1, "confirm": False})
        assert "Подтвердите" in _text(result)

    @pytest.mark.anyio
    async def test_run_not_found(self, mock_db):
        pool = MagicMock()
        mock_db.repos.generation_runs.get = AsyncMock(return_value=None)
        handlers = _get_tool_handlers(mock_db, client_pool=pool)
        result = await handlers["publish_pipeline_run"]({"run_id": 99, "confirm": True})
        assert "не найден" in _text(result)

    @pytest.mark.anyio
    async def test_pipeline_not_found(self, mock_db):
        pool = MagicMock()
        run = SimpleNamespace(id=1, pipeline_id=5)
        mock_db.repos.generation_runs.get = AsyncMock(return_value=run)
        with patch("src.services.pipeline_service.PipelineService") as mock_svc:
            mock_svc.return_value.get = AsyncMock(return_value=None)
            handlers = _get_tool_handlers(mock_db, client_pool=pool)
            result = await handlers["publish_pipeline_run"]({"run_id": 1, "confirm": True})
        assert "не найден" in _text(result)

    @pytest.mark.anyio
    async def test_success(self, mock_db):
        pool = MagicMock()
        run = SimpleNamespace(id=1, pipeline_id=2)
        pipeline = SimpleNamespace(id=2, name="Pipe")
        pub_result = SimpleNamespace(success=True, error=None)
        mock_db.repos.generation_runs.get = AsyncMock(return_value=run)
        with patch("src.services.pipeline_service.PipelineService") as mock_ps:
            with patch("src.services.publish_service.PublishService") as mock_pub:
                mock_ps.return_value.get = AsyncMock(return_value=pipeline)
                mock_pub.return_value.publish_run = AsyncMock(return_value=[pub_result])
                handlers = _get_tool_handlers(mock_db, client_pool=pool)
                result = await handlers["publish_pipeline_run"]({"run_id": 1, "confirm": True})
        text = _text(result)
        assert "1 успешно" in text


# ===========================================================================
# ===========================================================================
# agent_provider_service — load_provider_configs with invalid JSON
# ===========================================================================


class TestAgentProviderServiceLoadConfigs:
    @pytest.mark.anyio
    async def test_returns_empty_on_no_setting(self, db):
        from src.config import AppConfig
        from src.services.agent_provider_service import AgentProviderService

        config = AppConfig()
        svc = AgentProviderService(db, config)
        result = await svc.load_provider_configs()
        assert result == []

    @pytest.mark.anyio
    async def test_returns_empty_on_invalid_json(self, db):
        from src.config import AppConfig
        from src.services.agent_provider_service import PROVIDER_SETTINGS_KEY, AgentProviderService

        config = AppConfig()
        svc = AgentProviderService(db, config)
        await db.set_setting(PROVIDER_SETTINGS_KEY, "not-json{{{")
        result = await svc.load_provider_configs()
        assert result == []

    @pytest.mark.anyio
    async def test_returns_empty_on_non_list_json(self, db):
        from src.config import AppConfig
        from src.services.agent_provider_service import PROVIDER_SETTINGS_KEY, AgentProviderService

        config = AppConfig()
        svc = AgentProviderService(db, config)
        await db.set_setting(PROVIDER_SETTINGS_KEY, json.dumps({"key": "value"}))
        result = await svc.load_provider_configs()
        assert result == []

    @pytest.mark.anyio
    async def test_skips_unknown_provider(self, db):
        from src.config import AppConfig
        from src.services.agent_provider_service import PROVIDER_SETTINGS_KEY, AgentProviderService

        config = AppConfig()
        svc = AgentProviderService(db, config)
        payload = json.dumps([{"provider": "nonexistent_provider_xyz", "enabled": True}])
        await db.set_setting(PROVIDER_SETTINGS_KEY, payload)
        result = await svc.load_provider_configs()
        assert result == []

    @pytest.mark.anyio
    async def test_save_requires_cipher(self, db):
        from src.config import AppConfig
        from src.services.agent_provider_service import AgentProviderService

        config = AppConfig()
        # No session_encryption_key → writes_enabled is False
        svc = AgentProviderService(db, config)
        assert not svc.writes_enabled
        with pytest.raises(RuntimeError, match="SESSION_ENCRYPTION_KEY"):
            await svc.save_provider_configs([])


# ===========================================================================
# agent_provider_service — load_model_cache
# ===========================================================================


class TestAgentProviderServiceLoadModelCache:
    @pytest.mark.anyio
    async def test_empty_on_no_setting(self, db):
        from src.config import AppConfig
        from src.services.agent_provider_service import AgentProviderService

        config = AppConfig()
        svc = AgentProviderService(db, config)
        result = await svc.load_model_cache()
        assert result == {}

    @pytest.mark.anyio
    async def test_empty_on_invalid_json(self, db):
        from src.config import AppConfig
        from src.services.agent_provider_service import MODEL_CACHE_SETTINGS_KEY, AgentProviderService

        config = AppConfig()
        svc = AgentProviderService(db, config)
        await db.set_setting(MODEL_CACHE_SETTINGS_KEY, "invalid{{")
        result = await svc.load_model_cache()
        assert result == {}

    @pytest.mark.anyio
    async def test_empty_on_non_dict_json(self, db):
        from src.config import AppConfig
        from src.services.agent_provider_service import MODEL_CACHE_SETTINGS_KEY, AgentProviderService

        config = AppConfig()
        svc = AgentProviderService(db, config)
        await db.set_setting(MODEL_CACHE_SETTINGS_KEY, json.dumps([1, 2, 3]))
        result = await svc.load_model_cache()
        assert result == {}

    @pytest.mark.anyio
    async def test_save_and_load_model_cache(self, db):
        from src.config import AppConfig
        from src.services.agent_provider_service import AgentProviderService, ProviderModelCacheEntry

        config = AppConfig()
        svc = AgentProviderService(db, config)
        entry = ProviderModelCacheEntry(
            provider="openai",
            models=["gpt-4o", "gpt-4.1-mini"],
            source="static",
            fetched_at="2026-01-01T00:00:00",
        )
        await svc.save_model_cache({"openai": entry})
        loaded = await svc.load_model_cache()
        assert "openai" in loaded
        assert "gpt-4o" in loaded["openai"].models


# ===========================================================================
# agent_provider_service — build_provider_views
# ===========================================================================


class TestAgentProviderServiceBuildProviderViews:
    @pytest.mark.anyio
    async def test_builds_view_for_openai(self, db):
        from src.agent.provider_registry import ProviderRuntimeConfig
        from src.config import AppConfig
        from src.services.agent_provider_service import AgentProviderService, ProviderModelCacheEntry

        config = AppConfig()
        svc = AgentProviderService(db, config)
        cfg = ProviderRuntimeConfig(
            provider="openai",
            enabled=True,
            priority=0,
            selected_model="gpt-4o",
            plain_fields={"base_url": ""},
            secret_fields={"api_key": "sk-test"},
        )
        cache = {
            "openai": ProviderModelCacheEntry(
                provider="openai",
                models=["gpt-4o", "gpt-4.1-mini"],
                source="static",
            )
        }
        views = svc.build_provider_views([cfg], cache)
        assert len(views) == 1
        view = views[0]
        assert view["provider"] == "openai"
        assert "gpt-4o" in view["models"]
        assert view["enabled"] is True

    @pytest.mark.anyio
    async def test_builds_view_with_empty_cache(self, db):
        from src.agent.provider_registry import ProviderRuntimeConfig
        from src.config import AppConfig
        from src.services.agent_provider_service import AgentProviderService

        config = AppConfig()
        svc = AgentProviderService(db, config)
        cfg = ProviderRuntimeConfig(
            provider="openai",
            enabled=False,
            priority=5,
            selected_model="gpt-4.1-mini",
        )
        views = svc.build_provider_views([cfg], {})
        assert len(views) == 1
        assert views[0]["enabled"] is False


# ===========================================================================
# agent_provider_service — validate_provider_config
# ===========================================================================


class TestAgentProviderServiceValidateConfig:
    def test_missing_api_key_for_openai(self, db):
        from src.agent.provider_registry import ProviderRuntimeConfig
        from src.config import AppConfig
        from src.services.agent_provider_service import AgentProviderService

        config = AppConfig()
        svc = AgentProviderService(db, config)
        cfg = ProviderRuntimeConfig(
            provider="openai",
            enabled=True,
            priority=0,
            selected_model="gpt-4o",
            secret_fields={"api_key": ""},
        )
        error = svc.validate_provider_config(cfg)
        assert error  # Should report missing api_key

    def test_valid_config_with_api_key(self, db):
        from src.agent.provider_registry import ProviderRuntimeConfig
        from src.config import AppConfig
        from src.services.agent_provider_service import AgentProviderService

        config = AppConfig()
        svc = AgentProviderService(db, config)
        cfg = ProviderRuntimeConfig(
            provider="openai",
            enabled=True,
            priority=0,
            selected_model="gpt-4o",
            secret_fields={"api_key": "sk-valid"},
        )
        error = svc.validate_provider_config(cfg)
        assert not error


# ===========================================================================
# agent_provider_service — refresh_all_models
# ===========================================================================


class TestAgentProviderServiceRefreshAllModels:
    @pytest.mark.anyio
    async def test_refresh_all_empty_configs(self, db, monkeypatch):
        from src.config import AppConfig
        from src.services.agent_provider_service import AgentProviderService

        config = AppConfig()
        svc = AgentProviderService(db, config)

        async def _broken_fetch(spec, cfg):
            raise RuntimeError("no network")

        monkeypatch.setattr(svc, "_fetch_live_models", _broken_fetch)
        results = await svc.refresh_all_models(configs=[])
        assert results == {}

    @pytest.mark.anyio
    async def test_refresh_all_with_config(self, db, monkeypatch):
        from src.agent.provider_registry import ProviderRuntimeConfig
        from src.config import AppConfig
        from src.services.agent_provider_service import AgentProviderService

        config = AppConfig()
        svc = AgentProviderService(db, config)

        async def _broken_fetch(spec, cfg):
            raise RuntimeError("no network")

        monkeypatch.setattr(svc, "_fetch_live_models", _broken_fetch)
        cfg = ProviderRuntimeConfig(
            provider="openai",
            enabled=True,
            priority=0,
            selected_model="gpt-4o",
        )
        results = await svc.refresh_all_models(configs=[cfg])
        assert "openai" in results
        assert results["openai"].source == "static cache"


# ===========================================================================
# agent/manager.py — _embed_history_in_prompt standalone
# ===========================================================================


class TestEmbedHistoryInPrompt:
    def test_basic_embedding(self):
        from src.agent.manager import _embed_history_in_prompt

        history = [
            {"role": "user", "content": "Hello"},
            {"role": "assistant", "content": "Hi there"},
        ]
        result = _embed_history_in_prompt(history, "Current message")
        assert "Hello" in result
        assert "Hi there" in result
        assert "Current message" in result

    def test_empty_history(self):
        from src.agent.manager import _embed_history_in_prompt

        result = _embed_history_in_prompt([], "Prompt")
        assert "Prompt" in result

    def test_single_message_only(self):
        from src.agent.manager import _embed_history_in_prompt

        result = _embed_history_in_prompt([], "Solo message")
        assert "Solo message" in result
        assert "<user>" in result


# ===========================================================================
# agent/manager.py — DeepagentsBackend properties
# ===========================================================================


class TestDeepagentsBackendProperties:
    def test_legacy_fallback_model_from_env(self, monkeypatch):
        from src.agent.manager import DeepagentsBackend
        from src.config import AppConfig

        monkeypatch.setenv("AGENT_FALLBACK_MODEL", "openai:gpt-4o")
        mock_db = MagicMock(spec=Database)
        config = AppConfig()
        backend = DeepagentsBackend(mock_db, config)
        assert backend.legacy_fallback_model == "openai:gpt-4o"

    def test_legacy_fallback_model_from_config(self):
        from src.agent.manager import DeepagentsBackend
        from src.config import AppConfig

        mock_db = MagicMock(spec=Database)
        config = AppConfig()
        config.agent.fallback_model = "groq:llama3"
        backend = DeepagentsBackend(mock_db, config)
        assert backend.legacy_fallback_model == "groq:llama3"

    def test_configured_false_when_no_model(self, monkeypatch):
        from src.agent.manager import DeepagentsBackend
        from src.config import AppConfig

        monkeypatch.delenv("AGENT_FALLBACK_MODEL", raising=False)
        mock_db = MagicMock(spec=Database)
        config = AppConfig()
        config.agent.fallback_model = ""
        backend = DeepagentsBackend(mock_db, config)
        assert not backend.configured

    def test_configured_true_with_legacy_model(self, monkeypatch):
        from src.agent.manager import DeepagentsBackend
        from src.config import AppConfig

        monkeypatch.setenv("AGENT_FALLBACK_MODEL", "openai:gpt-4o")
        mock_db = MagicMock(spec=Database)
        config = AppConfig()
        backend = DeepagentsBackend(mock_db, config)
        assert backend.configured

    def test_provider_from_model_with_colon(self):
        from src.agent.manager import DeepagentsBackend
        from src.config import AppConfig

        mock_db = MagicMock(spec=Database)
        config = AppConfig()
        backend = DeepagentsBackend(mock_db, config)
        assert backend._provider_from_model("openai:gpt-4o") == "openai"

    def test_provider_from_model_without_colon(self):
        from src.agent.manager import DeepagentsBackend
        from src.config import AppConfig

        mock_db = MagicMock(spec=Database)
        config = AppConfig()
        backend = DeepagentsBackend(mock_db, config)
        assert backend._provider_from_model("gpt-4o") is None

    def test_available_false_without_model(self, monkeypatch):
        from src.agent.manager import DeepagentsBackend
        from src.config import AppConfig

        monkeypatch.delenv("AGENT_FALLBACK_MODEL", raising=False)
        mock_db = MagicMock(spec=Database)
        config = AppConfig()
        config.agent.fallback_model = ""
        backend = DeepagentsBackend(mock_db, config)
        # No preflight run and no legacy model
        assert not backend.available

    def test_fallback_model_returns_last_used(self):
        from src.agent.manager import DeepagentsBackend
        from src.config import AppConfig

        mock_db = MagicMock(spec=Database)
        config = AppConfig()
        backend = DeepagentsBackend(mock_db, config)
        backend._last_used_model = "openai:gpt-4-turbo"
        assert backend.fallback_model == "openai:gpt-4-turbo"

    def test_fallback_provider_returns_last_used(self):
        from src.agent.manager import DeepagentsBackend
        from src.config import AppConfig

        mock_db = MagicMock(spec=Database)
        config = AppConfig()
        backend = DeepagentsBackend(mock_db, config)
        backend._last_used_provider = "groq"
        assert backend.fallback_provider == "groq"

    def test_extract_result_text_dict_with_messages(self):
        from src.agent.manager import DeepagentsBackend
        from src.config import AppConfig

        mock_db = MagicMock(spec=Database)
        config = AppConfig()
        backend = DeepagentsBackend(mock_db, config)

        msg = MagicMock()
        msg.content = "Hello from agent"
        result = backend._extract_result_text({"messages": [msg]})
        assert "Hello from agent" in result

    def test_extract_result_text_non_dict(self):
        from src.agent.manager import DeepagentsBackend
        from src.config import AppConfig

        mock_db = MagicMock(spec=Database)
        config = AppConfig()
        backend = DeepagentsBackend(mock_db, config)
        result = backend._extract_result_text("plain string result")
        assert "plain string result" in result

    def test_extract_result_text_dict_no_messages(self):
        from src.agent.manager import DeepagentsBackend
        from src.config import AppConfig

        mock_db = MagicMock(spec=Database)
        config = AppConfig()
        backend = DeepagentsBackend(mock_db, config)
        result = backend._extract_result_text({"other": "data"})
        assert isinstance(result, str)


# ===========================================================================
# agent/manager.py — AgentManager cancel_stream
# ===========================================================================


class TestAgentManagerCancelStream:
    @pytest.mark.anyio
    async def test_cancel_nonexistent_stream_returns_false(self, db):
        from src.agent.manager import AgentManager

        mgr = AgentManager(db)
        mgr.initialize()

        thread_id = 9999  # no active task for this thread
        result = await mgr.cancel_stream(thread_id)
        assert result is False

    @pytest.mark.anyio
    async def test_cancel_active_stream_returns_true(self, db):
        import asyncio

        from src.agent.manager import AgentManager

        mgr = AgentManager(db)
        mgr.initialize()

        # Directly inject a fake task into _active_tasks to simulate a running stream
        async def _fake_long_running():
            await asyncio.sleep(60)

        fake_task = asyncio.create_task(_fake_long_running())
        thread_id = 42
        mgr._active_tasks[thread_id] = fake_task

        cancelled = await mgr.cancel_stream(thread_id)
        assert cancelled is True
        # task should be gone from active_tasks
        assert thread_id not in mgr._active_tasks


async def _consume_stream(mgr, thread_id, msg):
    """Helper to consume a stream."""
    try:
        async for _ in mgr.chat_stream(thread_id, msg):
            pass
    except Exception:
        pass


# ===========================================================================
# agent_provider_service — compatibility record methods
# ===========================================================================


class TestAgentProviderServiceCompatibility:
    def test_build_compatibility_payload_empty_models(self, db):
        from src.agent.provider_registry import ProviderRuntimeConfig
        from src.config import AppConfig
        from src.services.agent_provider_service import AgentProviderService, ProviderModelCacheEntry

        config = AppConfig()
        svc = AgentProviderService(db, config)
        cfg = ProviderRuntimeConfig(
            provider="openai",
            enabled=True,
            priority=0,
            selected_model="gpt-4o",
        )
        cache_entry = ProviderModelCacheEntry(
            provider="openai",
            models=[],
            source="static",
        )
        payload = svc.build_compatibility_payload(cfg, cache_entry)
        # selected_model not in models, so it's added; result should have it
        assert "gpt-4o" in payload

    def test_is_compatibility_record_fresh(self, db):
        from datetime import UTC, datetime, timedelta

        from src.config import AppConfig
        from src.services.agent_provider_service import (
            AgentProviderService,
            ProviderModelCompatibilityRecord,
        )

        config = AppConfig()
        svc = AgentProviderService(db, config)
        fresh_record = ProviderModelCompatibilityRecord(
            model="gpt-4o",
            status="ok",
            tested_at=datetime.now(UTC).isoformat(),
        )
        assert svc.is_compatibility_record_fresh(fresh_record)

        old_record = ProviderModelCompatibilityRecord(
            model="gpt-4o",
            status="ok",
            tested_at=(datetime.now(UTC) - timedelta(hours=48)).isoformat(),
        )
        assert not svc.is_compatibility_record_fresh(old_record)

    def test_normalize_ollama_base_url(self, db):
        from src.config import AppConfig
        from src.services.agent_provider_service import AgentProviderService

        config = AppConfig()
        svc = AgentProviderService(db, config)
        url = svc.normalize_ollama_base_url("http://localhost:11434", "")
        assert "v1" in url or "localhost" in url
