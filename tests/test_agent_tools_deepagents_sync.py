"""Tests for agent tools: deepagents_sync.build_deepagents_tools() — sync wrappers."""
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch


class TestDeepagentsSyncListChannels:
    def test_empty(self, mock_db):
        from src.agent.tools.deepagents_sync import build_deepagents_tools

        mock_db.get_channels = AsyncMock(return_value=[])
        tools = build_deepagents_tools(mock_db)
        tool_map = {t.__name__: t for t in tools}
        result = tool_map["list_channels"](active_only=False)
        assert "не найдены" in result

    def test_with_channels(self, mock_db):
        from src.agent.tools.deepagents_sync import build_deepagents_tools

        ch = SimpleNamespace(channel_id=100, title="TestCh", is_active=True)
        mock_db.get_channels = AsyncMock(return_value=[ch])
        tools = build_deepagents_tools(mock_db)
        tool_map = {t.__name__: t for t in tools}
        result = tool_map["list_channels"](active_only=False)
        assert "TestCh" in result
        assert "активен" in result

    def test_error_returns_text(self, mock_db):
        from src.agent.tools.deepagents_sync import build_deepagents_tools

        mock_db.get_channels = AsyncMock(side_effect=Exception("boom"))
        tools = build_deepagents_tools(mock_db)
        tool_map = {t.__name__: t for t in tools}
        result = tool_map["list_channels"](active_only=False)
        assert "Ошибка" in result


class TestDeepagentsSyncGetChannelStats:
    def test_empty(self, mock_db):
        from src.agent.tools.deepagents_sync import build_deepagents_tools

        mock_db.repos = MagicMock()
        mock_db.repos.channels.get_latest_stats_for_all = AsyncMock(return_value={})
        tools = build_deepagents_tools(mock_db)
        tool_map = {t.__name__: t for t in tools}
        result = tool_map["get_channel_stats"]()
        assert "не собрана" in result

    def test_with_stats(self, mock_db):
        from src.agent.tools.deepagents_sync import build_deepagents_tools

        stat = SimpleNamespace(subscriber_count=9999)
        mock_db.repos = MagicMock()
        mock_db.repos.channels.get_latest_stats_for_all = AsyncMock(return_value={42: stat})
        tools = build_deepagents_tools(mock_db)
        tool_map = {t.__name__: t for t in tools}
        result = tool_map["get_channel_stats"]()
        assert "9999" in result
        assert "channel_id=42" in result


class TestDeepagentsSyncListAccounts:
    def test_empty(self, mock_db):
        from src.agent.tools.deepagents_sync import build_deepagents_tools

        mock_db.get_accounts = AsyncMock(return_value=[])
        tools = build_deepagents_tools(mock_db)
        tool_map = {t.__name__: t for t in tools}
        result = tool_map["list_accounts"]()
        assert "не найдены" in result

    def test_with_accounts(self, mock_db):
        from src.agent.tools.deepagents_sync import build_deepagents_tools

        acc = SimpleNamespace(id=1, phone="+79001234567", is_active=True)
        mock_db.get_accounts = AsyncMock(return_value=[acc])
        tools = build_deepagents_tools(mock_db)
        tool_map = {t.__name__: t for t in tools}
        result = tool_map["list_accounts"]()
        assert "+79001234567" in result
        assert "активен" in result


class TestDeepagentsSyncGetFloodStatus:
    def test_no_accounts(self, mock_db):
        from src.agent.tools.deepagents_sync import build_deepagents_tools

        mock_db.get_accounts = AsyncMock(return_value=[])
        tools = build_deepagents_tools(mock_db)
        tool_map = {t.__name__: t for t in tools}
        result = tool_map["get_flood_status"]()
        assert "не найдены" in result

    def test_with_accounts(self, mock_db):
        from src.agent.tools.deepagents_sync import build_deepagents_tools

        acc = SimpleNamespace(phone="+79001234567", flood_wait_until=None)
        mock_db.get_accounts = AsyncMock(return_value=[acc])
        tools = build_deepagents_tools(mock_db)
        tool_map = {t.__name__: t for t in tools}
        result = tool_map["get_flood_status"]()
        assert "+79001234567" in result
        assert "Flood-статус" in result


class TestDeepagentsSyncAgentThreads:
    def test_list_empty(self, mock_db):
        from src.agent.tools.deepagents_sync import build_deepagents_tools

        mock_db.get_agent_threads = AsyncMock(return_value=[])
        tools = build_deepagents_tools(mock_db)
        tool_map = {t.__name__: t for t in tools}
        result = tool_map["list_agent_threads"]()
        assert "не найдены" in result

    def test_list_with_threads(self, mock_db):
        from src.agent.tools.deepagents_sync import build_deepagents_tools

        mock_db.get_agent_threads = AsyncMock(return_value=[{"id": 5, "title": "Chat 1"}])
        tools = build_deepagents_tools(mock_db)
        tool_map = {t.__name__: t for t in tools}
        result = tool_map["list_agent_threads"]()
        assert "Chat 1" in result
        assert "id=5" in result

    def test_create_thread(self, mock_db):
        from src.agent.tools.deepagents_sync import build_deepagents_tools

        mock_db.create_agent_thread = AsyncMock(return_value=7)
        tools = build_deepagents_tools(mock_db)
        tool_map = {t.__name__: t for t in tools}
        result = tool_map["create_agent_thread"](title="New Thread")
        assert "id=7" in result
        assert "создан" in result


class TestDeepagentsSyncGetSettings:
    def test_returns_settings(self, mock_db):
        from src.agent.tools.deepagents_sync import build_deepagents_tools

        mock_db.get_setting = AsyncMock(return_value="30")
        tools = build_deepagents_tools(mock_db)
        tool_map = {t.__name__: t for t in tools}
        result = tool_map["get_settings"]()
        assert "Настройки" in result

    def test_unset_value(self, mock_db):
        from src.agent.tools.deepagents_sync import build_deepagents_tools

        mock_db.get_setting = AsyncMock(return_value=None)
        tools = build_deepagents_tools(mock_db)
        tool_map = {t.__name__: t for t in tools}
        result = tool_map["get_settings"]()
        assert "не задано" in result


class TestDeepagentsSyncGetSystemInfo:
    def test_returns_info(self, mock_db):
        from src.agent.tools.deepagents_sync import build_deepagents_tools

        mock_db.get_stats = AsyncMock(return_value={"channels": 5, "messages": 100})
        tools = build_deepagents_tools(mock_db)
        tool_map = {t.__name__: t for t in tools}
        result = tool_map["get_system_info"]()
        assert "channels" in result
        assert "5" in result

    def test_error_returns_text(self, mock_db):
        from src.agent.tools.deepagents_sync import build_deepagents_tools

        mock_db.get_stats = AsyncMock(side_effect=Exception("db fail"))
        tools = build_deepagents_tools(mock_db)
        tool_map = {t.__name__: t for t in tools}
        result = tool_map["get_system_info"]()
        assert "Ошибка" in result


class TestDeepagentsSyncListSearchQueries:
    def test_empty(self, mock_db):
        from src.agent.tools.deepagents_sync import build_deepagents_tools

        svc_mock = MagicMock()
        svc_mock.list = AsyncMock(return_value=[])
        with patch("src.services.search_query_service.SearchQueryService", return_value=svc_mock):
            tools = build_deepagents_tools(mock_db)
            tool_map = {t.__name__: t for t in tools}
            result = tool_map["list_search_queries"](active_only=False)
        assert "не найдены" in result

    def test_with_queries(self, mock_db):
        from src.agent.tools.deepagents_sync import build_deepagents_tools

        q = SimpleNamespace(id=3, query="crypto", is_active=True, interval_minutes=60)
        svc_mock = MagicMock()
        svc_mock.list = AsyncMock(return_value=[q])
        with patch("src.services.search_query_service.SearchQueryService", return_value=svc_mock):
            tools = build_deepagents_tools(mock_db)
            tool_map = {t.__name__: t for t in tools}
            result = tool_map["list_search_queries"](active_only=False)
        assert "crypto" in result
        assert "id=3" in result


class TestDeepagentsSyncGetSchedulerStatus:
    def test_returns_status(self, mock_db):
        from src.agent.tools.deepagents_sync import build_deepagents_tools

        mgr_mock = MagicMock()
        mgr_mock.load_settings = AsyncMock(return_value=None)
        mgr_mock.is_running = True
        mgr_mock.interval_minutes = 15
        with patch("src.scheduler.service.SchedulerManager", return_value=mgr_mock):
            tools = build_deepagents_tools(mock_db)
            tool_map = {t.__name__: t for t in tools}
            result = tool_map["get_scheduler_status"]()
        assert "Планировщик" in result

    def test_error_returns_text(self, mock_db):
        from src.agent.tools.deepagents_sync import build_deepagents_tools

        with patch("src.scheduler.service.SchedulerManager", side_effect=Exception("no sched")):
            tools = build_deepagents_tools(mock_db)
            tool_map = {t.__name__: t for t in tools}
            result = tool_map["get_scheduler_status"]()
        assert "Ошибка" in result


class TestDeepagentsSyncGetNotificationStatus:
    def test_not_configured(self, mock_db):
        from src.agent.tools.deepagents_sync import build_deepagents_tools

        svc_mock = MagicMock()
        svc_mock.get_status = AsyncMock(return_value=None)
        target_svc_mock = MagicMock()
        with patch("src.services.notification_service.NotificationService", return_value=svc_mock):
            with patch(
                "src.services.notification_target_service.NotificationTargetService",
                return_value=target_svc_mock,
            ):
                tools = build_deepagents_tools(mock_db)
                tool_map = {t.__name__: t for t in tools}
                result = tool_map["get_notification_status"]()
        assert "не настроен" in result

    def test_configured(self, mock_db):
        from src.agent.tools.deepagents_sync import build_deepagents_tools

        bot = SimpleNamespace(bot_username="mybot", chat_id=12345)
        svc_mock = MagicMock()
        svc_mock.get_status = AsyncMock(return_value=bot)
        target_svc_mock = MagicMock()
        with patch("src.services.notification_service.NotificationService", return_value=svc_mock):
            with patch(
                "src.services.notification_target_service.NotificationTargetService",
                return_value=target_svc_mock,
            ):
                tools = build_deepagents_tools(mock_db)
                tool_map = {t.__name__: t for t in tools}
                result = tool_map["get_notification_status"]()
        assert "@mybot" in result
        assert "12345" in result


class TestDeepagentsSyncListImageProviders:
    def test_no_providers(self, mock_db):
        from src.agent.tools.deepagents_sync import build_deepagents_tools

        img_svc_mock = MagicMock()
        img_svc_mock.adapter_names = []
        with patch(
            "src.services.image_generation_service.ImageGenerationService", return_value=img_svc_mock
        ):
            tools = build_deepagents_tools(mock_db)
            tool_map = {t.__name__: t for t in tools}
            result = tool_map["list_image_providers"]()
        assert "не настроены" in result

    def test_with_providers(self, mock_db):
        from src.agent.tools.deepagents_sync import build_deepagents_tools

        img_svc_mock = MagicMock()
        img_svc_mock.adapter_names = ["together", "hf"]
        with patch(
            "src.services.image_generation_service.ImageGenerationService", return_value=img_svc_mock
        ):
            tools = build_deepagents_tools(mock_db)
            tool_map = {t.__name__: t for t in tools}
            result = tool_map["list_image_providers"]()
        assert "together" in result
        assert "hf" in result


class TestDeepagentsSyncAnalyzeFilters:
    def test_returns_analysis(self, mock_db):
        from src.agent.tools.deepagents_sync import build_deepagents_tools

        analyzer_mock = MagicMock()
        result_item = SimpleNamespace(should_filter=True)
        report = SimpleNamespace(results=[result_item, SimpleNamespace(should_filter=False)])
        analyzer_mock.analyze_all = AsyncMock(return_value=report)
        with patch("src.filters.analyzer.ChannelAnalyzer", return_value=analyzer_mock):
            tools = build_deepagents_tools(mock_db)
            tool_map = {t.__name__: t for t in tools}
            result = tool_map["analyze_filters"]()
        assert "Анализ" in result
        assert "2 проверено" in result
        assert "1 к фильтрации" in result

    def test_error_returns_text(self, mock_db):
        from src.agent.tools.deepagents_sync import build_deepagents_tools

        with patch("src.filters.analyzer.ChannelAnalyzer", side_effect=Exception("no filter")):
            tools = build_deepagents_tools(mock_db)
            tool_map = {t.__name__: t for t in tools}
            result = tool_map["analyze_filters"]()
        assert "Ошибка" in result


class TestDeepagentsSyncListPendingModeration:
    def test_empty(self, mock_db):
        from src.agent.tools.deepagents_sync import build_deepagents_tools

        mock_db.repos = MagicMock()
        mock_db.repos.generation_runs.list_pending_moderation = AsyncMock(return_value=[])
        tools = build_deepagents_tools(mock_db)
        tool_map = {t.__name__: t for t in tools}
        result = tool_map["list_pending_moderation"](limit=10)
        assert "Нет черновиков" in result

    def test_with_runs(self, mock_db):
        from src.agent.tools.deepagents_sync import build_deepagents_tools

        run = SimpleNamespace(id=9, pipeline_id=2, generated_text="Draft content")
        mock_db.repos = MagicMock()
        mock_db.repos.generation_runs.list_pending_moderation = AsyncMock(return_value=[run])
        tools = build_deepagents_tools(mock_db)
        tool_map = {t.__name__: t for t in tools}
        result = tool_map["list_pending_moderation"](limit=10)
        assert "run_id=9" in result
        assert "Draft content" in result


class TestDeepagentsSyncListPipelines:
    def test_empty(self, mock_db):
        from src.agent.tools.deepagents_sync import build_deepagents_tools

        svc_mock = MagicMock()
        svc_mock.list = AsyncMock(return_value=[])
        with patch("src.services.pipeline_service.PipelineService", return_value=svc_mock):
            tools = build_deepagents_tools(mock_db)
            tool_map = {t.__name__: t for t in tools}
            result = tool_map["list_pipelines"](active_only=False)
        assert "не найдены" in result

    def test_with_pipelines(self, mock_db):
        from src.agent.tools.deepagents_sync import build_deepagents_tools

        p = SimpleNamespace(id=1, name="Pipeline A", is_active=True, llm_model="gpt-4o")
        svc_mock = MagicMock()
        svc_mock.list = AsyncMock(return_value=[p])
        with patch("src.services.pipeline_service.PipelineService", return_value=svc_mock):
            tools = build_deepagents_tools(mock_db)
            tool_map = {t.__name__: t for t in tools}
            result = tool_map["list_pipelines"](active_only=False)
        assert "Pipeline A" in result
        assert "gpt-4o" in result
