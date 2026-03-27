"""Comprehensive tests for agent tools: deepagents_sync, pipelines, images, my_telegram, analytics.

Tests cover:
- deepagents_sync.build_deepagents_tools() — sync wrappers
- pipelines.register() — pipeline management MCP tools
- images.register() — image generation MCP tools
- my_telegram.register() — dialog and cache tools
- analytics.register() — content analytics tools
"""

from __future__ import annotations

from types import SimpleNamespace
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
    """Extract text from MCP tool result payload."""
    return result["content"][0]["text"]


# ---------------------------------------------------------------------------
# deepagents_sync tests
# ---------------------------------------------------------------------------


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
        with patch("src.scheduler.manager.SchedulerManager", return_value=mgr_mock):
            tools = build_deepagents_tools(mock_db)
            tool_map = {t.__name__: t for t in tools}
            result = tool_map["get_scheduler_status"]()
        assert "Планировщик" in result

    def test_error_returns_text(self, mock_db):
        from src.agent.tools.deepagents_sync import build_deepagents_tools

        with patch("src.scheduler.manager.SchedulerManager", side_effect=Exception("no sched")):
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
    def test_no_providers(self):
        from src.agent.tools.deepagents_sync import build_deepagents_tools

        mock_db2 = MagicMock()
        img_svc_mock = MagicMock()
        img_svc_mock.adapter_names = []
        with patch(
            "src.services.image_generation_service.ImageGenerationService", return_value=img_svc_mock
        ):
            tools = build_deepagents_tools(mock_db2)
            tool_map = {t.__name__: t for t in tools}
            result = tool_map["list_image_providers"]()
        assert "не настроены" in result

    def test_with_providers(self):
        from src.agent.tools.deepagents_sync import build_deepagents_tools

        mock_db2 = MagicMock()
        img_svc_mock = MagicMock()
        img_svc_mock.adapter_names = ["together", "hf"]
        with patch(
            "src.services.image_generation_service.ImageGenerationService", return_value=img_svc_mock
        ):
            tools = build_deepagents_tools(mock_db2)
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


# ---------------------------------------------------------------------------
# pipelines.py MCP tools tests
# ---------------------------------------------------------------------------


class TestPipelinesToolListPipelines:
    @pytest.mark.asyncio
    async def test_empty(self, mock_db):
        with patch("src.services.pipeline_service.PipelineService") as mock_svc:
            mock_svc.return_value.list = AsyncMock(return_value=[])
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["list_pipelines"]({"active_only": False})
        assert "не найдены" in _text(result)

    @pytest.mark.asyncio
    async def test_with_pipeline(self, mock_db):
        p = SimpleNamespace(
            id=5,
            name="Test Pipeline",
            is_active=True,
            llm_model="gpt-4",
            publish_mode="moderated",
            schedule_cron="0 * * * *",
            generation_backend="chain",
        )
        with patch("src.services.pipeline_service.PipelineService") as mock_svc:
            mock_svc.return_value.list = AsyncMock(return_value=[p])
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["list_pipelines"]({"active_only": True})
        text = _text(result)
        assert "Test Pipeline" in text
        assert "id=5" in text
        assert "gpt-4" in text

    @pytest.mark.asyncio
    async def test_error(self, mock_db):
        with patch("src.services.pipeline_service.PipelineService") as mock_svc:
            mock_svc.return_value.list = AsyncMock(side_effect=Exception("db err"))
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["list_pipelines"]({})
        assert "Ошибка" in _text(result)


class TestPipelinesToolGetPipelineDetail:
    @pytest.mark.asyncio
    async def test_missing_id(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["get_pipeline_detail"]({})
        assert "pipeline_id обязателен" in _text(result)

    @pytest.mark.asyncio
    async def test_not_found(self, mock_db):
        with patch("src.services.pipeline_service.PipelineService") as mock_svc:
            mock_svc.return_value.get_detail = AsyncMock(return_value=None)
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["get_pipeline_detail"]({"pipeline_id": 999})
        assert "не найден" in _text(result)

    @pytest.mark.asyncio
    async def test_found(self, mock_db):
        p = SimpleNamespace(
            id=2,
            name="Detail Pipeline",
            is_active=True,
            llm_model="claude-3",
            publish_mode="auto",
            generation_backend="sdk",
            schedule_cron=None,
            generate_interval_minutes=120,
            prompt_template="Generate about {topic}",
        )
        detail = {
            "pipeline": p,
            "source_titles": ["Channel A", "Channel B"],
            "target_refs": ["@target"],
        }
        with patch("src.services.pipeline_service.PipelineService") as mock_svc:
            mock_svc.return_value.get_detail = AsyncMock(return_value=detail)
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["get_pipeline_detail"]({"pipeline_id": 2})
        text = _text(result)
        assert "Detail Pipeline" in text
        assert "Channel A" in text


class TestPipelinesToolAddPipeline:
    @pytest.mark.asyncio
    async def test_requires_confirmation(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["add_pipeline"]({
            "name": "New",
            "prompt_template": "tmpl",
            "source_channel_ids": "1,2",
            "target_refs": "+7123|456",
            "confirm": False,
        })
        assert "Подтвердите" in _text(result)

    @pytest.mark.asyncio
    async def test_missing_required_fields(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["add_pipeline"]({"confirm": True, "name": "only name"})
        assert "обязательны" in _text(result)

    @pytest.mark.asyncio
    async def test_invalid_target_ref_format(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["add_pipeline"]({
            "confirm": True,
            "name": "P",
            "prompt_template": "t",
            "source_channel_ids": "1",
            "target_refs": "invalidformat",
        })
        assert "Неверный формат" in _text(result)

    @pytest.mark.asyncio
    async def test_creates_pipeline(self, mock_db):
        with patch("src.services.pipeline_service.PipelineService") as mock_svc:
            with patch("src.services.pipeline_service.PipelineTargetRef"):
                mock_svc.return_value.add = AsyncMock(return_value=10)
                handlers = _get_tool_handlers(mock_db)
                result = await handlers["add_pipeline"]({
                    "confirm": True,
                    "name": "My Pipeline",
                    "prompt_template": "Write about {topic}",
                    "source_channel_ids": "1,2",
                    "target_refs": "+7123456|789",
                    "llm_model": "gpt-4",
                    "publish_mode": "auto",
                })
        text = _text(result)
        assert "id=10" in text
        assert "My Pipeline" in text


class TestPipelinesToolTogglePipeline:
    @pytest.mark.asyncio
    async def test_missing_id(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["toggle_pipeline"]({})
        assert "pipeline_id обязателен" in _text(result)

    @pytest.mark.asyncio
    async def test_not_found(self, mock_db):
        with patch("src.services.pipeline_service.PipelineService") as mock_svc:
            mock_svc.return_value.toggle = AsyncMock(return_value=False)
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["toggle_pipeline"]({"pipeline_id": 99})
        assert "не найден" in _text(result)

    @pytest.mark.asyncio
    async def test_toggled(self, mock_db):
        p = SimpleNamespace(id=1, name="P1", is_active=True)
        with patch("src.services.pipeline_service.PipelineService") as mock_svc:
            mock_svc.return_value.toggle = AsyncMock(return_value=True)
            mock_svc.return_value.get = AsyncMock(return_value=p)
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["toggle_pipeline"]({"pipeline_id": 1})
        text = _text(result)
        assert "P1" in text
        assert "активирован" in text


class TestPipelinesToolDeletePipeline:
    @pytest.mark.asyncio
    async def test_missing_id(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["delete_pipeline"]({})
        assert "pipeline_id обязателен" in _text(result)

    @pytest.mark.asyncio
    async def test_requires_confirmation(self, mock_db):
        p = SimpleNamespace(id=1, name="PipeX")
        with patch("src.services.pipeline_service.PipelineService") as mock_svc:
            mock_svc.return_value.get = AsyncMock(return_value=p)
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["delete_pipeline"]({"pipeline_id": 1, "confirm": False})
        assert "Подтвердите" in _text(result)

    @pytest.mark.asyncio
    async def test_deletes_with_confirmation(self, mock_db):
        p = SimpleNamespace(id=1, name="PipeToDelete")
        with patch("src.services.pipeline_service.PipelineService") as mock_svc:
            mock_svc.return_value.get = AsyncMock(return_value=p)
            mock_svc.return_value.delete = AsyncMock()
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["delete_pipeline"]({"pipeline_id": 1, "confirm": True})
        text = _text(result)
        assert "PipeToDelete" in text
        assert "удалён" in text


class TestPipelinesToolListPipelineRuns:
    @pytest.mark.asyncio
    async def test_missing_pipeline_id(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["list_pipeline_runs"]({})
        assert "pipeline_id обязателен" in _text(result)

    @pytest.mark.asyncio
    async def test_empty(self, mock_db):
        mock_db.repos = MagicMock()
        mock_db.repos.generation_runs.list_by_pipeline = AsyncMock(return_value=[])
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["list_pipeline_runs"]({"pipeline_id": 1})
        assert "Нет генераций" in _text(result)

    @pytest.mark.asyncio
    async def test_with_runs(self, mock_db):
        run = SimpleNamespace(
            id=10,
            status="done",
            moderation_status="approved",
            generated_text="Content here",
            created_at="2026-01-01",
        )
        mock_db.repos = MagicMock()
        mock_db.repos.generation_runs.list_by_pipeline = AsyncMock(return_value=[run])
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["list_pipeline_runs"]({"pipeline_id": 1, "limit": 10})
        text = _text(result)
        assert "run_id=10" in text
        assert "approved" in text


class TestPipelinesToolGetPipelineRun:
    @pytest.mark.asyncio
    async def test_missing_run_id(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["get_pipeline_run"]({})
        assert "run_id обязателен" in _text(result)

    @pytest.mark.asyncio
    async def test_not_found(self, mock_db):
        mock_db.repos = MagicMock()
        mock_db.repos.generation_runs.get = AsyncMock(return_value=None)
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["get_pipeline_run"]({"run_id": 99})
        assert "не найден" in _text(result)

    @pytest.mark.asyncio
    async def test_found(self, mock_db):
        run = SimpleNamespace(
            id=5,
            pipeline_id=2,
            status="published",
            moderation_status="approved",
            quality_score=0.9,
            created_at="2026-01-01",
            updated_at="2026-01-02",
            generated_text="Full content text",
        )
        mock_db.repos = MagicMock()
        mock_db.repos.generation_runs.get = AsyncMock(return_value=run)
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["get_pipeline_run"]({"run_id": 5})
        text = _text(result)
        assert "Run id=5" in text
        assert "Full content text" in text
        assert "approved" in text


# ---------------------------------------------------------------------------
# images.py MCP tools tests
# ---------------------------------------------------------------------------


class TestImagesToolGenerateImage:
    @pytest.mark.asyncio
    async def test_missing_prompt(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["generate_image"]({"prompt": ""})
        assert "prompt обязателен" in _text(result)

    @pytest.mark.asyncio
    async def test_not_available(self, mock_db):
        with patch("src.services.image_generation_service.ImageGenerationService") as mock_svc:
            mock_svc.return_value.is_available = AsyncMock(return_value=False)
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["generate_image"]({"prompt": "a cat"})
        assert "не настроена" in _text(result)

    @pytest.mark.asyncio
    async def test_local_path_result(self, mock_db):
        with patch("src.services.image_generation_service.ImageGenerationService") as mock_svc:
            mock_svc.return_value.is_available = AsyncMock(return_value=True)
            mock_svc.return_value.generate = AsyncMock(return_value="/local/path/image.png")
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["generate_image"]({"prompt": "a dog"})
        text = _text(result)
        assert "/local/path/image.png" in text

    @pytest.mark.asyncio
    async def test_no_result(self, mock_db):
        with patch("src.services.image_generation_service.ImageGenerationService") as mock_svc:
            mock_svc.return_value.is_available = AsyncMock(return_value=True)
            mock_svc.return_value.generate = AsyncMock(return_value=None)
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["generate_image"]({"prompt": "something"})
        assert "не вернула результат" in _text(result)

    @pytest.mark.asyncio
    async def test_error_returns_text(self, mock_db):
        with patch("src.services.image_generation_service.ImageGenerationService") as mock_svc:
            mock_svc.return_value.is_available = AsyncMock(side_effect=Exception("provider down"))
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["generate_image"]({"prompt": "test"})
        assert "Ошибка" in _text(result)


class TestImagesToolListImageModels:
    @pytest.mark.asyncio
    async def test_missing_provider(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["list_image_models"]({"provider": ""})
        assert "provider обязателен" in _text(result)

    @pytest.mark.asyncio
    async def test_empty_models(self, mock_db):
        with patch("src.services.image_generation_service.ImageGenerationService") as mock_svc:
            mock_svc.return_value.search_models = AsyncMock(return_value=[])
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["list_image_models"]({"provider": "together"})
        assert "не найдены" in _text(result)

    @pytest.mark.asyncio
    async def test_with_models(self, mock_db):
        models = [
            {"id": "flux-schnell", "run_count": 10000, "rank": 1},
            {"id": "flux-dev", "run_count": 5000},
        ]
        with patch("src.services.image_generation_service.ImageGenerationService") as mock_svc:
            mock_svc.return_value.search_models = AsyncMock(return_value=models)
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["list_image_models"]({"provider": "together", "query": "flux"})
        text = _text(result)
        assert "flux-schnell" in text
        assert "10,000 runs" in text
        assert "rank 1" in text


class TestImagesToolListImageProviders:
    @pytest.mark.asyncio
    async def test_no_providers(self, mock_db):
        with patch("src.services.image_generation_service.ImageGenerationService") as mock_svc:
            mock_svc.return_value.adapter_names = []
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["list_image_providers"]({})
        assert "не настроены" in _text(result)

    @pytest.mark.asyncio
    async def test_with_providers(self, mock_db):
        with patch("src.services.image_generation_service.ImageGenerationService") as mock_svc:
            mock_svc.return_value.adapter_names = ["together", "hf", "replicate"]
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["list_image_providers"]({})
        text = _text(result)
        assert "together" in text
        assert "hf" in text
        assert "replicate" in text
        assert "Провайдеры изображений (3)" in text

    @pytest.mark.asyncio
    async def test_error(self, mock_db):
        with patch("src.services.image_generation_service.ImageGenerationService") as mock_svc:
            mock_svc.side_effect = Exception("provider fail")
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["list_image_providers"]({})
        assert "Ошибка" in _text(result)


class TestImagesToolListGeneratedImages:
    @pytest.mark.asyncio
    async def test_empty(self, mock_db):
        mock_db.repos = MagicMock()
        mock_db.repos.generated_images.list_recent = AsyncMock(return_value=[])
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["list_generated_images"]({})
        assert "Нет сгенерированных" in _text(result)

    @pytest.mark.asyncio
    async def test_with_images(self, mock_db):
        img = SimpleNamespace(
            id=1,
            prompt="a beautiful cat",
            model="together:flux",
            local_path="/data/img/abc.png",
            created_at="2026-01-01",
        )
        mock_db.repos = MagicMock()
        mock_db.repos.generated_images.list_recent = AsyncMock(return_value=[img])
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["list_generated_images"]({"limit": 5})
        text = _text(result)
        assert "a beautiful cat" in text
        assert "together:flux" in text
        assert "/data/img/abc.png" in text

    @pytest.mark.asyncio
    async def test_long_prompt_truncated(self, mock_db):
        long_prompt = "x" * 100
        img = SimpleNamespace(
            id=2,
            prompt=long_prompt,
            model=None,
            local_path=None,
            created_at="2026-01-01",
        )
        mock_db.repos = MagicMock()
        mock_db.repos.generated_images.list_recent = AsyncMock(return_value=[img])
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["list_generated_images"]({})
        text = _text(result)
        assert "..." in text
        # truncated to 60 chars + "..."
        assert "x" * 60 in text
        assert "x" * 61 not in text

    @pytest.mark.asyncio
    async def test_error(self, mock_db):
        mock_db.repos = MagicMock()
        mock_db.repos.generated_images.list_recent = AsyncMock(side_effect=Exception("db err"))
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["list_generated_images"]({})
        assert "Ошибка" in _text(result)


# ---------------------------------------------------------------------------
# my_telegram.py MCP tools tests
# ---------------------------------------------------------------------------


class TestMyTelegramToolListDialogs:
    @pytest.mark.asyncio
    async def test_no_pool(self, mock_db):
        handlers = _get_tool_handlers(mock_db, client_pool=None)
        result = await handlers["list_dialogs"]({"phone": "+7123456"})
        assert "CLI-режиме" in _text(result)

    @pytest.mark.asyncio
    async def test_empty_dialogs(self, mock_db):
        mock_db.get_accounts = AsyncMock(
            return_value=[SimpleNamespace(phone="+79001234567", is_primary=True)]
        )
        mock_db.get_setting = AsyncMock(return_value=None)
        mock_pool = MagicMock()
        ch_svc = MagicMock()
        ch_svc.get_my_dialogs = AsyncMock(return_value=[])
        with patch("src.services.channel_service.ChannelService", return_value=ch_svc):
            handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
            result = await handlers["list_dialogs"]({"phone": "+79001234567"})
        assert "не найдены" in _text(result)

    @pytest.mark.asyncio
    async def test_with_dialogs(self, mock_db):
        mock_db.get_accounts = AsyncMock(
            return_value=[SimpleNamespace(phone="+79001234567", is_primary=True)]
        )
        mock_db.get_setting = AsyncMock(return_value=None)
        mock_pool = MagicMock()
        dialogs = [
            {"title": "My Channel", "channel_id": 111, "channel_type": "channel"},
            {"title": "My Group", "channel_id": 222, "channel_type": "group"},
        ]
        ch_svc = MagicMock()
        ch_svc.get_my_dialogs = AsyncMock(return_value=dialogs)
        with patch("src.services.channel_service.ChannelService", return_value=ch_svc):
            handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
            result = await handlers["list_dialogs"]({"phone": "+79001234567"})
        text = _text(result)
        assert "My Channel" in text
        assert "id=111" in text


class TestMyTelegramToolRefreshDialogs:
    @pytest.mark.asyncio
    async def test_no_pool(self, mock_db):
        handlers = _get_tool_handlers(mock_db, client_pool=None)
        result = await handlers["refresh_dialogs"]({"phone": "+7123456"})
        assert "CLI-режиме" in _text(result)

    @pytest.mark.asyncio
    async def test_refresh_success(self, mock_db):
        mock_db.get_accounts = AsyncMock(
            return_value=[SimpleNamespace(phone="+79001234567", is_primary=True)]
        )
        mock_db.get_setting = AsyncMock(return_value=None)
        mock_pool = MagicMock()
        ch_svc = MagicMock()
        ch_svc.get_my_dialogs = AsyncMock(return_value=[{"title": "X", "channel_id": 1, "channel_type": "channel"}])
        with patch("src.services.channel_service.ChannelService", return_value=ch_svc):
            handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
            result = await handlers["refresh_dialogs"]({"phone": "+79001234567"})
        text = _text(result)
        assert "обновлены" in text
        assert "1" in text


class TestMyTelegramToolLeaveDialogs:
    @pytest.mark.asyncio
    async def test_no_pool(self, mock_db):
        handlers = _get_tool_handlers(mock_db, client_pool=None)
        result = await handlers["leave_dialogs"]({"phone": "+7123456", "dialog_ids": "1,2"})
        assert "CLI-режиме" in _text(result)

    @pytest.mark.asyncio
    async def test_missing_dialog_ids(self, mock_db):
        mock_db.get_accounts = AsyncMock(
            return_value=[SimpleNamespace(phone="+79001234567", is_primary=True)]
        )
        mock_db.get_setting = AsyncMock(return_value=None)
        mock_pool = MagicMock()
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["leave_dialogs"]({"phone": "+79001234567", "dialog_ids": "", "confirm": True})
        assert "dialog_ids обязателен" in _text(result)

    @pytest.mark.asyncio
    async def test_requires_confirmation(self, mock_db):
        mock_db.get_accounts = AsyncMock(
            return_value=[SimpleNamespace(phone="+79001234567", is_primary=True)]
        )
        mock_db.get_setting = AsyncMock(return_value=None)
        mock_pool = MagicMock()
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["leave_dialogs"]({"phone": "+79001234567", "dialog_ids": "1,2", "confirm": False})
        assert "Подтвердите" in _text(result)


class TestMyTelegramToolGetForumTopics:
    @pytest.mark.asyncio
    async def test_missing_channel_id(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["get_forum_topics"]({})
        assert "channel_id обязателен" in _text(result)

    @pytest.mark.asyncio
    async def test_empty_topics(self, mock_db):
        mock_db.get_forum_topics = AsyncMock(return_value=[])
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["get_forum_topics"]({"channel_id": 123})
        assert "не найдены" in _text(result)

    @pytest.mark.asyncio
    async def test_with_topics(self, mock_db):
        topics = [
            {"topic_id": 1, "title": "General"},
            {"topic_id": 2, "title": "Off-topic"},
        ]
        mock_db.get_forum_topics = AsyncMock(return_value=topics)
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["get_forum_topics"]({"channel_id": 123})
        text = _text(result)
        assert "General" in text
        assert "Off-topic" in text
        assert "id=1" in text

    @pytest.mark.asyncio
    async def test_error(self, mock_db):
        mock_db.get_forum_topics = AsyncMock(side_effect=Exception("no access"))
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["get_forum_topics"]({"channel_id": 123})
        assert "Ошибка" in _text(result)


class TestMyTelegramToolClearDialogCache:
    @pytest.mark.asyncio
    async def test_requires_confirmation(self, mock_db):
        mock_db.get_setting = AsyncMock(return_value=None)
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["clear_dialog_cache"]({"phone": "+79001234567", "confirm": False})
        assert "Подтвердите" in _text(result)

    @pytest.mark.asyncio
    async def test_clears_for_phone(self, mock_db):
        mock_db.get_setting = AsyncMock(return_value=None)
        mock_db.repos = MagicMock()
        mock_db.repos.dialog_cache.clear_dialogs = AsyncMock()
        mock_pool = MagicMock()
        mock_pool.invalidate_dialogs_cache = MagicMock()
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["clear_dialog_cache"]({"phone": "+79001234567", "confirm": True})
        assert "очищен" in _text(result)
        mock_db.repos.dialog_cache.clear_dialogs.assert_awaited_once_with("+79001234567")

    @pytest.mark.asyncio
    async def test_clears_all_when_no_phone(self, mock_db):
        mock_db.repos = MagicMock()
        mock_db.repos.dialog_cache.clear_all_dialogs = AsyncMock()
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["clear_dialog_cache"]({"phone": "", "confirm": True})
        assert "очищен" in _text(result)
        mock_db.repos.dialog_cache.clear_all_dialogs.assert_awaited_once()


class TestMyTelegramToolGetCacheStatus:
    @pytest.mark.asyncio
    async def test_empty_cache(self, mock_db):
        mock_db.repos = MagicMock()
        mock_db.repos.dialog_cache.get_all_phones = AsyncMock(return_value=[])
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["get_cache_status"]({})
        assert "пуст" in _text(result)

    @pytest.mark.asyncio
    async def test_with_cache_entries(self, mock_db):
        from datetime import datetime, timezone

        mock_db.repos = MagicMock()
        mock_db.repos.dialog_cache.get_all_phones = AsyncMock(return_value=["+79001234567"])
        mock_db.repos.dialog_cache.count_dialogs = AsyncMock(return_value=42)
        mock_db.repos.dialog_cache.get_cached_at = AsyncMock(
            return_value=datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        )
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["get_cache_status"]({})
        text = _text(result)
        assert "+79001234567" in text
        assert "42" in text
        assert "2026-01-01" in text

    @pytest.mark.asyncio
    async def test_error(self, mock_db):
        mock_db.repos = MagicMock()
        mock_db.repos.dialog_cache.get_all_phones = AsyncMock(side_effect=Exception("cache err"))
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["get_cache_status"]({})
        assert "Ошибка" in _text(result)


# ---------------------------------------------------------------------------
# analytics.py MCP tools tests
# ---------------------------------------------------------------------------


class TestAnalyticsToolGetAnalyticsSummary:
    @pytest.mark.asyncio
    async def test_returns_summary(self, mock_db):
        summary = {
            "total_generations": 100,
            "total_published": 60,
            "total_pending": 20,
            "total_rejected": 10,
            "pipelines_count": 5,
        }
        with patch("src.services.content_analytics_service.ContentAnalyticsService") as mock_svc:
            mock_svc.return_value.get_summary = AsyncMock(return_value=summary)
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["get_analytics_summary"]({})
        text = _text(result)
        assert "100" in text
        assert "60" in text
        assert "Аналитика контента" in text
        assert "Пайплайнов: 5" in text

    @pytest.mark.asyncio
    async def test_error(self, mock_db):
        with patch("src.services.content_analytics_service.ContentAnalyticsService") as mock_svc:
            mock_svc.return_value.get_summary = AsyncMock(side_effect=Exception("analytics down"))
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["get_analytics_summary"]({})
        assert "Ошибка" in _text(result)


class TestAnalyticsToolGetPipelineStats:
    @pytest.mark.asyncio
    async def test_empty(self, mock_db):
        with patch("src.services.content_analytics_service.ContentAnalyticsService") as mock_svc:
            mock_svc.return_value.get_pipeline_stats = AsyncMock(return_value=[])
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["get_pipeline_stats"]({})
        assert "не найдена" in _text(result)

    @pytest.mark.asyncio
    async def test_with_stats(self, mock_db):
        stat = SimpleNamespace(
            pipeline_id=1,
            pipeline_name="Pipeline X",
            total_generations=50,
            total_published=40,
            total_rejected=5,
            pending_moderation=5,
            success_rate=0.80,
        )
        with patch("src.services.content_analytics_service.ContentAnalyticsService") as mock_svc:
            mock_svc.return_value.get_pipeline_stats = AsyncMock(return_value=[stat])
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["get_pipeline_stats"]({"pipeline_id": 1})
        text = _text(result)
        assert "Pipeline X" in text
        assert "генераций=50" in text
        assert "80%" in text

    @pytest.mark.asyncio
    async def test_error(self, mock_db):
        with patch("src.services.content_analytics_service.ContentAnalyticsService") as mock_svc:
            mock_svc.return_value.get_pipeline_stats = AsyncMock(side_effect=Exception("err"))
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["get_pipeline_stats"]({})
        assert "Ошибка" in _text(result)


class TestAnalyticsToolGetTrendingTopics:
    @pytest.mark.asyncio
    async def test_empty(self, mock_db):
        with patch("src.services.trend_service.TrendService") as mock_svc:
            mock_svc.return_value.get_trending_topics = AsyncMock(return_value=[])
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["get_trending_topics"]({"days": 7, "limit": 10})
        assert "не найдены" in _text(result)

    @pytest.mark.asyncio
    async def test_with_topics(self, mock_db):
        topics = [
            SimpleNamespace(keyword="Python", count=500),
            SimpleNamespace(keyword="AI", count=300),
        ]
        with patch("src.services.trend_service.TrendService") as mock_svc:
            mock_svc.return_value.get_trending_topics = AsyncMock(return_value=topics)
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["get_trending_topics"]({"days": 14, "limit": 5})
        text = _text(result)
        assert "Python" in text
        assert "500 упоминаний" in text
        assert "AI" in text

    @pytest.mark.asyncio
    async def test_error(self, mock_db):
        with patch("src.services.trend_service.TrendService") as mock_svc:
            mock_svc.return_value.get_trending_topics = AsyncMock(side_effect=Exception("trend err"))
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["get_trending_topics"]({})
        assert "Ошибка" in _text(result)


class TestAnalyticsToolGetTrendingChannels:
    @pytest.mark.asyncio
    async def test_empty(self, mock_db):
        with patch("src.services.trend_service.TrendService") as mock_svc:
            mock_svc.return_value.get_trending_channels = AsyncMock(return_value=[])
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["get_trending_channels"]({})
        assert "не найдены" in _text(result)

    @pytest.mark.asyncio
    async def test_with_channels(self, mock_db):
        channels = [
            SimpleNamespace(title="TechChannel", channel_id=100, count=200),
            SimpleNamespace(title="NewsChannel", channel_id=200, count=150),
        ]
        with patch("src.services.trend_service.TrendService") as mock_svc:
            mock_svc.return_value.get_trending_channels = AsyncMock(return_value=channels)
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["get_trending_channels"]({"days": 7, "limit": 10})
        text = _text(result)
        assert "TechChannel" in text
        assert "id=100" in text
        assert "200 сообщений" in text


class TestAnalyticsToolGetMessageVelocity:
    @pytest.mark.asyncio
    async def test_empty(self, mock_db):
        with patch("src.services.trend_service.TrendService") as mock_svc:
            mock_svc.return_value.get_message_velocity = AsyncMock(return_value=[])
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["get_message_velocity"]({"days": 30})
        assert "не найдены" in _text(result)

    @pytest.mark.asyncio
    async def test_with_velocity(self, mock_db):
        velocity = [
            SimpleNamespace(date="2026-01-01", count=1000),
            SimpleNamespace(date="2026-01-02", count=1200),
        ]
        with patch("src.services.trend_service.TrendService") as mock_svc:
            mock_svc.return_value.get_message_velocity = AsyncMock(return_value=velocity)
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["get_message_velocity"]({"days": 7})
        text = _text(result)
        assert "2026-01-01" in text
        assert "1000 сообщений" in text

    @pytest.mark.asyncio
    async def test_error(self, mock_db):
        with patch("src.services.trend_service.TrendService") as mock_svc:
            mock_svc.return_value.get_message_velocity = AsyncMock(side_effect=Exception("err"))
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["get_message_velocity"]({})
        assert "Ошибка" in _text(result)


class TestAnalyticsToolGetPeakHours:
    @pytest.mark.asyncio
    async def test_empty(self, mock_db):
        with patch("src.services.trend_service.TrendService") as mock_svc:
            mock_svc.return_value.get_peak_hours = AsyncMock(return_value=[])
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["get_peak_hours"]({})
        assert "не найдены" in _text(result)

    @pytest.mark.asyncio
    async def test_with_hours(self, mock_db):
        hours = [
            SimpleNamespace(hour=9, count=500),
            SimpleNamespace(hour=18, count=800),
        ]
        with patch("src.services.trend_service.TrendService") as mock_svc:
            mock_svc.return_value.get_peak_hours = AsyncMock(return_value=hours)
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["get_peak_hours"]({})
        text = _text(result)
        assert "09:00" in text
        assert "500 сообщений" in text
        assert "18:00" in text

    @pytest.mark.asyncio
    async def test_error(self, mock_db):
        with patch("src.services.trend_service.TrendService") as mock_svc:
            mock_svc.return_value.get_peak_hours = AsyncMock(side_effect=Exception("err"))
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["get_peak_hours"]({})
        assert "Ошибка" in _text(result)


class TestAnalyticsToolGetCalendar:
    @pytest.mark.asyncio
    async def test_empty(self, mock_db):
        with patch("src.services.content_calendar_service.ContentCalendarService") as mock_svc:
            mock_svc.return_value.get_upcoming = AsyncMock(return_value=[])
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["get_calendar"]({"limit": 10})
        assert "Нет запланированных" in _text(result)

    @pytest.mark.asyncio
    async def test_with_events(self, mock_db):
        event = SimpleNamespace(
            run_id=7,
            pipeline_id=2,
            pipeline_name="Content Pipe",
            moderation_status="approved",
            scheduled_time="2026-05-01T09:00:00",
            created_at="2026-04-30",
            preview="Preview of upcoming post",
        )
        with patch("src.services.content_calendar_service.ContentCalendarService") as mock_svc:
            mock_svc.return_value.get_upcoming = AsyncMock(return_value=[event])
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["get_calendar"]({})
        text = _text(result)
        assert "run_id=7" in text
        assert "Content Pipe" in text
        assert "Preview of upcoming post" in text

    @pytest.mark.asyncio
    async def test_error(self, mock_db):
        with patch("src.services.content_calendar_service.ContentCalendarService") as mock_svc:
            mock_svc.return_value.get_upcoming = AsyncMock(side_effect=Exception("cal err"))
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["get_calendar"]({})
        assert "Ошибка" in _text(result)


class TestAnalyticsToolGetDailyStats:
    @pytest.mark.asyncio
    async def test_empty(self, mock_db):
        with patch("src.services.content_analytics_service.ContentAnalyticsService") as mock_svc:
            mock_svc.return_value.get_daily_stats = AsyncMock(return_value=[])
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["get_daily_stats"]({"days": 7})
        assert "Нет данных" in _text(result)

    @pytest.mark.asyncio
    async def test_with_data(self, mock_db):
        rows = [
            {"date": "2026-01-01", "count": 10, "published": 8},
            {"date": "2026-01-02", "count": 15, "published": 12},
        ]
        with patch("src.services.content_analytics_service.ContentAnalyticsService") as mock_svc:
            mock_svc.return_value.get_daily_stats = AsyncMock(return_value=rows)
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["get_daily_stats"]({"days": 7, "pipeline_id": 1})
        text = _text(result)
        assert "2026-01-01" in text
        assert "генераций=10" in text
        assert "опубл.=8" in text

    @pytest.mark.asyncio
    async def test_default_days(self, mock_db):
        with patch("src.services.content_analytics_service.ContentAnalyticsService") as mock_svc:
            mock_svc.return_value.get_daily_stats = AsyncMock(return_value=[])
            handlers = _get_tool_handlers(mock_db)
            await handlers["get_daily_stats"]({})
            # Should call with days=30 by default
            mock_svc.return_value.get_daily_stats.assert_awaited_once_with(days=30, pipeline_id=None)

    @pytest.mark.asyncio
    async def test_error(self, mock_db):
        with patch("src.services.content_analytics_service.ContentAnalyticsService") as mock_svc:
            mock_svc.return_value.get_daily_stats = AsyncMock(side_effect=Exception("stats err"))
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["get_daily_stats"]({})
        assert "Ошибка" in _text(result)
