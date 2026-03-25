"""Tests for src/agent/tools.py - MCP tools for Agent.

These tests call the actual tool handler functions via the @tool decorator's
.handler attribute, ensuring argument parsing, formatting, and error handling
are all exercised.
"""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.database import Database


@pytest.fixture
def mock_db():
    """Create a mock Database for testing tools."""
    return MagicMock(spec=Database)


def _get_tool_handlers(mock_db, client_pool=None, config=None):
    """Build MCP tools and return their handlers keyed by name."""
    captured_tools = []

    with patch(
        "src.agent.tools.create_sdk_mcp_server",
        side_effect=lambda **kwargs: captured_tools.extend(kwargs.get("tools", [])),
    ):
        from src.agent.tools import make_mcp_server

        make_mcp_server(mock_db, client_pool=client_pool, config=config)

    return {t.name: t.handler for t in captured_tools}


def _text(result: dict) -> str:
    """Extract text from tool result payload."""
    return result["content"][0]["text"]


# ---------------------------------------------------------------------------
# search_messages tool
# ---------------------------------------------------------------------------


class TestSearchMessagesTool:
    """Tests for the search_messages tool handler."""

    @pytest.mark.asyncio
    async def test_empty_result(self, mock_db):
        mock_db.search_messages = AsyncMock(return_value=([], 0))
        handlers = _get_tool_handlers(mock_db)

        result = await handlers["search_messages"]({"query": "nonexistent", "limit": 20})

        assert result["content"][0]["type"] == "text"
        assert "Ничего не найдено" in _text(result)
        assert "nonexistent" in _text(result)
        mock_db.search_messages.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_with_results(self, mock_db):
        mock_messages = [
            SimpleNamespace(
                channel_id=100,
                message_id=1,
                text="Test message one",
                date="2025-01-01",
            ),
            SimpleNamespace(
                channel_id=200,
                message_id=2,
                text="Another test message",
                date="2025-01-02",
            ),
        ]
        mock_db.search_messages = AsyncMock(return_value=(mock_messages, 2))
        handlers = _get_tool_handlers(mock_db)

        result = await handlers["search_messages"]({"query": "test", "limit": 10})

        text = _text(result)
        assert "Найдено 2 сообщений" in text
        assert "channel_id=100" in text
        assert "channel_id=200" in text
        mock_db.search_messages.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_text_truncation(self, mock_db):
        """Tool truncates message text to 300 chars."""
        long_text = "x" * 500
        mock_messages = [
            SimpleNamespace(channel_id=100, message_id=1, text=long_text, date="2025-01-01"),
        ]
        mock_db.search_messages = AsyncMock(return_value=(mock_messages, 1))
        handlers = _get_tool_handlers(mock_db)

        result = await handlers["search_messages"]({"query": "x", "limit": 20})

        text = _text(result)
        # Preview is capped at 300 chars
        assert "x" * 300 in text
        assert "x" * 301 not in text

    @pytest.mark.asyncio
    async def test_none_text_handled(self, mock_db):
        """Tool handles messages with None text without crashing."""
        mock_messages = [
            SimpleNamespace(channel_id=100, message_id=1, text=None, date="2025-01-01"),
        ]
        mock_db.search_messages = AsyncMock(return_value=(mock_messages, 1))
        handlers = _get_tool_handlers(mock_db)

        result = await handlers["search_messages"]({"query": "test", "limit": 20})

        text = _text(result)
        assert "channel_id=100" in text

    @pytest.mark.asyncio
    async def test_default_limit(self, mock_db):
        """Tool applies default limit=20 when not provided."""
        mock_db.search_messages = AsyncMock(return_value=([], 0))
        handlers = _get_tool_handlers(mock_db)

        await handlers["search_messages"]({"query": "test"})

        mock_db.search_messages.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_custom_limit(self, mock_db):
        mock_db.search_messages = AsyncMock(return_value=([], 0))
        handlers = _get_tool_handlers(mock_db)

        await handlers["search_messages"]({"query": "test", "limit": 50})

        mock_db.search_messages.assert_awaited_once()
        call_kwargs = mock_db.search_messages.await_args.kwargs
        assert call_kwargs["query"] == "test"
        assert call_kwargs["limit"] == 50

    @pytest.mark.asyncio
    async def test_error_returns_text_not_exception(self, mock_db):
        """Tool catches DB errors and returns error text (no exception raised)."""
        mock_db.search_messages = AsyncMock(side_effect=Exception("DB connection error"))
        handlers = _get_tool_handlers(mock_db)

        result = await handlers["search_messages"]({"query": "test", "limit": 20})

        text = _text(result)
        assert "Ошибка поиска сообщений" in text
        assert "DB connection error" in text


class TestSemanticSearchTool:
    @pytest.mark.asyncio
    async def test_with_results(self, mock_db):
        mock_messages = [
            SimpleNamespace(
                channel_id=100,
                message_id=1,
                text="Semantic result",
                date="2025-01-01",
            ),
        ]
        mock_db.search_semantic_messages = AsyncMock(return_value=(mock_messages, 1))

        class FakeEmbeddingService:
            def __init__(self, _db, **_kwargs):
                pass

            async def index_pending_messages(self):
                return 0

            async def embed_query(self, query):
                assert query == "semantic"
                return [1.0, 0.0]

        with patch("src.services.embedding_service.EmbeddingService", FakeEmbeddingService):
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["semantic_search"]({"query": "semantic", "limit": 5})

        text = _text(result)
        assert "Семантически найдено 1 сообщений" in text
        assert "Semantic result" in text
        mock_db.search_semantic_messages.assert_awaited_once_with([1.0, 0.0], limit=5)

    @pytest.mark.asyncio
    async def test_error_returns_text_not_exception(self, mock_db):
        class BrokenEmbeddingService:
            def __init__(self, _db, **_kwargs):
                pass

            async def embed_query(self, query):
                raise RuntimeError("vec unavailable")

        with patch("src.services.embedding_service.EmbeddingService", BrokenEmbeddingService):
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["semantic_search"]({"query": "semantic", "limit": 5})

        text = _text(result)
        assert "Ошибка семантического поиска" in text
        assert "vec unavailable" in text


# ---------------------------------------------------------------------------
# get_channels tool
# ---------------------------------------------------------------------------


class TestGetChannelsTool:
    """Tests for the get_channels tool handler."""

    @pytest.mark.asyncio
    async def test_empty(self, mock_db):
        mock_db.get_channels = AsyncMock(return_value=[])
        handlers = _get_tool_handlers(mock_db)

        result = await handlers["list_channels"]({})

        assert "Каналы не найдены" in _text(result)

    @pytest.mark.asyncio
    async def test_with_channels(self, mock_db):
        mock_channels = [
            SimpleNamespace(
                channel_id=100,
                title="Active Channel",
                username="active_ch",
                is_active=True,
                is_filtered=False,
                channel_type="channel",
            ),
            SimpleNamespace(
                channel_id=200,
                title="Inactive Channel",
                username="inactive_ch",
                is_active=False,
                is_filtered=True,
                channel_type="channel",
            ),
        ]
        mock_db.get_channels = AsyncMock(return_value=mock_channels)
        handlers = _get_tool_handlers(mock_db)

        result = await handlers["list_channels"]({})

        text = _text(result)
        assert "Каналы (2)" in text
        assert "@active_ch" in text
        assert "активен" in text
        assert "неактивен" in text
        assert "[отфильтрован]" in text

    @pytest.mark.asyncio
    async def test_none_username(self, mock_db):
        """Channel with username=None renders @None (known pre-existing issue)."""
        mock_channels = [
            SimpleNamespace(
                channel_id=100,
                title="Private Channel",
                username=None,
                is_active=True,
                is_filtered=False,
                channel_type="channel",
            ),
        ]
        mock_db.get_channels = AsyncMock(return_value=mock_channels)
        handlers = _get_tool_handlers(mock_db)

        result = await handlers["list_channels"]({})

        text = _text(result)
        assert "Private Channel" in text
        # Known issue: renders @None for channels without username
        assert "@None" in text

    @pytest.mark.asyncio
    async def test_error_returns_text_not_exception(self, mock_db):
        """Tool catches DB errors and returns error text."""
        mock_db.get_channels = AsyncMock(side_effect=Exception("DB query failed"))
        handlers = _get_tool_handlers(mock_db)

        result = await handlers["list_channels"]({})

        text = _text(result)
        assert "Ошибка получения каналов" in text
        assert "DB query failed" in text


# ---------------------------------------------------------------------------
# list_pipelines tool
# ---------------------------------------------------------------------------


class TestListPipelinesTool:
    @pytest.mark.asyncio
    async def test_empty(self, mock_db):
        with patch("src.services.pipeline_service.PipelineService") as mock_svc:
            mock_svc.return_value.list = AsyncMock(return_value=[])
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["list_pipelines"]({"active_only": False})

        assert "Пайплайны не найдены" in _text(result)

    @pytest.mark.asyncio
    async def test_with_pipelines(self, mock_db):
        pipeline = SimpleNamespace(
            id=1,
            name="News Pipeline",
            is_active=True,
            llm_model="gpt-4o",
            publish_mode="auto",
            schedule_cron="0 */6 * * *",
            generation_backend="chain",
        )
        with patch("src.services.pipeline_service.PipelineService") as mock_svc:
            mock_svc.return_value.list = AsyncMock(return_value=[pipeline])
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["list_pipelines"]({})

        text = _text(result)
        assert "News Pipeline" in text
        assert "id=1" in text
        assert "gpt-4o" in text


# ---------------------------------------------------------------------------
# list_pending_moderation tool
# ---------------------------------------------------------------------------


class TestListPendingModerationTool:
    @pytest.mark.asyncio
    async def test_empty(self, mock_db):
        mock_db.repos = MagicMock()
        mock_db.repos.generation_runs.list_pending_moderation = AsyncMock(return_value=[])
        handlers = _get_tool_handlers(mock_db)

        result = await handlers["list_pending_moderation"]({})
        assert "Нет черновиков на модерации" in _text(result)

    @pytest.mark.asyncio
    async def test_with_runs(self, mock_db):
        run = SimpleNamespace(
            id=42, pipeline_id=1, generated_text="Draft content here", created_at="2026-01-01"
        )
        mock_db.repos = MagicMock()
        mock_db.repos.generation_runs.list_pending_moderation = AsyncMock(return_value=[run])
        handlers = _get_tool_handlers(mock_db)

        result = await handlers["list_pending_moderation"]({"limit": 10})
        text = _text(result)
        assert "run_id=42" in text
        assert "Draft content" in text


# ---------------------------------------------------------------------------
# approve_run / reject_run tools
# ---------------------------------------------------------------------------


class TestApproveRejectRunTools:
    @pytest.mark.asyncio
    async def test_approve_success(self, mock_db):
        mock_db.repos = MagicMock()
        mock_db.repos.generation_runs.get = AsyncMock(
            return_value=SimpleNamespace(id=1, moderation_status="pending")
        )
        mock_db.repos.generation_runs.set_moderation_status = AsyncMock()
        handlers = _get_tool_handlers(mock_db)

        result = await handlers["approve_run"]({"run_id": 1})
        assert "одобрен" in _text(result)
        mock_db.repos.generation_runs.set_moderation_status.assert_awaited_once_with(1, "approved")

    @pytest.mark.asyncio
    async def test_reject_success(self, mock_db):
        mock_db.repos = MagicMock()
        mock_db.repos.generation_runs.get = AsyncMock(
            return_value=SimpleNamespace(id=1, moderation_status="pending")
        )
        mock_db.repos.generation_runs.set_moderation_status = AsyncMock()
        handlers = _get_tool_handlers(mock_db)

        result = await handlers["reject_run"]({"run_id": 1})
        assert "отклонён" in _text(result)
        mock_db.repos.generation_runs.set_moderation_status.assert_awaited_once_with(1, "rejected")

    @pytest.mark.asyncio
    async def test_approve_missing_run_id(self, mock_db):
        mock_db.repos = MagicMock()
        handlers = _get_tool_handlers(mock_db)

        result = await handlers["approve_run"]({})
        assert "run_id обязателен" in _text(result)

    @pytest.mark.asyncio
    async def test_approve_not_found(self, mock_db):
        mock_db.repos = MagicMock()
        mock_db.repos.generation_runs.get = AsyncMock(return_value=None)
        handlers = _get_tool_handlers(mock_db)

        result = await handlers["approve_run"]({"run_id": 999})
        assert "не найден" in _text(result)


# ---------------------------------------------------------------------------
# get_analytics_summary tool
# ---------------------------------------------------------------------------


class TestAnalyticsSummaryTool:
    @pytest.mark.asyncio
    async def test_summary(self, mock_db):
        summary = {
            "total_generations": 50,
            "total_published": 30,
            "total_pending": 10,
            "total_rejected": 5,
            "pipelines_count": 3,
        }
        with patch("src.services.content_analytics_service.ContentAnalyticsService") as mock_svc:
            mock_svc.return_value.get_summary = AsyncMock(return_value=summary)
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["get_analytics_summary"]({})

        text = _text(result)
        assert "50" in text
        assert "30" in text
        assert "Опубликовано" in text


# ---------------------------------------------------------------------------
# get_trending_topics tool
# ---------------------------------------------------------------------------


class TestTrendingTopicsTool:
    @pytest.mark.asyncio
    async def test_with_topics(self, mock_db):
        topics = [
            SimpleNamespace(keyword="AI", count=100),
            SimpleNamespace(keyword="blockchain", count=50),
        ]
        with patch("src.services.trend_service.TrendService") as mock_svc:
            mock_svc.return_value.get_trending_topics = AsyncMock(return_value=topics)
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["get_trending_topics"]({"days": 7, "limit": 10})

        text = _text(result)
        assert "AI" in text
        assert "100 упоминаний" in text

    @pytest.mark.asyncio
    async def test_empty(self, mock_db):
        with patch("src.services.trend_service.TrendService") as mock_svc:
            mock_svc.return_value.get_trending_topics = AsyncMock(return_value=[])
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["get_trending_topics"]({})

        assert "не найдены" in _text(result)


# ---------------------------------------------------------------------------
# get_calendar tool
# ---------------------------------------------------------------------------


class TestCalendarTool:
    @pytest.mark.asyncio
    async def test_with_events(self, mock_db):
        event = SimpleNamespace(
            run_id=1,
            pipeline_id=1,
            pipeline_name="News",
            moderation_status="approved",
            scheduled_time="2026-03-25T10:00:00",
            created_at="2026-03-24",
            preview="Upcoming post content preview",
        )
        with patch("src.services.content_calendar_service.ContentCalendarService") as mock_svc:
            mock_svc.return_value.get_upcoming = AsyncMock(return_value=[event])
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["get_calendar"]({"limit": 5})

        text = _text(result)
        assert "News" in text
        assert "run_id=1" in text

    @pytest.mark.asyncio
    async def test_empty(self, mock_db):
        with patch("src.services.content_calendar_service.ContentCalendarService") as mock_svc:
            mock_svc.return_value.get_upcoming = AsyncMock(return_value=[])
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["get_calendar"]({})

        assert "Нет запланированных" in _text(result)


# ---------------------------------------------------------------------------
# get_pipeline_stats tool
# ---------------------------------------------------------------------------


class TestPipelineStatsTool:
    @pytest.mark.asyncio
    async def test_with_stats(self, mock_db):
        stat = SimpleNamespace(
            pipeline_id=1,
            pipeline_name="News",
            total_generations=20,
            total_published=15,
            total_rejected=2,
            pending_moderation=3,
            success_rate=0.75,
        )
        with patch("src.services.content_analytics_service.ContentAnalyticsService") as mock_svc:
            mock_svc.return_value.get_pipeline_stats = AsyncMock(return_value=[stat])
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["get_pipeline_stats"]({"pipeline_id": 1})

        text = _text(result)
        assert "News" in text
        assert "генераций=20" in text
        assert "75%" in text


# ---------------------------------------------------------------------------
# get_channel_stats tool
# ---------------------------------------------------------------------------


class TestChannelStatsTool:
    @pytest.mark.asyncio
    async def test_with_stats(self, mock_db):
        mock_db.repos = MagicMock()
        mock_db.repos.channels.get_latest_stats_for_all = AsyncMock(
            return_value={100: SimpleNamespace(channel_id=100, subscriber_count=5000, avg_views=1200)}
        )
        handlers = _get_tool_handlers(mock_db)

        result = await handlers["get_channel_stats"]({})
        text = _text(result)
        assert "channel_id=100" in text
        assert "5000" in text

    @pytest.mark.asyncio
    async def test_empty(self, mock_db):
        mock_db.repos = MagicMock()
        mock_db.repos.channels.get_latest_stats_for_all = AsyncMock(return_value={})
        handlers = _get_tool_handlers(mock_db)

        result = await handlers["get_channel_stats"]({})
        assert "не собрана" in _text(result)


# ---------------------------------------------------------------------------
# run_pipeline tool
# ---------------------------------------------------------------------------


class TestRunPipelineTool:
    @pytest.mark.asyncio
    async def test_missing_pipeline_id(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["run_pipeline"]({})
        assert "pipeline_id обязателен" in _text(result)

    @pytest.mark.asyncio
    async def test_pipeline_not_found(self, mock_db):
        with patch("src.services.pipeline_service.PipelineService") as mock_svc:
            mock_svc.return_value.get = AsyncMock(return_value=None)
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["run_pipeline"]({"pipeline_id": 999})

        assert "не найден" in _text(result)

    @pytest.mark.asyncio
    async def test_pipeline_inactive(self, mock_db):
        pipeline = SimpleNamespace(id=1, name="Inactive", is_active=False)
        with patch("src.services.pipeline_service.PipelineService") as mock_svc:
            mock_svc.return_value.get = AsyncMock(return_value=pipeline)
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["run_pipeline"]({"pipeline_id": 1})

        assert "неактивен" in _text(result)


# ---------------------------------------------------------------------------
# make_mcp_server factory
# ---------------------------------------------------------------------------


class TestMakeMcpServer:
    """Tests for make_mcp_server factory function."""

    def test_creates_server_returns_dict(self, mock_db):
        from src.agent.tools import make_mcp_server

        server = make_mcp_server(mock_db)
        assert server is not None
        assert isinstance(server, dict)
        assert server["name"] == "telegram_db"

    def test_server_has_instance(self, mock_db):
        from src.agent.tools import make_mcp_server

        server = make_mcp_server(mock_db)
        assert "instance" in server


# ---------------------------------------------------------------------------
# Image tools — DB provider loading
# ---------------------------------------------------------------------------


class TestImageToolsDBProviders:
    """Tests for image tools loading providers from DB."""

    async def test_generate_image_uses_db_providers(self, mock_db):
        """generate_image should load adapters from DB when config is provided."""
        fake_config = SimpleNamespace()

        mock_adapter = AsyncMock(return_value="/tmp/image.png")
        fake_configs = [SimpleNamespace(provider="together", enabled=True, api_key="test-key")]

        with (
            patch(
                "src.services.image_provider_service.ImageProviderService",
            ) as mock_prov_svc,
            patch(
                "src.services.image_generation_service.ImageGenerationService",
            ) as mock_img_svc,
        ):
            # Setup provider service mock
            prov_instance = mock_prov_svc.return_value
            prov_instance.load_provider_configs = AsyncMock(return_value=fake_configs)
            prov_instance.build_adapters.return_value = {"together": mock_adapter}

            # Setup image service mock
            img_instance = mock_img_svc.return_value
            img_instance.is_available = AsyncMock(return_value=True)
            img_instance.generate = AsyncMock(return_value="/tmp/image.png")

            handlers = _get_tool_handlers(mock_db, config=fake_config)
            result = await handlers["generate_image"]({"prompt": "a cat"})
            text = _text(result)

            assert "сгенерировано" in text.lower() or "/tmp/image.png" in text
            # Should have been created with adapters from DB
            mock_img_svc.assert_called_once_with(adapters={"together": mock_adapter})

    async def test_generate_image_falls_back_to_env(self, mock_db):
        """generate_image should fall back to env vars when no config provided."""
        with patch(
            "src.services.image_generation_service.ImageGenerationService",
        ) as mock_img_svc:
            img_instance = mock_img_svc.return_value
            img_instance.is_available = AsyncMock(return_value=False)

            handlers = _get_tool_handlers(mock_db)
            result = await handlers["generate_image"]({"prompt": "a cat"})
            text = _text(result)

            assert "не настроена" in text.lower() or "настройках" in text.lower()
            # Should have been created without adapters (env fallback)
            mock_img_svc.assert_called_once_with()

    async def test_list_image_providers_uses_db(self, mock_db):
        """list_image_providers should load from DB when config provided."""
        fake_config = SimpleNamespace()
        mock_adapter = AsyncMock()

        with (
            patch(
                "src.services.image_provider_service.ImageProviderService",
            ) as mock_prov_svc,
            patch(
                "src.services.image_generation_service.ImageGenerationService",
            ) as mock_img_svc,
        ):
            prov_instance = mock_prov_svc.return_value
            prov_instance.load_provider_configs = AsyncMock(
                return_value=[SimpleNamespace(provider="together", enabled=True, api_key="test-key")]
            )
            prov_instance.build_adapters.return_value = {"together": mock_adapter}

            img_instance = mock_img_svc.return_value
            img_instance.adapter_names = ["together"]

            handlers = _get_tool_handlers(mock_db, config=fake_config)
            result = await handlers["list_image_providers"]({})
            text = _text(result)

            assert "together" in text


# ---------------------------------------------------------------------------
# Config propagation — EmbeddingService & SearchEngine
# ---------------------------------------------------------------------------


class TestConfigPropagation:
    """Tests for config being properly passed to services."""

    async def test_embedding_service_receives_config(self, mock_db):
        """EmbeddingService should receive config from make_mcp_server."""
        fake_config = SimpleNamespace()

        with patch("src.services.embedding_service.EmbeddingService") as mock_cls:
            mock_cls.return_value = MagicMock()
            _get_tool_handlers(mock_db, config=fake_config)
            mock_cls.assert_called_once_with(mock_db, config=fake_config)

    async def test_embedding_service_none_config(self, mock_db):
        """EmbeddingService should receive config=None when no config provided."""
        with patch("src.services.embedding_service.EmbeddingService") as mock_cls:
            mock_cls.return_value = MagicMock()
            _get_tool_handlers(mock_db)
            mock_cls.assert_called_once_with(mock_db, config=None)

    async def test_run_pipeline_passes_config_to_search_engine(self, mock_db):
        """run_pipeline should pass config to SearchEngine."""
        fake_config = SimpleNamespace()

        with (
            patch("src.services.embedding_service.EmbeddingService") as mock_embed_cls,
            patch("src.search.engine.SearchEngine") as mock_search_cls,
            patch("src.services.content_generation_service.ContentGenerationService") as mock_gen_cls,
            patch("src.services.pipeline_service.PipelineService") as mock_pipe_cls,
            patch("src.services.image_generation_service.ImageGenerationService") as mock_img_cls,
        ):
            mock_embed_cls.return_value = MagicMock()
            mock_search_cls.return_value = MagicMock()
            mock_img_cls.return_value = MagicMock()

            mock_pipeline = SimpleNamespace(
                id=1, name="test", is_active=True, llm_model=None,
                prompt_template="test", publish_mode="moderated",
            )
            mock_pipe_cls.return_value.get = AsyncMock(return_value=mock_pipeline)

            mock_run = SimpleNamespace(
                id=1, generated_text="test output", moderation_status="pending",
            )
            mock_gen_cls.return_value.generate = AsyncMock(return_value=mock_run)

            handlers = _get_tool_handlers(mock_db, config=fake_config)
            await handlers["run_pipeline"]({"pipeline_id": 1})

            mock_search_cls.assert_called_with(mock_db, config=fake_config)
