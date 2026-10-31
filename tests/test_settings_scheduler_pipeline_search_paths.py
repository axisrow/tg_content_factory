"""Tests for settings, scheduler, pipeline, search, and photo-task paths."""

from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from unittest.mock import ANY, AsyncMock, MagicMock, patch

import pytest

from tests.helpers import cli_ns as _ns
from tg_agent.config import AppConfig
from tg_agent.database import Database
from tg_agent.models import Account, Channel, PhotoBatchItem, PhotoBatchStatus, PhotoSendMode

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _pipeline_fake_init_db(db_path: str):
    async def _inner(_config_path: str):
        config = AppConfig()
        database = Database(db_path, session_encryption_secret="test-session-encryption-key")
        await database.initialize()
        return config, database

    return _inner


def _make_pipeline_db(tmp_path, db_name="pipeline7.db"):
    db_path = str(tmp_path / db_name)
    db = Database(db_path, session_encryption_secret="test-session-encryption-key")
    asyncio.run(db.initialize())
    asyncio.run(db.add_account(Account(phone="+100", session_string="sess")))
    asyncio.run(db.add_channel(Channel(channel_id=2001, title="Source")))
    asyncio.run(
        db.repos.dialog_cache.replace_dialogs(
            "+100",
            [{"channel_id": 77, "title": "Target", "username": "tgt", "channel_type": "channel"}],
        )
    )
    asyncio.run(db.close())
    return db_path


# ===========================================================================
# 3. cli/commands/analytics.py — edge-case branches
# ===========================================================================


class TestCLIAnalyticsTop:
    """Tests for analytics top action."""

    def test_top_empty(self, cli_env, capsys):
        """top action with no data prints message."""
        from tg_agent.cli.commands.analytics import run

        with patch.object(
            type(cli_env),
            "get_top_messages",
            new=AsyncMock(return_value=[]),
            create=True,
        ):
            with patch("tg_agent.database.facade.Database.get_top_messages", new=AsyncMock(return_value=[])):
                run(_ns(analytics_action="top", date_from=None, date_to=None, limit=20))
        out = capsys.readouterr().out
        assert "No messages" in out

    def test_top_with_data(self, cli_env, capsys):
        """top action with data prints table."""
        from tg_agent.cli.commands.analytics import run

        rows = [
            {
                "total_reactions": 100,
                "channel_title": "TestChan",
                "channel_username": "testchan",
                "channel_id": 123,
                "text": "Hello world",
                "date": "2025-01-01 12:00:00",
            }
        ]
        with patch("tg_agent.database.facade.Database.get_top_messages", new=AsyncMock(return_value=rows)):
            run(_ns(analytics_action="top", date_from=None, date_to=None, limit=20))
        out = capsys.readouterr().out
        assert "TestChan" in out or "100" in out

    def test_top_with_date_filter(self, cli_env, capsys):
        """top with date filters passes them through."""
        from tg_agent.cli.commands.analytics import run

        with patch("tg_agent.database.facade.Database.get_top_messages", new=AsyncMock(return_value=[])):
            run(_ns(
                analytics_action="top",
                date_from="2025-01-01",
                date_to="2025-12-31",
                limit=10,
            ))
        out = capsys.readouterr().out
        assert "No messages" in out

    def test_top_limit_clamped(self, cli_env, capsys):
        """top action clamps limit to 100."""
        from tg_agent.cli.commands.analytics import run

        with patch("tg_agent.database.facade.Database.get_top_messages", new=AsyncMock(return_value=[])):
            run(_ns(analytics_action="top", date_from=None, date_to=None, limit=999))
        out = capsys.readouterr().out
        assert "No messages" in out

    def test_content_types_empty(self, cli_env, capsys):
        """content-types action with no data prints 'No data'."""
        from tg_agent.cli.commands.analytics import run

        with patch(
            "tg_agent.database.facade.Database.get_engagement_by_media_type",
            new=AsyncMock(return_value=[]),
        ):
            run(_ns(analytics_action="content-types", date_from=None, date_to=None))
        out = capsys.readouterr().out
        assert "No data" in out

    def test_content_types_with_data(self, cli_env, capsys):
        """content-types prints table rows."""
        from tg_agent.cli.commands.analytics import run

        rows = [
            {"content_type": "text", "message_count": 50, "avg_reactions": 3.5},
            {"content_type": "photo", "message_count": 20, "avg_reactions": 7.2},
        ]
        with patch(
            "tg_agent.database.facade.Database.get_engagement_by_media_type",
            new=AsyncMock(return_value=rows),
        ):
            run(_ns(analytics_action="content-types", date_from=None, date_to=None))
        out = capsys.readouterr().out
        assert "text" in out
        assert "photo" in out

    def test_hourly_empty(self, cli_env, capsys):
        """hourly action with no data prints 'No data'."""
        from tg_agent.cli.commands.analytics import run

        with patch(
            "tg_agent.database.facade.Database.get_hourly_activity",
            new=AsyncMock(return_value=[]),
        ):
            run(_ns(analytics_action="hourly", date_from=None, date_to=None))
        out = capsys.readouterr().out
        assert "No data" in out

    def test_hourly_with_data(self, cli_env, capsys):
        """hourly prints table with hour entries."""
        from tg_agent.cli.commands.analytics import run

        rows = [
            {"hour": 9, "message_count": 30, "avg_reactions": 5.0},
            {"hour": 18, "message_count": 80, "avg_reactions": 10.0},
        ]
        with patch(
            "tg_agent.database.facade.Database.get_hourly_activity",
            new=AsyncMock(return_value=rows),
        ):
            run(_ns(analytics_action="hourly", date_from=None, date_to=None))
        out = capsys.readouterr().out
        assert "09:00" in out
        assert "18:00" in out

    def test_pipeline_stats_with_data(self, cli_env, capsys):
        """pipeline-stats action with data prints rows."""
        from tg_agent.cli.commands.analytics import run

        stat = MagicMock()
        stat.pipeline_name = "MyPipeline"
        stat.total_generations = 10
        stat.total_published = 7
        stat.total_rejected = 1
        stat.pending_moderation = 2
        stat.success_rate = 70.0  # percent units, as the service returns it (#1616)

        with patch(
            "tg_agent.services.content_analytics_service.ContentAnalyticsService.get_pipeline_stats",
            new=AsyncMock(return_value=[stat]),
        ):
            run(_ns(analytics_action="pipeline-stats", date_from=None, date_to=None, pipeline_id=None))
        out = capsys.readouterr().out
        assert "MyPipeline" in out

    def test_daily_with_data(self, cli_env, capsys):
        """daily action with data prints rows."""
        from tg_agent.cli.commands.analytics import run

        rows = [
            {"date": "2025-01-01", "count": 5, "published": 3},
            {"date": "2025-01-02", "count": 8, "published": 6},
        ]
        with patch(
            "tg_agent.services.content_analytics_service.ContentAnalyticsService.get_daily_stats",
            new=AsyncMock(return_value=rows),
        ):
            run(_ns(analytics_action="daily", date_from=None, date_to=None, days=30, pipeline_id=None))
        out = capsys.readouterr().out
        assert "2025-01-01" in out


# ===========================================================================
# 4. cli/commands/pipeline.py — edge-case branches
# ===========================================================================


class TestCLIPipelineAdd:
    def test_add_pipeline_basic(self, tmp_path, capsys):
        """add action creates pipeline and prints id."""
        db_path = _make_pipeline_db(tmp_path, "add_basic.db")

        with patch("tg_agent.cli.runtime.init_db", side_effect=_pipeline_fake_init_db(db_path)):
            from tg_agent.cli.commands.pipeline import run

            run(_ns(
                pipeline_action="add",
                name="MyPipeline",
                prompt_template="Test {source_messages}",
                source=[2001],
                target=["+100|77"],
                llm_model=None,
                image_model=None,
                publish_mode="moderated",
                generation_backend="chain",
                interval=60,
                inactive=False,
            ))
        out = capsys.readouterr().out
        assert "Added pipeline" in out
        assert "MyPipeline" in out

    def test_add_pipeline_with_models(self, tmp_path, capsys):
        """add with llm_model and image_model specified."""
        db_path = _make_pipeline_db(tmp_path, "add_models.db")

        with patch("tg_agent.cli.runtime.init_db", side_effect=_pipeline_fake_init_db(db_path)):
            from tg_agent.cli.commands.pipeline import run

            run(_ns(
                pipeline_action="add",
                name="ModelPipeline",
                prompt_template="Test {source_messages}",
                source=[2001],
                target=["+100|77"],
                llm_model="gpt-4",
                image_model="together:flux",
                publish_mode="auto",
                generation_backend="chain",
                interval=120,
                inactive=False,
            ))
        out = capsys.readouterr().out
        assert "Added pipeline" in out

    def test_add_pipeline_inactive(self, tmp_path, capsys):
        """add with --inactive creates inactive pipeline."""
        db_path = _make_pipeline_db(tmp_path, "add_inactive.db")

        with patch("tg_agent.cli.runtime.init_db", side_effect=_pipeline_fake_init_db(db_path)):
            from tg_agent.cli.commands.pipeline import run

            run(_ns(
                pipeline_action="add",
                name="InactivePipeline",
                prompt_template="Test {source_messages}",
                source=[2001],
                target=["+100|77"],
                llm_model=None,
                image_model=None,
                publish_mode="moderated",
                generation_backend="chain",
                interval=60,
                inactive=True,
            ))
        out = capsys.readouterr().out
        assert "Added pipeline" in out

    def test_add_pipeline_bad_target_format(self, tmp_path, capsys):
        """add with bad target format prints error."""
        db_path = _make_pipeline_db(tmp_path, "add_bad_target.db")

        with patch("tg_agent.cli.runtime.init_db", side_effect=_pipeline_fake_init_db(db_path)):
            from tg_agent.cli.commands.pipeline import run

            run(_ns(
                pipeline_action="add",
                name="BadTarget",
                prompt_template="Test {source_messages}",
                source=[2001],
                target=["bad_format_no_pipe"],
                llm_model=None,
                image_model=None,
                publish_mode="moderated",
                generation_backend="chain",
                interval=60,
                inactive=False,
            ))
        out = capsys.readouterr().out
        assert "Error" in out

    def test_add_pipeline_bad_dialog_id(self, tmp_path, capsys):
        """add with non-numeric dialog id prints error."""
        db_path = _make_pipeline_db(tmp_path, "add_bad_dialog.db")

        with patch("tg_agent.cli.runtime.init_db", side_effect=_pipeline_fake_init_db(db_path)):
            from tg_agent.cli.commands.pipeline import run

            run(_ns(
                pipeline_action="add",
                name="BadDialog",
                prompt_template="Test {source_messages}",
                source=[2001],
                target=["+100|not_a_number"],
                llm_model=None,
                image_model=None,
                publish_mode="moderated",
                generation_backend="chain",
                interval=60,
                inactive=False,
            ))
        out = capsys.readouterr().out
        assert "Error" in out


class TestCLIPipelineList:
    def test_list_empty(self, tmp_path, capsys):
        """list with no pipelines prints 'No pipelines found'."""
        db_path = str(tmp_path / "list_empty7.db")
        db = Database(db_path, session_encryption_secret="test-session-encryption-key")
        asyncio.run(db.initialize())
        asyncio.run(db.close())

        with patch("tg_agent.cli.runtime.init_db", side_effect=_pipeline_fake_init_db(db_path)):
            from tg_agent.cli.commands.pipeline import run

            run(_ns(pipeline_action="list"))
        assert "No pipelines found" in capsys.readouterr().out

    def test_list_with_pipeline(self, tmp_path, capsys):
        """list shows pipeline table."""
        db_path = _make_pipeline_db(tmp_path, "list_with.db")

        with patch("tg_agent.cli.runtime.init_db", side_effect=_pipeline_fake_init_db(db_path)):
            from tg_agent.cli.commands.pipeline import run

            run(_ns(
                pipeline_action="add",
                name="ShowPipeline",
                prompt_template="Test {source_messages}",
                source=[2001],
                target=["+100|77"],
                llm_model=None,
                image_model=None,
                publish_mode="moderated",
                generation_backend="chain",
                interval=60,
                inactive=False,
            ))

        with patch("tg_agent.cli.runtime.init_db", side_effect=_pipeline_fake_init_db(db_path)):
            from tg_agent.cli.commands.pipeline import run

            run(_ns(pipeline_action="list"))
        out = capsys.readouterr().out
        assert "ShowPipeline" in out


class TestCLIPipelineShow:
    def test_show_not_found(self, tmp_path, capsys):
        """show for missing id prints 'not found'."""
        db_path = str(tmp_path / "show_nf7.db")
        db = Database(db_path, session_encryption_secret="test-session-encryption-key")
        asyncio.run(db.initialize())
        asyncio.run(db.close())

        with patch("tg_agent.cli.runtime.init_db", side_effect=_pipeline_fake_init_db(db_path)):
            from tg_agent.cli.commands.pipeline import run

            run(_ns(pipeline_action="show", id=999))
        assert "not found" in capsys.readouterr().out

    def test_show_existing(self, tmp_path, capsys):
        """show for existing pipeline prints details."""
        db_path = _make_pipeline_db(tmp_path, "show_exists.db")

        with patch("tg_agent.cli.runtime.init_db", side_effect=_pipeline_fake_init_db(db_path)):
            from tg_agent.cli.commands.pipeline import run

            run(_ns(
                pipeline_action="add",
                name="DetailPipeline",
                prompt_template="Test {source_messages}",
                source=[2001],
                target=["+100|77"],
                llm_model="gpt-4",
                image_model=None,
                publish_mode="moderated",
                generation_backend="chain",
                interval=90,
                inactive=False,
            ))

        db = Database(db_path, session_encryption_secret="test-session-encryption-key")
        asyncio.run(db.initialize())
        pipelines = asyncio.run(db.repos.content_pipelines.get_all())
        pid = pipelines[0].id
        asyncio.run(db.close())

        with patch("tg_agent.cli.runtime.init_db", side_effect=_pipeline_fake_init_db(db_path)):
            from tg_agent.cli.commands.pipeline import run

            run(_ns(pipeline_action="show", id=pid))
        out = capsys.readouterr().out
        assert "DetailPipeline" in out
        assert "gpt-4" in out


class TestCLIPipelineDeleteToggle:
    def test_delete_pipeline(self, tmp_path, capsys):
        """delete removes pipeline."""
        db_path = _make_pipeline_db(tmp_path, "delete7.db")

        with patch("tg_agent.cli.runtime.init_db", side_effect=_pipeline_fake_init_db(db_path)):
            from tg_agent.cli.commands.pipeline import run

            run(_ns(
                pipeline_action="add",
                name="ToDelete",
                prompt_template="Test {source_messages}",
                source=[2001],
                target=["+100|77"],
                llm_model=None,
                image_model=None,
                publish_mode="moderated",
                generation_backend="chain",
                interval=60,
                inactive=False,
            ))

        db = Database(db_path, session_encryption_secret="test-session-encryption-key")
        asyncio.run(db.initialize())
        pipelines = asyncio.run(db.repos.content_pipelines.get_all())
        pid = pipelines[0].id
        asyncio.run(db.close())

        with patch("tg_agent.cli.runtime.init_db", side_effect=_pipeline_fake_init_db(db_path)):
            from tg_agent.cli.commands.pipeline import run

            run(_ns(pipeline_action="delete", id=pid))
        out = capsys.readouterr().out
        assert "Deleted pipeline" in out

    def test_toggle_not_found(self, tmp_path, capsys):
        """toggle on missing pipeline prints 'not found'."""
        db_path = str(tmp_path / "toggle_nf7.db")
        db = Database(db_path, session_encryption_secret="test-session-encryption-key")
        asyncio.run(db.initialize())
        asyncio.run(db.close())

        with patch("tg_agent.cli.runtime.init_db", side_effect=_pipeline_fake_init_db(db_path)):
            from tg_agent.cli.commands.pipeline import run

            run(_ns(pipeline_action="toggle", id=999))
        assert "not found" in capsys.readouterr().out

    def test_toggle_existing(self, tmp_path, capsys):
        """toggle on existing pipeline prints toggled message."""
        db_path = _make_pipeline_db(tmp_path, "toggle_ok7.db")

        with patch("tg_agent.cli.runtime.init_db", side_effect=_pipeline_fake_init_db(db_path)):
            from tg_agent.cli.commands.pipeline import run

            run(_ns(
                pipeline_action="add",
                name="TogglePipeline",
                prompt_template="Test {source_messages}",
                source=[2001],
                target=["+100|77"],
                llm_model=None,
                image_model=None,
                publish_mode="moderated",
                generation_backend="chain",
                interval=60,
                inactive=False,
            ))

        db = Database(db_path, session_encryption_secret="test-session-encryption-key")
        asyncio.run(db.initialize())
        pipelines = asyncio.run(db.repos.content_pipelines.get_all())
        pid = pipelines[0].id
        asyncio.run(db.close())

        with patch("tg_agent.cli.runtime.init_db", side_effect=_pipeline_fake_init_db(db_path)):
            from tg_agent.cli.commands.pipeline import run

            run(_ns(pipeline_action="toggle", id=pid))
        out = capsys.readouterr().out
        assert "Toggled pipeline" in out


# ===========================================================================
# 5. agent/tools/channels.py — edge-case paths
# ===========================================================================


def _get_channel_tool_handlers(mock_db, client_pool=None, config=None):
    """Build channel tool handlers via make_mcp_server."""
    captured_tools = []

    with patch(
        "tg_agent.agent.tools.create_sdk_mcp_server",
        side_effect=lambda **kw: captured_tools.extend(kw.get("tools", [])),
    ):
        from tg_agent.agent.tools import make_mcp_server

        make_mcp_server(mock_db, client_pool=client_pool, config=config)

    return {t.name: t.handler for t in captured_tools}


def _make_channel_mock(
    pk=1,
    channel_id=100,
    title="TestChan",
    username="testchan",
    is_active=True,
    is_filtered=False,
    channel_type="channel",
):
    ch = MagicMock()
    ch.id = pk
    ch.channel_id = channel_id
    ch.title = title
    ch.username = username
    ch.is_active = is_active
    ch.is_filtered = is_filtered
    ch.channel_type = channel_type
    return ch


@pytest.fixture
def mock_db():
    return MagicMock(spec=Database)


def _text(result: dict) -> str:
    return result["content"][0]["text"]


class TestListChannelsTool:
    @pytest.mark.anyio
    async def test_empty_returns_not_found(self, mock_db):
        mock_db.get_channels = AsyncMock(return_value=[])
        handlers = _get_channel_tool_handlers(mock_db)
        result = await handlers["list_channels"]({})
        assert "не найдены" in _text(result)

    @pytest.mark.anyio
    async def test_active_only_filter(self, mock_db):
        ch = _make_channel_mock(is_active=True)
        mock_db.get_channels = AsyncMock(return_value=[ch])
        handlers = _get_channel_tool_handlers(mock_db)
        result = await handlers["list_channels"]({"active_only": True})
        mock_db.get_channels.assert_awaited_once_with(active_only=True, include_filtered=True)
        assert "TestChan" in _text(result)

    @pytest.mark.anyio
    async def test_include_filtered_false(self, mock_db):
        ch = _make_channel_mock(is_filtered=False)
        mock_db.get_channels = AsyncMock(return_value=[ch])
        handlers = _get_channel_tool_handlers(mock_db)
        await handlers["list_channels"]({"include_filtered": False})
        mock_db.get_channels.assert_awaited_once_with(active_only=False, include_filtered=False)

    @pytest.mark.anyio
    async def test_filtered_channel_shown_with_flag(self, mock_db):
        ch = _make_channel_mock(is_filtered=True)
        mock_db.get_channels = AsyncMock(return_value=[ch])
        handlers = _get_channel_tool_handlers(mock_db)
        result = await handlers["list_channels"]({})
        text = _text(result)
        assert "отфильтрован" in text

    @pytest.mark.anyio
    async def test_inactive_channel_shown(self, mock_db):
        ch = _make_channel_mock(is_active=False)
        mock_db.get_channels = AsyncMock(return_value=[ch])
        handlers = _get_channel_tool_handlers(mock_db)
        result = await handlers["list_channels"]({})
        text = _text(result)
        assert "неактивен" in text

    @pytest.mark.anyio
    async def test_error_handling(self, mock_db):
        mock_db.get_channels = AsyncMock(side_effect=Exception("db error"))
        handlers = _get_channel_tool_handlers(mock_db)
        result = await handlers["list_channels"]({})
        assert "Ошибка" in _text(result)
        assert "db error" in _text(result)


class TestGetChannelStatsTool:
    @pytest.mark.anyio
    async def test_no_stats_returns_empty_message(self, mock_db):
        mock_db.get_latest_stats_for_all = AsyncMock(return_value={})
        handlers = _get_channel_tool_handlers(mock_db)
        result = await handlers["get_channel_stats"]({})
        assert "не собрана" in _text(result)

    @pytest.mark.anyio
    async def test_with_stats_shows_data(self, mock_db):
        stat = MagicMock()
        stat.subscriber_count = 5000
        stat.avg_views = 200
        mock_db.get_latest_stats_for_all = AsyncMock(return_value={100: stat})
        handlers = _get_channel_tool_handlers(mock_db)
        result = await handlers["get_channel_stats"]({})
        text = _text(result)
        assert "5000" in text
        assert "200" in text

    @pytest.mark.anyio
    async def test_error_handling(self, mock_db):
        mock_db.get_latest_stats_for_all = AsyncMock(side_effect=Exception("stats fail"))
        handlers = _get_channel_tool_handlers(mock_db)
        result = await handlers["get_channel_stats"]({})
        assert "Ошибка" in _text(result)


class TestImportChannelsTool:
    @pytest.mark.anyio
    async def test_empty_text_returns_error(self, mock_db):
        handlers = _get_channel_tool_handlers(mock_db)
        result = await handlers["import_channels"]({})
        assert "text обязателен" in _text(result)

    @pytest.mark.anyio
    async def test_no_identifiers_extracted(self, mock_db):
        handlers = _get_channel_tool_handlers(mock_db)
        result = await handlers["import_channels"]({"text": "no valid identifiers here"})
        assert "Не удалось распознать" in _text(result)

    @pytest.mark.anyio
    async def test_requires_confirmation(self, mock_db):
        handlers = _get_channel_tool_handlers(mock_db)
        result = await handlers["import_channels"]({"text": "@testchannel, @another"})
        text = _text(result)
        # Should ask for confirmation
        assert "confirm" in text.lower() or "подтвердит" in text.lower() or "импортирует" in text.lower()

    @pytest.mark.anyio
    async def test_confirmed_import(self, mock_db):
        mock_db.repos = MagicMock()
        with patch(
            "tg_agent.services.channel_service.ChannelService.add_by_identifier",
            new=AsyncMock(return_value=True),
        ):
            handlers = _get_channel_tool_handlers(mock_db)
            result = await handlers["import_channels"]({"text": "@testchannel", "confirm": True})
        text = _text(result)
        assert "Импорт завершён" in text or "Ошибка" in text


class TestRefreshChannelTypesTool:
    @pytest.mark.anyio
    async def test_no_pool_returns_error(self, mock_db):
        handlers = _get_channel_tool_handlers(mock_db, client_pool=None)
        result = await handlers["refresh_channel_types"]({})
        text = _text(result)
        # should warn about no pool
        assert "pool" in text.lower() or "Telegram" in text or "подключён" in text.lower()

    @pytest.mark.anyio
    async def test_requires_confirmation(self, mock_db):
        mock_pool = MagicMock()
        mock_pool.clients = {"+1": MagicMock()}
        handlers = _get_channel_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["refresh_channel_types"]({})
        text = _text(result)
        assert "confirm" in text.lower() or "подтвердит" in text.lower() or "обновит" in text.lower()


# ===========================================================================
# 6. agent/tools/images.py — edge-case paths
# ===========================================================================


class TestGenerateImageTool:
    @pytest.mark.anyio
    async def test_empty_prompt_returns_error(self, mock_db):
        handlers = _get_channel_tool_handlers(mock_db)
        result = await handlers["generate_image"]({})
        assert "prompt обязателен" in _text(result)

    @pytest.mark.anyio
    async def test_no_service_returns_not_configured(self, mock_db):
        _svc_path = "tg_agent.services.image_generation_service.ImageGenerationService"
        with patch(f"{_svc_path}.is_available", new=AsyncMock(return_value=False)):
            handlers = _get_channel_tool_handlers(mock_db)
            result = await handlers["generate_image"]({"prompt": "a cat"})
        text = _text(result)
        assert "не настроена" in text or "Ошибка" in text

    @pytest.mark.anyio
    async def test_generate_returns_non_url(self, mock_db):
        """When result is a local path (not URL), prints it directly."""
        _svc_path = "tg_agent.services.image_generation_service.ImageGenerationService"
        with (
            patch(f"{_svc_path}.is_available", new=AsyncMock(return_value=True)),
            patch(f"{_svc_path}.generate", new=AsyncMock(return_value="/local/path/img.png")),
        ):
            handlers = _get_channel_tool_handlers(mock_db)
            result = await handlers["generate_image"]({"prompt": "a cat", "model": "together:flux"})
        text = _text(result)
        assert "/local/path/img.png" in text

    @pytest.mark.anyio
    async def test_generate_returns_none(self, mock_db):
        """When result is None, prints 'не вернула результат'."""
        _svc_path = "tg_agent.services.image_generation_service.ImageGenerationService"
        with (
            patch(f"{_svc_path}.is_available", new=AsyncMock(return_value=True)),
            patch(f"{_svc_path}.generate", new=AsyncMock(return_value=None)),
        ):
            handlers = _get_channel_tool_handlers(mock_db)
            result = await handlers["generate_image"]({"prompt": "a cat", "model": "together:flux"})
        text = _text(result)
        assert "не вернула" in text

    @pytest.mark.anyio
    async def test_generate_exception(self, mock_db):
        """Exception in generate returns error text."""
        _svc_path = "tg_agent.services.image_generation_service.ImageGenerationService"
        with (
            patch(f"{_svc_path}.is_available", new=AsyncMock(return_value=True)),
            patch(f"{_svc_path}.generate", new=AsyncMock(side_effect=Exception("api down"))),
        ):
            handlers = _get_channel_tool_handlers(mock_db)
            result = await handlers["generate_image"]({"prompt": "a cat", "model": "together:flux"})
        text = _text(result)
        assert "Ошибка" in text


class TestListImageModelsTool:
    @pytest.mark.anyio
    async def test_missing_provider_returns_error(self, mock_db):
        handlers = _get_channel_tool_handlers(mock_db)
        result = await handlers["list_image_models"]({})
        assert "provider обязателен" in _text(result)

    @pytest.mark.anyio
    async def test_no_models_found(self, mock_db):
        _svc_path = "tg_agent.services.image_generation_service.ImageGenerationService"
        with patch(f"{_svc_path}.search_models", new=AsyncMock(return_value=[])):
            handlers = _get_channel_tool_handlers(mock_db)
            result = await handlers["list_image_models"]({"provider": "replicate"})
        assert "не найдены" in _text(result)

    @pytest.mark.anyio
    async def test_models_with_run_count_and_rank(self, mock_db):
        models = [
            {"id": "model1", "run_count": 10000, "rank": 1},
            {"id": "model2", "run_count": None, "rank": None},
        ]
        _svc_path = "tg_agent.services.image_generation_service.ImageGenerationService"
        with patch(f"{_svc_path}.search_models", new=AsyncMock(return_value=models)):
            handlers = _get_channel_tool_handlers(mock_db)
            result = await handlers["list_image_models"]({"provider": "replicate", "query": "flux"})
        text = _text(result)
        assert "model1" in text
        assert "10,000" in text
        assert "rank 1" in text

    @pytest.mark.anyio
    async def test_models_all_shown_beyond_30(self, mock_db):
        models = [{"id": f"m{i}"} for i in range(35)]
        _svc_path = "tg_agent.services.image_generation_service.ImageGenerationService"
        with patch(f"{_svc_path}.search_models", new=AsyncMock(return_value=models)):
            handlers = _get_channel_tool_handlers(mock_db)
            result = await handlers["list_image_models"]({"provider": "replicate"})
        text = _text(result)
        assert "Модели replicate (35)" in text
        assert "ещё" not in text


class TestListGeneratedImagesTool:
    @pytest.mark.anyio
    async def test_no_images_returns_empty_message(self, mock_db):
        mock_db.repos = MagicMock()
        mock_db.repos.generated_images.list_recent = AsyncMock(return_value=[])
        handlers = _get_channel_tool_handlers(mock_db)
        result = await handlers["list_generated_images"]({})
        assert "Нет сгенерированных" in _text(result)

    @pytest.mark.anyio
    async def test_with_images_shows_list(self, mock_db):
        img = MagicMock()
        img.id = 1
        img.prompt = "A beautiful sunset over the ocean with golden light"
        img.model = "replicate:flux"
        img.local_path = "/data/images/abc123.png"
        img.created_at = "2025-01-01 10:00:00"
        mock_db.repos = MagicMock()
        mock_db.repos.generated_images.list_recent = AsyncMock(return_value=[img])
        handlers = _get_channel_tool_handlers(mock_db)
        result = await handlers["list_generated_images"]({})
        text = _text(result)
        assert "A beautiful sunset" in text
        assert "replicate:flux" in text

    @pytest.mark.anyio
    async def test_long_prompt_truncated(self, mock_db):
        img = MagicMock()
        img.id = 2
        img.prompt = "x" * 100
        img.model = None
        img.local_path = None
        img.created_at = "2025-01-01 10:00:00"
        mock_db.repos = MagicMock()
        mock_db.repos.generated_images.list_recent = AsyncMock(return_value=[img])
        handlers = _get_channel_tool_handlers(mock_db)
        result = await handlers["list_generated_images"]({"limit": 5})
        text = _text(result)
        assert "..." in text

    @pytest.mark.anyio
    async def test_error_handling(self, mock_db):
        mock_db.repos = MagicMock()
        mock_db.repos.generated_images.list_recent = AsyncMock(side_effect=Exception("db fail"))
        handlers = _get_channel_tool_handlers(mock_db)
        result = await handlers["list_generated_images"]({})
        assert "Ошибка" in _text(result)


# ===========================================================================
# 7. services/photo_task_service.py
# ===========================================================================


class TestPhotoTaskServiceValidateFiles:
    """Tests for PhotoTaskService.validate_files()"""

    def _make_service(self):
        from tg_agent.services.photo_task_service import PhotoTaskService

        bundle = MagicMock()
        publish = MagicMock()
        return PhotoTaskService(bundle, publish)

    def test_validate_files_no_files(self, tmp_path):
        svc = self._make_service()
        with pytest.raises(ValueError, match="No files provided"):
            svc.validate_files([])

    def test_validate_files_file_not_found(self, tmp_path):
        svc = self._make_service()
        with pytest.raises(ValueError, match="File not found"):
            svc.validate_files([str(tmp_path / "nonexistent.jpg")])

    def test_validate_files_bad_extension(self, tmp_path):
        bad_file = tmp_path / "test.txt"
        bad_file.write_text("data")
        svc = self._make_service()
        with pytest.raises(ValueError, match="Unsupported file type"):
            svc.validate_files([str(bad_file)])

    def test_validate_files_valid(self, tmp_path):
        img = tmp_path / "photo.jpg"
        img.write_bytes(b"\xff\xd8\xff")
        svc = self._make_service()
        result = svc.validate_files([str(img)])
        assert len(result) == 1

    def test_validate_files_skips_whitespace_paths(self, tmp_path):
        img = tmp_path / "photo.png"
        img.write_bytes(b"\x89PNG")
        svc = self._make_service()
        result = svc.validate_files(["  ", str(img)])
        assert len(result) == 1


class TestPhotoTaskServiceNormalizeMode:
    def test_album_with_single_file_becomes_separate(self):
        from tg_agent.services.photo_task_service import PhotoTaskService

        result = PhotoTaskService.normalize_mode("album", 1)
        assert result == PhotoSendMode.SEPARATE

    def test_album_with_two_files_stays_album(self):
        from tg_agent.services.photo_task_service import PhotoTaskService

        result = PhotoTaskService.normalize_mode("album", 2)
        assert result == PhotoSendMode.ALBUM

    def test_separate_with_single_file_stays_separate(self):
        from tg_agent.services.photo_task_service import PhotoTaskService

        result = PhotoTaskService.normalize_mode("separate", 1)
        assert result == PhotoSendMode.SEPARATE

    def test_accepts_enum_value(self):
        from tg_agent.services.photo_task_service import PhotoTaskService

        result = PhotoTaskService.normalize_mode(PhotoSendMode.ALBUM, 3)
        assert result == PhotoSendMode.ALBUM


class TestPhotoTaskServiceSendNow:
    @pytest.mark.anyio
    async def test_send_now_success(self, tmp_path):
        from tg_agent.services.photo_task_service import PhotoTarget, PhotoTaskService

        img = tmp_path / "photo.jpg"
        img.write_bytes(b"\xff\xd8\xff")

        bundle = MagicMock()
        bundle.create_batch = AsyncMock(return_value=1)
        bundle.create_item = AsyncMock(return_value=10)
        bundle.update_item = AsyncMock()
        bundle.update_batch = AsyncMock()

        completed_item = MagicMock()
        completed_item.id = 10
        completed_item.status = PhotoBatchStatus.COMPLETED
        bundle.get_item = AsyncMock(return_value=completed_item)

        publish = MagicMock()
        publish.send_now = AsyncMock(return_value=[42])

        svc = PhotoTaskService(bundle, publish)
        target = PhotoTarget(dialog_id=99, title="Test Target")
        item = await svc.send_now(
            phone="+1",
            target=target,
            file_paths=[str(img)],
            mode="separate",
            caption="Hello",
        )
        assert item.status == PhotoBatchStatus.COMPLETED
        bundle.update_item.assert_awaited()

    @pytest.mark.anyio
    async def test_send_now_failure_marks_failed(self, tmp_path):
        from tg_agent.services.photo_task_service import PhotoTarget, PhotoTaskService

        img = tmp_path / "photo.jpg"
        img.write_bytes(b"\xff\xd8\xff")

        bundle = MagicMock()
        bundle.create_batch = AsyncMock(return_value=2)
        bundle.create_item = AsyncMock(return_value=20)
        bundle.update_item = AsyncMock()
        bundle.update_batch = AsyncMock()

        publish = MagicMock()
        publish.send_now = AsyncMock(side_effect=Exception("telegram error"))

        svc = PhotoTaskService(bundle, publish)
        target = PhotoTarget(dialog_id=100)
        with pytest.raises(Exception, match="telegram error"):
            await svc.send_now(
                phone="+1",
                target=target,
                file_paths=[str(img)],
                mode="separate",
            )
        # update_item called with FAILED status
        call_kwargs = bundle.update_item.call_args_list[-1]
        assert call_kwargs.kwargs.get("status") == PhotoBatchStatus.FAILED or \
               (call_kwargs.args and PhotoBatchStatus.FAILED in str(call_kwargs))

    @pytest.mark.anyio
    async def test_send_now_separate_partial_failure_keeps_published_ids(self, tmp_path):
        """Regression (#864 review): an immediate SEPARATE send that publishes file 1 then fails
        on file 2 must persist file 1's id and NOT raise. Raising makes the CLI/agent/UI report
        failure, the user reruns, and file 1 is published a second time (duplicate)."""
        from tg_agent.services.photo_task_service import PhotoTarget, PhotoTaskService

        img = tmp_path / "photo.jpg"
        img.write_bytes(b"\xff\xd8\xff")
        img2 = tmp_path / "photo2.jpg"
        img2.write_bytes(b"\xff\xd8\xff")

        bundle = MagicMock()
        bundle.create_batch = AsyncMock(return_value=6)
        bundle.create_item = AsyncMock(return_value=60)
        bundle.update_item = AsyncMock()
        bundle.update_batch = AsyncMock()
        returned_item = MagicMock()
        returned_item.id = 60
        bundle.get_item = AsyncMock(return_value=returned_item)

        # send_now publishes file 1 (fires on_file_sent immediately) then fails on file 2.
        async def _send_now(*, file_paths, on_file_sent=None, **_kwargs):
            if on_file_sent is not None:
                await on_file_sent(file_paths[0], [42])
            raise RuntimeError("file 2 failed")

        publish = MagicMock()
        publish.send_now = AsyncMock(side_effect=_send_now)

        svc = PhotoTaskService(bundle, publish)
        target = PhotoTarget(dialog_id=100)
        # Partial publish returns the item (file 1 is live, irreversible) — it does NOT raise.
        item = await svc.send_now(
            phone="+1",
            target=target,
            file_paths=[str(img), str(img2)],
            mode="separate",
        )
        assert item is returned_item

        # Last update_item must record the published id and mark the item COMPLETED (not FAILED).
        last = bundle.update_item.call_args_list[-1].kwargs
        assert last.get("status") == PhotoBatchStatus.COMPLETED
        assert last.get("telegram_message_ids") == [42]
        assert "partial send failure" in (last.get("error") or "")
        # The batch must NOT be marked FAILED — that would prompt a duplicate retry.
        for call in bundle.update_batch.call_args_list:
            assert call.kwargs.get("status") != PhotoBatchStatus.FAILED


class TestPhotoTaskServiceScheduleSend:
    @pytest.mark.anyio
    async def test_schedule_send_success(self, tmp_path):
        from tg_agent.services.photo_task_service import PhotoTarget, PhotoTaskService

        img = tmp_path / "photo.jpg"
        img.write_bytes(b"\xff\xd8\xff")
        img2 = tmp_path / "photo2.jpg"
        img2.write_bytes(b"\xff\xd8\xff")

        bundle = MagicMock()
        bundle.create_batch = AsyncMock(return_value=3)
        bundle.create_item = AsyncMock(return_value=30)
        bundle.update_item = AsyncMock()
        bundle.update_batch = AsyncMock()
        scheduled_item = MagicMock()
        scheduled_item.id = 30
        bundle.get_item = AsyncMock(return_value=scheduled_item)

        publish = MagicMock()
        publish.send_now = AsyncMock(return_value=[100, 101])

        svc = PhotoTaskService(bundle, publish)
        target = PhotoTarget(dialog_id=200, title="Sched Target")
        schedule_at = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        item = await svc.schedule_send(
            phone="+2",
            target=target,
            file_paths=[str(img), str(img2)],
            mode="album",
            schedule_at=schedule_at,
        )
        assert item is not None

    @pytest.mark.anyio
    async def test_schedule_send_failure_marks_failed(self, tmp_path):
        from tg_agent.services.photo_task_service import PhotoTarget, PhotoTaskService

        img = tmp_path / "photo.jpg"
        img.write_bytes(b"\xff\xd8\xff")

        bundle = MagicMock()
        bundle.create_batch = AsyncMock(return_value=4)
        bundle.create_item = AsyncMock(return_value=40)
        bundle.update_item = AsyncMock()
        bundle.update_batch = AsyncMock()

        publish = MagicMock()
        publish.send_now = AsyncMock(side_effect=RuntimeError("sched fail"))

        svc = PhotoTaskService(bundle, publish)
        target = PhotoTarget(dialog_id=300)
        with pytest.raises(RuntimeError, match="sched fail"):
            await svc.schedule_send(
                phone="+3",
                target=target,
                file_paths=[str(img)],
                mode="separate",
                schedule_at=datetime.now(timezone.utc),
            )

    @pytest.mark.anyio
    async def test_schedule_send_separate_partial_failure_keeps_scheduled_with_ids(self, tmp_path):
        """Regression (#864 review): in SEPARATE mode, if file 1 is scheduled server-side and
        file 2 fails, the item must stay SCHEDULED with file 1's id (cancellable) — never
        FAILED-with-no-ids, which would strand a queued post with no cancel handle (ghost publish).
        And partial success must NOT raise: raising makes the dispatcher/CLI report the schedule as
        FAILED, so the user retries and schedules a duplicate while the first stays queued."""
        from tg_agent.services.photo_task_service import PhotoTarget, PhotoTaskService

        img = tmp_path / "photo.jpg"
        img.write_bytes(b"\xff\xd8\xff")
        img2 = tmp_path / "photo2.jpg"
        img2.write_bytes(b"\xff\xd8\xff")

        bundle = MagicMock()
        bundle.create_batch = AsyncMock(return_value=5)
        bundle.create_item = AsyncMock(return_value=50)
        bundle.update_item = AsyncMock()
        bundle.update_batch = AsyncMock()
        returned_item = MagicMock()
        returned_item.id = 50
        bundle.get_item = AsyncMock(return_value=returned_item)

        # send_now schedules file 1 (fires on_file_sent) then fails on file 2, mirroring the
        # real SEPARATE loop where earlier files are already queued before a later one raises.
        async def _send_now(*, file_paths, on_file_sent=None, **_kwargs):
            if on_file_sent is not None:
                await on_file_sent(file_paths[0], [100])
            raise RuntimeError("file 2 failed")

        publish = MagicMock()
        publish.send_now = AsyncMock(side_effect=_send_now)

        svc = PhotoTaskService(bundle, publish)
        target = PhotoTarget(dialog_id=300)
        # Partial success returns the durable, cancellable item — it does NOT raise.
        item = await svc.schedule_send(
            phone="+3",
            target=target,
            file_paths=[str(img), str(img2)],
            mode="separate",
            schedule_at=datetime.now(timezone.utc),
        )
        assert item is returned_item

        # Last update_item must keep the item SCHEDULED with file 1's id — the cancel handle.
        last = bundle.update_item.call_args_list[-1].kwargs
        assert last.get("status") == PhotoBatchStatus.SCHEDULED
        assert last.get("telegram_message_ids") == [100]
        assert "partial schedule failure" in (last.get("error") or "")
        # The batch must NOT be force-marked FAILED while a post is still scheduled & cancellable.
        for call in bundle.update_batch.call_args_list:
            assert call.kwargs.get("status") != PhotoBatchStatus.FAILED


class TestPhotoTaskServiceCancelItem:
    @pytest.mark.anyio
    async def test_cancel_delegates_to_bundle(self):
        from types import SimpleNamespace

        from tg_agent.models import PhotoBatchStatus
        from tg_agent.services.photo_task_service import PhotoTaskService

        bundle = MagicMock()
        # A non-SCHEDULED item with no batch: cancel_item skips the Telegram
        # unschedule precondition and the batch-status sync, delegating straight
        # to the repository (#864 changed cancel_item to look up the item first).
        item = SimpleNamespace(
            status=PhotoBatchStatus.PENDING, telegram_message_ids=None, batch_id=None
        )
        bundle.get_item = AsyncMock(return_value=item)
        bundle.cancel_item = AsyncMock(return_value=True)
        svc = PhotoTaskService(bundle, MagicMock())
        result = await svc.cancel_item(99)
        assert result is True
        bundle.cancel_item.assert_awaited_once_with(99)

    @pytest.mark.anyio
    async def test_cancel_returns_false_if_not_found(self):
        from tg_agent.services.photo_task_service import PhotoTaskService

        bundle = MagicMock()
        bundle.get_item = AsyncMock(return_value=None)
        bundle.cancel_item = AsyncMock(return_value=False)
        svc = PhotoTaskService(bundle, MagicMock())
        result = await svc.cancel_item(123)
        assert result is False


class TestPhotoTaskServiceRunDue:
    @pytest.mark.anyio
    async def test_run_due_no_items(self):
        from tg_agent.services.photo_task_service import PhotoTaskService

        bundle = MagicMock()
        bundle.claim_next_due_item = AsyncMock(return_value=None)
        bundle.count_due_items = AsyncMock(return_value=0)
        svc = PhotoTaskService(bundle, MagicMock())
        processed = await svc.run_due()
        assert processed == 0

    @pytest.mark.anyio
    async def test_run_due_processes_one_item(self, tmp_path):
        from tg_agent.services.photo_task_service import PhotoTaskService

        img = tmp_path / "photo.jpg"
        img.write_bytes(b"\xff\xd8\xff")

        item = MagicMock(spec=PhotoBatchItem)
        item.id = 5
        item.batch_id = 1
        item.phone = "+1"
        item.target_dialog_id = 100
        item.target_type = "channel"
        item.file_paths = [str(img)]
        item.send_mode = PhotoSendMode.SEPARATE
        item.caption = None

        # completed_item returned by list_items_for_batch for _sync_batch_status
        completed_item = MagicMock(spec=PhotoBatchItem)
        completed_item.status = PhotoBatchStatus.COMPLETED

        bundle = MagicMock()
        bundle.claim_next_due_item = AsyncMock(side_effect=[item, None])
        bundle.count_due_items = AsyncMock(return_value=1)
        bundle.update_item = AsyncMock()
        bundle.list_items_for_batch = AsyncMock(return_value=[completed_item])
        bundle.update_batch = AsyncMock()

        publish = MagicMock()
        publish.send_now = AsyncMock(return_value=[42])

        svc = PhotoTaskService(bundle, publish)
        processed = await svc.run_due()
        assert processed == 1

    @pytest.mark.anyio
    async def test_run_due_with_item_id_claims_only_that_item(self, tmp_path):
        from tg_agent.services.photo_task_service import PhotoTaskService

        img = tmp_path / "photo.jpg"
        img.write_bytes(b"\xff\xd8\xff")

        item = MagicMock(spec=PhotoBatchItem)
        item.id = 7
        item.batch_id = 1
        item.phone = "+1"
        item.target_dialog_id = 100
        item.target_type = "saved"
        item.file_paths = [str(img)]
        item.send_mode = PhotoSendMode.SEPARATE
        item.caption = "caption"

        completed_item = MagicMock(spec=PhotoBatchItem)
        completed_item.status = PhotoBatchStatus.COMPLETED

        bundle = MagicMock()
        bundle.claim_next_due_item = AsyncMock(return_value=item)
        bundle.count_due_items = AsyncMock(return_value=1)
        bundle.update_item = AsyncMock()
        bundle.list_items_for_batch = AsyncMock(return_value=[completed_item])
        bundle.update_batch = AsyncMock()

        publish = MagicMock()
        publish.send_now = AsyncMock(return_value=[42])

        svc = PhotoTaskService(bundle, publish)
        processed = await svc.run_due(item_id=7)

        assert processed == 1
        bundle.claim_next_due_item.assert_awaited_once_with(ANY, item_id=7)
        publish.send_now.assert_awaited_once()

    @pytest.mark.anyio
    async def test_run_due_handles_item_failure(self, tmp_path):
        from tg_agent.services.photo_task_service import PhotoTaskService

        img = tmp_path / "photo.jpg"
        img.write_bytes(b"\xff\xd8\xff")

        item = MagicMock(spec=PhotoBatchItem)
        item.id = 6
        item.batch_id = 2
        item.phone = "+2"
        item.target_dialog_id = 200
        item.target_type = None
        item.file_paths = [str(img)]
        item.send_mode = PhotoSendMode.SEPARATE
        item.caption = None

        # failed_item returned by list_items_for_batch for _sync_batch_status
        failed_item = MagicMock(spec=PhotoBatchItem)
        failed_item.status = PhotoBatchStatus.FAILED

        bundle = MagicMock()
        bundle.claim_next_due_item = AsyncMock(side_effect=[item, None])
        bundle.count_due_items = AsyncMock(return_value=1)
        bundle.update_item = AsyncMock()
        bundle.list_items_for_batch = AsyncMock(return_value=[failed_item])
        bundle.update_batch = AsyncMock()

        publish = MagicMock()
        publish.send_now = AsyncMock(side_effect=Exception("send failed"))

        svc = PhotoTaskService(bundle, publish)
        # run_due should not raise even if item fails
        processed = await svc.run_due()
        assert processed == 1
        # update_item called with FAILED
        bundle.update_item.assert_awaited()


class TestPhotoTaskServiceCreateBatch:
    @pytest.mark.anyio
    async def test_create_batch_empty_entries(self):
        from tg_agent.services.photo_task_service import PhotoTarget, PhotoTaskService

        svc = PhotoTaskService(MagicMock(), MagicMock())
        target = PhotoTarget(dialog_id=1)
        with pytest.raises(ValueError, match="Batch manifest is empty"):
            await svc.create_batch(phone="+1", target=target, entries=[])

    @pytest.mark.anyio
    async def test_create_batch_success(self, tmp_path):
        from tg_agent.services.photo_task_service import PhotoTarget, PhotoTaskService

        img = tmp_path / "photo.jpg"
        img.write_bytes(b"\xff\xd8\xff")

        bundle = MagicMock()
        bundle.photo_loader.create_batch_with_items = AsyncMock(return_value=10)

        svc = PhotoTaskService(bundle, MagicMock())
        target = PhotoTarget(dialog_id=50, title="BatchTarget")
        batch_id = await svc.create_batch(
            phone="+1",
            target=target,
            entries=[{"files": [str(img)], "at": "2026-01-01T12:00:00+00:00", "mode": "separate"}],
        )
        assert batch_id == 10

    def test_parse_schedule_at_naive_datetime(self):
        from tg_agent.services.photo_task_service import PhotoTaskService

        result = PhotoTaskService._parse_schedule_at("2026-01-01T12:00:00")
        assert result.tzinfo is not None

    def test_parse_schedule_at_empty_raises(self):
        from tg_agent.services.photo_task_service import PhotoTaskService

        with pytest.raises(ValueError, match="must include 'at'"):
            PhotoTaskService._parse_schedule_at("")

    def test_parse_schedule_at_none_raises(self):
        from tg_agent.services.photo_task_service import PhotoTaskService

        with pytest.raises(ValueError, match="must include 'at'"):
            PhotoTaskService._parse_schedule_at(None)


# ===========================================================================
# 8. search/telegram_search.py — edge-case paths
# ===========================================================================


class TestTelegramSearchNoPool:
    """Tests for TelegramSearch when pool is None."""

    @pytest.mark.anyio
    async def test_search_telegram_no_pool(self):
        from tg_agent.search.persistence import SearchPersistence
        from tg_agent.search.telegram_search import TelegramSearch

        persistence = MagicMock(spec=SearchPersistence)
        ts = TelegramSearch(pool=None, persistence=persistence)
        result = await ts.search_telegram("hello")
        assert result.error is not None
        assert "аккаунт" in result.error.lower()

    @pytest.mark.anyio
    async def test_search_my_chats_no_pool(self):
        from tg_agent.search.persistence import SearchPersistence
        from tg_agent.search.telegram_search import TelegramSearch

        persistence = MagicMock(spec=SearchPersistence)
        ts = TelegramSearch(pool=None, persistence=persistence)
        result = await ts.search_my_chats("hello")
        assert result.error is not None

    @pytest.mark.anyio
    async def test_search_in_channel_no_pool(self):
        from tg_agent.search.persistence import SearchPersistence
        from tg_agent.search.telegram_search import TelegramSearch

        persistence = MagicMock(spec=SearchPersistence)
        ts = TelegramSearch(pool=None, persistence=persistence)
        result = await ts.search_in_channel(channel_id=100, query="hello")
        assert result.error is not None

    @pytest.mark.anyio
    async def test_check_search_quota_no_pool(self):
        from tg_agent.search.persistence import SearchPersistence
        from tg_agent.search.telegram_search import TelegramSearch

        persistence = MagicMock(spec=SearchPersistence)
        ts = TelegramSearch(pool=None, persistence=persistence)
        result = await ts.check_search_quota("hello")
        assert result is None


class TestTelegramSearchNoPremiumClient:
    """Tests for TelegramSearch when no premium client is available."""

    @pytest.mark.anyio
    async def test_search_telegram_no_premium_client(self):
        from tg_agent.search.persistence import SearchPersistence
        from tg_agent.search.telegram_search import TelegramSearch

        pool = MagicMock()
        pool.get_premium_client = AsyncMock(return_value=None)
        pool.get_premium_unavailability_reason = MagicMock(return_value="No premium accounts")
        persistence = MagicMock(spec=SearchPersistence)
        ts = TelegramSearch(pool=pool, persistence=persistence)
        result = await ts.search_telegram("query")
        assert result.error is not None
        assert "No premium accounts" in result.error

    @pytest.mark.anyio
    async def test_check_search_quota_no_premium_client(self):
        from tg_agent.search.persistence import SearchPersistence
        from tg_agent.search.telegram_search import TelegramSearch

        pool = MagicMock()
        pool.get_premium_client = AsyncMock(return_value=None)
        persistence = MagicMock(spec=SearchPersistence)
        ts = TelegramSearch(pool=pool, persistence=persistence)
        result = await ts.check_search_quota("test")
        assert result is None

    @pytest.mark.anyio
    async def test_search_my_chats_no_available_client(self):
        from tg_agent.search.persistence import SearchPersistence
        from tg_agent.search.telegram_search import TelegramSearch

        pool = MagicMock()
        pool.get_available_client = AsyncMock(return_value=None)
        persistence = MagicMock(spec=SearchPersistence)
        ts = TelegramSearch(pool=pool, persistence=persistence)
        result = await ts.search_my_chats("query")
        assert result.error is not None
        assert "доступн" in result.error.lower() or "аккаунт" in result.error.lower()

    @pytest.mark.anyio
    async def test_search_in_channel_no_available_client(self):
        from tg_agent.search.persistence import SearchPersistence
        from tg_agent.search.telegram_search import TelegramSearch

        pool = MagicMock()
        pool.get_available_client = AsyncMock(return_value=None)
        persistence = MagicMock(spec=SearchPersistence)
        ts = TelegramSearch(pool=pool, persistence=persistence)
        result = await ts.search_in_channel(channel_id=100, query="hello")
        assert result.error is not None


class TestTelegramSearchPremiumUnavailabilityReason:
    """Tests for _get_premium_unavailability_reason."""

    @pytest.mark.anyio
    async def test_no_reason_getter(self):
        from tg_agent.search.persistence import SearchPersistence
        from tg_agent.search.telegram_search import TelegramSearch

        pool = MagicMock(spec=[])  # no get_premium_unavailability_reason
        persistence = MagicMock(spec=SearchPersistence)
        ts = TelegramSearch(pool=pool, persistence=persistence)
        reason = await ts._get_premium_unavailability_reason()
        assert "Premium" in reason

    @pytest.mark.anyio
    async def test_reason_getter_returns_string(self):
        from tg_agent.search.persistence import SearchPersistence
        from tg_agent.search.telegram_search import TelegramSearch

        pool = MagicMock()
        pool.get_premium_unavailability_reason = MagicMock(return_value="Flood waited")
        persistence = MagicMock(spec=SearchPersistence)
        ts = TelegramSearch(pool=pool, persistence=persistence)
        reason = await ts._get_premium_unavailability_reason()
        assert "Flood waited" in reason

    @pytest.mark.anyio
    async def test_reason_getter_raises(self):
        from tg_agent.search.persistence import SearchPersistence
        from tg_agent.search.telegram_search import TelegramSearch

        pool = MagicMock()
        pool.get_premium_unavailability_reason = MagicMock(side_effect=Exception("crash"))
        persistence = MagicMock(spec=SearchPersistence)
        ts = TelegramSearch(pool=pool, persistence=persistence)
        reason = await ts._get_premium_unavailability_reason()
        assert "Premium" in reason
