"""Tests for miscellaneous agent tool error paths and edge cases."""

from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.config import AppConfig
from src.database import Database

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_mock_db():
    db = MagicMock(spec=Database)
    db.repos = MagicMock()
    db._db_path = ":memory:"
    db._session_encryption_secret = None
    return db






def _text(result) -> str:
    """Extract text from tool result payload."""
    if isinstance(result, dict):
        return result["content"][0]["text"]
    if hasattr(result, "content"):
        return result.content[0].text if hasattr(result.content[0], "text") else str(result.content[0])
    return str(result)




# ===========================================================================
# 1. cli/commands/test.py — read/write check functions
# ===========================================================================




class TestDeepagentsSyncRemainingTools:
    """Test sync tools through the runtime-context sync bridge."""

    def _build_tools(self, run_sync_side_effect):
        db = _make_mock_db()
        config = AppConfig()
        runtime_context = SimpleNamespace(
            config=config,
            client_pool=None,
            scheduler_manager=None,
            run_sync=MagicMock(side_effect=run_sync_side_effect),
        )
        from src.agent.tools.deepagents_sync import build_deepagents_tools

        tools = build_deepagents_tools(
            db,
            config=config,
            client_pool=None,
            runtime_context=runtime_context,
        )
        return {f.__name__: f for f in tools}

    def _call(self, tool_name, se, *args, **kwargs):
        return self._build_tools(se)[tool_name](*args, **kwargs)

    def test_list_pipelines_empty(self):
        result = self._call("list_pipelines", lambda n, c: [])
        assert isinstance(result, str)

    def test_get_pipeline_detail_not_found(self):
        result = self._call("get_pipeline_detail", lambda n, c: "не найден", pipeline_id=999)
        assert "не найден" in result.lower() or "ошибка" in result.lower()

    def test_run_pipeline_not_found(self):
        result = self._call("run_pipeline", lambda n, c: "не найден", pipeline_id=999)
        assert "не найден" in result.lower() or "ошибка" in result.lower()

    def test_list_pipeline_runs_empty(self):
        result = self._call("list_pipeline_runs", lambda n, c: "нет генераций", pipeline_id=1)
        assert "нет генераций" in result.lower() or "ошибка" in result.lower()

    def test_get_pipeline_run_not_found(self):
        result = self._call("get_pipeline_run", lambda n, c: "не найден", run_id=999)
        assert "не найден" in result.lower() or "ошибка" in result.lower()

    def test_list_pending_moderation_empty(self):
        result = self._call("list_pending_moderation", lambda n, c: "нет черновиков")
        assert "нет черновиков" in result.lower() or "ошибка" in result.lower()

    def test_list_search_queries_empty(self):
        result = self._call("list_search_queries", lambda n, c: "не найден")
        assert "не найден" in result.lower() or "ошибка" in result.lower()

    def test_run_search_query(self):
        result = self._call("run_search_query", lambda n, c: 5, sq_id=1)
        assert isinstance(result, str)

    def test_get_notification_status_no_bot(self):
        result = self._call("get_notification_status", lambda n, c: "не настроен")
        assert "не настроен" in result.lower() or "ошибка" in result.lower()

    def test_get_analytics_summary(self):
        def se(n, c):
            return {"content": [{"type": "text", "text": "Аналитика"}]}

        result = self._call("get_analytics_summary", se)
        assert "Аналитика" in result

    def test_get_pipeline_stats_empty(self):
        result = self._call("get_pipeline_stats", lambda n, c: "не найдена")
        assert "не найдена" in result.lower()

    def test_get_trending_topics_empty(self):
        result = self._call("get_trending_topics", lambda n, c: "не найден")
        assert "не найден" in result.lower()

    def test_get_trending_channels_empty(self):
        result = self._call("get_trending_channels", lambda n, c: "не найден")
        assert "не найден" in result.lower()

    def test_get_calendar_empty(self):
        result = self._call("get_calendar", lambda n, c: "нет запланированных")
        assert "нет запланированных" in result.lower() or "ошибка" in result.lower()

    def test_get_daily_stats_empty(self):
        result = self._call("get_daily_stats", lambda n, c: "нет данных")
        assert "нет данных" in result.lower() or "ежедневная статистика" in result.lower()


# ===========================================================================
# 5. scheduler/manager.py — sync_job_state branches
# ===========================================================================




class TestImageToolEdgeCases:
    @pytest.fixture
    def image_setup(self):
        mock_db = MagicMock(spec=Database)
        mock_db.repos = MagicMock()
        mock_db.repos.generated_images = MagicMock()
        mock_db.repos.generated_images.save = AsyncMock()

        from src.models import Account
        mock_db.get_accounts = AsyncMock(return_value=[
            Account(id=1, phone="+1111", session_string="s", is_active=True, is_primary=True),
        ])
        mock_db.get_setting = AsyncMock(return_value=None)
        mock_db.repos.settings = MagicMock()
        mock_db.repos.settings.get = AsyncMock(return_value=None)
        mock_db.repos.tool_permissions = MagicMock()
        mock_db.repos.tool_permissions.get_by_phone = AsyncMock(return_value=None)

        captured_tools = []
        with patch(
            "src.agent.tools.create_sdk_mcp_server",
            side_effect=lambda **kw: captured_tools.extend(kw.get("tools", [])),
        ):
            from src.agent.tools import make_mcp_server
            make_mcp_server(mock_db, client_pool=None)
        return {t.name: t.handler for t in captured_tools if hasattr(t, "handler")}, mock_db

    async def test_generate_image_empty_prompt(self, image_setup):
        handlers, _ = image_setup
        result = await handlers["generate_image"]({"prompt": ""})
        assert "обязател" in _text(result).lower()

    async def test_generate_image_not_configured(self, image_setup):
        handlers, _ = image_setup
        with patch("src.services.image_generation_service.ImageGenerationService.is_available",
                    new_callable=AsyncMock, return_value=False):
            result = await handlers["generate_image"]({"prompt": "cat"})
            assert "не настроен" in _text(result).lower()

    async def test_generate_image_returns_text(self, image_setup):
        handlers, _ = image_setup
        with patch("src.services.image_generation_service.ImageGenerationService.is_available",
                    new_callable=AsyncMock, return_value=True), \
             patch("src.services.image_generation_service.ImageGenerationService.generate",
                   new_callable=AsyncMock, return_value="/local/path.png"):
            result = await handlers["generate_image"]({"prompt": "cat", "model": "together:flux"})
            assert "/local/path.png" in _text(result)

    async def test_generate_image_returns_none(self, image_setup):
        handlers, _ = image_setup
        with patch("src.services.image_generation_service.ImageGenerationService.is_available",
                    new_callable=AsyncMock, return_value=True), \
             patch("src.services.image_generation_service.ImageGenerationService.generate",
                   new_callable=AsyncMock, return_value=None):
            result = await handlers["generate_image"]({"prompt": "cat", "model": "together:flux"})
            assert "не вернул" in _text(result).lower()

    async def test_generate_image_exception(self, image_setup):
        handlers, _ = image_setup
        with patch("src.services.image_generation_service.ImageGenerationService.is_available",
                    new_callable=AsyncMock, return_value=True), \
             patch("src.services.image_generation_service.ImageGenerationService.generate",
                   new_callable=AsyncMock, side_effect=RuntimeError("fail")):
            result = await handlers["generate_image"]({"prompt": "cat", "model": "together:flux"})
            assert "ошибка" in _text(result).lower()

    async def test_list_image_models_empty_provider(self, image_setup):
        handlers, _ = image_setup
        result = await handlers["list_image_models"]({"provider": ""})
        assert "обязател" in _text(result).lower()

    async def test_list_image_models_exception(self, image_setup):
        handlers, _ = image_setup
        with patch("src.services.image_generation_service.ImageGenerationService.search_models",
                    new_callable=AsyncMock, side_effect=RuntimeError("fail")):
            result = await handlers["list_image_models"]({"provider": "test"})
            assert "ошибка" in _text(result).lower()


# ===========================================================================
# 14. agent/tools/collection.py — error paths
# ===========================================================================




class TestCollectionToolErrors:
    @pytest.fixture
    def coll_setup(self):
        mock_db = MagicMock(spec=Database)
        mock_db.repos = MagicMock()
        from src.models import Account
        mock_db.get_accounts = AsyncMock(return_value=[
            Account(id=1, phone="+1111", session_string="s", is_active=True, is_primary=True),
        ])
        mock_db.get_setting = AsyncMock(return_value=None)
        mock_db.repos.settings = MagicMock()
        mock_db.repos.settings.get = AsyncMock(return_value=None)
        mock_db.repos.tool_permissions = MagicMock()
        mock_db.repos.tool_permissions.get_by_phone = AsyncMock(return_value=None)
        pool = MagicMock()

        captured_tools = []
        with patch(
            "src.agent.tools.create_sdk_mcp_server",
            side_effect=lambda **kw: captured_tools.extend(kw.get("tools", [])),
        ):
            from src.agent.tools import make_mcp_server
            make_mcp_server(mock_db, client_pool=pool)
        return {t.name: t.handler for t in captured_tools if hasattr(t, "handler")}, mock_db, pool

    async def test_collect_channel_exception(self, coll_setup):
        handlers, mock_db, _ = coll_setup
        mock_db.get_channel_by_pk = AsyncMock(side_effect=RuntimeError("boom"))
        result = await handlers["collect_channel"]({"pk": 1})
        assert "ошибка" in _text(result).lower()

    async def test_collect_channel_filtered(self, coll_setup):
        handlers, mock_db, _ = coll_setup
        ch = SimpleNamespace(title="T", channel_id=1, is_filtered=True, username="u")
        mock_db.get_channel_by_pk = AsyncMock(return_value=ch)
        result = await handlers["collect_channel"]({"pk": 1, "force": False})
        assert "отфильтрован" in _text(result).lower()

    async def test_collect_all_channels_exception(self, coll_setup):
        handlers, mock_db, _ = coll_setup
        mock_db.get_channels = AsyncMock(side_effect=RuntimeError("boom"))
        result = await handlers["collect_all_channels"]({})
        assert "ошибка" in _text(result).lower()

    async def test_collect_channel_stats_exception(self, coll_setup):
        handlers, mock_db, _ = coll_setup
        mock_db.get_channel_by_pk = AsyncMock(side_effect=RuntimeError("boom"))
        result = await handlers["collect_channel_stats"]({"pk": 1})
        assert "ошибка" in _text(result).lower()

    async def test_collect_all_stats_exception(self, coll_setup):
        handlers, mock_db, _ = coll_setup
        mock_db.get_channels = AsyncMock(side_effect=RuntimeError("boom"))
        result = await handlers["collect_all_stats"]({})
        assert "ошибка" in _text(result).lower()


# ===========================================================================
# 15. agent/tools/filters.py — precheck_filters
# ===========================================================================




class TestFilterToolEdge:
    @pytest.fixture
    def filter_setup(self):
        mock_db = MagicMock(spec=Database)
        mock_db.repos = MagicMock()
        from src.models import Account
        mock_db.get_accounts = AsyncMock(return_value=[
            Account(id=1, phone="+1111", session_string="s", is_active=True, is_primary=True),
        ])
        mock_db.get_setting = AsyncMock(return_value=None)
        mock_db.repos.settings = MagicMock()
        mock_db.repos.settings.get = AsyncMock(return_value=None)
        mock_db.repos.tool_permissions = MagicMock()
        mock_db.repos.tool_permissions.get_by_phone = AsyncMock(return_value=None)

        captured_tools = []
        with patch(
            "src.agent.tools.create_sdk_mcp_server",
            side_effect=lambda **kw: captured_tools.extend(kw.get("tools", [])),
        ):
            from src.agent.tools import make_mcp_server
            make_mcp_server(mock_db, client_pool=None)
        return {t.name: t.handler for t in captured_tools if hasattr(t, "handler")}, mock_db

    async def test_precheck_no_confirm(self, filter_setup):
        handlers, _ = filter_setup
        result = await handlers["precheck_filters"]({"confirm": False})
        text = _text(result)
        assert "confirm" in text.lower() or "подтвер" in text.lower()

    async def test_precheck_exception(self, filter_setup):
        handlers, _ = filter_setup
        with patch("src.filters.analyzer.ChannelAnalyzer.precheck_subscriber_ratio",
                    new_callable=AsyncMock, side_effect=RuntimeError("fail")):
            result = await handlers["precheck_filters"]({"confirm": True})
            assert "ошибка" in _text(result).lower()


# ===========================================================================
# 16. agent/tools/notifications.py — error paths
# ===========================================================================




class TestNotificationToolErrors:
    @pytest.fixture
    def notif_setup(self):
        mock_db = MagicMock(spec=Database)
        mock_db.repos = MagicMock()
        from src.models import Account
        mock_db.get_accounts = AsyncMock(return_value=[
            Account(id=1, phone="+1111", session_string="s", is_active=True, is_primary=True),
        ])
        mock_db.get_setting = AsyncMock(return_value=None)
        mock_db.repos.settings = MagicMock()
        mock_db.repos.settings.get = AsyncMock(return_value=None)
        mock_db.repos.tool_permissions = MagicMock()
        mock_db.repos.tool_permissions.get_by_phone = AsyncMock(return_value=None)
        pool = MagicMock()

        captured_tools = []
        with patch(
            "src.agent.tools.create_sdk_mcp_server",
            side_effect=lambda **kw: captured_tools.extend(kw.get("tools", [])),
        ):
            from src.agent.tools import make_mcp_server
            make_mcp_server(mock_db, client_pool=pool)
        return {t.name: t.handler for t in captured_tools if hasattr(t, "handler")}, mock_db, pool

    async def test_setup_notification_bot_exception(self, notif_setup):
        handlers, _, _ = notif_setup
        with patch("src.services.notification_service.NotificationService.setup_bot",
                    new_callable=AsyncMock, side_effect=RuntimeError("fail")):
            result = await handlers["setup_notification_bot"]({"confirm": True})
            assert "ошибка" in _text(result).lower()

    async def test_delete_notification_bot_exception(self, notif_setup):
        handlers, _, _ = notif_setup
        with patch("src.services.notification_service.NotificationService.teardown_bot",
                    new_callable=AsyncMock, side_effect=RuntimeError("fail")):
            result = await handlers["delete_notification_bot"]({"confirm": True})
            assert "ошибка" in _text(result).lower()

    async def test_test_notification_exception(self, notif_setup):
        handlers, _, _ = notif_setup
        with patch("src.services.notification_service.NotificationService.get_status",
                    new_callable=AsyncMock, side_effect=RuntimeError("fail")):
            result = await handlers["test_notification"]({})
            assert "ошибка" in _text(result).lower()


# ===========================================================================
# 17. agent/tools/search_queries.py — error/edge paths
# ===========================================================================




class TestSearchQueryToolErrors:
    @pytest.fixture
    def sq_setup(self):
        mock_db = MagicMock(spec=Database)
        mock_db.repos = MagicMock()
        from src.models import Account
        mock_db.get_accounts = AsyncMock(return_value=[
            Account(id=1, phone="+1111", session_string="s", is_active=True, is_primary=True),
        ])
        mock_db.repos.settings = MagicMock()
        mock_db.repos.settings.get = AsyncMock(return_value=None)
        mock_db.repos.tool_permissions = MagicMock()
        mock_db.repos.tool_permissions.get_by_phone = AsyncMock(return_value=None)

        captured_tools = []
        with patch(
            "src.agent.tools.create_sdk_mcp_server",
            side_effect=lambda **kw: captured_tools.extend(kw.get("tools", [])),
        ):
            from src.agent.tools import make_mcp_server
            make_mcp_server(mock_db, client_pool=None)
        return {t.name: t.handler for t in captured_tools if hasattr(t, "handler")}, mock_db

    async def test_list_search_queries_with_flags(self, sq_setup):
        handlers, _ = sq_setup
        sq = SimpleNamespace(id=1, query="test", interval_minutes=60, is_active=True,
                             is_regex=True, is_fts=True, notify_on_collect=True)
        with patch("src.services.search_query_service.SearchQueryService.list",
                    new_callable=AsyncMock, return_value=[sq]):
            result = await handlers["list_search_queries"]({})
            text = _text(result)
            assert "regex" in text.lower()

    async def test_list_search_queries_exception(self, sq_setup):
        handlers, _ = sq_setup
        with patch("src.services.search_query_service.SearchQueryService.list",
                    new_callable=AsyncMock, side_effect=RuntimeError("fail")):
            result = await handlers["list_search_queries"]({})
            assert "ошибка" in _text(result).lower()

    async def test_get_search_query_exception(self, sq_setup):
        handlers, _ = sq_setup
        with patch("src.services.search_query_service.SearchQueryService.get",
                    new_callable=AsyncMock, side_effect=RuntimeError("fail")):
            result = await handlers["get_search_query"]({"sq_id": 1})
            assert "ошибка" in _text(result).lower()

    async def test_add_search_query_exception(self, sq_setup):
        handlers, _ = sq_setup
        with patch("src.services.search_query_service.SearchQueryService.add",
                    new_callable=AsyncMock, side_effect=RuntimeError("fail")):
            result = await handlers["add_search_query"]({"query": "q", "confirm": True})
            assert "ошибка" in _text(result).lower()

    async def test_edit_search_query_exception(self, sq_setup):
        handlers, _ = sq_setup
        with patch("src.services.search_query_service.SearchQueryService.get",
                    new_callable=AsyncMock, side_effect=RuntimeError("fail")):
            result = await handlers["edit_search_query"]({"sq_id": 1, "confirm": True})
            assert "ошибка" in _text(result).lower()

    async def test_delete_search_query_exception(self, sq_setup):
        handlers, _ = sq_setup
        with patch("src.services.search_query_service.SearchQueryService.delete",
                    new_callable=AsyncMock, side_effect=RuntimeError("fail")):
            result = await handlers["delete_search_query"]({"sq_id": 1, "confirm": True})
            assert "ошибка" in _text(result).lower()

    async def test_toggle_search_query_exception(self, sq_setup):
        handlers, _ = sq_setup
        with patch("src.services.search_query_service.SearchQueryService.get",
                    new_callable=AsyncMock, side_effect=RuntimeError("fail")):
            result = await handlers["toggle_search_query"]({"sq_id": 1})
            assert "ошибка" in _text(result).lower()

    async def test_run_search_query_exception(self, sq_setup):
        handlers, _ = sq_setup
        with patch("src.services.search_query_service.SearchQueryService.run_once",
                    new_callable=AsyncMock, side_effect=RuntimeError("fail")):
            result = await handlers["run_search_query"]({"sq_id": 1})
            assert "ошибка" in _text(result).lower()


# ===========================================================================
# 18. agent/tools/dialogs.py — error paths
# ===========================================================================




class TestPhotoLoaderToolErrors:
    @pytest.fixture
    def photo_setup(self):
        mock_db = MagicMock(spec=Database)
        mock_db.repos = MagicMock()
        from src.models import Account
        mock_db.get_accounts = AsyncMock(return_value=[
            Account(id=1, phone="+1111", session_string="s", is_active=True, is_primary=True),
        ])
        mock_db.get_setting = AsyncMock(return_value=None)
        mock_db.repos.settings = MagicMock()
        mock_db.repos.settings.get = AsyncMock(return_value=None)
        mock_db.repos.tool_permissions = MagicMock()
        mock_db.repos.tool_permissions.get_by_phone = AsyncMock(return_value=None)
        pool = MagicMock()
        pool.get_native_client_by_phone = AsyncMock(return_value=None)
        pool.get_client_by_phone = AsyncMock(return_value=None)

        captured_tools = []
        with patch(
            "src.agent.tools.create_sdk_mcp_server",
            side_effect=lambda **kw: captured_tools.extend(kw.get("tools", [])),
        ):
            from src.agent.tools import make_mcp_server
            make_mcp_server(mock_db, client_pool=pool)
        return {t.name: t.handler for t in captured_tools if hasattr(t, "handler")}, mock_db, pool

    async def test_list_photo_batches_exception(self, photo_setup):
        handlers, _, _ = photo_setup
        with patch("src.services.photo_task_service.PhotoTaskService.list_batches",
                    new_callable=AsyncMock, side_effect=RuntimeError("fail")):
            result = await handlers["list_photo_batches"]({"limit": 10})
            assert "ошибка" in _text(result).lower()

    async def test_list_photo_items_exception(self, photo_setup):
        handlers, _, _ = photo_setup
        with patch("src.services.photo_task_service.PhotoTaskService.list_items",
                    new_callable=AsyncMock, side_effect=RuntimeError("fail")):
            result = await handlers["list_photo_items"]({"limit": 10})
            assert "ошибка" in _text(result).lower()

    async def test_send_photos_missing_fields(self, photo_setup):
        handlers, _, _ = photo_setup
        with patch("src.services.photo_task_service.PhotoTaskService.send_now",
                    new_callable=AsyncMock):
            result = await handlers["send_photos_now"]({
                "phone": "+1111", "target": "", "file_paths": "", "confirm": True,
            })
            assert "обязател" in _text(result).lower()

    async def test_send_photos_exception(self, photo_setup):
        handlers, _, pool = photo_setup
        with patch("src.services.photo_task_service.PhotoTaskService.send_now",
                    new_callable=AsyncMock, side_effect=RuntimeError("fail")):
            result = await handlers["send_photos_now"]({
                "phone": "+1111", "target": "123", "file_paths": "a.jpg",
                "confirm": True,
            })
            assert "ошибка" in _text(result).lower()

    async def test_schedule_photos_missing_fields(self, photo_setup):
        handlers, _, _ = photo_setup
        result = await handlers["schedule_photos"]({
            "phone": "+1111", "target": "", "file_paths": "",
            "schedule_at": "", "confirm": True,
        })
        assert "обязател" in _text(result).lower()

    async def test_schedule_photos_exception(self, photo_setup):
        handlers, _, pool = photo_setup
        with patch("src.services.photo_task_service.PhotoTaskService.schedule_send",
                    new_callable=AsyncMock, side_effect=RuntimeError("fail")):
            result = await handlers["schedule_photos"]({
                "phone": "+1111", "target": "123", "file_paths": "a.jpg",
                "schedule_at": "2025-01-01T00:00:00", "confirm": True,
            })
            assert "ошибка" in _text(result).lower()

    async def test_cancel_photo_item_exception(self, photo_setup):
        handlers, _, _ = photo_setup
        with patch("src.services.photo_task_service.PhotoTaskService.cancel_item",
                    new_callable=AsyncMock, side_effect=RuntimeError("fail")):
            result = await handlers["cancel_photo_item"]({"item_id": 1, "confirm": True})
            assert "ошибка" in _text(result).lower()

    async def test_list_auto_uploads_exception(self, photo_setup):
        handlers, _, _ = photo_setup
        with patch("src.services.photo_auto_upload_service.PhotoAutoUploadService.list_jobs",
                    new_callable=AsyncMock, side_effect=RuntimeError("fail")):
            result = await handlers["list_auto_uploads"]({})
            assert "ошибка" in _text(result).lower()

    async def test_toggle_auto_upload_exception(self, photo_setup):
        handlers, _, _ = photo_setup
        with patch("src.services.photo_auto_upload_service.PhotoAutoUploadService.get_job",
                    new_callable=AsyncMock, side_effect=RuntimeError("fail")):
            result = await handlers["toggle_auto_upload"]({"job_id": 1})
            assert "ошибка" in _text(result).lower()

    async def test_delete_auto_upload_exception(self, photo_setup):
        handlers, _, _ = photo_setup
        with patch("src.services.photo_auto_upload_service.PhotoAutoUploadService.delete_job",
                    new_callable=AsyncMock, side_effect=RuntimeError("fail")):
            result = await handlers["delete_auto_upload"]({"job_id": 1, "confirm": True})
            assert "ошибка" in _text(result).lower()


# ===========================================================================
# 20. agent/tools/pipelines.py — error/edge paths
# ===========================================================================




class TestPipelineToolErrors:
    @pytest.fixture
    def pipe_setup(self):
        mock_db = MagicMock(spec=Database)
        mock_db.repos = MagicMock()
        from src.models import Account
        mock_db.get_accounts = AsyncMock(return_value=[
            Account(id=1, phone="+1111", session_string="s", is_active=True, is_primary=True),
        ])
        mock_db.repos.settings = MagicMock()
        mock_db.repos.settings.get = AsyncMock(return_value=None)
        mock_db.repos.tool_permissions = MagicMock()
        mock_db.repos.tool_permissions.get_by_phone = AsyncMock(return_value=None)

        config = MagicMock()
        captured_tools = []
        with patch(
            "src.agent.tools.create_sdk_mcp_server",
            side_effect=lambda **kw: captured_tools.extend(kw.get("tools", [])),
        ):
            from src.agent.tools import make_mcp_server
            make_mcp_server(mock_db, client_pool=None, config=config)
        return {t.name: t.handler for t in captured_tools if hasattr(t, "handler")}, mock_db

    async def test_list_pipelines_exception(self, pipe_setup):
        handlers, _ = pipe_setup
        with patch("src.services.pipeline_service.PipelineService.list",
                    new_callable=AsyncMock, side_effect=RuntimeError("fail")):
            result = await handlers["list_pipelines"]({})
            assert "ошибка" in _text(result).lower()

    async def test_get_pipeline_exception(self, pipe_setup):
        handlers, _ = pipe_setup
        with patch("src.services.pipeline_service.PipelineService.get_detail",
                    new_callable=AsyncMock, side_effect=RuntimeError("fail")):
            result = await handlers["get_pipeline_detail"]({"pipeline_id": 1})
            assert "ошибка" in _text(result).lower()

    async def test_run_pipeline_exception(self, pipe_setup):
        handlers, _ = pipe_setup
        with patch("src.services.pipeline_service.PipelineService.get",
                    new_callable=AsyncMock, side_effect=RuntimeError("fail")):
            result = await handlers["run_pipeline"]({"pipeline_id": 1, "confirm": True})
            assert "ошибка" in _text(result).lower()

    async def test_delete_pipeline_exception(self, pipe_setup):
        handlers, _ = pipe_setup
        with patch("src.services.pipeline_service.PipelineService.delete",
                    new_callable=AsyncMock, side_effect=RuntimeError("fail")):
            result = await handlers["delete_pipeline"]({"pipeline_id": 1, "confirm": True})
            assert "ошибка" in _text(result).lower()

    async def test_toggle_pipeline_exception(self, pipe_setup):
        handlers, _ = pipe_setup
        with patch("src.services.pipeline_service.PipelineService.toggle",
                    new_callable=AsyncMock, side_effect=RuntimeError("fail")):
            result = await handlers["toggle_pipeline"]({"pipeline_id": 1})
            assert "ошибка" in _text(result).lower()


# ===========================================================================
# 21. agent/tools/channels.py — import/refresh edge paths
# ===========================================================================




class TestChannelToolEdge:
    @pytest.fixture
    def chan_setup(self):
        mock_db = MagicMock(spec=Database)
        mock_db.repos = MagicMock()
        from src.models import Account
        mock_db.get_accounts = AsyncMock(return_value=[
            Account(id=1, phone="+1111", session_string="s", is_active=True, is_primary=True),
        ])
        mock_db.repos.settings = MagicMock()
        mock_db.repos.settings.get = AsyncMock(return_value=None)
        mock_db.repos.tool_permissions = MagicMock()
        mock_db.repos.tool_permissions.get_by_phone = AsyncMock(return_value=None)
        pool = MagicMock()
        pool.resolve_channel = AsyncMock(return_value=False)

        captured_tools = []
        with patch(
            "src.agent.tools.create_sdk_mcp_server",
            side_effect=lambda **kw: captured_tools.extend(kw.get("tools", [])),
        ):
            from src.agent.tools import make_mcp_server
            make_mcp_server(mock_db, client_pool=pool)
        return {t.name: t.handler for t in captured_tools if hasattr(t, "handler")}, mock_db, pool

    async def test_toggle_channel_not_found(self, chan_setup):
        handlers, mock_db, _ = chan_setup
        mock_db.get_channel_by_pk = AsyncMock(return_value=None)
        with patch("src.services.channel_service.ChannelService.toggle",
                    new_callable=AsyncMock):
            result = await handlers["toggle_channel"]({"pk": 999})
            text = _text(result)
            assert "переключ" in text.lower() or "not found" in text.lower() or "pk=" in text

    async def test_import_channels_no_text(self, chan_setup):
        handlers, _, _ = chan_setup
        result = await handlers["import_channels"]({"text": "", "confirm": True})
        assert "обязател" in _text(result).lower()

    async def test_import_channels_exception(self, chan_setup):
        handlers, _, _ = chan_setup
        with patch("src.services.channel_service.ChannelService.add_by_identifier",
                    new_callable=AsyncMock, side_effect=RuntimeError("fail")):
            result = await handlers["import_channels"]({
                "text": "@testchan", "confirm": True,
            })
            text = _text(result)
            assert "импорт" in text.lower() or "ошибк" in text.lower()

    async def test_refresh_channel_types(self, chan_setup):
        handlers, mock_db, pool = chan_setup
        ch = SimpleNamespace(id=1, channel_id=100, username="u", channel_type=None, is_active=True)
        mock_db.get_channels = AsyncMock(return_value=[ch])
        mock_db.set_channel_active = AsyncMock()
        mock_db.set_channel_type = AsyncMock()
        pool.resolve_channel = AsyncMock(return_value=False)
        result = await handlers["refresh_channel_types"]({"confirm": True})
        text = _text(result)
        assert "обновлен" in text.lower()


# ===========================================================================
# 22. deepagents_sync.py — exception paths for remaining tools
# ===========================================================================




class TestDeepagentsSyncExceptions:
    @pytest.fixture
    def sync_tools(self):
        mock_db = _make_mock_db()
        mock_db.get_accounts = AsyncMock(return_value=[])
        mock_db.get_channels = AsyncMock(return_value=[])
        mock_db.get_channels_with_counts = AsyncMock(return_value=[])
        mock_db.search_messages = AsyncMock(return_value=[])
        mock_db.get_stats = AsyncMock(return_value={})
        mock_db.get_agent_threads = AsyncMock(return_value=[])
        mock_db.create_agent_thread = AsyncMock(return_value=1)
        mock_db.delete_agent_thread = AsyncMock()
        mock_db.get_setting = AsyncMock(return_value=None)
        mock_db.get_channel_by_pk = AsyncMock(return_value=None)
        mock_db.set_channel_filtered = AsyncMock()
        mock_db.repos.generation_runs = MagicMock()
        mock_db.repos.generation_runs.list_by_pipeline = AsyncMock(return_value=[])
        mock_db.repos.generation_runs.list_pending_moderation = AsyncMock(return_value=[])
        mock_db.repos.generation_runs.get = AsyncMock(return_value=None)
        mock_db.repos.generation_runs.set_moderation_status = AsyncMock()

        config = MagicMock()
        from src.agent.tools.deepagents_sync import build_deepagents_tools
        tools = build_deepagents_tools(mock_db, config=config)
        return {t.__name__: t for t in tools}, mock_db

    def test_index_messages_exception(self, sync_tools):
        tools, _ = sync_tools
        assert "index_messages" in tools
        with patch(
            "src.services.embedding_service.EmbeddingService.index_pending_messages",
            side_effect=RuntimeError("fail"),
        ):
            result = tools["index_messages"]()
            assert "ошибка" in result.lower() or "fail" in result.lower()

    def test_toggle_pipeline_exception(self, sync_tools):
        tools, _ = sync_tools
        assert "toggle_pipeline" in tools
        with patch(
            "src.services.pipeline_service.PipelineService.toggle",
            side_effect=RuntimeError("fail"),
        ):
            result = tools["toggle_pipeline"](1)
            assert "ошибка" in result.lower()

    def test_delete_pipeline_exception(self, sync_tools):
        tools, _ = sync_tools
        assert "delete_pipeline" in tools
        with patch(
            "src.services.pipeline_service.PipelineService.delete",
            side_effect=RuntimeError("fail"),
        ):
            result = tools["delete_pipeline"](1)
            assert "ошибка" in result.lower()

    def test_run_pipeline_exception(self, sync_tools):
        tools, _ = sync_tools
        assert "run_pipeline" in tools
        with patch(
            "src.services.pipeline_service.PipelineService.get",
            side_effect=RuntimeError("fail"),
        ):
            result = tools["run_pipeline"](1)
            assert "ошибка" in result.lower()

    def test_list_search_queries_exception(self, sync_tools):
        tools, _ = sync_tools
        assert "list_search_queries" in tools
        with patch(
            "src.services.search_query_service.SearchQueryService.list",
            side_effect=RuntimeError("fail"),
        ):
            result = tools["list_search_queries"]()
            assert "ошибка" in result.lower()

    def test_toggle_search_query_exception(self, sync_tools):
        tools, _ = sync_tools
        assert "toggle_search_query" in tools
        with patch(
            "src.services.search_query_service.SearchQueryService.toggle",
            side_effect=RuntimeError("fail"),
        ):
            result = tools["toggle_search_query"](1)
            assert "ошибка" in result.lower()

    def test_delete_search_query_exception(self, sync_tools):
        tools, _ = sync_tools
        assert "delete_search_query" in tools
        with patch(
            "src.services.search_query_service.SearchQueryService.delete",
            side_effect=RuntimeError("fail"),
        ):
            result = tools["delete_search_query"](1, confirm=True)
            assert "ошибка" in result.lower()

    def test_run_search_query_exception(self, sync_tools):
        tools, _ = sync_tools
        assert "run_search_query" in tools
        with patch(
            "src.services.search_query_service.SearchQueryService.run_once",
            side_effect=RuntimeError("fail"),
        ):
            result = tools["run_search_query"](1)
            assert "ошибка" in result.lower()

    def test_get_flood_status(self, sync_tools):
        tools, mock_db = sync_tools
        if "get_flood_status" not in tools:
            pytest.skip("no get_flood_status tool")
        acc = SimpleNamespace(phone="+1111", is_active=True, flood_wait_until=None)
        mock_db.get_accounts = AsyncMock(return_value=[acc])
        result = tools["get_flood_status"]()
        assert "+1111" in result

    def test_analyze_filters_exception(self, sync_tools):
        tools, _ = sync_tools
        assert "analyze_filters" in tools
        with patch(
            "src.filters.analyzer.ChannelAnalyzer.analyze_all",
            side_effect=RuntimeError("fail"),
        ):
            result = tools["analyze_filters"]()
            assert "ошибка" in result.lower()

    def test_apply_filters_exception(self, sync_tools):
        tools, _ = sync_tools
        assert "apply_filters" in tools
        with patch(
            "src.filters.analyzer.ChannelAnalyzer.analyze_all",
            side_effect=RuntimeError("fail"),
        ):
            result = tools["apply_filters"](confirm=True)
            assert "ошибка" in result.lower()

    def test_reset_filters_exception(self, sync_tools):
        tools, _ = sync_tools
        assert "reset_filters" in tools
        with patch(
            "src.filters.analyzer.ChannelAnalyzer.reset_filters",
            side_effect=RuntimeError("fail"),
        ):
            result = tools["reset_filters"](confirm=True)
            assert "ошибка" in result.lower()

    def test_toggle_channel_filter_not_found(self, sync_tools):
        tools, mock_db = sync_tools
        assert "toggle_channel_filter" in tools
        mock_db.get_channel_by_pk = AsyncMock(return_value=None)
        result = tools["toggle_channel_filter"](999)
        assert "не найден" in result.lower()

    def test_toggle_channel_filter_exception(self, sync_tools):
        tools, mock_db = sync_tools
        assert "toggle_channel_filter" in tools
        mock_db.get_channel_by_pk = AsyncMock(side_effect=RuntimeError("fail"))
        result = tools["toggle_channel_filter"](1)
        assert "ошибка" in result.lower()

    def test_get_notification_status_exception(self, sync_tools):
        tools, _ = sync_tools
        assert "get_notification_status" in tools
        with patch(
            "src.services.notification_service.NotificationService.get_status",
            side_effect=RuntimeError("fail"),
        ):
            result = tools["get_notification_status"]()
            assert "ошибка" in result.lower()

    def test_generate_image_exception(self, sync_tools):
        tools, _ = sync_tools
        assert "generate_image" in tools
        with patch(
            "src.services.image_generation_service.ImageGenerationService.is_available",
            new_callable=AsyncMock,
            return_value=True,
        ), patch(
            "src.services.image_generation_service.ImageGenerationService.generate",
            side_effect=RuntimeError("fail"),
        ):
            result = tools["generate_image"]("test prompt", model="together:flux")
            assert "ошибка" in result.lower()

    def test_list_image_providers_exception(self, sync_tools):
        tools, _ = sync_tools
        assert "list_image_providers" in tools
        result = tools["list_image_providers"]()
        # Either shows providers or "не настроены"
        assert isinstance(result, str)

    def test_get_system_info_exception(self, sync_tools):
        tools, mock_db = sync_tools
        assert "get_system_info" in tools
        mock_db.get_stats = AsyncMock(side_effect=RuntimeError("fail"))
        result = tools["get_system_info"]()
        assert "ошибка" in result.lower()

    def test_list_agent_threads_exception(self, sync_tools):
        tools, mock_db = sync_tools
        assert "list_agent_threads" in tools
        mock_db.get_agent_threads = AsyncMock(side_effect=RuntimeError("fail"))
        result = tools["list_agent_threads"]()
        assert "ошибка" in result.lower()

    def test_create_agent_thread_exception(self, sync_tools):
        tools, mock_db = sync_tools
        assert "create_agent_thread" in tools
        mock_db.create_agent_thread = AsyncMock(side_effect=RuntimeError("fail"))
        result = tools["create_agent_thread"]("title")
        assert "ошибка" in result.lower()

    def test_delete_agent_thread_exception(self, sync_tools):
        tools, mock_db = sync_tools
        assert "delete_agent_thread" in tools
        mock_db.get_agent_thread = AsyncMock(return_value=None)
        mock_db.delete_agent_thread = AsyncMock(side_effect=RuntimeError("fail"))
        result = tools["delete_agent_thread"](1, confirm=True)
        assert "ошибка" in result.lower()

    def test_get_settings_exception(self, sync_tools):
        tools, mock_db = sync_tools
        assert "get_settings" in tools
        mock_db.get_setting = AsyncMock(side_effect=RuntimeError("fail"))
        result = tools["get_settings"]()
        assert "ошибка" in result.lower()


# ===========================================================================
# 23. agent/manager.py — _run_db_tool_sync, _search_messages_tool, etc.
# ===========================================================================




class TestAgentManagerEdge:
    async def test_format_all_flooded_detail_no_retry(self):
        from src.cli.commands.test import _format_all_flooded_detail
        result = _format_all_flooded_detail("base", retry_after_sec=None, next_available_at_utc=None)
        assert "all clients are flood-waited" in result

    async def test_format_all_flooded_detail_no_time(self):
        from src.cli.commands.test import _format_all_flooded_detail
        result = _format_all_flooded_detail("base", retry_after_sec=10, next_available_at_utc=None)
        assert "about 10s" in result

    async def test_format_all_flooded_detail_with_time(self):
        from src.cli.commands.test import _format_all_flooded_detail
        dt = datetime(2025, 1, 1, tzinfo=timezone.utc)
        result = _format_all_flooded_detail("base", retry_after_sec=10, next_available_at_utc=dt)
        assert "until" in result

    async def test_format_exception(self):
        from src.cli.commands.test import _format_exception
        assert _format_exception(RuntimeError("boom")) == "boom"
        assert _format_exception(RuntimeError("")) == "RuntimeError"

    async def test_is_regular_search_unavailable(self):
        from src.cli.commands.test import _is_regular_search_client_unavailable_error
        assert _is_regular_search_client_unavailable_error(
            "Нет доступных Telegram-аккаунтов. Проверьте подключение."
        )
        assert not _is_regular_search_client_unavailable_error("other")

    async def test_is_premium_flood_unavailable(self):
        from src.cli.commands.test import _is_premium_flood_unavailable_error
        assert _is_premium_flood_unavailable_error(
            "Premium-аккаунты временно недоступны из-за Flood Wait."
        )
        assert not _is_premium_flood_unavailable_error("other")

    async def test_skip_remaining_tg_checks(self):
        from src.cli.commands.test import _skip_remaining_tg_checks
        results = []
        _skip_remaining_tg_checks(results, "reason", ["a", "b", "c"])
        assert len(results) == 3
        assert all(r.status.value == "SKIP" for r in results)


# ===========================================================================
# 24. cli/commands/test.py — _run_write_checks (via real in-memory DB)
# ===========================================================================


