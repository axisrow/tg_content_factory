"""Tests for channel CLI, pipeline CLI, settings routes, channel routes, and photo loader paths."""
# Web-panel tests split out of tests/test_cli_channel_pipeline_route_paths.py (#1720 wave 3).


from __future__ import annotations

import base64
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from tg_agent.config import AppConfig
from tg_agent.database import Database
from tg_agent.models import Account, Channel

# ---------------------------------------------------------------------------
# Helpers shared across CLI tests
# ---------------------------------------------------------------------------

NOW_STR = "2025-01-01T12:00:00+00:00"


# ===========================================================================
# 4 & 5. Web route tests — channels and settings
# ===========================================================================

# We replicate the route_client fixture inline since this file is in tests/
# (not tests/routes/) and we want to keep everything in one place.


@pytest.fixture
async def _base_app_b3(tmp_path):
    """Standalone base_app equivalent for batch3 tests."""
    from tg_agent.collection_queue import CollectionQueue
    from tg_agent.scheduler.service import SchedulerManager
    from tg_agent.search.ai_search import AISearchEngine
    from tg_agent.search.engine import SearchEngine
    from tg_agent.telegram.collector import Collector
    from tg_shared.auth import TelegramAuth
    from tg_webui.app import create_app

    config = AppConfig()
    config.database.path = str(tmp_path / "batch3.db")
    config.telegram.api_id = 12345
    config.telegram.api_hash = "test_hash"
    config.web.password = "testpass"

    app = create_app(config)
    db = Database(config.database.path, session_encryption_secret="test-session-encryption-key")
    await db.initialize()
    app.state.db = db
    app.state.config = config

    pool_mock = MagicMock()
    pool_mock.clients = {"+1234567890": MagicMock()}
    pool_mock.resolve_channel = AsyncMock(return_value=None)
    pool_mock.fetch_channel_meta = AsyncMock(return_value=None)
    pool_mock.get_forum_topics = AsyncMock(return_value=[])
    pool_mock.get_dialogs_for_phone = AsyncMock(return_value=[])
    app.state.pool = pool_mock

    app.state.auth = TelegramAuth(12345, "test_hash")
    app.state.notifier = None

    collector = Collector(pool_mock, db, config.scheduler)
    app.state.collector = collector

    collection_queue = CollectionQueue(collector, db)
    app.state.collection_queue = collection_queue

    app.state.search_engine = SearchEngine(db)
    app.state.ai_search = AISearchEngine(config.llm, db)
    app.state.scheduler = SchedulerManager(config.scheduler)
    app.state.session_secret = "test_secret_key"
    app.state.shutting_down = False

    await db.add_account(Account(phone="+1234567890", session_string="test_session"))
    await db.add_channel(Channel(channel_id=100, title="Test Channel"))

    yield app, db, pool_mock

    await collection_queue.shutdown()
    await db.close()


@pytest.fixture
async def _route_client_b3(_base_app_b3):
    from httpx import ASGITransport, AsyncClient

    app, db, pool_mock = _base_app_b3
    transport = ASGITransport(app=app)
    auth_header = base64.b64encode(b":testpass").decode()
    async with AsyncClient(
        transport=transport,
        base_url="http://test",
        follow_redirects=True,
        headers={
            "Authorization": f"Basic {auth_header}",
            "Origin": "http://test",
        },
    ) as c:
        c._transport_app = app
        c._db = db
        c._pool = pool_mock
        yield c


# --- Channel routes ---


@pytest.mark.anyio
async def test_channels_refresh_types_redirect(_route_client_b3):
    """POST /channels/refresh-types enqueues a channels.refresh_types command."""
    client = _route_client_b3
    pool = client._pool
    db = client._db
    resp = await client.post("/channels/refresh-types", follow_redirects=False)
    assert resp.status_code == 303
    assert "command_id=" in resp.headers["location"]
    # web layer must not touch the pool directly anymore
    pool.resolve_channel.assert_not_called()
    commands = await db.repos.telegram_commands.list_commands(limit=1)
    assert len(commands) == 1
    assert commands[0].command_type == "channels.refresh_types"


@pytest.mark.anyio
async def test_channels_refresh_types_enqueues_single_command(_route_client_b3):
    """A second invocation enqueues another command; web path never calls pool."""
    client = _route_client_b3
    pool = client._pool
    db = client._db
    resp = await client.post("/channels/refresh-types", follow_redirects=False)
    assert resp.status_code == 303
    pool.resolve_channel.assert_not_called()
    commands = await db.repos.telegram_commands.list_commands(limit=5)
    assert any(c.command_type == "channels.refresh_types" for c in commands)


# removed: replaced by queued-command model — web no longer runs resolve inline
# removed: replaced by queued-command model — web no longer runs resolve inline


@pytest.mark.anyio
async def test_channels_refresh_meta_success(_route_client_b3):
    """POST /channels/refresh-meta enqueues a channels.refresh_meta command."""
    client = _route_client_b3
    pool = client._pool
    db = client._db
    resp = await client.post("/channels/refresh-meta", follow_redirects=False)
    assert resp.status_code == 303
    assert "command_id=" in resp.headers["location"]
    pool.fetch_channel_meta.assert_not_called()
    commands = await db.repos.telegram_commands.list_commands(limit=1)
    assert len(commands) == 1
    assert commands[0].command_type == "channels.refresh_meta"


# removed: replaced by queued-command model — web no longer fetches meta inline
# removed: replaced by queued-command model — web no longer fetches meta inline


# --- Settings routes ---


@pytest.fixture
def _settings_patch():
    """Patch ProviderConfigService methods to avoid needing config."""
    with patch(
        "tg_webui.settings.handlers.ProviderConfigService.load_provider_configs",
        AsyncMock(return_value=[]),
    ), patch(
        "tg_webui.settings.handlers.ProviderConfigService.load_model_cache",
        AsyncMock(return_value={}),
    ), patch(
        "tg_webui.settings.handlers.ImageProviderService.load_provider_configs",
        AsyncMock(return_value=[]),
    ):
        yield


@pytest.mark.anyio
async def test_settings_page_renders_b3(_route_client_b3, _settings_patch):
    client = _route_client_b3
    resp = await client.get("/settings/")
    assert resp.status_code == 200


@pytest.mark.anyio
async def test_settings_save_scheduler_valid_b3(_route_client_b3):
    client = _route_client_b3
    resp = await client.post(
        "/settings/save-scheduler",
        data={"collect_interval_minutes": "45"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "msg=scheduler_saved" in resp.headers["location"]


@pytest.mark.anyio
async def test_settings_save_scheduler_invalid_b3(_route_client_b3):
    client = _route_client_b3
    resp = await client.post(
        "/settings/save-scheduler",
        data={"collect_interval_minutes": "notanumber"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "error=invalid_value" in resp.headers["location"]


@pytest.mark.anyio
async def test_settings_save_credentials_valid_b3(_route_client_b3):
    client = _route_client_b3
    resp = await client.post(
        "/settings/save-credentials",
        data={"api_id": "12345", "api_hash": "hashvalue123"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "msg=credentials_saved" in resp.headers["location"]


@pytest.mark.anyio
async def test_settings_save_credentials_invalid_api_id_b3(_route_client_b3):
    client = _route_client_b3
    resp = await client.post(
        "/settings/save-credentials",
        data={"api_id": "notanumber", "api_hash": "hash"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "error=invalid_api_id" in resp.headers["location"]


@pytest.mark.anyio
async def test_settings_save_filters_valid_b3(_route_client_b3):
    client = _route_client_b3
    resp = await client.post(
        "/settings/save-filters",
        data={"min_subscribers_filter": "0", "auto_delete_filtered": "0"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "msg=filters_saved" in resp.headers["location"]


@pytest.mark.anyio
async def test_settings_save_filters_invalid_b3(_route_client_b3):
    client = _route_client_b3
    resp = await client.post(
        "/settings/save-filters",
        data={"min_subscribers_filter": "abc"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "error=invalid_value" in resp.headers["location"]


@pytest.mark.anyio
async def test_settings_save_agent_backend_override_b3(_route_client_b3):
    client = _route_client_b3
    resp = await client.post(
        "/settings/save-agent",
        data={"agent_form_scope": "backend_override", "agent_backend_override": "auto"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "msg=agent_saved" in resp.headers["location"]


@pytest.mark.anyio
async def test_settings_save_agent_tool_permissions_b3(_route_client_b3):
    """save-agent with tool_permissions scope saves permissions."""
    client = _route_client_b3
    resp = await client.post(
        "/settings/save-agent",
        data={"agent_form_scope": "tool_permissions", "phone": ""},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "tool_permissions_saved" in resp.headers["location"]


@pytest.mark.anyio
async def test_settings_save_notification_account_empty_b3(_route_client_b3):
    """Saving empty notification phone clears it."""
    client = _route_client_b3
    with patch(
        "tg_webui.settings.handlers.deps.get_notification_target_service"
    ) as mock_svc, patch("tg_webui.settings.handlers.deps.get_notifier") as mock_notifier:
        mock_svc.return_value.set_configured_phone = AsyncMock()
        mock_notifier.return_value = None
        resp = await client.post(
            "/settings/save-notification-account",
            data={"notification_account_phone": ""},
            follow_redirects=False,
        )
    assert resp.status_code == 303
    assert "msg=notification_account_saved" in resp.headers["location"]


@pytest.mark.anyio
async def test_settings_save_semantic_search_valid_b3(_route_client_b3):
    client = _route_client_b3
    resp = await client.post(
        "/settings/save-semantic-search",
        data={
            "semantic_embeddings_provider": "openai",
            "semantic_embeddings_model": "text-embedding-ada-002",
            "semantic_embeddings_batch_size": "50",
        },
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "msg=semantic_saved" in resp.headers["location"]


@pytest.mark.anyio
async def test_settings_save_semantic_search_invalid_batch_b3(_route_client_b3):
    client = _route_client_b3
    resp = await client.post(
        "/settings/save-semantic-search",
        data={
            "semantic_embeddings_provider": "openai",
            "semantic_embeddings_model": "text-embedding-ada-002",
            "semantic_embeddings_batch_size": "notanumber",
        },
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "error=semantic_invalid_value" in resp.headers["location"]


@pytest.mark.anyio
async def test_settings_save_semantic_search_empty_provider_b3(_route_client_b3):
    client = _route_client_b3
    resp = await client.post(
        "/settings/save-semantic-search",
        data={
            "semantic_embeddings_provider": "",
            "semantic_embeddings_model": "model",
            "semantic_embeddings_batch_size": "50",
        },
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "error=semantic_invalid_value" in resp.headers["location"]


# ===========================================================================
# 6. web/photo_loader/handlers.py — build_feedback helper
# ===========================================================================


class TestPhotoBuildFeedback:
    """Unit tests for the build_feedback helper in photo_loader handlers."""

    def _make_item(self, **kwargs):
        from types import SimpleNamespace

        return SimpleNamespace(**kwargs)

    def test_photo_sent_no_items(self):
        from tg_webui.photo_loader.handlers import build_feedback as _build_feedback

        result = _build_feedback("photo_sent", None, batches=[], items=[], auto_jobs=[])
        assert result is not None
        assert result["variant"] == "success"
        assert "Фото успешно отправлены" in result["body"]

    def test_photo_sent_with_target(self):
        from tg_webui.photo_loader.handlers import build_feedback as _build_feedback

        item = self._make_item(target_title="My Channel", target_dialog_id=123)
        result = _build_feedback("photo_sent", None, batches=[], items=[item], auto_jobs=[])
        assert "My Channel" in result["body"]

    def test_photo_scheduled_no_items(self):
        from tg_webui.photo_loader.handlers import build_feedback as _build_feedback

        result = _build_feedback("photo_scheduled", None, batches=[], items=[], auto_jobs=[])
        assert result["variant"] == "success"
        assert "Отложенная отправка создана" in result["body"]

    def test_photo_batch_created_with_batch(self):
        from tg_webui.photo_loader.handlers import build_feedback as _build_feedback

        batch = self._make_item(target_title="Batch Target", target_dialog_id=456)
        result = _build_feedback(
            "photo_batch_created", None, batches=[batch], items=[], auto_jobs=[]
        )
        assert "Batch Target" in result["body"]

    def test_photo_auto_created_with_job(self):
        from tg_webui.photo_loader.handlers import build_feedback as _build_feedback

        job = self._make_item(target_title="Auto Target", target_dialog_id=789)
        result = _build_feedback(
            "photo_auto_created", None, batches=[], items=[], auto_jobs=[job]
        )
        assert "Auto Target" in result["body"]

    def test_error_photo_send_failed(self):
        from tg_webui.photo_loader.handlers import build_feedback as _build_feedback

        result = _build_feedback(None, "photo_send_failed", batches=[], items=[], auto_jobs=[])
        assert result["variant"] == "error"

    def test_error_photo_target_required(self):
        from tg_webui.photo_loader.handlers import build_feedback as _build_feedback

        result = _build_feedback(
            None, "photo_target_required", batches=[], items=[], auto_jobs=[]
        )
        assert result["variant"] == "error"

    def test_error_photo_target_invalid(self):
        from tg_webui.photo_loader.handlers import build_feedback as _build_feedback

        result = _build_feedback(
            None, "photo_target_invalid", batches=[], items=[], auto_jobs=[]
        )
        assert result["variant"] == "error"

    def test_error_photo_schedule_failed(self):
        from tg_webui.photo_loader.handlers import build_feedback as _build_feedback

        result = _build_feedback(
            None, "photo_schedule_failed", batches=[], items=[], auto_jobs=[]
        )
        assert result["variant"] == "error"

    def test_error_photo_batch_failed(self):
        from tg_webui.photo_loader.handlers import build_feedback as _build_feedback

        result = _build_feedback(None, "photo_batch_failed", batches=[], items=[], auto_jobs=[])
        assert result["variant"] == "error"

    def test_error_photo_auto_failed(self):
        from tg_webui.photo_loader.handlers import build_feedback as _build_feedback

        result = _build_feedback(None, "photo_auto_failed", batches=[], items=[], auto_jobs=[])
        assert result["variant"] == "error"

    def test_no_msg_no_error_returns_none(self):
        from tg_webui.photo_loader.handlers import build_feedback as _build_feedback

        result = _build_feedback(None, None, batches=[], items=[], auto_jobs=[])
        assert result is None

    def test_target_label_with_title(self):
        from tg_webui.photo_loader.handlers import _target_label

        assert _target_label("My Channel", 123) == "My Channel"

    def test_target_label_with_id_only(self):
        from tg_webui.photo_loader.handlers import _target_label

        assert _target_label(None, 123) == "123"

    def test_target_label_none(self):
        from tg_webui.photo_loader.handlers import _target_label

        assert _target_label(None, None) is None

    def test_parse_schedule_at_naive(self):
        """_parse_schedule_at with naive datetime localises and converts to UTC."""
        from tg_webui.photo_loader.forms import parse_schedule_at as _parse_schedule_at

        result = _parse_schedule_at("2025-06-15T14:30:00")
        from datetime import timezone

        assert result.tzinfo == timezone.utc

    def test_parse_schedule_at_aware(self):
        from tg_webui.photo_loader.forms import parse_schedule_at as _parse_schedule_at

        result = _parse_schedule_at("2025-06-15T14:30:00+00:00")
        from datetime import timezone

        assert result.tzinfo == timezone.utc

    def test_parse_target_without_form_values(self):
        """_parse_target looks up title/type from dialogs when not in form."""
        from tg_webui.photo_loader.forms import parse_target as _parse_target

        dialogs = [{"channel_id": 99, "title": "Found Title", "channel_type": "channel"}]
        form = {"target_dialog_id": "99", "target_title": "", "target_type": ""}
        target = _parse_target(form, dialogs)
        assert target.title == "Found Title"
        assert target.target_type == "channel"
