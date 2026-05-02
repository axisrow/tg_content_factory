"""Tests for settings routes."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.models import RuntimeSnapshot
from src.security import SessionCipher


@pytest.fixture
async def db(base_app):
    """Get db from base_app."""
    _, db, _ = base_app
    return db


@pytest.mark.anyio
async def test_settings_page_renders(route_client):
    """Test settings page renders."""
    with patch(
        "src.web.settings.handlers.AgentProviderService.load_provider_configs",
        AsyncMock(return_value=[]),
    ), patch(
        "src.web.settings.handlers.AgentProviderService.load_model_cache",
        AsyncMock(return_value={}),
    ):
        resp = await route_client.get("/settings/")
        assert resp.status_code == 200


@pytest.mark.anyio
async def test_settings_shows_accounts(route_client):
    """Test settings page shows accounts."""
    with patch(
        "src.web.settings.handlers.AgentProviderService.load_provider_configs",
        AsyncMock(return_value=[]),
    ), patch(
        "src.web.settings.handlers.AgentProviderService.load_model_cache",
        AsyncMock(return_value={}),
    ):
        resp = await route_client.get("/settings/")
        assert resp.status_code == 200
        assert "+1234567890" in resp.text


@pytest.mark.anyio
async def test_settings_degrades_when_account_session_key_is_wrong(route_client, db):
    encrypted = SessionCipher("correct-session-key").encrypt("test_session")
    await db.execute(
        "UPDATE accounts SET session_string = ? WHERE phone = ?",
        (encrypted, "+1234567890"),
    )
    await db.db.commit()
    db._accounts._session_cipher = SessionCipher("wrong-session-key")

    with patch(
        "src.web.settings.handlers.AgentProviderService.load_provider_configs",
        AsyncMock(return_value=[]),
    ), patch(
        "src.web.settings.handlers.AgentProviderService.load_model_cache",
        AsyncMock(return_value={}),
    ):
        resp = await route_client.get("/settings/")

    assert resp.status_code == 200
    assert "Saved Telegram logins and provider API keys cannot be decrypted" in resp.text
    assert "decrypt_failed" in resp.text


@pytest.mark.anyio
async def test_readonly_routes_do_not_crash_when_account_session_key_is_wrong(route_client, db):
    encrypted = SessionCipher("correct-session-key").encrypt("test_session")
    await db.execute(
        "UPDATE accounts SET session_string = ? WHERE phone = ?",
        (encrypted, "+1234567890"),
    )
    await db.db.commit()
    db._accounts._session_cipher = SessionCipher("wrong-session-key")

    for path in ("/dashboard/", "/dialogs/", "/scheduler/", "/settings/flood-status"):
        resp = await route_client.get(path)
        assert resp.status_code == 200, path


@pytest.mark.anyio
async def test_settings_shows_flood_banner_and_account_availability(route_client, db):
    accounts = await db.get_accounts(active_only=False)
    future = datetime.now(timezone.utc) + timedelta(hours=1)
    for acc in accounts:
        await db.update_account_flood(acc.phone, future)

    with patch(
        "src.web.settings.handlers.AgentProviderService.load_provider_configs",
        AsyncMock(return_value=[]),
    ), patch(
        "src.web.settings.handlers.AgentProviderService.load_model_cache",
        AsyncMock(return_value={}),
    ):
        resp = await route_client.get("/settings/")
        assert resp.status_code == 200
        assert "Все подключённые аккаунты сейчас во Flood Wait" in resp.text
        assert "Открыть планировщик и посмотреть рекомендации" in resp.text
        assert "Flood" in resp.text


@pytest.mark.anyio
async def test_settings_msg_param(route_client):
    """Test settings page with message param."""
    with patch(
        "src.web.settings.handlers.AgentProviderService.load_provider_configs",
        AsyncMock(return_value=[]),
    ), patch(
        "src.web.settings.handlers.AgentProviderService.load_model_cache",
        AsyncMock(return_value={}),
    ):
        resp = await route_client.get("/settings/?msg=credentials_saved")
        assert resp.status_code == 200


@pytest.mark.anyio
async def test_save_scheduler(route_client, db):
    """Test save scheduler settings."""
    resp = await route_client.post(
        "/settings/save-scheduler",
        data={"collect_interval_minutes": "30"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "msg=scheduler_saved" in resp.headers["location"]


@pytest.mark.anyio
async def test_save_scheduler_invalid(route_client):
    """Test save scheduler with invalid value."""
    resp = await route_client.post(
        "/settings/save-scheduler",
        data={"collect_interval_minutes": "abc"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "error=invalid_value" in resp.headers["location"]


@pytest.mark.anyio
async def test_save_credentials_valid(route_client, db):
    """Test save credentials with valid values."""
    resp = await route_client.post(
        "/settings/save-credentials",
        data={"api_id": "99999", "api_hash": "testhash123456"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "msg=credentials_saved" in resp.headers["location"]


@pytest.mark.anyio
async def test_save_credentials_invalid_id(route_client):
    """Test save credentials with invalid api_id."""
    resp = await route_client.post(
        "/settings/save-credentials",
        data={"api_id": "not_a_number", "api_hash": "testhash"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "error=invalid_api_id" in resp.headers["location"]


@pytest.mark.anyio
async def test_toggle_account_enqueues_command(route_client):
    """Web route only enqueues a telegram command; worker flips is_active and reconciles the pool."""
    resp = await route_client.post("/settings/1/toggle", follow_redirects=False)
    assert resp.status_code == 303
    assert "msg=account_toggle_queued" in resp.headers["location"]
    assert "command_id=" in resp.headers["location"]

    db = route_client._transport.app.state.db
    commands = await db.repos.telegram_commands.list_commands(limit=1)
    assert commands[0].command_type == "accounts.toggle"
    assert commands[0].payload == {"account_id": 1}


@pytest.mark.anyio
async def test_delete_account_enqueues_command(route_client):
    """Web route only enqueues a telegram command; worker removes the route_client and deletes the row."""
    resp = await route_client.post("/settings/1/delete", follow_redirects=False)
    assert resp.status_code == 303
    assert "msg=account_delete_queued" in resp.headers["location"]
    assert "command_id=" in resp.headers["location"]

    db = route_client._transport.app.state.db
    commands = await db.repos.telegram_commands.list_commands(limit=1)
    assert commands[0].command_type == "accounts.delete"
    assert commands[0].payload == {"account_id": 1}


@pytest.mark.anyio
async def test_save_filters(route_client, db):
    """Test save filter settings."""
    resp = await route_client.post(
        "/settings/save-filters",
        data={
            "min_subscribers_filter": "100",
            "auto_delete_filtered": "1",
            "auto_delete_on_collect": "0",
        },
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "msg=filters_saved" in resp.headers["location"]


@pytest.mark.anyio
async def test_save_filters_invalid(route_client):
    """Test save filters with invalid value."""
    resp = await route_client.post(
        "/settings/save-filters",
        data={"min_subscribers_filter": "not_a_number"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "error=invalid_value" in resp.headers["location"]


@pytest.mark.anyio
async def test_save_semantic_search(route_client, db):
    """Test save semantic search settings."""
    resp = await route_client.post(
        "/settings/save-semantic-search",
        data={
            "semantic_embeddings_provider": "openai",
            "semantic_embeddings_model": "text-embedding-3-small",
            "semantic_embeddings_batch_size": "100",
        },
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "msg=semantic_saved" in resp.headers["location"]


@pytest.mark.anyio
async def test_save_agent_backend(route_client, db):
    """Test save agent backend settings."""
    resp = await route_client.post(
        "/settings/save-agent",
        data={
            "agent_form_scope": "backend_override",
            "agent_backend_override": "deepagents",
        },
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "msg=agent_saved" in resp.headers["location"]


@pytest.mark.anyio
async def test_save_notification_account(route_client, db):
    """Test save notification account."""
    with patch(
        "src.web.routes.settings.deps.get_notification_target_service"
    ) as mock_svc, patch(
        "src.web.routes.settings.deps.get_notifier"
    ) as mock_notifier:
        mock_svc.return_value.set_configured_phone = AsyncMock()
        mock_notifier.return_value = None
        resp = await route_client.post(
            "/settings/save-notification-account",
            data={"notification_account_phone": "+1234567890"},
            follow_redirects=False,
        )
        assert resp.status_code == 303
        assert "msg=notification_account_saved" in resp.headers["location"]


@pytest.mark.anyio
async def test_save_notification_account_invalid(route_client, db):
    """Test save notification account with invalid phone."""
    resp = await route_client.post(
        "/settings/save-notification-account",
        data={"notification_account_phone": "+9999999999"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "error=notification_account_invalid" in resp.headers["location"]


# === Semantic Search Settings tests ===


@pytest.mark.anyio
async def test_save_semantic_search_invalid_batch_size(route_client, db):
    """Test save semantic search with invalid batch size."""
    resp = await route_client.post(
        "/settings/save-semantic-search",
        data={
            "semantic_embeddings_provider": "openai",
            "semantic_embeddings_model": "text-embedding-3-small",
            "semantic_embeddings_batch_size": "not_a_number",
        },
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "error=semantic_invalid_value" in resp.headers["location"]


@pytest.mark.anyio
async def test_save_semantic_search_empty_provider(route_client, db):
    """Test save semantic search with empty provider."""
    resp = await route_client.post(
        "/settings/save-semantic-search",
        data={
            "semantic_embeddings_provider": "",
            "semantic_embeddings_model": "text-embedding-3-small",
            "semantic_embeddings_batch_size": "100",
        },
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "error=semantic_invalid_value" in resp.headers["location"]


@pytest.mark.anyio
async def test_save_semantic_search_empty_model(route_client, db):
    """Test save semantic search with empty model."""
    resp = await route_client.post(
        "/settings/save-semantic-search",
        data={
            "semantic_embeddings_provider": "openai",
            "semantic_embeddings_model": "",
            "semantic_embeddings_batch_size": "100",
        },
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "error=semantic_invalid_value" in resp.headers["location"]


@pytest.mark.anyio
async def test_save_semantic_search_reset_index(route_client, db):
    """Test save semantic search with reset index flag."""
    # First set some initial values
    await db.set_setting("semantic_embeddings_provider", "openai")
    await db.set_setting("semantic_embeddings_model", "text-embedding-3-small")

    resp = await route_client.post(
        "/settings/save-semantic-search",
        data={
            "semantic_embeddings_provider": "cohere",
            "semantic_embeddings_model": "embed-english-v3.0",
            "semantic_embeddings_batch_size": "100",
            "semantic_reset_index": "1",
        },
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "msg=semantic_saved" in resp.headers["location"]


@pytest.mark.anyio
async def test_run_semantic_index(route_client, db):
    """Test running semantic index."""
    with patch(
        "src.web.settings.handlers.EmbeddingService.index_pending_messages",
        AsyncMock(return_value=5),
    ):
        resp = await route_client.post(
            "/settings/semantic-index",
            data={},
            follow_redirects=False,
        )
        assert resp.status_code == 303
        assert "msg=semantic_indexed" in resp.headers["location"]
        assert "indexed=5" in resp.headers["location"]


@pytest.mark.anyio
async def test_run_semantic_index_with_reset(route_client, db):
    """Test running semantic index with reset."""
    with patch(
        "src.web.settings.handlers.EmbeddingService.index_pending_messages",
        AsyncMock(return_value=3),
    ):
        resp = await route_client.post(
            "/settings/semantic-index",
            data={"semantic_reset_index": "1"},
            follow_redirects=False,
        )
        assert resp.status_code == 303
        assert "msg=semantic_indexed" in resp.headers["location"]


# === Agent Settings tests ===


@pytest.mark.anyio
async def test_save_agent_prompt_template_invalid(route_client, db):
    """Test save agent with invalid prompt template."""
    await db.set_setting("agent_dev_mode_enabled", "1")

    resp = await route_client.post(
        "/settings/save-agent",
        data={
            "agent_form_scope": "prompt_template",
            "agent_prompt_template": "Invalid {unknown_var}",
        },
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "error=agent_prompt_template_invalid" in resp.headers["location"]


@pytest.mark.anyio
async def test_save_agent_prompt_template_valid(route_client, db):
    """Test save agent with valid prompt template."""
    await db.set_setting("agent_dev_mode_enabled", "1")

    resp = await route_client.post(
        "/settings/save-agent",
        data={
            "agent_form_scope": "prompt_template",
            "agent_prompt_template": "Summarize {source_messages} for {channel_title}",
        },
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "msg=agent_saved" in resp.headers["location"]


@pytest.mark.anyio
async def test_save_agent_dev_mode_with_disclaimer(route_client, db):
    """Test enabling dev mode with disclaimer."""
    resp = await route_client.post(
        "/settings/save-agent",
        data={
            "agent_form_scope": "dev_mode",
            "agent_dev_mode_enabled": "1",
            "agent_dev_mode_disclaimer": "1",
        },
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "msg=agent_saved" in resp.headers["location"]

    # Verify dev mode was enabled
    enabled = await db.get_setting("agent_dev_mode_enabled")
    assert enabled == "1"


@pytest.mark.anyio
async def test_save_agent_dev_mode_without_disclaimer(route_client, db):
    """Test enabling dev mode without disclaimer (should not enable)."""
    await db.set_setting("agent_dev_mode_enabled", "0")

    resp = await route_client.post(
        "/settings/save-agent",
        data={
            "agent_form_scope": "dev_mode",
            "agent_dev_mode_enabled": "1",
            "agent_dev_mode_disclaimer": "0",
        },
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "msg=agent_saved" in resp.headers["location"]

    # Dev mode should NOT be enabled without disclaimer
    enabled = await db.get_setting("agent_dev_mode_enabled")
    assert enabled == "0"


@pytest.mark.anyio
async def test_save_agent_logs_rejected_deepagents_override(route_client, db, caplog):
    """Rejected deepagents override should be written to logs."""
    await db.set_setting("agent_dev_mode_enabled", "1")

    with caplog.at_level(logging.WARNING, logger="src.web.routes.settings"):
        resp = await route_client.post(
            "/settings/save-agent",
            data={
                "agent_form_scope": "backend_override",
                "agent_backend_override": "deepagents",
            },
            follow_redirects=False,
        )

    assert resp.status_code == 303
    assert "error=agent_backend_no_valid_providers" in resp.headers["location"]
    assert "Rejected deepagents override in dev mode" in caplog.text


# === Agent Provider tests ===


@pytest.mark.anyio
async def test_add_agent_provider_writes_disabled(route_client, db):
    """Test add agent provider when writes are disabled."""
    await db.set_setting("agent_dev_mode_enabled", "1")

    with patch("src.web.settings.handlers.AgentProviderService") as mock_service_cls:
        mock_service = MagicMock()
        mock_service.writes_enabled = False
        mock_service.load_provider_configs = AsyncMock(return_value=[])
        mock_service_cls.return_value = mock_service

        resp = await route_client.post(
            "/settings/agent-providers/add",
            data={"provider": "openai"},
            follow_redirects=False,
        )
        assert resp.status_code == 303
        assert "error=agent_provider_secret_required" in resp.headers["location"]


@pytest.mark.anyio
async def test_add_agent_provider_dev_mode_required(route_client, db):
    """Test add agent provider requires dev mode."""
    await db.set_setting("agent_dev_mode_enabled", "0")

    with patch("src.web.settings.handlers.AgentProviderService") as mock_service_cls:
        mock_service = MagicMock()
        mock_service.writes_enabled = True  # Set writes_enabled first
        mock_service.load_provider_configs = AsyncMock(return_value=[])
        mock_service.provider_specs = {}
        mock_service_cls.return_value = mock_service

        resp = await route_client.post(
            "/settings/agent-providers/add",
            data={"provider": "openai"},
            follow_redirects=False,
        )
        assert resp.status_code == 303
        assert "error=agent_dev_mode_required" in resp.headers["location"]


@pytest.mark.anyio
async def test_save_agent_providers_writes_disabled(route_client, db):
    """Test save agent providers when writes are disabled."""
    await db.set_setting("agent_dev_mode_enabled", "1")

    with patch("src.web.settings.handlers.AgentProviderService") as mock_service_cls:
        mock_service = MagicMock()
        mock_service.writes_enabled = False
        mock_service.load_provider_configs = AsyncMock(return_value=[])
        mock_service_cls.return_value = mock_service

        resp = await route_client.post(
            "/settings/agent-providers/save",
            data={},
            follow_redirects=False,
        )
        assert resp.status_code == 303
        assert "error=agent_provider_secret_required" in resp.headers["location"]


@pytest.mark.anyio
async def test_delete_agent_provider_writes_disabled(route_client, db):
    """Test delete agent provider when writes are disabled."""
    await db.set_setting("agent_dev_mode_enabled", "1")

    with patch("src.web.settings.handlers.AgentProviderService") as mock_service_cls:
        mock_service = MagicMock()
        mock_service.writes_enabled = False
        mock_service.load_provider_configs = AsyncMock(return_value=[])
        mock_service_cls.return_value = mock_service

        resp = await route_client.post(
            "/settings/agent-providers/openai/delete",
            follow_redirects=False,
        )
        assert resp.status_code == 303
        assert "error=agent_provider_secret_required" in resp.headers["location"]


@pytest.mark.anyio
async def test_refresh_agent_provider_models_writes_disabled(route_client, db):
    """Test refresh agent provider models when writes are disabled."""
    await db.set_setting("agent_dev_mode_enabled", "1")

    resp = await route_client.post(
        "/settings/agent-providers/openai/refresh",
        follow_redirects=False,
    )
    # Should return 409 because writes_enabled is False (no SESSION_ENCRYPTION_KEY)
    assert resp.status_code == 409


@pytest.mark.anyio
async def test_refresh_all_agent_provider_models_writes_disabled(route_client, db):
    """Test refresh all provider models when writes are disabled."""
    await db.set_setting("agent_dev_mode_enabled", "1")

    resp = await route_client.post(
        "/settings/agent-providers/refresh-all",
        follow_redirects=False,
    )
    assert resp.status_code == 409


@pytest.mark.anyio
async def test_probe_agent_provider_model_writes_disabled(route_client, db):
    """Test probe agent provider model when writes are disabled."""
    await db.set_setting("agent_dev_mode_enabled", "1")

    resp = await route_client.post(
        "/settings/agent-providers/openai/probe",
        follow_redirects=False,
    )
    assert resp.status_code == 409


# === Test All Agent Provider Models ===


@pytest.mark.anyio
async def test_test_all_agent_provider_models_writes_disabled(route_client, db):
    """Test test all agent provider models when writes are disabled."""
    await db.set_setting("agent_dev_mode_enabled", "1")

    resp = await route_client.post(
        "/settings/agent-providers/test-all",
        follow_redirects=False,
    )
    assert resp.status_code == 409


@pytest.mark.anyio
async def test_test_all_agent_provider_models_dev_mode_required(route_client, db):
    """Test test all requires dev mode."""
    await db.set_setting("agent_dev_mode_enabled", "0")

    with patch("src.web.settings.handlers.AgentProviderService") as mock_service_cls:
        mock_service = MagicMock()
        mock_service.writes_enabled = True  # Must be True to reach dev_mode check
        mock_service_cls.return_value = mock_service

        resp = await route_client.post(
            "/settings/agent-providers/test-all",
            follow_redirects=False,
        )
        assert resp.status_code == 403


@pytest.mark.anyio
async def test_test_all_status_writes_disabled(route_client, db):
    """Test test all status when writes are disabled."""
    await db.set_setting("agent_dev_mode_enabled", "1")

    resp = await route_client.get("/settings/agent-providers/test-all/status")
    assert resp.status_code == 409


@pytest.mark.anyio
async def test_test_all_status_dev_mode_required(route_client, db):
    """Test test all status requires dev mode."""
    await db.set_setting("agent_dev_mode_enabled", "0")

    with patch("src.web.settings.handlers.AgentProviderService") as mock_service_cls:
        mock_service = MagicMock()
        mock_service.writes_enabled = True  # Must be True to reach dev_mode check
        mock_service_cls.return_value = mock_service

        resp = await route_client.get("/settings/agent-providers/test-all/status")
        assert resp.status_code == 403


# === Notification tests ===


@pytest.mark.anyio
async def test_notification_setup_json_response(route_client, db):
    """Test notification setup with JSON accept header."""
    resp = await route_client.post(
        "/settings/notifications/setup",
        headers={"Accept": "application/json"},
        follow_redirects=False,
    )
    assert resp.status_code == 202
    data = resp.json()
    assert data["status"] == "queued"
    command = await db.repos.telegram_commands.get_command(data["command_id"])
    assert command is not None
    assert command.command_type == "notifications.setup_bot"


@pytest.mark.anyio
async def test_notification_setup_runtime_error(route_client, db):
    """Test notification setup is queued instead of running inline."""
    resp = await route_client.post(
        "/settings/notifications/setup",
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "command_id=" in resp.headers["location"]


@pytest.mark.anyio
async def test_notification_setup_runtime_error_json(route_client, db):
    """Test notification setup JSON response returns queued command."""
    resp = await route_client.post(
        "/settings/notifications/setup",
        headers={"Accept": "application/json"},
        follow_redirects=False,
    )
    assert resp.status_code == 202
    data = resp.json()
    assert data["status"] == "queued"


@pytest.mark.anyio
async def test_notification_bot_status(route_client, db):
    """Test notification bot status endpoint."""
    await db.repos.runtime_snapshots.upsert_snapshot(
        RuntimeSnapshot(
            snapshot_type="notification_target_status",
            payload={"target": {"state": "unavailable", "message": "unavailable"}, "bot": {"configured": False}},
        )
    )
    resp = await route_client.get("/settings/notifications/status")
    assert resp.status_code == 409
    data = resp.json()
    assert data["configured"] is False


@pytest.mark.anyio
async def test_notification_delete(route_client, db):
    """Test notification bot deletion."""
    resp = await route_client.post(
        "/settings/notifications/delete",
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "command_id=" in resp.headers["location"]
    commands = await db.repos.telegram_commands.list_commands(limit=1)
    assert commands[0].command_type == "notifications.delete_bot"


@pytest.mark.anyio
async def test_notification_delete_json(route_client, db):
    """Test notification bot deletion JSON response returns queued command."""
    resp = await route_client.post(
        "/settings/notifications/delete",
        headers={"Accept": "application/json"},
        follow_redirects=False,
    )
    assert resp.status_code == 202
    data = resp.json()
    assert data["status"] == "queued"


# === Test notification ===


@pytest.mark.anyio
async def test_test_notification_success(route_client, db):
    """Test test notification success."""
    resp = await route_client.post(
        "/settings/notifications/test",
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "command_id=" in resp.headers["location"]
    commands = await db.repos.telegram_commands.list_commands(limit=1)
    assert commands[0].command_type == "notifications.test"
