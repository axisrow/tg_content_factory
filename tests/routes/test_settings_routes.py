"""Tests for settings routes."""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest


@pytest.fixture
async def client(route_client):
    """Use shared route_client fixture."""
    return route_client


@pytest.fixture
async def db(base_app):
    """Get db from base_app."""
    _, db, _ = base_app
    return db


@pytest.mark.asyncio
async def test_settings_page_renders(client):
    """Test settings page renders."""
    with patch(
        "src.web.routes.settings.AgentProviderService.load_provider_configs",
        AsyncMock(return_value=[]),
    ), patch(
        "src.web.routes.settings.AgentProviderService.load_model_cache",
        AsyncMock(return_value={}),
    ):
        resp = await client.get("/settings/")
        assert resp.status_code == 200


@pytest.mark.asyncio
async def test_settings_shows_accounts(client):
    """Test settings page shows accounts."""
    with patch(
        "src.web.routes.settings.AgentProviderService.load_provider_configs",
        AsyncMock(return_value=[]),
    ), patch(
        "src.web.routes.settings.AgentProviderService.load_model_cache",
        AsyncMock(return_value={}),
    ):
        resp = await client.get("/settings/")
        assert resp.status_code == 200
        assert "+1234567890" in resp.text


@pytest.mark.asyncio
async def test_settings_msg_param(client):
    """Test settings page with message param."""
    with patch(
        "src.web.routes.settings.AgentProviderService.load_provider_configs",
        AsyncMock(return_value=[]),
    ), patch(
        "src.web.routes.settings.AgentProviderService.load_model_cache",
        AsyncMock(return_value={}),
    ):
        resp = await client.get("/settings/?msg=credentials_saved")
        assert resp.status_code == 200


@pytest.mark.asyncio
async def test_save_scheduler(client, db):
    """Test save scheduler settings."""
    resp = await client.post(
        "/settings/save-scheduler",
        data={"collect_interval_minutes": "30"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "msg=scheduler_saved" in resp.headers["location"]


@pytest.mark.asyncio
async def test_save_scheduler_invalid(client):
    """Test save scheduler with invalid value."""
    resp = await client.post(
        "/settings/save-scheduler",
        data={"collect_interval_minutes": "abc"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "error=invalid_value" in resp.headers["location"]


@pytest.mark.asyncio
async def test_save_credentials_valid(client, db):
    """Test save credentials with valid values."""
    resp = await client.post(
        "/settings/save-credentials",
        data={"api_id": "99999", "api_hash": "testhash123456"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "msg=credentials_saved" in resp.headers["location"]


@pytest.mark.asyncio
async def test_save_credentials_invalid_id(client):
    """Test save credentials with invalid api_id."""
    resp = await client.post(
        "/settings/save-credentials",
        data={"api_id": "not_a_number", "api_hash": "testhash"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "error=invalid_api_id" in resp.headers["location"]


@pytest.mark.asyncio
async def test_toggle_account(client):
    """Test toggle account."""
    with patch("src.web.routes.settings.deps.account_service") as mock_svc:
        mock_svc.return_value.toggle = AsyncMock()
        resp = await client.post("/settings/1/toggle", follow_redirects=False)
        assert resp.status_code == 303
        assert "msg=account_toggled" in resp.headers["location"]


@pytest.mark.asyncio
async def test_delete_account(client):
    """Test delete account."""
    with patch("src.web.routes.settings.deps.account_service") as mock_svc:
        mock_svc.return_value.delete = AsyncMock()
        resp = await client.post("/settings/1/delete", follow_redirects=False)
        assert resp.status_code == 303
        assert "msg=account_deleted" in resp.headers["location"]


@pytest.mark.asyncio
async def test_save_filters(client, db):
    """Test save filter settings."""
    resp = await client.post(
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


@pytest.mark.asyncio
async def test_save_filters_invalid(client):
    """Test save filters with invalid value."""
    resp = await client.post(
        "/settings/save-filters",
        data={"min_subscribers_filter": "not_a_number"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "error=invalid_value" in resp.headers["location"]


@pytest.mark.asyncio
async def test_save_semantic_search(client, db):
    """Test save semantic search settings."""
    resp = await client.post(
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


@pytest.mark.asyncio
async def test_save_agent_backend(client, db):
    """Test save agent backend settings."""
    resp = await client.post(
        "/settings/save-agent",
        data={
            "agent_form_scope": "backend_override",
            "agent_backend_override": "deepagents",
        },
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "msg=agent_saved" in resp.headers["location"]


@pytest.mark.asyncio
async def test_save_notification_account(client, db):
    """Test save notification account."""
    with patch(
        "src.web.routes.settings.deps.get_notification_target_service"
    ) as mock_svc, patch(
        "src.web.routes.settings.deps.get_notifier"
    ) as mock_notifier:
        mock_svc.return_value.set_configured_phone = AsyncMock()
        mock_notifier.return_value = None
        resp = await client.post(
            "/settings/save-notification-account",
            data={"notification_account_phone": "+1234567890"},
            follow_redirects=False,
        )
        assert resp.status_code == 303
        assert "msg=notification_account_saved" in resp.headers["location"]


@pytest.mark.asyncio
async def test_save_notification_account_invalid(client, db):
    """Test save notification account with invalid phone."""
    resp = await client.post(
        "/settings/save-notification-account",
        data={"notification_account_phone": "+9999999999"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "error=notification_account_invalid" in resp.headers["location"]
