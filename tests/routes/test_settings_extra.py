"""Extra tests for settings routes targeting uncovered lines."""

from __future__ import annotations

import logging
import os
from datetime import datetime, timedelta, timezone
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


# ── settings_page: uncovered branches ──────────────────────────────────


@pytest.mark.asyncio
async def test_settings_page_flood_wait_naive_tz(client, db):
    """Test settings page when flood_wait_until has no tzinfo (line 424)."""
    accounts = await db.get_accounts(active_only=False)
    future_naive = datetime.now() + timedelta(hours=1)  # no tzinfo
    for acc in accounts:
        await db.update_account_flood(acc.phone, future_naive)

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
async def test_settings_page_expired_flood_cleared(client, db):
    """Test settings page clears expired flood waits (lines 424-427)."""
    accounts = await db.get_accounts(active_only=False)
    past = datetime.now(timezone.utc) - timedelta(hours=1)
    for acc in accounts:
        await db.update_account_flood(acc.phone, past)

    with patch(
        "src.web.routes.settings.AgentProviderService.load_provider_configs",
        AsyncMock(return_value=[]),
    ), patch(
        "src.web.routes.settings.AgentProviderService.load_model_cache",
        AsyncMock(return_value={}),
    ):
        resp = await client.get("/settings/")
        assert resp.status_code == 200

    # Flood should be cleared
    accounts_after = await db.get_accounts(active_only=False)
    for acc in accounts_after:
        assert acc.flood_wait_until is None


@pytest.mark.asyncio
async def test_settings_page_inactive_account(client, db):
    """Test settings page with inactive account (line 434)."""
    accounts = await db.get_accounts(active_only=False)
    for acc in accounts:
        await db.repos.accounts.set_account_active(acc.id, False)

    # Re-add an active account so the page can load
    from src.models import Account
    await db.add_account(Account(phone="+15555555555", session_string="test2"))

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
async def test_settings_page_notification_bot_error(client, db):
    """Test settings page when notification bot fails to load (lines 467-471)."""
    with patch(
        "src.web.routes.settings.AgentProviderService.load_provider_configs",
        AsyncMock(return_value=[]),
    ), patch(
        "src.web.routes.settings.AgentProviderService.load_model_cache",
        AsyncMock(return_value={}),
    ), patch(
        "src.web.routes.settings.deps.get_notification_target_service"
    ) as mock_target_svc:
        mock_target_svc.return_value.describe_target = AsyncMock(
            return_value=MagicMock(state="available", configured_phone="+1234567890")
        )
        with patch(
            "src.web.routes.settings.NotificationService"
        ) as mock_notif_cls:
            mock_notif = MagicMock()
            mock_notif.get_status = AsyncMock(side_effect=RuntimeError("No client"))
            mock_notif_cls.return_value = mock_notif

            resp = await client.get("/settings/")
            assert resp.status_code == 200


# ── save_scheduler: line 560-566 (interval update via scheduler) ───────


@pytest.mark.asyncio
async def test_save_scheduler_updates_running_scheduler(client, db):
    """Test save scheduler updates interval on running scheduler (lines 567-569)."""
    mock_scheduler = MagicMock()
    mock_scheduler.update_interval = MagicMock()
    client._transport_app.state.scheduler = mock_scheduler

    resp = await client.post(
        "/settings/save-scheduler",
        data={"collect_interval_minutes": "45"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "msg=scheduler_saved" in resp.headers["location"]
    mock_scheduler.update_interval.assert_called_once_with(45)


@pytest.mark.asyncio
async def test_save_scheduler_clamps_interval(client, db):
    """Test save scheduler clamps interval to 1..1440."""
    resp = await client.post(
        "/settings/save-scheduler",
        data={"collect_interval_minutes": "0"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "msg=scheduler_saved" in resp.headers["location"]
    val = await db.get_setting("collect_interval_minutes")
    assert int(val) >= 1


# ── save_semantic_search: lines 576-591 (branch coverage) ──────────────


@pytest.mark.asyncio
async def test_save_semantic_search_saves_api_key(client, db):
    """Test save semantic search saves API key when not masked (lines 610-613)."""
    resp = await client.post(
        "/settings/save-semantic-search",
        data={
            "semantic_embeddings_provider": "openai",
            "semantic_embeddings_model": "text-embedding-3-small",
            "semantic_embeddings_batch_size": "100",
            "semantic_embeddings_api_key": "sk-test-key-123",
        },
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "msg=semantic_saved" in resp.headers["location"]
    saved_key = await db.get_setting("semantic_embeddings_api_key")
    assert saved_key == "sk-test-key-123"


@pytest.mark.asyncio
async def test_save_semantic_search_preserves_masked_api_key(client, db):
    """Test that masked API key is preserved (lines 611-612)."""
    await db.set_setting("semantic_embeddings_api_key", "original-key")
    resp = await client.post(
        "/settings/save-semantic-search",
        data={
            "semantic_embeddings_provider": "openai",
            "semantic_embeddings_model": "text-embedding-3-small",
            "semantic_embeddings_batch_size": "100",
            "semantic_embeddings_api_key": "••••••••",
        },
        follow_redirects=False,
    )
    assert resp.status_code == 303
    saved_key = await db.get_setting("semantic_embeddings_api_key")
    assert saved_key == "original-key"


@pytest.mark.asyncio
async def test_save_semantic_search_no_api_key_field(client, db):
    """Test save semantic search with no api_key field at all (line 610)."""
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


# ── semantic-index: lines 624, 628 ─────────────────────────────────────


@pytest.mark.asyncio
async def test_run_semantic_index_unavailable(client, db):
    """Test running semantic index when not available (line 624)."""
    with patch(
        "src.web.routes.settings.deps.get_search_engine"
    ) as mock_se:
        mock_se.return_value.semantic_available = False
        resp = await client.post(
            "/settings/semantic-index",
            data={},
            follow_redirects=False,
        )
        assert resp.status_code == 303
        assert "error=semantic_unavailable" in resp.headers["location"]


@pytest.mark.asyncio
async def test_run_semantic_index_with_reset_flag(client, db):
    """Test semantic index with reset flag (line 628)."""
    with patch(
        "src.web.routes.settings.EmbeddingService.index_pending_messages",
        AsyncMock(return_value=0),
    ), patch(
        "src.web.routes.settings.deps.get_search_engine"
    ) as mock_se:
        se = mock_se.return_value
        se.semantic_available = True
        se.invalidate_numpy_index = MagicMock()

        resp = await client.post(
            "/settings/semantic-index",
            data={"semantic_reset_index": "1"},
            follow_redirects=False,
        )
        assert resp.status_code == 303
        assert "msg=semantic_indexed" in resp.headers["location"]


# ── save_agent: lines 641-652 (tool_permissions scope) ─────────────────


@pytest.mark.asyncio
async def test_save_agent_tool_permissions(client, db):
    """Test save agent with tool_permissions scope (lines 645-653)."""
    resp = await client.post(
        "/settings/save-agent",
        data={
            "agent_form_scope": "tool_permissions",
            "tool_perm__search_messages": "1",
            "tool_perm__send_message": "0",
        },
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "msg=tool_permissions_saved" in resp.headers["location"]


@pytest.mark.asyncio
async def test_save_agent_tool_permissions_with_phone(client, db):
    """Test save agent tool_permissions with phone (line 648)."""
    resp = await client.post(
        "/settings/save-agent",
        data={
            "agent_form_scope": "tool_permissions",
            "phone": "+1234567890",
            "tool_perm__search_messages": "1",
        },
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "msg=tool_permissions_saved" in resp.headers["location"]


# ── save_agent: lines 655-669 (backend_override branches) ──────────────


@pytest.mark.asyncio
async def test_save_agent_backend_override_invalid(client, db):
    """Test save agent with invalid backend override (line 669)."""
    await db.set_setting("agent_dev_mode_enabled", "1")

    resp = await client.post(
        "/settings/save-agent",
        data={
            "agent_form_scope": "backend_override",
            "agent_backend_override": "invalid_backend",
            "agent_dev_mode_enabled": "1",
            "agent_dev_mode_disclaimer": "1",
        },
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "msg=agent_saved" in resp.headers["location"]
    # Invalid override should be reset to "auto"
    saved = await db.get_setting("agent_backend_override")
    assert saved == "auto"


@pytest.mark.asyncio
async def test_save_agent_dev_mode_no_disclaimer_keeps_current(client, db):
    """Test dev_mode scope without disclaimer keeps current state (line 679)."""
    # Start with dev mode enabled; request wants to enable but no disclaimer
    # => should stay at current (which is "1")
    await db.set_setting("agent_dev_mode_enabled", "1")

    resp = await client.post(
        "/settings/save-agent",
        data={
            "agent_form_scope": "dev_mode",
            "agent_dev_mode_enabled": "1",
            "agent_dev_mode_disclaimer": "0",
        },
        follow_redirects=False,
    )
    assert resp.status_code == 303
    # wants_dev_mode=True but no disclaimer => dev_mode_enabled = current = "1"
    saved = await db.get_setting("agent_dev_mode_enabled")
    assert saved == "1"


@pytest.mark.asyncio
async def test_save_agent_prompt_template_empty_falls_back(client, db):
    """Test empty prompt template falls back to default (lines 683-684)."""
    await db.set_setting("agent_dev_mode_enabled", "1")

    resp = await client.post(
        "/settings/save-agent",
        data={
            "agent_form_scope": "prompt_template",
            "agent_prompt_template": "",
        },
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "msg=agent_saved" in resp.headers["location"]


# ── save_agent: lines 714-723 (claude override no credentials) ─────────


@pytest.mark.asyncio
async def test_save_agent_claude_override_no_credentials(client, db, caplog):
    """Test claude override rejected without API key (lines 714-723)."""
    await db.set_setting("agent_dev_mode_enabled", "1")

    with patch.dict(os.environ, {}, clear=False):
        # Ensure no Claude credentials
        os.environ.pop("ANTHROPIC_API_KEY", None)
        os.environ.pop("CLAUDE_CODE_OAUTH_TOKEN", None)

        with caplog.at_level(logging.WARNING, logger="src.web.routes.settings"):
            resp = await client.post(
                "/settings/save-agent",
                data={
                    "agent_form_scope": "backend_override",
                    "agent_backend_override": "claude",
                    "agent_dev_mode_enabled": "1",
                    "agent_dev_mode_disclaimer": "1",
                },
                follow_redirects=False,
            )

    assert resp.status_code == 303
    assert "error=agent_backend_claude_unavailable" in resp.headers["location"]


# ── save_agent: line 730 (agent_manager refresh) ───────────────────────


@pytest.mark.asyncio
async def test_save_agent_refreshes_agent_manager(client, db):
    """Test save agent refreshes agent_manager settings cache (line 730)."""
    await db.set_setting("agent_dev_mode_enabled", "0")

    mock_manager = MagicMock()
    mock_manager.refresh_settings_cache = AsyncMock()
    client._transport_app.state.agent_manager = mock_manager

    resp = await client.post(
        "/settings/save-agent",
        data={
            "agent_form_scope": "dev_mode",
            "agent_dev_mode_enabled": "0",
        },
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "msg=agent_saved" in resp.headers["location"]
    mock_manager.refresh_settings_cache.assert_called_once()


# ── agent-providers/add: lines 746-749, 751, 757 ──────────────────────


@pytest.mark.asyncio
async def test_add_agent_provider_invalid_name(client, db):
    """Test add agent provider with invalid name (lines 746-749)."""
    await db.set_setting("agent_dev_mode_enabled", "1")

    with patch(
        "src.web.routes.settings.AgentProviderService"
    ) as mock_service_cls, patch(
        "src.web.routes.settings.deepagents_provider_spec"
    ) as mock_spec:
        mock_service = MagicMock()
        mock_service.writes_enabled = True
        mock_service_cls.return_value = mock_service
        mock_spec.return_value = None

        resp = await client.post(
            "/settings/agent-providers/add",
            data={"provider": "nonexistent_provider"},
            follow_redirects=False,
        )
        assert resp.status_code == 303
        assert "error=agent_provider_invalid" in resp.headers["location"]


@pytest.mark.asyncio
async def test_add_agent_provider_already_exists(client, db):
    """Test add agent provider when it already exists (line 751)."""
    await db.set_setting("agent_dev_mode_enabled", "1")

    with patch(
        "src.web.routes.settings.AgentProviderService"
    ) as mock_service_cls, patch(
        "src.web.routes.settings.deepagents_provider_spec"
    ) as mock_spec:
        mock_service = MagicMock()
        mock_service.writes_enabled = True
        mock_service.load_provider_configs = AsyncMock(
            return_value=[MagicMock(provider="openai")]
        )
        mock_service_cls.return_value = mock_service
        mock_spec.return_value = MagicMock()

        resp = await client.post(
            "/settings/agent-providers/add",
            data={"provider": "openai"},
            follow_redirects=False,
        )
        assert resp.status_code == 303
        assert "msg=agent_saved" in resp.headers["location"]


@pytest.mark.asyncio
async def test_add_agent_provider_refreshes_manager(client, db):
    """Test add agent provider refreshes agent manager (line 757)."""
    await db.set_setting("agent_dev_mode_enabled", "1")

    mock_manager = MagicMock()
    mock_manager.refresh_settings_cache = AsyncMock()
    client._transport_app.state.agent_manager = mock_manager

    with patch(
        "src.web.routes.settings.AgentProviderService"
    ) as mock_service_cls, patch(
        "src.web.routes.settings.deepagents_provider_spec"
    ) as mock_spec:
        mock_service = MagicMock()
        mock_service.writes_enabled = True
        mock_service.load_provider_configs = AsyncMock(return_value=[])
        mock_service.create_empty_config = MagicMock(return_value=MagicMock())
        mock_service.save_provider_configs = AsyncMock()
        mock_service_cls.return_value = mock_service
        mock_spec.return_value = MagicMock()

        resp = await client.post(
            "/settings/agent-providers/add",
            data={"provider": "openai"},
            follow_redirects=False,
        )
        assert resp.status_code == 303
        mock_manager.refresh_settings_cache.assert_called_once()


# ── agent-providers/save: lines 771, 773, 801 ──────────────────────────


@pytest.mark.asyncio
async def test_save_agent_providers_dev_mode_required(client, db):
    """Test save agent providers requires dev mode (line 771)."""
    await db.set_setting("agent_dev_mode_enabled", "0")

    with patch(
        "src.web.routes.settings.AgentProviderService"
    ) as mock_service_cls:
        mock_service = MagicMock()
        mock_service.writes_enabled = True
        mock_service_cls.return_value = mock_service

        resp = await client.post(
            "/settings/agent-providers/save",
            data={},
            follow_redirects=False,
        )
        assert resp.status_code == 303
        assert "error=agent_dev_mode_required" in resp.headers["location"]


@pytest.mark.asyncio
async def test_save_agent_providers_probes_enabled(client, db):
    """Test save agent providers probes enabled providers (lines 777-801)."""
    await db.set_setting("agent_dev_mode_enabled", "1")

    cfg = MagicMock()
    cfg.enabled = True
    cfg.provider = "openai"

    with patch(
        "src.web.routes.settings.AgentProviderService"
    ) as mock_service_cls, patch(
        "src.web.routes.settings.deps.get_agent_manager"
    ) as mock_get_mgr, patch(
        "src.web.routes.settings._probe_provider_config",
        AsyncMock(return_value=MagicMock()),
    ):
        mock_service = MagicMock()
        mock_service.writes_enabled = True
        mock_service.load_provider_configs = AsyncMock(return_value=[cfg])
        mock_service.parse_provider_form = MagicMock(return_value=[cfg])
        mock_service.validate_provider_config = MagicMock(return_value="")
        mock_service.save_provider_configs = AsyncMock()
        mock_service_cls.return_value = mock_service

        mock_manager = MagicMock()
        mock_manager.refresh_settings_cache = AsyncMock()
        mock_get_mgr.return_value = mock_manager

        resp = await client.post(
            "/settings/agent-providers/save",
            data={},
            follow_redirects=False,
        )
        assert resp.status_code == 303
        assert "msg=agent_saved" in resp.headers["location"]


# ── agent-providers/{name}/delete: lines 813-825 ───────────────────────


@pytest.mark.asyncio
async def test_delete_agent_provider_dev_mode_required(client, db):
    """Test delete agent provider requires dev mode (lines 813-815)."""
    await db.set_setting("agent_dev_mode_enabled", "0")

    with patch(
        "src.web.routes.settings.AgentProviderService"
    ) as mock_service_cls:
        mock_service = MagicMock()
        mock_service.writes_enabled = True
        mock_service.load_provider_configs = AsyncMock(return_value=[])
        mock_service.save_provider_configs = AsyncMock()
        mock_service_cls.return_value = mock_service

        resp = await client.post(
            "/settings/agent-providers/openai/delete",
            follow_redirects=False,
        )
        assert resp.status_code == 303
        assert "error=agent_dev_mode_required" in resp.headers["location"]


@pytest.mark.asyncio
async def test_delete_agent_provider_refreshes_manager(client, db):
    """Test delete provider refreshes agent manager (lines 821-824)."""
    await db.set_setting("agent_dev_mode_enabled", "1")

    mock_manager = MagicMock()
    mock_manager.refresh_settings_cache = AsyncMock()
    client._transport_app.state.agent_manager = mock_manager

    with patch(
        "src.web.routes.settings.AgentProviderService"
    ) as mock_service_cls:
        mock_service = MagicMock()
        mock_service.writes_enabled = True
        mock_service.load_provider_configs = AsyncMock(
            return_value=[MagicMock(provider="openai")]
        )
        mock_service.save_provider_configs = AsyncMock()
        mock_service_cls.return_value = mock_service

        resp = await client.post(
            "/settings/agent-providers/openai/delete",
            follow_redirects=False,
        )
        assert resp.status_code == 303
        assert "msg=agent_saved" in resp.headers["location"]
        mock_manager.refresh_settings_cache.assert_called_once()


# ── agent-providers/{name}/refresh: lines 837, 839 ─────────────────────


@pytest.mark.asyncio
async def test_refresh_agent_provider_dev_mode_required_json(client, db):
    """Test refresh provider requires dev mode (JSON) (line 837)."""
    await db.set_setting("agent_dev_mode_enabled", "0")

    with patch(
        "src.web.routes.settings.AgentProviderService"
    ) as mock_service_cls:
        mock_service = MagicMock()
        mock_service.writes_enabled = True
        mock_service_cls.return_value = mock_service

        resp = await client.post(
            "/settings/agent-providers/openai/refresh",
            follow_redirects=False,
        )
        assert resp.status_code == 403


@pytest.mark.asyncio
async def test_refresh_agent_provider_unknown(client, db):
    """Test refresh unknown provider (line 839)."""
    await db.set_setting("agent_dev_mode_enabled", "1")

    with patch(
        "src.web.routes.settings.AgentProviderService"
    ) as mock_service_cls, patch(
        "src.web.routes.settings.deepagents_provider_spec"
    ) as mock_spec:
        mock_service = MagicMock()
        mock_service.writes_enabled = True
        mock_service_cls.return_value = mock_service
        mock_spec.return_value = None

        resp = await client.post(
            "/settings/agent-providers/nonexistent/refresh",
            follow_redirects=False,
        )
        assert resp.status_code == 404


# ── agent-providers/{name}/probe: lines 902, 904, 907, 911-917 ────────


@pytest.mark.asyncio
async def test_probe_agent_provider_unknown(client, db):
    """Test probe unknown provider (line 904)."""
    await db.set_setting("agent_dev_mode_enabled", "1")

    with patch(
        "src.web.routes.settings.AgentProviderService"
    ) as mock_service_cls, patch(
        "src.web.routes.settings.deepagents_provider_spec"
    ) as mock_spec:
        mock_service = MagicMock()
        mock_service.writes_enabled = True
        mock_service_cls.return_value = mock_service
        mock_spec.return_value = None

        resp = await client.post(
            "/settings/agent-providers/nonexistent/probe",
            follow_redirects=False,
        )
        assert resp.status_code == 404


@pytest.mark.asyncio
async def test_probe_agent_provider_validation_blocked(client, db):
    """Test probe provider when validation fails (lines 910-917)."""
    await db.set_setting("agent_dev_mode_enabled", "1")

    with patch(
        "src.web.routes.settings.AgentProviderService"
    ) as mock_service_cls, patch(
        "src.web.routes.settings.deepagents_provider_spec"
    ) as mock_spec:
        mock_service = MagicMock()
        mock_service.writes_enabled = True
        mock_service.load_provider_configs = AsyncMock(return_value=[])
        mock_cfg = MagicMock()
        mock_cfg.selected_model = "gpt-4"
        mock_service.parse_single_provider_form = MagicMock(return_value=mock_cfg)
        mock_service.validate_provider_config = MagicMock(return_value="Missing API key")
        mock_service.config_fingerprint = MagicMock(return_value="fp123")
        mock_service_cls.return_value = mock_service
        mock_spec.return_value = MagicMock()

        resp = await client.post(
            "/settings/agent-providers/openai/probe",
            follow_redirects=False,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "unsupported"
        assert data["reason"] == "Missing API key"


@pytest.mark.asyncio
async def test_probe_agent_provider_success(client, db):
    """Test probe provider with successful probe (lines 929-960)."""
    await db.set_setting("agent_dev_mode_enabled", "1")

    from src.services.agent_provider_service import ProviderModelCompatibilityRecord

    record = ProviderModelCompatibilityRecord(
        model="gpt-4",
        status="supported",
        reason="",
        config_fingerprint="fp123",
        probe_kind="auto-select",
    )

    with patch(
        "src.web.routes.settings.AgentProviderService"
    ) as mock_service_cls, patch(
        "src.web.routes.settings.deepagents_provider_spec"
    ) as mock_spec, patch(
        "src.web.routes.settings._probe_provider_config",
        AsyncMock(return_value=record),
    ):
        mock_service = MagicMock()
        mock_service.writes_enabled = True
        mock_service.load_provider_configs = AsyncMock(return_value=[])
        mock_cfg = MagicMock()
        mock_cfg.selected_model = "gpt-4"
        mock_service.parse_single_provider_form = MagicMock(return_value=mock_cfg)
        mock_service.validate_provider_config = MagicMock(return_value="")
        mock_service_cls.return_value = mock_service
        mock_spec.return_value = MagicMock()

        resp = await client.post(
            "/settings/agent-providers/openai/probe",
            follow_redirects=False,
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["ok"] is True
        assert data["status"] == "supported"


# ── save-filters: lines 1020-1024, 1048-1049 ──────────────────────────


@pytest.mark.asyncio
async def test_save_filters_zero_subs_no_auto_delete(client, db):
    """Test save filters with zero min_subscribers and no auto_delete (lines 1020-1042)."""
    resp = await client.post(
        "/settings/save-filters",
        data={
            "min_subscribers_filter": "0",
        },
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "msg=filters_saved" in resp.headers["location"]


@pytest.mark.asyncio
async def test_save_filters_with_auto_delete_on_collect(client, db):
    """Test save filters with auto_delete_on_collect checked."""
    resp = await client.post(
        "/settings/save-filters",
        data={
            "min_subscribers_filter": "50",
            "auto_delete_on_collect": "1",
        },
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "msg=filters_saved" in resp.headers["location"]


# ── save-notification-account: lines 1048-1049, 1058 ───────────────────


@pytest.mark.asyncio
async def test_save_notification_account_with_notifier(client, db):
    """Test save notification account invalidates notifier cache (line 1058)."""
    mock_notifier = MagicMock()
    mock_notifier.invalidate_me_cache = MagicMock()

    with patch(
        "src.web.routes.settings.deps.get_notification_target_service"
    ) as mock_svc, patch(
        "src.web.routes.settings.deps.get_notifier"
    ) as mock_get_notifier:
        mock_svc.return_value.set_configured_phone = AsyncMock()
        mock_get_notifier.return_value = mock_notifier

        resp = await client.post(
            "/settings/save-notification-account",
            data={"notification_account_phone": "+1234567890"},
            follow_redirects=False,
        )
        assert resp.status_code == 303
        assert "msg=notification_account_saved" in resp.headers["location"]
        mock_notifier.invalidate_me_cache.assert_called_once()


@pytest.mark.asyncio
async def test_save_notification_account_empty_phone(client, db):
    """Test save notification account with empty phone clears setting."""
    with patch(
        "src.web.routes.settings.deps.get_notification_target_service"
    ) as mock_svc, patch(
        "src.web.routes.settings.deps.get_notifier"
    ) as mock_get_notifier:
        mock_svc.return_value.set_configured_phone = AsyncMock()
        mock_get_notifier.return_value = None

        resp = await client.post(
            "/settings/save-notification-account",
            data={"notification_account_phone": ""},
            follow_redirects=False,
        )
        assert resp.status_code == 303
        assert "msg=notification_account_saved" in resp.headers["location"]
        mock_svc.return_value.set_configured_phone.assert_called_once_with(None)


# ── save-credentials: lines 1065-1078, 1087 ────────────────────────────


@pytest.mark.asyncio
async def test_save_credentials_only_hash(client, db):
    """Test save credentials with only api_hash changed (lines 1079-1080)."""
    await db.set_setting("tg_api_id", "99999")

    resp = await client.post(
        "/settings/save-credentials",
        data={"api_id": "••••••••", "api_hash": "newhash123"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "msg=credentials_saved" in resp.headers["location"]


@pytest.mark.asyncio
async def test_save_credentials_no_changes(client, db):
    """Test save credentials when nothing changed."""
    resp = await client.post(
        "/settings/save-credentials",
        data={"api_id": "••••••••", "api_hash": "••••••••"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "msg=credentials_saved" in resp.headers["location"]


# ── notifications/setup: lines 1098-1099, 1107, 1117 ──────────────────


@pytest.mark.asyncio
async def test_notification_setup_general_exception(client, db):
    """Test notification setup with general exception (lines 1104-1108)."""
    with patch(
        "src.web.routes.settings.deps.get_notification_target_service"
    ) as mock_svc:
        mock_svc.return_value.describe_target = AsyncMock(
            return_value=MagicMock(state="available", configured_phone="+1234567890")
        )
        with patch(
            "src.web.routes.settings.NotificationService"
        ) as mock_notif_cls:
            mock_notif = MagicMock()
            mock_notif.setup_bot = AsyncMock(side_effect=Exception("Unexpected"))
            mock_notif_cls.return_value = mock_notif

            resp = await client.post(
                "/settings/notifications/setup",
                follow_redirects=False,
            )
            assert resp.status_code == 303
            assert "error=notification_action_failed" in resp.headers["location"]


@pytest.mark.asyncio
async def test_notification_setup_general_exception_json(client, db):
    """Test notification setup with general exception, JSON response (line 1107)."""
    with patch(
        "src.web.routes.settings.deps.get_notification_target_service"
    ) as mock_svc:
        mock_svc.return_value.describe_target = AsyncMock(
            return_value=MagicMock(state="available", configured_phone="+1234567890")
        )
        with patch(
            "src.web.routes.settings.NotificationService"
        ) as mock_notif_cls:
            mock_notif = MagicMock()
            mock_notif.setup_bot = AsyncMock(side_effect=Exception("Unexpected"))
            mock_notif_cls.return_value = mock_notif

            resp = await client.post(
                "/settings/notifications/setup",
                headers={"Accept": "application/json"},
                follow_redirects=False,
            )
            assert resp.status_code == 500
            data = resp.json()
            assert "Unexpected" in data["error"]


@pytest.mark.asyncio
async def test_notification_setup_success_redirect(client, db):
    """Test notification setup success with redirect (line 1117)."""
    with patch(
        "src.web.routes.settings.deps.get_notification_target_service"
    ) as mock_svc:
        mock_svc.return_value.describe_target = AsyncMock(
            return_value=MagicMock(state="available", configured_phone="+1234567890")
        )
        with patch(
            "src.web.routes.settings.NotificationService"
        ) as mock_notif_cls:
            mock_notif = MagicMock()
            mock_notif.setup_bot = AsyncMock(
                return_value=MagicMock(bot_username="test_bot", bot_id=12345)
            )
            mock_notif_cls.return_value = mock_notif

            resp = await client.post(
                "/settings/notifications/setup",
                follow_redirects=False,
            )
            assert resp.status_code == 303
            assert "msg=notification_bot_created" in resp.headers["location"]


# ── notifications/delete: lines 1142-1153 ──────────────────────────────


@pytest.mark.asyncio
async def test_notification_delete_runtime_error_account(client, db):
    """Test notification delete RuntimeError mentioning 'аккаунт' (lines 1146-1148)."""
    with patch(
        "src.web.routes.settings.deps.get_notification_target_service"
    ) as mock_svc:
        mock_svc.return_value.describe_target = AsyncMock(
            return_value=MagicMock(state="available")
        )
        with patch(
            "src.web.routes.settings.NotificationService"
        ) as mock_notif_cls:
            mock_notif = MagicMock()
            mock_notif.teardown_bot = AsyncMock(
                side_effect=RuntimeError("Аккаунт недоступен")
            )
            mock_notif_cls.return_value = mock_notif

            resp = await client.post(
                "/settings/notifications/delete",
                follow_redirects=False,
            )
            assert resp.status_code == 303
            assert "error=notification_account_unavailable" in resp.headers["location"]


@pytest.mark.asyncio
async def test_notification_delete_runtime_error_other(client, db):
    """Test notification delete RuntimeError with other message (lines 1142-1148)."""
    with patch(
        "src.web.routes.settings.deps.get_notification_target_service"
    ) as mock_svc:
        mock_svc.return_value.describe_target = AsyncMock(
            return_value=MagicMock(state="available")
        )
        with patch(
            "src.web.routes.settings.NotificationService"
        ) as mock_notif_cls:
            mock_notif = MagicMock()
            mock_notif.teardown_bot = AsyncMock(
                side_effect=RuntimeError("Bot not found")
            )
            mock_notif_cls.return_value = mock_notif

            resp = await client.post(
                "/settings/notifications/delete",
                follow_redirects=False,
            )
            assert resp.status_code == 303
            assert "error=notification_bot_missing" in resp.headers["location"]


@pytest.mark.asyncio
async def test_notification_delete_general_exception(client, db):
    """Test notification delete with general exception (lines 1149-1153)."""
    with patch(
        "src.web.routes.settings.deps.get_notification_target_service"
    ) as mock_svc:
        mock_svc.return_value.describe_target = AsyncMock(
            return_value=MagicMock(state="available")
        )
        with patch(
            "src.web.routes.settings.NotificationService"
        ) as mock_notif_cls:
            mock_notif = MagicMock()
            mock_notif.teardown_bot = AsyncMock(side_effect=Exception("Unexpected"))
            mock_notif_cls.return_value = mock_notif

            resp = await client.post(
                "/settings/notifications/delete",
                follow_redirects=False,
            )
            assert resp.status_code == 303
            assert "error=notification_action_failed" in resp.headers["location"]


@pytest.mark.asyncio
async def test_notification_delete_general_exception_json(client, db):
    """Test notification delete general exception with JSON response (line 1151-1152)."""
    with patch(
        "src.web.routes.settings.deps.get_notification_target_service"
    ) as mock_svc:
        mock_svc.return_value.describe_target = AsyncMock(
            return_value=MagicMock(state="available")
        )
        with patch(
            "src.web.routes.settings.NotificationService"
        ) as mock_notif_cls:
            mock_notif = MagicMock()
            mock_notif.teardown_bot = AsyncMock(side_effect=Exception("Unexpected"))
            mock_notif_cls.return_value = mock_notif

            resp = await client.post(
                "/settings/notifications/delete",
                headers={"Accept": "application/json"},
                follow_redirects=False,
            )
            assert resp.status_code == 500
            data = resp.json()
            assert "Unexpected" in data["error"]


# ── notifications/test: lines 1167, 1169-1170, 1175-1184, 1192 ────────


@pytest.mark.asyncio
async def test_test_notification_no_notifier_no_bot(client, db):
    """Test notification test when no notifier and no bot (lines 1169-1170)."""
    with patch(
        "src.web.routes.settings.deps.get_notifier"
    ) as mock_get_notifier:
        mock_get_notifier.return_value = None

        with patch(
            "src.web.routes.settings.NotificationService"
        ) as mock_notif_cls:
            mock_notif = MagicMock()
            mock_notif.get_status = AsyncMock(return_value=None)
            mock_notif_cls.return_value = mock_notif

            resp = await client.post(
                "/settings/notifications/test",
                follow_redirects=False,
            )
            assert resp.status_code == 303
            assert "error=notification_test_failed" in resp.headers["location"]


@pytest.mark.asyncio
async def test_test_notification_bot_sends_start(client, db):
    """Test notification test sends /start to bot (lines 1174-1184)."""
    mock_bot_status = MagicMock()
    mock_bot_status.bot_username = "test_notify_bot"
    mock_bot_status.tg_user_id = 12345

    mock_notifier = MagicMock()
    mock_notifier.admin_chat_id = None

    mock_client = AsyncMock()

    with patch(
        "src.web.routes.settings.deps.get_notifier"
    ) as mock_get_notifier, patch(
        "src.web.routes.settings.deps.get_notification_target_service"
    ) as mock_target_svc, patch(
        "src.web.routes.settings.NotificationService"
    ) as mock_notif_cls, patch(
        "src.web.routes.settings.Notifier"
    ) as mock_notifier_cls:
        mock_get_notifier.return_value = mock_notifier
        mock_notif = MagicMock()
        mock_notif.get_status = AsyncMock(return_value=mock_bot_status)
        mock_notif_cls.return_value = mock_notif

        mock_target_svc.return_value.use_client.return_value.__aenter__ = AsyncMock(
            return_value=(mock_client, None)
        )
        mock_target_svc.return_value.use_client.return_value.__aexit__ = AsyncMock(
            return_value=None
        )

        mock_notifier_instance = MagicMock()
        mock_notifier_instance.notify = AsyncMock(return_value=True)
        mock_notifier_cls.return_value = mock_notifier_instance

        resp = await client.post(
            "/settings/notifications/test",
            follow_redirects=False,
        )
        assert resp.status_code == 303
        assert "msg=notification_test_sent" in resp.headers["location"]


@pytest.mark.asyncio
async def test_test_notification_notify_fails(client, db):
    """Test notification test when notify returns False (line 1192)."""
    with patch(
        "src.web.routes.settings.deps.get_notifier"
    ) as mock_get_notifier:
        mock_notifier = MagicMock()
        mock_notifier.admin_chat_id = 12345
        mock_get_notifier.return_value = mock_notifier

        with patch(
            "src.web.routes.settings.NotificationService"
        ) as mock_notif_cls:
            mock_notif = MagicMock()
            mock_notif.get_status = AsyncMock(return_value=None)
            mock_notif_cls.return_value = mock_notif

            with patch(
                "src.web.routes.settings.Notifier"
            ) as mock_notifier_cls:
                mock_notifier_instance = MagicMock()
                mock_notifier_instance.notify = AsyncMock(return_value=False)
                mock_notifier_cls.return_value = mock_notifier_instance

                resp = await client.post(
                    "/settings/notifications/test",
                    follow_redirects=False,
                )
                assert resp.status_code == 303
                assert "error=notification_test_failed" in resp.headers["location"]


# ── Image Providers: lines 1202-1262 ───────────────────────────────────


@pytest.mark.asyncio
async def test_add_image_provider_invalid(client, db):
    """Test add image provider with invalid name (lines 1204-1207)."""
    with patch(
        "src.web.routes.settings.ImageProviderService"
    ) as mock_service_cls, patch(
        "src.web.routes.settings.image_provider_spec"
    ) as mock_spec:
        mock_service = MagicMock()
        mock_service.writes_enabled = True
        mock_service_cls.return_value = mock_service
        mock_spec.return_value = None

        resp = await client.post(
            "/settings/image-providers/add",
            data={"provider": "nonexistent"},
            follow_redirects=False,
        )
        assert resp.status_code == 303
        assert "error=image_provider_invalid" in resp.headers["location"]


@pytest.mark.asyncio
async def test_add_image_provider_already_exists(client, db):
    """Test add image provider when it already exists (lines 1208-1209)."""
    with patch(
        "src.web.routes.settings.ImageProviderService"
    ) as mock_service_cls, patch(
        "src.web.routes.settings.image_provider_spec"
    ) as mock_spec:
        mock_service = MagicMock()
        mock_service.writes_enabled = True
        mock_service.load_provider_configs = AsyncMock(
            return_value=[MagicMock(provider="together")]
        )
        mock_service_cls.return_value = mock_service
        mock_spec.return_value = MagicMock()

        resp = await client.post(
            "/settings/image-providers/add",
            data={"provider": "together"},
            follow_redirects=False,
        )
        assert resp.status_code == 303
        assert "msg=image_saved" in resp.headers["location"]


@pytest.mark.asyncio
async def test_save_image_providers_missing_key(client, db):
    """Test save image providers with missing API key (lines 1226-1233)."""
    with patch(
        "src.web.routes.settings.ImageProviderService"
    ) as mock_service_cls, patch(
        "src.web.routes.settings.image_provider_spec"
    ) as mock_spec:
        mock_service = MagicMock()
        mock_service.writes_enabled = True
        mock_cfg = MagicMock()
        mock_cfg.enabled = True
        mock_cfg.provider = "together"
        mock_cfg.api_key = ""
        mock_service.load_provider_configs = AsyncMock(return_value=[])
        mock_service.parse_provider_form = MagicMock(return_value=[mock_cfg])
        mock_service_cls.return_value = mock_service

        mock_spec_obj = MagicMock()
        mock_spec_obj.env_vars = ["TOGETHER_API_KEY"]
        mock_spec.return_value = mock_spec_obj

        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("TOGETHER_API_KEY", None)
            resp = await client.post(
                "/settings/image-providers/save",
                data={},
                follow_redirects=False,
            )
        assert resp.status_code == 303
        assert "error=image_provider_missing_key" in resp.headers["location"]


@pytest.mark.asyncio
async def test_save_image_providers_success(client, db):
    """Test save image providers success (lines 1236-1239)."""
    with patch(
        "src.web.routes.settings.ImageProviderService"
    ) as mock_service_cls:
        mock_service = MagicMock()
        mock_service.writes_enabled = True
        mock_cfg = MagicMock()
        mock_cfg.enabled = False
        mock_service.load_provider_configs = AsyncMock(return_value=[])
        mock_service.parse_provider_form = MagicMock(return_value=[mock_cfg])
        mock_service.save_provider_configs = AsyncMock()
        mock_service_cls.return_value = mock_service

        resp = await client.post(
            "/settings/image-providers/save",
            data={"default_image_model": "test-model"},
            follow_redirects=False,
        )
        assert resp.status_code == 303
        assert "msg=image_saved" in resp.headers["location"]


@pytest.mark.asyncio
async def test_delete_image_provider(client, db):
    """Test delete image provider (lines 1246-1250)."""
    with patch(
        "src.web.routes.settings.ImageProviderService"
    ) as mock_service_cls:
        mock_service = MagicMock()
        mock_service.writes_enabled = True
        mock_service.load_provider_configs = AsyncMock(
            return_value=[MagicMock(provider="together")]
        )
        mock_service.save_provider_configs = AsyncMock()
        mock_service_cls.return_value = mock_service

        resp = await client.post(
            "/settings/image-providers/together/delete",
            follow_redirects=False,
        )
        assert resp.status_code == 303
        assert "msg=image_saved" in resp.headers["location"]


# ── Translation settings: lines 1255-1262, 1267-1269 ──────────────────


@pytest.mark.asyncio
async def test_save_translation_settings(client, db):
    """Test save translation settings (lines 1255-1262)."""
    resp = await client.post(
        "/settings/save-translation",
        data={
            "translation_provider": "openai",
            "translation_model": "gpt-4",
            "translation_target_lang": "EN",
            "translation_source_filter": "ru,ua",
            "translation_auto_on_collect": "1",
        },
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "msg=translation_saved" in resp.headers["location"]

    saved_provider = await db.get_setting("translation_provider")
    assert saved_provider == "openai"
    saved_lang = await db.get_setting("translation_target_lang")
    assert saved_lang == "en"  # .lower() applied


@pytest.mark.asyncio
async def test_translation_backfill(client, db):
    """Test translation backfill (lines 1267-1269)."""
    resp = await client.post(
        "/settings/translation-backfill",
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "msg=translation_backfill_done" in resp.headers["location"]


# ── translation-run: lines 1276-1293 ───────────────────────────────────


@pytest.mark.asyncio
async def test_translation_run_batch(client, db):
    """Test translation run batch (lines 1276-1293)."""
    await db.set_setting("translation_source_filter", "ru,ua")

    resp = await client.post(
        "/settings/translation-run",
        data={"target_lang": "en"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "msg=translation_run_started" in resp.headers["location"]


# ── _reload_llm_providers: lines 80-83 ─────────────────────────────────


@pytest.mark.asyncio
async def test_settings_page_reload_llm_providers_failure(client, db, caplog):
    """Test _reload_llm_providers handles exception (lines 80-83)."""
    mock_llm_svc = MagicMock()
    mock_llm_svc.reload_db_providers = AsyncMock(side_effect=Exception("DB error"))
    client._transport_app.state.llm_provider_service = mock_llm_svc

    with patch(
        "src.web.routes.settings.AgentProviderService.load_provider_configs",
        AsyncMock(return_value=[]),
    ), patch(
        "src.web.routes.settings.AgentProviderService.load_model_cache",
        AsyncMock(return_value={}),
    ), caplog.at_level(
        logging.WARNING, logger="src.web.routes.settings"
    ):
        resp = await client.get("/settings/")
        assert resp.status_code == 200
