import asyncio
import base64
import logging
import re
import tomllib
from datetime import datetime, timezone
from pathlib import Path
from types import MethodType, SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from httpx import ASGITransport, AsyncClient

from src.agent.prompt_template import AGENT_PROMPT_TEMPLATE_SETTING, DEFAULT_AGENT_PROMPT_TEMPLATE
from src.agent.provider_registry import ProviderRuntimeConfig
from src.collection_queue import CollectionQueue
from src.config import AppConfig
from src.database import Database
from src.models import (
    Account,
    CollectionTaskStatus,
    CollectionTaskType,
    Message,
    RuntimeSnapshot,
    StatsAllTaskPayload,
)
from src.scheduler.service import SchedulerManager
from src.search.ai_search import AISearchEngine
from src.search.engine import SearchEngine
from src.services.agent_provider_service import (
    AgentProviderService,
    ProviderModelCacheEntry,
    ProviderModelCompatibilityRecord,
)
from src.services.embedding_service import EmbeddingService
from src.telegram.collector import Collector
from src.web.app import create_app
from src.web.routes.channel_collection import _COLLECT_ALL_BTN, _COLLECT_ALL_FORM
from src.web.session import COOKIE_NAME, create_session_token
from src.web.template_globals import PYPROJECT_PATH, _agent_available_for_request, get_app_version
from tests.helpers import build_web_app, make_auth_client, make_test_config


@pytest.fixture
async def client(tmp_path, real_pool_harness_factory):
    config = make_test_config(tmp_path)
    config.security.session_encryption_key = "test-encryption-key"

    harness = real_pool_harness_factory()
    app, db = await build_web_app(
        config,
        harness,
        add_account="+1234567890",
    )

    async def _no_users(self):
        return []

    async def _resolve_channel(self, identifier):
        return {
            "channel_id": -1001234567890,
            "title": "Resolved Channel",
            "username": identifier.lstrip("@"),
            "channel_type": "channel",
        }

    async def _get_dialogs(self):
        return [
            {
                "channel_id": -100111,
                "title": "Dialog Chan 1",
                "username": "chan1",
                "channel_type": "channel",
            },
            {
                "channel_id": -100222,
                "title": "Dialog Chan 2",
                "username": None,
                "channel_type": "group",
            },
        ]

    async def _get_dialogs_for_phone(
        self,
        phone,
        include_dm=False,
        mode="channels_only",
        refresh=False,
    ):
        return []

    pool = app.state.pool
    pool.get_users_info = MethodType(_no_users, pool)
    pool.resolve_channel = MethodType(_resolve_channel, pool)
    pool.get_dialogs = MethodType(_get_dialogs, pool)
    pool.get_dialogs_for_phone = MethodType(_get_dialogs_for_phone, pool)

    async with make_auth_client(app) as c:
        yield c

    await app.state.collection_queue.shutdown()
    await db.close()


@pytest.mark.anyio
async def test_health_endpoint(client):
    resp = await client.get("/health")
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] in ("healthy", "degraded")
    assert "db" in data
    assert "accounts_connected" in data


@pytest.mark.anyio
async def test_health_endpoint_logs_db_probe_failure(client, monkeypatch, caplog):
    async def _broken_execute(query):
        raise RuntimeError("db unavailable")

    monkeypatch.setattr(client._transport.app.state.db, "execute", _broken_execute)

    with caplog.at_level(logging.WARNING, logger="src.web.assembly"):
        resp = await client.get("/health")

    assert resp.status_code == 200
    assert resp.json()["status"] == "degraded"
    assert "Health check DB probe failed" in caplog.text


@pytest.mark.anyio
async def test_dashboard(client):
    resp = await client.get("/dashboard/")
    assert resp.status_code == 200
    assert "Панель" in resp.text


def test_templates_have_actual_app_version():
    app = create_app(AppConfig())
    expected_version = tomllib.loads(
        PYPROJECT_PATH.read_text(encoding="utf-8"),
    )[
        "project"
    ]["version"]
    assert app.state.templates.env.globals["app_version"] == expected_version
    assert get_app_version() == expected_version


def test_agent_available_uses_embedded_worker_manager(monkeypatch):
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    monkeypatch.delenv("CLAUDE_CODE_OAUTH_TOKEN", raising=False)
    monkeypatch.delenv("AGENT_FALLBACK_MODEL", raising=False)

    request = SimpleNamespace(
        app=SimpleNamespace(
            state=SimpleNamespace(
                config=AppConfig(),
                embedded_worker=SimpleNamespace(
                    _ready_event=SimpleNamespace(is_set=lambda: True),
                    container=SimpleNamespace(agent_manager=SimpleNamespace(available=True)),
                ),
            )
        )
    )

    assert _agent_available_for_request(request) is True


def test_agent_available_ignores_embedded_worker_before_ready(monkeypatch):
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    monkeypatch.delenv("CLAUDE_CODE_OAUTH_TOKEN", raising=False)
    monkeypatch.delenv("AGENT_FALLBACK_MODEL", raising=False)

    request = SimpleNamespace(
        app=SimpleNamespace(
            state=SimpleNamespace(
                config=AppConfig(),
                embedded_worker=SimpleNamespace(
                    _ready_event=SimpleNamespace(is_set=lambda: False),
                    container=SimpleNamespace(agent_manager=SimpleNamespace(available=True)),
                ),
            )
        )
    )

    assert _agent_available_for_request(request) is False


def test_agent_available_ignores_invalid_fallback_model(monkeypatch):
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    monkeypatch.delenv("CLAUDE_CODE_OAUTH_TOKEN", raising=False)
    monkeypatch.setenv("AGENT_FALLBACK_MODEL", "llama3")

    request = SimpleNamespace(app=SimpleNamespace(state=SimpleNamespace(config=AppConfig())))

    assert _agent_available_for_request(request) is False


@pytest.mark.anyio
async def test_footer_renders_actual_version(client):
    expected_version = tomllib.loads(
        PYPROJECT_PATH.read_text(encoding="utf-8"),
    )[
        "project"
    ]["version"]
    resp = await client.get("/dashboard/")
    assert resp.status_code == 200
    assert f"TG Agent v{expected_version}" in resp.text


@pytest.mark.anyio
async def test_login_page(client):
    resp = await client.get("/auth/login")
    assert resp.status_code == 200
    assert "/settings" in resp.text


@pytest.mark.anyio
async def test_web_login_page_without_auth(unauth_client):
    resp = await unauth_client.get("/login", follow_redirects=False)
    assert resp.status_code == 200
    assert "Вход в веб-панель" in resp.text
    assert 'action="/login"' in resp.text


@pytest.mark.anyio
async def test_web_login_rejects_unsafe_next_without_auth(unauth_client):
    resp = await unauth_client.get(
        "/login?next=https://evil.example",
        follow_redirects=False,
    )
    assert resp.status_code == 200
    assert 'name="next" value="/"' in resp.text


@pytest.mark.anyio
async def test_settings_page(client):
    resp = await client.get("/settings/")
    assert resp.status_code == 200
    assert "Аккаунт для уведомлений" in resp.text
    assert "Semantic Search & Embeddings" in resp.text
    assert "Режим разработчика" in resp.text
    assert "Я понимаю, что включаю потенциально опасные изменения" in resp.text
    assert "Backend override" not in resp.text
    assert 'src="/static/settings.js' in resp.text
    assert "activateTabFromHash" not in resp.text


@pytest.mark.anyio
async def test_settings_static_js_handles_pane_hashes(client):
    resp = await client.get("/static/settings.js")
    assert resp.status_code == 200
    assert "settingsTabIdFromHash" in resp.text
    assert "replace(/^pane-/, '')" in resp.text
    assert "hashchange" in resp.text


@pytest.mark.anyio
async def test_settings_page_image_providers_tab(client):
    resp = await client.get("/settings/")
    assert resp.status_code == 200
    assert "Image Generation Providers" in resp.text
    assert "Изображения" in resp.text  # tab label


@pytest.mark.anyio
async def test_add_image_provider(client):
    resp = await client.post(
        "/settings/image-providers/add",
        data={"provider": "together"},
    )
    assert resp.status_code == 200
    # Verify provider was added by checking settings page
    resp2 = await client.get("/settings/")
    assert "Together AI" in resp2.text


@pytest.mark.anyio
async def test_add_image_provider_invalid(client):
    resp = await client.post(
        "/settings/image-providers/add",
        data={"provider": "nonexistent"},
    )
    assert resp.status_code == 200
    assert "error=image_provider_invalid" in str(resp.url)


@pytest.mark.anyio
async def test_save_image_providers(client):
    # First add a provider
    await client.post("/settings/image-providers/add", data={"provider": "openai"})
    # Then save with a key
    resp = await client.post(
        "/settings/image-providers/save",
        data={
            "img_provider_present__openai": "1",
            "img_provider_enabled__openai": "1",
            "img_provider_secret__openai__api_key": "sk-test",
        },
    )
    assert resp.status_code == 200


@pytest.mark.anyio
async def test_delete_image_provider(client):
    await client.post("/settings/image-providers/add", data={"provider": "replicate"})
    resp = await client.post("/settings/image-providers/replicate/delete")
    assert resp.status_code == 200


@pytest.mark.anyio
async def test_images_page_shows_db_configured_provider(client):
    """Provider added via Settings should appear on /images page."""
    # Add provider with API key via settings
    await client.post("/settings/image-providers/add", data={"provider": "replicate"})
    await client.post(
        "/settings/image-providers/save",
        data={
            "img_provider_present__replicate": "1",
            "img_provider_enabled__replicate": "1",
            "img_provider_secret__replicate__api_key": "r8_test_key",
        },
    )
    # Verify it shows up on /images
    resp = await client.get("/images/")
    assert resp.status_code == 200
    assert "replicate" in resp.text


@pytest.mark.anyio
async def test_startup_continues_after_pool_initialize_timeout(tmp_path, db, caplog, monkeypatch):
    """start_container proceeds when pool.initialize() hangs past the timeout."""
    import asyncio
    from types import SimpleNamespace

    from src.web.bootstrap import start_container

    # Shrink timeout so test runs fast
    monkeypatch.setattr("src.web.bootstrap._POOL_INIT_TIMEOUT", 0.1)

    async def _hang():
        await asyncio.sleep(9999)

    pool = SimpleNamespace(initialize=_hang)
    auth = SimpleNamespace(is_configured=True)
    channel_bundle = SimpleNamespace(
        fail_running_collection_tasks_on_startup=AsyncMock(return_value=0),
    )
    photo_task_service = SimpleNamespace(recover_running=AsyncMock(return_value=0))
    ai_search = SimpleNamespace(initialize=lambda: None)
    scheduler = SimpleNamespace(load_settings=AsyncMock(), start=AsyncMock())
    gen_runs = SimpleNamespace(reset_running_on_startup=AsyncMock(return_value=0))
    tg_cmds = SimpleNamespace(reset_running_on_startup=AsyncMock(return_value=0))

    container = SimpleNamespace(
        auth=auth,
        pool=pool,
        channel_bundle=channel_bundle,
        photo_task_service=photo_task_service,
        db=SimpleNamespace(
            repos=SimpleNamespace(generation_runs=gen_runs, telegram_commands=tg_cmds),
            get_setting=AsyncMock(return_value=None),
        ),
        collection_queue=None,
        unified_dispatcher=None,
        ai_search=ai_search,
        agent_manager=None,
        scheduler=scheduler,
    )

    with caplog.at_level("WARNING"):
        await asyncio.wait_for(start_container(container), timeout=5)

    assert "telegram pool timed out" in caplog.text


@pytest.mark.anyio
async def test_settings_page_hides_credentials_form_when_env_credentials_configured(
    client, monkeypatch
):
    monkeypatch.setenv("TG_API_ID", "12345")
    monkeypatch.setenv("TG_API_HASH", "env-hash")

    resp = await client.get("/settings/")

    assert resp.status_code == 200
    assert "Управляется через окружение" in resp.text
    assert 'action="/settings/save-credentials"' not in resp.text
    assert "Telegram-аккаунты" in resp.text
    assert 'href="/auth/login"' in resp.text
    assert "Добавить аккаунт" in resp.text
    template_text = Path("src/web/templates/settings.html").read_text(encoding="utf-8")
    assert "_accounts.html" in template_text
    assert "_scheduler.html" in template_text
    assert template_text.index("_accounts.html") < template_text.index("_scheduler.html")


@pytest.mark.anyio
async def test_settings_page_keeps_credentials_form_for_invalid_env_api_id(client, monkeypatch):
    monkeypatch.setenv("TG_API_ID", "not-a-number")
    monkeypatch.setenv("TG_API_HASH", "env-hash")

    resp = await client.get("/settings/")

    assert resp.status_code == 200
    assert "Управляется через окружение" not in resp.text
    assert 'action="/settings/save-credentials"' in resp.text


@pytest.mark.anyio
async def test_settings_page_ignores_invalid_persisted_numeric_settings(client):
    db = client._transport.app.state.db
    await db.set_setting("min_subscribers_filter", "broken")
    await db.set_setting("collect_interval_minutes", "oops")

    resp = await client.get("/settings/")

    assert resp.status_code == 200
    assert 'name="min_subscribers_filter"' in resp.text
    assert 'value="0"' in resp.text
    assert 'name="collect_interval_minutes"' in resp.text
    assert 'value="60"' in resp.text


@pytest.mark.anyio
async def test_settings_save_semantic_persists_values_and_resets_index(client):
    db = client._transport.app.state.db
    await db.insert_messages_batch(
        [
            Message(
                channel_id=-100444,
                message_id=1,
                text="Semantic reset test",
                date=datetime.now(timezone.utc),
            )
        ]
    )
    rows = await db.execute_fetchall("SELECT id FROM messages ORDER BY id")
    await db.repos.messages.upsert_message_embeddings([(int(rows[0]["id"]), [1.0, 0.0])])
    await db.set_setting("semantic_last_embedded_id", "1")

    resp = await client.post(
        "/settings/save-semantic-search",
        data={
            "semantic_embeddings_provider": "openai",
            "semantic_embeddings_model": "text-embedding-3-small",
            "semantic_embeddings_base_url": "https://api.openai.com/v1",
            "semantic_embeddings_api_key": "secret-key",
            "semantic_embeddings_batch_size": "32",
            "semantic_reset_index": "1",
        },
        follow_redirects=False,
    )

    assert resp.status_code == 303
    assert await db.get_setting("semantic_embeddings_provider") == "openai"
    assert await db.get_setting("semantic_embeddings_model") == "text-embedding-3-small"
    assert await db.get_setting("semantic_embeddings_base_url") == "https://api.openai.com/v1"
    assert await db.get_setting("semantic_embeddings_api_key") == "secret-key"
    assert await db.get_setting("semantic_embeddings_batch_size") == "32"
    assert await db.get_setting("semantic_last_embedded_id") is None


@pytest.mark.anyio
async def test_settings_semantic_index_runs_embedding_service(client, monkeypatch):
    monkeypatch.setattr(
        EmbeddingService,
        "index_pending_messages",
        AsyncMock(return_value=7),
    )

    resp = await client.post("/settings/semantic-index", data={}, follow_redirects=False)

    assert resp.status_code == 303
    assert "msg=semantic_indexed" in resp.headers["location"]
    assert "indexed=7" in resp.headers["location"]


@pytest.mark.anyio
async def test_settings_save_agent_persists_dev_mode_and_override(client):
    db = client._transport.app.state.db
    # Need a valid provider config for deepagents override to be accepted
    import json

    await db.set_setting(
        "agent_deepagents_providers_v1",
        json.dumps([{
            "provider": "ollama", "enabled": True, "priority": 0,
            "selected_model": "llama3.2", "plain_fields": {"base_url": ""},
            "secret_fields_enc": {}, "last_validation_error": "",
        }]),
    )

    resp = await client.post(
        "/settings/save-agent",
        data={
            "agent_form_scope": "dev_mode",
            "agent_dev_mode_enabled": "1",
            "agent_dev_mode_disclaimer": "1",
            "agent_backend_override": "deepagents",
        },
        follow_redirects=False,
    )

    assert resp.status_code == 303
    assert await db.get_setting("agent_dev_mode_enabled") == "1"
    assert await db.get_setting("agent_backend_override") == "deepagents"


@pytest.mark.anyio
async def test_settings_page_shows_ai_agent_block_only_in_dev_mode(client):
    db = client._transport.app.state.db
    await db.set_setting("agent_dev_mode_enabled", "1")
    await db.set_setting("agent_backend_override", "deepagents")

    resp = await client.get("/settings/")

    assert resp.status_code == 200
    assert "Режим разработчика" in resp.text
    assert "Backend override" in resp.text
    assert "Deepagents Providers" in resp.text
    assert "Тестировать все модели" in resp.text
    assert 'id="bulk-test-agent-providers-btn"' in resp.text
    assert 'id="agent-provider-actions-status"' in resp.text
    assert 'name="agent_backend_override"' in resp.text
    assert 'name="agent_prompt_template"' in resp.text
    assert 'name="agent_form_scope" value="dev_mode"' in resp.text
    assert 'name="agent_form_scope" value="backend_override"' in resp.text
    assert 'name="agent_form_scope" value="prompt_template"' in resp.text


@pytest.mark.anyio
async def test_settings_save_agent_preserves_override_when_toggling_dev_mode_only(client):
    db = client._transport.app.state.db
    await db.set_setting("agent_backend_override", "deepagents")
    import json

    await db.set_setting(
        "agent_deepagents_providers_v1",
        json.dumps([{
            "provider": "ollama", "enabled": True, "priority": 0,
            "selected_model": "llama3.2", "plain_fields": {"base_url": ""},
            "secret_fields_enc": {}, "last_validation_error": "",
        }]),
    )

    resp = await client.post(
        "/settings/save-agent",
        data={
            "agent_form_scope": "dev_mode",
            "agent_dev_mode_enabled": "1",
            "agent_dev_mode_disclaimer": "1",
        },
        follow_redirects=False,
    )

    assert resp.status_code == 303
    assert await db.get_setting("agent_dev_mode_enabled") == "1"
    assert await db.get_setting("agent_backend_override") == "deepagents"


@pytest.mark.anyio
async def test_settings_save_agent_requires_disclaimer_to_enable_dev_mode(client):
    db = client._transport.app.state.db

    resp = await client.post(
        "/settings/save-agent",
        data={"agent_form_scope": "dev_mode", "agent_dev_mode_enabled": "1"},
        follow_redirects=False,
    )

    assert resp.status_code == 303
    assert await db.get_setting("agent_dev_mode_enabled") == "0"


@pytest.mark.anyio
async def test_settings_save_agent_backend_override_keeps_dev_mode_enabled(client):
    db = client._transport.app.state.db
    await db.set_setting("agent_dev_mode_enabled", "1")
    await db.set_setting("agent_backend_override", "auto")
    # Need a valid provider config for deepagents override to be accepted
    import json

    await db.set_setting(
        "agent_deepagents_providers_v1",
        json.dumps([{
            "provider": "ollama", "enabled": True, "priority": 0,
            "selected_model": "llama3.2", "plain_fields": {"base_url": ""},
            "secret_fields_enc": {}, "last_validation_error": "",
        }]),
    )

    resp = await client.post(
        "/settings/save-agent",
        data={
            "agent_form_scope": "backend_override",
            "agent_backend_override": "deepagents",
        },
        follow_redirects=False,
    )

    assert resp.status_code == 303
    assert await db.get_setting("agent_dev_mode_enabled") == "1"
    assert await db.get_setting("agent_backend_override") == "deepagents"


@pytest.mark.anyio
async def test_settings_save_agent_can_disable_dev_mode_without_disclaimer(client):
    db = client._transport.app.state.db
    await db.set_setting("agent_dev_mode_enabled", "1")
    await db.set_setting("agent_backend_override", "deepagents")

    resp = await client.post(
        "/settings/save-agent",
        data={"agent_form_scope": "dev_mode"},
        follow_redirects=False,
    )

    assert resp.status_code == 303
    assert await db.get_setting("agent_dev_mode_enabled") == "0"
    # Override is reset to "auto" when dev mode is disabled
    assert await db.get_setting("agent_backend_override") == "auto"


@pytest.mark.anyio
async def test_settings_save_agent_persists_prompt_template(client):
    db = client._transport.app.state.db
    template = "Канал: {channel_title}\nТема: {topic}\nДата: {date}\n{source_messages}"

    resp = await client.post(
        "/settings/save-agent",
        data={
            "agent_form_scope": "prompt_template",
            "agent_prompt_template": template,
        },
        follow_redirects=False,
    )

    assert resp.status_code == 303
    assert await db.get_setting(AGENT_PROMPT_TEMPLATE_SETTING) == template


@pytest.mark.anyio
async def test_settings_save_agent_rejects_invalid_prompt_template(client):
    db = client._transport.app.state.db
    await db.set_setting(AGENT_PROMPT_TEMPLATE_SETTING, "Канал: {channel_title}")

    resp = await client.post(
        "/settings/save-agent",
        data={
            "agent_form_scope": "prompt_template",
            "agent_prompt_template": "Канал: {unknown}",
        },
        follow_redirects=False,
    )

    assert resp.status_code == 303
    assert "error=agent_prompt_template_invalid" in resp.headers["location"]
    assert await db.get_setting(AGENT_PROMPT_TEMPLATE_SETTING) == "Канал: {channel_title}"


@pytest.mark.anyio
async def test_settings_save_agent_blank_prompt_template_resets_to_default(client):
    db = client._transport.app.state.db
    await db.set_setting(AGENT_PROMPT_TEMPLATE_SETTING, "Канал: {channel_title}")

    resp = await client.post(
        "/settings/save-agent",
        data={
            "agent_form_scope": "prompt_template",
            "agent_prompt_template": "   ",
        },
        follow_redirects=False,
    )

    assert resp.status_code == 303
    assert await db.get_setting(AGENT_PROMPT_TEMPLATE_SETTING) == DEFAULT_AGENT_PROMPT_TEMPLATE


@pytest.mark.anyio
async def test_settings_backend_override_submit_keeps_ai_agent_block_visible(client):
    db = client._transport.app.state.db
    await db.set_setting("agent_dev_mode_enabled", "1")
    await db.set_setting("agent_backend_override", "auto")

    resp = await client.post(
        "/settings/save-agent",
        data={
            "agent_form_scope": "backend_override",
            "agent_backend_override": "deepagents",
        },
        follow_redirects=False,
    )

    assert resp.status_code == 303

    page = await client.get("/settings/")
    assert page.status_code == 200
    assert "Backend override" in page.text


@pytest.mark.anyio
async def test_settings_add_agent_provider_requires_dev_mode(client):
    db = client._transport.app.state.db

    resp = await client.post(
        "/settings/agent-providers/add",
        data={"provider": "openai"},
        follow_redirects=False,
    )

    assert resp.status_code == 303
    assert "agent_dev_mode_required" in resp.headers["location"]
    assert await db.get_setting("agent_deepagents_providers_v1") is None


@pytest.mark.anyio
async def test_settings_add_agent_provider_persists_provider_in_db(client):
    db = client._transport.app.state.db
    await db.set_setting("agent_dev_mode_enabled", "1")

    resp = await client.post(
        "/settings/agent-providers/add",
        data={"provider": "openai"},
        follow_redirects=False,
    )

    assert resp.status_code == 303
    raw = await db.get_setting("agent_deepagents_providers_v1")
    assert raw is not None
    assert "openai" in raw


@pytest.mark.anyio
async def test_settings_save_agent_providers_preserves_priority_order(client, monkeypatch):
    db = client._transport.app.state.db
    await db.set_setting("agent_dev_mode_enabled", "1")
    from src.web.settings import handlers as settings_handlers

    probe_mock = AsyncMock()
    fake_manager = SimpleNamespace(refresh_settings_cache=AsyncMock())
    monkeypatch.setattr(settings_handlers, "_probe_provider_config", probe_mock)
    monkeypatch.setattr(
        settings_handlers, "_settings_agent_manager", lambda request: (fake_manager, False)
    )

    await client.post(
        "/settings/agent-providers/add", data={"provider": "openai"}, follow_redirects=False
    )
    await client.post(
        "/settings/agent-providers/add", data={"provider": "anthropic"}, follow_redirects=False
    )

    resp = await client.post(
        "/settings/agent-providers/save",
        data={
            "provider_present__openai": "1",
            "provider_priority__openai": "1",
            "provider_enabled__openai": "1",
            "provider_model__openai": "gpt-4.1-mini",
            "provider_secret__openai__api_key": "openai-key",
            "provider_present__anthropic": "1",
            "provider_priority__anthropic": "0",
            "provider_enabled__anthropic": "1",
            "provider_model__anthropic": "claude-sonnet-4-6",
            "provider_secret__anthropic__api_key": "anthropic-key",
        },
        follow_redirects=False,
    )

    assert resp.status_code == 303
    raw = await db.get_setting("agent_deepagents_providers_v1")
    assert raw is not None
    assert raw.index('"provider": "anthropic"') < raw.index('"provider": "openai"')


@pytest.mark.anyio
async def test_settings_save_agent_providers_skips_probe_for_disabled_provider(client, monkeypatch):
    db = client._transport.app.state.db
    await db.set_setting("agent_dev_mode_enabled", "1")
    await client.post(
        "/settings/agent-providers/add", data={"provider": "openai"}, follow_redirects=False
    )
    from src.web.settings import handlers as settings_handlers

    probe_mock = AsyncMock()
    fake_manager = SimpleNamespace(refresh_settings_cache=AsyncMock())
    monkeypatch.setattr(settings_handlers, "_probe_provider_config", probe_mock)
    monkeypatch.setattr(
        settings_handlers, "_settings_agent_manager", lambda request: (fake_manager, False)
    )

    resp = await client.post(
        "/settings/agent-providers/save",
        data={
            "provider_present__openai": "1",
            "provider_priority__openai": "0",
            "provider_enabled__openai": "",
            "provider_model__openai": "gpt-4.1-mini",
            "provider_secret__openai__api_key": "openai-key",
        },
        follow_redirects=False,
    )

    assert resp.status_code == 303
    probe_mock.assert_not_awaited()
    service = AgentProviderService(db, client._transport.app.state.config)
    configs = await service.load_provider_configs()
    assert configs[0].enabled is False
    assert configs[0].last_validation_error == ""


@pytest.mark.anyio
async def test_settings_refresh_agent_provider_models_returns_json(client, monkeypatch):
    db = client._transport.app.state.db
    await db.set_setting("agent_dev_mode_enabled", "1")
    await client.post(
        "/settings/agent-providers/add", data={"provider": "openai"}, follow_redirects=False
    )

    async def _live_fetch(self, spec, cfg):
        return ["gpt-4.1", "gpt-4.1-mini"]

    monkeypatch.setattr(
        "src.services.agent_provider_service.AgentProviderService._fetch_live_models",
        _live_fetch,
    )

    resp = await client.post("/settings/agent-providers/openai/refresh")

    assert resp.status_code == 200
    payload = resp.json()
    assert payload["ok"] is True
    assert payload["source"] == "live"
    assert "gpt-4.1-mini" in payload["models"]
    assert "compatibility" in payload


@pytest.mark.anyio
async def test_settings_refresh_agent_provider_models_uses_unsaved_form_values(client, monkeypatch):
    db = client._transport.app.state.db
    await db.set_setting("agent_dev_mode_enabled", "1")
    await client.post(
        "/settings/agent-providers/add", data={"provider": "openai"}, follow_redirects=False
    )

    seen_cfg = None

    async def _live_fetch(self, spec, cfg):
        nonlocal seen_cfg
        seen_cfg = cfg
        return ["gpt-4.1-mini"]

    monkeypatch.setattr(
        "src.services.agent_provider_service.AgentProviderService._fetch_live_models",
        _live_fetch,
    )

    resp = await client.post(
        "/settings/agent-providers/openai/refresh",
        data={
            "provider_present__openai": "1",
            "provider_priority__openai": "0",
            "provider_enabled__openai": "1",
            "provider_model__openai": "gpt-4.1-mini",
            "provider_field__openai__base_url": "https://example.invalid/v1",
            "provider_secret__openai__api_key": "unsaved-openai-key",
        },
    )

    assert resp.status_code == 200
    assert seen_cfg is not None
    assert seen_cfg.plain_fields["base_url"] == "https://example.invalid/v1"
    assert seen_cfg.secret_fields["api_key"] == "unsaved-openai-key"


@pytest.mark.anyio
async def test_settings_page_refresh_provider_posts_unsaved_form_data(client):
    db = client._transport.app.state.db
    await db.set_setting("agent_dev_mode_enabled", "1")
    await client.post(
        "/settings/agent-providers/add", data={"provider": "openai"}, follow_redirects=False
    )

    resp = await client.get("/settings/")

    assert resp.status_code == 200
    assert 'src="/static/settings.js' in resp.text
    assert 'data-provider-card="openai"' in resp.text
    assert 'data-provider-refresh-btn="openai"' in resp.text


@pytest.mark.anyio
async def test_settings_refresh_agent_provider_models_preserves_saved_values_when_form_empty(
    client, monkeypatch
):
    db = client._transport.app.state.db
    await db.set_setting("agent_dev_mode_enabled", "1")
    service = AgentProviderService(db, client._transport.app.state.config)
    await service.save_provider_configs(
        [
            ProviderRuntimeConfig(
                provider="openai",
                enabled=True,
                priority=3,
                selected_model="gpt-4.1-mini",
                plain_fields={"base_url": "https://saved.example/v1"},
                secret_fields={"api_key": "saved-openai-key"},
            )
        ]
    )

    seen_cfg = None

    async def _live_fetch(self, spec, cfg):
        nonlocal seen_cfg
        seen_cfg = cfg
        return ["gpt-4.1-mini"]

    monkeypatch.setattr(
        "src.services.agent_provider_service.AgentProviderService._fetch_live_models",
        _live_fetch,
    )

    resp = await client.post("/settings/agent-providers/openai/refresh")

    assert resp.status_code == 200
    assert seen_cfg is not None
    assert seen_cfg.priority == 3
    assert seen_cfg.enabled is True
    assert seen_cfg.selected_model == "gpt-4.1-mini"
    assert seen_cfg.plain_fields["base_url"] == "https://saved.example/v1"
    assert seen_cfg.secret_fields["api_key"] == "saved-openai-key"


@pytest.mark.anyio
async def test_settings_refresh_all_agent_provider_models_requires_dev_mode(client):
    resp = await client.post("/settings/agent-providers/refresh-all")

    assert resp.status_code == 403
    assert resp.json()["ok"] is False
    assert resp.json()["error"] == "Developer mode is required."


@pytest.mark.anyio
async def test_settings_refresh_all_agent_provider_models_uses_unsaved_form_values(
    client, monkeypatch
):
    db = client._transport.app.state.db
    await db.set_setting("agent_dev_mode_enabled", "1")
    service = AgentProviderService(db, client._transport.app.state.config)
    await service.save_provider_configs(
        [
            ProviderRuntimeConfig(
                provider="openai",
                enabled=True,
                priority=0,
                selected_model="gpt-4.1-mini",
                plain_fields={"base_url": "https://saved.example/v1"},
                secret_fields={"api_key": "saved-openai-key"},
            )
        ]
    )

    seen_cfg = None

    async def _fake_refresh(self, provider_name, cfg=None):
        nonlocal seen_cfg
        assert provider_name == "openai"
        seen_cfg = cfg
        return ProviderModelCacheEntry(
            provider=provider_name,
            models=["gpt-4.1-mini"],
            source="live",
            fetched_at="2026-03-12T00:00:00+00:00",
        )

    monkeypatch.setattr(AgentProviderService, "refresh_models_for_provider", _fake_refresh)

    resp = await client.post(
        "/settings/agent-providers/refresh-all",
        data={
            "provider_present__openai": "1",
            "provider_priority__openai": "0",
            "provider_enabled__openai": "1",
            "provider_model__openai": "gpt-4.1-mini",
            "provider_field__openai__base_url": "https://unsaved.example/v1",
            "provider_secret__openai__api_key": "unsaved-openai-key",
        },
    )

    assert resp.status_code == 200
    assert seen_cfg is not None
    assert seen_cfg.plain_fields["base_url"] == "https://unsaved.example/v1"
    assert seen_cfg.secret_fields["api_key"] == "unsaved-openai-key"


@pytest.mark.anyio
async def test_settings_page_renders_selected_model_compatibility(client):
    db = client._transport.app.state.db
    await db.set_setting("agent_dev_mode_enabled", "1")
    service = AgentProviderService(db, client._transport.app.state.config)
    cfg = ProviderRuntimeConfig(
        provider="openai",
        enabled=True,
        priority=0,
        selected_model="gpt-4.1-mini",
        secret_fields={"api_key": "openai-key"},
    )
    await service.save_provider_configs([cfg])
    fingerprint = service.config_fingerprint(cfg)
    await service.save_model_cache(
        {
            "openai": ProviderModelCacheEntry(
                provider="openai",
                models=["gpt-4.1-mini"],
                source="live",
                compatibility={
                    fingerprint: ProviderModelCompatibilityRecord(
                        model="gpt-4.1-mini",
                        status="supported",
                        tested_at="2026-03-12T00:00:00+00:00",
                        config_fingerprint=fingerprint,
                        probe_kind="auto-select",
                    )
                },
            )
        }
    )

    resp = await client.get("/settings/")

    assert resp.status_code == 200
    assert "compatibility:" in resp.text
    assert "gpt-4.1-mini [supported]" in resp.text


@pytest.mark.anyio
async def test_settings_probe_agent_provider_model_returns_cached_json(client, monkeypatch):
    db = client._transport.app.state.db
    await db.set_setting("agent_dev_mode_enabled", "1")
    await client.post(
        "/settings/agent-providers/add", data={"provider": "openai"}, follow_redirects=False
    )
    from src.web.settings import handlers as settings_handlers

    fake_manager = SimpleNamespace(
        probe_provider_config=AsyncMock(
            return_value=ProviderModelCompatibilityRecord(
                model="gpt-4.1-mini",
                status="supported",
                tested_at="2026-03-12T00:00:00+00:00",
                config_fingerprint="probe-fingerprint",
                probe_kind="auto-select",
            )
        )
    )
    monkeypatch.setattr(
        settings_handlers, "_settings_agent_manager", lambda request: (fake_manager, False)
    )

    resp = await client.post(
        "/settings/agent-providers/openai/probe",
        data={
            "provider_present__openai": "1",
            "provider_priority__openai": "0",
            "provider_enabled__openai": "1",
            "provider_model__openai": "gpt-4.1-mini",
            "provider_secret__openai__api_key": "openai-key",
        },
    )

    assert resp.status_code == 200
    payload = resp.json()
    assert payload["ok"] is True
    assert payload["status"] == "supported"
    service = AgentProviderService(db, client._transport.app.state.config)
    cache = await service.load_model_cache()
    assert any(record.status == "supported" for record in cache["openai"].compatibility.values())


@pytest.mark.anyio
async def test_settings_save_agent_providers_keeps_unsupported_probe_in_cache_only(
    client, monkeypatch
):
    db = client._transport.app.state.db
    await db.set_setting("agent_dev_mode_enabled", "1")
    await client.post(
        "/settings/agent-providers/add", data={"provider": "openai"}, follow_redirects=False
    )
    from src.web.settings import handlers as settings_handlers

    fake_manager = SimpleNamespace(
        probe_provider_config=AsyncMock(
            return_value=ProviderModelCompatibilityRecord(
                model="gpt-4.1-mini",
                status="unsupported",
                reason="tool-calling is broken",
                tested_at="2026-03-12T00:00:00+00:00",
                config_fingerprint="probe-fingerprint",
                probe_kind="save-time",
            )
        ),
        refresh_settings_cache=AsyncMock(),
    )
    monkeypatch.setattr(
        settings_handlers, "_settings_agent_manager", lambda request: (fake_manager, False)
    )

    resp = await client.post(
        "/settings/agent-providers/save",
        data={
            "provider_present__openai": "1",
            "provider_priority__openai": "0",
            "provider_enabled__openai": "1",
            "provider_model__openai": "gpt-4.1-mini",
            "provider_secret__openai__api_key": "openai-key",
        },
        follow_redirects=False,
    )

    assert resp.status_code == 303
    service = AgentProviderService(db, client._transport.app.state.config)
    configs = await service.load_provider_configs()
    assert configs[0].last_validation_error == ""
    cache = await service.load_model_cache()
    assert any(record.status == "unsupported" for record in cache["openai"].compatibility.values())


@pytest.mark.anyio
async def test_settings_bulk_test_requires_dev_mode(client):
    resp = await client.post("/settings/agent-providers/test-all")

    assert resp.status_code == 403
    assert resp.json()["ok"] is False


@pytest.mark.anyio
async def test_settings_bulk_test_uses_unsaved_form_values(client, monkeypatch):
    db = client._transport.app.state.db
    await db.set_setting("agent_dev_mode_enabled", "1")
    service = AgentProviderService(db, client._transport.app.state.config)
    await service.save_provider_configs(
        [
            ProviderRuntimeConfig(
                provider="openai",
                enabled=True,
                priority=0,
                selected_model="gpt-4.1-mini",
                plain_fields={"base_url": "https://saved.example/v1"},
                secret_fields={"api_key": "saved-openai-key"},
            )
        ]
    )
    from src.web.settings import handlers as settings_handlers

    captured_configs: list[ProviderRuntimeConfig] = []
    started = asyncio.Event()

    async def _fake_run_bulk_test_job(request, configs=None):
        del request
        if configs is not None:
            captured_configs.extend(configs)
        started.set()

    monkeypatch.setattr(settings_handlers, "_run_bulk_test_job", _fake_run_bulk_test_job)

    resp = await client.post(
        "/settings/agent-providers/test-all",
        data={
            "provider_present__openai": "1",
            "provider_priority__openai": "0",
            "provider_enabled__openai": "1",
            "provider_model__openai": "gpt-4.1-mini",
            "provider_field__openai__base_url": "https://unsaved.example/v1",
            "provider_secret__openai__api_key": "unsaved-openai-key",
        },
    )

    assert resp.status_code == 200
    await asyncio.wait_for(started.wait(), timeout=1)
    assert captured_configs
    assert captured_configs[0].plain_fields["base_url"] == "https://unsaved.example/v1"
    assert captured_configs[0].secret_fields["api_key"] == "unsaved-openai-key"


@pytest.mark.anyio
async def test_settings_bulk_test_clears_running_status_when_startup_raises(client, monkeypatch):
    db = client._transport.app.state.db
    await db.set_setting("agent_dev_mode_enabled", "1")
    from src.web.settings import handlers as settings_handlers

    request = SimpleNamespace(app=client._transport.app, state=SimpleNamespace())

    def _broken_logger_info(*args, **kwargs):
        raise RuntimeError("log sink unavailable")

    monkeypatch.setattr(settings_handlers.logger, "info", _broken_logger_info)

    await settings_handlers._run_bulk_test_job(request, configs=[])

    status = settings_handlers._bulk_test_status_payload(request)
    assert status["running"] is False
    assert status["error"] == "log sink unavailable"


@pytest.mark.anyio
async def test_settings_bulk_test_refreshes_each_provider_once(client, monkeypatch, tmp_path):
    db = client._transport.app.state.db
    await db.set_setting("agent_dev_mode_enabled", "1")
    service = AgentProviderService(db, client._transport.app.state.config)
    await service.save_provider_configs(
        [
            ProviderRuntimeConfig(
                provider="openai",
                enabled=True,
                priority=0,
                selected_model="gpt-4.1-mini",
                secret_fields={"api_key": "openai-key"},
            )
        ]
    )
    from src.web.settings import handlers as settings_handlers

    refresh_calls: list[str] = []
    request = SimpleNamespace(app=client._transport.app, state=SimpleNamespace())
    probe_mock = AsyncMock(
        return_value=ProviderModelCompatibilityRecord(
            model="gpt-4.1-mini",
            status="supported",
            tested_at="2026-03-12T00:00:01+00:00",
            config_fingerprint="probe-fingerprint",
            probe_kind="dev-bulk",
        )
    )
    export_path = tmp_path / "compat_catalog.json"

    async def _fake_refresh_models_for_provider(self, provider_name, cfg=None):
        del cfg
        refresh_calls.append(provider_name)
        return ProviderModelCacheEntry(
            provider=provider_name,
            models=["gpt-4.1-mini"],
            source="live",
            fetched_at="2026-03-12T00:00:00+00:00",
        )

    async def _fake_export_catalog(self, configs, cache=None, *, path=None):
        del configs, cache, path
        export_path.write_text('{"providers": []}', encoding="utf-8")
        return export_path

    monkeypatch.setattr(
        AgentProviderService,
        "refresh_models_for_provider",
        _fake_refresh_models_for_provider,
    )
    monkeypatch.setattr(settings_handlers, "_probe_provider_config", probe_mock)
    monkeypatch.setattr(
        AgentProviderService,
        "export_compatibility_catalog",
        _fake_export_catalog,
    )
    monkeypatch.setattr(
        settings_handlers,
        "_settings_agent_manager",
        lambda request: (SimpleNamespace(refresh_settings_cache=AsyncMock()), False),
    )

    await settings_handlers._run_bulk_test_job(request)

    assert refresh_calls == ["openai"]
    probe_mock.assert_awaited_once()


@pytest.mark.anyio
async def test_settings_bulk_test_exports_catalog(client, monkeypatch, tmp_path):
    db = client._transport.app.state.db
    await db.set_setting("agent_dev_mode_enabled", "1")
    service = AgentProviderService(db, client._transport.app.state.config)
    await service.save_provider_configs(
        [
            ProviderRuntimeConfig(
                provider="openai",
                enabled=True,
                priority=0,
                selected_model="gpt-4.1-mini",
                secret_fields={"api_key": "openai-key"},
            )
        ]
    )
    from src.web.settings import handlers as settings_handlers

    export_path = tmp_path / "compat_catalog.json"

    async def _fake_run_bulk_test_job(request, configs=None):
        del configs
        status = settings_handlers._bulk_test_status_payload(request)
        status.update(
            {
                "running": False,
                "started_at": "2026-03-12T00:00:00+00:00",
                "finished_at": "2026-03-12T00:00:05+00:00",
                "current_provider": "",
                "current_model": "",
                "completed_probes": 2,
                "total_probes": 2,
                "summary": {"supported": 2, "unsupported": 0, "unknown": 0},
                "providers": {
                    "openai": {
                        "models": [
                            {
                                "model": "gpt-4.1",
                                "status": "supported",
                                "reason": "",
                                "tested_at": "2026-03-12T00:00:01+00:00",
                            },
                            {
                                "model": "gpt-4.1-mini",
                                "status": "supported",
                                "reason": "",
                                "tested_at": "2026-03-12T00:00:02+00:00",
                            },
                        ],
                        "source": "live",
                        "summary": {"supported": 2, "unsupported": 0, "unknown": 0},
                    }
                },
                "catalog_path": str(export_path),
                "error": "",
                "recent_events": ["00:00:05 Тестирование завершено."],
            }
        )
        export_path.write_text('{"providers": []}', encoding="utf-8")

    monkeypatch.setattr(settings_handlers, "_run_bulk_test_job", _fake_run_bulk_test_job)

    resp = await client.post("/settings/agent-providers/test-all")

    assert resp.status_code == 200
    payload = resp.json()
    assert payload["ok"] is True
    assert payload["started"] is True
    await asyncio.sleep(0)

    status_resp = await client.get("/settings/agent-providers/test-all/status")

    assert status_resp.status_code == 200
    status_payload = status_resp.json()
    assert status_payload["summary"]["supported"] == 2
    assert status_payload["providers"]["openai"]["summary"]["supported"] == 2
    assert status_payload["catalog_path"] == str(export_path)
    assert export_path.exists()


@pytest.mark.anyio
async def test_settings_page_blocks_agent_provider_writes_without_encryption_secret(tmp_path):
    config = AppConfig()
    config.database.path = str(tmp_path / "test_no_secret.db")
    config.telegram.api_id = 12345
    config.telegram.api_hash = "test_hash"
    config.web.password = "testpass"
    app = create_app(config)

    db = Database(config.database.path)
    await db.initialize()
    app.state.db = db
    app.state.pool = type(
        "Pool",
        (),
        {
            "clients": {},
            "get_users_info": AsyncMock(return_value=[]),
            "get_dialogs": AsyncMock(return_value=[]),
            "get_dialogs_for_phone": AsyncMock(return_value=[]),
            "resolve_channel": AsyncMock(),
        },
    )()
    from src.telegram.auth import TelegramAuth

    app.state.auth = TelegramAuth(12345, "test_hash")
    app.state.notifier = None
    collector = Collector(app.state.pool, db, config.scheduler)
    app.state.collector = collector
    app.state.collection_queue = CollectionQueue(collector, db)
    app.state.search_engine = SearchEngine(db)
    app.state.ai_search = AISearchEngine(config.llm, db)
    app.state.scheduler = SchedulerManager(config.scheduler)
    app.state.session_secret = "test_secret_key"
    await db.add_account(Account(phone="+1234567890", session_string="test_session"))
    await db.set_setting("agent_dev_mode_enabled", "1")

    transport = ASGITransport(app=app)
    auth_header = base64.b64encode(b":testpass").decode()
    async with AsyncClient(
        transport=transport,
        base_url="http://test",
        follow_redirects=True,
        headers={"Authorization": f"Basic {auth_header}", "Origin": "http://test"},
    ) as client:
        page = await client.get("/settings/")
        assert "SESSION_ENCRYPTION_KEY" in page.text

        resp = await client.post(
            "/settings/agent-providers/add", data={"provider": "openai"}, follow_redirects=False
        )
        assert resp.status_code == 303
        assert "agent_provider_secret_required" in resp.headers["location"]

    await app.state.collection_queue.shutdown()
    await db.close()


@pytest.mark.anyio
async def test_channels_page(client):
    resp = await client.get("/channels/")
    assert resp.status_code == 200


@pytest.mark.anyio
async def test_search_page(client):
    resp = await client.get("/search")
    assert resp.status_code == 200
    assert "Поиск" in resp.text


@pytest.mark.anyio
async def test_scheduler_page(client):
    resp = await client.get("/scheduler/")
    assert resp.status_code == 200


@pytest.mark.anyio
async def test_scheduler_filter_active(client):
    db = client._transport.app.state.db
    await db.create_collection_task(-100901, "Active Task")
    done_id = await db.create_collection_task(-100902, "Done Task")
    await db.update_collection_task(done_id, "completed", messages_collected=10)

    resp = await client.get("/scheduler/?status=active")
    assert resp.status_code == 200
    assert "Active Task" in resp.text
    assert "Done Task" not in resp.text

    resp = await client.get("/scheduler/?status=completed")
    assert resp.status_code == 200
    assert "Done Task" in resp.text
    assert "Active Task" not in resp.text


@pytest.mark.anyio
async def test_scheduler_filter_invalid_status(client):
    resp = await client.get("/scheduler/?status=bogus")
    assert resp.status_code == 200


@pytest.mark.anyio
async def test_scheduler_pagination_out_of_range(client):
    db = client._transport.app.state.db
    await db.create_collection_task(-100950, "Some Task")

    resp = await client.get("/scheduler/?page=999")
    assert resp.status_code == 200
    assert "Some Task" in resp.text


@pytest.mark.anyio
async def test_scheduler_limit_preserved_in_links(client):
    db = client._transport.app.state.db
    for i in range(15):
        await db.create_collection_task(-100800 - i, f"Task {i}")

    resp = await client.get("/scheduler/?limit=10")
    assert resp.status_code == 200
    assert "limit=10" in resp.text


@pytest.mark.anyio
async def test_search_with_query(client):
    resp = await client.get("/search?q=test&mode=local")
    assert resp.status_code == 200
    assert "test" in resp.text


@pytest.mark.anyio
async def test_search_with_invalid_channel_id_returns_error(client):
    resp = await client.get("/search?q=test&mode=channel&channel_id=abc")
    assert resp.status_code == 200
    assert "Некорректный ID канала: abc" in resp.text


@pytest.mark.anyio
async def test_search_with_semantic_mode(client, monkeypatch):
    from src.models import Message
    from src.services.embedding_service import EmbeddingService

    db = client._transport.app.state.db

    await db.insert_messages_batch(
        [
            Message(
                channel_id=-100123,
                message_id=1,
                text="Bitcoin rebounds strongly",
                date=datetime.now(timezone.utc),
            ),
        ]
    )
    rows = await db.execute_fetchall("SELECT id FROM messages ORDER BY id")
    emb = [(int(rows[0]["id"]), [1.0, 0.0])]
    await db.repos.messages.upsert_message_embeddings(emb)
    await db.repos.messages.upsert_message_embedding_json(emb)
    monkeypatch.setattr(
        EmbeddingService,
        "index_pending_messages",
        AsyncMock(return_value=0),
    )
    monkeypatch.setattr(
        EmbeddingService,
        "embed_query",
        AsyncMock(return_value=[1.0, 0.0]),
    )

    resp = await client.get("/search?q=bitcoin&mode=semantic")

    assert resp.status_code == 200
    assert "Bitcoin rebounds strongly" in resp.text
    assert "Семантический" in resp.text


@pytest.mark.anyio
async def test_search_runtime_error_is_rendered(client, monkeypatch):
    from src.web import deps

    class BrokenSearchService:
        async def search(self, **kwargs):
            raise RuntimeError("boom")

        async def check_quota(self, query=""):
            return None

    monkeypatch.setattr(deps, "search_service", lambda request: BrokenSearchService())

    resp = await client.get("/search?q=test&mode=telegram")

    assert resp.status_code == 200
    assert "Ошибка поиска: boom" in resp.text


@pytest.fixture
async def unauth_client(client):
    """Client without auth headers, reusing the same app from client fixture."""
    transport = client._transport
    async with AsyncClient(transport=transport, base_url="http://test", follow_redirects=True) as c:
        yield c


@pytest.mark.anyio
async def test_no_auth_browser_redirects_to_web_login(unauth_client):
    resp = await unauth_client.get(
        "/",
        headers={"Accept": "text/html"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert resp.headers["location"] == "/login?next=%2F"


@pytest.mark.anyio
async def test_no_auth_htmx_returns_hx_redirect(unauth_client):
    resp = await unauth_client.get(
        "/",
        headers={"HX-Request": "true"},
        follow_redirects=False,
    )
    assert resp.status_code == 401
    assert resp.headers["HX-Redirect"] == "/login?next=%2F"


@pytest.mark.anyio
async def test_no_auth_api_returns_401(unauth_client):
    resp = await unauth_client.get(
        "/",
        headers={"Accept": "application/json"},
        follow_redirects=False,
    )
    assert resp.status_code == 401
    assert "WWW-Authenticate" in resp.headers


@pytest.mark.anyio
async def test_health_no_auth(unauth_client):
    resp = await unauth_client.get("/health")
    assert resp.status_code == 200


@pytest.mark.anyio
async def test_basic_auth_sets_cookie(client):
    resp = await client.get("/dashboard/", follow_redirects=False)
    assert resp.status_code == 200
    assert COOKIE_NAME in resp.cookies


@pytest.mark.anyio
async def test_cookie_auth_without_basic(client):
    token = create_session_token("admin", "test_secret_key")
    transport = client._transport
    async with AsyncClient(
        transport=transport,
        base_url="http://test",
        cookies={COOKIE_NAME: token},
    ) as c:
        resp = await c.get("/dashboard/")
        assert resp.status_code == 200


@pytest.mark.anyio
async def test_root_redirects_to_search_when_agent_unavailable(client):
    """Without LLM keys the root page should redirect to /search, not /agent."""
    resp = await client.get("/", follow_redirects=False)
    assert resp.status_code == 303
    assert resp.headers["location"] == "/search"


@pytest.mark.anyio
async def test_web_login_post_sets_cookie_and_redirects_to_next(unauth_client):
    resp = await unauth_client.post(
        "/login",
        data={"password": "testpass", "next": "/channels/"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert resp.headers["location"] == "/channels/"
    assert COOKIE_NAME in resp.headers.get("set-cookie", "")

    page = await unauth_client.get("/channels/", follow_redirects=False)
    assert page.status_code == 200


@pytest.mark.anyio
async def test_web_login_post_rejects_invalid_password(unauth_client):
    resp = await unauth_client.post(
        "/login",
        data={"password": "wrong", "next": "/channels/"},
        follow_redirects=False,
    )
    assert resp.status_code == 401
    assert "Неверный пароль" in resp.text
    assert COOKIE_NAME not in resp.headers.get("set-cookie", "")


@pytest.mark.anyio
async def test_web_login_post_blocks_open_redirect(unauth_client):
    resp = await unauth_client.post(
        "/login",
        data={"password": "testpass", "next": "https://evil.example"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert resp.headers["location"] == "/"


@pytest.mark.anyio
async def test_web_login_post_blocks_backslash_redirect(unauth_client):
    resp = await unauth_client.post(
        "/login",
        data={"password": "testpass", "next": "/\\evil.com"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert resp.headers["location"] == "/"


@pytest.mark.anyio
async def test_web_login_post_blocks_percent_encoded_backslash_redirect(unauth_client):
    resp = await unauth_client.post(
        "/login",
        data={"password": "testpass", "next": "/%5Cevil.com"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert resp.headers["location"] == "/"


@pytest.mark.anyio
async def test_web_login_post_non_ascii_password_accepted(tmp_path, real_pool_harness_factory):
    """Non-ASCII passwords (e.g. Cyrillic) must not raise TypeError in secrets.compare_digest."""
    non_ascii_pw = "пароль123"
    config = make_test_config(tmp_path, password=non_ascii_pw)
    harness = real_pool_harness_factory()
    app, db = await build_web_app(config, harness)
    try:
        transport = ASGITransport(app=app)
        async with AsyncClient(transport=transport, base_url="http://test", follow_redirects=False) as c:
            resp = await c.post("/login", data={"password": non_ascii_pw, "next": "/"})
        assert resp.status_code == 303

        async with AsyncClient(transport=transport, base_url="http://test", follow_redirects=False) as c:
            resp = await c.post("/login", data={"password": "wrong", "next": "/"})
        assert resp.status_code == 401
    finally:
        await app.state.collection_queue.shutdown()
        await db.close()


@pytest.mark.anyio
async def test_web_login_redirects_authenticated_user_to_next(client):
    token = create_session_token("admin", "test_secret_key")
    transport = client._transport
    async with AsyncClient(
        transport=transport,
        base_url="http://test",
        cookies={COOKIE_NAME: token},
    ) as c:
        resp = await c.get("/login?next=/channels/", follow_redirects=False)
        assert resp.status_code == 303
        assert resp.headers["location"] == "/channels/"


@pytest.mark.anyio
async def test_logout_clears_cookie_and_redirects_to_login(client):
    resp = await client.get("/logout", follow_redirects=False)
    assert resp.status_code == 303
    assert resp.headers["location"] == "/login"
    assert COOKIE_NAME in resp.headers.get("set-cookie", "")
    cookie_header = resp.headers.get("set-cookie", "")
    assert "Max-Age=0" in cookie_header or "max-age=0" in cookie_header


@pytest.mark.anyio
async def test_cookie_not_secure_on_http(client):
    resp = await client.get("/dashboard/", follow_redirects=False)
    cookie_header = resp.headers.get("set-cookie", "")
    assert "Secure" not in cookie_header


@pytest.mark.anyio
async def test_cookie_secure_on_https(client):
    transport = client._transport
    auth_header = base64.b64encode(b":testpass").decode()
    async with AsyncClient(
        transport=transport,
        base_url="https://test",
        follow_redirects=False,
        headers={"Authorization": f"Basic {auth_header}", "Origin": "https://test"},
    ) as c:
        resp = await c.get("/")
        cookie_header = resp.headers.get("set-cookie", "")
        assert "Secure" in cookie_header


@pytest.mark.anyio
async def test_invalid_cookie_falls_back(unauth_client):
    unauth_client.cookies.set(COOKIE_NAME, "fake.token")
    resp = await unauth_client.get(
        "/",
        headers={"Accept": "text/html"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert resp.headers["location"] == "/login?next=%2F"


@pytest.mark.anyio
async def test_logout_no_auth_required(unauth_client):
    resp = await unauth_client.get("/logout", follow_redirects=False)
    assert resp.status_code == 303
    assert resp.headers["location"] == "/login"


@pytest.mark.anyio
async def test_settings_shows_accounts(tmp_path):
    """Settings page displays accounts from DB."""
    config = AppConfig()
    config.database.path = str(tmp_path / "test.db")
    config.telegram.api_id = 12345
    config.telegram.api_hash = "test_hash"
    config.web.password = "testpass"
    app = create_app(config)

    db = Database(config.database.path)
    await db.initialize()
    app.state.db = db

    account = Account(phone="+79991234567", session_string="test_session", is_primary=True)
    await db.add_account(account)

    app.state.pool = type("Pool", (), {"clients": {"+79991234567": object()}})()
    from src.telegram.auth import TelegramAuth

    app.state.auth = TelegramAuth(12345, "test_hash")
    app.state.notifier = None
    collector = Collector(app.state.pool, db, config.scheduler)
    app.state.collector = collector
    app.state.search_engine = SearchEngine(db)
    app.state.ai_search = AISearchEngine(config.llm, db)
    app.state.scheduler = SchedulerManager(config.scheduler)
    app.state.session_secret = "test_secret_key"

    transport = ASGITransport(app=app)
    auth_header = base64.b64encode(b":testpass").decode()
    async with AsyncClient(
        transport=transport,
        base_url="http://test",
        follow_redirects=True,
        headers={"Authorization": f"Basic {auth_header}", "Origin": "http://test"},
    ) as c:
        resp = await c.get("/settings/")
        assert resp.status_code == 200
        assert "+79991234567" in resp.text
        assert "Добавьте первый аккаунт" not in resp.text

    await db.close()


@pytest.mark.anyio
async def test_settings_no_accounts(client):
    """Settings page shows 'no accounts' message when DB has no accounts."""
    db = client._transport.app.state.db
    for acc in await db.get_accounts(active_only=False):
        await db.delete_account(acc.id)
    resp = await client.get("/settings/")
    assert resp.status_code == 200
    assert "Добавьте первый аккаунт" in resp.text
    assert "/auth/login" in resp.text


@pytest.mark.anyio
async def test_settings_rejects_invalid_api_id(client):
    db = client._transport.app.state.db

    resp = await client.post(
        "/settings/save-credentials",
        data={"api_id": "abc", "api_hash": "hash"},
    )

    assert resp.status_code == 200
    assert resp.url.params.get("error") == "invalid_api_id"
    assert await db.get_setting("tg_api_id") is None


@pytest.mark.anyio
async def test_resolve_channel_success(client):
    """Adding a channel via identifier queues a worker command."""
    db = client._transport.app.state.db
    resp = await client.post("/channels/add", data={"identifier": "@testchan"})
    assert resp.status_code == 200
    commands = await db.repos.telegram_commands.list_commands(limit=1)
    assert commands[0].command_type == "channels.add_identifier"


@pytest.mark.anyio
async def test_channels_add_logs_unexpected_error(client, monkeypatch, caplog):
    db = client._transport.app.state.db
    with caplog.at_level(logging.ERROR, logger="src.web.routes.channels"):
        resp = await client.post(
            "/channels/add",
            data={"identifier": "@broken"},
            follow_redirects=False,
        )

    assert resp.status_code == 303
    assert "command_id=" in resp.headers["location"]
    commands = await db.repos.telegram_commands.list_commands(limit=1)
    assert commands[0].payload["identifier"] == "@broken"
    assert "Channel add runtime failure" not in caplog.text


@pytest.mark.anyio
async def test_auth_send_code_logs_exception(client, monkeypatch, caplog):
    db = client._transport.app.state.db
    monkeypatch.setattr(
        client._transport.app.state.auth,
        "send_code",
        AsyncMock(side_effect=RuntimeError("boom")),
    )

    with caplog.at_level(logging.ERROR, logger="src.web.routes.auth"):
        resp = await client.post("/auth/send-code", data={"phone": "+7000"}, follow_redirects=False)

    assert resp.status_code == 303
    assert "command_id=" in resp.headers["location"]
    commands = await db.repos.telegram_commands.list_commands(limit=1)
    assert commands[0].command_type == "auth.send_code"
    assert commands[0].payload["phone"] == "+7000"
    assert "Failed to send auth code for phone=+7000" not in caplog.text


@pytest.mark.anyio
async def test_notification_setup_logs_exception(client, monkeypatch, caplog):
    with caplog.at_level(logging.ERROR, logger="src.web.routes.settings"):
        resp = await client.post("/settings/notifications/setup", follow_redirects=False)

    assert resp.status_code == 303
    assert "command_id=" in resp.headers["location"]
    assert "Notification setup failed" not in caplog.text


@pytest.mark.anyio
async def test_error_redirects_are_logged_globally(client, caplog):
    with caplog.at_level(logging.WARNING, logger="src.web.app"):
        resp = await client.post(
            "/settings/save-scheduler",
            data={"collect_interval_minutes": "abc"},
            follow_redirects=False,
        )

    assert resp.status_code == 303
    assert resp.headers["location"] == "/settings?error=invalid_value"
    assert "Web request redirected with error" in caplog.text
    assert "error=invalid_value" in caplog.text


@pytest.mark.anyio
async def test_csrf_blocks_cross_origin_post(client):
    resp = await client.post(
        "/channels/add",
        data={"identifier": "@testchan"},
        headers={"Origin": "https://evil.example"},
        follow_redirects=False,
    )
    assert resp.status_code == 403
    assert "CSRF validation failed" in resp.text


@pytest.mark.anyio
async def test_csrf_blocks_null_origin(client):
    resp = await client.post(
        "/channels/add",
        data={"identifier": "@testchan"},
        headers={"Origin": "null"},
        follow_redirects=False,
    )
    assert resp.status_code == 403
    assert "CSRF validation failed" in resp.text


@pytest.mark.anyio
async def test_csrf_blocks_post_without_origin_or_referer(client):
    """POST without Origin/Referer headers remains allowed for Basic-auth clients."""
    transport = client._transport
    auth_header = base64.b64encode(b":testpass").decode()
    async with AsyncClient(
        transport=transport,
        base_url="http://test",
        headers={"Authorization": f"Basic {auth_header}"},
    ) as c:
        resp = await c.post(
            "/channels/add",
            data={"identifier": "@testchan"},
            follow_redirects=False,
        )
        assert resp.status_code == 303


@pytest.mark.anyio
async def test_csrf_blocks_cookie_auth_post_without_origin_or_referer(client):
    token = create_session_token("admin", "test_secret_key")
    transport = client._transport
    async with AsyncClient(
        transport=transport,
        base_url="http://test",
        cookies={COOKIE_NAME: token},
    ) as c:
        resp = await c.post(
            "/channels/add",
            data={"identifier": "@testchan"},
            follow_redirects=False,
        )
        assert resp.status_code == 403
        assert "CSRF" in resp.text


@pytest.mark.anyio
async def test_csrf_allows_same_origin_post(client):
    resp = await client.post(
        "/channels/add",
        data={"identifier": "@testchan"},
        headers={"Origin": "http://test"},
        follow_redirects=False,
    )
    assert resp.status_code == 303


@pytest.mark.anyio
async def test_csrf_allows_same_origin_post_behind_proxy(client):
    resp = await client.post(
        "/channels/add",
        data={"identifier": "@testchan"},
        headers={
            "Origin": "https://example.com",
            "X-Forwarded-Proto": "https",
            "X-Forwarded-Host": "example.com",
        },
        follow_redirects=False,
    )
    assert resp.status_code == 303


@pytest.mark.anyio
async def test_resolve_channel_fail(tmp_path):
    """Failed resolve redirects with error query param."""
    config = AppConfig()
    config.database.path = str(tmp_path / "test.db")
    config.telegram.api_id = 12345
    config.telegram.api_hash = "test_hash"
    config.web.password = "testpass"
    app = create_app(config)

    db = Database(config.database.path)
    await db.initialize()
    app.state.db = db

    async def _no_users(self):
        return []

    async def _fail_resolve(self, identifier):
        raise ValueError("not found")

    async def _get_dialogs(self):
        return []

    app.state.pool = type(
        "Pool",
        (),
        {
            "clients": {},
            "get_users_info": _no_users,
            "resolve_channel": _fail_resolve,
            "get_dialogs": _get_dialogs,
        },
    )()
    from src.telegram.auth import TelegramAuth

    app.state.auth = TelegramAuth(12345, "test_hash")
    app.state.notifier = None
    collector = Collector(app.state.pool, db, config.scheduler)
    app.state.collector = collector
    app.state.search_engine = SearchEngine(db)
    app.state.ai_search = AISearchEngine(config.llm, db)
    app.state.scheduler = SchedulerManager(config.scheduler)
    app.state.session_secret = "test_secret_key"

    transport = ASGITransport(app=app)
    auth_header = base64.b64encode(b":testpass").decode()
    async with AsyncClient(
        transport=transport,
        base_url="http://test",
        follow_redirects=True,
        headers={"Authorization": f"Basic {auth_header}", "Origin": "http://test"},
    ) as c:
        resp = await c.post("/channels/add", data={"identifier": "@nonexistent"}, follow_redirects=False)
        assert resp.status_code == 303
        assert "command_id=" in resp.headers.get("location", "")

    await db.close()


@pytest.mark.anyio
async def test_dialogs_endpoint(client):
    """GET /channels/dialogs returns JSON list of channels."""
    db = client._transport.app.state.db
    await db.repos.dialog_cache.replace_dialogs(
        "+1234567890",
        [
            {"channel_id": -100111, "title": "Dialog Chan 1", "channel_type": "channel"},
            {"channel_id": -100222, "title": "Dialog Chan 2", "channel_type": "channel"},
        ],
    )
    resp = await client.get("/channels/dialogs")
    assert resp.status_code == 200
    data = resp.json()
    assert len(data) == 2
    assert data[0]["title"] == "Dialog Chan 1"
    assert data[0]["channel_id"] == -100111
    assert "already_added" in data[0]


@pytest.mark.anyio
async def test_add_bulk(client):
    """POST /channels/add-bulk adds selected channels from dialogs."""
    db = client._transport.app.state.db
    await db.repos.dialog_cache.replace_dialogs(
        "+1234567890",
        [
            {"channel_id": -100111, "title": "Dialog Chan 1", "channel_type": "channel"},
            {"channel_id": -100222, "title": "Dialog Chan 2", "channel_type": "channel"},
        ],
    )
    resp = await client.post(
        "/channels/add-bulk",
        data={"channel_ids": ["-100111", "-100222"]},
    )
    assert resp.status_code == 200
    # Verify channels page shows added channels
    resp = await client.get("/channels/")
    assert "Dialog Chan 1" in resp.text
    assert "Dialog Chan 2" in resp.text


@pytest.mark.anyio
async def test_add_channel_redirect_has_msg(client):
    """Adding a channel redirects with queued command id."""
    resp = await client.post(
        "/channels/add", data={"identifier": "@testchan"}, follow_redirects=False
    )
    assert resp.status_code == 303
    assert "command_id=" in resp.headers["location"]


@pytest.mark.anyio
async def test_save_notification_account_round_trip(client):
    from src.models import Account

    db = client._transport.app.state.db
    await db.add_account(Account(phone="+79990000001", session_string="session", is_primary=True))

    resp = await client.post(
        "/settings/save-notification-account",
        data={"notification_account_phone": "+79990000001"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "msg=notification_account_saved" in resp.headers["location"]
    assert await db.get_setting("notification_account_phone") == "+79990000001"

    resp = await client.get("/settings/")
    assert "+79990000001" in resp.text


@pytest.mark.anyio
async def test_settings_page_shows_stale_notification_account_warning(client):
    db = client._transport.app.state.db
    await db.set_setting("notification_account_phone", "+79990000009")

    resp = await client.get("/settings/")
    assert resp.status_code == 200
    assert "Выбранный аккаунт уведомлений удалён." in resp.text


@pytest.mark.anyio
async def test_notification_status_returns_error_for_unavailable_selected_account(client):
    from src.models import Account

    db = client._transport.app.state.db
    await db.add_account(Account(phone="+79990000002", session_string="session", is_primary=True))
    await db.set_setting("notification_account_phone", "+79990000002")
    await db.repos.runtime_snapshots.upsert_snapshot(
        RuntimeSnapshot(
            snapshot_type="notification_target_status",
            payload={
                "target": {
                    "mode": "selected",
                    "state": "disconnected",
                    "message": "Аккаунт +79990000002 не подключён.",
                    "configured_phone": "+79990000002",
                    "effective_phone": "+79990000002",
                },
                "bot": {"configured": False},
            },
        )
    )

    resp = await client.get(
        "/settings/notifications/status",
        headers={"Accept": "application/json"},
    )
    assert resp.status_code == 409
    data = resp.json()
    assert data["configured"] is False
    assert "не подключён" in data["error"]


@pytest.mark.anyio
async def test_channel_type_displayed(client):
    """Channel type column is shown on channels page after adding a channel."""
    from src.models import Channel

    await client._transport.app.state.db.add_channel(
        Channel(channel_id=-100123, title="Test", username="testchan", channel_type="channel")
    )
    resp = await client.get("/channels/")
    assert resp.status_code == 200
    assert "Канал" in resp.text
    assert "Тип" in resp.text


@pytest.mark.anyio
async def test_add_scam_channel_is_inactive(tmp_path):
    """Adding a scam channel via /channels/add queues a worker command."""
    config = AppConfig()
    config.database.path = str(tmp_path / "test.db")
    config.telegram.api_id = 12345
    config.telegram.api_hash = "test_hash"
    config.web.password = "testpass"
    app = create_app(config)

    db = Database(config.database.path)
    await db.initialize()
    app.state.db = db

    async def _no_users(self):
        return []

    async def _resolve_scam(self, identifier):
        return {
            "channel_id": -1009999999,
            "title": "Scam Channel",
            "username": "scamchan",
            "channel_type": "scam",
            "deactivate": True,
        }

    async def _get_dialogs(self):
        return []

    async def _fetch_meta(self, channel_id, channel_type=None):
        return None

    app.state.pool = type(
        "Pool",
        (),
        {
            "clients": {},
            "get_users_info": _no_users,
            "resolve_channel": _resolve_scam,
            "get_dialogs": _get_dialogs,
            "fetch_channel_meta": _fetch_meta,
        },
    )()
    from src.telegram.auth import TelegramAuth

    app.state.auth = TelegramAuth(12345, "test_hash")
    app.state.notifier = None
    collector = Collector(app.state.pool, db, config.scheduler)
    app.state.collector = collector
    app.state.search_engine = SearchEngine(db)
    app.state.ai_search = AISearchEngine(config.llm, db)
    app.state.scheduler = SchedulerManager(config.scheduler)
    app.state.session_secret = "test_secret_key"

    transport = ASGITransport(app=app)
    auth_header = base64.b64encode(b":testpass").decode()
    async with AsyncClient(
        transport=transport,
        base_url="http://test",
        follow_redirects=True,
        headers={"Authorization": f"Basic {auth_header}", "Origin": "http://test"},
    ) as c:
        resp = await c.post("/channels/add", data={"identifier": "@scamchan"})
        assert resp.status_code == 200

    commands = await db.repos.telegram_commands.list_commands(limit=1)
    assert commands[0].command_type == "channels.add_identifier"

    await db.close()


@pytest.mark.anyio
async def test_bulk_add_scam_dialog_is_inactive(tmp_path):
    """Adding a scam dialog via /channels/add-bulk creates it with is_active=False."""
    config = AppConfig()
    config.database.path = str(tmp_path / "test.db")
    config.telegram.api_id = 12345
    config.telegram.api_hash = "test_hash"
    config.web.password = "testpass"
    app = create_app(config)

    db = Database(config.database.path)
    await db.initialize()
    app.state.db = db
    await db.repos.dialog_cache.replace_dialogs(
        "+1234567890",
        [
            {
                "channel_id": -100777,
                "title": "Scam Dialog",
                "username": "scamdialog",
                "channel_type": "scam",
                "deactivate": True,
            }
        ],
    )

    async def _no_users(self):
        return []

    async def _resolve_channel(self, identifier):
        return None

    async def _get_dialogs_scam(self):
        return [
            {
                "channel_id": -100777,
                "title": "Scam Dialog",
                "username": "scamdialog",
                "channel_type": "scam",
                "deactivate": True,
            }
        ]

    app.state.pool = type(
        "Pool",
        (),
        {
            "clients": {},
            "get_users_info": _no_users,
            "resolve_channel": _resolve_channel,
            "get_dialogs": _get_dialogs_scam,
        },
    )()
    from src.telegram.auth import TelegramAuth

    app.state.auth = TelegramAuth(12345, "test_hash")
    app.state.notifier = None
    collector = Collector(app.state.pool, db, config.scheduler)
    app.state.collector = collector
    app.state.search_engine = SearchEngine(db)
    app.state.ai_search = AISearchEngine(config.llm, db)
    app.state.scheduler = SchedulerManager(config.scheduler)
    app.state.session_secret = "test_secret_key"

    transport = ASGITransport(app=app)
    auth_header = base64.b64encode(b":testpass").decode()
    async with AsyncClient(
        transport=transport,
        base_url="http://test",
        follow_redirects=True,
        headers={"Authorization": f"Basic {auth_header}", "Origin": "http://test"},
    ) as c:
        resp = await c.post("/channels/add-bulk", data={"channel_ids": ["-100777"]})
        assert resp.status_code == 200

    channels = await db.get_channels()
    assert len(channels) == 1
    assert channels[0].is_active is False

    await db.close()


@pytest.mark.anyio
async def test_filter_analyze_applies_filters(client):
    from datetime import datetime, timezone

    from src.models import Channel, Message

    db = client._transport.app.state.db
    ch = Channel(channel_id=-100551, title="Spam", username="spamchan", channel_type="channel")
    await db.add_channel(ch)
    now = datetime.now(timezone.utc)
    await db.insert_messages_batch(
        [
            Message(
                channel_id=-100551,
                message_id=i + 1,
                text="same spam line",
                date=now,
            )
            for i in range(20)
        ]
    )

    resp = await client.post("/channels/filter/analyze", follow_redirects=False)
    assert resp.status_code == 303
    assert "/channels/filter/manage" in resp.headers["location"]

    channel = await db.get_channel_by_channel_id(-100551)
    assert channel is not None
    assert channel.is_filtered is True


@pytest.mark.anyio
async def test_filter_apply_with_snapshot_skips_reanalyze(client, monkeypatch):
    from src.models import Channel

    db = client._transport.app.state.db
    await db.add_channel(
        Channel(channel_id=-100661, title="Snapshot", username="snapshot", channel_type="channel")
    )

    async def _boom(self):
        raise AssertionError("analyze_all should not be called for snapshot apply")

    monkeypatch.setattr("src.web.routes.filter.ChannelAnalyzer.analyze_all", _boom)

    resp = await client.post(
        "/channels/filter/apply",
        data={"snapshot": "1", "selected": "-100661|low_uniqueness"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "msg=filter_applied" in resp.headers["location"]

    cur = await db.execute(
        "SELECT is_filtered, filter_flags FROM channels WHERE channel_id = ?",
        (-100661,),
    )
    row = await cur.fetchone()
    assert row["is_filtered"] == 1
    assert row["filter_flags"] == "low_uniqueness"


@pytest.mark.anyio
async def test_filter_apply_without_snapshot_returns_error(client, monkeypatch):
    from src.models import Channel

    db = client._transport.app.state.db
    await db.add_channel(
        Channel(channel_id=-100662, title="Fallback", username="fallback", channel_type="channel")
    )

    async def _boom(self):
        raise AssertionError("analyze_all should not be called without snapshot")

    monkeypatch.setattr("src.web.routes.filter.ChannelAnalyzer.analyze_all", _boom)

    resp = await client.post("/channels/filter/apply", data={}, follow_redirects=False)
    assert resp.status_code == 303
    assert "error=filter_snapshot_required" in resp.headers["location"]

    cur = await db.execute(
        "SELECT is_filtered, filter_flags FROM channels WHERE channel_id = ?",
        (-100662,),
    )
    row = await cur.fetchone()
    assert row["is_filtered"] == 0
    assert row["filter_flags"] == ""


@pytest.mark.anyio
async def test_filter_toggle_missing_channel_returns_not_found_msg(client):
    resp = await client.post("/channels/999999/filter-toggle", follow_redirects=False)
    assert resp.status_code == 303
    assert "msg=channel_not_found" in resp.headers["location"]


@pytest.mark.anyio
async def test_filter_toggle_sets_manual_flag(client):
    from src.models import Channel

    db = client._transport.app.state.db
    await db.add_channel(
        Channel(channel_id=-100664, title="Manual", username="manual", channel_type="channel")
    )
    channel = next(ch for ch in await db.get_channels() if ch.channel_id == -100664)

    resp = await client.post(f"/channels/{channel.id}/filter-toggle", follow_redirects=False)
    assert resp.status_code == 303
    assert "msg=filter_toggled" in resp.headers["location"]

    cur = await db.execute(
        "SELECT is_filtered, filter_flags FROM channels WHERE channel_id = ?",
        (-100664,),
    )
    row = await cur.fetchone()
    assert row["is_filtered"] == 1
    assert row["filter_flags"] == "manual"


@pytest.mark.anyio
async def test_collect_filtered_channel_is_allowed(client):
    """Manual collect (web UI) must proceed even when channel is filtered."""
    from src.models import Channel

    db = client._transport.app.state.db
    client._transport.app.state.collection_queue = CollectionQueue(
        client._transport.app.state.collector,
        db,
    )
    await db.add_channel(
        Channel(channel_id=-100663, title="Filtered", username="filtered", channel_type="channel")
    )
    await db.set_channels_filtered_bulk([(-100663, "low_uniqueness")])
    channels = await db.get_channels(include_filtered=True)
    channel = next(ch for ch in channels if ch.channel_id == -100663)

    resp = await client.post(f"/channels/{channel.id}/collect", follow_redirects=False)
    assert resp.status_code == 303
    assert "error" not in resp.headers["location"]

    tasks = await db.get_collection_tasks()
    assert len(tasks) == 1

    await client._transport.app.state.collection_queue.shutdown()


@pytest.mark.anyio
async def test_delete_channel_cancels_pending_collection_tasks(client):
    from src.models import Channel

    db = client._transport.app.state.db
    await db.add_channel(
        Channel(channel_id=-100664, title="Delete me", username="deleteme", channel_type="channel")
    )
    channel = next(ch for ch in await db.get_channels() if ch.channel_id == -100664)
    task_id = await db.create_collection_task(channel.channel_id, channel.title)

    resp = await client.post(f"/channels/{channel.id}/delete", follow_redirects=False)
    assert resp.status_code == 303
    assert "msg=channel_deleted" in resp.headers["location"]

    task = await db.get_collection_task(task_id)
    assert task is not None
    assert task.status == CollectionTaskStatus.CANCELLED
    assert task.note == "Канал удалён пользователем."


@pytest.mark.anyio
async def test_stats_all_creates_pending_task(client):
    db = client._transport.app.state.db

    resp = await client.post("/channels/stats/all", follow_redirects=False)
    assert resp.status_code == 303
    assert "msg=stats_collection_started" in resp.headers["location"]

    tasks = await db.get_collection_tasks()
    assert len(tasks) == 1
    assert tasks[0].task_type == CollectionTaskType.STATS_ALL
    assert tasks[0].channel_id is None
    assert tasks[0].status == CollectionTaskStatus.PENDING
    assert isinstance(tasks[0].payload, StatsAllTaskPayload)
    assert tasks[0].payload.task_kind == CollectionTaskType.STATS_ALL.value


@pytest.mark.anyio
async def test_stats_all_queued_when_collector_running(client):
    app = client._transport.app.state
    db = app.db
    app.collector._running = True
    try:
        resp = await client.post("/channels/stats/all", follow_redirects=False)
    finally:
        app.collector._running = False

    assert resp.status_code == 303
    assert "msg=stats_collection_queued" in resp.headers["location"]

    tasks = await db.get_collection_tasks()
    assert len(tasks) == 1
    assert tasks[0].status == CollectionTaskStatus.PENDING


@pytest.mark.anyio
async def test_stats_all_blocks_duplicate_active_task(client):
    db = client._transport.app.state.db
    await db.create_stats_task(StatsAllTaskPayload(channel_ids=[]))

    resp = await client.post("/channels/stats/all", follow_redirects=False)
    assert resp.status_code == 303
    assert "error=stats_running" in resp.headers["location"]


@pytest.mark.anyio
async def test_stats_all_prioritizes_channels_without_stats(client):
    from src.models import Channel, ChannelStats

    db = client._transport.app.state.db
    await db.add_channel(Channel(channel_id=-100901, title="With stats"))
    await db.add_channel(Channel(channel_id=-100902, title="No stats 1"))
    await db.add_channel(Channel(channel_id=-100903, title="No stats 2"))

    await db.save_channel_stats(ChannelStats(channel_id=-100901, subscriber_count=1))

    resp = await client.post("/channels/stats/all", follow_redirects=False)
    assert resp.status_code == 303
    assert "msg=stats_collection_started" in resp.headers["location"]

    tasks = await db.get_collection_tasks()
    payload = tasks[0].payload
    assert isinstance(payload, StatsAllTaskPayload)
    channel_ids = payload.channel_ids
    assert channel_ids.index(-100901) > channel_ids.index(-100902)
    assert channel_ids.index(-100901) > channel_ids.index(-100903)


@pytest.mark.anyio
async def test_search_results_have_tg_links(client):
    """Search results contain links to original Telegram messages."""
    from datetime import datetime, timezone

    from src.models import Channel, Message

    db = client._transport.app.state.db
    ch = Channel(channel_id=-100123, title="Test", username="testchan", channel_type="channel")
    await db.add_channel(ch)
    msg = Message(
        channel_id=-100123,
        message_id=42,
        text="Hello world",
        date=datetime.now(timezone.utc),
    )
    await db.insert_message(msg)

    resp = await client.get("/search?q=Hello&mode=local")
    assert resp.status_code == 200
    assert "t.me/testchan/42" in resp.text
    assert "bi-link-45deg" in resp.text


@pytest.mark.anyio
async def test_search_results_private_channel_link(client):
    """Private channel messages get t.me/c/ links."""
    from datetime import datetime, timezone

    from src.models import Channel, Message

    db = client._transport.app.state.db
    ch = Channel(channel_id=-100999, title="Private", username=None, channel_type="group")
    await db.add_channel(ch)
    msg = Message(
        channel_id=-100999,
        message_id=7,
        text="Secret message",
        date=datetime.now(timezone.utc),
    )
    await db.insert_message(msg)

    resp = await client.get("/search?q=Secret&mode=local")
    assert resp.status_code == 200
    assert "t.me/c/-100999/7" in resp.text


@pytest.mark.anyio
async def test_collect_all_htmx_returns_scheduler_link_and_creates_tasks(client, monkeypatch):
    """POST /channels/collect-all with HTMX header returns explicit status and queues tasks."""
    from src.models import Channel

    db = client._transport.app.state.db
    monkeypatch.setattr(
        client._transport.app.state.collection_queue,
        "_ensure_worker",
        lambda: None,
    )
    await db.add_channel(Channel(channel_id=-100701, title="Ch1", username="ch1"))
    await db.add_channel(Channel(channel_id=-100702, title="Ch2", username="ch2"))

    resp = await client.post(
        "/channels/collect-all",
        headers={"HX-Request": "true"},
        follow_redirects=False,
    )
    assert resp.status_code == 200
    assert "Добавлено задач: 2." in resp.text
    assert 'href="/scheduler"' in resp.text
    assert "Собрать все каналы" in resp.text

    tasks = await db.get_collection_tasks()
    assert len(tasks) == 2
    assert {task.channel_id for task in tasks} == {-100701, -100702}
    assert all(task.status == "pending" for task in tasks)


@pytest.mark.anyio
async def test_collect_all_htmx_noop_when_tasks_already_exist(client):
    from src.models import Channel

    db = client._transport.app.state.db
    client._transport.app.state.collection_queue._ensure_worker = lambda: None
    await db.add_channel(Channel(channel_id=-100703, title="Ch3", username="ch3"))
    await db.add_channel(Channel(channel_id=-100704, title="Ch4", username="ch4"))
    channel = await db.get_channel_by_channel_id(-100703)
    assert channel is not None
    await client._transport.app.state.collection_queue.enqueue(channel, force=True)

    resp = await client.post(
        "/channels/collect-all",
        headers={"HX-Request": "true"},
        follow_redirects=False,
    )

    assert resp.status_code == 200
    assert "Добавлено задач: 1." in resp.text

    resp = await client.post(
        "/channels/collect-all",
        headers={"HX-Request": "true"},
        follow_redirects=False,
    )
    assert resp.status_code == 200
    assert "Новых задач не добавлено" in resp.text

    tasks = await db.get_collection_tasks(limit=10)
    assert len(tasks) == 2
    assert {task.channel_id for task in tasks} == {-100703, -100704}


@pytest.mark.anyio
async def test_collect_all_non_htmx_redirects_with_new_message_and_creates_tasks(
    client, monkeypatch
):
    """POST /channels/collect-all without HTMX redirects with queue message."""
    from src.models import Channel

    db = client._transport.app.state.db
    monkeypatch.setattr(
        client._transport.app.state.collection_queue,
        "_ensure_worker",
        lambda: None,
    )
    await db.add_channel(Channel(channel_id=-100705, title="Ch5", username="ch5"))
    await db.add_channel(Channel(channel_id=-100706, title="Filtered", username="filtered"))
    await db.set_channels_filtered_bulk([(-100706, "manual")])
    await db.add_channel(
        Channel(channel_id=-100707, title="Inactive", username="inactive", is_active=False)
    )

    resp = await client.post("/channels/collect-all", follow_redirects=False)
    assert resp.status_code == 303
    assert "msg=collect_all_queued" in resp.headers["location"]

    tasks = await db.get_collection_tasks()
    assert len(tasks) == 1
    assert tasks[0].channel_id == -100705
    assert tasks[0].status == "pending"


@pytest.mark.anyio
async def test_collect_all_non_htmx_redirects_with_empty_message_when_no_channels(client):
    resp = await client.post("/channels/collect-all", follow_redirects=False)
    assert resp.status_code == 303
    assert "msg=collect_all_empty" in resp.headers["location"]

    resp = await client.post(
        "/channels/collect-all",
        headers={"HX-Request": "true"},
        follow_redirects=False,
    )
    assert resp.status_code == 200
    assert "Нет активных каналов для загрузки." in resp.text
    assert 'href="/scheduler"' not in resp.text


@pytest.mark.anyio
async def test_enqueue_all_channels_skips_inactive_filtered_and_duplicate_tasks(
    client, monkeypatch
):
    from src.models import Channel
    from src.services.collection_service import CollectionService

    db = client._transport.app.state.db
    collector = client._transport.app.state.collector
    queue = client._transport.app.state.collection_queue
    monkeypatch.setattr(
        queue,
        "_ensure_worker",
        lambda: None,
    )
    await db.add_channel(Channel(channel_id=-100708, title="Active 1", username="active1"))
    await db.add_channel(Channel(channel_id=-100709, title="Active 2", username="active2"))
    await db.add_channel(Channel(channel_id=-100710, title="Filtered", username="filtered"))
    await db.set_channels_filtered_bulk([(-100710, "manual")])
    await db.add_channel(
        Channel(
            channel_id=-100711,
            title="Inactive",
            username="inactive",
            is_active=False,
        )
    )

    channel = await db.get_channel_by_channel_id(-100708)
    assert channel is not None
    await queue.enqueue(channel, force=True)

    result = await CollectionService(db, collector, queue).enqueue_all_channels()

    assert result.total_candidates == 2
    assert result.queued_count == 1
    assert result.skipped_existing_count == 1

    tasks = await db.get_collection_tasks(limit=10)
    assert len(tasks) == 2
    assert {task.channel_id for task in tasks} == {-100708, -100709}


@pytest.mark.anyio
async def test_get_channel_ids_with_active_tasks_returns_distinct_non_stats_ids(client):
    db = client._transport.app.state.db
    await db.create_collection_task(-100801, "One")
    task_id = await db.create_collection_task(-100802, "Two")
    await db.update_collection_task(task_id, CollectionTaskStatus.RUNNING)
    await db.create_collection_task(-100801, "One duplicate")
    await db.create_stats_task(StatsAllTaskPayload(channel_ids=[]))

    active_ids = await db.get_channel_ids_with_active_tasks()

    assert active_ids == {-100801, -100802}


@pytest.mark.anyio
async def test_channels_page_collect_all_button_matches_htmx_fragment(client):
    template_path = Path("src/web/templates/channels.html")
    template_text = template_path.read_text(encoding="utf-8")
    match = re.search(r'(<span id="collect-all-btn">.*?</span>)', template_text, re.S)
    assert match is not None
    template_fragment = match.group(1)

    for expected in (
        'id="collect-all-btn"',
        'action="/channels/collect-all"',
        'hx-post="/channels/collect-all"',
        'hx-target="#collect-all-btn"',
        'hx-swap="outerHTML"',
        "btn-secondary",
        "Собрать все каналы",
    ):
        assert expected in template_fragment
        assert expected in _COLLECT_ALL_BTN

    assert _COLLECT_ALL_BTN == f'<span id="collect-all-btn">{_COLLECT_ALL_FORM}</span>'


@pytest.mark.anyio
async def test_save_scheduler_valid(client):
    """POST /settings/save-scheduler with valid interval persists and redirects."""
    resp = await client.post(
        "/settings/save-scheduler",
        data={"collect_interval_minutes": "30"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "msg=scheduler_saved" in resp.headers["location"]
    db = client._transport.app.state.db
    assert await db.get_setting("collect_interval_minutes") == "30"


@pytest.mark.anyio
async def test_save_scheduler_invalid_value(client):
    """POST /settings/save-scheduler with non-numeric value redirects to error."""
    resp = await client.post(
        "/settings/save-scheduler",
        data={"collect_interval_minutes": "abc"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "error=invalid_value" in resp.headers["location"]


@pytest.mark.anyio
async def test_save_scheduler_clamps_to_min(client):
    """POST /settings/save-scheduler clamps value below 1 to 1."""
    resp = await client.post(
        "/settings/save-scheduler",
        data={"collect_interval_minutes": "0"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "msg=scheduler_saved" in resp.headers["location"]
    db = client._transport.app.state.db
    assert await db.get_setting("collect_interval_minutes") == "1"


@pytest.mark.anyio
async def test_save_scheduler_clamps_to_max(client):
    """POST /settings/save-scheduler clamps value above 1440 to 1440."""
    resp = await client.post(
        "/settings/save-scheduler",
        data={"collect_interval_minutes": "9999"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "msg=scheduler_saved" in resp.headers["location"]
    db = client._transport.app.state.db
    assert await db.get_setting("collect_interval_minutes") == "1440"


@pytest.mark.anyio
async def test_save_filters_valid(client):
    from src.models import Channel, ChannelStats

    db = client._transport.app.state.db
    await db.add_channel(Channel(channel_id=-100501, title="Small"))
    await db.save_channel_stats(ChannelStats(channel_id=-100501, subscriber_count=3))

    resp = await client.post(
        "/settings/save-filters",
        data={"min_subscribers_filter": "10"},
        follow_redirects=False,
    )

    assert resp.status_code == 303
    assert "msg=filters_saved" in resp.headers["location"]
    assert await db.get_setting("min_subscribers_filter") == "10"
    channel = await db.get_channel_by_channel_id(-100501)
    assert channel is not None
    assert channel.is_filtered is True


@pytest.mark.anyio
async def test_save_filters_invalid_value(client):
    resp = await client.post(
        "/settings/save-filters",
        data={"min_subscribers_filter": "bad"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "error=invalid_value" in resp.headers["location"]


@pytest.mark.anyio
async def test_save_credentials_valid_and_masked_path(client):
    db = client._transport.app.state.db
    auth = client._transport.app.state.auth

    resp = await client.post(
        "/settings/save-credentials",
        data={"api_id": "54321", "api_hash": "hash-1"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "msg=credentials_saved" in resp.headers["location"]
    assert await db.get_setting("tg_api_id") == "54321"
    assert await db.get_setting("tg_api_hash") == "hash-1"
    assert auth._api_id == 54321
    assert auth._api_hash == "hash-1"

    resp = await client.post(
        "/settings/save-credentials",
        data={"api_id": "••••••••", "api_hash": "hash-2"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "msg=credentials_saved" in resp.headers["location"]
    assert await db.get_setting("tg_api_id") == "54321"
    assert await db.get_setting("tg_api_hash") == "hash-2"
    assert auth._api_id == 54321
    assert auth._api_hash == "hash-2"


@pytest.mark.anyio
async def test_notification_setup_and_delete_json(client, monkeypatch):

    from src.models import Account

    db = client._transport.app.state.db
    pool = client._transport.app.state.pool
    await db.add_account(Account(phone="+79990000003", session_string="session", is_primary=True))
    await db.set_setting("notification_account_phone", "+79990000003")
    # Mark the phone as connected without a real backend connection; get_native_client_by_phone
    # is mocked immediately below, so we only need the phone key in pool.clients.
    pool.clients["+79990000003"] = object()

    fake_client = SimpleNamespace(
        get_me=AsyncMock(return_value=SimpleNamespace(id=42, username="owner")),
        send_message=AsyncMock(),
        get_entity=AsyncMock(return_value=SimpleNamespace(id=777)),
    )
    pool.get_native_client_by_phone = AsyncMock(return_value=(fake_client, "+79990000003"))
    pool.release_client = AsyncMock()

    async def _create_bot(_client, _name, _username):
        return "token-123"

    async def _delete_bot(_client, _username):
        return None

    monkeypatch.setattr("src.services.notification_service.botfather.create_bot", _create_bot)
    monkeypatch.setattr("src.services.notification_service.botfather.delete_bot", _delete_bot)

    resp = await client.post(
        "/settings/notifications/setup",
        headers={"Accept": "application/json"},
    )
    assert resp.status_code == 202
    assert resp.json()["status"] == "queued"

    delete_resp = await client.post(
        "/settings/notifications/delete",
        headers={"Accept": "application/json"},
    )
    assert delete_resp.status_code == 202
    assert delete_resp.json()["status"] == "queued"


@pytest.mark.anyio
async def test_notification_setup_returns_conflict_when_account_unavailable(client):
    from src.models import Account

    db = client._transport.app.state.db
    await db.add_account(Account(phone="+79990000004", session_string="session", is_primary=True))
    await db.set_setting("notification_account_phone", "+79990000004")

    resp = await client.post(
        "/settings/notifications/setup",
        headers={"Accept": "application/json"},
    )
    assert resp.status_code == 202
    assert resp.json()["status"] == "queued"


@pytest.mark.anyio
async def test_collect_stats_route_enqueues_command(client):
    """Web route only enqueues a telegram command; worker executes the collection."""
    from src.models import Channel

    db = client._transport.app.state.db
    await db.add_channel(Channel(channel_id=-1002001, title="Stats", username="teststats", channel_type="channel"))
    channel = next(ch for ch in await db.get_channels() if ch.username == "teststats")

    # Sanity: even if collector is mocked, route must not call it directly.
    client._transport.app.state.collector.collect_channel_stats = AsyncMock()

    resp = await client.post(f"/channels/{channel.id}/stats", follow_redirects=False)
    assert resp.status_code == 303
    assert "stats_collection_queued" in resp.headers["location"]
    assert "command_id=" in resp.headers["location"]

    # No direct Telegram RPC from the web request.
    client._transport.app.state.collector.collect_channel_stats.assert_not_awaited()

    commands = await db.repos.telegram_commands.list_commands(limit=1)
    assert commands[0].command_type == "channels.collect_stats"
    assert commands[0].payload == {"channel_pk": channel.id}


@pytest.mark.anyio
async def test_collect_stats_route_missing_channel(client):
    """If the channel does not exist, the route redirects to /channels without enqueueing."""
    db = client._transport.app.state.db

    resp = await client.post("/channels/999999/stats", follow_redirects=False)
    assert resp.status_code == 303

    commands = await db.repos.telegram_commands.list_commands(limit=5)
    assert not any(c.command_type == "channels.collect_stats" for c in commands)


@pytest.mark.anyio
async def test_edit_search_query_route(client):
    # Add a search query first
    resp = await client.post(
        "/search-queries/add",
        data={"query": "original", "interval_minutes": "60", "track_stats": "true"},
        follow_redirects=False,
    )
    assert resp.status_code == 303

    db = client._transport.app.state.db
    queries = await db.repos.search_queries.get_all()
    assert len(queries) == 1
    sq_id = queries[0].id

    # Edit the query
    resp = await client.post(
        f"/search-queries/{sq_id}/edit",
        data={
            "query": "updated",
            "interval_minutes": "30",
            "is_regex": "true",
            "notify_on_collect": "true",
        },
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "msg=sq_edited" in resp.headers["location"]

    updated = await db.repos.search_queries.get_by_id(sq_id)
    assert updated.query == "updated"
    assert updated.interval_minutes == 30
    assert updated.is_regex is True
    assert updated.notify_on_collect is True
    assert updated.track_stats is False


class TestWebAgent:
    @pytest.mark.anyio
    async def test_agent_page_auto_creates_thread(self, client):
        client._transport.app.state.agent_manager = AsyncMock()
        client._transport.app.state.agent_manager.get_runtime_status = AsyncMock(
            return_value=SimpleNamespace(
                claude_available=True,
                deepagents_available=False,
                dev_mode_enabled=False,
                backend_override="auto",
                selected_backend="claude",
                fallback_model="",
                fallback_provider="",
                using_override=False,
                error=None,
            )
        )
        resp = await client.get("/agent")
        assert resp.status_code == 200
        assert "Новый тред" in resp.text
        assert resp.url.path == "/agent"
        assert "thread_id=" in str(resp.url.query)

    @pytest.mark.anyio
    async def test_agent_page_redirects_to_first_thread(self, client):
        db = client._transport.app.state.db
        client._transport.app.state.agent_manager = AsyncMock()
        client._transport.app.state.agent_manager.get_runtime_status = AsyncMock(
            return_value=SimpleNamespace(
                claude_available=False,
                deepagents_available=True,
                dev_mode_enabled=True,
                backend_override="deepagents",
                selected_backend="deepagents",
                fallback_model="openai:gpt-4.1-mini",
                fallback_provider="openai",
                using_override=True,
                error=None,
            )
        )
        await db.create_agent_thread("First")
        await db.create_agent_thread("Second")

        resp = await client.get("/agent")
        assert resp.status_code == 200
        assert str(resp.url).endswith("?thread_id=1")

    @pytest.mark.anyio
    async def test_agent_page_shows_deepagents_status_and_hides_claude_model_select(self, client):
        db = client._transport.app.state.db
        await db.create_agent_thread("First")
        client._transport.app.state.agent_manager = AsyncMock()
        client._transport.app.state.agent_manager.get_runtime_status = AsyncMock(
            return_value=SimpleNamespace(
                claude_available=False,
                deepagents_available=True,
                dev_mode_enabled=True,
                backend_override="deepagents",
                selected_backend="deepagents",
                fallback_model="openai:gpt-4.1-mini",
                fallback_provider="openai",
                using_override=True,
                error=None,
            )
        )

        resp = await client.get("/agent?thread_id=1")

        assert resp.status_code == 200
        assert "deepagents" in resp.text
        assert "dev override" in resp.text
        assert "openai:gpt-4.1-mini" in resp.text
        assert 'id="model-select"' not in resp.text

    @pytest.mark.anyio
    async def test_agent_thread_creation_and_deletion(self, client):
        # Create
        resp_create = await client.post("/agent/threads", follow_redirects=False)
        assert resp_create.status_code == 303
        thread_id = resp_create.headers["location"].split("=")[1]

        # Rename
        resp_rename = await client.post(
            f"/agent/threads/{thread_id}/rename", json={"title": "Renamed"}
        )
        assert resp_rename.status_code == 200

        db = client._transport.app.state.db
        thread = await db.get_agent_thread(int(thread_id))
        assert thread["title"] == "Renamed"

        # Delete
        resp_delete = await client.delete(f"/agent/threads/{thread_id}")
        assert resp_delete.status_code == 200

        thread_after = await db.get_agent_thread(int(thread_id))
        assert thread_after is None

    @pytest.mark.anyio
    async def test_get_channels_and_topics(self, client):
        from src.models import Channel

        db = client._transport.app.state.db
        pool = client._transport.app.state.pool

        await db.add_channel(Channel(channel_id=1, title="Forum", channel_type="forum"))
        pool.get_forum_topics = AsyncMock(return_value=[{"id": 10, "title": "Topic"}])

        resp_ch = await client.get("/agent/channels-json")
        assert resp_ch.status_code == 200
        assert len(resp_ch.json()) > 0

        resp_topics = await client.get("/agent/forum-topics?channel_id=1")
        assert resp_topics.status_code == 202
        assert resp_topics.json()["status"] == "queued"

    @pytest.mark.anyio
    async def test_inject_context_success(self, client):
        from datetime import datetime

        from src.models import Channel, Message

        db = client._transport.app.state.db
        thread_id = await db.create_agent_thread("Context")
        await db.add_channel(Channel(channel_id=1, title="Context Channel"))
        await db.insert_message(
            Message(channel_id=1, message_id=1, text="ctx", date=datetime.now())
        )

        resp = await client.post(f"/agent/threads/{thread_id}/context", json={"channel_id": "1"})
        assert resp.status_code == 200
        assert "Context Channel" in resp.json()["content"]

    @pytest.mark.anyio
    async def test_chat_stream_and_stop(self, client):
        db = client._transport.app.state.db
        thread_id = await db.create_agent_thread("Новый тред")

        # Mock AgentManager
        agent_manager = AsyncMock()

        async def mock_stream(*args, **kwargs):
            yield 'data: {"done": true, "full_text": "Final"}\n\n'

        agent_manager.chat_stream = mock_stream
        agent_manager.estimate_prompt_tokens = AsyncMock(return_value=10)
        agent_manager.cancel_stream = AsyncMock(return_value=True)
        agent_manager.get_runtime_status = AsyncMock(
            return_value=SimpleNamespace(
                claude_available=True,
                deepagents_available=False,
                dev_mode_enabled=False,
                backend_override="auto",
                selected_backend="claude",
                fallback_model="",
                fallback_provider="",
                using_override=False,
                error=None,
            )
        )
        client._transport.app.state.agent_manager = agent_manager

        resp = await client.post(
            f"/agent/threads/{thread_id}/chat", json={"message": "First message"}
        )
        assert resp.status_code == 200
        assert "Final" in resp.text

        # Check thread was renamed and message saved
        thread = await db.get_agent_thread(thread_id)
        assert "First message" in thread["title"]
        messages = await db.get_agent_messages(thread_id)
        assert len(messages) == 2  # User + Assistant

        # Test stop
        resp_stop = await client.post(f"/agent/threads/{thread_id}/stop")
        assert resp_stop.status_code == 200
        agent_manager.cancel_stream.assert_called_once_with(thread_id)


@pytest.mark.anyio
async def test_test_notification_no_bot(client):
    """Route now queues worker-side notification test command."""
    db = client._transport.app.state.db
    resp = await client.post("/scheduler/test-notification", follow_redirects=False)
    assert resp.status_code == 303
    assert "msg=test_notification_queued" in resp.headers.get("location", "")
    commands = await db.repos.telegram_commands.list_commands(limit=1)
    assert commands[0].command_type == "notifications.test"


@pytest.mark.anyio
async def test_test_notification_no_queries(client, monkeypatch):
    """Notification route queues command even without matching queries."""
    db = client._transport.app.state.db
    resp = await client.post("/scheduler/test-notification", follow_redirects=False)
    assert resp.status_code == 303
    assert "msg=test_notification_queued" in resp.headers.get("location", "")
    commands = await db.repos.telegram_commands.list_commands(limit=1)
    assert commands[0].command_type == "notifications.test"


@pytest.mark.anyio
async def test_test_notification_no_messages(client, monkeypatch):
    """Active query exists but web still only queues notification command."""
    from src.models import SearchQuery

    db = client._transport.app.state.db

    await db.repos.search_queries.add(
        SearchQuery(query="testword", notify_on_collect=True, is_active=True)
    )

    resp = await client.post("/scheduler/test-notification")
    assert resp.status_code == 200
    commands = await db.repos.telegram_commands.list_commands(limit=1)
    assert commands[0].command_type == "notifications.test"


@pytest.mark.anyio
async def test_test_notification_with_public_channel_message(client, monkeypatch):
    """Message preview generation moved to worker; web only queues command."""
    from datetime import datetime, timezone

    from src.models import Channel, Message, SearchQuery

    db = client._transport.app.state.db

    await db.repos.search_queries.add(
        SearchQuery(query="hello", notify_on_collect=True, is_active=True)
    )
    await db.add_channel(Channel(channel_id=-100999, username="mychan", title="My Channel"))
    await db.insert_message(
        Message(
            channel_id=-100999,
            message_id=42,
            text="hello world",
            date=datetime.now(timezone.utc),
        )
    )

    resp = await client.post("/scheduler/test-notification")
    assert resp.status_code == 200
    commands = await db.repos.telegram_commands.list_commands(limit=1)
    assert commands[0].command_type == "notifications.test"


@pytest.mark.anyio
async def test_test_notification_with_private_channel_message(client, monkeypatch):
    """Private channel preview generation moved to worker; web only queues command."""
    from datetime import datetime, timezone

    from src.models import Channel, Message, SearchQuery

    db = client._transport.app.state.db

    await db.repos.search_queries.add(
        SearchQuery(query="secret", notify_on_collect=True, is_active=True)
    )
    await db.add_channel(Channel(channel_id=-100888, username=None, title="Private Chan"))
    await db.insert_message(
        Message(
            channel_id=-100888,
            message_id=77,
            text="secret data here",
            date=datetime.now(timezone.utc),
        )
    )

    resp = await client.post("/scheduler/test-notification")
    assert resp.status_code == 200
    commands = await db.repos.telegram_commands.list_commands(limit=1)
    assert commands[0].command_type == "notifications.test"


@pytest.mark.anyio
async def test_test_notification_notify_fails(client, monkeypatch):
    """Route now queues command and leaves success/failure to worker result."""
    db = client._transport.app.state.db
    resp = await client.post("/scheduler/test-notification", follow_redirects=False)
    assert resp.status_code == 303
    assert "msg=test_notification_queued" in resp.headers.get("location", "")
    commands = await db.repos.telegram_commands.list_commands(limit=1)
    assert commands[0].command_type == "notifications.test"


# --- Global error handler ---


@pytest.fixture
async def error_client(client):
    """Client with raise_app_exceptions=False so exception handler is tested."""
    app = client._transport.app

    @app.get("/test-500")
    async def _blow_up():
        raise RuntimeError("kaboom")

    transport = ASGITransport(app=app, raise_app_exceptions=False)
    auth_header = base64.b64encode(b":testpass").decode()
    async with AsyncClient(
        transport=transport,
        base_url="http://test",
        headers={"Authorization": f"Basic {auth_header}", "Origin": "http://test"},
    ) as c:
        yield c


@pytest.mark.anyio
async def test_unhandled_exception_returns_error_page(error_client):
    """Unhandled exception in a route returns 500 with error template."""
    resp = await error_client.get("/test-500")
    assert resp.status_code == 500
    assert "Ошибка сервера" in resp.text
    assert "kaboom" not in resp.text  # exception detail must not leak
    assert "/debug/" in resp.text


@pytest.mark.anyio
async def test_unhandled_exception_htmx_returns_fragment(error_client):
    """HTMX request to a broken route returns an alert fragment, not full page."""
    resp = await error_client.get("/test-500", headers={"HX-Request": "true"})
    assert resp.status_code == 500
    assert "alert-danger" in resp.text
    assert "/debug/" in resp.text


@pytest.mark.anyio
async def test_unhandled_exception_logged_to_logbuffer(error_client):
    """Exception traceback is captured by logger (and thus by LogBuffer)."""
    app = error_client._transport.app
    log_buffer = app.state.log_buffer

    initial_count = len(log_buffer.get_records())
    await error_client.get("/test-500")

    new_records = log_buffer.get_records()[initial_count:]
    assert any("kaboom" in r["message"] for r in new_records)


@pytest.mark.anyio
async def test_scheduler_job_toggle_invalid_id(client):
    """POST /scheduler/jobs/{job_id}/toggle with invalid job_id redirects to error."""
    resp = await client.post("/scheduler/jobs/bogus_job/toggle", follow_redirects=False)
    assert resp.status_code == 303
    assert "error=invalid_job" in resp.headers["location"]


@pytest.mark.anyio
async def test_scheduler_job_toggle_enables_and_disables(client):
    """Toggling collect_all job persists disabled flag to settings."""
    db = client._transport.app.state.db

    # First toggle: should disable (default is enabled)
    resp = await client.post("/scheduler/jobs/collect_all/toggle", follow_redirects=False)
    assert resp.status_code == 303
    assert await db.repos.settings.get_setting("scheduler_job_disabled:collect_all") == "1"

    # Second toggle: should re-enable
    resp = await client.post("/scheduler/jobs/collect_all/toggle", follow_redirects=False)
    assert resp.status_code == 303
    assert await db.repos.settings.get_setting("scheduler_job_disabled:collect_all") == "0"


@pytest.mark.anyio
async def test_scheduler_job_set_interval_invalid_id(client):
    """POST /scheduler/jobs/{job_id}/set-interval with invalid job_id redirects to error."""
    resp = await client.post(
        "/scheduler/jobs/malicious_id/set-interval",
        data={"interval_minutes": "10"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "error=invalid_job" in resp.headers["location"]


@pytest.mark.anyio
async def test_scheduler_job_set_interval_collect_all(client):
    """Setting interval for collect_all persists to settings."""
    db = client._transport.app.state.db
    resp = await client.post(
        "/scheduler/jobs/collect_all/set-interval",
        data={"interval_minutes": "45"},
        follow_redirects=False,
    )
    assert resp.status_code == 303
    assert "msg=interval_updated" in resp.headers["location"]
    assert await db.repos.settings.get_setting("collect_interval_minutes") == "45"


@pytest.mark.anyio
async def test_scheduler_job_set_interval_clamps_values(client):
    """Interval is clamped to 1–1440 range."""
    db = client._transport.app.state.db
    await client.post(
        "/scheduler/jobs/collect_all/set-interval",
        data={"interval_minutes": "0"},
        follow_redirects=False,
    )
    assert await db.repos.settings.get_setting("collect_interval_minutes") == "1"

    await client.post(
        "/scheduler/jobs/collect_all/set-interval",
        data={"interval_minutes": "9999"},
        follow_redirects=False,
    )
    assert await db.repos.settings.get_setting("collect_interval_minutes") == "1440"


@pytest.mark.anyio
async def test_scheduler_page_shows_disabled_job(client):
    """Disabled job shows unchecked checkbox on scheduler page."""
    db = client._transport.app.state.db
    await db.repos.settings.set_setting("scheduler_job_disabled:collect_all", "1")
    resp = await client.get("/scheduler/")
    assert resp.status_code == 200
    # The disabled job row should not have 'checked' for collect_all checkbox
    # and the label should have text-muted class
    assert "collect_all" in resp.text


@pytest.mark.anyio
async def test_analytics_page_empty(client):
    """Analytics page renders without error when no messages exist."""
    resp = await client.get("/analytics")
    assert resp.status_code == 200
    assert "Аналитика" in resp.text


@pytest.mark.anyio
async def test_analytics_page_with_date_filter(client):
    """Analytics page accepts date_from/date_to query params."""
    resp = await client.get("/analytics?date_from=2025-01-01&date_to=2025-12-31&limit=20")
    assert resp.status_code == 200
    assert "Аналитика" in resp.text
    assert 'value="2025-01-01"' in resp.text
    assert 'value="2025-12-31"' in resp.text


# ── Backend override validation tests ──────────────────────────────────────────────


@pytest.mark.anyio
async def test_save_agent_rejects_deepagents_override_without_providers(client):
    """Deepagents override is rejected when no valid providers are configured."""
    db = client._transport.app.state.db

    resp = await client.post(
        "/settings/save-agent",
        data={
            "agent_form_scope": "dev_mode",
            "agent_dev_mode_enabled": "1",
            "agent_dev_mode_disclaimer": "1",
            "agent_backend_override": "deepagents",
        },
        follow_redirects=False,
    )

    assert resp.status_code == 303
    assert "error=agent_backend_no_valid_providers" in resp.headers["location"]
    assert await db.get_setting("agent_backend_override") != "deepagents"


@pytest.mark.anyio
async def test_save_agent_rejects_claude_override_without_api_key(client, monkeypatch):
    """Claude override is rejected when no API key is available."""
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    monkeypatch.delenv("CLAUDE_CODE_OAUTH_TOKEN", raising=False)
    db = client._transport.app.state.db

    resp = await client.post(
        "/settings/save-agent",
        data={
            "agent_form_scope": "dev_mode",
            "agent_dev_mode_enabled": "1",
            "agent_dev_mode_disclaimer": "1",
            "agent_backend_override": "claude",
        },
        follow_redirects=False,
    )

    assert resp.status_code == 303
    assert "error=agent_backend_claude_unavailable" in resp.headers["location"]
    assert await db.get_setting("agent_backend_override") != "claude"


@pytest.mark.anyio
async def test_save_agent_accepts_auto_override_always(client):
    """Auto override is always accepted regardless of provider/key state."""
    db = client._transport.app.state.db

    resp = await client.post(
        "/settings/save-agent",
        data={
            "agent_form_scope": "dev_mode",
            "agent_dev_mode_enabled": "1",
            "agent_dev_mode_disclaimer": "1",
            "agent_backend_override": "auto",
        },
        follow_redirects=False,
    )

    assert resp.status_code == 303
    assert "msg=agent_saved" in resp.headers["location"]
    assert await db.get_setting("agent_backend_override") == "auto"


@pytest.mark.anyio
async def test_flood_status_returns_ok_for_expired(tmp_path, real_pool_harness_factory):
    """GET /settings/flood-status returns ok/0 when flood_wait_until is in the past."""
    from datetime import timedelta

    config = make_test_config(tmp_path)
    harness = real_pool_harness_factory()
    app, db = await build_web_app(config, harness)
    try:
        past = datetime.now(timezone.utc) - timedelta(hours=1)
        await db.add_account(Account(phone="+70010", session_string="s10", is_active=True))
        await db.update_account_flood("+70010", past)

        async with make_auth_client(app) as c:
            resp = await c.get("/settings/flood-status")

        assert resp.status_code == 200
        data = resp.json()
        entry = next(d for d in data if d["phone"] == "+70010")
        assert entry["flood_wait_until"] == "ok"
        assert entry["remaining_seconds"] == 0
    finally:
        await app.state.collection_queue.shutdown()
        await db.close()


@pytest.mark.anyio
async def test_settings_clears_stale_flood_on_load(tmp_path, real_pool_harness_factory, monkeypatch):
    """GET /settings clears flood_wait_until from DB when the timestamp is expired."""
    from datetime import timedelta

    from src.web.settings import handlers as settings_handlers

    config = make_test_config(tmp_path)
    harness = real_pool_harness_factory()
    app, db = await build_web_app(config, harness)
    try:
        past = datetime.now(timezone.utc) - timedelta(hours=1)
        await db.add_account(Account(phone="+70011", session_string="s11", is_active=True))
        await db.update_account_flood("+70011", past)

        probe_mock = AsyncMock(return_value=("ok", None))
        monkeypatch.setattr(settings_handlers, "_probe_provider_config", probe_mock)
        fake_manager = SimpleNamespace(available=False)
        monkeypatch.setattr(
            settings_handlers, "_settings_agent_manager", lambda request: (fake_manager, False)
        )

        async with make_auth_client(app) as c:
            resp = await c.get("/settings")

        assert resp.status_code == 200

        accounts = await db.get_accounts()
        acc = next(a for a in accounts if a.phone == "+70011")
        assert acc.flood_wait_until is None
    finally:
        await app.state.collection_queue.shutdown()
        await db.close()
