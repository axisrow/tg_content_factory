from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from src.agent.manager import AgentManager
from src.agent.provider_registry import ProviderRuntimeConfig
from src.config import AppConfig
from src.services.agent_provider_service import (
    MODEL_CACHE_SETTINGS_KEY,
    PROVIDER_SETTINGS_KEY,
    AgentProviderService,
    ProviderModelCacheEntry,
    ProviderModelCompatibilityRecord,
)


@pytest.mark.asyncio
async def test_provider_configs_are_encrypted_in_settings(db):
    config = AppConfig()
    config.security.session_encryption_key = "provider-secret"
    service = AgentProviderService(db, config)

    await service.save_provider_configs(
        [
            ProviderRuntimeConfig(
                provider="openai",
                enabled=True,
                priority=0,
                selected_model="gpt-4.1-mini",
                plain_fields={"base_url": "https://api.openai.com/v1"},
                secret_fields={"api_key": "sk-test"},
            )
        ]
    )

    raw = await db.get_setting(PROVIDER_SETTINGS_KEY)
    assert raw is not None
    assert "sk-test" not in raw
    assert "enc:v2:" in raw

    loaded = await service.load_provider_configs()
    assert loaded[0].secret_fields["api_key"] == "sk-test"


@pytest.mark.asyncio
async def test_load_provider_configs_tolerates_undecryptable_secrets(db):
    write_config = AppConfig()
    write_config.security.session_encryption_key = "provider-secret"
    writer = AgentProviderService(db, write_config)
    await writer.save_provider_configs(
        [
            ProviderRuntimeConfig(
                provider="openai",
                enabled=True,
                priority=0,
                selected_model="gpt-4.1-mini",
                secret_fields={"api_key": "sk-test"},
            )
        ]
    )

    read_config = AppConfig()
    read_config.security.session_encryption_key = "different-secret"
    reader = AgentProviderService(db, read_config)

    loaded = await reader.load_provider_configs()

    assert len(loaded) == 1
    assert loaded[0].provider == "openai"
    assert loaded[0].secret_fields == {}
    assert "could not be decrypted" in loaded[0].last_validation_error


@pytest.mark.asyncio
async def test_refresh_models_uses_static_cache_on_live_fetch_failure(db, monkeypatch):
    config = AppConfig()
    config.security.session_encryption_key = "provider-secret"
    service = AgentProviderService(db, config)

    async def _broken_fetch(spec, cfg):
        raise RuntimeError("upstream unavailable")

    monkeypatch.setattr(service, "_fetch_live_models", _broken_fetch)

    entry = await service.refresh_models_for_provider("openai")

    assert entry.source == "static cache"
    assert entry.models
    saved = json.loads(await db.get_setting(MODEL_CACHE_SETTINGS_KEY) or "{}")
    assert saved["openai"]["source"] == "static cache"


@pytest.mark.asyncio
async def test_refresh_models_uses_live_source_on_success(db, monkeypatch):
    config = AppConfig()
    config.security.session_encryption_key = "provider-secret"
    service = AgentProviderService(db, config)

    async def _live_fetch(spec, cfg):
        return ["gpt-4.1", "gpt-4.1-mini"]

    monkeypatch.setattr(service, "_fetch_live_models", _live_fetch)

    entry = await service.refresh_models_for_provider("openai")

    assert entry.source == "live"
    assert entry.models == ["gpt-4.1", "gpt-4.1-mini"]


@pytest.mark.asyncio
async def test_refresh_all_models_only_refreshes_configured_providers(db, monkeypatch):
    config = AppConfig()
    config.security.session_encryption_key = "provider-secret"
    service = AgentProviderService(db, config)
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

    calls: list[str] = []

    async def _fake_refresh(provider_name: str, cfg: ProviderRuntimeConfig | None = None):
        calls.append(provider_name)
        return ProviderModelCacheEntry(provider=provider_name, models=["gpt-4.1-mini"], source="live")

    monkeypatch.setattr(service, "refresh_models_for_provider", _fake_refresh)

    results = await service.refresh_all_models()

    assert calls == ["openai"]
    assert set(results) == {"openai"}


@pytest.mark.asyncio
async def test_load_model_cache_supports_legacy_entries_without_compatibility(db):
    await db.set_setting(
        MODEL_CACHE_SETTINGS_KEY,
        json.dumps(
            {
                "openai": {
                    "models": ["gpt-4.1-mini"],
                    "source": "live",
                    "fetched_at": "2026-03-12T00:00:00+00:00",
                    "error": "",
                }
            }
        ),
    )
    service = AgentProviderService(db, AppConfig())

    cache = await service.load_model_cache()

    assert "openai" in cache
    assert cache["openai"].models == ["gpt-4.1-mini"]
    assert cache["openai"].compatibility == {}


@pytest.mark.asyncio
async def test_ensure_model_compatibility_reuses_fresh_cached_result(db):
    config = AppConfig()
    config.security.session_encryption_key = "provider-secret"
    service = AgentProviderService(db, config)
    cfg = ProviderRuntimeConfig(
        provider="openai",
        enabled=True,
        priority=0,
        selected_model="gpt-4.1-mini",
        secret_fields={"api_key": "openai-key"},
    )
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

    async def _probe_runner(_cfg, _probe_kind):
        raise AssertionError("probe runner should not be called for a fresh cached result")

    result = await service.ensure_model_compatibility(cfg, probe_runner=_probe_runner)

    assert result.status == "supported"
    assert result.config_fingerprint == fingerprint


@pytest.mark.asyncio
async def test_ensure_model_compatibility_force_bypasses_cached_result(db):
    config = AppConfig()
    config.security.session_encryption_key = "provider-secret"
    service = AgentProviderService(db, config)
    cfg = ProviderRuntimeConfig(
        provider="openai",
        enabled=True,
        priority=0,
        selected_model="gpt-4.1-mini",
        secret_fields={"api_key": "openai-key"},
    )
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
    calls = {"count": 0}

    async def _probe_runner(_cfg, _probe_kind):
        calls["count"] += 1
        return ProviderModelCompatibilityRecord(
            model="gpt-4.1-mini",
            status="unsupported",
            reason="forced reprobe",
            tested_at="2026-03-12T01:00:00+00:00",
            config_fingerprint=fingerprint,
            probe_kind="dev-bulk",
        )

    result = await service.ensure_model_compatibility(
        cfg,
        probe_runner=_probe_runner,
        force=True,
    )

    assert calls["count"] == 1
    assert result.status == "unsupported"
    assert result.reason == "forced reprobe"


def test_compatibility_error_ignores_stale_unsupported_result(db):
    service = AgentProviderService(db, AppConfig())
    cfg = ProviderRuntimeConfig(
        provider="openai",
        enabled=True,
        priority=0,
        selected_model="gpt-4.1-mini",
        secret_fields={"api_key": "openai-key"},
    )
    fingerprint = service.config_fingerprint(cfg)
    cache_entry = ProviderModelCacheEntry(
        provider="openai",
        models=["gpt-4.1-mini"],
        source="live",
        compatibility={
            fingerprint: ProviderModelCompatibilityRecord(
                model="gpt-4.1-mini",
                status="unsupported",
                reason="stale unsupported",
                tested_at="2026-03-10T00:00:00+00:00",
                config_fingerprint=fingerprint,
                probe_kind="auto-select",
            )
        },
    )

    assert service.compatibility_error_for_config(cfg, cache_entry) == ""


def test_config_fingerprint_depends_on_routing_fields(db):
    service = AgentProviderService(db, AppConfig())
    cfg_local = ProviderRuntimeConfig(
        provider="ollama",
        enabled=True,
        priority=0,
        selected_model="gpt-oss:120b",
        plain_fields={"base_url": "http://localhost:11434"},
    )
    cfg_cloud = ProviderRuntimeConfig(
        provider="ollama",
        enabled=True,
        priority=0,
        selected_model="gpt-oss:120b",
        plain_fields={"base_url": "https://ollama.com"},
        secret_fields={"api_key": "ollama-key"},
    )

    assert service.config_fingerprint(cfg_local) != service.config_fingerprint(cfg_cloud)


@pytest.mark.asyncio
async def test_fetch_ollama_models_supports_cloud_api_key(db, monkeypatch):
    config = AppConfig()
    config.security.session_encryption_key = "provider-secret"
    service = AgentProviderService(db, config)
    captured: dict[str, object] = {}

    async def _fake_fetch_json(url: str, headers=None):
        captured["url"] = url
        captured["headers"] = headers
        return {"models": [{"name": "gpt-oss:120b"}]}

    monkeypatch.setattr(service, "_fetch_json", _fake_fetch_json)

    models = await service._fetch_ollama_models("", "ollama-key")

    assert models == ["gpt-oss:120b"]
    assert captured["url"] == "https://ollama.com/api/tags"
    assert captured["headers"] == {"Authorization": "Bearer ollama-key"}


@pytest.mark.asyncio
async def test_fetch_ollama_models_normalizes_cloud_api_base_url(db, monkeypatch):
    config = AppConfig()
    config.security.session_encryption_key = "provider-secret"
    service = AgentProviderService(db, config)
    captured: dict[str, object] = {}

    async def _fake_fetch_json(url: str, headers=None):
        captured["url"] = url
        captured["headers"] = headers
        return {"models": [{"name": "gpt-oss:120b"}]}

    monkeypatch.setattr(service, "_fetch_json", _fake_fetch_json)

    models = await service._fetch_ollama_models("https://ollama.com/api", "ollama-key")

    assert models == ["gpt-oss:120b"]
    assert captured["url"] == "https://ollama.com/api/tags"
    assert captured["headers"] == {"Authorization": "Bearer ollama-key"}


@pytest.mark.asyncio
async def test_fetch_ollama_models_normalizes_local_api_base_url(db, monkeypatch):
    config = AppConfig()
    config.security.session_encryption_key = "provider-secret"
    service = AgentProviderService(db, config)
    captured: dict[str, object] = {}

    async def _fake_fetch_json(url: str, headers=None):
        captured["url"] = url
        captured["headers"] = headers
        return {"models": [{"name": "llama3.2"}]}

    monkeypatch.setattr(service, "_fetch_json", _fake_fetch_json)

    models = await service._fetch_ollama_models("http://localhost:11434/api", "")

    assert models == ["llama3.2"]
    assert captured["url"] == "http://localhost:11434/api/tags"
    assert captured["headers"] is None


@pytest.mark.asyncio
async def test_agent_manager_prefers_db_provider_configs_over_legacy_env(db, monkeypatch):
    config = AppConfig()
    config.security.session_encryption_key = "provider-secret"
    config.agent.fallback_model = "anthropic:claude-sonnet-4-5-20250929"
    config.agent.fallback_api_key = "legacy-key"

    service = AgentProviderService(db, config)
    await service.save_provider_configs(
        [
            ProviderRuntimeConfig(
                provider="openai",
                enabled=True,
                priority=0,
                selected_model="gpt-4.1-mini",
                secret_fields={"api_key": "db-key"},
            )
        ]
    )

    thread_id = await db.create_agent_thread("db-first")
    await db.save_agent_message(thread_id, "user", "hello")
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    monkeypatch.delenv("CLAUDE_CODE_OAUTH_TOKEN", raising=False)

    mgr = AgentManager(db, config)
    await mgr.refresh_settings_cache()

    def fake_init_chat_model(*, model, model_provider, **kwargs):
        assert model_provider == "openai"
        assert model == "gpt-4.1-mini"
        assert kwargs["api_key"] == "db-key"
        return SimpleNamespace(model_provider=model_provider)

    create_agent = MagicMock(return_value=MagicMock(run=MagicMock(return_value="ok-db")))

    with patch("langchain.chat_models.init_chat_model", fake_init_chat_model), patch(
        "deepagents.create_deep_agent", create_agent
    ):
        chunks = [chunk async for chunk in mgr.chat_stream(thread_id, "hello")]

    assert any('"provider": "openai"' in chunk for chunk in chunks)


@pytest.mark.asyncio
async def test_agent_manager_falls_back_to_legacy_env_when_db_provider_is_unsupported(
    db, monkeypatch
):
    config = AppConfig()
    config.security.session_encryption_key = "provider-secret"
    config.agent.fallback_model = "anthropic:claude-sonnet-4-5-20250929"
    config.agent.fallback_api_key = "legacy-key"

    service = AgentProviderService(db, config)
    openai_cfg = ProviderRuntimeConfig(
        provider="openai",
        enabled=True,
        priority=0,
        selected_model="gpt-4.1-mini",
        secret_fields={"api_key": "openai-key"},
    )
    await service.save_provider_configs([openai_cfg])
    openai_fingerprint = service.config_fingerprint(openai_cfg)
    await service.save_model_cache(
        {
            "openai": ProviderModelCacheEntry(
                provider="openai",
                models=["gpt-4.1-mini"],
                source="live",
                compatibility={
                    openai_fingerprint: ProviderModelCompatibilityRecord(
                        model="gpt-4.1-mini",
                        status="unsupported",
                        reason="tool-calling is broken",
                        tested_at="2026-03-12T00:00:00+00:00",
                        config_fingerprint=openai_fingerprint,
                        probe_kind="save-time",
                    )
                },
            )
        }
    )

    thread_id = await db.create_agent_thread("legacy-fallback")
    await db.save_agent_message(thread_id, "user", "hello")
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    monkeypatch.delenv("CLAUDE_CODE_OAUTH_TOKEN", raising=False)

    mgr = AgentManager(db, config)
    await mgr.refresh_settings_cache()
    seen_providers: list[str] = []

    def fake_init_chat_model(*, model, model_provider, **kwargs):
        seen_providers.append(model_provider)
        if model_provider == "anthropic":
            assert model == "claude-sonnet-4-5-20250929"
            assert kwargs["api_key"] == "legacy-key"
        return SimpleNamespace(model_provider=model_provider)

    def fake_create_agent(model, tools, system_prompt):
        del tools, system_prompt
        return MagicMock(run=MagicMock(return_value=f"ok-{model.model_provider}"))

    with patch("langchain.chat_models.init_chat_model", fake_init_chat_model), patch(
        "deepagents.create_deep_agent", fake_create_agent
    ):
        status = await mgr.get_runtime_status()
        chunks = [chunk async for chunk in mgr.chat_stream(thread_id, "hello")]

    assert status.selected_backend == "deepagents"
    assert status.deepagents_available is True
    assert status.fallback_provider == "anthropic"
    assert "openai" not in seen_providers
    assert "anthropic" in seen_providers
    assert any('"provider": "anthropic"' in chunk for chunk in chunks)


@pytest.mark.asyncio
async def test_agent_manager_fails_over_to_next_provider_on_init_error(db, monkeypatch):
    config = AppConfig()
    config.security.session_encryption_key = "provider-secret"
    service = AgentProviderService(db, config)
    await service.save_provider_configs(
        [
            ProviderRuntimeConfig(
                provider="openai",
                enabled=True,
                priority=0,
                selected_model="gpt-4.1-mini",
                secret_fields={"api_key": "openai-key"},
            ),
            ProviderRuntimeConfig(
                provider="anthropic",
                enabled=True,
                priority=1,
                selected_model="claude-sonnet-4-5-20250929",
                secret_fields={"api_key": "anthropic-key"},
            ),
        ]
    )

    thread_id = await db.create_agent_thread("failover")
    await db.save_agent_message(thread_id, "user", "hello")
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    monkeypatch.delenv("CLAUDE_CODE_OAUTH_TOKEN", raising=False)

    mgr = AgentManager(db, config)
    await mgr.refresh_settings_cache()

    def fake_init_chat_model(*, model, model_provider, **kwargs):
        if model_provider == "openai":
            raise RuntimeError("openai init failed")
        return SimpleNamespace(model_provider=model_provider)

    def fake_create_agent(model, tools, system_prompt):
        return MagicMock(run=MagicMock(return_value=f"ok-{model.model_provider}"))

    with patch("langchain.chat_models.init_chat_model", fake_init_chat_model), patch(
        "deepagents.create_deep_agent", fake_create_agent
    ):
        chunks = [chunk async for chunk in mgr.chat_stream(thread_id, "hello")]

    assert any('"provider": "anthropic"' in chunk for chunk in chunks)


@pytest.mark.asyncio
async def test_agent_manager_fails_over_to_next_provider_on_run_error(db, monkeypatch):
    config = AppConfig()
    config.security.session_encryption_key = "provider-secret"
    service = AgentProviderService(db, config)
    await service.save_provider_configs(
        [
            ProviderRuntimeConfig(
                provider="openai",
                enabled=True,
                priority=0,
                selected_model="gpt-4.1-mini",
                secret_fields={"api_key": "openai-key"},
            ),
            ProviderRuntimeConfig(
                provider="groq",
                enabled=True,
                priority=1,
                selected_model="llama-3.1-8b-instant",
                secret_fields={"api_key": "groq-key"},
            ),
        ]
    )

    thread_id = await db.create_agent_thread("run-failover")
    await db.save_agent_message(thread_id, "user", "hello")
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    monkeypatch.delenv("CLAUDE_CODE_OAUTH_TOKEN", raising=False)

    mgr = AgentManager(db, config)
    await mgr.refresh_settings_cache()

    def fake_init_chat_model(*, model, model_provider, **kwargs):
        return SimpleNamespace(model_provider=model_provider)

    def fake_create_agent(model, tools, system_prompt):
        if model.model_provider == "openai":
            return MagicMock(run=MagicMock(side_effect=RuntimeError("run failed")))
        return MagicMock(run=MagicMock(return_value="ok-groq"))

    with patch("langchain.chat_models.init_chat_model", fake_init_chat_model), patch(
        "deepagents.create_deep_agent", fake_create_agent
    ):
        chunks = [chunk async for chunk in mgr.chat_stream(thread_id, "hello")]

    assert any('"provider": "groq"' in chunk for chunk in chunks)


@pytest.mark.asyncio
async def test_ollama_cloud_provider_uses_bearer_headers(db, monkeypatch):
    config = AppConfig()
    config.security.session_encryption_key = "provider-secret"
    service = AgentProviderService(db, config)
    await service.save_provider_configs(
        [
            ProviderRuntimeConfig(
                provider="ollama",
                enabled=True,
                priority=0,
                selected_model="gpt-oss:120b",
                secret_fields={"api_key": "ollama-key"},
            )
        ]
    )

    thread_id = await db.create_agent_thread("ollama-cloud")
    await db.save_agent_message(thread_id, "user", "hello")
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    monkeypatch.delenv("CLAUDE_CODE_OAUTH_TOKEN", raising=False)

    mgr = AgentManager(db, config)
    await mgr.refresh_settings_cache()

    def fake_init_chat_model(*, model, model_provider, **kwargs):
        assert model_provider == "ollama"
        assert model == "gpt-oss:120b"
        assert kwargs["base_url"] == "https://ollama.com"
        assert kwargs["client_kwargs"] == {
            "headers": {"Authorization": "Bearer ollama-key"}
        }
        return SimpleNamespace(model_provider=model_provider)

    create_agent = MagicMock(return_value=MagicMock(run=MagicMock(return_value="ok-ollama-cloud")))

    with patch("langchain.chat_models.init_chat_model", fake_init_chat_model), patch(
        "deepagents.create_deep_agent", create_agent
    ):
        chunks = [chunk async for chunk in mgr.chat_stream(thread_id, "hello")]

    assert any('"provider": "ollama"' in chunk for chunk in chunks)


@pytest.mark.asyncio
async def test_ollama_provider_normalizes_api_suffix_for_runtime(db, monkeypatch):
    config = AppConfig()
    config.security.session_encryption_key = "provider-secret"
    service = AgentProviderService(db, config)
    await service.save_provider_configs(
        [
            ProviderRuntimeConfig(
                provider="ollama",
                enabled=True,
                priority=0,
                selected_model="gpt-oss:120b",
                plain_fields={"base_url": "https://ollama.com/api"},
                secret_fields={"api_key": "ollama-key"},
            )
        ]
    )

    thread_id = await db.create_agent_thread("ollama-cloud-api-suffix")
    await db.save_agent_message(thread_id, "user", "hello")
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    monkeypatch.delenv("CLAUDE_CODE_OAUTH_TOKEN", raising=False)

    mgr = AgentManager(db, config)
    await mgr.refresh_settings_cache()

    def fake_init_chat_model(*, model, model_provider, **kwargs):
        assert model_provider == "ollama"
        assert model == "gpt-oss:120b"
        assert kwargs["base_url"] == "https://ollama.com"
        assert kwargs["client_kwargs"] == {
            "headers": {"Authorization": "Bearer ollama-key"}
        }
        return SimpleNamespace(model_provider=model_provider)

    create_agent = MagicMock(return_value=MagicMock(run=MagicMock(return_value="ok-ollama-cloud")))

    with patch("langchain.chat_models.init_chat_model", fake_init_chat_model), patch(
        "deepagents.create_deep_agent", create_agent
    ):
        chunks = [chunk async for chunk in mgr.chat_stream(thread_id, "hello")]

    assert any('"provider": "ollama"' in chunk for chunk in chunks)


@pytest.mark.asyncio
async def test_runtime_status_reports_db_provider_preflight_failure(db, monkeypatch):
    config = AppConfig()
    config.security.session_encryption_key = "provider-secret"
    service = AgentProviderService(db, config)
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
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    monkeypatch.delenv("CLAUDE_CODE_OAUTH_TOKEN", raising=False)

    mgr = AgentManager(db, config)

    with patch.object(
        mgr._deepagents_backend,
        "_build_agent",
        side_effect=RuntimeError("provider init failed"),
    ):
        status = await mgr.get_runtime_status()

    assert status.selected_backend is None
    assert status.deepagents_available is False
    assert "provider init failed" in (status.error or "")


@pytest.mark.asyncio
async def test_agent_manager_skips_cached_unsupported_provider_and_fails_over(db, monkeypatch):
    config = AppConfig()
    config.security.session_encryption_key = "provider-secret"
    service = AgentProviderService(db, config)
    openai_cfg = ProviderRuntimeConfig(
        provider="openai",
        enabled=True,
        priority=0,
        selected_model="gpt-4.1-mini",
        secret_fields={"api_key": "openai-key"},
    )
    anthropic_cfg = ProviderRuntimeConfig(
        provider="anthropic",
        enabled=True,
        priority=1,
        selected_model="claude-sonnet-4-5-20250929",
        secret_fields={"api_key": "anthropic-key"},
    )
    await service.save_provider_configs([openai_cfg, anthropic_cfg])
    openai_fingerprint = service.config_fingerprint(openai_cfg)
    await service.save_model_cache(
        {
            "openai": ProviderModelCacheEntry(
                provider="openai",
                models=["gpt-4.1-mini"],
                source="live",
                compatibility={
                    openai_fingerprint: ProviderModelCompatibilityRecord(
                        model="gpt-4.1-mini",
                        status="unsupported",
                        reason="tool-calling is broken",
                        tested_at="2026-03-12T00:00:00+00:00",
                        config_fingerprint=openai_fingerprint,
                        probe_kind="save-time",
                    )
                },
            )
        }
    )

    thread_id = await db.create_agent_thread("compat-failover")
    await db.save_agent_message(thread_id, "user", "hello")
    monkeypatch.delenv("ANTHROPIC_API_KEY", raising=False)
    monkeypatch.delenv("CLAUDE_CODE_OAUTH_TOKEN", raising=False)

    mgr = AgentManager(db, config)
    await mgr.refresh_settings_cache()

    seen_providers: list[str] = []

    def fake_init_chat_model(*, model, model_provider, **kwargs):
        del model, kwargs
        seen_providers.append(model_provider)
        return SimpleNamespace(model_provider=model_provider)

    def fake_create_agent(model, tools, system_prompt):
        del tools, system_prompt
        return MagicMock(run=MagicMock(return_value=f"ok-{model.model_provider}"))

    with patch("langchain.chat_models.init_chat_model", fake_init_chat_model), patch(
        "deepagents.create_deep_agent", fake_create_agent
    ):
        chunks = [chunk async for chunk in mgr.chat_stream(thread_id, "hello")]

    assert "openai" not in seen_providers
    assert seen_providers
    assert any('"provider": "anthropic"' in chunk for chunk in chunks)
