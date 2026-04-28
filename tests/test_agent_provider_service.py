from __future__ import annotations

import json
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from src.agent.manager import AgentManager
from src.agent.provider_registry import ProviderRuntimeConfig, provider_spec
from src.config import AppConfig
from src.services.agent_provider_service import (
    MODEL_CACHE_SETTINGS_KEY,
    PROVIDER_SETTINGS_KEY,
    AgentProviderService,
    ProviderModelCacheEntry,
    ProviderModelCompatibilityRecord,
)


@pytest.mark.anyio
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


@pytest.mark.anyio
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


@pytest.mark.anyio
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


@pytest.mark.anyio
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


@pytest.mark.anyio
async def test_fetch_google_genai_models_uses_api_key_header(db, monkeypatch):
    service = AgentProviderService(db, AppConfig())
    captured: dict[str, object] = {}

    async def _fake_fetch_json(url: str, headers: dict[str, str] | None = None):
        captured["url"] = url
        captured["headers"] = headers
        return {"models": [{"name": "models/gemini-2.5-pro"}]}

    monkeypatch.setattr(service, "_fetch_json", _fake_fetch_json)

    models = await service._fetch_google_genai_models("google-api-key")

    assert models == ["gemini-2.5-pro"]
    assert captured["url"] == "https://generativelanguage.googleapis.com/v1beta/models"
    assert captured["headers"] == {"x-goog-api-key": "google-api-key"}


@pytest.mark.anyio
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
        return ProviderModelCacheEntry(
            provider=provider_name, models=["gpt-4.1-mini"], source="live"
        )

    monkeypatch.setattr(service, "refresh_models_for_provider", _fake_refresh)

    results = await service.refresh_all_models()

    assert calls == ["openai"]
    assert set(results) == {"openai"}


def test_validate_provider_config_returns_error_for_unknown_provider(db):
    service = AgentProviderService(db, AppConfig())
    cfg = ProviderRuntimeConfig(
        provider="unknown",
        enabled=True,
        priority=0,
        selected_model="unknown:model",
    )

    assert service.validate_provider_config(cfg) == "Unknown provider: unknown"


@pytest.mark.anyio
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


@pytest.mark.anyio
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
                        tested_at=datetime.now(UTC).isoformat(),
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


@pytest.mark.anyio
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
                        tested_at=datetime.now(UTC).isoformat(),
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
            tested_at=datetime.now(UTC).isoformat(),
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


@pytest.mark.anyio
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


@pytest.mark.anyio
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


@pytest.mark.anyio
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


@pytest.mark.anyio
async def test_agent_manager_prefers_db_provider_configs_over_legacy_env(db, monkeypatch):
    config = AppConfig()
    config.security.session_encryption_key = "provider-secret"
    config.agent.fallback_model = "anthropic:claude-sonnet-4-6"
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

    with (
        patch("langchain.chat_models.init_chat_model", fake_init_chat_model),
        patch("deepagents.create_deep_agent", create_agent),
    ):
        chunks = [chunk async for chunk in mgr.chat_stream(thread_id, "hello")]

    assert any('"provider": "openai"' in chunk for chunk in chunks)


@pytest.mark.anyio
async def test_agent_manager_falls_back_to_legacy_env_when_db_provider_is_unsupported(
    db, monkeypatch
):
    config = AppConfig()
    config.security.session_encryption_key = "provider-secret"
    config.agent.fallback_model = "anthropic:claude-sonnet-4-6"
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
                        tested_at=datetime.now(UTC).isoformat(),
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
            assert model == "claude-sonnet-4-6"
            assert kwargs["api_key"] == "legacy-key"
        return SimpleNamespace(model_provider=model_provider)

    def fake_create_agent(model, tools, system_prompt):
        del tools, system_prompt
        return MagicMock(run=MagicMock(return_value=f"ok-{model.model_provider}"))

    with (
        patch("langchain.chat_models.init_chat_model", fake_init_chat_model),
        patch("deepagents.create_deep_agent", fake_create_agent),
    ):
        status = await mgr.get_runtime_status()
        chunks = [chunk async for chunk in mgr.chat_stream(thread_id, "hello")]

    assert status.selected_backend == "deepagents"
    assert status.deepagents_available is True
    assert status.fallback_provider == "anthropic"
    assert "openai" not in seen_providers
    assert "anthropic" in seen_providers
    assert any('"provider": "anthropic"' in chunk for chunk in chunks)


@pytest.mark.anyio
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
                selected_model="claude-sonnet-4-6",
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

    with (
        patch("langchain.chat_models.init_chat_model", fake_init_chat_model),
        patch("deepagents.create_deep_agent", fake_create_agent),
    ):
        chunks = [chunk async for chunk in mgr.chat_stream(thread_id, "hello")]

    assert any('"provider": "anthropic"' in chunk for chunk in chunks)


@pytest.mark.anyio
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

    with (
        patch("langchain.chat_models.init_chat_model", fake_init_chat_model),
        patch("deepagents.create_deep_agent", fake_create_agent),
    ):
        chunks = [chunk async for chunk in mgr.chat_stream(thread_id, "hello")]

    assert any('"provider": "groq"' in chunk for chunk in chunks)


@pytest.mark.anyio
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
        assert kwargs["client_kwargs"] == {"headers": {"Authorization": "Bearer ollama-key"}}
        return SimpleNamespace(model_provider=model_provider)

    create_agent = MagicMock(return_value=MagicMock(run=MagicMock(return_value="ok-ollama-cloud")))

    with (
        patch("langchain.chat_models.init_chat_model", fake_init_chat_model),
        patch("deepagents.create_deep_agent", create_agent),
    ):
        chunks = [chunk async for chunk in mgr.chat_stream(thread_id, "hello")]

    assert any('"provider": "ollama"' in chunk for chunk in chunks)


@pytest.mark.anyio
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
        assert kwargs["client_kwargs"] == {"headers": {"Authorization": "Bearer ollama-key"}}
        return SimpleNamespace(model_provider=model_provider)

    create_agent = MagicMock(return_value=MagicMock(run=MagicMock(return_value="ok-ollama-cloud")))

    with (
        patch("langchain.chat_models.init_chat_model", fake_init_chat_model),
        patch("deepagents.create_deep_agent", create_agent),
    ):
        chunks = [chunk async for chunk in mgr.chat_stream(thread_id, "hello")]

    assert any('"provider": "ollama"' in chunk for chunk in chunks)


@pytest.mark.anyio
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


@pytest.mark.anyio
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
        selected_model="claude-sonnet-4-6",
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
                        tested_at=datetime.now(UTC).isoformat(),
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

    with (
        patch("langchain.chat_models.init_chat_model", fake_init_chat_model),
        patch("deepagents.create_deep_agent", fake_create_agent),
    ):
        chunks = [chunk async for chunk in mgr.chat_stream(thread_id, "hello")]

    assert "openai" not in seen_providers
    assert seen_providers
    assert any('"provider": "anthropic"' in chunk for chunk in chunks)


# === _fetch_live_models HTTP error handling tests ===


@pytest.mark.anyio
async def test_fetch_live_models_http_error_fallback_to_static(db, monkeypatch):
    """_fetch_live_models propagates exception, caller handles fallback."""
    import aiohttp

    config = AppConfig()
    config.security.session_encryption_key = "provider-secret"
    service = AgentProviderService(db, config)

    # Create a config for OpenAI
    cfg = ProviderRuntimeConfig(
        provider="openai",
        enabled=True,
        priority=0,
        selected_model="gpt-4.1-mini",
        secret_fields={"api_key": "test-key"},
    )

    async def _broken_fetch_json(url, headers=None):
        raise aiohttp.ClientError("Connection refused")

    monkeypatch.setattr(service, "_fetch_json", _broken_fetch_json)

    from src.agent.provider_registry import provider_spec

    spec = provider_spec("openai")

    # _fetch_live_models raises exception, refresh_models_for_provider catches it
    with pytest.raises(aiohttp.ClientError):
        await service._fetch_live_models(spec, cfg)


@pytest.mark.anyio
async def test_fetch_live_models_success_updates_cache(db, monkeypatch):
    """_fetch_live_models returns live models on success."""
    config = AppConfig()
    config.security.session_encryption_key = "provider-secret"
    service = AgentProviderService(db, config)

    cfg = ProviderRuntimeConfig(
        provider="openai",
        enabled=True,
        priority=0,
        selected_model="gpt-4.1-mini",
        secret_fields={"api_key": "test-key"},
    )

    async def _fake_fetch_json(url, headers=None):
        return {"data": [{"id": "gpt-4.1"}, {"id": "gpt-4.1-mini"}]}

    monkeypatch.setattr(service, "_fetch_json", _fake_fetch_json)

    from src.agent.provider_registry import provider_spec

    spec = provider_spec("openai")
    models = await service._fetch_live_models(spec, cfg)

    assert "gpt-4.1" in models
    assert "gpt-4.1-mini" in models


@pytest.mark.anyio
async def test_fetch_live_models_zai_uses_bearer_auth(db, monkeypatch):
    """Z.AI live model fetch uses native API endpoint with Bearer auth."""
    config = AppConfig()
    config.security.session_encryption_key = "provider-secret"
    service = AgentProviderService(db, config)

    cfg = ProviderRuntimeConfig(
        provider="zai",
        enabled=True,
        priority=0,
        selected_model="glm-5",
        secret_fields={"api_key": "zai-key"},
    )

    captured: dict[str, object] = {}

    async def _fake_fetch_json(url, headers=None):
        captured["url"] = url
        captured["headers"] = headers
        return {"data": [{"id": "glm-5"}]}

    monkeypatch.setattr(service, "_fetch_json", _fake_fetch_json)

    spec = provider_spec("zai")
    assert spec is not None
    models = await service._fetch_live_models(spec, cfg)

    assert models == ["glm-5"]
    assert captured["url"] == "https://api.z.ai/api/paas/v4/models"
    assert captured["headers"] == {"Authorization": "Bearer zai-key"}


# === save_provider_configs encryption tests ===


@pytest.mark.anyio
async def test_save_provider_configs_requires_encryption_key(db):
    """save_provider_configs requires encryption key."""
    service = AgentProviderService(db, AppConfig())  # No encryption key

    cfg = ProviderRuntimeConfig(
        provider="openai",
        enabled=True,
        priority=0,
        selected_model="gpt-4.1-mini",
    )

    with pytest.raises(RuntimeError, match="SESSION_ENCRYPTION_KEY"):
        await service.save_provider_configs([cfg])


# === compatibility record tests ===


def test_is_compatibility_record_fresh_with_recent_record(db):
    """is_compatibility_record_fresh returns True for recent records."""
    service = AgentProviderService(db, AppConfig())

    recent_record = ProviderModelCompatibilityRecord(
        model="gpt-4.1-mini",
        status="supported",
        tested_at=datetime.now(UTC).isoformat(),
    )

    assert service.is_compatibility_record_fresh(recent_record) is True


def test_is_compatibility_record_fresh_with_stale_record(db):
    """is_compatibility_record_fresh returns False for stale records."""
    service = AgentProviderService(db, AppConfig())

    stale_record = ProviderModelCompatibilityRecord(
        model="gpt-4.1-mini",
        status="supported",
        tested_at="2024-01-01T00:00:00+00:00",
    )

    assert service.is_compatibility_record_fresh(stale_record) is False


def test_is_compatibility_record_fresh_with_invalid_date(db):
    """is_compatibility_record_fresh returns False for invalid date."""
    service = AgentProviderService(db, AppConfig())

    invalid_record = ProviderModelCompatibilityRecord(
        model="gpt-4.1-mini",
        status="supported",
        tested_at="invalid-date",
    )

    assert service.is_compatibility_record_fresh(invalid_record) is False


# === parse_provider_form tests ===


def test_parse_provider_form_handles_missing_fields(db):
    """parse_provider_form handles missing form fields gracefully."""
    config = AppConfig()
    config.security.session_encryption_key = "provider-secret"
    service = AgentProviderService(db, config)

    form = {}  # Empty form

    result = service.parse_provider_form(form, [])

    # Should return empty list when no providers are marked present
    assert result == []


def test_parse_provider_form_with_enabled_provider(db):
    """parse_provider_form parses enabled provider from form."""
    config = AppConfig()
    config.security.session_encryption_key = "provider-secret"
    service = AgentProviderService(db, config)

    form = {
        "provider_present__openai": "1",
        "provider_enabled__openai": "1",
        "provider_model__openai": "gpt-4.1-mini",
        "provider_priority__openai": "0",
    }

    result = service.parse_provider_form(form, [])

    assert len(result) == 1
    assert result[0].provider == "openai"
    assert result[0].enabled is True
    assert result[0].selected_model == "gpt-4.1-mini"


# === build_provider_views tests ===


def test_build_provider_views_includes_compatibility(db):
    """build_provider_views includes compatibility information."""
    config = AppConfig()
    config.security.session_encryption_key = "provider-secret"
    service = AgentProviderService(db, config)

    cfg = ProviderRuntimeConfig(
        provider="openai",
        enabled=True,
        priority=0,
        selected_model="gpt-4.1-mini",
        secret_fields={"api_key": "test-key"},
    )

    fingerprint = service.config_fingerprint(cfg)
    cache = {
        "openai": ProviderModelCacheEntry(
            provider="openai",
            models=["gpt-4.1-mini"],
            source="live",
            compatibility={
                fingerprint: ProviderModelCompatibilityRecord(
                    model="gpt-4.1-mini",
                    status="supported",
                    tested_at=datetime.now(UTC).isoformat(),
                    config_fingerprint=fingerprint,
                )
            },
        )
    }

    views = service.build_provider_views([cfg], cache)

    assert len(views) == 1
    assert views[0]["provider"] == "openai"
    assert views[0]["selected_compatibility"] is not None
    assert views[0]["selected_compatibility"]["status"] == "supported"


def test_build_provider_views_keeps_empty_plain_field_value(db):
    """build_provider_views keeps empty values separate from placeholders."""
    config = AppConfig()
    config.security.session_encryption_key = "provider-secret"
    service = AgentProviderService(db, config)

    cfg = ProviderRuntimeConfig(
        provider="ollama",
        enabled=True,
        priority=0,
        selected_model="llama3.1",
        plain_fields={"base_url": ""},
    )

    cache = {
        "ollama": ProviderModelCacheEntry(
            provider="ollama",
            models=["llama3.1"],
            source="live",
        )
    }

    views = service.build_provider_views([cfg], cache)
    plain_fields = {field["name"]: field for field in views[0]["plain_fields"]}

    assert plain_fields["base_url"]["value"] == ""
    assert plain_fields["base_url"]["placeholder"]


# === export_compatibility_catalog tests ===


@pytest.mark.anyio
async def test_export_compatibility_catalog_creates_file(db, tmp_path):
    """export_compatibility_catalog creates catalog file."""
    config = AppConfig()
    config.security.session_encryption_key = "provider-secret"
    service = AgentProviderService(db, config)

    cfg = ProviderRuntimeConfig(
        provider="openai",
        enabled=True,
        priority=0,
        selected_model="gpt-4.1-mini",
        secret_fields={"api_key": "test-key"},
    )

    fingerprint = service.config_fingerprint(cfg)
    cache = {
        "openai": ProviderModelCacheEntry(
            provider="openai",
            models=["gpt-4.1-mini"],
            source="live",
            compatibility={
                fingerprint: ProviderModelCompatibilityRecord(
                    model="gpt-4.1-mini",
                    status="supported",
                    tested_at=datetime.now(UTC).isoformat(),
                    config_fingerprint=fingerprint,
                )
            },
        )
    }

    export_path = tmp_path / "catalog.json"
    result = await service.export_compatibility_catalog(
        [cfg], cache, path=export_path
    )

    assert result == export_path
    assert export_path.exists()

    import json

    data = json.loads(export_path.read_text())
    assert "generated_at" in data
    assert "providers" in data


# === canonical_endpoint_fingerprint tests ===


def test_canonical_endpoint_fingerprint_for_openai(db):
    """canonical_endpoint_fingerprint returns default URL for OpenAI."""
    service = AgentProviderService(db, AppConfig())

    cfg = ProviderRuntimeConfig(
        provider="openai",
        enabled=True,
        priority=0,
        selected_model="gpt-4.1-mini",
        plain_fields={"base_url": "https://api.openai.com/v1"},
    )

    fingerprint = service.canonical_endpoint_fingerprint(cfg)

    assert fingerprint == "https://api.openai.com/v1"


def test_canonical_endpoint_fingerprint_for_custom_url_returns_none(db):
    """canonical_endpoint_fingerprint returns None for custom URLs."""
    service = AgentProviderService(db, AppConfig())

    cfg = ProviderRuntimeConfig(
        provider="openai",
        enabled=True,
        priority=0,
        selected_model="gpt-4.1-mini",
        plain_fields={"base_url": "https://custom.api.com/v1"},
    )

    fingerprint = service.canonical_endpoint_fingerprint(cfg)

    assert fingerprint is None


def test_canonical_endpoint_fingerprint_for_ollama_cloud(db):
    """canonical_endpoint_fingerprint returns 'ollama://cloud' for Ollama cloud."""
    service = AgentProviderService(db, AppConfig())

    cfg = ProviderRuntimeConfig(
        provider="ollama",
        enabled=True,
        priority=0,
        selected_model="llama3.2",
        plain_fields={"base_url": "https://ollama.com"},
        secret_fields={"api_key": "test-key"},
    )

    fingerprint = service.canonical_endpoint_fingerprint(cfg)

    assert fingerprint == "ollama://cloud"


# === Z.AI edge case tests ===


@pytest.mark.anyio
async def test_zai_fetch_models_http_error_propagates(db, monkeypatch):
    """Z.AI model fetch propagates HTTP errors (caller handles fallback)."""
    config = AppConfig()
    config.security.session_encryption_key = "provider-secret"
    service = AgentProviderService(db, config)

    cfg = ProviderRuntimeConfig(
        provider="zai",
        enabled=True,
        priority=0,
        selected_model="glm-5",
        secret_fields={"api_key": "zai-key"},
    )

    async def _failing_fetch(url, headers=None):
        raise ConnectionError("API unreachable")

    monkeypatch.setattr(service, "_fetch_json", _failing_fetch)

    spec = provider_spec("zai")
    with pytest.raises(ConnectionError, match="API unreachable"):
        await service._fetch_live_models(spec, cfg)


@pytest.mark.anyio
async def test_zai_fetch_models_empty_response(db, monkeypatch):
    """Z.AI model fetch handles empty response gracefully."""
    config = AppConfig()
    config.security.session_encryption_key = "provider-secret"
    service = AgentProviderService(db, config)

    cfg = ProviderRuntimeConfig(
        provider="zai",
        enabled=True,
        priority=0,
        selected_model="",
        secret_fields={"api_key": "zai-key"},
    )

    async def _empty_fetch(url, headers=None):
        return {}

    monkeypatch.setattr(service, "_fetch_json", _empty_fetch)

    spec = provider_spec("zai")
    models = await service._fetch_live_models(spec, cfg)
    assert models == []


@pytest.mark.anyio
async def test_zai_fetch_models_missing_api_key(db, monkeypatch):
    """Z.AI model fetch handles missing api_key gracefully."""
    config = AppConfig()
    config.security.session_encryption_key = "provider-secret"
    service = AgentProviderService(db, config)

    cfg = ProviderRuntimeConfig(
        provider="zai",
        enabled=True,
        priority=0,
        selected_model="",
        # No api_key in secret_fields
    )

    async def _fake_empty_fetch(url, headers=None):
        return {"data": []}

    monkeypatch.setattr(service, "_fetch_json", _fake_empty_fetch)

    spec = provider_spec("zai")
    models = await service._fetch_live_models(spec, cfg)
    assert models == []
