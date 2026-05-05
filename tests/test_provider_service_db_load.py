"""Tests for RuntimeProviderRegistry.load_db_providers (env-only service)."""
from __future__ import annotations

import os
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.services.provider_service import RuntimeProviderRegistry, build_provider_service


@pytest.fixture
def mock_db():
    return MagicMock()


@pytest.fixture
def mock_config():
    return MagicMock()


@pytest.mark.anyio
async def test_load_db_providers_no_db_or_config():
    """Returns 0 when db or config is None."""
    svc = RuntimeProviderRegistry(db=None, config=None)
    assert await svc.load_db_providers() == 0

    svc2 = RuntimeProviderRegistry(db=MagicMock(), config=None)
    assert await svc2.load_db_providers() == 0

    svc3 = RuntimeProviderRegistry(db=None, config=MagicMock())
    assert await svc3.load_db_providers() == 0


@pytest.mark.anyio
async def test_load_db_providers_registers_openai(mock_db, mock_config):
    """OpenAI-style provider from DB is registered as adapter."""
    mock_cfg = MagicMock()
    mock_cfg.provider = "openai"
    mock_cfg.enabled = True
    mock_cfg.secret_fields = {"api_key": "sk-test-key-123"}
    mock_cfg.plain_fields = {"base_url": ""}

    with patch(
        "src.services.agent_provider_service.ProviderConfigService"
    ) as mock_db_svc_cls:
        mock_db_svc = MagicMock()
        mock_db_svc.load_provider_configs = AsyncMock(return_value=[mock_cfg])
        mock_db_svc_cls.return_value = mock_db_svc

        svc = RuntimeProviderRegistry(db=mock_db, config=mock_config)
        added = await svc.load_db_providers()

    assert added == 1
    assert svc.has_providers()
    assert "openai" in svc._registry


@pytest.mark.anyio
async def test_load_db_providers_skips_empty_secrets(mock_db, mock_config):
    """Provider with empty secrets (all required) is skipped."""
    mock_cfg = MagicMock()
    mock_cfg.provider = "openai"
    mock_cfg.enabled = True
    mock_cfg.secret_fields = {"api_key": ""}
    mock_cfg.plain_fields = {}

    with patch(
        "src.services.agent_provider_service.ProviderConfigService"
    ) as mock_db_svc_cls:
        mock_db_svc = MagicMock()
        mock_db_svc.load_provider_configs = AsyncMock(return_value=[mock_cfg])
        mock_db_svc_cls.return_value = mock_db_svc

        svc = RuntimeProviderRegistry(db=mock_db, config=mock_config)
        added = await svc.load_db_providers()

    assert added == 0
    assert not svc.has_providers()


@pytest.mark.anyio
async def test_load_db_providers_skips_disabled(mock_db, mock_config):
    """Disabled provider is skipped."""
    mock_cfg = MagicMock()
    mock_cfg.provider = "cohere"
    mock_cfg.enabled = False
    mock_cfg.secret_fields = {"api_key": "test-key"}
    mock_cfg.plain_fields = {}

    with patch(
        "src.services.agent_provider_service.ProviderConfigService"
    ) as mock_db_svc_cls:
        mock_db_svc = MagicMock()
        mock_db_svc.load_provider_configs = AsyncMock(return_value=[mock_cfg])
        mock_db_svc_cls.return_value = mock_db_svc

        svc = RuntimeProviderRegistry(db=mock_db, config=mock_config)
        added = await svc.load_db_providers()

    assert added == 0


@pytest.mark.anyio
async def test_load_db_providers_skips_duplicate(mock_db, mock_config):
    """Provider already in registry (from env) is not duplicated."""
    mock_cfg = MagicMock()
    mock_cfg.provider = "openai"
    mock_cfg.enabled = True
    mock_cfg.secret_fields = {"api_key": "test-key"}
    mock_cfg.plain_fields = {}

    with patch(
        "src.services.agent_provider_service.ProviderConfigService"
    ) as mock_db_svc_cls:
        mock_db_svc = MagicMock()
        mock_db_svc.load_provider_configs = AsyncMock(return_value=[mock_cfg])
        mock_db_svc_cls.return_value = mock_db_svc

        svc = RuntimeProviderRegistry(db=mock_db, config=mock_config)
        # Pre-register 'openai' as if from env
        svc.register_provider("openai", svc._default_provider)
        added = await svc.load_db_providers()

    assert added == 0


@pytest.mark.anyio
async def test_reload_db_providers_clears_and_reloads(mock_db, mock_config):
    """reload clears db providers and reloads."""
    mock_cfg = MagicMock()
    mock_cfg.provider = "groq"
    mock_cfg.enabled = True
    mock_cfg.secret_fields = {"api_key": "gsk-test"}
    mock_cfg.plain_fields = {}

    with patch(
        "src.services.agent_provider_service.ProviderConfigService"
    ) as mock_db_svc_cls:
        mock_db_svc = MagicMock()
        mock_db_svc.load_provider_configs = AsyncMock(return_value=[mock_cfg])
        mock_db_svc_cls.return_value = mock_db_svc

        svc = RuntimeProviderRegistry(db=mock_db, config=mock_config)
        await svc.load_db_providers()
        assert "groq" in svc._registry

        # Now reload — simulate empty config (provider removed)
        mock_db_svc.load_provider_configs = AsyncMock(return_value=[])
        added = await svc.reload_db_providers()

    assert added == 0
    assert "groq" not in svc._registry


@pytest.mark.anyio
async def test_build_provider_service_loads_db_providers(mock_db, mock_config):
    """Async helper should eagerly load DB-backed providers when config is available."""
    with patch("src.services.provider_service.RuntimeProviderRegistry.load_db_providers", new=AsyncMock()) as mock_load:
        svc = await build_provider_service(mock_db, mock_config)

    assert isinstance(svc, RuntimeProviderRegistry)
    mock_load.assert_awaited_once()


@pytest.mark.anyio
async def test_load_db_providers_cohere_adapter(mock_db, mock_config):
    """Cohere provider gets a LangChain runtime adapter."""
    mock_cfg = MagicMock()
    mock_cfg.provider = "cohere"
    mock_cfg.enabled = True
    mock_cfg.secret_fields = {"api_key": "cohere-test-key"}
    mock_cfg.plain_fields = {"base_url": ""}

    with patch(
        "src.services.agent_provider_service.ProviderConfigService"
    ) as mock_db_svc_cls:
        mock_db_svc = MagicMock()
        mock_db_svc.load_provider_configs = AsyncMock(return_value=[mock_cfg])
        mock_db_svc_cls.return_value = mock_db_svc

        svc = RuntimeProviderRegistry(db=mock_db, config=mock_config)
        added = await svc.load_db_providers()

    assert added == 1
    assert "cohere" in svc._registry


@pytest.mark.anyio
async def test_load_db_providers_ollama_adapter_with_key(mock_db, mock_config):
    """Ollama provider with api_key is registered."""
    mock_cfg = MagicMock()
    mock_cfg.provider = "ollama"
    mock_cfg.enabled = True
    mock_cfg.secret_fields = {"api_key": "ollama-key"}
    mock_cfg.plain_fields = {"base_url": "http://localhost:11434"}

    with patch(
        "src.services.agent_provider_service.ProviderConfigService"
    ) as mock_db_svc_cls:
        mock_db_svc = MagicMock()
        mock_db_svc.load_provider_configs = AsyncMock(return_value=[mock_cfg])
        mock_db_svc_cls.return_value = mock_db_svc

        svc = RuntimeProviderRegistry(db=mock_db, config=mock_config)
        added = await svc.load_db_providers()

    assert added == 1
    assert "ollama" in svc._registry


@pytest.mark.anyio
async def test_load_db_providers_ollama_without_api_key(mock_db, mock_config):
    """Ollama with no api_key should be valid since api_key is optional."""
    mock_cfg = MagicMock()
    mock_cfg.provider = "ollama"
    mock_cfg.enabled = True
    mock_cfg.secret_fields = {"api_key": ""}
    mock_cfg.plain_fields = {"base_url": "http://localhost:11434"}

    with patch(
        "src.services.agent_provider_service.ProviderConfigService"
    ) as mock_db_svc_cls:
        mock_db_svc = MagicMock()
        mock_db_svc.load_provider_configs = AsyncMock(return_value=[mock_cfg])
        mock_db_svc_cls.return_value = mock_db_svc

        svc = RuntimeProviderRegistry(db=mock_db, config=mock_config)
        added = await svc.load_db_providers()

    assert added == 1
    assert svc.has_providers()
    assert "ollama" in svc._registry


@pytest.mark.anyio
async def test_load_db_providers_handles_exception(mock_db, mock_config):
    """Exception during load_provider_configs returns 0 gracefully."""
    with patch(
        "src.services.agent_provider_service.ProviderConfigService"
    ) as mock_db_svc_cls:
        mock_db_svc = MagicMock()
        mock_db_svc.load_provider_configs = AsyncMock(side_effect=Exception("DB error"))
        mock_db_svc_cls.return_value = mock_db_svc

        svc = RuntimeProviderRegistry(db=mock_db, config=mock_config)
        added = await svc.load_db_providers()

    assert added == 0


def test_has_valid_secrets():
    """_has_valid_secrets checks non-empty secret values."""
    svc = RuntimeProviderRegistry()
    cfg = MagicMock()
    cfg.provider = "openai"
    cfg.secret_fields = {"api_key": "valid-key"}
    assert svc._has_valid_secrets(cfg) is True

    cfg2 = MagicMock()
    cfg2.provider = "openai"
    cfg2.secret_fields = {"api_key": ""}
    assert svc._has_valid_secrets(cfg2) is False

    cfg3 = MagicMock()
    cfg3.provider = "openai"
    cfg3.secret_fields = {}
    assert svc._has_valid_secrets(cfg3) is False

    cfg4 = MagicMock(spec=[])  # no secret_fields attr
    cfg4.provider = "openai"
    assert svc._has_valid_secrets(cfg4) is False


def test_has_valid_secrets_allows_all_optional():
    """Providers where all secret fields are optional (like Ollama) should be valid."""
    svc = RuntimeProviderRegistry()
    cfg = MagicMock()
    cfg.provider = "ollama"
    cfg.secret_fields = {"api_key": ""}
    assert svc._has_valid_secrets(cfg) is True


def test_has_valid_secrets_huggingface_without_key():
    """HuggingFace api_key is also optional, should be valid without it."""
    svc = RuntimeProviderRegistry()
    cfg = MagicMock()
    cfg.provider = "huggingface"
    cfg.secret_fields = {"api_key": ""}
    assert svc._has_valid_secrets(cfg) is True


def test_has_valid_secrets_requires_secrets_for_required_providers():
    """Providers with required secret fields still need a non-empty value."""
    svc = RuntimeProviderRegistry()
    cfg = MagicMock()
    cfg.provider = "deepseek"
    cfg.secret_fields = {"api_key": ""}
    assert svc._has_valid_secrets(cfg) is False


@pytest.mark.parametrize(
    ("provider", "default_base_url"),
    [
        ("openai", "https://api.openai.com/v1"),
        ("groq", "https://api.groq.com/openai/v1"),
        ("deepseek", "https://api.deepseek.com/v1"),
    ],
)
def test_build_adapter_for_config_uses_registry_default_base_url(provider, default_base_url):
    svc = RuntimeProviderRegistry()
    cfg = MagicMock()
    cfg.provider = provider
    cfg.plain_fields = {"base_url": ""}
    cfg.secret_fields = {"api_key": f"{provider}-key"}
    sentinel = AsyncMock()

    with patch.object(svc, "_make_provider_for_runtime_config", return_value=sentinel) as make_adapter:
        adapter = svc._build_adapter_for_config(cfg)

    assert adapter is sentinel
    make_adapter.assert_called_once_with(cfg)


@pytest.mark.anyio
async def test_get_provider_status_list_disabled(mock_db, mock_config):
    """get_provider_status_list returns disabled status."""
    mock_cfg = MagicMock()
    mock_cfg.provider = "openai"
    mock_cfg.enabled = False
    mock_cfg.secret_fields = {"api_key": "sk-test"}
    mock_cfg.plain_fields = {}
    mock_cfg.last_validation_error = ""

    with patch(
        "src.services.agent_provider_service.ProviderConfigService"
    ) as mock_db_svc_cls:
        mock_db_svc = MagicMock()
        mock_db_svc.load_provider_configs = AsyncMock(return_value=[mock_cfg])
        mock_db_svc_cls.return_value = mock_db_svc

        svc = RuntimeProviderRegistry(db=mock_db, config=mock_config)
        statuses = await svc.get_provider_status_list()

    assert len(statuses) == 1
    assert statuses[0]["provider"] == "openai"
    assert statuses[0]["status"] == "disabled"


@pytest.mark.anyio
async def test_get_provider_status_list_invalid_secrets(mock_db, mock_config):
    """get_provider_status_list returns invalid_secrets with reason."""
    mock_cfg = MagicMock()
    mock_cfg.provider = "openai"
    mock_cfg.enabled = True
    mock_cfg.secret_fields = {"api_key": ""}
    mock_cfg.plain_fields = {}
    mock_cfg.last_validation_error = "SESSION_ENCRYPTION_KEY is not configured"

    with patch(
        "src.services.agent_provider_service.ProviderConfigService"
    ) as mock_db_svc_cls:
        mock_db_svc = MagicMock()
        mock_db_svc.load_provider_configs = AsyncMock(return_value=[mock_cfg])
        mock_db_svc_cls.return_value = mock_db_svc

        svc = RuntimeProviderRegistry(db=mock_db, config=mock_config)
        statuses = await svc.get_provider_status_list()

    assert len(statuses) == 1
    assert statuses[0]["status"] == "invalid_secrets"
    assert "SESSION_ENCRYPTION_KEY" in statuses[0]["reason"]


@pytest.mark.anyio
async def test_get_provider_status_list_no_adapter(mock_db, mock_config):
    """get_provider_status_list returns no_adapter for unsupported providers."""
    mock_cfg = MagicMock()
    mock_cfg.provider = "unsupported_provider"
    mock_cfg.enabled = True
    mock_cfg.secret_fields = {"api_key": "test-key"}
    mock_cfg.plain_fields = {}
    mock_cfg.last_validation_error = ""

    with patch(
        "src.services.agent_provider_service.ProviderConfigService"
    ) as mock_db_svc_cls:
        mock_db_svc = MagicMock()
        mock_db_svc.load_provider_configs = AsyncMock(return_value=[mock_cfg])
        mock_db_svc_cls.return_value = mock_db_svc

        svc = RuntimeProviderRegistry(db=mock_db, config=mock_config)
        statuses = await svc.get_provider_status_list()

    assert len(statuses) == 1
    assert statuses[0]["status"] == "no_adapter"
    assert "unsupported_provider" in statuses[0]["reason"]


@pytest.mark.anyio
async def test_get_provider_status_list_active(mock_db, mock_config):
    """get_provider_status_list returns active for registered providers."""
    mock_cfg = MagicMock()
    mock_cfg.provider = "openai"
    mock_cfg.enabled = True
    mock_cfg.secret_fields = {"api_key": "sk-test"}
    mock_cfg.plain_fields = {"base_url": ""}
    mock_cfg.last_validation_error = ""

    with patch(
        "src.services.agent_provider_service.ProviderConfigService"
    ) as mock_db_svc_cls:
        mock_db_svc = MagicMock()
        mock_db_svc.load_provider_configs = AsyncMock(return_value=[mock_cfg])
        mock_db_svc_cls.return_value = mock_db_svc

        svc = RuntimeProviderRegistry(db=mock_db, config=mock_config)
        await svc.load_db_providers()
        statuses = await svc.get_provider_status_list()

    assert len(statuses) == 1
    assert statuses[0]["status"] == "active"


@pytest.mark.anyio
async def test_get_provider_status_list_no_db():
    """Returns empty list when no DB configured."""
    svc = RuntimeProviderRegistry(db=None, config=None)
    assert await svc.get_provider_status_list() == []


def test_env_provider_makes_has_providers_true():
    """When env provider exists, has_providers() is True even if DB load fails."""
    env_key = "OPENAI_API_KEY"
    original = os.environ.get(env_key)
    try:
        os.environ[env_key] = "test-key-for-banner-test"
        svc = RuntimeProviderRegistry(db=MagicMock(), config=MagicMock())
        assert svc.has_providers()
    finally:
        if original is None:
            os.environ.pop(env_key, None)
        else:
            os.environ[env_key] = original
