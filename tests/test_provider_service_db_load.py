"""Tests for AgentProviderService.load_db_providers (env-only service)."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.services.provider_service import AgentProviderService


@pytest.fixture
def mock_db():
    return MagicMock()


@pytest.fixture
def mock_config():
    return MagicMock()


@pytest.mark.asyncio
async def test_load_db_providers_no_db_or_config():
    """Returns 0 when db or config is None."""
    svc = AgentProviderService(db=None, config=None)
    assert await svc.load_db_providers() == 0

    svc2 = AgentProviderService(db=MagicMock(), config=None)
    assert await svc2.load_db_providers() == 0

    svc3 = AgentProviderService(db=None, config=MagicMock())
    assert await svc3.load_db_providers() == 0


@pytest.mark.asyncio
async def test_load_db_providers_registers_openai(mock_db, mock_config):
    """OpenAI-style provider from DB is registered as adapter."""
    mock_cfg = MagicMock()
    mock_cfg.provider = "openai"
    mock_cfg.enabled = True
    mock_cfg.secret_fields = {"api_key": "sk-test-key-123"}
    mock_cfg.plain_fields = {"base_url": ""}

    with patch(
        "src.services.agent_provider_service.AgentProviderService"
    ) as mock_db_svc_cls:
        mock_db_svc = MagicMock()
        mock_db_svc.load_provider_configs = AsyncMock(return_value=[mock_cfg])
        mock_db_svc_cls.return_value = mock_db_svc

        svc = AgentProviderService(db=mock_db, config=mock_config)
        added = await svc.load_db_providers()

    assert added == 1
    assert svc.has_providers()
    assert "openai" in svc._registry


@pytest.mark.asyncio
async def test_load_db_providers_skips_empty_secrets(mock_db, mock_config):
    """Provider with empty secrets is skipped."""
    mock_cfg = MagicMock()
    mock_cfg.provider = "openai"
    mock_cfg.enabled = True
    mock_cfg.secret_fields = {"api_key": ""}
    mock_cfg.plain_fields = {}

    with patch(
        "src.services.agent_provider_service.AgentProviderService"
    ) as mock_db_svc_cls:
        mock_db_svc = MagicMock()
        mock_db_svc.load_provider_configs = AsyncMock(return_value=[mock_cfg])
        mock_db_svc_cls.return_value = mock_db_svc

        svc = AgentProviderService(db=mock_db, config=mock_config)
        added = await svc.load_db_providers()

    assert added == 0
    assert not svc.has_providers()


@pytest.mark.asyncio
async def test_load_db_providers_skips_disabled(mock_db, mock_config):
    """Disabled provider is skipped."""
    mock_cfg = MagicMock()
    mock_cfg.provider = "cohere"
    mock_cfg.enabled = False
    mock_cfg.secret_fields = {"api_key": "test-key"}
    mock_cfg.plain_fields = {}

    with patch(
        "src.services.agent_provider_service.AgentProviderService"
    ) as mock_db_svc_cls:
        mock_db_svc = MagicMock()
        mock_db_svc.load_provider_configs = AsyncMock(return_value=[mock_cfg])
        mock_db_svc_cls.return_value = mock_db_svc

        svc = AgentProviderService(db=mock_db, config=mock_config)
        added = await svc.load_db_providers()

    assert added == 0


@pytest.mark.asyncio
async def test_load_db_providers_skips_duplicate(mock_db, mock_config):
    """Provider already in registry (from env) is not duplicated."""
    mock_cfg = MagicMock()
    mock_cfg.provider = "openai"
    mock_cfg.enabled = True
    mock_cfg.secret_fields = {"api_key": "test-key"}
    mock_cfg.plain_fields = {}

    with patch(
        "src.services.agent_provider_service.AgentProviderService"
    ) as mock_db_svc_cls:
        mock_db_svc = MagicMock()
        mock_db_svc.load_provider_configs = AsyncMock(return_value=[mock_cfg])
        mock_db_svc_cls.return_value = mock_db_svc

        svc = AgentProviderService(db=mock_db, config=mock_config)
        # Pre-register 'openai' as if from env
        svc.register_provider("openai", svc._default_provider)
        added = await svc.load_db_providers()

    assert added == 0


@pytest.mark.asyncio
async def test_reload_db_providers_clears_and_reloads(mock_db, mock_config):
    """reload clears db providers and reloads."""
    mock_cfg = MagicMock()
    mock_cfg.provider = "groq"
    mock_cfg.enabled = True
    mock_cfg.secret_fields = {"api_key": "gsk-test"}
    mock_cfg.plain_fields = {}

    with patch(
        "src.services.agent_provider_service.AgentProviderService"
    ) as mock_db_svc_cls:
        mock_db_svc = MagicMock()
        mock_db_svc.load_provider_configs = AsyncMock(return_value=[mock_cfg])
        mock_db_svc_cls.return_value = mock_db_svc

        svc = AgentProviderService(db=mock_db, config=mock_config)
        await svc.load_db_providers()
        assert "groq" in svc._registry

        # Now reload — simulate empty config (provider removed)
        mock_db_svc.load_provider_configs = AsyncMock(return_value=[])
        added = await svc.reload_db_providers()

    assert added == 0
    assert "groq" not in svc._registry


@pytest.mark.asyncio
async def test_load_db_providers_cohere_adapter(mock_db, mock_config):
    """Cohere provider gets make_cohere_adapter."""
    mock_cfg = MagicMock()
    mock_cfg.provider = "cohere"
    mock_cfg.enabled = True
    mock_cfg.secret_fields = {"api_key": "cohere-test-key"}
    mock_cfg.plain_fields = {"base_url": ""}

    with patch(
        "src.services.agent_provider_service.AgentProviderService"
    ) as mock_db_svc_cls:
        mock_db_svc = MagicMock()
        mock_db_svc.load_provider_configs = AsyncMock(return_value=[mock_cfg])
        mock_db_svc_cls.return_value = mock_db_svc

        svc = AgentProviderService(db=mock_db, config=mock_config)
        added = await svc.load_db_providers()

    assert added == 1
    assert "cohere" in svc._registry


@pytest.mark.asyncio
async def test_load_db_providers_ollama_adapter(mock_db, mock_config):
    """Ollama provider with no api_key has empty secrets -> skipped."""
    mock_cfg = MagicMock()
    mock_cfg.provider = "ollama"
    mock_cfg.enabled = True
    mock_cfg.secret_fields = {"api_key": ""}
    mock_cfg.plain_fields = {"base_url": "http://localhost:11434"}

    with patch(
        "src.services.agent_provider_service.AgentProviderService"
    ) as mock_db_svc_cls:
        mock_db_svc = MagicMock()
        mock_db_svc.load_provider_configs = AsyncMock(return_value=[mock_cfg])
        mock_db_svc_cls.return_value = mock_db_svc

        svc = AgentProviderService(db=mock_db, config=mock_config)
        added = await svc.load_db_providers()

    # No api_key = empty secrets -> skipped
    assert added == 0


@pytest.mark.asyncio
async def test_load_db_providers_handles_exception(mock_db, mock_config):
    """Exception during load_provider_configs returns 0 gracefully."""
    with patch(
        "src.services.agent_provider_service.AgentProviderService"
    ) as mock_db_svc_cls:
        mock_db_svc = MagicMock()
        mock_db_svc.load_provider_configs = AsyncMock(side_effect=Exception("DB error"))
        mock_db_svc_cls.return_value = mock_db_svc

        svc = AgentProviderService(db=mock_db, config=mock_config)
        added = await svc.load_db_providers()

    assert added == 0


def test_has_valid_secrets():
    """_has_valid_secrets checks non-empty secret values."""
    cfg = MagicMock()
    cfg.secret_fields = {"api_key": "valid-key"}
    assert AgentProviderService._has_valid_secrets(cfg) is True

    cfg2 = MagicMock()
    cfg2.secret_fields = {"api_key": ""}
    assert AgentProviderService._has_valid_secrets(cfg2) is False

    cfg3 = MagicMock()
    cfg3.secret_fields = {}
    assert AgentProviderService._has_valid_secrets(cfg3) is False

    cfg4 = MagicMock(spec=[])  # no secret_fields attr
    assert AgentProviderService._has_valid_secrets(cfg4) is False
