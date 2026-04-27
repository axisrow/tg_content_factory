from __future__ import annotations

import os
from unittest.mock import AsyncMock, MagicMock, patch

from src.services.provider_service import AgentProviderService


def _make_service(**env_vars):
    with patch.dict(os.environ, env_vars, clear=False):
        svc = AgentProviderService(db=None, config=None)
    return svc


def test_default_provider_registered():
    svc = _make_service()
    assert "default" in svc._registry
    assert not svc.has_providers()


async def test_default_provider_returns_draft():
    svc = _make_service()
    result = await svc._default_provider(prompt="hello world")
    assert "DRAFT" in result
    assert "hello world" in result


def test_register_provider():
    svc = _make_service()
    mock_fn = AsyncMock(return_value="result")
    svc.register_provider("test_provider", mock_fn)
    assert "test_provider" in svc._registry
    assert svc.has_providers()


def test_get_provider_callable_default():
    svc = _make_service()
    fn = svc.get_provider_callable()
    assert fn == svc._registry["default"]


def test_get_provider_callable_named():
    svc = _make_service()
    mock_fn = AsyncMock(return_value="result")
    svc.register_provider("myprov", mock_fn)
    fn = svc.get_provider_callable("myprov")
    assert fn == mock_fn


def test_get_provider_callable_unknown_falls_back():
    svc = _make_service()
    fn = svc.get_provider_callable("unknown_provider")
    assert fn == svc._registry["default"]


def test_get_provider_callable_no_name_uses_first_real():
    svc = _make_service()
    mock_fn = AsyncMock(return_value="result")
    svc.register_provider("real", mock_fn)
    fn = svc.get_provider_callable()
    assert fn == mock_fn


def test_openai_env_provider():
    svc = _make_service(OPENAI_API_KEY="test-key")
    assert "openai" in svc._registry
    assert svc.has_providers()


def test_get_provider_callable_gpt_model():
    svc = _make_service(OPENAI_API_KEY="test-key")
    fn = svc.get_provider_callable("gpt-4o")
    assert fn is not None


def test_make_openai_compat_provider():
    fn = AgentProviderService._make_openai_compat_provider("https://api.test.com/v1", "key123")
    assert callable(fn)


async def test_load_db_providers_no_db():
    svc = _make_service()
    result = await svc.load_db_providers()
    assert result == 0


async def test_reload_db_providers_no_db():
    svc = _make_service()
    result = await svc.reload_db_providers()
    assert result == 0


async def test_get_provider_status_list_no_db():
    svc = _make_service()
    result = await svc.get_provider_status_list()
    assert result == []


async def test_build_provider_status_list():
    db = MagicMock()
    config = MagicMock()
    svc = AgentProviderService(db=db, config=config)

    mock_cfg = MagicMock()
    mock_cfg.provider = "openai"
    mock_cfg.enabled = True
    mock_cfg.secret_fields = {"api_key": "sk-test"}
    mock_cfg.plain_fields = {}
    mock_cfg.last_validation_error = ""

    # The method creates a new APS internally, so we need to mock at module level
    with patch("src.services.agent_provider_service.AgentProviderService") as mock_aps:
        mock_instance = MagicMock()
        mock_instance.load_provider_configs = AsyncMock(return_value=[mock_cfg])
        mock_aps.return_value = mock_instance
        statuses = await svc.get_provider_status_list()
        assert len(statuses) >= 1
        assert statuses[0]["provider"] == "openai"
        assert "status" in statuses[0]


def test_has_valid_secrets_empty():
    svc = _make_service()
    cfg = MagicMock(secret_fields={}, provider="test")
    result = svc._has_valid_secrets(cfg)
    assert result is False


def test_has_valid_secrets_with_value():
    svc = _make_service()
    cfg = MagicMock(secret_fields={"api_key": "sk-test"}, provider="test")
    result = svc._has_valid_secrets(cfg)
    assert result is True
