"""Tests for provider service adapter registration and failure paths."""
from __future__ import annotations

import os
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.services.provider_service import AgentProviderService


@pytest.fixture(autouse=True)
def clean_env():
    saved = {}
    for var in [
        "OPENAI_API_KEY", "COHERE_API_KEY", "OLLAMA_BASE", "OLLAMA_URL",
        "HUGGINGFACE_API_KEY", "HUGGINGFACE_TOKEN", "FIREWORKS_BASE",
        "FIREWORKS_API_BASE", "FIREWORKS_API_KEY", "DEEPSEEK_BASE",
        "DEEPSEEK_API_BASE", "DEEPSEEK_API_KEY", "TOGETHER_BASE",
        "TOGETHER_API_BASE", "TOGETHER_API_KEY", "CONTEXT7_API_KEY",
        "CTX7_API_KEY", "ZAI_API_KEY",
    ]:
        saved[var] = os.environ.get(var)
        if var in os.environ:
            del os.environ[var]
    yield
    for var, val in saved.items():
        if val is None:
            os.environ.pop(var, None)
        else:
            os.environ[var] = val


# === Z.AI registration failure ===


def test_zai_registration_failure_path(clean_env):
    """Z.AI adapter registration failure is caught gracefully."""
    os.environ["ZAI_API_KEY"] = "zai-test-key"
    with patch("src.agent.provider_registry.ZAI_DEFAULT_BASE_URL", "http://zai.test", create=True):
        with patch("src.services.provider_adapters.make_anthropic_adapter", side_effect=ImportError("no anthropic")):
            svc = AgentProviderService()
    assert "zai" not in svc._registry


# === Context7 registration failure ===


def test_context7_registration_failure_path(clean_env):
    """Context7 adapter registration failure is caught gracefully."""
    os.environ["CONTEXT7_API_KEY"] = "ctx7-test-key"
    with patch("src.services.provider_adapters.make_context7_adapter", side_effect=ImportError("no ctx7")):
        svc = AgentProviderService()
    assert "context7" not in svc._registry


# === Import failure for HTTP adapters ===


def test_http_adapter_import_failure(clean_env):
    """When provider_adapters import fails, adapters are skipped."""
    os.environ["COHERE_API_KEY"] = "cohere-test"
    # Simulate ImportError by patching the import
    with patch.dict("sys.modules", {"src.services.provider_adapters": None}):
        svc = AgentProviderService()
    assert "cohere" not in svc._registry


# === Exception during individual HTTP adapter registration ===


def test_http_adapter_registration_exception(clean_env):
    """Single adapter failure doesn't prevent others from registering."""
    os.environ["COHERE_API_KEY"] = "cohere-key"
    os.environ["OLLAMA_BASE"] = "http://localhost:11434"

    call_count = 0

    def mock_cohere(key):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise ValueError("bad adapter")
        return AsyncMock()

    with patch("src.services.provider_adapters.make_cohere_adapter", mock_cohere):
        with patch("src.services.provider_adapters.make_ollama_adapter", return_value=AsyncMock()):
            with patch("src.services.provider_adapters.make_huggingface_adapter"):
                with patch("src.services.provider_adapters.make_generic_http_adapter"):
                    svc = AgentProviderService()

    # cohere failed but ollama may have registered
    assert "cohere" not in svc._registry


# === get_provider_status_list exception ===


@pytest.mark.anyio
async def test_get_provider_status_list_db_exception():
    """Returns empty list when DB load fails."""
    svc = AgentProviderService()
    mock_db = MagicMock()
    mock_config = MagicMock()
    svc.db = mock_db
    svc._config = mock_config

    with patch("src.services.agent_provider_service.AgentProviderService") as mock_cls:
        mock_instance = MagicMock()
        mock_instance.load_provider_configs = AsyncMock(side_effect=Exception("DB error"))
        mock_cls.return_value = mock_instance
        result = await svc.get_provider_status_list()

    assert result == []


# === get_provider_callable: OpenAI not registered but GPT model requested ===


@pytest.mark.anyio
async def test_get_provider_gpt_fallback_without_openai():
    """When OpenAI not registered but gpt model requested, falls back to default."""
    svc = AgentProviderService()
    provider = svc.get_provider_callable("gpt-4")
    result = await provider(prompt="test")
    assert "DRAFT" in result


# === _make_openai_compat_provider: non-200 status ===


@pytest.mark.anyio
async def test_openai_compat_provider_non_200():
    """Provider raises RuntimeError on non-200 status."""
    svc = AgentProviderService()
    provider_fn = svc._make_openai_compat_provider("http://localhost:1234", "test-key")

    mock_resp = MagicMock()
    mock_resp.status = 429
    mock_resp.text = AsyncMock(return_value="rate limited")
    mock_resp.json = AsyncMock(return_value={})

    mock_cm = MagicMock()
    mock_cm.__aenter__ = AsyncMock(return_value=mock_resp)
    mock_cm.__aexit__ = AsyncMock(return_value=False)

    mock_session = MagicMock()
    mock_session.post.return_value = mock_cm
    mock_session_cm = MagicMock()
    mock_session_cm.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session_cm.__aexit__ = AsyncMock(return_value=False)

    with patch("src.services.provider_service.aiohttp.ClientSession", return_value=mock_session_cm):
        with pytest.raises(RuntimeError, match="Provider error 429"):
            await provider_fn(prompt="test")


# === _make_openai_compat_provider: malformed response ===


@pytest.mark.anyio
async def test_openai_compat_provider_malformed_response():
    """Provider returns stringified response when choices not found."""
    svc = AgentProviderService()
    provider_fn = svc._make_openai_compat_provider("http://localhost:1234", "test-key")

    mock_resp = MagicMock()
    mock_resp.status = 200
    mock_resp.text = AsyncMock(return_value="ok")
    mock_resp.json = AsyncMock(return_value={"error": "no choices"})

    mock_cm = MagicMock()
    mock_cm.__aenter__ = AsyncMock(return_value=mock_resp)
    mock_cm.__aexit__ = AsyncMock(return_value=False)

    mock_session = MagicMock()
    mock_session.post.return_value = mock_cm
    mock_session_cm = MagicMock()
    mock_session_cm.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session_cm.__aexit__ = AsyncMock(return_value=False)

    with patch("src.services.provider_service.aiohttp.ClientSession", return_value=mock_session_cm):
        result = await provider_fn(prompt="test")
    # Falls back to str(data) since choices[0].message.content fails
    assert "no choices" in result


# === _make_openai_provider: malformed response fallback ===


@pytest.mark.anyio
async def test_openai_provider_malformed_response_fallback(clean_env):
    """OpenAI provider returns stringified response when choices not found."""
    os.environ["OPENAI_API_KEY"] = "test-key"

    mock_resp = MagicMock()
    mock_resp.status = 200
    mock_resp.text = AsyncMock(return_value="ok")
    mock_resp.json = AsyncMock(return_value={"unexpected": "format"})

    mock_cm = MagicMock()
    mock_cm.__aenter__ = AsyncMock(return_value=mock_resp)
    mock_cm.__aexit__ = AsyncMock(return_value=False)

    mock_session = MagicMock()
    mock_session.post.return_value = mock_cm
    mock_session_cm = MagicMock()
    mock_session_cm.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session_cm.__aexit__ = AsyncMock(return_value=False)

    with patch("src.services.provider_service.aiohttp.ClientSession", return_value=mock_session_cm):
        svc = AgentProviderService()
        provider = svc.get_provider_callable("openai")
        result = await provider(prompt="test")
    assert "unexpected" in result


# === get_provider_status_list: with configs ===


@pytest.mark.anyio
async def test_get_provider_status_list_with_configs():
    """Returns status list for configured providers."""
    svc = AgentProviderService()

    async def fake_provider(**kwargs):
        return "test"

    svc.register_provider("openai", fake_provider)
    mock_db = MagicMock()
    mock_config = MagicMock()
    svc.db = mock_db
    svc._config = mock_config

    cfg1 = MagicMock()
    cfg1.provider = "openai"
    cfg1.enabled = True

    cfg2 = MagicMock()
    cfg2.provider = "disabled_prov"
    cfg2.enabled = False

    cfg3 = MagicMock()
    cfg3.provider = "no_secrets"
    cfg3.enabled = True

    with patch("src.services.agent_provider_service.AgentProviderService") as mock_cls:
        mock_instance = MagicMock()
        mock_instance.load_provider_configs = AsyncMock(return_value=[cfg1, cfg2, cfg3])
        mock_cls.return_value = mock_instance

        with patch.object(svc, "_has_valid_secrets", side_effect=lambda c: c.provider != "no_secrets"):
            result = await svc.get_provider_status_list()

    assert len(result) == 3
    assert result[0]["status"] == "active"
    assert result[1]["status"] == "disabled"
    assert result[2]["status"] == "invalid_secrets"
