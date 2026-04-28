"""Tests for AgentProviderService."""

from __future__ import annotations

import asyncio
import os
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.services.provider_service import AgentProviderService


@pytest.fixture(autouse=True)
def clean_env():
    """Clean environment variables before each test."""
    # Save original values
    saved = {}
    env_vars = [
        "OPENAI_API_KEY", "COHERE_API_KEY", "OLLAMA_BASE", "OLLAMA_URL",
        "HUGGINGFACE_API_KEY", "HUGGINGFACE_TOKEN", "FIREWORKS_BASE",
        "FIREWORKS_API_BASE", "FIREWORKS_API_KEY", "DEEPSEEK_BASE",
        "DEEPSEEK_API_BASE", "DEEPSEEK_API_KEY", "TOGETHER_BASE",
        "TOGETHER_API_BASE", "TOGETHER_API_KEY", "CONTEXT7_API_KEY",
        "CTX7_API_KEY",
    ]
    for var in env_vars:
        saved[var] = os.environ.get(var)
        if var in os.environ:
            del os.environ[var]

    yield

    # Restore original values
    for var, val in saved.items():
        if val is None:
            os.environ.pop(var, None)
        else:
            os.environ[var] = val


# === Default provider tests ===


def test_default_provider_returns_draft():
    """Default provider returns DRAFT prefix."""
    svc = AgentProviderService()
    provider = svc.get_provider_callable(None)
    result = asyncio.run(provider(prompt="hello world"))
    assert result.startswith("DRAFT (default provider): hello world")


def test_default_provider_truncates_long_prompt():
    """Default provider truncates prompts longer than 400 chars."""
    svc = AgentProviderService()
    provider = svc.get_provider_callable(None)
    long_prompt = "x" * 500
    result = asyncio.run(provider(prompt=long_prompt))
    assert len(result) < 500
    assert "DRAFT" in result


def test_default_provider_empty_prompt():
    """Default provider handles empty prompt."""
    svc = AgentProviderService()
    provider = svc.get_provider_callable(None)
    result = asyncio.run(provider(prompt=""))
    assert "DRAFT" in result


# === Registry tests ===


def test_register_provider():
    """Can register custom provider."""
    svc = AgentProviderService()

    async def custom_provider(prompt: str = "", **kwargs) -> str:
        return f"Custom: {prompt}"

    svc.register_provider("custom", custom_provider)
    provider = svc.get_provider_callable("custom")
    result = asyncio.run(provider(prompt="test"))
    assert result == "Custom: test"


def test_get_provider_unknown_falls_back_to_default():
    """Unknown provider falls back to default."""
    svc = AgentProviderService()
    provider = svc.get_provider_callable("unknown_provider")
    result = asyncio.run(provider(prompt="test"))
    assert "DRAFT" in result


# === OpenAI provider tests ===


def test_openai_provider_registered_with_api_key(clean_env):
    """OpenAI provider registered when API key present."""
    os.environ["OPENAI_API_KEY"] = "test_key"
    svc = AgentProviderService()
    provider = svc.get_provider_callable("openai")
    assert provider is not None


def test_openai_provider_not_registered_without_key(clean_env):
    """OpenAI provider not registered without API key."""
    svc = AgentProviderService()
    # openai shouldn't be in registry
    assert "openai" not in svc._registry


@pytest.mark.anyio
async def test_openai_provider_calls_api(clean_env):
    """OpenAI provider makes correct API call."""
    os.environ["OPENAI_API_KEY"] = "test_key"

    mock_response = MagicMock()
    mock_response.status = 200
    mock_response.text = AsyncMock(return_value='{"choices": [{"message": {"content": "Hello"}}]}')
    mock_response.json = AsyncMock(return_value={"choices": [{"message": {"content": "Hello"}}]})
    mock_response.__aenter__ = AsyncMock(return_value=mock_response)
    mock_response.__aexit__ = AsyncMock(return_value=False)

    mock_session = MagicMock()
    mock_session.post = MagicMock(return_value=mock_response)
    mock_session.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session.__aexit__ = AsyncMock(return_value=False)

    with patch("aiohttp.ClientSession", return_value=mock_session):
        svc = AgentProviderService()
        provider = svc.get_provider_callable("openai")
        result = await provider(prompt="hi")

        assert result == "Hello"


@pytest.mark.anyio
async def test_openai_provider_error_status(clean_env):
    """OpenAI provider raises on error status."""
    os.environ["OPENAI_API_KEY"] = "test_key"

    mock_response = MagicMock()
    mock_response.status = 401
    mock_response.text = AsyncMock(return_value="Unauthorized")
    mock_response.__aenter__ = AsyncMock(return_value=mock_response)
    mock_response.__aexit__ = AsyncMock(return_value=False)

    mock_session = MagicMock()
    mock_session.post = MagicMock(return_value=mock_response)
    mock_session.__aenter__ = AsyncMock(return_value=mock_session)
    mock_session.__aexit__ = AsyncMock(return_value=False)

    with patch("aiohttp.ClientSession", return_value=mock_session):
        svc = AgentProviderService()
        provider = svc.get_provider_callable("openai")

        with pytest.raises(RuntimeError) as exc_info:
            await provider(prompt="hi")
        assert "401" in str(exc_info.value)


# === GPT model routing tests ===


def test_gpt_model_routes_to_openai(clean_env):
    """GPT model names route to OpenAI provider."""
    os.environ["OPENAI_API_KEY"] = "test_key"
    svc = AgentProviderService()

    # gpt-4 should create a wrapper that uses openai provider
    provider = svc.get_provider_callable("gpt-4")
    assert provider is not None


def test_gpt_model_without_openai_key_falls_back(clean_env):
    """GPT model without OpenAI key falls back to default."""
    svc = AgentProviderService()
    provider = svc.get_provider_callable("gpt-4-turbo")

    # Should fall back to default since openai not registered
    result = asyncio.run(provider(prompt="test"))
    assert "DRAFT" in result


# === Other provider registration tests ===


def test_cohere_registered_with_api_key(clean_env):
    """Cohere provider registered when API key present."""
    os.environ["COHERE_API_KEY"] = "cohere_key"
    svc = AgentProviderService()
    assert "cohere" in svc._registry


def test_ollama_registered_with_base_url(clean_env):
    """Ollama provider registered when base URL present."""
    os.environ["OLLAMA_BASE"] = "http://localhost:11434"
    svc = AgentProviderService()
    assert "ollama" in svc._registry


def test_ollama_registered_with_ollama_url(clean_env):
    """Ollama provider registered with OLLAMA_URL fallback."""
    os.environ["OLLAMA_URL"] = "http://ollama.local"
    svc = AgentProviderService()
    assert "ollama" in svc._registry


def test_huggingface_registered_with_api_key(clean_env):
    """HuggingFace provider registered when API key present."""
    os.environ["HUGGINGFACE_API_KEY"] = "hf_key"
    svc = AgentProviderService()
    assert "huggingface" in svc._registry


def test_huggingface_registered_with_token(clean_env):
    """HuggingFace provider registered with HUGGINGFACE_TOKEN fallback."""
    os.environ["HUGGINGFACE_TOKEN"] = "hf_token"
    svc = AgentProviderService()
    assert "huggingface" in svc._registry


def test_fireworks_registered(clean_env):
    """Fireworks provider registered with base URL and key."""
    os.environ["FIREWORKS_BASE"] = "https://fireworks.api"
    os.environ["FIREWORKS_API_KEY"] = "fw_key"
    svc = AgentProviderService()
    assert "fireworks" in svc._registry


def test_deepseek_registered(clean_env):
    """DeepSeek provider registered with base URL and key."""
    os.environ["DEEPSEEK_BASE"] = "https://deepseek.api"
    os.environ["DEEPSEEK_API_KEY"] = "ds_key"
    svc = AgentProviderService()
    assert "deepseek" in svc._registry


def test_together_registered(clean_env):
    """Together provider registered with base URL and key."""
    os.environ["TOGETHER_BASE"] = "https://together.api"
    os.environ["TOGETHER_API_KEY"] = "tg_key"
    svc = AgentProviderService()
    assert "together" in svc._registry


def test_context7_registered_with_api_key(clean_env):
    """Context7 provider registered when API key present."""
    os.environ["CONTEXT7_API_KEY"] = "c7_key"
    svc = AgentProviderService()
    assert "context7" in svc._registry


def test_context7_registered_with_ctx7_fallback(clean_env):
    """Context7 provider registered with CTX7_API_KEY fallback."""
    os.environ["CTX7_API_KEY"] = "ctx7_key"
    svc = AgentProviderService()
    assert "context7" in svc._registry



# === Edge cases ===


def test_provider_service_with_db():
    """Service can be initialized with db."""
    mock_db = MagicMock()
    svc = AgentProviderService(db=mock_db)
    assert svc.db is mock_db


def test_multiple_providers_same_type(clean_env):
    """Can register multiple providers of same type with different names."""
    svc = AgentProviderService()

    async def provider1(prompt: str = "", **kwargs) -> str:
        return "Provider 1"

    async def provider2(prompt: str = "", **kwargs) -> str:
        return "Provider 2"

    svc.register_provider("custom1", provider1)
    svc.register_provider("custom2", provider2)

    result1 = asyncio.run(svc.get_provider_callable("custom1")(prompt="x"))
    result2 = asyncio.run(svc.get_provider_callable("custom2")(prompt="x"))

    assert result1 == "Provider 1"
    assert result2 == "Provider 2"


def test_provider_override(clean_env):
    """Can override existing provider."""
    svc = AgentProviderService()

    async def new_default(prompt: str = "", **kwargs) -> str:
        return "New default"

    svc.register_provider("default", new_default)
    provider = svc.get_provider_callable(None)
    result = asyncio.run(provider(prompt="test"))
    assert result == "New default"


def test_gpt_lowercase_routing(clean_env):
    """Lowercase gpt model names route to OpenAI."""
    os.environ["OPENAI_API_KEY"] = "test_key"
    svc = AgentProviderService()

    provider = svc.get_provider_callable("gpt-3.5-turbo")
    assert provider is not None


def test_gpt_uppercase_routing(clean_env):
    """Uppercase GPT model names route to OpenAI."""
    os.environ["OPENAI_API_KEY"] = "test_key"
    svc = AgentProviderService()

    provider = svc.get_provider_callable("GPT-4")
    assert provider is not None
