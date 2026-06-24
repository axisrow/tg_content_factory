"""Tests for RuntimeProviderRegistry."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from src.services.provider_service import RuntimeProviderRegistry

# === Default provider tests ===


def test_default_provider_returns_draft():
    """Default provider returns DRAFT prefix."""
    svc = RuntimeProviderRegistry()
    provider = svc.get_provider_callable(None)
    result = asyncio.run(provider(prompt="hello world"))
    assert result.startswith("DRAFT (default provider): hello world")


def test_default_provider_truncates_long_prompt():
    """Default provider truncates prompts longer than 400 chars."""
    svc = RuntimeProviderRegistry()
    provider = svc.get_provider_callable(None)
    long_prompt = "x" * 500
    result = asyncio.run(provider(prompt=long_prompt))
    assert len(result) < 500
    assert "DRAFT" in result


def test_default_provider_empty_prompt():
    """Default provider handles empty prompt."""
    svc = RuntimeProviderRegistry()
    provider = svc.get_provider_callable(None)
    result = asyncio.run(provider(prompt=""))
    assert "DRAFT" in result


# === Registry tests ===


def test_register_provider():
    """Can register custom provider."""
    svc = RuntimeProviderRegistry()

    async def custom_provider(prompt: str = "", **kwargs) -> str:
        return f"Custom: {prompt}"

    svc.register_provider("custom", custom_provider)
    provider = svc.get_provider_callable("custom")
    result = asyncio.run(provider(prompt="test"))
    assert result == "Custom: test"


def test_get_provider_unknown_falls_back_to_default():
    """Unknown provider falls back to default."""
    svc = RuntimeProviderRegistry()
    provider = svc.get_provider_callable("unknown_provider")
    result = asyncio.run(provider(prompt="test"))
    assert "DRAFT" in result


def test_resolve_provider_callable_raises_on_unknown_name():
    """resolve_provider_callable must NOT silently return the stub (#994)."""
    import pytest

    svc = RuntimeProviderRegistry()

    async def custom_provider(prompt: str = "", **kwargs) -> str:
        return prompt

    svc.register_provider("cohere", custom_provider)

    with pytest.raises(ValueError) as excinfo:
        svc.resolve_provider_callable("does-not-exist")
    # The error must name the registered providers so the operator can recover.
    assert "cohere" in str(excinfo.value)


def test_resolve_provider_callable_returns_registered():
    """A registered name resolves to the real callable, no error."""
    svc = RuntimeProviderRegistry()

    async def custom_provider(prompt: str = "", **kwargs) -> str:
        return f"Custom: {prompt}"

    svc.register_provider("custom", custom_provider)
    provider = svc.resolve_provider_callable("custom")
    assert asyncio.run(provider(prompt="x")) == "Custom: x"


def test_resolve_provider_callable_no_name_uses_first_real():
    """With no name, resolve picks the first real provider (no stub error)."""
    svc = RuntimeProviderRegistry()

    async def custom_provider(prompt: str = "", **kwargs) -> str:
        return "real"

    svc.register_provider("custom", custom_provider)
    provider = svc.resolve_provider_callable(None)
    assert asyncio.run(provider(prompt="x")) == "real"


# === OpenAI provider tests ===


def test_openai_provider_registered_with_api_key():
    """OpenAI provider registered when API key present."""
    svc = RuntimeProviderRegistry(env={"OPENAI_API_KEY": "test_key"})
    provider = svc.get_provider_callable("openai")
    assert provider is not None


def test_openai_provider_not_registered_without_key():
    """OpenAI provider not registered without API key."""
    svc = RuntimeProviderRegistry()
    # openai shouldn't be in registry
    assert "openai" not in svc._registry


@pytest.mark.anyio
async def test_openai_provider_calls_api():
    """OpenAI provider calls LangChain chat runtime with expected options."""
    captured = {}

    class FakeChatModel:
        async def ainvoke(self, prompt):
            captured["prompt"] = prompt
            return SimpleNamespace(content="Hello")

    def fake_init_chat_model(**kwargs):
        captured["kwargs"] = kwargs
        return FakeChatModel()

    with patch("langchain.chat_models.init_chat_model", fake_init_chat_model):
        svc = RuntimeProviderRegistry(env={"OPENAI_API_KEY": "test_key"})
        provider = svc.get_provider_callable("openai")
        result = await provider(prompt="hi")

    assert result == "Hello"
    assert captured["prompt"] == "hi"
    assert captured["kwargs"]["model_provider"] == "openai"
    assert captured["kwargs"]["model"] == "gpt-3.5-turbo"
    assert captured["kwargs"]["api_key"] == "test_key"


@pytest.mark.anyio
async def test_openai_provider_error_status():
    """OpenAI provider propagates LangChain runtime errors."""
    with patch("langchain.chat_models.init_chat_model", side_effect=RuntimeError("OpenAI error 401")):
        svc = RuntimeProviderRegistry(env={"OPENAI_API_KEY": "test_key"})
        provider = svc.get_provider_callable("openai")

        with pytest.raises(RuntimeError) as exc_info:
            await provider(prompt="hi")
    assert "401" in str(exc_info.value)


# === GPT model routing tests ===


def test_gpt_model_routes_to_openai():
    """GPT model names route to OpenAI provider."""
    svc = RuntimeProviderRegistry(env={"OPENAI_API_KEY": "test_key"})

    # gpt-4 should create a wrapper that uses openai provider
    provider = svc.get_provider_callable("gpt-4")
    assert provider is not None


def test_gpt_model_without_openai_key_falls_back():
    """GPT model without OpenAI key falls back to default."""
    svc = RuntimeProviderRegistry()
    provider = svc.get_provider_callable("gpt-4-turbo")

    # Should fall back to default since openai not registered
    result = asyncio.run(provider(prompt="test"))
    assert "DRAFT" in result


# === Other provider registration tests ===


def test_cohere_registered_with_api_key():
    """Cohere provider registered when API key present."""
    svc = RuntimeProviderRegistry(env={"COHERE_API_KEY": "cohere_key"})
    assert "cohere" in svc._registry


def test_ollama_registered_with_base_url():
    """Ollama provider registered when base URL present."""
    svc = RuntimeProviderRegistry(env={"OLLAMA_BASE": "http://localhost:11434"})
    assert "ollama" in svc._registry


def test_ollama_registered_with_ollama_url():
    """Ollama provider registered with OLLAMA_URL fallback."""
    svc = RuntimeProviderRegistry(env={"OLLAMA_URL": "http://ollama.local"})
    assert "ollama" in svc._registry


def test_huggingface_registered_with_api_key():
    """HuggingFace provider registered when API key present."""
    svc = RuntimeProviderRegistry(env={"HUGGINGFACE_API_KEY": "hf_key"})
    assert "huggingface" in svc._registry


def test_huggingface_registered_with_token():
    """HuggingFace provider registered with HUGGINGFACE_TOKEN fallback."""
    svc = RuntimeProviderRegistry(env={"HUGGINGFACE_TOKEN": "hf_token"})
    assert "huggingface" in svc._registry


def test_fireworks_registered():
    """Fireworks provider registered with base URL and key."""
    svc = RuntimeProviderRegistry(
        env={"FIREWORKS_BASE": "https://fireworks.api", "FIREWORKS_API_KEY": "fw_key"}
    )
    assert "fireworks" in svc._registry


def test_deepseek_registered():
    """DeepSeek provider registered with base URL and key."""
    svc = RuntimeProviderRegistry(
        env={"DEEPSEEK_BASE": "https://deepseek.api", "DEEPSEEK_API_KEY": "ds_key"}
    )
    assert "deepseek" in svc._registry


def test_together_registered():
    """Together provider registered with base URL and key."""
    svc = RuntimeProviderRegistry(
        env={"TOGETHER_BASE": "https://together.api", "TOGETHER_API_KEY": "tg_key"}
    )
    assert "together" in svc._registry


def test_context7_registered_with_api_key():
    """Context7 provider registered when API key present."""
    svc = RuntimeProviderRegistry(env={"CONTEXT7_API_KEY": "c7_key"})
    assert "context7" in svc._registry


def test_context7_registered_with_ctx7_fallback():
    """Context7 provider registered with CTX7_API_KEY fallback."""
    svc = RuntimeProviderRegistry(env={"CTX7_API_KEY": "ctx7_key"})
    assert "context7" in svc._registry



# === Edge cases ===


def test_provider_service_with_db():
    """Service can be initialized with db."""
    mock_db = MagicMock()
    svc = RuntimeProviderRegistry(db=mock_db)
    assert svc.db is mock_db


def test_multiple_providers_same_type():
    """Can register multiple providers of same type with different names."""
    svc = RuntimeProviderRegistry()

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


def test_provider_override():
    """Can override existing provider."""
    svc = RuntimeProviderRegistry()

    async def new_default(prompt: str = "", **kwargs) -> str:
        return "New default"

    svc.register_provider("default", new_default)
    provider = svc.get_provider_callable(None)
    result = asyncio.run(provider(prompt="test"))
    assert result == "New default"


def test_gpt_lowercase_routing():
    """Lowercase gpt model names route to OpenAI."""
    svc = RuntimeProviderRegistry(env={"OPENAI_API_KEY": "test_key"})

    provider = svc.get_provider_callable("gpt-3.5-turbo")
    assert provider is not None


def test_gpt_uppercase_routing():
    """Uppercase GPT model names route to OpenAI."""
    svc = RuntimeProviderRegistry(env={"OPENAI_API_KEY": "test_key"})

    provider = svc.get_provider_callable("GPT-4")
    assert provider is not None
