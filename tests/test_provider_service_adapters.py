"""Tests for provider service adapter registration and failure paths."""
from __future__ import annotations

import os
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.services.provider_service import RuntimeProviderRegistry


@pytest.fixture(autouse=True)
def clean_env():
    saved = {}
    for var in [
        "OPENAI_API_KEY", "COHERE_API_KEY", "OLLAMA_BASE", "OLLAMA_URL",
        "HUGGINGFACE_API_KEY", "HUGGINGFACE_TOKEN", "FIREWORKS_BASE",
        "FIREWORKS_API_BASE", "FIREWORKS_API_KEY", "DEEPSEEK_BASE",
        "DEEPSEEK_API_BASE", "DEEPSEEK_API_KEY", "TOGETHER_BASE",
        "TOGETHER_API_BASE", "TOGETHER_API_KEY", "CONTEXT7_API_KEY",
        "CTX7_API_KEY", "ZAI_API_KEY", "ZAI_BASE_URL",
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
    os.environ["ZAI_BASE_URL"] = "https://api.z.ai/api/coding/paas/v4"
    with patch.object(
        RuntimeProviderRegistry,
        "_make_openai_compat_provider",
        side_effect=RuntimeError("no openai"),
    ):
        svc = RuntimeProviderRegistry()
    assert "zai" not in svc._registry


def test_zai_env_registration_defaults_without_base_url(clean_env):
    """ZAI_API_KEY without ZAI_BASE_URL uses the subscription endpoint."""
    os.environ["ZAI_API_KEY"] = "zai-test-key"
    svc = RuntimeProviderRegistry()
    assert "zai" in svc._registry


# === Context7 registration failure ===


def test_context7_registration_failure_path(clean_env):
    """Context7 adapter registration failure is caught gracefully."""
    os.environ["CONTEXT7_API_KEY"] = "ctx7-test-key"
    with patch("src.services.provider_adapters.make_context7_adapter", side_effect=ImportError("no ctx7")):
        svc = RuntimeProviderRegistry()
    assert "context7" not in svc._registry


# === Env LangChain adapters ===


def test_env_langchain_adapter_registration_does_not_depend_on_provider_adapters(clean_env):
    """LLM env adapters no longer depend on custom provider_adapters."""
    os.environ["COHERE_API_KEY"] = "cohere-test"
    with patch.dict("sys.modules", {"src.services.provider_adapters": None}):
        svc = RuntimeProviderRegistry()
    assert "cohere" in svc._registry


# === Exception during individual env adapter registration ===


def test_env_adapter_registration_exception(clean_env):
    """Single adapter failure doesn't prevent others from registering."""
    os.environ["COHERE_API_KEY"] = "cohere-key"
    os.environ["OLLAMA_BASE"] = "http://localhost:11434"

    async def fallback_provider(**kwargs):
        return "ok"

    with patch.object(
        RuntimeProviderRegistry,
        "_make_provider_for_runtime_config",
        side_effect=[ValueError("bad adapter"), fallback_provider],
    ):
        svc = RuntimeProviderRegistry()

    assert "cohere" not in svc._registry
    assert "ollama" in svc._registry


# === get_provider_status_list exception ===


@pytest.mark.anyio
async def test_get_provider_status_list_db_exception():
    """Returns empty list when DB load fails."""
    svc = RuntimeProviderRegistry()
    mock_db = MagicMock()
    mock_config = MagicMock()
    svc.db = mock_db
    svc._config = mock_config

    with patch("src.services.agent_provider_service.ProviderConfigService") as mock_cls:
        mock_instance = MagicMock()
        mock_instance.load_provider_configs = AsyncMock(side_effect=Exception("DB error"))
        mock_cls.return_value = mock_instance
        result = await svc.get_provider_status_list()

    assert result == []


# === get_provider_callable: OpenAI not registered but GPT model requested ===


@pytest.mark.anyio
async def test_get_provider_gpt_fallback_without_openai():
    """When OpenAI not registered but gpt model requested, falls back to default."""
    svc = RuntimeProviderRegistry()
    provider = svc.get_provider_callable("gpt-4")
    result = await provider(prompt="test")
    assert "DRAFT" in result


# === _make_openai_compat_provider: LangChain error ===


@pytest.mark.anyio
async def test_openai_compat_provider_langchain_error():
    """Provider propagates LangChain runtime errors."""
    svc = RuntimeProviderRegistry()
    provider_fn = svc._make_openai_compat_provider("http://localhost:1234", "test-key")

    with patch("langchain.chat_models.init_chat_model", side_effect=RuntimeError("Provider error 429")):
        with pytest.raises(RuntimeError, match="Provider error 429"):
            await provider_fn(prompt="test")


# === _make_openai_compat_provider: response extraction ===


@pytest.mark.anyio
async def test_openai_compat_provider_extracts_langchain_content_blocks():
    """Provider extracts text from LangChain content blocks."""
    svc = RuntimeProviderRegistry()
    provider_fn = svc._make_openai_compat_provider("http://localhost:1234", "test-key")
    captured = {}

    class FakeChatModel:
        async def ainvoke(self, prompt):
            captured["prompt"] = prompt
            return SimpleNamespace(content=[{"text": "hello"}, {"content": "world"}])

    def fake_init_chat_model(**kwargs):
        captured["kwargs"] = kwargs
        return FakeChatModel()

    with patch("langchain.chat_models.init_chat_model", fake_init_chat_model):
        result = await provider_fn(prompt="test", model="gpt-test", max_tokens=17, temperature=0.3)

    assert result == "hello\nworld"
    assert captured["prompt"] == "test"
    assert captured["kwargs"]["model"] == "gpt-test"
    assert captured["kwargs"]["model_provider"] == "openai"
    assert captured["kwargs"]["base_url"] == "http://localhost:1234"
    assert captured["kwargs"]["api_key"] == "test-key"
    assert captured["kwargs"]["max_tokens"] == 17
    assert captured["kwargs"]["temperature"] == 0.3


# === _make_openai_provider: unknown response fallback ===


@pytest.mark.anyio
async def test_openai_provider_unknown_response_fallback(clean_env):
    """OpenAI provider returns stringified response when no text/content is exposed."""
    os.environ["OPENAI_API_KEY"] = "test-key"

    class WeirdResponse:
        def __str__(self):
            return "{'unexpected': 'format'}"

    class FakeChatModel:
        async def ainvoke(self, prompt):
            return WeirdResponse()

    with patch("langchain.chat_models.init_chat_model", return_value=FakeChatModel()):
        svc = RuntimeProviderRegistry()
        provider = svc.get_provider_callable("openai")
        result = await provider(prompt="test")
    assert "unexpected" in result


# === get_provider_status_list: with configs ===


@pytest.mark.anyio
async def test_get_provider_status_list_with_configs():
    """Returns status list for configured providers."""
    svc = RuntimeProviderRegistry()

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

    with patch("src.services.agent_provider_service.ProviderConfigService") as mock_cls:
        mock_instance = MagicMock()
        mock_instance.load_provider_configs = AsyncMock(return_value=[cfg1, cfg2, cfg3])
        mock_cls.return_value = mock_instance

        with patch.object(svc, "_has_valid_secrets", side_effect=lambda c: c.provider != "no_secrets"):
            result = await svc.get_provider_status_list()

    assert len(result) == 3
    assert result[0]["status"] == "active"
    assert result[1]["status"] == "disabled"
    assert result[2]["status"] == "invalid_secrets"
