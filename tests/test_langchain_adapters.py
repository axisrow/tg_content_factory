"""Tests for LangChain adapters."""

from __future__ import annotations

import types
from unittest.mock import MagicMock, patch

import pytest

from src.services.langchain_adapters import is_langchain_available, make_langchain_adapter


# === is_langchain_available tests ===


def test_is_langchain_available_true(monkeypatch):
    """Returns True when langchain is importable."""
    def fake_import(name):
        if name == "langchain":
            return types.SimpleNamespace()
        raise ImportError(f"No module {name}")

    monkeypatch.setattr("importlib.import_module", fake_import)
    assert is_langchain_available() is True


def test_is_langchain_available_false(monkeypatch):
    """Returns False when langchain is not importable."""
    def fake_import(name):
        raise ImportError(f"No module {name}")

    monkeypatch.setattr("importlib.import_module", fake_import)
    assert is_langchain_available() is False


# === OpenAI provider tests ===


@pytest.mark.asyncio
async def test_langchain_adapter_openai_success(monkeypatch):
    """OpenAI adapter works with mock LangChain."""
    def fake_import(name):
        if name == "langchain.chat_models":
            m = types.SimpleNamespace()

            class ChatOpenAI:
                def __init__(self, **kwargs):
                    pass

                async def agenerate(self, messages):
                    g = types.SimpleNamespace(text="OpenAI LC reply")
                    return types.SimpleNamespace(generations=[[g]])

            setattr(m, "ChatOpenAI", ChatOpenAI)
            return m

        if name == "langchain.schema":
            m = types.SimpleNamespace()

            class HumanMessage:
                def __init__(self, content=None):
                    self.content = content

            setattr(m, "HumanMessage", HumanMessage)
            return m

        if name == "langchain.llms":
            return types.SimpleNamespace()

        raise ImportError(f"No module {name}")

    monkeypatch.setattr("src.services.langchain_adapters.is_langchain_available", lambda: True)
    monkeypatch.setattr("src.services.langchain_adapters.importlib.import_module", fake_import)

    adapter = make_langchain_adapter("openai", {"api_key": "fake"})
    out = await adapter("hello")
    assert "OpenAI LC reply" in out


@pytest.mark.asyncio
async def test_langchain_adapter_openai_uses_env_vars(monkeypatch):
    """OpenAI adapter uses environment variables."""
    import os

    monkeypatch.setenv("OPENAI_API_KEY", "env_key")
    monkeypatch.setenv("OPENAI_API_BASE", "https://custom.api")

    captured_kwargs = {}

    def fake_import(name):
        if name == "langchain.chat_models":
            m = types.SimpleNamespace()

            class ChatOpenAI:
                def __init__(self, **kwargs):
                    captured_kwargs.update(kwargs)

                async def agenerate(self, messages):
                    g = types.SimpleNamespace(text="Reply")
                    return types.SimpleNamespace(generations=[[g]])

            setattr(m, "ChatOpenAI", ChatOpenAI)
            return m

        if name == "langchain.schema":
            m = types.SimpleNamespace()

            class HumanMessage:
                def __init__(self, content=None):
                    self.content = content

            setattr(m, "HumanMessage", HumanMessage)
            return m

        if name == "langchain.llms":
            return types.SimpleNamespace()

        raise ImportError(f"No module {name}")

    monkeypatch.setattr("src.services.langchain_adapters.is_langchain_available", lambda: True)
    monkeypatch.setattr("src.services.langchain_adapters.importlib.import_module", fake_import)

    adapter = make_langchain_adapter("openai", {})
    await adapter("test")

    assert captured_kwargs.get("openai_api_key") == "env_key"
    assert captured_kwargs.get("openai_api_base") == "https://custom.api"


# === Anthropic provider tests ===


@pytest.mark.asyncio
async def test_langchain_adapter_anthropic_success(monkeypatch):
    """Anthropic adapter works with mock LangChain."""
    def fake_import(name):
        if name == "langchain.chat_models":
            m = types.SimpleNamespace()

            class Anthropic:
                def __init__(self, **kwargs):
                    pass

                async def agenerate(self, messages):
                    g = types.SimpleNamespace(text="Anthropic reply")
                    return types.SimpleNamespace(generations=[[g]])

            setattr(m, "Anthropic", Anthropic)
            return m

        if name == "langchain.schema":
            m = types.SimpleNamespace()

            class HumanMessage:
                def __init__(self, content=None):
                    self.content = content

            setattr(m, "HumanMessage", HumanMessage)
            return m

        if name == "langchain.llms":
            return types.SimpleNamespace()

        raise ImportError(f"No module {name}")

    monkeypatch.setattr("src.services.langchain_adapters.is_langchain_available", lambda: True)
    monkeypatch.setattr("src.services.langchain_adapters.importlib.import_module", fake_import)

    adapter = make_langchain_adapter("anthropic", {})
    out = await adapter("test")
    assert "Anthropic reply" in out


# === Ollama provider tests ===


@pytest.mark.asyncio
async def test_langchain_adapter_ollama_success(monkeypatch):
    """Ollama adapter works with mock LangChain."""
    def fake_import(name):
        if name == "langchain.chat_models":
            m = types.SimpleNamespace()

            class Ollama:
                def __init__(self, **kwargs):
                    pass

                async def agenerate(self, messages):
                    g = types.SimpleNamespace(text="Ollama reply")
                    return types.SimpleNamespace(generations=[[g]])

            setattr(m, "Ollama", Ollama)
            return m

        if name == "langchain.schema":
            m = types.SimpleNamespace()

            class HumanMessage:
                def __init__(self, content=None):
                    self.content = content

            setattr(m, "HumanMessage", HumanMessage)
            return m

        if name == "langchain.llms":
            return types.SimpleNamespace()

        raise ImportError(f"No module {name}")

    monkeypatch.setattr("src.services.langchain_adapters.is_langchain_available", lambda: True)
    monkeypatch.setattr("src.services.langchain_adapters.importlib.import_module", fake_import)

    adapter = make_langchain_adapter("ollama", {})
    out = await adapter("test")
    assert "Ollama reply" in out


@pytest.mark.asyncio
async def test_langchain_adapter_ollama_uses_env_vars(monkeypatch):
    """Ollama adapter uses environment variables."""
    import os

    monkeypatch.setenv("OLLAMA_BASE", "http://ollama.local:11434")
    monkeypatch.setenv("OLLAMA_API_KEY", "ollama_key")

    captured_kwargs = {}

    def fake_import(name):
        if name == "langchain.chat_models":
            m = types.SimpleNamespace()

            class Ollama:
                def __init__(self, **kwargs):
                    captured_kwargs.update(kwargs)

                async def agenerate(self, messages):
                    g = types.SimpleNamespace(text="Reply")
                    return types.SimpleNamespace(generations=[[g]])

            setattr(m, "Ollama", Ollama)
            return m

        if name == "langchain.schema":
            m = types.SimpleNamespace()

            class HumanMessage:
                def __init__(self, content=None):
                    self.content = content

            setattr(m, "HumanMessage", HumanMessage)
            return m

        if name == "langchain.llms":
            return types.SimpleNamespace()

        raise ImportError(f"No module {name}")

    monkeypatch.setattr("src.services.langchain_adapters.is_langchain_available", lambda: True)
    monkeypatch.setattr("src.services.langchain_adapters.importlib.import_module", fake_import)

    adapter = make_langchain_adapter("ollama", {})
    await adapter("test")

    assert captured_kwargs.get("base_url") == "http://ollama.local:11434"
    assert captured_kwargs.get("api_key") == "ollama_key"


# === Cohere provider tests ===


@pytest.mark.asyncio
async def test_langchain_adapter_cohere_success(monkeypatch):
    """Cohere adapter works with mock LangChain."""
    def fake_import(name):
        if name == "langchain.chat_models":
            m = types.SimpleNamespace()
            return m

        if name == "langchain.schema":
            m = types.SimpleNamespace()

            class HumanMessage:
                def __init__(self, content=None):
                    self.content = content

            setattr(m, "HumanMessage", HumanMessage)
            return m

        if name == "langchain.llms":
            m = types.SimpleNamespace()

            class Cohere:
                def __init__(self, **kwargs):
                    pass

                async def agenerate(self, messages):
                    g = types.SimpleNamespace(text="Cohere reply")
                    return types.SimpleNamespace(generations=[[g]])

            setattr(m, "Cohere", Cohere)
            return m

        raise ImportError(f"No module {name}")

    monkeypatch.setattr("src.services.langchain_adapters.is_langchain_available", lambda: True)
    monkeypatch.setattr("src.services.langchain_adapters.importlib.import_module", fake_import)

    adapter = make_langchain_adapter("cohere", {})
    out = await adapter("test")
    assert "Cohere reply" in out


# === HuggingFace provider tests ===


@pytest.mark.asyncio
async def test_langchain_adapter_huggingface_success(monkeypatch):
    """HuggingFace adapter works with mock LangChain."""
    def fake_import(name):
        if name == "langchain.chat_models":
            m = types.SimpleNamespace()
            return m

        if name == "langchain.schema":
            m = types.SimpleNamespace()

            class HumanMessage:
                def __init__(self, content=None):
                    self.content = content

            setattr(m, "HumanMessage", HumanMessage)
            return m

        if name == "langchain.llms":
            m = types.SimpleNamespace()

            class HuggingFaceHub:
                def __init__(self, **kwargs):
                    pass

                async def agenerate(self, messages):
                    g = types.SimpleNamespace(text="HF reply")
                    return types.SimpleNamespace(generations=[[g]])

            setattr(m, "HuggingFaceHub", HuggingFaceHub)
            return m

        raise ImportError(f"No module {name}")

    monkeypatch.setattr("src.services.langchain_adapters.is_langchain_available", lambda: True)
    monkeypatch.setattr("src.services.langchain_adapters.importlib.import_module", fake_import)

    adapter = make_langchain_adapter("huggingface", {})
    out = await adapter("test")
    assert "HF reply" in out


# === Error handling tests ===


@pytest.mark.asyncio
async def test_langchain_adapter_raises_when_missing(monkeypatch):
    """Raises RuntimeError when LangChain is not installed."""
    monkeypatch.setattr("src.services.langchain_adapters.is_langchain_available", lambda: False)
    adapter = make_langchain_adapter("openai", {"api_key": "fake"})

    with pytest.raises(RuntimeError) as exc_info:
        await adapter("hello")
    assert "LangChain is not installed" in str(exc_info.value)


@pytest.mark.asyncio
async def test_langchain_adapter_unsupported_provider(monkeypatch):
    """Raises RuntimeError for unsupported provider."""
    def fake_import(name):
        if name == "langchain.chat_models":
            return types.SimpleNamespace()  # Empty - no classes
        if name == "langchain.schema":
            m = types.SimpleNamespace()
            setattr(m, "HumanMessage", lambda content=None: None)
            return m
        if name == "langchain.llms":
            return types.SimpleNamespace()
        raise ImportError(f"No module {name}")

    monkeypatch.setattr("src.services.langchain_adapters.is_langchain_available", lambda: True)
    monkeypatch.setattr("src.services.langchain_adapters.importlib.import_module", fake_import)

    adapter = make_langchain_adapter("unsupported_provider", {})
    with pytest.raises(RuntimeError) as exc_info:
        await adapter("test")
    assert "not available" in str(exc_info.value)


@pytest.mark.asyncio
async def test_langchain_adapter_init_failure_fallback(monkeypatch):
    """Falls back when __init__ fails with model_name."""
    captured_kwargs = {}

    def fake_import(name):
        if name == "langchain.chat_models":
            m = types.SimpleNamespace()

            class ChatOpenAI:
                def __init__(self, **kwargs):
                    captured_kwargs.update(kwargs)
                    if "model_name" in kwargs:
                        raise TypeError("model_name not supported")

                async def agenerate(self, messages):
                    g = types.SimpleNamespace(text="Reply after fallback")
                    return types.SimpleNamespace(generations=[[g]])

            setattr(m, "ChatOpenAI", ChatOpenAI)
            return m

        if name == "langchain.schema":
            m = types.SimpleNamespace()

            class HumanMessage:
                def __init__(self, content=None):
                    self.content = content

            setattr(m, "HumanMessage", HumanMessage)
            return m

        if name == "langchain.llms":
            return types.SimpleNamespace()

        raise ImportError(f"No module {name}")

    monkeypatch.setattr("src.services.langchain_adapters.is_langchain_available", lambda: True)
    monkeypatch.setattr("src.services.langchain_adapters.importlib.import_module", fake_import)

    adapter = make_langchain_adapter("openai", {})
    out = await adapter("test", model="gpt-4")
    assert "Reply after fallback" in out


# === Sync fallback tests ===


@pytest.mark.asyncio
async def test_langchain_adapter_sync_generate_fallback(monkeypatch):
    """Falls back to sync generate when agenerate not available."""
    def fake_import(name):
        if name == "langchain.chat_models":
            m = types.SimpleNamespace()

            class ChatOpenAI:
                def __init__(self, **kwargs):
                    pass

                # No agenerate - only sync generate
                def generate(self, messages):
                    g = types.SimpleNamespace(text="Sync reply")
                    return types.SimpleNamespace(generations=[[g]])

            setattr(m, "ChatOpenAI", ChatOpenAI)
            return m

        if name == "langchain.schema":
            m = types.SimpleNamespace()

            class HumanMessage:
                def __init__(self, content=None):
                    self.content = content

            setattr(m, "HumanMessage", HumanMessage)
            return m

        if name == "langchain.llms":
            return types.SimpleNamespace()

        raise ImportError(f"No module {name}")

    monkeypatch.setattr("src.services.langchain_adapters.is_langchain_available", lambda: True)
    monkeypatch.setattr("src.services.langchain_adapters.importlib.import_module", fake_import)

    adapter = make_langchain_adapter("openai", {})
    out = await adapter("test")
    assert "Sync reply" in out


@pytest.mark.asyncio
async def test_langchain_adapter_no_generate_raises(monkeypatch):
    """Raises when LLM has no generate or agenerate."""
    def fake_import(name):
        if name == "langchain.chat_models":
            m = types.SimpleNamespace()

            class ChatOpenAI:
                def __init__(self, **kwargs):
                    pass
                # No generate methods

            setattr(m, "ChatOpenAI", ChatOpenAI)
            return m

        if name == "langchain.schema":
            m = types.SimpleNamespace()

            class HumanMessage:
                def __init__(self, content=None):
                    self.content = content

            setattr(m, "HumanMessage", HumanMessage)
            return m

        if name == "langchain.llms":
            return types.SimpleNamespace()

        raise ImportError(f"No module {name}")

    monkeypatch.setattr("src.services.langchain_adapters.is_langchain_available", lambda: True)
    monkeypatch.setattr("src.services.langchain_adapters.importlib.import_module", fake_import)

    adapter = make_langchain_adapter("openai", {})
    with pytest.raises(RuntimeError) as exc_info:
        await adapter("test")
    assert "no generate/agenerate method" in str(exc_info.value)


# === Message building tests ===


@pytest.mark.asyncio
async def test_langchain_adapter_uses_human_message(monkeypatch):
    """Uses HumanMessage class when available."""
    received_messages = None

    def fake_import(name):
        if name == "langchain.chat_models":
            m = types.SimpleNamespace()

            class ChatOpenAI:
                def __init__(self, **kwargs):
                    pass

                async def agenerate(self, messages):
                    nonlocal received_messages
                    received_messages = messages
                    g = types.SimpleNamespace(text="Reply")
                    return types.SimpleNamespace(generations=[[g]])

            setattr(m, "ChatOpenAI", ChatOpenAI)
            return m

        if name == "langchain.schema":
            m = types.SimpleNamespace()

            class HumanMessage:
                def __init__(self, content=None):
                    self.content = content

            setattr(m, "HumanMessage", HumanMessage)
            return m

        if name == "langchain.llms":
            return types.SimpleNamespace()

        raise ImportError(f"No module {name}")

    monkeypatch.setattr("src.services.langchain_adapters.is_langchain_available", lambda: True)
    monkeypatch.setattr("src.services.langchain_adapters.importlib.import_module", fake_import)

    adapter = make_langchain_adapter("openai", {})
    await adapter("test message")

    assert received_messages is not None
    assert len(received_messages) == 1


@pytest.mark.asyncio
async def test_langchain_adapter_fallback_dict_message(monkeypatch):
    """Uses dict message when HumanMessage not available."""
    received_messages = None

    def fake_import(name):
        if name == "langchain.chat_models":
            m = types.SimpleNamespace()

            class ChatOpenAI:
                def __init__(self, **kwargs):
                    pass

                async def agenerate(self, messages):
                    nonlocal received_messages
                    received_messages = messages
                    g = types.SimpleNamespace(text="Reply")
                    return types.SimpleNamespace(generations=[[g]])

            setattr(m, "ChatOpenAI", ChatOpenAI)
            return m

        if name == "langchain.schema":
            # No HumanMessage class
            return types.SimpleNamespace()

        if name == "langchain.llms":
            return types.SimpleNamespace()

        raise ImportError(f"No module {name}")

    monkeypatch.setattr("src.services.langchain_adapters.is_langchain_available", lambda: True)
    monkeypatch.setattr("src.services.langchain_adapters.importlib.import_module", fake_import)

    adapter = make_langchain_adapter("openai", {})
    await adapter("test message")

    assert received_messages is not None
    assert received_messages[0] == {"role": "user", "content": "test message"}


# === Parameter passing tests ===


@pytest.mark.asyncio
async def test_langchain_adapter_passes_model(monkeypatch):
    """Passes model name to LLM constructor."""
    captured_kwargs = {}

    def fake_import(name):
        if name == "langchain.chat_models":
            m = types.SimpleNamespace()

            class ChatOpenAI:
                def __init__(self, **kwargs):
                    captured_kwargs.update(kwargs)

                async def agenerate(self, messages):
                    g = types.SimpleNamespace(text="Reply")
                    return types.SimpleNamespace(generations=[[g]])

            setattr(m, "ChatOpenAI", ChatOpenAI)
            return m

        if name == "langchain.schema":
            m = types.SimpleNamespace()

            class HumanMessage:
                def __init__(self, content=None):
                    self.content = content

            setattr(m, "HumanMessage", HumanMessage)
            return m

        if name == "langchain.llms":
            return types.SimpleNamespace()

        raise ImportError(f"No module {name}")

    monkeypatch.setattr("src.services.langchain_adapters.is_langchain_available", lambda: True)
    monkeypatch.setattr("src.services.langchain_adapters.importlib.import_module", fake_import)

    adapter = make_langchain_adapter("openai", {})
    await adapter("test", model="gpt-4-turbo")

    assert captured_kwargs.get("model_name") == "gpt-4-turbo"


@pytest.mark.asyncio
async def test_langchain_adapter_passes_temperature(monkeypatch):
    """Passes temperature to LLM constructor."""
    captured_kwargs = {}

    def fake_import(name):
        if name == "langchain.chat_models":
            m = types.SimpleNamespace()

            class ChatOpenAI:
                def __init__(self, **kwargs):
                    captured_kwargs.update(kwargs)

                async def agenerate(self, messages):
                    g = types.SimpleNamespace(text="Reply")
                    return types.SimpleNamespace(generations=[[g]])

            setattr(m, "ChatOpenAI", ChatOpenAI)
            return m

        if name == "langchain.schema":
            m = types.SimpleNamespace()

            class HumanMessage:
                def __init__(self, content=None):
                    self.content = content

            setattr(m, "HumanMessage", HumanMessage)
            return m

        if name == "langchain.llms":
            return types.SimpleNamespace()

        raise ImportError(f"No module {name}")

    monkeypatch.setattr("src.services.langchain_adapters.is_langchain_available", lambda: True)
    monkeypatch.setattr("src.services.langchain_adapters.importlib.import_module", fake_import)

    adapter = make_langchain_adapter("openai", {})
    await adapter("test", temperature=0.7)

    assert captured_kwargs.get("temperature") == 0.7
