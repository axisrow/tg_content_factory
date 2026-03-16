import types
import pytest

import importlib

from src.services.langchain_adapters import make_langchain_adapter


@pytest.mark.asyncio
async def test_langchain_adapter_raises_when_missing(monkeypatch):
    # Simulate LangChain not being installed
    monkeypatch.setattr("src.services.langchain_adapters.is_langchain_available", lambda: False)
    adapter = make_langchain_adapter("openai", {"api_key": "fake"})

    with pytest.raises(RuntimeError):
        await adapter("hello")


@pytest.mark.asyncio
async def test_langchain_adapter_success(monkeypatch):
    # Simulate minimal langchain modules and classes
    def fake_import(name):
        if name == "langchain.chat_models":
            m = types.SimpleNamespace()

            class ChatOpenAI:
                def __init__(self, **kwargs):
                    pass

                async def agenerate(self, messages):
                    # Return an object with .generations[0][0].text
                    g = types.SimpleNamespace(text="LC reply")
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
    assert "LC reply" in out
