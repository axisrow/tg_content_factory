from __future__ import annotations

import asyncio
import importlib
import os
from typing import Any, Awaitable, Callable, Dict, Optional


def is_langchain_available() -> bool:
    """Return True if langchain package is importable."""
    try:
        importlib.import_module("langchain")
        return True
    except Exception:
        return False


def make_langchain_adapter(
    provider: str, credentials: Optional[Dict[str, str]] = None
) -> Callable[..., Awaitable[str]]:
    """Create a LangChain-backed provider callable for `provider`.

    The returned callable has signature:
        async def provider(prompt, model=None, max_tokens=256, temperature=0.0, stream=False, **kwargs) -> str

    This function imports LangChain at runtime and attempts to instantiate a
    provider-specific wrapper. If LangChain or the provider wrapper isn't
    available, a RuntimeError is raised when the callable is invoked.
    """
    credentials = credentials or {}
    provider_lower = provider.lower()

    async def provider_callable(
        prompt: str = "",
        model: Optional[str] = None,
        max_tokens: int = 256,
        temperature: float = 0.0,
        stream: bool = False,
        **kwargs: Any,
    ) -> str:
        if not is_langchain_available():
            raise RuntimeError(
                "LangChain is not installed. Set USE_LANGCHAIN=1 and install 'langchain' and provider packages to use LangChain adapters."
            )

        # Import modules lazily to avoid hard dependency at import time
        chat_models = importlib.import_module("langchain.chat_models")
        schema = importlib.import_module("langchain.schema")

        # Map provider name to a LangChain class if available
        cls = None
        if provider_lower in ("openai",):
            cls = getattr(chat_models, "ChatOpenAI", None) or getattr(chat_models, "OpenAI", None)
        elif provider_lower in ("anthropic",):
            cls = getattr(chat_models, "Anthropic", None)
        elif provider_lower in ("ollama",):
            cls = getattr(chat_models, "Ollama", None)
        elif provider_lower in ("cohere",):
            try:
                llms = importlib.import_module("langchain.llms")
                cls = getattr(llms, "Cohere", None)
            except Exception:
                cls = None
        elif provider_lower in ("huggingface", "huggingface_hub"):
            try:
                llms = importlib.import_module("langchain.llms")
                cls = getattr(llms, "HuggingFaceHub", None) or getattr(llms, "HuggingFace", None)
            except Exception:
                cls = None

        if cls is None:
            raise RuntimeError(
                f"LangChain adapter for provider '{provider}' is not available in the installed langchain package."
            )

        # Build constructor kwargs using common names; tolerate differences with fallbacks
        init_kwargs: Dict[str, Any] = {}
        if provider_lower == "openai":
            api_key = credentials.get("api_key") or os.environ.get("OPENAI_API_KEY")
            if api_key:
                init_kwargs["openai_api_key"] = api_key
            base_url = os.environ.get("OPENAI_API_BASE")
            if base_url:
                init_kwargs["openai_api_base"] = base_url
        elif provider_lower == "ollama":
            base = (
                credentials.get("base_url")
                or os.environ.get("OLLAMA_BASE")
                or os.environ.get("OLLAMA_URL")
            )
            if base:
                init_kwargs["base_url"] = base
            api_key = credentials.get("api_key") or os.environ.get("OLLAMA_API_KEY")
            if api_key:
                init_kwargs["api_key"] = api_key

        if model:
            # many LangChain classes accept model_name
            init_kwargs["model_name"] = model

        if temperature is not None:
            init_kwargs["temperature"] = float(temperature)

        # Instantiate with fallbacks to handle different langchain versions
        llm = None
        try:
            llm = cls(**init_kwargs)
        except TypeError:
            try:
                tmp = dict(init_kwargs)
                tmp.pop("model_name", None)
                llm = cls(**tmp)
            except Exception as ex:
                raise RuntimeError(
                    f"Failed to instantiate LangChain LLM for provider {provider}: {ex}"
                )

        # Build message payload using schema.HumanMessage when available
        HumanMessage = getattr(schema, "HumanMessage", None)
        messages = (
            [HumanMessage(content=prompt)]
            if HumanMessage is not None
            else [{"role": "user", "content": prompt}]
        )

        # Prefer async API if available
        if hasattr(llm, "agenerate"):
            result = await llm.agenerate(messages)
            try:
                return result.generations[0][0].text
            except Exception:
                return str(result)
        elif hasattr(llm, "generate"):
            loop = asyncio.get_event_loop()

            def sync_call():
                return llm.generate(messages)

            res = await loop.run_in_executor(None, sync_call)
            try:
                return res.generations[0][0].text
            except Exception:
                return str(res)
        else:
            raise RuntimeError("LangChain LLM instance has no generate/agenerate method")

    return provider_callable
