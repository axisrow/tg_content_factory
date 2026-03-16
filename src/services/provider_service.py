from __future__ import annotations

import os
from typing import Any, Awaitable, Callable, Dict, Optional

import aiohttp


class AgentProviderService:
    """Simple provider registry for generation providers.

    Provider callable signature (async):
        async def provider(prompt: str, model: Optional[str]=None, max_tokens: int=256, temperature: float=0.0, stream: bool=False) -> str

    The service registers a default stub provider under the name 'default'. If an
    OPENAI_API_KEY is present in the environment, a basic OpenAI chat provider is
    registered under the name 'openai' and will be used when model names like
    'gpt-3.5-turbo' are passed.
    """

    def __init__(self, db: Optional[object] = None) -> None:
        self.db = db
        self._registry: Dict[str, Callable[..., Awaitable[str]]] = {}
        # register default provider
        self.register_provider("default", self._default_provider)

        # optional OpenAI provider (HTTP REST, minimal)
        openai_key = os.environ.get("OPENAI_API_KEY")
        if openai_key:
            self.register_provider("openai", self._make_openai_provider(openai_key))

        # optional Context7 provider (user may supply CONTEXT7_API_KEY)
        context7_key = os.environ.get("CONTEXT7_API_KEY") or os.environ.get("CTX7_API_KEY")
        if context7_key:
            try:
                from src.services.provider_adapters import make_context7_adapter

                self.register_provider("context7", make_context7_adapter(context7_key))
            except Exception:
                # ignore if adapter module unavailable
                pass

        # Register lightweight HTTP adapters when env vars are present
        try:
            from src.services.provider_adapters import (
                make_cohere_adapter,
                make_ollama_adapter,
                make_huggingface_adapter,
                make_generic_http_adapter,
            )

            cohere_key = os.environ.get("COHERE_API_KEY")
            if cohere_key and "cohere" not in self._registry:
                self.register_provider("cohere", make_cohere_adapter(cohere_key))

            ollama_base = os.environ.get("OLLAMA_BASE") or os.environ.get("OLLAMA_URL")
            if ollama_base and "ollama" not in self._registry:
                self.register_provider("ollama", make_ollama_adapter(ollama_base, os.environ.get("OLLAMA_API_KEY")))

            hf_key = os.environ.get("HUGGINGFACE_API_KEY") or os.environ.get("HUGGINGFACE_TOKEN")
            if hf_key and "huggingface" not in self._registry:
                self.register_provider("huggingface", make_huggingface_adapter(hf_key))

            # Generic providers (Fireworks / DeepSeek / Together) via base URL env vars
            fireworks_base = os.environ.get("FIREWORKS_BASE") or os.environ.get("FIREWORKS_API_BASE")
            fireworks_key = os.environ.get("FIREWORKS_API_KEY")
            if fireworks_base and "fireworks" not in self._registry:
                self.register_provider("fireworks", make_generic_http_adapter(fireworks_base, fireworks_key))

            deepseek_base = os.environ.get("DEEPSEEK_BASE") or os.environ.get("DEEPSEEK_API_BASE")
            deepseek_key = os.environ.get("DEEPSEEK_API_KEY")
            if deepseek_base and "deepseek" not in self._registry:
                self.register_provider("deepseek", make_generic_http_adapter(deepseek_base, deepseek_key))

            together_base = os.environ.get("TOGETHER_BASE") or os.environ.get("TOGETHER_API_BASE")
            together_key = os.environ.get("TOGETHER_API_KEY")
            if together_base and "together" not in self._registry:
                self.register_provider("together", make_generic_http_adapter(together_base, together_key))
        except Exception:
            # provider_adapters import failed — skip lightweight adapter registration
            pass

        # Optional LangChain-backed adapters (enable by setting USE_LANGCHAIN=1).
        # When enabled, attempt to register LangChain adapters for common providers.
        if os.environ.get("USE_LANGCHAIN", "").lower() in ("1", "true", "yes"):
            try:
                from src.services.langchain_adapters import make_langchain_adapter
                for _p in ("openai", "anthropic", "ollama", "cohere", "huggingface"):
                    try:
                        adapter = make_langchain_adapter(_p, None)
                        # prefer LangChain adapter: override existing if present
                        self._registry[_p] = adapter
                    except Exception:
                        # ignore provider-specific failures
                        continue
            except Exception:
                # LangChain not available or adapter import failed; continue silently
                pass

    def register_provider(self, name: str, func: Callable[..., Awaitable[str]]) -> None:
        self._registry[name] = func

    def get_provider_callable(self, name: Optional[str] = None) -> Callable[..., Awaitable[str]]:
        """Resolve a provider callable.

        If `name` matches a registered provider, it is returned. Otherwise, if an
        OpenAI provider is registered and `name` looks like an OpenAI model id
        (contains 'gpt'), return a wrapper that calls the OpenAI provider with
        the model preset to `name`.
        """
        if not name:
            return self._registry["default"]
        if name in self._registry:
            return self._registry[name]
        lower = name.lower() if isinstance(name, str) else ""
        if "openai" in self._registry and ("gpt" in lower or lower.startswith("gpt")):
            base = self._registry["openai"]

            async def _call(prompt: str = "", model: Optional[str] = None, **kwargs: Any) -> str:
                # force the model to the requested name
                return await base(prompt=prompt, model=name, **kwargs)

            return _call
        # fallback
        return self._registry["default"]

    def _make_openai_provider(self, api_key: str) -> Callable[..., Awaitable[str]]:
        async def _openai_provider(
            prompt: str = "",
            model: Optional[str] = None,
            max_tokens: int = 256,
            temperature: float = 0.0,
            stream: bool = False,
            **kwargs: Any,
        ) -> str:
            model_to_use = model or os.environ.get("OPENAI_DEFAULT_MODEL", "gpt-3.5-turbo")
            base_url = os.environ.get("OPENAI_API_BASE", "https://api.openai.com/v1")
            url = f"{base_url}/chat/completions"
            headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}
            payload = {
                "model": model_to_use,
                "messages": [{"role": "user", "content": prompt}],
                "max_tokens": int(max_tokens or 256),
                "temperature": float(temperature or 0.0),
            }
            timeout = aiohttp.ClientTimeout(total=int(os.environ.get("OPENAI_TIMEOUT_SECONDS", "60")))
            async with aiohttp.ClientSession() as session:
                async with session.post(url, json=payload, headers=headers, timeout=timeout) as resp:
                    text = await resp.text()
                    if resp.status != 200:
                        raise RuntimeError(f"OpenAI error {resp.status}: {text}")
                    data = await resp.json()
                    # Support chat completion response
                    try:
                        return data["choices"][0]["message"]["content"]
                    except Exception:
                        # Fallback to stringified response
                        return str(data)

        return _openai_provider

    async def _default_provider(self, **kwargs: Any) -> str:
        prompt = kwargs.get("prompt", "") or ""
        # Minimal safe stub provider for local testing / fallback
        return "DRAFT (default provider): " + (prompt[:400])
