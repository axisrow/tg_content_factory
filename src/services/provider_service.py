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
            timeout = int(os.environ.get("OPENAI_TIMEOUT_SECONDS", "60"))
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
