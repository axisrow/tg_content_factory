from __future__ import annotations

import asyncio
from typing import Any, Awaitable, Callable, Dict, Optional


class AgentProviderService:
    """Simple provider registry for generation providers.

    Provider callable signature (async):
        async def provider(prompt: str, model: Optional[str]=None, max_tokens: int=256, temperature: float=0.0, stream: bool=False) -> str

    The service registers a default stub provider under the name 'default'.
    """

    def __init__(self, db: Optional[object] = None) -> None:
        self.db = db
        self._registry: Dict[str, Callable[..., Awaitable[str]]] = {}
        # register default provider
        self.register_provider("default", self._default_provider)

    def register_provider(self, name: str, func: Callable[..., Awaitable[str]]) -> None:
        self._registry[name] = func

    def get_provider_callable(self, name: Optional[str] = None) -> Callable[..., Awaitable[str]]:
        if not name:
            name = "default"
        return self._registry.get(name, self._registry["default"])

    async def _default_provider(self, **kwargs: Any) -> str:
        prompt = kwargs.get("prompt", "") or ""
        # Minimal safe stub provider for local testing / fallback
        return "DRAFT (default provider): " + (prompt[:400])
