from __future__ import annotations

import logging
import os
from typing import Any, Awaitable, Callable, Dict, Optional

import aiohttp

from src.agent.provider_registry import (
    is_zai_legacy_anthropic_base_url,
    normalize_ollama_base_url,
    normalize_zai_base_url,
    provider_spec,
)

logger = logging.getLogger(__name__)

async def build_provider_service(
    db: object | None = None,
    config: object | None = None,
) -> "AgentProviderService":
    """Create AgentProviderService and eagerly load DB-backed providers when possible."""
    svc = AgentProviderService(db, config)
    if db is not None and config is not None:
        await svc.load_db_providers()
    return svc


class AgentProviderService:
    """Simple provider registry for generation providers.

    Provider callable signature (async):
        async def provider(
            prompt: str,
            model: Optional[str] = None,
            max_tokens: int = 256,
            temperature: float = 0.0,
            stream: bool = False,
        ) -> str

    The service registers a default stub provider under the name 'default'. If an
    OPENAI_API_KEY is present in the environment, a basic OpenAI chat provider is
    registered under the name 'openai' and will be used when model names like
    'gpt-3.5-turbo' are passed.

    DB-backed providers are loaded via ``load_db_providers()`` and complement
    env-based ones.
    """

    def __init__(self, db: Optional[object] = None, config: Optional[object] = None) -> None:
        self.db = db
        self._config = config
        self._registry: Dict[str, Callable[..., Awaitable[str]]] = {}
        self._db_provider_names: set[str] = set()
        # register default provider
        self.register_provider("default", self._default_provider)
        self._register_env_providers()

    def _register_env_providers(self) -> None:
        """Register providers from environment variables (original behaviour)."""
        # optional OpenAI provider (HTTP REST, minimal)
        openai_key = os.environ.get("OPENAI_API_KEY")
        if openai_key:
            self.register_provider("openai", self._make_openai_provider(openai_key))

        # optional Z.AI provider. ZAI_BASE_URL is optional; empty value means
        # the subscription/Coding Plan endpoint.
        zai_key = os.environ.get("ZAI_API_KEY")
        zai_base = normalize_zai_base_url(os.environ.get("ZAI_BASE_URL") or "")
        if zai_key and "zai" not in self._registry:
            try:
                self.register_provider(
                    "zai", self._make_openai_compat_provider(zai_base, zai_key)
                )
            except Exception:
                logger.debug("Failed to register zai adapter", exc_info=True)

        # optional Context7 provider (user may supply CONTEXT7_API_KEY)
        context7_key = os.environ.get("CONTEXT7_API_KEY") or os.environ.get("CTX7_API_KEY")
        if context7_key:
            try:
                from src.services.provider_adapters import make_context7_adapter

                self.register_provider("context7", make_context7_adapter(context7_key))
            except Exception:
                logger.debug("Failed to register context7 adapter", exc_info=True)

        # Register lightweight HTTP adapters when env vars are present
        try:
            from src.services.provider_adapters import (
                make_cohere_adapter,
                make_generic_http_adapter,
                make_huggingface_adapter,
                make_ollama_adapter,
            )
        except ImportError:
            logger.debug("provider_adapters not available, skipping HTTP adapters")
            make_cohere_adapter = make_generic_http_adapter = None
            make_huggingface_adapter = make_ollama_adapter = None

        # Each provider in its own try so one failure doesn't block the rest
        _http_adapters: list[tuple[str, Any]] = []
        if make_cohere_adapter is not None:
            cohere_key = os.environ.get("COHERE_API_KEY")
            if cohere_key and "cohere" not in self._registry:
                _http_adapters.append(("cohere", lambda: make_cohere_adapter(cohere_key)))

            ollama_base = os.environ.get("OLLAMA_BASE") or os.environ.get("OLLAMA_URL")
            if ollama_base and "ollama" not in self._registry:
                normalized_ollama_base = normalize_ollama_base_url(
                    ollama_base,
                    os.environ.get("OLLAMA_API_KEY", ""),
                )
                _http_adapters.append(
                    ("ollama", lambda: make_ollama_adapter(
                        normalized_ollama_base, os.environ.get("OLLAMA_API_KEY")
                    ))
                )

            hf_key = os.environ.get("HUGGINGFACE_API_KEY") or os.environ.get("HUGGINGFACE_TOKEN")
            if hf_key and "huggingface" not in self._registry:
                _http_adapters.append(
                    ("huggingface", lambda: make_huggingface_adapter(hf_key))
                )

            fireworks_base = os.environ.get("FIREWORKS_BASE") or os.environ.get(
                "FIREWORKS_API_BASE"
            )
            fireworks_key = os.environ.get("FIREWORKS_API_KEY")
            if fireworks_base and "fireworks" not in self._registry:
                _http_adapters.append(
                    ("fireworks", lambda: make_generic_http_adapter(fireworks_base, fireworks_key))
                )

            deepseek_base = os.environ.get("DEEPSEEK_BASE") or os.environ.get(
                "DEEPSEEK_API_BASE"
            )
            deepseek_key = os.environ.get("DEEPSEEK_API_KEY")
            if deepseek_base and "deepseek" not in self._registry:
                _http_adapters.append(
                    ("deepseek", lambda: make_generic_http_adapter(deepseek_base, deepseek_key))
                )

            together_base = os.environ.get("TOGETHER_BASE") or os.environ.get(
                "TOGETHER_API_BASE"
            )
            together_key = os.environ.get("TOGETHER_API_KEY")
            if together_base and "together" not in self._registry:
                _http_adapters.append(
                    ("together", lambda: make_generic_http_adapter(together_base, together_key))
                )

        for adapter_name, adapter_factory in _http_adapters:
            try:
                self.register_provider(adapter_name, adapter_factory())
            except Exception:
                logger.debug("Failed to register %s adapter", adapter_name, exc_info=True)

    # ------------------------------------------------------------------
    # DB-backed provider loading
    # ------------------------------------------------------------------

    async def load_db_providers(self, _reloading_names: set[str] | None = None) -> int:
        """Load ProviderRuntimeConfig-s from DB and register them as adapters.

        Returns the number of newly registered providers.
        """
        if self.db is None or self._config is None:
            return 0
        # Lazy import to avoid circular dependency
        from src.services.agent_provider_service import AgentProviderService as DbProviderService

        try:
            db_svc = DbProviderService(self.db, self._config)
            configs = await db_svc.load_provider_configs()
        except Exception:
            logger.debug("Failed to load db provider configs", exc_info=True)
            return 0

        added = 0
        reloading_names = _reloading_names or set()
        for cfg in configs:
            if not cfg.enabled:
                logger.debug("Skipping db provider %s: disabled", cfg.provider)
                continue
            if not self._has_valid_secrets(cfg):
                logger.warning("Skipping db provider %s: empty/invalid secrets", cfg.provider)
                continue
            try:
                adapter = self._build_adapter_for_config(cfg)
                if adapter is None:
                    logger.warning("Skipping db provider %s: no adapter mapping", cfg.provider)
                    continue
                name = cfg.provider
                # Register if new; overwrite if previously DB-sourced
                # (env-registered providers are never overwritten by DB loads).
                if name not in self._registry or name in reloading_names:
                    self.register_provider(name, adapter)
                    self._db_provider_names.add(name)
                    added += 1
            except Exception:
                logger.warning("Failed to register db provider %s", cfg.provider, exc_info=True)
        return added

    async def reload_db_providers(self) -> int:
        """Remove DB-sourced providers and reload from DB.

        Keeps existing providers live during the DB round-trip so
        has_providers() never returns False during a reload.
        """
        old_names = set(self._db_provider_names)
        self._db_provider_names.clear()
        added = await self.load_db_providers(_reloading_names=old_names)
        # Remove only names that were NOT re-registered
        for name in old_names - self._db_provider_names:
            self._registry.pop(name, None)
        return added

    def _has_valid_secrets(self, cfg: Any) -> bool:
        secrets = getattr(cfg, "secret_fields", None) or {}
        if any((v or "").strip() for v in secrets.values()):
            return True
        # Providers where ALL secret fields are optional (e.g. Ollama)
        # are valid even without secrets.
        from src.agent.provider_registry import provider_spec

        spec = provider_spec(getattr(cfg, "provider", ""))
        if spec is not None and spec.secret_fields and all(not f.required for f in spec.secret_fields):
            return True
        return False

    def _build_adapter_for_config(self, cfg: Any) -> Callable[..., Awaitable[str]] | None:
        """Map a ProviderRuntimeConfig to a provider adapter callable."""
        from src.services.provider_adapters import (
            make_anthropic_adapter,
            make_cohere_adapter,
            make_huggingface_adapter,
            make_ollama_adapter,
        )

        provider = cfg.provider
        api_key = (cfg.secret_fields.get("api_key", "") or "").strip()

        spec = provider_spec(provider)
        if spec is None:
            return None

        if provider == "zai":
            raw_base_url = cfg.plain_fields.get("base_url", "")
            if is_zai_legacy_anthropic_base_url(raw_base_url):
                logger.debug(
                    "Skipping db provider %s: Anthropic-compatible URL is not OpenAI-compatible",
                    provider,
                )
                return None
            base_url = normalize_zai_base_url(raw_base_url)
            return self._make_openai_compat_provider(base_url, api_key)

        # OpenAI-compatible providers
        if spec.openai_compatible and spec.default_base_url:
            base_url = (cfg.plain_fields.get("base_url", "") or "").strip()
            if not base_url:
                base_url = spec.default_base_url
            return self._make_openai_compat_provider(base_url, api_key)

        if provider == "cohere":
            base_url = (cfg.plain_fields.get("base_url", "") or "").strip()
            return make_cohere_adapter(api_key, base_url=base_url or None)

        if provider == "ollama":
            base_url = (cfg.plain_fields.get("base_url", "") or "").strip()
            base_url = normalize_ollama_base_url(base_url, api_key)
            return make_ollama_adapter(base_url=base_url or None, api_key=api_key or None)

        if provider == "huggingface":
            base_url = (cfg.plain_fields.get("base_url", "") or "").strip()
            return make_huggingface_adapter(api_key, base_url=base_url or None)

        if provider == "anthropic":
            base_url = (cfg.plain_fields.get("base_url", "") or "").strip()
            return make_anthropic_adapter(api_key, base_url=base_url or None)

        if provider == "google_genai":
            # Google GenAI also needs a different request schema.
            logger.debug("Skipping db provider %s: incompatible request schema", provider)
            return None

        return None

    @staticmethod
    def _make_openai_compat_provider(
        base_url: str, api_key: str
    ) -> Callable[..., Awaitable[str]]:
        """Create an OpenAI-compatible chat completion provider."""
        endpoint = f"{base_url.rstrip('/')}/chat/completions"

        async def _provider(
            prompt: str = "",
            model: Optional[str] = None,
            max_tokens: int = 256,
            temperature: float = 0.0,
            stream: bool = False,
            **kwargs: Any,
        ) -> str:
            headers = {"Content-Type": "application/json"}
            if api_key:
                headers["Authorization"] = f"Bearer {api_key}"
            payload = {
                "model": model or "gpt-3.5-turbo",
                "messages": [{"role": "user", "content": prompt}],
                "max_tokens": int(max_tokens or 256),
                "temperature": float(temperature or 0.0),
            }
            timeout = aiohttp.ClientTimeout(total=60)
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    endpoint, json=payload, headers=headers, timeout=timeout
                ) as resp:
                    text = await resp.text()
                    if resp.status != 200:
                        raise RuntimeError(f"Provider error {resp.status}: {text}")
                    data = await resp.json()
                    try:
                        return data["choices"][0]["message"]["content"]
                    except Exception:
                        return str(data)

        return _provider

    def has_providers(self) -> bool:
        """Return True if any real (non-default) provider is registered."""
        return any(name != "default" for name in self._registry)

    async def get_provider_status_list(self) -> list[dict[str, str]]:
        """Return per-provider diagnostic status for UI display.

        Each entry: {"provider": str, "status": str, "reason": str}.
        Statuses: active, disabled, invalid_secrets, no_adapter.
        """
        if self.db is None or self._config is None:
            return []
        from src.services.agent_provider_service import AgentProviderService as DbProviderService

        try:
            db_svc = DbProviderService(self.db, self._config)
            configs = await db_svc.load_provider_configs()
        except Exception:
            logger.warning("Failed to load provider statuses from DB", exc_info=True)
            return []

        statuses: list[dict[str, str]] = []
        for cfg in configs:
            if cfg.provider in self._registry and cfg.provider != "default":
                statuses.append({"provider": cfg.provider, "status": "active", "reason": ""})
            elif not cfg.enabled:
                statuses.append({"provider": cfg.provider, "status": "disabled", "reason": "Провайдер отключён."})
            elif not self._has_valid_secrets(cfg):
                reason = cfg.last_validation_error or "API-ключ или секрет пуст."
                statuses.append({"provider": cfg.provider, "status": "invalid_secrets", "reason": reason})
            else:
                adapter = self._build_adapter_for_config(cfg)
                if adapter is None:
                    reason = f"Адаптер для {cfg.provider} ещё не реализован."
                    statuses.append({"provider": cfg.provider, "status": "no_adapter", "reason": reason})
                else:
                    statuses.append({
                        "provider": cfg.provider,
                        "status": "unknown_skip",
                        "reason": "Провайдер пропущен по неизвестной причине.",
                    })
        return statuses

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
            # Return the first non-default registered provider; fall back to stub.
            real = next((fn for n, fn in self._registry.items() if n != "default"), None)
            return real if real is not None else self._registry["default"]
        if name in self._registry:
            return self._registry[name]
        lower = name.lower() if isinstance(name, str) else ""
        if "openai" in self._registry and ("gpt" in lower or lower.startswith("gpt")):
            base = self._registry["openai"]

            async def _call(prompt: str = "", model: Optional[str] = None, **kwargs: Any) -> str:
                # force the model to the requested name
                return await base(prompt=prompt, model=name, **kwargs)

            return _call
        # fallback — provider not registered, likely a missing API key
        logger.warning("Provider %r not registered, falling back to stub default", name)
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
            timeout = aiohttp.ClientTimeout(
                total=int(os.environ.get("OPENAI_TIMEOUT_SECONDS", "60"))
            )
            async with aiohttp.ClientSession() as session:
                async with session.post(
                    url, json=payload, headers=headers, timeout=timeout
                ) as resp:
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
