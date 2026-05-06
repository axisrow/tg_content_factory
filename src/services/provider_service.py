from __future__ import annotations

import logging
import os
from typing import Any, Awaitable, Callable, Dict, Optional

from src.agent.provider_registry import (
    ProviderRuntimeConfig,
    is_zai_legacy_anthropic_base_url,
    normalize_ollama_base_url,
    normalize_zai_base_url,
    provider_spec,
)

logger = logging.getLogger(__name__)

async def build_provider_service(
    db: object | None = None,
    config: object | None = None,
) -> "RuntimeProviderRegistry":
    """Create RuntimeProviderRegistry and eagerly load DB-backed providers when possible."""
    svc = RuntimeProviderRegistry(db, config)
    if db is not None and config is not None:
        await svc.load_db_providers()
    return svc


class RuntimeProviderRegistry:
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
        # Optional OpenAI provider. Runtime calls go through LangChain so the
        # app uses the same model integration path as DeepAgents.
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
                    "zai",
                    self._make_openai_compat_provider(
                        zai_base,
                        zai_key,
                        provider_name="zai",
                        default_model="glm-5-turbo",
                    ),
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

        # Register LangChain-backed adapters when env vars are present. Each
        # provider is isolated so one bad env config does not block the rest.
        env_adapters: list[tuple[str, Callable[[], Callable[..., Awaitable[str]]]]] = []
        cohere_key = os.environ.get("COHERE_API_KEY")
        if cohere_key and "cohere" not in self._registry:
            env_adapters.append(
                (
                    "cohere",
                    lambda: self._make_provider_for_runtime_config(
                        self._env_runtime_config("cohere", secret_fields={"api_key": cohere_key})
                    ),
                )
            )

        ollama_base = os.environ.get("OLLAMA_BASE") or os.environ.get("OLLAMA_URL")
        ollama_key = os.environ.get("OLLAMA_API_KEY", "")
        if (ollama_base or ollama_key) and "ollama" not in self._registry:
            env_adapters.append(
                (
                    "ollama",
                    lambda: self._make_provider_for_runtime_config(
                        self._env_runtime_config(
                            "ollama",
                            plain_fields={"base_url": normalize_ollama_base_url(ollama_base or "", ollama_key)},
                            secret_fields={"api_key": ollama_key},
                        )
                    ),
                )
            )

        hf_key = os.environ.get("HUGGINGFACE_API_KEY") or os.environ.get("HUGGINGFACE_TOKEN")
        if hf_key and "huggingface" not in self._registry:
            env_adapters.append(
                (
                    "huggingface",
                    lambda: self._make_provider_for_runtime_config(
                        self._env_runtime_config("huggingface", secret_fields={"api_key": hf_key})
                    ),
                )
            )

        openai_compatible_env = (
            (
                "fireworks",
                os.environ.get("FIREWORKS_BASE") or os.environ.get("FIREWORKS_API_BASE"),
                os.environ.get("FIREWORKS_API_KEY"),
            ),
            (
                "deepseek",
                os.environ.get("DEEPSEEK_BASE") or os.environ.get("DEEPSEEK_API_BASE"),
                os.environ.get("DEEPSEEK_API_KEY"),
            ),
            (
                "together",
                os.environ.get("TOGETHER_BASE") or os.environ.get("TOGETHER_API_BASE"),
                os.environ.get("TOGETHER_API_KEY"),
            ),
        )
        for provider_name, base_env, key_env in openai_compatible_env:
            if (base_env or key_env) and provider_name not in self._registry:
                def _factory(
                    provider_name: str = provider_name,
                    base_env: str | None = base_env,
                    key_env: str | None = key_env,
                ) -> Callable[..., Awaitable[str]]:
                    return self._make_provider_for_runtime_config(
                        self._env_runtime_config(
                            provider_name,
                            plain_fields={"base_url": base_env or ""},
                            secret_fields={"api_key": key_env or ""},
                        )
                    )

                env_adapters.append(
                    (
                        provider_name,
                        _factory,
                    )
                )

        for adapter_name, adapter_factory in env_adapters:
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
        from src.services.agent_provider_service import ProviderConfigService

        try:
            db_svc = ProviderConfigService(self.db, self._config)
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
        provider = cfg.provider

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

        return self._make_provider_for_runtime_config(cfg)

    def _env_runtime_config(
        self,
        provider: str,
        *,
        plain_fields: dict[str, str] | None = None,
        secret_fields: dict[str, str] | None = None,
        selected_model: str | None = None,
    ) -> ProviderRuntimeConfig:
        spec = provider_spec(provider)
        default_model = selected_model or (spec.static_models[0] if spec and spec.static_models else "")
        return ProviderRuntimeConfig(
            provider=provider,
            enabled=True,
            priority=0,
            selected_model=default_model,
            plain_fields=plain_fields or {},
            secret_fields=secret_fields or {},
        )

    @staticmethod
    def _runtime_options_for_config(cfg: Any) -> tuple[str, dict[str, object]]:
        spec = provider_spec(cfg.provider)
        if spec is None:
            raise RuntimeError(f"Unknown provider: {cfg.provider}")

        model_provider = spec.resolved_runtime_provider
        extra: dict[str, object] = {
            key: value for key, value in cfg.plain_fields.items() if value.strip()
        }
        if cfg.provider == "ollama":
            api_key = cfg.secret_fields.get("api_key", "").strip()
            extra["base_url"] = normalize_ollama_base_url(
                str(extra.get("base_url", "")),
                api_key,
            )
            if api_key:
                extra["client_kwargs"] = {"headers": {"Authorization": f"Bearer {api_key}"}}
            return model_provider, extra

        extra.update({key: value for key, value in cfg.secret_fields.items() if value.strip()})
        if cfg.provider == "zai":
            raw_base_url = cfg.plain_fields.get("base_url", "")
            if is_zai_legacy_anthropic_base_url(raw_base_url):
                raise RuntimeError(
                    "This URL is the Z.AI Anthropic-compatible proxy. Configure the "
                    "anthropic provider with this URL instead, or use the OpenAI-compatible "
                    "endpoint."
                )
            extra["base_url"] = normalize_zai_base_url(raw_base_url)
        return model_provider, extra

    @staticmethod
    def _response_text(response: Any) -> str:
        if response is None:
            return ""
        if isinstance(response, str):
            return response
        text_attr = getattr(response, "text", None)
        if callable(text_attr):
            try:
                text = text_attr()
                if isinstance(text, str):
                    return text
            except TypeError:
                pass
        content = getattr(response, "content", None)
        if isinstance(content, str):
            return content
        if isinstance(content, list):
            parts: list[str] = []
            for item in content:
                if isinstance(item, str):
                    parts.append(item)
                elif isinstance(item, dict):
                    value = item.get("text") or item.get("content")
                    if value is not None:
                        parts.append(str(value))
                else:
                    parts.append(str(item))
            return "\n".join(part for part in parts if part)
        return str(response)

    @staticmethod
    def _strip_matching_provider_prefix(provider: str, model: str) -> str:
        configured_provider, sep, bare_model_name = model.partition(":")
        if sep and configured_provider == provider and bare_model_name:
            return bare_model_name
        return model

    def _make_provider_for_runtime_config(self, cfg: Any) -> Callable[..., Awaitable[str]]:
        provider_name = str(getattr(cfg, "provider", "") or "")
        raw_default_model = getattr(cfg, "selected_model", "")
        default_model = raw_default_model.strip() if isinstance(raw_default_model, str) else ""
        model_provider, base_extra = self._runtime_options_for_config(cfg)

        async def _provider(
            prompt: str = "",
            model: Optional[str] = None,
            max_tokens: int = 256,
            temperature: float = 0.0,
            stream: bool = False,
            **kwargs: Any,
        ) -> str:
            del stream
            from langchain.chat_models import init_chat_model

            selected_model = str(model or default_model or "").strip()
            if not selected_model:
                spec = provider_spec(provider_name)
                selected_model = spec.static_models[0] if spec and spec.static_models else ""
            resolved_model = self._strip_matching_provider_prefix(provider_name, selected_model)
            extra = dict(base_extra)
            extra.update({key: value for key, value in kwargs.items() if value is not None})
            if max_tokens is not None:
                extra["max_tokens"] = int(max_tokens)
            if temperature is not None:
                extra["temperature"] = float(temperature)
            chat_model = init_chat_model(
                model=resolved_model,
                model_provider=model_provider,
                **extra,
            )
            response = await chat_model.ainvoke(prompt)
            return self._response_text(response)

        return _provider

    def _make_openai_compat_provider(
        self,
        base_url: str,
        api_key: str,
        *,
        provider_name: str = "openai",
        default_model: str = "gpt-3.5-turbo",
    ) -> Callable[..., Awaitable[str]]:
        """Create an OpenAI-compatible chat provider through LangChain."""
        cfg = ProviderRuntimeConfig(
            provider=provider_name,
            enabled=True,
            priority=0,
            selected_model=default_model,
            plain_fields={"base_url": base_url},
            secret_fields={"api_key": api_key},
        )
        return self._make_provider_for_runtime_config(cfg)

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
        from src.services.agent_provider_service import ProviderConfigService

        try:
            db_svc = ProviderConfigService(self.db, self._config)
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
        return self._make_openai_compat_provider(
            os.environ.get("OPENAI_API_BASE", "https://api.openai.com/v1"),
            api_key,
            provider_name="openai",
            default_model=os.environ.get("OPENAI_DEFAULT_MODEL", "gpt-3.5-turbo"),
        )

    async def _default_provider(self, **kwargs: Any) -> str:
        prompt = kwargs.get("prompt", "") or ""
        # Minimal safe stub provider for local testing / fallback
        return "DRAFT (default provider): " + (prompt[:400])
