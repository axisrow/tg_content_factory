from __future__ import annotations

import asyncio
import logging
import os
import time
from collections.abc import Awaitable, Callable, Coroutine
from datetime import UTC, datetime
from typing import Any, TypeVar, cast

from src.agent.backends._stream import _embed_history_in_prompt, _ToolTracker
from src.agent.prompt_template import DEFAULT_AGENT_PROMPT_TEMPLATE
from src.agent.provider_registry import ProviderRuntimeConfig
from src.agent.runtime_context import AgentRuntimeContext
from src.agent.zai_errors import format_provider_error
from src.config import AppConfig
from src.database import Database
from src.services.agent_provider_service import (
    ProviderConfigService,
    ProviderModelCacheEntry,
    ProviderModelCompatibilityRecord,
)
from src.utils.json import safe_json_dumps

logger = logging.getLogger(__name__)

_OLLAMA_NO_NATIVE_FC: frozenset[str] = frozenset({
    "kimi-k2.5",
    "kimi-k1.5",
})

_DEEPAGENTS_PROBE_PROMPT = (
    "Compatibility probe. You must use the tool that lists active Telegram "
    "channels before answering. "
    "Do not answer from memory. After completing the tool call, reply with exactly PROBE_OK."
)
_DEEPAGENTS_PROBE_TIMEOUT_SECONDS = 45.0
_ToolResult = TypeVar("_ToolResult")


class DeepagentsBackend:
    def __init__(self, db: Database, config: AppConfig, client_pool=None, scheduler_manager=None) -> None:
        self._db = db
        self._config = config
        self._client_pool = client_pool
        self._scheduler_manager = scheduler_manager
        self._runtime_context = AgentRuntimeContext.build(
            db=db,
            config=config,
            client_pool=client_pool,
            scheduler_manager=scheduler_manager,
        )
        self._provider_service = ProviderConfigService(db, config)
        self._cached_db_configs: list[ProviderRuntimeConfig] = []
        self._cached_model_cache: dict[str, ProviderModelCacheEntry] = {}
        self._last_used_provider: str = ""
        self._last_used_model: str = ""
        self._preflight_available: bool | None = None
        self._init_error: str | None = None
        self._init_attempted_model: str | None = None

    @property
    def legacy_fallback_model(self) -> str:
        return (
            self._config.agent.fallback_model.strip()
            or os.environ.get("AGENT_FALLBACK_MODEL", "").strip()
        )

    @property
    def fallback_model(self) -> str:
        if self._last_used_model:
            return self._last_used_model
        first = next((cfg for cfg in self._cached_db_configs if cfg.enabled), None)
        if first is not None:
            return first.selected_model
        return self.legacy_fallback_model

    @property
    def fallback_provider(self) -> str:
        if self._last_used_provider:
            return self._last_used_provider
        first = next((cfg for cfg in self._cached_db_configs if cfg.enabled), None)
        if first is not None:
            return first.provider
        return self._provider_from_model(self.legacy_fallback_model) or ""

    @property
    def configured(self) -> bool:
        return bool(self._enabled_db_configs(include_invalid=True)) or bool(
            self.legacy_fallback_model
        )

    @property
    def available(self) -> bool:
        if self._preflight_available is not None:
            return self._preflight_available
        # If a legacy fallback model is configured via ENV or config and no DB provider configs
        # are present, treat deepagents as available when it doesn't require extra credentials
        # (e.g., non-anthropic providers) or when an explicit fallback API key is provided.
        try:
            legacy_model = self.legacy_fallback_model
            # Only let the legacy fallback short-circuit the verdict when there is
            # no usable non-legacy DB provider config; otherwise a legacy
            # anthropic fallback without AGENT_FALLBACK_API_KEY would falsely
            # declare deepagents unavailable despite a working DB config (#837/5).
            if legacy_model and not self.has_usable_db_provider_configs:
                legacy_provider = self._provider_from_model(legacy_model)
                if legacy_provider is None:
                    return False
                if legacy_provider == "anthropic":
                    return bool(self._fallback_api_key())
                return True
        except Exception:
            pass
        return any(not self._validation_error(cfg) for cfg in self._candidate_configs_from_cache())

    @property
    def has_usable_db_provider_configs(self) -> bool:
        """True if at least one non-legacy, validation-passing provider config exists in the DB.

        Requires refresh_settings_cache() to have been called first.
        """
        return any(
            not self._is_legacy_candidate(cfg) and not self._validation_error(cfg)
            for cfg in self._candidate_configs_from_cache()
        )

    @property
    def init_error(self) -> str | None:
        return self._init_error

    @property
    def preflight_available(self) -> bool | None:
        return self._preflight_available

    def _fallback_api_key(self) -> str:
        return (
            self._config.agent.fallback_api_key.strip()
            or os.environ.get("AGENT_FALLBACK_API_KEY", "").strip()
        )

    def _provider_from_model(self, model_name: str) -> str | None:
        if ":" not in model_name:
            return None
        provider, _, _model = model_name.partition(":")
        return provider or None

    async def refresh_settings_cache(self) -> None:
        configs = await self._provider_service.load_provider_configs()
        model_cache = await self._provider_service.load_model_cache()
        configs.sort(key=lambda cfg: cfg.priority)
        if configs != self._cached_db_configs or model_cache != self._cached_model_cache:
            self._preflight_available = None
            self._init_error = None
            self._last_used_provider = ""
            self._last_used_model = ""
        self._cached_db_configs = configs
        self._cached_model_cache = model_cache
        if not self._cached_db_configs:
            self._last_used_provider = ""
            self._last_used_model = ""

    def _enabled_db_configs(self, *, include_invalid: bool = False) -> list[ProviderRuntimeConfig]:
        configs = [cfg for cfg in self._cached_db_configs if cfg.enabled]
        if include_invalid:
            return configs
        return [cfg for cfg in configs if not self._provider_service.validate_provider_config(cfg)]

    def _legacy_fallback_config(self) -> ProviderRuntimeConfig | None:
        model_name = self.legacy_fallback_model
        if not model_name:
            return None
        provider = self._provider_from_model(model_name)
        if not provider:
            return None
        secret_fields = {}
        api_key = self._fallback_api_key()
        if api_key:
            secret_fields["api_key"] = api_key
        return ProviderRuntimeConfig(
            provider=provider,
            enabled=True,
            priority=0,
            selected_model=model_name,
            plain_fields={},
            secret_fields=secret_fields,
        )

    def _candidate_configs_from_cache(self) -> list[ProviderRuntimeConfig]:
        db_configs = self._enabled_db_configs()
        legacy_cfg = self._legacy_fallback_config()
        if legacy_cfg is None:
            return db_configs
        if any(cfg.model_name == legacy_cfg.model_name for cfg in db_configs):
            return db_configs
        return [*db_configs, legacy_cfg]

    def _is_legacy_candidate(self, cfg: ProviderRuntimeConfig) -> bool:
        legacy_provider = self._provider_from_model(self.legacy_fallback_model)
        return (
            bool(self.legacy_fallback_model)
            and cfg.provider == (legacy_provider or "")
            and cfg.selected_model == self.legacy_fallback_model
        )

    def _legacy_validation_error(self, cfg: ProviderRuntimeConfig) -> str:
        if not cfg.selected_model:
            return "Deepagents provider model is not configured."
        if cfg.provider == "anthropic" and not cfg.secret_fields.get("api_key", "").strip():
            return (
                "Для anthropic fallback требуется AGENT_FALLBACK_API_KEY, "
                "иначе deepagents переиспользует Claude SDK credentials."
            )
        return ""

    def _validation_error(self, cfg: ProviderRuntimeConfig) -> str:
        if self._is_legacy_candidate(cfg):
            return self._legacy_validation_error(cfg)
        validation_error = (
            cfg.last_validation_error or self._provider_service.validate_provider_config(cfg)
        )
        if validation_error:
            return validation_error
        cache_entry = self._cached_model_cache.get(cfg.provider)
        return self._provider_service.compatibility_error_for_config(cfg, cache_entry)

    async def _candidate_configs(self) -> list[ProviderRuntimeConfig]:
        await self.refresh_settings_cache()
        return self._candidate_configs_from_cache()

    def _run_db_tool_sync(
        self,
        tool_name: str,
        operation: Callable[[], Awaitable[_ToolResult]],
    ) -> _ToolResult:
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            return asyncio.run(cast(Coroutine[Any, Any, _ToolResult], operation()))
        raise RuntimeError(
            f"Deepagents tool '{tool_name}' cannot run inside an active event loop: {loop}"
        )

    def _default_tools(
        self,
        permissions: dict[str, bool] | None = None,
        *,
        access_policy: dict | None = None,
        gate_active: bool = False,
    ) -> list[Callable]:
        """Return the tool set for deepagents backend, filtered by access policy."""
        from src.agent.tools.deepagents_sync import build_deepagents_tools

        all_tools = build_deepagents_tools(
            self._db,
            client_pool=self._client_pool,
            config=self._config,
            runtime_context=self._runtime_context,
        )
        if access_policy is not None:
            from src.agent.tools.permissions import is_tool_visible_for_llm

            return [
                tool
                for tool in all_tools
                if is_tool_visible_for_llm(tool.__name__, access_policy, gate_active=gate_active)
            ]
        if permissions is None:
            return all_tools
        return [t for t in all_tools if permissions.get(t.__name__, True)]

    def _search_messages_tool(self, query_text: str) -> str:
        """Search messages — used by probe. Delegates to sync tools."""
        tools = self._default_tools()
        search_fn = next((t for t in tools if t.__name__ == "search_messages"), None)
        if search_fn:
            return search_fn(query_text)
        return "Поиск недоступен."

    def _get_channels_tool(self) -> str:
        """List channels — used by probe. Delegates to sync tools."""
        tools = self._default_tools()
        channels_fn = next((t for t in tools if t.__name__ == "list_channels"), None)
        if channels_fn:
            return channels_fn()
        return "Каналы недоступны."

    def _build_agent(
        self,
        cfg: ProviderRuntimeConfig,
        *,
        tools: list[Callable] | None = None,
        permissions: dict[str, bool] | None = None,
        access_policy: dict | None = None,
        gate_active: bool = False,
        record_last_used: bool = True,
        system_prompt: str = DEFAULT_AGENT_PROMPT_TEMPLATE,
    ) -> Any:
        configured_model_name = cfg.selected_model.strip()
        if not configured_model_name:
            raise RuntimeError("Deepagents provider model is not configured.")
        provider = cfg.provider
        resolved_model_name = configured_model_name
        configured_provider, has_provider_prefix, bare_model_name = configured_model_name.partition(
            ":"
        )
        if has_provider_prefix and configured_provider == provider and bare_model_name:
            resolved_model_name = bare_model_name
        if provider == "anthropic" and not cfg.secret_fields.get("api_key", "").strip():
            self._init_error = (
                "Для anthropic fallback требуется AGENT_FALLBACK_API_KEY, "
                "иначе deepagents переиспользует Claude SDK credentials."
            )
            raise RuntimeError(self._init_error)
        try:
            model_provider, extra = self._provider_service.deepagents_runtime_options(cfg)
        except RuntimeError as exc:
            self._init_error = str(exc)
            raise
        self._init_attempted_model = cfg.model_name

        # ReAct fallback for Ollama models without native function calling
        if provider == "ollama":
            bare_model = resolved_model_name.split(":")[0]
            if bare_model in _OLLAMA_NO_NATIVE_FC:
                logger.debug(
                    "Model %r does not support native FC via Ollama; using ReAct fallback",
                    resolved_model_name,
                )
                from src.agent.react_agent import OllamaReActAgent
                agent: Any = OllamaReActAgent(
                    base_url=str(extra.get("base_url", "http://localhost:11434")),
                    model=resolved_model_name,
                    tools=tools or self._default_tools(
                        permissions=permissions,
                        access_policy=access_policy,
                        gate_active=gate_active,
                    ),
                    system_prompt=system_prompt,
                    api_key=cfg.secret_fields.get("api_key", ""),
                )
                if record_last_used:
                    self._last_used_provider = provider
                    self._last_used_model = configured_model_name
                return agent

        try:
            from deepagents import create_deep_agent
        except ImportError as exc:
            self._init_error = f"Не удалось импортировать deepagents: {exc}"
            raise RuntimeError(self._init_error) from exc

        try:
            from langchain.chat_models import init_chat_model

            model_kwargs = cast(dict[str, Any], extra)
            model = init_chat_model(
                model=resolved_model_name,
                model_provider=model_provider,
                **model_kwargs,
            )
        except ImportError as exc:
            package = (
                f"langchain-{model_provider.replace('_', '-')}"
                if model_provider
                else "langchain provider"
            )
            self._init_error = (
                f"Не установлена интеграция для provider '{provider}'. "
                f"Установите пакет вроде '{package}'. Детали: {exc}"
            )
            raise RuntimeError(self._init_error) from exc

        try:
            agent = create_deep_agent(
                model=model,
                tools=tools or self._default_tools(
                    permissions=permissions,
                    access_policy=access_policy,
                    gate_active=gate_active,
                ),
                system_prompt=system_prompt,
            )
            if record_last_used:
                self._last_used_provider = provider
                self._last_used_model = configured_model_name
            return agent
        except ImportError as exc:
            self._init_error = f"Ошибка импорта при создании агента: {exc}"
            raise RuntimeError(self._init_error) from exc
        except ValueError as exc:
            self._init_error = f"Некорректная конфигурация fallback модели: {exc}"
            raise RuntimeError(self._init_error) from exc
        except Exception as exc:
            self._init_error = f"Не удалось инициализировать deepagents: {exc}"
            raise RuntimeError(self._init_error) from exc

    def initialize(self) -> None:
        if not self.configured:
            logger.info("Deepagents backend disabled: no fallback model configured")
            return
        candidates = self._candidate_configs_from_cache()
        if not candidates:
            if self.legacy_fallback_model:
                self._preflight_available = False
                self._init_error = (
                    "AGENT_FALLBACK_MODEL должен быть в формате provider:model для deepagents."
                )
                raise RuntimeError(self._init_error)
            errors: list[str] = []
            for cfg in self._enabled_db_configs(include_invalid=True):
                validation_error = self._validation_error(cfg)
                if validation_error:
                    errors.append(f"{cfg.provider}: {validation_error}")
            self._preflight_available = False
            self._init_error = (
                "; ".join(errors) if errors else "Deepagents providers are not configured."
            )
            raise RuntimeError(self._init_error)

        # If the only candidates are legacy fallback configs, avoid trying to build an agent
        # during preflight as optional provider integration packages may be absent in the test
        # environment. Treat legacy fallback as available when validation passes (e.g.,
        # anthopic fallback requires explicit fallback API key).
        if all(self._is_legacy_candidate(cfg) for cfg in candidates):
            legacy_errors: list[str] = []
            for cfg in candidates:
                validation_error = (
                    self._legacy_validation_error(cfg)
                    if self._is_legacy_candidate(cfg)
                    else self._validation_error(cfg)
                )
                if validation_error:
                    legacy_errors.append(f"{cfg.provider}: {validation_error}")
                    continue
                self._preflight_available = True
                self._init_error = None
                logger.info(
                    (
                        "Deepagents backend preflight: legacy fallback configured "
                        "for provider %s and model %s"
                    ),
                    cfg.provider,
                    cfg.selected_model,
                )
                return
            self._preflight_available = False
            self._init_error = (
                "; ".join(legacy_errors) if legacy_errors else "Deepagents providers are not configured."
            )
            raise RuntimeError(self._init_error)

        candidate_errors: list[str] = []
        for cfg in candidates:
            validation_error = self._validation_error(cfg)
            if validation_error:
                candidate_errors.append(f"{cfg.provider}: {validation_error}")
                continue
            try:
                self._build_agent(cfg)
                self._preflight_available = True
                self._init_error = None
                logger.info(
                    "Deepagents backend initialized with provider %s and model %s",
                    cfg.provider,
                    cfg.selected_model,
                )
                return
            except Exception as exc:
                candidate_errors.append(format_provider_error(cfg.provider, exc))
                logger.warning("Deepagents provider preflight failed (%s): %s", cfg.provider, exc)
        self._preflight_available = False
        self._init_error = (
            "; ".join(candidate_errors) if candidate_errors else "Deepagents providers are not configured."
        )
        raise RuntimeError(self._init_error)

    def _extract_result_text(self, result: object) -> str:
        if isinstance(result, dict):
            messages = result.get("messages") or []
            if messages:
                last_message = messages[-1]
                content = getattr(last_message, "content", None)
                if isinstance(content, str):
                    return content
                if isinstance(content, list):
                    return "\n".join(
                        block.get("text", "") if isinstance(block, dict) else str(block)
                        for block in content
                    ).strip()
            return str(result)
        return str(result)

    def _run_agent(
        self,
        prompt: str,
        cfg: ProviderRuntimeConfig,
        *,
        history_msgs: list[dict] | None = None,
        system_prompt: str = DEFAULT_AGENT_PROMPT_TEMPLATE,
        permissions: dict[str, bool] | None = None,
        access_policy: dict | None = None,
        gate_active: bool = False,
    ) -> str:
        # Append available tools list to system prompt so models without
        # native function calling still know which tools exist.
        filtered_tools = self._default_tools(
            permissions=permissions,
            access_policy=access_policy,
            gate_active=gate_active,
        )
        tool_names = [t.__name__ for t in filtered_tools]
        if tool_names:
            system_prompt += (
                "\n\nУ тебя есть доступ к следующим инструментам (tools). "
                "Используй их для выполнения задач:\n" + "\n".join(f"- {n}" for n in tool_names)
            )
        if "get_account_info" in tool_names:
            system_prompt += (
                "\n\nДля диагностики Telegram-аккаунтов, подключения номера или reconnect "
                "сначала вызывай get_account_info. Не утверждай, что нужен SMS/2FA или что "
                "аккаунт выключен/not connected, если live tool этого не подтвердил."
            )
        agent = self._build_agent(
            cfg,
            system_prompt=system_prompt,
            permissions=permissions,
            access_policy=access_policy,
            gate_active=gate_active,
        )
        logger.info(
            "Deepagents _run_agent: provider=%s, model=%s, tools=%d, agent_type=%s",
            cfg.provider, cfg.selected_model, len(filtered_tools), type(agent).__name__,
        )
        # Different agent frameworks have different history handling:
        # - `.run(prompt_str)` agents receive history embedded as XML tags in a single string
        # - `.invoke({"messages": [...]})` agents receive history as a structured message list
        if hasattr(agent, "run"):
            run_prompt = _embed_history_in_prompt(history_msgs, prompt) if history_msgs else prompt
            result = agent.run(run_prompt)
        else:
            messages: list[dict] = []
            for msg in history_msgs or []:
                messages.append({"role": msg["role"], "content": msg["content"]})
            messages.append({"role": "user", "content": prompt})
            result = agent.invoke({"messages": messages})
        extracted = self._extract_result_text(result)
        logger.info(
            "Deepagents _run_agent result: type=%s, len=%d, preview=%r",
            type(result).__name__, len(extracted), extracted[:200] if extracted else "(empty)",
        )
        return extracted

    def _probe_tools(self) -> tuple[list[Callable], dict[str, int]]:
        calls = {"search_messages": 0, "get_channels": 0}

        def _search_messages_tool(query_text: str) -> str:
            """Search recent Telegram messages by free-text query and return short previews."""
            calls["search_messages"] += 1
            return self._search_messages_tool(query_text)

        def _get_channels_tool() -> str:
            """List active Telegram channels that are available to the agent."""
            calls["get_channels"] += 1
            return self._get_channels_tool()

        return [_search_messages_tool, _get_channels_tool], calls

    def _run_probe(self, cfg: ProviderRuntimeConfig) -> None:
        previous_state = (
            self._last_used_provider,
            self._last_used_model,
            self._init_error,
            self._init_attempted_model,
        )
        try:
            tools, calls = self._probe_tools()
            agent = self._build_agent(cfg, tools=tools, record_last_used=False)
            if hasattr(agent, "run"):
                result = agent.run(_DEEPAGENTS_PROBE_PROMPT)
            else:
                result = agent.invoke(
                    {"messages": [{"role": "user", "content": _DEEPAGENTS_PROBE_PROMPT}]}
                )
            _ = self._extract_result_text(result)
            if calls["get_channels"] < 1:
                raise RuntimeError(
                    "Compatibility probe finished without a required get_channels tool call."
                )
        finally:
            (
                self._last_used_provider,
                self._last_used_model,
                self._init_error,
                self._init_attempted_model,
            ) = previous_state

    def _classify_probe_failure(self, exc: Exception) -> tuple[str, str]:
        text = str(exc).strip() or exc.__class__.__name__
        lowered = text.lower()
        if any(
            marker in lowered
            for marker in (
                "timed out",
                "timeout",
                "rate limit",
                "429",
                "service unavailable",
                "connection reset",
                "connection refused",
                "network",
                "temporarily unavailable",
                "bad gateway",
                "gateway timeout",
                "unauthorized",
                "forbidden",
                "authentication",
                "api key",
                "403",
                "401",
            )
        ):
            return "unknown", text
        return "unsupported", text

    async def probe_config(
        self,
        cfg: ProviderRuntimeConfig,
        *,
        probe_kind: str = "auto-select",
    ) -> ProviderModelCompatibilityRecord:
        tested_at = datetime.now(UTC).isoformat()
        fingerprint = self._provider_service.config_fingerprint(cfg)
        logger.info(
            "Deepagents compatibility probe started: provider=%s model=%s kind=%s",
            cfg.provider,
            cfg.selected_model or "<empty>",
            probe_kind,
        )
        try:
            await asyncio.wait_for(
                asyncio.to_thread(self._run_probe, cfg),
                timeout=_DEEPAGENTS_PROBE_TIMEOUT_SECONDS,
            )
            result = ProviderModelCompatibilityRecord(
                model=cfg.selected_model,
                status="supported",
                tested_at=tested_at,
                config_fingerprint=fingerprint,
                probe_kind=probe_kind,
            )
            logger.info(
                "Deepagents compatibility probe finished: provider=%s model=%s status=%s kind=%s",
                cfg.provider,
                cfg.selected_model or "<empty>",
                result.status,
                probe_kind,
            )
            return result
        except asyncio.TimeoutError:
            result = ProviderModelCompatibilityRecord(
                model=cfg.selected_model,
                status="unknown",
                reason="Compatibility probe timed out before the tool loop completed.",
                tested_at=tested_at,
                config_fingerprint=fingerprint,
                probe_kind=probe_kind,
            )
            logger.info(
                (
                    "Deepagents compatibility probe finished: provider=%s "
                    "model=%s status=%s kind=%s reason=%s"
                ),
                cfg.provider,
                cfg.selected_model or "<empty>",
                result.status,
                probe_kind,
                result.reason,
            )
            return result
        except Exception as exc:
            status, reason = self._classify_probe_failure(exc)
            result = ProviderModelCompatibilityRecord(
                model=cfg.selected_model,
                status=status,
                reason=reason,
                tested_at=tested_at,
                config_fingerprint=fingerprint,
                probe_kind=probe_kind,
            )
            logger.info(
                (
                    "Deepagents compatibility probe finished: provider=%s "
                    "model=%s status=%s kind=%s reason=%s"
                ),
                cfg.provider,
                cfg.selected_model or "<empty>",
                result.status,
                probe_kind,
                result.reason,
            )
            return result

    async def chat_stream(
        self,
        *,
        thread_id: int,
        prompt: str,
        system_prompt: str,
        stats: dict,
        model: str | None,
        queue: asyncio.Queue[str | None],
        history_msgs: list[dict] | None = None,
        session_id: str = "web",
    ) -> None:
        del thread_id, stats, model, session_id

        from src.agent.permission_gate import get_gate, get_request_context
        from src.agent.tools.permissions import load_tool_access_policy, visible_tools_for_llm

        access_policy = await load_tool_access_policy(self._db)
        gate_active = get_gate() is not None and get_request_context() is not None
        visible_tools = visible_tools_for_llm(
            [tool.__name__ for tool in self._default_tools()],
            access_policy,
            gate_active=gate_active,
        )
        self._runtime_context.bind_owner_loop(asyncio.get_running_loop())
        logger.info(
            "Deepagents tool permissions: %d/%d visible (gate_active=%s)",
            len(visible_tools), len(access_policy), gate_active,
        )

        tracker = _ToolTracker(queue=queue)
        errors: list[str] = []
        attempt = 0
        for cfg in await self._candidate_configs():
            validation_error = self._validation_error(cfg)
            if validation_error:
                errors.append(f"{cfg.provider}: {validation_error}")
                continue
            if attempt > 0:
                await tracker.on_status(f"Пробую провайдер {cfg.provider}...")
            attempt += 1
            try:
                agent_start = time.monotonic()
                await tracker.on_first_event()
                agent_label = f"{cfg.provider}/{cfg.selected_model or '?'}"
                await tracker.on_tool_start("agent", 0)

                full_text = await asyncio.to_thread(
                    self._run_agent,
                    prompt,
                    cfg,
                    history_msgs=history_msgs,
                    system_prompt=system_prompt,
                    access_policy=access_policy,
                    gate_active=gate_active,
                )

                agent_duration = round(time.monotonic() - agent_start, 1)
                await tracker._put({
                    "type": "tool_end",
                    "tool": "agent",
                    "duration": agent_duration,
                    "is_error": False,
                    "summary": agent_label,
                })

                self._init_error = None
                if not full_text:
                    logger.debug("Deepagents returned empty response for provider=%s", cfg.provider)
                if full_text:
                    chunk_payload = safe_json_dumps({"text": full_text}, ensure_ascii=False)
                    await queue.put(f"data: {chunk_payload}\n\n")
                done_payload = safe_json_dumps(
                    {
                        "done": True,
                        "full_text": full_text,
                        "backend": "deepagents",
                        "provider": cfg.provider,
                        "model": cfg.selected_model,
                    },
                    ensure_ascii=False,
                )
                await queue.put(f"data: {done_payload}\n\n")
                return
            except Exception as exc:
                agent_duration = round(time.monotonic() - agent_start, 1)
                await tracker._put({
                    "type": "tool_end",
                    "tool": "agent",
                    "duration": agent_duration,
                    "is_error": True,
                    "summary": str(exc),
                })
                errors.append(format_provider_error(cfg.provider, exc))
                logger.warning("Deepagents provider failed (%s): %s", cfg.provider, exc)
        self._init_error = (
            "; ".join(errors) if errors else "Deepagents providers are not configured."
        )
        raise RuntimeError(self._init_error)

    async def run_researcher_writer(
        self,
        research_prompt: str,
        writer_prompt: str,
        model: str | None = None,
    ) -> str:
        """Run researcher-writer pipeline for content generation.

        1. Researcher: gathers context using search_messages tool
        2. Writer: produces final content based on research

        Returns the final written content.
        """
        errors: list[str] = []
        self._runtime_context.bind_owner_loop(asyncio.get_running_loop())
        for cfg in await self._candidate_configs():
            validation_error = self._validation_error(cfg)
            if validation_error:
                errors.append(f"{cfg.provider}: {validation_error}")
                continue
            try:
                # Step 1: Research phase
                research_result = await asyncio.to_thread(
                    self._run_agent,
                    research_prompt,
                    cfg,
                    system_prompt=(
                        "You are a researcher. Gather relevant information from the Telegram channels "
                        "using available tools. Be thorough and cite your sources."
                    ),
                )

                # Step 2: Writer phase
                combined_prompt = f"{writer_prompt}\n\nResearch context:\n{research_result}"
                final_result = await asyncio.to_thread(
                    self._run_agent,
                    combined_prompt,
                    cfg,
                    system_prompt=(
                        "You are a content writer. Write high-quality content based on the research "
                        "provided. Be engaging and informative."
                    ),
                )

                self._init_error = None
                return final_result
            except Exception as exc:
                errors.append(format_provider_error(cfg.provider, exc))
                logger.warning("Deepagents researcher-writer failed (%s): %s", cfg.provider, exc)

        self._init_error = (
            "; ".join(errors) if errors else "Deepagents providers are not configured."
        )
        raise RuntimeError(self._init_error)
