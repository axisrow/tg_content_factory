from __future__ import annotations

import asyncio
import json
import logging
import os
import shutil
from collections.abc import AsyncGenerator, Awaitable, Callable
from contextlib import suppress
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TypeVar

from claude_agent_sdk import AssistantMessage, ClaudeAgentOptions, ResultMessage, TextBlock, query

from src.agent.models import CLAUDE_MODEL_IDS
from src.agent.prompt_template import (
    AGENT_PROMPT_TEMPLATE_SETTING,
    DEFAULT_AGENT_PROMPT_TEMPLATE,
    PromptTemplateError,
    build_prompt_template_context,
    render_prompt_template,
)
from src.agent.provider_registry import ProviderRuntimeConfig
from src.config import AppConfig
from src.database import Database
from src.services.agent_provider_service import (
    AgentProviderService,
    ProviderModelCacheEntry,
    ProviderModelCompatibilityRecord,
)

logger = logging.getLogger(__name__)

_HISTORY_BUDGET = 100_000 * 4  # ~100K tokens in chars


def _embed_history_in_prompt(history_msgs: list[dict], message: str) -> str:
    """Build a single prompt string with conversation history and current message."""
    parts: list[str] = []
    for msg in history_msgs:
        tag = "user" if msg["role"] == "user" else "assistant"
        parts.append(f"<{tag}>\n{msg['content']}\n</{tag}>")
    parts.append(f"<user>\n{message}\n</user>")
    return "\n".join(parts)


_ALLOWED_TOOLS = ["mcp__telegram_db__search_messages", "mcp__telegram_db__get_channels"]
_DEEPAGENTS_PROBE_PROMPT = (
    "Compatibility probe. You must use the tool that lists active Telegram "
    "channels before answering. "
    "Do not answer from memory. After completing the tool call, reply with exactly PROBE_OK."
)
_DEEPAGENTS_PROBE_TIMEOUT_SECONDS = 45.0
_ToolResult = TypeVar("_ToolResult")


@dataclass(slots=True)
class AgentRuntimeStatus:
    claude_available: bool
    deepagents_available: bool
    dev_mode_enabled: bool
    backend_override: str
    selected_backend: str | None
    fallback_model: str
    fallback_provider: str
    using_override: bool
    error: str | None = None


class ClaudeSdkBackend:
    def __init__(self, db: Database, config: AppConfig) -> None:
        self._db = db
        self._config = config
        self._server = None

    def initialize(self) -> None:
        from src.agent.tools import make_mcp_server

        os.environ.setdefault("CLAUDE_CODE_STREAM_CLOSE_TIMEOUT", "300000")
        self._server = make_mcp_server(self._db)
        logger.info("Claude SDK backend initialized")

    @property
    def available(self) -> bool:
        return bool(
            os.environ.get("ANTHROPIC_API_KEY") or os.environ.get("CLAUDE_CODE_OAUTH_TOKEN")
        )

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
    ) -> None:
        if history_msgs:
            prompt = _embed_history_in_prompt(history_msgs, prompt)
        resolved_model = model or self._config.agent.model.strip() or os.environ.get("AGENT_MODEL")
        extra: dict = {}
        if resolved_model:
            extra["model"] = resolved_model

        stderr_lines: list[str] = []

        def _on_stderr(line: str) -> None:
            stderr_lines.append(line)
            logger.warning("claude-cli stderr: %s", line)

        cli_path = shutil.which("claude")
        logger.info("claude-cli path: %s", cli_path)

        options = ClaudeAgentOptions(
            system_prompt=system_prompt,
            mcp_servers={"telegram_db": self._server},
            allowed_tools=_ALLOWED_TOOLS,
            cli_path=cli_path or None,
            stderr=_on_stderr,
            **extra,
        )

        last_err: Exception | None = None
        for attempt in range(2):
            draining = False
            full_text = ""
            try:
                async for msg in query(prompt=prompt, options=options):
                    if draining:
                        continue
                    try:
                        if isinstance(msg, AssistantMessage):
                            for block in msg.content:
                                if isinstance(block, TextBlock):
                                    full_text += block.text
                                    chunk_payload = json.dumps(
                                        {"text": block.text}, ensure_ascii=False
                                    )
                                    await queue.put(f"data: {chunk_payload}\n\n")
                        elif isinstance(msg, ResultMessage):
                            done_payload = json.dumps(
                                {"done": True, "full_text": full_text, "backend": "claude"},
                                ensure_ascii=False,
                            )
                            await queue.put(f"data: {done_payload}\n\n")
                    except asyncio.CancelledError:
                        draining = True
                return
            except Exception as exc:
                if attempt == 0 and "Control request timeout" in str(exc):
                    logger.warning("Agent init timeout, retrying (thread %d)", thread_id)
                    last_err = exc
                    continue
                last_err = exc
                break

        if stderr_lines:
            logger.error(
                "claude-cli stderr dump (thread %d):\n%s",
                thread_id,
                "\n".join(stderr_lines),
            )
        elif last_err is not None:
            logger.error(
                (
                    "claude-cli failed with no stderr (thread %d): %s. "
                    "Prompt was %d chars (~%dK tokens)."
                ),
                thread_id,
                last_err,
                stats["prompt_chars"],
                stats["prompt_chars"] // 4000,
            )
        if last_err is not None:
            raise last_err


class DeepagentsBackend:
    def __init__(self, db: Database, config: AppConfig) -> None:
        self._db = db
        self._config = config
        self._provider_service = AgentProviderService(db, config)
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
            if legacy_model:
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
            return asyncio.run(operation())
        raise RuntimeError(
            f"Deepagents tool '{tool_name}' cannot run inside an active event loop: {loop}"
        )

    def _search_messages_tool(self, query_text: str) -> str:
        """Search recent Telegram messages by free-text query and return short previews."""
        try:
            messages, total = self._run_db_tool_sync(
                "search_messages",
                lambda: self._db.search_messages(query_text, limit=20),
            )
        except Exception as exc:
            logger.warning("Deepagents search_messages tool failed: %s", exc)
            return "Поиск сообщений временно недоступен из-за внутренней ошибки."

        if not messages:
            return f"Ничего не найдено по запросу: {query_text}"

        lines = [f"Найдено {total} сообщений по запросу '{query_text}'. Топ результатов:"]
        for message in messages:
            preview = (message.text or "").replace("\n", " ")[:200]
            lines.append(
                (
                    f"- [{message.date}] channel_id={message.channel_id} "
                    f"message_id={message.message_id}: {preview}"
                )
            )
        return "\n".join(lines)

    def _get_channels_tool(self) -> str:
        """List active Telegram channels that are available to the agent."""
        try:
            channels = self._run_db_tool_sync(
                "get_channels",
                lambda: self._db.get_channels(active_only=True, include_filtered=False),
            )
        except Exception as exc:
            logger.warning("Deepagents get_channels tool failed: %s", exc)
            return "Список активных каналов временно недоступен из-за внутренней ошибки."
        if not channels:
            return "Активные каналы не найдены."
        lines = ["Активные каналы:"]
        for channel in channels[:200]:
            title = channel.title or str(channel.channel_id)
            lines.append(
                f"- channel_id={channel.channel_id}, title={title}, type={channel.channel_type}"
            )
        return "\n".join(lines)

    def _build_agent(
        self,
        cfg: ProviderRuntimeConfig,
        *,
        tools: list[Callable] | None = None,
        record_last_used: bool = True,
        system_prompt: str = DEFAULT_AGENT_PROMPT_TEMPLATE,
    ):
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
        extra: dict[str, object] = {
            key: value for key, value in cfg.plain_fields.items() if value.strip()
        }
        if provider == "ollama":
            api_key = cfg.secret_fields.get("api_key", "").strip()
            normalized_base_url = self._provider_service.normalize_ollama_base_url(
                str(extra.get("base_url", "")),
                api_key,
            )
            extra["base_url"] = normalized_base_url
            if api_key:
                extra["client_kwargs"] = {"headers": {"Authorization": f"Bearer {api_key}"}}
        else:
            extra.update({key: value for key, value in cfg.secret_fields.items() if value.strip()})
        self._init_attempted_model = cfg.model_name

        try:
            from deepagents import create_deep_agent
            from langchain.chat_models import init_chat_model

            model = init_chat_model(model=resolved_model_name, model_provider=provider, **extra)
            agent = create_deep_agent(
                model=model,
                tools=tools or [self._search_messages_tool, self._get_channels_tool],
                system_prompt=system_prompt,
            )
            if record_last_used:
                self._last_used_provider = provider
                self._last_used_model = configured_model_name
            return agent
        except ImportError as exc:
            if "deepagents" in str(exc):
                self._init_error = "deepagents не установлен."
                raise RuntimeError(self._init_error) from exc
            package = (
                f"langchain-{provider.replace('_', '-')}" if provider else "langchain provider"
            )
            self._init_error = (
                f"Не установлена интеграция для provider '{provider}'. "
                f"Установите пакет вроде '{package}'."
            )
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
            errors: list[str] = []
            for cfg in candidates:
                validation_error = (
                    self._legacy_validation_error(cfg)
                    if self._is_legacy_candidate(cfg)
                    else self._validation_error(cfg)
                )
                if validation_error:
                    errors.append(f"{cfg.provider}: {validation_error}")
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
                "; ".join(errors)
                if errors
                else "Deepagents providers are not configured."
            )
            raise RuntimeError(self._init_error)

        errors: list[str] = []
        for cfg in candidates:
            validation_error = self._validation_error(cfg)
            if validation_error:
                errors.append(f"{cfg.provider}: {validation_error}")
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
                errors.append(f"{cfg.provider}: {exc}")
                logger.warning("Deepagents provider preflight failed (%s): %s", cfg.provider, exc)
        self._preflight_available = False
        self._init_error = (
            "; ".join(errors) if errors else "Deepagents providers are not configured."
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
    ) -> str:
        agent = self._build_agent(cfg, system_prompt=system_prompt)
        # Different agent frameworks have different history handling:
        # - `.run(prompt_str)` agents receive history embedded as XML tags in a single string
        # - `.invoke({"messages": [...]})` agents receive history as a structured message list
        if hasattr(agent, "run"):
            run_prompt = (
                _embed_history_in_prompt(history_msgs, prompt) if history_msgs else prompt
            )
            result = agent.run(run_prompt)
        else:
            messages: list[dict] = []
            for msg in (history_msgs or []):
                messages.append({"role": msg["role"], "content": msg["content"]})
            messages.append({"role": "user", "content": prompt})
            result = agent.invoke({"messages": messages})
        return self._extract_result_text(result)

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
    ) -> None:
        del thread_id, stats, model
        errors: list[str] = []
        for cfg in await self._candidate_configs():
            validation_error = self._validation_error(cfg)
            if validation_error:
                errors.append(f"{cfg.provider}: {validation_error}")
                continue
            try:
                full_text = await asyncio.to_thread(
                    self._run_agent,
                    prompt,
                    cfg,
                    history_msgs=history_msgs,
                    system_prompt=system_prompt,
                )
                self._init_error = None
                if full_text:
                    chunk_payload = json.dumps({"text": full_text}, ensure_ascii=False)
                    await queue.put(f"data: {chunk_payload}\n\n")
                done_payload = json.dumps(
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
                errors.append(f"{cfg.provider}: {exc}")
                logger.warning("Deepagents provider failed (%s): %s", cfg.provider, exc)
        self._init_error = (
            "; ".join(errors) if errors else "Deepagents providers are not configured."
        )
        raise RuntimeError(self._init_error)


class AgentManager:
    def __init__(self, db: Database, config: AppConfig | None = None) -> None:
        self._db = db
        self._config = config or AppConfig()
        self._claude_backend = ClaudeSdkBackend(db, self._config)
        self._deepagents_backend = DeepagentsBackend(db, self._config)
        self._active_tasks: dict[int, asyncio.Task] = {}

    async def refresh_settings_cache(self, *, preflight: bool = False) -> None:
        await self._deepagents_backend.refresh_settings_cache()
        if (
            preflight
            and self._deepagents_backend.configured
            and self._deepagents_backend.preflight_available is None
        ):
            try:
                self._deepagents_backend.initialize()
            except Exception:
                logger.warning("Deepagents backend pre-initialization failed", exc_info=True)

    def initialize(self) -> None:
        self._claude_backend.initialize()
        if (
            self._deepagents_backend.configured
            and self._deepagents_backend.preflight_available is None
        ):
            try:
                self._deepagents_backend.initialize()
            except Exception:
                logger.warning("Deepagents backend pre-initialization failed", exc_info=True)
        logger.info("AgentManager initialized")

    @property
    def available(self) -> bool:
        return self._claude_backend.available or self._deepagents_backend.available

    def _build_prompt_stats_only(self, history: list[dict], message: str) -> dict:
        """Compute prompt statistics without building the full formatted string."""
        user_part_chars = len(f"<user>\n{message}\n</user>")
        budget = _HISTORY_BUDGET
        used = user_part_chars

        total_msgs = len(history)
        kept_count = 0
        for msg in reversed(history):
            tag = "user" if msg["role"] == "user" else "assistant"
            part_chars = len(f"<{tag}>\n{msg['content']}\n</{tag}>")
            if used + part_chars > budget:
                break
            kept_count += 1
            used += part_chars

        # Approximate prompt_chars: sum of all parts plus newlines between them.
        # Each message part produces 1 newline separator + the part itself.
        sep_count = kept_count + 1  # kept messages + current message
        prompt_chars = used + sep_count - 1  # -1 because no separator before first
        return {
            "total_msgs": total_msgs,
            "kept_msgs": kept_count,
            "total_chars": sum(len(m["content"]) for m in history) + len(message),
            "prompt_chars": prompt_chars,
        }

    def _build_prompt(self, history: list[dict], message: str) -> tuple[str, dict]:
        user_part = f"<user>\n{message}\n</user>"
        budget = _HISTORY_BUDGET
        used = len(user_part)

        total_msgs = len(history)
        kept: list[str] = []
        for msg in reversed(history):
            tag = "user" if msg["role"] == "user" else "assistant"
            part = f"<{tag}>\n{msg['content']}\n</{tag}>"
            if used + len(part) > budget:
                break
            kept.append(part)
            used += len(part)

        kept.reverse()
        kept.append(user_part)
        prompt = "\n".join(kept)
        stats = {
            "total_msgs": total_msgs,
            "kept_msgs": len(kept) - 1,
            "total_chars": sum(len(m["content"]) for m in history) + len(message),
            "prompt_chars": len(prompt),
        }
        return prompt, stats

    async def _dev_mode_enabled(self) -> bool:
        return (await self._db.get_setting("agent_dev_mode_enabled") or "0") == "1"

    async def _backend_override(self) -> str:
        override = (await self._db.get_setting("agent_backend_override") or "auto").strip()
        if override not in {"auto", "claude", "deepagents"}:
            return "auto"
        return override

    async def get_runtime_status(self) -> AgentRuntimeStatus:
        await self.refresh_settings_cache(preflight=True)
        dev_mode_enabled = await self._dev_mode_enabled()
        backend_override = await self._backend_override()
        claude_available = self._claude_backend.available
        deepagents_available = self._deepagents_backend.available
        deepagents_error = self._deepagents_backend.init_error

        selected_backend: str | None
        error: str | None = None
        using_override = dev_mode_enabled and backend_override != "auto"
        if using_override:
            selected_backend = backend_override
            if selected_backend == "claude" and not claude_available:
                error = "claude-agent-sdk не сконфигурирован."
            elif selected_backend == "deepagents" and not deepagents_available:
                error = deepagents_error or "deepagents fallback не сконфигурирован."
        else:
            if (
                deepagents_available
                and self._deepagents_backend.has_usable_db_provider_configs
            ):
                selected_backend = "deepagents"
            elif claude_available:
                selected_backend = "claude"
            elif deepagents_available:
                selected_backend = "deepagents"
            else:
                selected_backend = None
                error = (
                    deepagents_error or "Не настроен ни claude-agent-sdk, ни deepagents fallback."
                )

        return AgentRuntimeStatus(
            claude_available=claude_available,
            deepagents_available=deepagents_available,
            dev_mode_enabled=dev_mode_enabled,
            backend_override=backend_override,
            selected_backend=selected_backend,
            fallback_model=self._deepagents_backend.fallback_model,
            fallback_provider=self._deepagents_backend.fallback_provider,
            using_override=using_override,
            error=error,
        )

    async def estimate_prompt_tokens(self, thread_id: int, message: str) -> int:
        history = await self._db.get_agent_messages(thread_id)
        prompt, _stats = self._build_prompt(history, message)
        return len(prompt) // 4

    async def probe_provider_config(
        self,
        cfg: ProviderRuntimeConfig,
        *,
        probe_kind: str = "auto-select",
    ) -> ProviderModelCompatibilityRecord:
        return await self._deepagents_backend.probe_config(cfg, probe_kind=probe_kind)

    async def chat_stream(
        self, thread_id: int, message: str, model: str | None = None
    ) -> AsyncGenerator[str, None]:
        history = await self._db.get_agent_messages(thread_id)
        assert not history or history[-1]["role"] == "user", (
            "Expected last DB message to be the user message just saved"
        )
        stats = self._build_prompt_stats_only(history[:-1], message)
        history_for_backend = history[:-1][-stats["kept_msgs"]:] if stats["kept_msgs"] else []
        prompt = message
        # stats["prompt_chars"] estimates total context size formatted as XML.
        # Not the actual prompt sent to backend (which is just `message`).
        # Useful approximation for monitoring context consumption.
        logger.info(
            "Prompt for thread %d: %d chars (~%dK tokens), %d/%d history msgs",
            thread_id,
            stats["prompt_chars"],
            stats["prompt_chars"] // 4000,
            stats["kept_msgs"],
            stats["total_msgs"],
        )
        prompt_template = (
            await self._db.get_setting(AGENT_PROMPT_TEMPLATE_SETTING)
            or DEFAULT_AGENT_PROMPT_TEMPLATE
        )
        try:
            system_prompt = render_prompt_template(
                prompt_template,
                build_prompt_template_context(history),
            )
        except PromptTemplateError:
            logger.warning(
                "Invalid saved agent prompt template, falling back to default.",
                exc_info=True,
            )
            system_prompt = DEFAULT_AGENT_PROMPT_TEMPLATE

        status = await self.get_runtime_status()
        backend_name = status.selected_backend
        if status.error and (backend_name is None or status.using_override):
            err_payload = json.dumps(
                {"error": f"Ошибка агента: {status.error}"}, ensure_ascii=False
            )
            yield f"data: {err_payload}\n\n"
            return
        if backend_name == "claude":
            backend = self._claude_backend
            if model not in CLAUDE_MODEL_IDS:
                model = None
        elif backend_name == "deepagents":
            backend = self._deepagents_backend
            model = None
        else:
            err_payload = json.dumps(
                {"error": "Ошибка агента: не удалось выбрать backend."},
                ensure_ascii=False,
            )
            yield f"data: {err_payload}\n\n"
            return

        queue: asyncio.Queue[str | None] = asyncio.Queue()

        async def _run_backend(
            selected_backend: ClaudeSdkBackend | DeepagentsBackend,
            failure_prefix: Callable[[str], str],
        ) -> None:
            try:
                await selected_backend.chat_stream(
                    thread_id=thread_id,
                    prompt=prompt,
                    system_prompt=system_prompt,
                    stats=stats,
                    model=model,
                    queue=queue,
                    history_msgs=history_for_backend,
                )
            except Exception as exc:
                logger.exception("Agent chat error for thread %d", thread_id)
                error_text = str(exc)
                lowered_error = error_text.lower()
                if "ollama" in lowered_error and "500" in lowered_error and any(
                    marker in lowered_error
                    for marker in ("internal server error", "server error", "status code")
                ):
                    error_text = (
                        "Внутренняя ошибка сервиса Ollama (500). "
                        "Возможно, модель не загрузилась или не хватает ресурсов (VRAM/RAM)."
                    )
                elif "ollama" in lowered_error and any(
                    marker in lowered_error
                    for marker in ("connection refused", "failed to connect", "connecterror")
                ):
                    error_text = "Не удалось подключиться к Ollama. Проверьте, что сервис запущен."

                err_payload = json.dumps(
                    {"error": failure_prefix(error_text)},
                    ensure_ascii=False,
                )
                await queue.put(f"data: {err_payload}\n\n")
            await queue.put(None)

        task = asyncio.create_task(
            _run_backend(backend, lambda text: f"Ошибка агента ({backend_name}): {text}")
        )
        self._active_tasks[thread_id] = task

        def _cleanup(t: asyncio.Task) -> None:
            if self._active_tasks.get(thread_id) is t:
                del self._active_tasks[thread_id]

        task.add_done_callback(_cleanup)
        try:
            while True:
                item = await queue.get()
                if item is None:
                    break
                yield item
        finally:
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task

    async def cancel_stream(self, thread_id: int) -> bool:
        task = self._active_tasks.pop(thread_id, None)
        if task is None:
            return False
        task.cancel()
        with suppress(asyncio.CancelledError):
            await task
        return True

    async def close_all(self) -> None:
        tasks = list(self._active_tasks.values())
        self._active_tasks.clear()
        for task in tasks:
            task.cancel()
        for task in tasks:
            with suppress(asyncio.CancelledError):
                await task
