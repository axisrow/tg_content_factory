from __future__ import annotations

import asyncio
import json
import logging
import os
import shutil
from collections.abc import AsyncGenerator, Callable
from contextlib import suppress
from dataclasses import dataclass

from claude_agent_sdk import AssistantMessage, ClaudeAgentOptions, ResultMessage, TextBlock, query

from src.config import AppConfig
from src.database import Database

logger = logging.getLogger(__name__)

_SYSTEM_PROMPT = (
    "Ты — аналитический ассистент для работы с данными из Telegram-каналов.\n"
    "Используй search_messages для поиска сообщений и get_channels для списка каналов.\n"
    "Основной use-case: анализ вопросов и ответов из каналов для создания учебного курса.\n"
    "Отвечай на русском языке. Будь точным и структурированным."
)

_ALLOWED_TOOLS = ["mcp__telegram_db__search_messages", "mcp__telegram_db__get_channels"]
_CLAUDE_MODELS = {
    "claude-sonnet-4-5",
    "claude-opus-4-6",
    "claude-haiku-4-5-20251001",
}
_PROTECTED_DEEPAGENT_ENV = ("ANTHROPIC_API_KEY", "CLAUDE_CODE_OAUTH_TOKEN")


@dataclass(slots=True)
class AgentRuntimeStatus:
    claude_available: bool
    deepagents_available: bool
    dev_mode_enabled: bool
    backend_override: str
    selected_backend: str | None
    fallback_model: str
    using_override: bool
    error: str | None = None


class _DeepAgentEnvGuard:
    def __init__(self) -> None:
        self._saved: dict[str, str] = {}

    def __enter__(self) -> None:
        for key in _PROTECTED_DEEPAGENT_ENV:
            if key in os.environ:
                self._saved[key] = os.environ[key]
                del os.environ[key]

    def __exit__(self, exc_type, exc, tb) -> None:
        for key, value in self._saved.items():
            os.environ[key] = value


class ClaudeSdkBackend:
    def __init__(self, db: Database) -> None:
        self._db = db
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
        stats: dict,
        model: str | None,
        queue: asyncio.Queue[str | None],
    ) -> None:
        resolved_model = model or os.environ.get("AGENT_MODEL")
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
            system_prompt=_SYSTEM_PROMPT,
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
            logger.error("claude-cli stderr dump (thread %d):\n%s", thread_id, "\n".join(stderr_lines))
        elif last_err is not None:
            logger.error(
                "claude-cli failed with no stderr (thread %d): %s. Prompt was %d chars (~%dK tokens).",
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
        self._agent = None
        self._agent_model: str | None = None

    @property
    def fallback_model(self) -> str:
        return self._config.agent.fallback_model.strip() or os.environ.get(
            "AGENT_FALLBACK_MODEL", ""
        ).strip()

    @property
    def available(self) -> bool:
        return bool(self.fallback_model)

    def _fallback_api_key(self) -> str:
        return self._config.agent.fallback_api_key.strip() or os.environ.get(
            "AGENT_FALLBACK_API_KEY", ""
        ).strip()

    def _provider_from_model(self, model_name: str) -> str | None:
        provider, _, _model = model_name.partition(":")
        return provider or None

    def _search_messages_tool(self, query_text: str) -> str:
        try:
            messages, total = asyncio.run(self._db.search_messages(query_text, limit=20))
        except RuntimeError:
            with suppress(RuntimeError):
                loop = asyncio.get_running_loop()
                raise RuntimeError(f"Unexpected running loop in deepagents worker: {loop}")
            messages, total = asyncio.run(self._db.search_messages(query_text, limit=20))

        if not messages:
            return f"Ничего не найдено по запросу: {query_text}"

        lines = [f"Найдено {total} сообщений по запросу '{query_text}'. Топ результатов:"]
        for message in messages:
            preview = (message.text or "").replace("\n", " ")[:200]
            lines.append(
                f"- [{message.date}] channel_id={message.channel_id} message_id={message.message_id}: {preview}"
            )
        return "\n".join(lines)

    def _get_channels_tool(self) -> str:
        channels = asyncio.run(self._db.get_channels(active_only=True, include_filtered=False))
        if not channels:
            return "Активные каналы не найдены."
        lines = ["Активные каналы:"]
        for channel in channels[:200]:
            title = channel.title or str(channel.channel_id)
            lines.append(
                f"- channel_id={channel.channel_id}, title={title}, type={channel.channel_type}"
            )
        return "\n".join(lines)

    def _build_agent(self) -> None:
        model_name = self.fallback_model
        if not model_name:
            raise RuntimeError("AGENT_FALLBACK_MODEL не задан.")
        if ":" not in model_name:
            raise RuntimeError(
                "AGENT_FALLBACK_MODEL должен быть в формате provider:model для deepagents."
            )

        api_key = self._fallback_api_key()
        provider = self._provider_from_model(model_name)
        extra: dict[str, str] = {}
        if api_key:
            extra["api_key"] = api_key

        try:
            from deepagents import create_deep_agent
            from langchain.chat_models import init_chat_model

            with _DeepAgentEnvGuard():
                model = init_chat_model(model_name, **extra)
                self._agent = create_deep_agent(
                    model=model,
                    tools=[self._search_messages_tool, self._get_channels_tool],
                    system_prompt=_SYSTEM_PROMPT,
                )
                self._agent_model = model_name
        except ImportError as exc:
            if "deepagents" in str(exc):
                raise RuntimeError("deepagents не установлен.") from exc
            package = f"langchain-{provider.replace('_', '-')}" if provider else "langchain provider"
            raise RuntimeError(
                f"Не установлена интеграция для provider '{provider}'. Установите пакет вроде '{package}'."
            ) from exc
        except ValueError as exc:
            raise RuntimeError(f"Некорректная конфигурация fallback модели: {exc}") from exc
        except Exception as exc:
            raise RuntimeError(f"Не удалось инициализировать deepagents: {exc}") from exc

    def initialize(self) -> None:
        if not self.available:
            logger.info("Deepagents backend disabled: no fallback model configured")
            return
        self._build_agent()
        logger.info("Deepagents backend initialized with model %s", self._agent_model)

    def _run_agent(self, prompt: str) -> str:
        if self._agent is None or self._agent_model != self.fallback_model:
            self._build_agent()
        assert self._agent is not None
        with _DeepAgentEnvGuard():
            if hasattr(self._agent, "run"):
                result = self._agent.run(prompt)
            else:
                result = self._agent.invoke({"messages": [{"role": "user", "content": prompt}]})
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

    async def chat_stream(
        self,
        *,
        thread_id: int,
        prompt: str,
        stats: dict,
        model: str | None,
        queue: asyncio.Queue[str | None],
    ) -> None:
        del thread_id, stats, model
        full_text = await asyncio.to_thread(self._run_agent, prompt)
        if full_text:
            chunk_payload = json.dumps({"text": full_text}, ensure_ascii=False)
            await queue.put(f"data: {chunk_payload}\n\n")
        done_payload = json.dumps(
            {"done": True, "full_text": full_text, "backend": "deepagents"},
            ensure_ascii=False,
        )
        await queue.put(f"data: {done_payload}\n\n")


class AgentManager:
    def __init__(self, db: Database, config: AppConfig | None = None) -> None:
        self._db = db
        self._config = config or AppConfig()
        self._claude_backend = ClaudeSdkBackend(db)
        self._deepagents_backend = DeepagentsBackend(db, self._config)
        self._active_tasks: dict[int, asyncio.Task] = {}

    def initialize(self) -> None:
        self._claude_backend.initialize()
        if self._deepagents_backend.available:
            try:
                self._deepagents_backend.initialize()
            except Exception:
                logger.warning("Deepagents backend pre-initialization failed", exc_info=True)
        logger.info("AgentManager initialized")

    @property
    def available(self) -> bool:
        return self._claude_backend.available or self._deepagents_backend.available

    def _build_prompt(self, history: list[dict], message: str) -> tuple[str, dict]:
        user_part = f"<user>\n{message}\n</user>"
        budget = 100_000 * 4
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
        dev_mode_enabled = await self._dev_mode_enabled()
        backend_override = await self._backend_override()
        claude_available = self._claude_backend.available
        deepagents_available = self._deepagents_backend.available

        selected_backend: str | None
        error: str | None = None
        using_override = dev_mode_enabled and backend_override != "auto"
        if using_override:
            selected_backend = backend_override
            if selected_backend == "claude" and not claude_available:
                error = "claude-agent-sdk не сконфигурирован."
            elif selected_backend == "deepagents" and not deepagents_available:
                error = "deepagents fallback не сконфигурирован."
        else:
            if claude_available:
                selected_backend = "claude"
            elif deepagents_available:
                selected_backend = "deepagents"
            else:
                selected_backend = None
                error = "Не настроен ни claude-agent-sdk, ни deepagents fallback."

        return AgentRuntimeStatus(
            claude_available=claude_available,
            deepagents_available=deepagents_available,
            dev_mode_enabled=dev_mode_enabled,
            backend_override=backend_override,
            selected_backend=selected_backend,
            fallback_model=self._deepagents_backend.fallback_model,
            using_override=using_override,
            error=error,
        )

    async def estimate_prompt_tokens(self, thread_id: int, message: str) -> int:
        history = await self._db.get_agent_messages(thread_id)
        prompt, _stats = self._build_prompt(history, message)
        return len(prompt) // 4

    async def chat_stream(
        self, thread_id: int, message: str, model: str | None = None
    ) -> AsyncGenerator[str, None]:
        history = await self._db.get_agent_messages(thread_id)
        assert not history or history[-1]["role"] == "user", (
            "Expected last DB message to be the user message just saved"
        )
        prompt, stats = self._build_prompt(history[:-1], message)
        logger.info(
            "Prompt for thread %d: %d chars (~%dK tokens), %d/%d history msgs kept",
            thread_id,
            stats["prompt_chars"],
            stats["prompt_chars"] // 4000,
            stats["kept_msgs"],
            stats["total_msgs"],
        )

        status = await self.get_runtime_status()
        backend_name = status.selected_backend
        if status.error and (backend_name is None or status.using_override):
            err_payload = json.dumps({"error": f"Ошибка агента: {status.error}"}, ensure_ascii=False)
            yield f"data: {err_payload}\n\n"
            return
        if backend_name == "claude":
            backend = self._claude_backend
            if model not in _CLAUDE_MODELS:
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
                    stats=stats,
                    model=model,
                    queue=queue,
                )
            except Exception as exc:
                logger.exception("Agent chat error for thread %d", thread_id)
                err_payload = json.dumps(
                    {"error": failure_prefix(str(exc))},
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
