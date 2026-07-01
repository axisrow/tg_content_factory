from __future__ import annotations

import asyncio
import logging
import threading
import time
from collections.abc import AsyncGenerator, Callable
from contextlib import nullcontext, suppress
from dataclasses import dataclass
from typing import Any

from src.agent.adk_backend import AdkSdkBackend
from src.agent.backends._stream import (
    _STDERR_STAGE_MAP,
    _as_prompt_stream,
    _auto_approve_tool,
    _await_with_countdown,
    _ChatStreamState,
    _diagnose_connection,
    _dispatch_assistant_message,
    _dispatch_content_block_delta,
    _dispatch_content_block_start,
    _dispatch_rate_limit_event,
    _dispatch_result_message,
    _dispatch_stream_error_event,
    _dispatch_stream_event,
    _dispatch_stream_message,
    _embed_history_in_prompt,
    _emit_error,
    _sse,
    _summarize_tool_args,
    _ToolTracker,
    _truncate,
)
from src.agent.backends.claude_sdk import ClaudeSdkBackend
from src.agent.backends.deepagents import DeepagentsBackend
from src.agent.codex_backend import CodexSdkBackend
from src.agent.models import (
    VALID_AGENT_BACKENDS,
    model_for_backend,
)
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
from src.live_runtime_pause import LiveRuntimePauseGate
from src.services.agent_provider_service import ProviderModelCompatibilityRecord

logger = logging.getLogger(__name__)

_HISTORY_BUDGET = 100_000 * 4  # ~100K tokens in chars
AgentBackend = ClaudeSdkBackend | DeepagentsBackend | CodexSdkBackend | AdkSdkBackend

__all__ = [
    "AgentManager",
    "AgentRuntimeStatus",
    "ClaudeSdkBackend",
    "DeepagentsBackend",
    "_ChatStreamState",
    "_STDERR_STAGE_MAP",
    "_SettingsCache",
    "_ToolTracker",
    "_as_prompt_stream",
    "_auto_approve_tool",
    "_await_with_countdown",
    "_diagnose_connection",
    "_dispatch_assistant_message",
    "_dispatch_content_block_delta",
    "_dispatch_content_block_start",
    "_dispatch_rate_limit_event",
    "_dispatch_result_message",
    "_dispatch_stream_error_event",
    "_dispatch_stream_event",
    "_dispatch_stream_message",
    "_embed_history_in_prompt",
    "_emit_error",
    "_sse",
    "_summarize_tool_args",
    "_truncate",
]


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
    codex_available: bool = False
    adk_available: bool = False

_SETTINGS_CACHE_TTL = 60.0  # seconds


class _SettingsCache:
    """Simple TTL cache for DB settings to avoid repeated queries per chat message."""

    __slots__ = ("_entries",)

    def __init__(self) -> None:
        self._entries: dict[str, tuple[Any, float]] = {}

    def get(self, key: str) -> Any | None:
        entry = self._entries.get(key)
        if entry is None:
            return None
        value, expires = entry
        if time.monotonic() > expires:
            del self._entries[key]
            return None
        return value

    def set(self, key: str, value: Any, ttl: float = _SETTINGS_CACHE_TTL) -> None:
        self._entries[key] = (value, time.monotonic() + ttl)

    def invalidate(self, key: str | None = None) -> None:
        if key is None:
            self._entries.clear()
        else:
            self._entries.pop(key, None)


class AgentManager:
    def __init__(
        self,
        db: Database,
        config: AppConfig | None = None,
        client_pool=None,
        scheduler_manager=None,
        live_runtime_pause_gate: LiveRuntimePauseGate | None = None,
    ) -> None:
        self._db = db
        self._config = config or AppConfig()
        self._live_runtime_pause_gate = live_runtime_pause_gate
        self._claude_backend = ClaudeSdkBackend(
            db, self._config, client_pool=client_pool, scheduler_manager=scheduler_manager,
        )
        self._deepagents_backend = DeepagentsBackend(
            db, self._config, client_pool=client_pool, scheduler_manager=scheduler_manager,
        )
        self._codex_backend = CodexSdkBackend(
            db, self._config, client_pool=client_pool, scheduler_manager=scheduler_manager,
        )
        self._adk_backend = AdkSdkBackend(
            db, self._config, client_pool=client_pool, scheduler_manager=scheduler_manager,
        )
        self._active_tasks: dict[int, asyncio.Task] = {}
        self._active_task_sessions: dict[int, str] = {}
        self._active_task_cancel_events: dict[int, threading.Event] = {}
        self._settings_cache = _SettingsCache()
        self._cached_allowed_tools: list[str] | None = None
        self._cached_filtered_tools: tuple[list[str], dict[str, bool]] | None = None
        from src.agent.permission_gate import PermissionGate

        self._permission_gate = PermissionGate()

    @property
    def permission_gate(self):
        """Return the PermissionGate for this manager (used by TUI/web to resolve dialogs)."""
        return self._permission_gate

    def enable_permission_gate(self) -> None:
        """Activate the permission gate (registers it globally for tool handlers).

        Call this in TUI/web mode to enable interactive permission dialogs.
        """
        from src.agent.permission_gate import set_gate

        set_gate(self._permission_gate)

    def disable_permission_gate(self) -> None:
        """Deactivate the permission gate (reverts to text-error behaviour)."""
        from src.agent.permission_gate import set_gate

        set_gate(None)

    async def refresh_settings_cache(self, *, preflight: bool = False) -> None:
        await self._deepagents_backend.refresh_settings_cache()
        if preflight and self._deepagents_backend.configured:
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
        return (
            self._claude_backend.available
            or self._deepagents_backend.available
            or self._codex_backend.available
            or self._adk_backend.available
        )

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

    async def _get_setting_cached(self, key: str, default: str = "") -> str:
        cached = self._settings_cache.get(key)
        if cached is not None:
            return cached
        value = await self._db.get_setting(key) or default
        self._settings_cache.set(key, value)
        return value

    async def _dev_mode_enabled(self) -> bool:
        return (await self._get_setting_cached("agent_dev_mode_enabled", "0")) == "1"

    async def _backend_override(self) -> str:
        override = (await self._get_setting_cached("agent_backend_override", "auto")).strip()
        if override not in VALID_AGENT_BACKENDS:
            return "auto"
        return override

    async def get_runtime_status(self) -> AgentRuntimeStatus:
        await self.refresh_settings_cache(preflight=True)
        dev_mode_enabled = await self._dev_mode_enabled()
        backend_override = await self._backend_override()
        claude_available = self._claude_backend.available
        deepagents_available = self._deepagents_backend.available
        codex_available = self._codex_backend.available
        adk_available = self._adk_backend.available
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
            elif selected_backend == "codex" and not codex_available:
                error = "Codex SDK не установлен или Codex CLI не авторизован."
            elif selected_backend == "adk" and not adk_available:
                error = "Google ADK не установлен или не задан GOOGLE_API_KEY / GEMINI_API_KEY."
        else:
            # Codex is intentionally NOT in the auto-fallback chain: each turn
            # runs a blocking `codex` CLI subprocess and spawns an mcp-server
            # subprocess (slow, heavy), so it must be opt-in via the dev-mode
            # `agent_backend_override`, never silently auto-selected — the same
            # rationale that makes the codex image adapter explicit_only.
            if deepagents_available and self._deepagents_backend.has_usable_db_provider_configs:
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
            codex_available=codex_available,
            adk_available=adk_available,
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
        self,
        thread_id: int,
        message: str,
        model: str | None = None,
        session_id: str = "web",
        *,
        interactive_permissions: bool = False,
    ) -> AsyncGenerator[str, None]:
        history = await self._db.get_agent_messages(thread_id)
        assert (
            not history or history[-1]["role"] == "user"
        ), "Expected last DB message to be the user message just saved"
        stats = self._build_prompt_stats_only(history[:-1], message)
        history_for_backend = history[:-1][-stats["kept_msgs"] :] if stats["kept_msgs"] else []
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
            await self._get_setting_cached(AGENT_PROMPT_TEMPLATE_SETTING, DEFAULT_AGENT_PROMPT_TEMPLATE)
        )
        try:
            system_prompt = render_prompt_template(
                prompt_template,
                build_prompt_template_context(history),
            )
        except (PromptTemplateError, KeyError, ValueError):
            logger.warning(
                "Invalid saved agent prompt template, falling back to default.",
                exc_info=True,
            )
            system_prompt = DEFAULT_AGENT_PROMPT_TEMPLATE

        status = await self.get_runtime_status()
        backend_name = status.selected_backend
        if status.error and (backend_name is None or status.using_override):
            yield _sse({"error": f"Ошибка агента: {status.error}"})
            return
        backend: AgentBackend
        if backend_name == "claude":
            backend = self._claude_backend
        elif backend_name == "deepagents":
            backend = self._deepagents_backend
        elif backend_name == "codex":
            backend = self._codex_backend
        elif backend_name == "adk":
            backend = self._adk_backend
        else:
            yield _sse({"error": "Ошибка агента: не удалось выбрать backend."})
            return
        # Drop a model ID that doesn't belong to the selected backend (e.g. a
        # Claude ID still in the request while codex/adk is active) → backend
        # default. deepagents always resolves to None (its model is settings-led).
        model = model_for_backend(backend_name, model)

        queue: asyncio.Queue[str | None] = asyncio.Queue()

        # Capture gate state and compute DB access policy before spawning the task.
        # The ContextVar token must be created AND reset inside the same asyncio task,
        # so the actual set/reset happens inside _run_backend (not in the generator).
        from src.agent.permission_gate import (
            AgentRequestContext,
            PermissionWaitTracker,
            reset_request_context,
            set_request_context,
        )

        _req_ctx: AgentRequestContext | None = None
        cancel_event = threading.Event()
        permission_wait_tracker = PermissionWaitTracker()
        if interactive_permissions:
            from src.agent.tools.permissions import load_tool_access_policy

            _access_policy = await load_tool_access_policy(self._db, use_cache=True)
            _req_ctx = AgentRequestContext(
                session_id=session_id,
                thread_id=thread_id,
                queue=queue,
                tool_access_policy=_access_policy,
                permission_gate=self._permission_gate,
                permission_timeout=self._config.agent.permission_timeout,
                cancel_event=cancel_event,
                permission_wait_tracker=permission_wait_tracker,
            )

        async def _run_backend(
            selected_backend: AgentBackend,
            failure_prefix: Callable[[str], str],
        ) -> None:
            # Set ContextVar here so token is created and reset in the same task context.
            _token = set_request_context(_req_ctx) if _req_ctx is not None else None
            try:
                # Pause background live-runtime work for the duration of the
                # chat when a gate is present (worker mode); otherwise no-op.
                gate = self._live_runtime_pause_gate
                pause = gate.agent_request() if gate is not None else nullcontext()
                async with pause:
                    # All backends share one chat_stream signature; deepagents
                    # ignores session_id (del'd in its body) rather than omitting it.
                    await selected_backend.chat_stream(
                        thread_id=thread_id,
                        prompt=prompt,
                        system_prompt=system_prompt,
                        stats=stats,
                        model=model,
                        queue=queue,
                        history_msgs=history_for_backend,
                        session_id=session_id,
                    )
            except Exception as exc:
                logger.exception("Agent chat error for thread %d", thread_id)
                error_text = str(exc)
                lowered_error = error_text.lower()
                if (
                    "ollama" in lowered_error
                    and "500" in lowered_error
                    and any(
                        marker in lowered_error
                        for marker in ("internal server error", "server error", "status code")
                    )
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

                await queue.put(_sse({"error": failure_prefix(error_text)}))
            finally:
                cancel_event.set()
                self._permission_gate.clear_thread(session_id, thread_id)
                if _token is not None:
                    reset_request_context(_token)
            await queue.put(None)

        # Cleanup stale done tasks before adding new one
        stale = [tid for tid, t in self._active_tasks.items() if t.done()]
        for tid in stale:
            del self._active_tasks[tid]

        task = asyncio.create_task(
            _run_backend(backend, lambda text: f"Ошибка агента ({backend_name}): {text}")
        )
        self._active_tasks[thread_id] = task
        self._active_task_sessions[thread_id] = session_id
        self._active_task_cancel_events[thread_id] = cancel_event

        def _cleanup(t: asyncio.Task) -> None:
            if self._active_tasks.get(thread_id) is t:
                del self._active_tasks[thread_id]
                self._active_task_sessions.pop(thread_id, None)
                self._active_task_cancel_events.pop(thread_id, None)

        task.add_done_callback(_cleanup)

        # Immediate feedback before backend connects (can take 10-30s)
        yield _sse({"type": "status", "text": f"Подключение к {backend_name}..."})

        try:
            while True:
                item = await queue.get()
                if item is None:
                    break
                yield item
        finally:
            cancel_event.set()
            self._permission_gate.clear_thread(session_id, thread_id)
            task.cancel()
            with suppress(asyncio.CancelledError):
                await task

    async def cancel_stream(self, thread_id: int, *, wait_timeout: float | None = None) -> bool:
        task = self._active_tasks.pop(thread_id, None)
        if task is None:
            return False
        session_id = self._active_task_sessions.pop(thread_id, None)
        cancel_event = self._active_task_cancel_events.pop(thread_id, None)
        if cancel_event is not None:
            cancel_event.set()
        if session_id is not None:
            self._permission_gate.clear_thread(session_id, thread_id)
        task.cancel()
        if wait_timeout is None:
            with suppress(asyncio.CancelledError):
                await task
        else:
            done, pending = await asyncio.wait({task}, timeout=wait_timeout)
            for done_task in done:
                with suppress(asyncio.CancelledError):
                    exc = done_task.exception()
                    if exc is not None:
                        logger.debug(
                            "Cancelled agent stream %d finished with error",
                            thread_id,
                            exc_info=(type(exc), exc, exc.__traceback__),
                        )
            if pending:
                logger.warning(
                    "Agent stream %d did not stop within %.1fs; continuing cleanup",
                    thread_id,
                    wait_timeout,
                )
        return True

    async def close_all(self) -> None:
        tasks = list(self._active_tasks.values())
        self._active_tasks.clear()
        for cancel_event in self._active_task_cancel_events.values():
            cancel_event.set()
        self._active_task_sessions.clear()
        self._active_task_cancel_events.clear()
        for task in tasks:
            task.cancel()
        if tasks:
            _, pending = await asyncio.wait(tasks, timeout=5.0)
            for task in pending:
                logger.debug("Agent task did not finish within timeout: %s", task.get_name())
        self._permission_gate.cancel_all()
