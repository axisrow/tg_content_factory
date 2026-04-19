from __future__ import annotations

import asyncio
import json
import logging
import os
import shutil
import time
from collections.abc import AsyncGenerator, AsyncIterator, Awaitable, Callable
from contextlib import suppress
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import Any, TypeVar

from claude_agent_sdk import (
    AssistantMessage,
    ClaudeAgentOptions,
    ClaudeSDKError,
    CLIConnectionError,
    CLINotFoundError,
    PermissionResultAllow,
    ProcessError,
    RateLimitEvent,
    ResultMessage,
    StreamEvent,
    TextBlock,
    ToolPermissionContext,
    ToolResultBlock,
    ToolUseBlock,
    UserMessage,
    query,
)

from src.agent.models import CLAUDE_MODEL_IDS
from src.agent.prompt_template import (
    AGENT_PROMPT_TEMPLATE_SETTING,
    DEFAULT_AGENT_PROMPT_TEMPLATE,
    PromptTemplateError,
    build_prompt_template_context,
    render_prompt_template,
)
from src.agent.provider_registry import ZAI_DEFAULT_BASE_URL, ProviderRuntimeConfig
from src.config import AppConfig
from src.database import Database
from src.services.agent_provider_service import (
    AgentProviderService,
    ProviderModelCacheEntry,
    ProviderModelCompatibilityRecord,
)
from src.utils.json import safe_json_dumps

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


def _summarize_tool_args(args: dict) -> str:
    if not args:
        return ""
    first_key = next(iter(args))
    first_val = str(args[first_key])
    if len(first_val) > 60:
        first_val = first_val[:57] + "..."
    if len(args) > 1:
        return f"{first_key}={first_val!r} (+{len(args) - 1})"
    return f"{first_key}={first_val!r}"


def _truncate(text: str, limit: int = 120) -> str:
    return text[:limit - 3] + "..." if len(text) > limit else text


def _diagnose_connection(cli_path: str | None, stderr_lines: list[str]) -> str:
    """Build a concrete diagnostic message by checking environment and stderr."""
    problems: list[str] = []

    if not cli_path:
        problems.append("Claude CLI не найден в PATH")

    has_key = bool(os.environ.get("ANTHROPIC_API_KEY"))
    has_oauth = bool(os.environ.get("CLAUDE_CODE_OAUTH_TOKEN"))
    if not has_key and not has_oauth:
        problems.append("Ни ANTHROPIC_API_KEY, ни CLAUDE_CODE_OAUTH_TOKEN не заданы")

    stderr_lower = " ".join(stderr_lines[-5:]).lower() if stderr_lines else ""
    if "invalid" in stderr_lower and "key" in stderr_lower:
        problems.append("API ключ невалиден (см. stderr)")
    if "rate limit" in stderr_lower or "429" in stderr_lower:
        problems.append("Сработал rate limit на API")
    if "network" in stderr_lower or "dns" in stderr_lower or "econnrefused" in stderr_lower:
        problems.append("Проблема с сетевым подключением")
    if "permission" in stderr_lower or "403" in stderr_lower:
        problems.append("Нет прав доступа к API (403)")
    if "401" in stderr_lower or "unauthorized" in stderr_lower:
        problems.append("API ключ отклонён (401)")

    if problems:
        return "Диагностика: " + "; ".join(problems) + "."
    return "Проверьте подключение к сети и API ключ."


@dataclass
class _ToolTracker:
    queue: asyncio.Queue
    _current_tool: str | None = field(default=None, init=False)
    _current_index: int | None = field(default=None, init=False)
    _tool_start_time: float = field(default=0.0, init=False)
    _input_chunks: list[str] = field(default_factory=list, init=False)
    _thinking_sent: bool = field(default=False, init=False)
    _tool_id_to_name: dict[str, str] = field(default_factory=dict, init=False)

    async def _put(self, payload: dict) -> None:
        await self.queue.put(f"data: {safe_json_dumps(payload, ensure_ascii=False)}\n\n")
        await asyncio.sleep(0)

    async def on_first_event(self) -> None:
        if not self._thinking_sent:
            self._thinking_sent = True
            await self._put({"type": "thinking"})

    async def on_tool_start(self, name: str, index: int, tool_use_id: str = "") -> None:
        self._current_tool = name
        self._current_index = index
        self._input_chunks = []
        self._tool_start_time = time.monotonic()
        if tool_use_id:
            self._tool_id_to_name[tool_use_id] = name
        await self._put({"type": "tool_start", "tool": name})

    def accumulate_input(self, chunk: str) -> None:
        self._input_chunks.append(chunk)

    async def on_block_stop(self, index: int) -> None:
        if self._current_tool is not None and self._current_index == index:
            duration = round(time.monotonic() - self._tool_start_time, 1)
            args_raw = "".join(self._input_chunks)
            try:
                args = json.loads(args_raw) if args_raw else {}
            except json.JSONDecodeError:
                args = {}
            summary = _summarize_tool_args(args)
            await self._put({
                "type": "tool_end",
                "tool": self._current_tool,
                "duration": duration,
                "is_error": False,
                "summary": summary,
            })
            self._current_tool = None
            self._current_index = None

    async def on_tool_result(self, tool_use_id: str, content: str | None, is_error: bool) -> None:
        tool_name = self._tool_id_to_name.get(tool_use_id, "tool")
        summary = _truncate(content or "", 120) if content else ""
        await self._put({
            "type": "tool_result",
            "tool": tool_name,
            "is_error": is_error,
            "summary": summary,
        })

    async def on_status(self, text: str) -> None:
        await self._put({"type": "status", "text": text})


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


async def _await_with_countdown(
    coro: Awaitable,
    timeout: float,
    queue: asyncio.Queue,
    label: str,
    countdown_interval: int = 10,
    activity_ts: list[float] | None = None,
    activity_extend: float = 30.0,
    max_timeout: float | None = None,
    api_request_ts: list[float] | None = None,
) -> Any:
    """Await *coro* with periodic countdown status updates pushed to *queue*.

    The coroutine is wrapped in ``ensure_future`` + ``asyncio.wait`` to enforce
    a hard deadline.  When *activity_ts* (a mutable ``[float]``) is provided,
    the deadline is automatically extended by *activity_extend* seconds when
    fresh SDK activity (text, tools) is detected and the deadline is close.
    *max_timeout* is a hard ceiling — the deadline will never exceed
    ``start + max_timeout``.

    *api_request_ts* tracks the last [api:request] stderr timestamp; when set,
    the ticker shows "Думает..." if Claude API is processing for >15s.
    """
    # We must wrap coro in a separate Task for timeout to work.
    # claude_agent_sdk swallows CancelledError internally (anyio task groups),
    # so asyncio.timeout() on the same task is useless — the cancellation
    # never propagates out.  Using ensure_future + asyncio.wait is the only
    # way to enforce a hard deadline.
    #
    # NOTE: this re-introduces the "cancel scope in different task" risk,
    # but only on the TIMEOUT path (task.cancel).  The happy path (task
    # completes normally) is unaffected because the result is returned
    # before any cancel-scope cleanup.
    task = asyncio.ensure_future(coro)
    start = time.monotonic()
    deadline = start + timeout
    if max_timeout is not None:
        hard_ceiling = start + max_timeout
    else:
        hard_ceiling = deadline

    thinking_delay = 15  # seconds after [api:request] before showing "Думает..."

    async def _ticker() -> None:
        nonlocal deadline
        try:
            last_extended_at = start
            extensions_remaining = 3
            _thinking_shown = False
            while True:
                await asyncio.sleep(countdown_interval)
                now = time.monotonic()
                # Extend deadline when real SDK activity is fresh AND deadline is close.
                if (
                    extensions_remaining > 0
                    and activity_ts is not None
                    and activity_ts[0] > last_extended_at
                ):
                    remaining = deadline - now
                    if remaining < activity_extend:
                        new_deadline = min(now + activity_extend, hard_ceiling)
                        if new_deadline > deadline:
                            deadline = new_deadline
                            last_extended_at = activity_ts[0]
                            extensions_remaining -= 1
                            logger.info(
                                "Timeout extended +%.0fs (SDK activity, was %.0fs left, %d extensions left)",
                                activity_extend, remaining, extensions_remaining,
                            )
                            ext_payload = safe_json_dumps(
                                {
                                    "type": "status",
                                    "text": f"Агент активен — продлён (+{int(activity_extend)}с)",
                                },
                                ensure_ascii=False,
                            )
                            try:
                                queue.put_nowait(f"data: {ext_payload}\n\n")
                            except Exception:
                                pass
                # Show "Думает..." when API request was sent but no SDK events yet
                if (
                    not _thinking_shown
                    and api_request_ts is not None
                    and api_request_ts[0] > 0
                    and (now - api_request_ts[0]) > thinking_delay
                ):
                    _thinking_shown = True
                    thinking_payload = safe_json_dumps(
                        {"type": "thinking", "text": "Думает..."},
                        ensure_ascii=False,
                    )
                    try:
                        queue.put_nowait(f"data: {thinking_payload}\n\n")
                    except Exception:
                        pass
                # Show countdown
                remaining_int = int(deadline - time.monotonic())
                if remaining_int > 0:
                    payload = safe_json_dumps(
                        {"type": "countdown", "text": f"{label} ({remaining_int}с до таймаута)"},
                        ensure_ascii=False,
                    )
                    try:
                        queue.put_nowait(f"data: {payload}\n\n")
                    except Exception:
                        pass
        except asyncio.CancelledError:
            pass

    ticker = asyncio.create_task(_ticker())
    try:
        while True:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                task.cancel()
                # SDK (anyio task groups) swallows CancelledError internally,
                # so `await task` would block forever.  Give it a short grace
                # period and proceed regardless — the task becomes a zombie but
                # the TimeoutError propagates correctly to the caller.
                await asyncio.wait({task}, timeout=5.0)
                raise asyncio.TimeoutError()
            done, _ = await asyncio.wait({task}, timeout=min(countdown_interval, remaining))
            if done:
                return task.result()
    except asyncio.CancelledError:
        task.cancel()
        await asyncio.wait({task}, timeout=5.0)
        raise
    finally:
        ticker.cancel()
        with suppress(asyncio.CancelledError):
            await ticker


async def _as_prompt_stream(text: str) -> AsyncIterator[dict]:
    """Wrap a string prompt as a single-message async iterable.

    claude-agent-sdk blocks the generator on string prompts until the entire
    conversation completes (wait_for_result_and_end_input).  Using an
    AsyncIterable makes the SDK spawn stream_input in a background task,
    allowing receive_messages to yield events immediately.
    """
    yield {
        "type": "user",
        "session_id": "",
        "message": {"role": "user", "content": text},
        "parent_tool_use_id": None,
    }


async def _auto_approve_tool(
    tool_name: str, tool_input: dict, context: ToolPermissionContext,
) -> PermissionResultAllow:
    """Auto-approve all CLI tool permission requests.

    Claude CLI sends can_use_tool control requests for tools that access the
    network (read_messages, send_message, etc.).  Without this callback the
    SDK raises "canUseTool callback is not provided" and the tool silently
    fails with "Tool permission stream closed before response received".
    We manage permissions through our own PermissionGate instead.
    """
    return PermissionResultAllow()


class ClaudeSdkBackend:
    def __init__(self, db: Database, config: AppConfig, client_pool=None, scheduler_manager=None) -> None:
        self._db = db
        self._config = config
        self._client_pool = client_pool
        self._scheduler_manager = scheduler_manager
        self._server = None
        self._initialized = False

    def initialize(self) -> None:
        self._ensure_initialized()

    def _ensure_initialized(self) -> None:
        if self._initialized:
            return
        from src.agent.tools import make_mcp_server

        # stream_close_timeout must be >= total_timeout: the CLI needs stdin open
        # for the entire conversation to receive MCP tool-call responses.
        effective = max(self._config.agent.stream_close_timeout, self._config.agent.total_timeout)
        os.environ.setdefault("CLAUDE_CODE_STREAM_CLOSE_TIMEOUT", str(effective * 1000))
        self._server = make_mcp_server(
            self._db, client_pool=self._client_pool, scheduler_manager=self._scheduler_manager,
            config=self._config,
        )
        self._initialized = True
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
        session_id: str = "web",
    ) -> None:
        self._ensure_initialized()
        original_prompt = prompt
        if history_msgs:
            prompt = _embed_history_in_prompt(history_msgs, prompt)
        resolved_model = model or self._config.agent.model.strip() or os.environ.get("AGENT_MODEL")
        extra: dict = {}
        if resolved_model:
            extra["model"] = resolved_model
        stderr_lines: list[str] = []
        debug_lines: list[str] = []
        # Heartbeat: updated when real SDK events arrive (text, tools, results).
        # _await_with_countdown checks this to extend the deadline while the agent
        # is making actual progress.
        _last_activity: list[float] = [time.monotonic()]
        # Timestamp of the last [api:request] stderr event — used to show
        # "Думает..." status when Claude API is processing for a long time.
        _api_request_ts: list[float] = [0.0]
        _api_request_count: list[int] = [0]
        # Last rate limit status — included in timeout error message for diagnostics.
        _last_rate_limit: list[str] = [""]

        # Stable stage keywords from claude-cli bootstrap sequence.
        # Checked case-insensitively; first match wins.
        _prompt_short = original_prompt[:100].replace("\n", " ")
        if len(original_prompt) > 100:
            _prompt_short += "…"
        # stderr stage labels — none extend timeouts (_last_activity is updated
        # only when real SDK events arrive: text deltas, tool results, etc.)
        # NOTE: [api:request] label is dynamic (includes counter), handled separately.
        _stage_map: list[tuple[str, str]] = [
            ("creating client", "Создание клиента"),
            ("installplugins", "Установка плагинов"),
            ("refreshed marketplace", "Обновление плагинов"),
            ("hooks:", "Загрузка хуков"),
            ("lsp server", "LSP"),
            ("settings changed", "Применение настроек"),
            ("rate limit event", "Rate limit"),
        ]
        _last_emitted: list[str] = [""]

        def _on_stderr(line: str) -> None:
            # claude-cli stderr format: "2026-03-30T01:28:36.888Z [DEBUG] ..."
            # Level tag is NOT at the start — check anywhere in the line.
            _lower = line.lower()
            is_error = False
            if "[debug]" in _lower or "[trace]" in _lower:
                debug_lines.append(line)
                logger.debug("claude-cli debug: %s", line)
            elif "[warn" in _lower:
                debug_lines.append(line)
                logger.warning("claude-cli warn: %s", line)
                is_error = True
            elif "[error]" in _lower or _lower.startswith("error"):
                stderr_lines.append(line)
                logger.error("claude-cli error: %s", line)
                is_error = True
            else:
                stderr_lines.append(line)
                logger.warning("claude-cli stderr: %s", line)
                # Treat untagged stderr as potential errors (e.g. "Error: Invalid URL")
                if "error" in _lower:
                    is_error = True

            # Emit connection progress to TUI/web queue.
            # _on_stderr is called from an anyio task in the same event loop,
            # so put_nowait on asyncio.Queue is safe here.
            # [api:request] is handled separately — each occurrence is unique
            # (counter incremented) so it must not be deduped.
            if "[api:request]" in _lower:
                _api_request_count[0] += 1
                _api_request_ts[0] = time.monotonic()
                label = f"Жду ответ Claude API #{_api_request_count[0]}: «{_prompt_short}»"
                payload = safe_json_dumps({"type": "status", "text": label}, ensure_ascii=False)
                try:
                    queue.put_nowait(f"data: {payload}\n\n")
                except Exception:
                    pass
                return  # skip stage_map + error checks for api:request

            label: str | None = None
            for keyword, stage in _stage_map:
                if keyword in _lower:
                    label = stage
                    break
            if label and label != _last_emitted[0]:
                _last_emitted[0] = label
                payload = safe_json_dumps({"type": "status", "text": label}, ensure_ascii=False)
                try:
                    queue.put_nowait(f"data: {payload}\n\n")
                except Exception:
                    pass

            # Surface errors/warnings to user — don't swallow them silently.
            if is_error and not label:
                # Strip timestamp prefix (ISO format) for cleaner display
                display = line.strip()
                if len(display) > 25 and display[10] == "T" and display[23] == "Z":
                    display = display[25:].strip()
                # Remove level tags for cleaner output
                for tag in ("[WARN]", "[warn]", "[ERROR]", "[error]"):
                    display = display.replace(tag, "").strip()
                if display:
                    warn_payload = safe_json_dumps(
                        {"type": "warning", "text": display},
                        ensure_ascii=False,
                    )
                    try:
                        queue.put_nowait(f"data: {warn_payload}\n\n")
                    except Exception:
                        pass

        cli_path = shutil.which("claude")
        logger.info("claude-cli path: %s", cli_path)

        from src.agent.tools.permissions import (
            BUILTIN_TOOLS,
            MCP_PREFIX,
            filter_allowed_tools,
            get_all_allowed_tools,
            load_tool_permissions_union,
        )

        all_tools = get_all_allowed_tools()
        permissions = await load_tool_permissions_union(self._db, use_cache=True)
        # When a PermissionGate is active (TUI/web mode), pass ALL tools to the LLM —
        # the gate intercepts restricted calls at runtime instead of hiding them.
        from src.agent.permission_gate import get_gate

        if get_gate() is not None:
            allowed = all_tools
            logger.debug("Agent tools: gate active, all %d tools visible to LLM", len(all_tools))
        else:
            allowed = filter_allowed_tools(all_tools, permissions)
            if len(allowed) < len(all_tools):
                denied = [t.removeprefix(MCP_PREFIX) for t in all_tools if t not in allowed]
                logger.debug("Agent tools: %d/%d allowed, denied: %s", len(allowed), len(all_tools), denied[:20])
            else:
                logger.debug("Agent tools: all %d tools allowed", len(allowed))

        enabled_builtins = [t for t in BUILTIN_TOOLS if t in allowed]

        options = ClaudeAgentOptions(
            system_prompt=system_prompt,
            mcp_servers={"telegram_db": self._server},
            tools=enabled_builtins or None,
            allowed_tools=allowed,
            cli_path=cli_path or None,
            stderr=_on_stderr,
            include_partial_messages=True,
            can_use_tool=_auto_approve_tool,
            extra_args={"debug-to-stderr": None},
            **extra,
        )

        cfg = self._config.agent
        last_err: Exception | None = None
        for attempt in range(2):
            if attempt > 0:
                tracker_retry = _ToolTracker(queue=queue)
                await tracker_retry.on_status("Повтор подключения к Claude...")
            tracker = _ToolTracker(queue=queue)
            draining = False
            full_text = ""
            streamed = False
            t0 = time.monotonic()
            first_event_logged = False
            _api_request_count[0] = 0
            try:
                aiter = query(prompt=_as_prompt_stream(prompt), options=options).__aiter__()
                try:
                  while True:
                    if not first_event_logged:
                        iter_timeout = cfg.first_event_timeout
                        iter_label = "Ожидание ответа Claude"
                    else:
                        iter_timeout = cfg.idle_timeout
                        iter_label = "Ожидание данных от Claude"

                    elapsed = time.monotonic() - t0
                    remaining_total = cfg.total_timeout - elapsed
                    if remaining_total <= 0:
                        logger.error(
                            "Total timeout %ds exceeded after %.1fs (thread %d)",
                            cfg.total_timeout, elapsed, thread_id,
                        )
                        raise TimeoutError(
                            f"Общий таймаут запроса ({cfg.total_timeout}с)"
                        )
                    effective_timeout = min(iter_timeout, remaining_total)

                    try:
                        msg = await _await_with_countdown(
                            aiter.__anext__(),
                            timeout=effective_timeout,
                            queue=queue,
                            label=iter_label,
                            activity_ts=_last_activity,
                            max_timeout=remaining_total,
                            api_request_ts=_api_request_ts,
                        )
                    except StopAsyncIteration:
                        break
                    except asyncio.TimeoutError:
                        if not first_event_logged:
                            logger.error(
                                "First event timeout %ds fired after %.1fs (thread %d)",
                                cfg.first_event_timeout, time.monotonic() - t0, thread_id,
                            )
                            diag = _diagnose_connection(cli_path, stderr_lines)
                            raise TimeoutError(
                                f"Claude не ответил за {cfg.first_event_timeout}с. {diag}"
                            )
                        logger.error(
                            "Idle timeout %ds fired after %.1fs total (thread %d)",
                            cfg.idle_timeout, time.monotonic() - t0, thread_id,
                        )
                        reason = "Возможно, соединение потеряно."
                        if _last_rate_limit[0]:
                            reason = f"Последний rate limit: {_last_rate_limit[0]}."
                        raise TimeoutError(
                            f"Стрим Claude замолчал на {cfg.idle_timeout}с. {reason}"
                        )

                    if not first_event_logged:
                        elapsed_fe = time.monotonic() - t0
                        logger.info(
                            "First SDK event after %.1fs (thread %d, attempt %d): %s",
                            elapsed_fe, thread_id, attempt + 1, type(msg).__name__,
                        )
                        first_event_logged = True
                    if draining:
                        continue
                    try:
                        if isinstance(msg, RateLimitEvent):
                            info = msg.rate_limit_info
                            rl_status = info.status if info else "unknown"
                            resets = info.resets_at if info else None
                            utilization = info.utilization if info else None
                            logger.warning(
                                "Rate limit event (thread %d): status=%s, resets_at=%s, utilization=%s",
                                thread_id, rl_status, resets, utilization,
                            )
                            rl_parts = [rl_status]
                            if utilization is not None:
                                rl_parts.append(f"{utilization:.0%}")
                            rl_summary = ", ".join(rl_parts)
                            rl_text = f"Rate limit: {rl_summary}"
                            _last_rate_limit[0] = rl_summary
                            if rl_status == "rejected":
                                # Hard reject — surface as warning, not just status
                                warn_payload = safe_json_dumps(
                                    {"type": "warning", "text": f"⛔ {rl_text}. API отклоняет запросы."},
                                    ensure_ascii=False,
                                )
                                await queue.put(f"data: {warn_payload}\n\n")
                            else:
                                await tracker.on_status(rl_text)
                        elif isinstance(msg, StreamEvent):
                            event = msg.event
                            event_type = event.get("type")
                            await tracker.on_first_event()

                            if event_type == "content_block_start":
                                block = event.get("content_block", {})
                                block_type = block.get("type")
                                if block_type == "tool_use":
                                    _last_activity[0] = time.monotonic()
                                    await tracker.on_tool_start(
                                        block.get("name", "unknown"),
                                        event.get("index", 0),
                                        tool_use_id=block.get("id", ""),
                                    )
                                else:
                                    logger.debug(
                                        "content_block_start type=%s (thread %d)",
                                        block_type, thread_id,
                                    )

                            elif event_type == "content_block_delta":
                                delta = event.get("delta", {})
                                delta_type = delta.get("type")
                                if delta_type == "text_delta":
                                    text_chunk = delta.get("text", "")
                                    if text_chunk:
                                        _last_activity[0] = time.monotonic()
                                        full_text += text_chunk
                                        streamed = True
                                        chunk_payload = safe_json_dumps(
                                            {"text": text_chunk}, ensure_ascii=False
                                        )
                                        await queue.put(f"data: {chunk_payload}\n\n")
                                        await asyncio.sleep(0)
                                elif delta_type == "input_json_delta":
                                    _last_activity[0] = time.monotonic()
                                    tracker.accumulate_input(delta.get("partial_json", ""))

                            elif event_type == "content_block_stop":
                                _last_activity[0] = time.monotonic()
                                await tracker.on_block_stop(event.get("index", 0))

                            elif event_type == "error":
                                error_info = event.get("error", {})
                                api_error_type = error_info.get("type", "unknown")
                                api_error_msg = error_info.get("message", str(error_info))
                                logger.warning(
                                    "API stream error event (thread %d): type=%s message=%s",
                                    thread_id, api_error_type, api_error_msg,
                                )
                                if api_error_type == "overloaded_error":
                                    raise CLIConnectionError(f"API overloaded: {api_error_msg}")
                                raise ClaudeSDKError(f"{api_error_type}: {api_error_msg}")

                        elif isinstance(msg, AssistantMessage):
                            _last_activity[0] = time.monotonic()
                            for _idx, block in enumerate(msg.content):
                                if isinstance(block, TextBlock) and not streamed:
                                    full_text += block.text
                                    chunk_payload = safe_json_dumps(
                                        {"text": block.text}, ensure_ascii=False
                                    )
                                    await queue.put(f"data: {chunk_payload}\n\n")
                                elif isinstance(block, ToolUseBlock):
                                    # SDK delivers tool calls via AssistantMessage (not
                                    # StreamEvent content_block_start), so we must
                                    # manually emit tool_start / tool_end events here.
                                    await tracker.on_tool_start(
                                        block.name, _idx, tool_use_id=block.id
                                    )
                                    tracker.accumulate_input(
                                        safe_json_dumps(block.input or {}, ensure_ascii=False)
                                    )
                                    await tracker.on_block_stop(_idx)
                                elif isinstance(block, ToolResultBlock):
                                    content = block.content if isinstance(block.content, str) else ""
                                    await tracker.on_tool_result(
                                        block.tool_use_id, content, bool(block.is_error)
                                    )
                        elif isinstance(msg, UserMessage):
                            # Tool results sent back to Claude — real progress.
                            _last_activity[0] = time.monotonic()
                        elif isinstance(msg, ResultMessage):
                            done_data: dict = {
                                "done": True,
                                "full_text": full_text,
                                "backend": "claude",
                            }
                            _usage = getattr(msg, "usage", None)
                            if isinstance(_usage, dict):
                                done_data["usage"] = _usage
                            _model_usage = getattr(msg, "model_usage", None)
                            if isinstance(_model_usage, dict):
                                done_data["model_usage"] = _model_usage
                            _cost = getattr(msg, "total_cost_usd", None)
                            if isinstance(_cost, (int, float)):
                                done_data["total_cost_usd"] = _cost
                            _turns = getattr(msg, "num_turns", None)
                            if isinstance(_turns, int) and _turns > 0:
                                done_data["num_turns"] = _turns
                            _sid = getattr(msg, "session_id", None)
                            if isinstance(_sid, str) and _sid:
                                done_data["session_id"] = _sid
                            done_payload = safe_json_dumps(done_data, ensure_ascii=False)
                            await queue.put(f"data: {done_payload}\n\n")
                        else:
                            logger.warning(
                                "Unhandled SDK message type: %s (thread %d)",
                                type(msg).__name__, thread_id,
                            )
                    except asyncio.CancelledError:
                        draining = True
                finally:
                    with suppress(Exception):
                        await aiter.aclose()
                return

            except TimeoutError as exc:
                elapsed = time.monotonic() - t0
                logger.error(
                    "Agent timeout after %.1fs (thread %d): %s", elapsed, thread_id, exc,
                )
                stderr_summary = "\n".join(stderr_lines[-10:]) if stderr_lines else None
                err_payload = safe_json_dumps(
                    {"error": str(exc), "details": stderr_summary},
                    ensure_ascii=False,
                )
                await queue.put(f"data: {err_payload}\n\n")
                await queue.put(None)
                return

            except CLINotFoundError as exc:
                logger.error("Claude CLI not found (thread %d): %s", thread_id, exc)
                err_msg = (
                    "Claude CLI не найден. "
                    "Установите: npm install -g @anthropic-ai/claude-code"
                )
                await queue.put(
                    f"data: {safe_json_dumps({'error': err_msg}, ensure_ascii=False)}\n\n"
                )
                await queue.put(None)
                return

            except ProcessError as exc:
                logger.error(
                    "Claude CLI process error (thread %d): exit_code=%s, stderr=%s",
                    thread_id, exc.exit_code, exc.stderr,
                )
                # exc.stderr is often generic; captured stderr_lines have the real output
                captured = "\n".join(stderr_lines[-20:]) if stderr_lines else None
                details = captured or exc.stderr or str(exc)
                # Truncate long stderr for UI
                if len(details) > 500:
                    details = details[:500] + "..."
                err_payload = safe_json_dumps(
                    {"error": "Ошибка процесса Claude CLI", "details": details},
                    ensure_ascii=False,
                )
                await queue.put(f"data: {err_payload}\n\n")
                await queue.put(None)
                return

            except CLIConnectionError as exc:
                elapsed = time.monotonic() - t0
                logger.error(
                    "Claude CLI connection error after %.1fs (thread %d): %s",
                    elapsed, thread_id, exc,
                )
                if attempt == 0:
                    last_err = exc
                    continue
                stderr_summary = "\n".join(stderr_lines[-10:]) if stderr_lines else str(exc)
                conn_msg = (
                    "Сервер Anthropic перегружен, попробуйте позже."
                    if "overloaded" in str(exc).lower()
                    else "Не удалось подключиться к Claude CLI"
                )
                err_payload = safe_json_dumps(
                    {"error": conn_msg, "details": stderr_summary},
                    ensure_ascii=False,
                )
                await queue.put(f"data: {err_payload}\n\n")
                await queue.put(None)
                return

            except ClaudeSDKError as exc:
                elapsed = time.monotonic() - t0
                logger.error(
                    "Claude SDK error after %.1fs (thread %d): %s", elapsed, thread_id, exc,
                )
                stderr_summary = "\n".join(stderr_lines[-10:]) if stderr_lines else ""
                err_payload = safe_json_dumps(
                    {
                        "error": f"Ошибка Claude SDK: {exc}",
                        "details": stderr_summary or None,
                    },
                    ensure_ascii=False,
                )
                await queue.put(f"data: {err_payload}\n\n")
                await queue.put(None)
                return

            except BaseExceptionGroup as eg:
                # anyio TaskGroup wraps sub-exceptions in ExceptionGroup;
                # unwrap for a readable error message and retry on transient failures.
                flat = [str(e) for e in eg.exceptions]
                summary = "; ".join(flat)
                elapsed = time.monotonic() - t0
                # Never retry if the group contains a timeout — it's our deadline, not transient.
                has_timeout = any(isinstance(e, (TimeoutError, asyncio.TimeoutError)) for e in eg.exceptions)
                retryable = not has_timeout and any(
                    kw in summary.lower()
                    for kw in ("stream closed", "control request timeout", "connection")
                )
                logger.error(
                    "ExceptionGroup after %.1fs (thread %d, attempt %d, retryable=%s): %s",
                    elapsed, thread_id, attempt + 1, retryable, summary,
                )
                if attempt == 0 and retryable:
                    last_err = eg
                    continue
                last_err = Exception(summary)
                break

            except Exception as exc:
                elapsed = time.monotonic() - t0
                if attempt == 0 and "Control request timeout" in str(exc):
                    logger.warning(
                        "Agent init timeout after %.1fs, retrying (thread %d)",
                        elapsed, thread_id,
                    )
                    if tracker._current_tool is not None:
                        await tracker._put({
                            "type": "tool_end",
                            "tool": tracker._current_tool,
                            "duration": 0,
                            "is_error": True,
                            "summary": "timeout",
                        })
                    last_err = exc
                    continue
                last_err = exc
                break

        if debug_lines:
            logger.debug(
                "claude-cli debug dump (thread %d, %d lines):\n%s",
                thread_id,
                len(debug_lines),
                "\n".join(debug_lines[-50:]),
            )
        if stderr_lines:
            logger.error(
                "claude-cli stderr dump (thread %d):\n%s",
                thread_id,
                "\n".join(stderr_lines),
            )
        if last_err is not None:
            if not stderr_lines:
                logger.error(
                    "claude-cli failed with no stderr (thread %d): %s. "
                    "Prompt was %d chars (~%dK tokens).",
                    thread_id,
                    last_err,
                    stats["prompt_chars"],
                    stats["prompt_chars"] // 4000,
                )
            # Send error details to user via SSE
            stderr_summary = "\n".join(stderr_lines[-10:]) if stderr_lines else ""
            err_payload = safe_json_dumps(
                {
                    "error": f"Ошибка агента: {last_err}",
                    "details": stderr_summary or None,
                },
                ensure_ascii=False,
            )
            await queue.put(f"data: {err_payload}\n\n")
            await queue.put(None)


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

    def _default_tools(self, permissions: dict[str, bool] | None = None) -> list[Callable]:
        """Return the tool set for deepagents backend, filtered by permissions."""
        from src.agent.tools.deepagents_sync import build_deepagents_tools

        all_tools = build_deepagents_tools(self._db, config=self._config)
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
        model_provider = provider
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

        if provider == "zai":
            model_provider = "anthropic"
            base_url = cfg.plain_fields.get("base_url", "").strip() or ZAI_DEFAULT_BASE_URL
            extra.pop("base_url", None)
            extra["anthropic_api_url"] = base_url
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
                agent = OllamaReActAgent(
                    base_url=str(extra.get("base_url", "http://localhost:11434")),
                    model=resolved_model_name,
                    tools=tools or self._default_tools(permissions=permissions),
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

            model = init_chat_model(
                model=resolved_model_name,
                model_provider=model_provider,
                **extra,
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
                tools=tools or self._default_tools(permissions=permissions),
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
                "; ".join(errors) if errors else "Deepagents providers are not configured."
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
        permissions: dict[str, bool] | None = None,
    ) -> str:
        # Append available tools list to system prompt so models without
        # native function calling still know which tools exist.
        filtered_tools = self._default_tools(permissions=permissions)
        tool_names = [t.__name__ for t in filtered_tools]
        if tool_names:
            system_prompt += (
                "\n\nУ тебя есть доступ к следующим инструментам (tools). "
                "Используй их для выполнения задач:\n" + "\n".join(f"- {n}" for n in tool_names)
            )
        agent = self._build_agent(cfg, system_prompt=system_prompt, permissions=permissions)
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
    ) -> None:
        del thread_id, stats, model

        from src.agent.tools.permissions import load_tool_permissions_union

        permissions = await load_tool_permissions_union(self._db)
        enabled_count = sum(1 for v in permissions.values() if v)
        logger.info(
            "Deepagents tool permissions: %d/%d enabled",
            enabled_count, len(permissions),
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
                    permissions=permissions,
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
                errors.append(f"{cfg.provider}: {exc}")
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
                errors.append(f"{cfg.provider}: {exc}")
                logger.warning("Deepagents researcher-writer failed (%s): %s", cfg.provider, exc)

        self._init_error = (
            "; ".join(errors) if errors else "Deepagents providers are not configured."
        )
        raise RuntimeError(self._init_error)


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
        self, db: Database, config: AppConfig | None = None, client_pool=None, scheduler_manager=None,
    ) -> None:
        self._db = db
        self._config = config or AppConfig()
        self._claude_backend = ClaudeSdkBackend(
            db, self._config, client_pool=client_pool, scheduler_manager=scheduler_manager,
        )
        self._deepagents_backend = DeepagentsBackend(db, self._config)
        self._active_tasks: dict[int, asyncio.Task] = {}
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
        self, thread_id: int, message: str, model: str | None = None, session_id: str = "web",
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
            err_payload = safe_json_dumps(
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
            err_payload = safe_json_dumps(
                {"error": "Ошибка агента: не удалось выбрать backend."},
                ensure_ascii=False,
            )
            yield f"data: {err_payload}\n\n"
            return

        queue: asyncio.Queue[str | None] = asyncio.Queue()

        # Capture gate state and compute DB permissions before spawning the task.
        # The ContextVar token must be created AND reset inside the same asyncio task,
        # so the actual set/reset happens inside _run_backend (not in the generator).
        from src.agent.permission_gate import (
            AgentRequestContext,
            get_gate,
            reset_request_context,
            set_request_context,
        )

        _gate = get_gate()
        _req_ctx: AgentRequestContext | None = None
        if _gate is not None:
            from src.agent.tools.permissions import load_tool_permissions_union

            _db_perms = await load_tool_permissions_union(self._db, use_cache=True)
            _req_ctx = AgentRequestContext(
                session_id=session_id,
                thread_id=thread_id,
                queue=queue,
                db_permissions=_db_perms,
                permission_timeout=self._config.agent.permission_timeout,
            )

        async def _run_backend(
            selected_backend: ClaudeSdkBackend | DeepagentsBackend,
            failure_prefix: Callable[[str], str],
        ) -> None:
            # Set ContextVar here so token is created and reset in the same task context.
            _token = set_request_context(_req_ctx) if _req_ctx is not None else None
            try:
                kwargs: dict = dict(
                    thread_id=thread_id,
                    prompt=prompt,
                    system_prompt=system_prompt,
                    stats=stats,
                    model=model,
                    queue=queue,
                    history_msgs=history_for_backend,
                )
                if isinstance(selected_backend, ClaudeSdkBackend):
                    kwargs["session_id"] = session_id
                await selected_backend.chat_stream(**kwargs)
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

                err_payload = safe_json_dumps(
                    {"error": failure_prefix(error_text)},
                    ensure_ascii=False,
                )
                await queue.put(f"data: {err_payload}\n\n")
            finally:
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

        def _cleanup(t: asyncio.Task) -> None:
            if self._active_tasks.get(thread_id) is t:
                del self._active_tasks[thread_id]

        task.add_done_callback(_cleanup)

        # Immediate feedback before backend connects (can take 10-30s)
        init_payload = safe_json_dumps(
            {"type": "status", "text": f"Подключение к {backend_name}..."},
            ensure_ascii=False,
        )
        yield f"data: {init_payload}\n\n"

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
        if tasks:
            _, pending = await asyncio.wait(tasks, timeout=5.0)
            for task in pending:
                logger.debug("Agent task did not finish within timeout: %s", task.get_name())
