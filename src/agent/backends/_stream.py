from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from collections.abc import AsyncIterator, Awaitable
from contextlib import suppress
from dataclasses import dataclass, field
from typing import Any

from claude_agent_sdk import (
    AssistantMessage,
    ClaudeSDKError,
    CLIConnectionError,
    PermissionResultAllow,
    PermissionResultDeny,
    RateLimitEvent,
    ResultMessage,
    StreamEvent,
    TextBlock,
    ToolPermissionContext,
    ToolResultBlock,
    ToolUseBlock,
    UserMessage,
)

from src.utils.json import safe_json_dumps

logger = logging.getLogger(__name__)


def _embed_history_in_prompt(history_msgs: list[dict], message: str) -> str:
    """Build a single prompt string with conversation history and current message."""
    parts: list[str] = []
    for msg in history_msgs:
        tag = "user" if msg["role"] == "user" else "assistant"
        parts.append(f"<{tag}>\n{msg['content']}\n</{tag}>")
    parts.append(f"<user>\n{message}\n</user>")
    return "\n".join(parts)

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


def _sse(payload: dict) -> str:
    """Serialize a payload dict to an SSE `data:` frame (UTF-8 preserved)."""
    return f"data: {safe_json_dumps(payload, ensure_ascii=False)}\n\n"


async def _emit_error(queue: asyncio.Queue, message: str, details: str | None = None) -> None:
    """Push an `{error, details}` SSE frame followed by the stream-end sentinel."""
    await queue.put(_sse({"error": message, "details": details}))
    await queue.put(None)


class _ChatStreamState:
    """Mutable accumulator for one `chat_stream` run, threaded through
    `_dispatch_stream_message` so per-message handlers update the running
    assistant text in place instead of mutating `chat_stream` closure locals."""

    __slots__ = ("full_text", "streamed")

    def __init__(self) -> None:
        self.full_text = ""
        self.streamed = False


async def _dispatch_rate_limit_event(
    msg: RateLimitEvent,
    *,
    tracker: _ToolTracker,
    queue: asyncio.Queue,
    last_rate_limit: list[str],
    thread_id: int,
) -> None:
    info = msg.rate_limit_info
    rl_status = info.status if info else "unknown"
    resets = info.resets_at if info else None
    utilization = info.utilization if info else None
    logger.warning(
        "Rate limit event (thread %d): status=%s, resets_at=%s, utilization=%s",
        thread_id, rl_status, resets, utilization,
    )
    rl_parts: list[str] = [rl_status]
    if utilization is not None:
        rl_parts.append(f"{utilization:.0%}")
    rl_summary = ", ".join(rl_parts)
    rl_text = f"Rate limit: {rl_summary}"
    last_rate_limit[0] = rl_summary
    if rl_status == "rejected":
        # Hard reject — surface as warning, not just status
        await queue.put(_sse({"type": "warning", "text": f"⛔ {rl_text}. API отклоняет запросы."}))
    else:
        await tracker.on_status(rl_text)


async def _dispatch_content_block_start(
    event: dict[str, Any],
    *,
    tracker: _ToolTracker,
    last_activity: list[float],
    thread_id: int,
) -> None:
    block = event.get("content_block", {})
    block_type = block.get("type")
    if block_type == "tool_use":
        last_activity[0] = time.monotonic()
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


async def _dispatch_content_block_delta(
    event: dict[str, Any],
    *,
    tracker: _ToolTracker,
    queue: asyncio.Queue,
    state: _ChatStreamState,
    last_activity: list[float],
) -> None:
    delta = event.get("delta", {})
    delta_type = delta.get("type")
    if delta_type == "text_delta":
        text_chunk = delta.get("text", "")
        if text_chunk:
            last_activity[0] = time.monotonic()
            state.full_text += text_chunk
            state.streamed = True
            await queue.put(_sse({"text": text_chunk}))
            await asyncio.sleep(0)
    elif delta_type == "input_json_delta":
        last_activity[0] = time.monotonic()
        tracker.accumulate_input(delta.get("partial_json", ""))


async def _dispatch_stream_error_event(
    event: dict[str, Any],
    *,
    thread_id: int,
) -> None:
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


async def _dispatch_stream_event(
    msg: StreamEvent,
    *,
    tracker: _ToolTracker,
    queue: asyncio.Queue,
    state: _ChatStreamState,
    last_activity: list[float],
    thread_id: int,
) -> None:
    event = msg.event
    event_type = event.get("type")
    await tracker.on_first_event()

    if event_type == "content_block_start":
        await _dispatch_content_block_start(
            event,
            tracker=tracker,
            last_activity=last_activity,
            thread_id=thread_id,
        )
    elif event_type == "content_block_delta":
        await _dispatch_content_block_delta(
            event,
            tracker=tracker,
            queue=queue,
            state=state,
            last_activity=last_activity,
        )
    elif event_type == "content_block_stop":
        last_activity[0] = time.monotonic()
        await tracker.on_block_stop(event.get("index", 0))
    elif event_type == "error":
        await _dispatch_stream_error_event(event, thread_id=thread_id)


async def _dispatch_assistant_message(
    msg: AssistantMessage,
    *,
    tracker: _ToolTracker,
    queue: asyncio.Queue,
    state: _ChatStreamState,
    last_activity: list[float],
) -> None:
    last_activity[0] = time.monotonic()
    for _idx, block in enumerate(msg.content):
        if isinstance(block, TextBlock) and not state.streamed:
            state.full_text += block.text
            await queue.put(_sse({"text": block.text}))
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


async def _dispatch_result_message(
    msg: ResultMessage,
    *,
    queue: asyncio.Queue,
    state: _ChatStreamState,
) -> None:
    done_data: dict = {
        "done": True,
        "full_text": state.full_text,
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
    await queue.put(_sse(done_data))


async def _dispatch_stream_message(
    msg,
    *,
    tracker: _ToolTracker,
    queue: asyncio.Queue,
    state: _ChatStreamState,
    last_activity: list[float],
    last_rate_limit: list[str],
    thread_id: int,
) -> None:
    """Handle one SDK stream message: emit its SSE frames, drive the tool
    tracker, and accumulate assistant text on `state`. Raises CLIConnectionError
    / ClaudeSDKError for API stream-error events (the caller's retry loop owns
    them); asyncio.CancelledError propagates for the caller's draining handling."""
    if isinstance(msg, RateLimitEvent):
        await _dispatch_rate_limit_event(
            msg,
            tracker=tracker,
            queue=queue,
            last_rate_limit=last_rate_limit,
            thread_id=thread_id,
        )
    elif isinstance(msg, StreamEvent):
        await _dispatch_stream_event(
            msg,
            tracker=tracker,
            queue=queue,
            state=state,
            last_activity=last_activity,
            thread_id=thread_id,
        )
    elif isinstance(msg, AssistantMessage):
        await _dispatch_assistant_message(
            msg,
            tracker=tracker,
            queue=queue,
            state=state,
            last_activity=last_activity,
        )
    elif isinstance(msg, UserMessage):
        # Tool results sent back to Claude — real progress.
        last_activity[0] = time.monotonic()
    elif isinstance(msg, ResultMessage):
        await _dispatch_result_message(msg, queue=queue, state=state)
    else:
        logger.warning(
            "Unhandled SDK message type: %s (thread %d)",
            type(msg).__name__, thread_id,
        )


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
) -> PermissionResultAllow | PermissionResultDeny:
    """Auto-approve CLI tool permission requests handled by our gates.

    Claude CLI sends can_use_tool control requests for tools that access the
    network (read_messages, send_message, etc.).  Without this callback the
    SDK raises "canUseTool callback is not provided" and the tool silently
    fails with "Tool permission stream closed before response received".
    MCP tools are still auto-approved here because our MCP wrappers perform
    the actual runtime permission checks. Built-in SDK tools do not use those
    wrappers, so requestable/denied built-ins must be handled here.
    """
    del tool_input, context

    from src.agent.permission_gate import get_gate, get_request_context
    from src.agent.tools.permissions import BUILTIN_TOOLS, ToolAccessState

    if tool_name not in BUILTIN_TOOLS:
        return PermissionResultAllow()

    ctx = get_request_context()
    access_policy = ctx.tool_access_policy if ctx is not None else None
    state = access_policy.get(tool_name) if access_policy is not None else None
    if state is None or state == ToolAccessState.ALLOWED:
        return PermissionResultAllow()
    if state == ToolAccessState.DENIED:
        return PermissionResultDeny(message=f"Инструмент '{tool_name}' не разрешён настройками агента.")

    gate = get_gate()
    if gate is None or ctx is None:
        return PermissionResultDeny(
            message=f"Инструмент '{tool_name}' требует интерактивное разрешение пользователя."
        )

    denied = await gate.check(tool_name, "")
    if denied is None:
        return PermissionResultAllow()

    message = f"Доступ к '{tool_name}' запрещён пользователем."
    content = denied.get("content") if isinstance(denied, dict) else None
    if isinstance(content, list):
        for item in content:
            if isinstance(item, dict) and isinstance(item.get("text"), str):
                message = item["text"]
                break
    return PermissionResultDeny(message=message)


# Stable stage keywords from the claude-cli bootstrap sequence, checked
# case-insensitively (first match wins). None of these extend timeouts —
# _last_activity is updated only on real SDK events. The dynamic [api:request]
# label (with its counter) is handled separately in _handle_stderr_line.
_STDERR_STAGE_MAP: list[tuple[str, str]] = [
    ("creating client", "Создание клиента"),
    ("installplugins", "Установка плагинов"),
    ("refreshed marketplace", "Обновление плагинов"),
    ("hooks:", "Загрузка хуков"),
    ("lsp server", "LSP"),
    ("settings changed", "Применение настроек"),
    ("rate limit event", "Rate limit"),
]
