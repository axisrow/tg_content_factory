from __future__ import annotations

import asyncio
import logging
import os
import shutil
import time
from contextlib import suppress
from typing import Any, Protocol, cast

from claude_agent_sdk import (
    ClaudeAgentOptions,
    ClaudeSDKError,
    CLIConnectionError,
    CLINotFoundError,
    ProcessError,
    query,
)

from src.agent.backends._stream import (
    _STDERR_STAGE_MAP,
    _as_prompt_stream,
    _auto_approve_tool,
    _await_with_countdown,
    _ChatStreamState,
    _diagnose_connection,
    _dispatch_stream_message,
    _embed_history_in_prompt,
    _emit_error,
    _sse,
    _ToolTracker,
)
from src.agent.runtime_context import AgentRuntimeContext
from src.config import AppConfig
from src.database import Database
from src.utils.json import safe_json_dumps

logger = logging.getLogger(__name__)


class _ClosableAsyncIterator(Protocol):
    def __aiter__(self) -> "_ClosableAsyncIterator": ...

    async def __anext__(self) -> object: ...

    async def aclose(self) -> None: ...


class ClaudeSdkBackend:
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

    def _handle_stderr_line(
        self,
        line: str,
        *,
        queue: asyncio.Queue,
        prompt_short: str,
        stderr_lines: list[str],
        debug_lines: list[str],
        api_request_count: list[int],
        api_request_ts: list[float],
        last_emitted: list[str],
    ) -> None:
        """Parse one claude-cli stderr line and emit progress/warning frames to
        the SSE queue. Extracted from ``chat_stream``'s nested ``_on_stderr``
        closure (#923); behavior identical, with the shared mutable state passed
        explicitly via the list-of-one holders."""
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
            api_request_count[0] += 1
            api_request_ts[0] = time.monotonic()
            request_label = f"Жду ответ Claude API #{api_request_count[0]}: «{prompt_short}»"
            payload = safe_json_dumps({"type": "status", "text": request_label}, ensure_ascii=False)
            try:
                queue.put_nowait(f"data: {payload}\n\n")
            except Exception:
                pass
            return  # skip stage_map + error checks for api:request

        label: str | None = None
        for keyword, stage in _STDERR_STAGE_MAP:
            if keyword in _lower:
                label = stage
                break
        if label and label != last_emitted[0]:
            last_emitted[0] = label
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

    async def _build_claude_options(
        self,
        *,
        system_prompt: str,
        cli_path: str | None,
        on_stderr,
        extra: dict,
    ) -> ClaudeAgentOptions:
        """Resolve the visible tool set (access policy + permission gate) and
        assemble the ``ClaudeAgentOptions`` for the query. Split out of
        ``chat_stream`` (#923)."""
        from src.agent.tools.permissions import (
            BUILTIN_TOOLS,
            MCP_PREFIX,
            get_all_allowed_tools,
            load_tool_access_policy,
            visible_tools_for_llm,
        )

        all_tools = get_all_allowed_tools()
        access_policy = await load_tool_access_policy(self._db, use_cache=True)
        # With PermissionGate active, requestable tools stay visible so the
        # runtime gate can ask; explicit denies remain hidden.
        from src.agent.permission_gate import get_gate, get_request_context

        gate_active = get_gate() is not None and get_request_context() is not None
        allowed = visible_tools_for_llm(all_tools, access_policy, gate_active=gate_active)
        if len(allowed) < len(all_tools):
            denied = [t.removeprefix(MCP_PREFIX) for t in all_tools if t not in allowed]
            logger.debug(
                "Agent tools: %d/%d visible (gate_active=%s), hidden: %s",
                len(allowed), len(all_tools), gate_active, denied[:20],
            )
        else:
            logger.debug("Agent tools: all %d tools visible (gate_active=%s)", len(allowed), gate_active)

        enabled_builtins = [t for t in BUILTIN_TOOLS if t in allowed]

        return ClaudeAgentOptions(
            system_prompt=system_prompt,
            mcp_servers={"telegram_db": cast(Any, self._server)},
            tools=enabled_builtins or None,
            allowed_tools=allowed,
            cli_path=cli_path or None,
            stderr=on_stderr,
            include_partial_messages=True,
            can_use_tool=_auto_approve_tool,
            extra_args={"debug-to-stderr": None},
            **extra,
        )

    async def _handle_stream_attempt_error(
        self,
        exc: BaseException,
        *,
        attempt: int,
        t0: float,
        thread_id: int,
        stderr_lines: list[str],
        tracker: "_ToolTracker",
        queue: asyncio.Queue,
    ) -> tuple[BaseException | None, str]:
        """Handle a failure from one ``chat_stream`` attempt. Returns
        ``(last_err, action)`` where ``action`` is ``"return"`` (error already
        emitted — stop), ``"retry"`` (continue the attempt loop) or ``"break"``
        (stop the loop, report ``last_err`` in the final block). The isinstance
        chain mirrors the original except-clause order exactly. Split out of
        ``chat_stream`` (#923)."""
        if isinstance(exc, TimeoutError):
            elapsed = time.monotonic() - t0
            logger.error(
                "Agent timeout after %.1fs (thread %d): %s", elapsed, thread_id, exc,
            )
            stderr_summary = "\n".join(stderr_lines[-10:]) if stderr_lines else None
            await _emit_error(queue, str(exc), stderr_summary)
            return None, "return"

        if isinstance(exc, CLINotFoundError):
            logger.error("Claude CLI not found (thread %d): %s", thread_id, exc)
            err_msg = (
                "Claude CLI не найден. "
                "Установите: npm install -g @anthropic-ai/claude-code"
            )
            await queue.put(_sse({"error": err_msg}))
            await queue.put(None)
            return None, "return"

        if isinstance(exc, ProcessError):
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
            await _emit_error(queue, "Ошибка процесса Claude CLI", details)
            return None, "return"

        if isinstance(exc, CLIConnectionError):
            elapsed = time.monotonic() - t0
            logger.error(
                "Claude CLI connection error after %.1fs (thread %d): %s",
                elapsed, thread_id, exc,
            )
            if attempt == 0:
                return exc, "retry"
            stderr_summary = "\n".join(stderr_lines[-10:]) if stderr_lines else str(exc)
            conn_msg = (
                "Сервер Anthropic перегружен, попробуйте позже."
                if "overloaded" in str(exc).lower()
                else "Не удалось подключиться к Claude CLI"
            )
            await _emit_error(queue, conn_msg, stderr_summary)
            return None, "return"

        if isinstance(exc, ClaudeSDKError):
            elapsed = time.monotonic() - t0
            logger.error(
                "Claude SDK error after %.1fs (thread %d): %s", elapsed, thread_id, exc,
            )
            stderr_summary = "\n".join(stderr_lines[-10:]) if stderr_lines else ""
            await _emit_error(queue, f"Ошибка Claude SDK: {exc}", stderr_summary or None)
            return None, "return"

        if isinstance(exc, BaseExceptionGroup):
            # anyio TaskGroup wraps sub-exceptions in ExceptionGroup;
            # unwrap for a readable error message and retry on transient failures.
            flat = [str(e) for e in exc.exceptions]
            summary = "; ".join(flat)
            elapsed = time.monotonic() - t0
            # Never retry if the group contains a timeout — it's our deadline, not transient.
            has_timeout = any(isinstance(e, (TimeoutError, asyncio.TimeoutError)) for e in exc.exceptions)
            retryable = not has_timeout and any(
                kw in summary.lower()
                for kw in ("stream closed", "control request timeout", "connection")
            )
            logger.error(
                "ExceptionGroup after %.1fs (thread %d, attempt %d, retryable=%s): %s",
                elapsed, thread_id, attempt + 1, retryable, summary,
            )
            if attempt == 0 and retryable:
                return exc, "retry"
            return Exception(summary), "break"

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
            return exc, "retry"
        return exc, "break"

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

        _prompt_short = original_prompt[:100].replace("\n", " ")
        if len(original_prompt) > 100:
            _prompt_short += "…"
        _last_emitted: list[str] = [""]

        def _on_stderr(line: str) -> None:
            self._handle_stderr_line(
                line,
                queue=queue,
                prompt_short=_prompt_short,
                stderr_lines=stderr_lines,
                debug_lines=debug_lines,
                api_request_count=_api_request_count,
                api_request_ts=_api_request_ts,
                last_emitted=_last_emitted,
            )

        cli_path = shutil.which("claude")
        logger.info("claude-cli path: %s", cli_path)

        options = await self._build_claude_options(
            system_prompt=system_prompt,
            cli_path=cli_path,
            on_stderr=_on_stderr,
            extra=extra,
        )

        cfg = self._config.agent
        # BaseException (not Exception): the retry path can stash a
        # BaseExceptionGroup / CLIConnectionError here via _handle_stream_attempt_error.
        last_err: BaseException | None = None
        for attempt in range(2):
            if attempt > 0:
                tracker_retry = _ToolTracker(queue=queue)
                await tracker_retry.on_status("Повтор подключения к Claude...")
            tracker = _ToolTracker(queue=queue)
            draining = False
            stream_state = _ChatStreamState()
            t0 = time.monotonic()
            first_event_logged = False
            _api_request_count[0] = 0
            try:
                aiter = cast(
                    _ClosableAsyncIterator,
                    query(prompt=_as_prompt_stream(prompt), options=options).__aiter__(),
                )
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
                    except asyncio.TimeoutError as exc:
                        if not first_event_logged:
                            logger.error(
                                "First event timeout %ds fired after %.1fs (thread %d)",
                                cfg.first_event_timeout, time.monotonic() - t0, thread_id,
                            )
                            diag = _diagnose_connection(cli_path, stderr_lines)
                            raise TimeoutError(
                                f"Claude не ответил за {cfg.first_event_timeout}с. {diag}"
                            ) from exc
                        logger.error(
                            "Idle timeout %ds fired after %.1fs total (thread %d)",
                            cfg.idle_timeout, time.monotonic() - t0, thread_id,
                        )
                        reason = "Возможно, соединение потеряно."
                        if _last_rate_limit[0]:
                            reason = f"Последний rate limit: {_last_rate_limit[0]}."
                        raise TimeoutError(
                            f"Стрим Claude замолчал на {cfg.idle_timeout}с. {reason}"
                        ) from exc

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
                        await _dispatch_stream_message(
                            msg,
                            tracker=tracker,
                            queue=queue,
                            state=stream_state,
                            last_activity=_last_activity,
                            last_rate_limit=_last_rate_limit,
                            thread_id=thread_id,
                        )
                    except asyncio.CancelledError:
                        draining = True
                finally:
                    with suppress(Exception):
                        await aiter.aclose()
                return

            except (
                TimeoutError,
                CLINotFoundError,
                ProcessError,
                CLIConnectionError,
                ClaudeSDKError,
                BaseExceptionGroup,
                Exception,
            ) as exc:
                last_err, action = await self._handle_stream_attempt_error(
                    exc,
                    attempt=attempt,
                    t0=t0,
                    thread_id=thread_id,
                    stderr_lines=stderr_lines,
                    tracker=tracker,
                    queue=queue,
                )
                if action == "return":
                    return
                if action == "retry":
                    continue
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
            await _emit_error(queue, f"Ошибка агента: {last_err}", stderr_summary or None)
