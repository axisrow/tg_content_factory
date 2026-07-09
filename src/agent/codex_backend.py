"""Codex SDK agent backend.

A third agent engine alongside ``ClaudeSdkBackend`` and ``DeepagentsBackend``.
Like claude-agent-sdk, Codex is a full agent engine: it streams output, reports
token usage, and — crucially — can use the project's tools. The difference is
transport: claude-agent-sdk loads an *in-process* MCP server, whereas Codex runs
the ``codex`` CLI as a subprocess and connects to MCP servers declared as
*external* stdio processes. So we point Codex at ``python -m src.main mcp-server``
(the standalone entrypoint), which serves the same ``build_agent_tool_registry``
the in-process path uses — one tool registry, two transports.

Codex is keyless: auth comes from the Codex CLI (``~/.codex/auth.json``).
"""

from __future__ import annotations

import asyncio
import logging
import sys

from src.config import AppConfig
from src.database import Database
from src.utils.json import safe_json_dumps

logger = logging.getLogger(__name__)

CODEX_DEFAULT_MODEL = "gpt-5.4"
# Name the project MCP server is registered under inside Codex's config.
PROJECT_MCP_SERVER_NAME = "telegram_db"

# Stream-notification method strings we consume (openai_codex
# ``notification_registry.NOTIFICATION_MODELS`` keys; verified by
# ``test_codex_backend.test_notification_methods_match_sdk_registry`` when the
# SDK is installed). Centralised so a rename in the SDK surfaces as one failing
# guard rather than silent dead branches in the stream loop.
NOTE_AGENT_MESSAGE_DELTA = "item/agentMessage/delta"
NOTE_ITEM_STARTED = "item/started"
NOTE_ITEM_COMPLETED = "item/completed"
NOTE_TOKEN_USAGE_UPDATED = "thread/tokenUsage/updated"
NOTE_TURN_COMPLETED = "turn/completed"


def _project_mcp_server_config(config_path: str, *, with_pool: bool) -> dict:
    """Codex ``mcp_servers`` entry that spawns the project's stdio MCP server.

    Mirrors the ``~/.codex/config.toml`` ``mcp_servers`` shape (command + args),
    so Codex launches ``python -m src.main mcp-server`` as a child process and
    discovers every project tool over stdio JSON-RPC.
    """
    args = ["-m", "src.main", "--config", config_path, "mcp-server"]
    if not with_pool:
        args.append("--no-pool")
    return {
        "mcp_servers": {
            PROJECT_MCP_SERVER_NAME: {
                "command": sys.executable,
                "args": args,
            }
        }
    }


class CodexSdkBackend:
    """Run the Codex agent engine with the project's tools wired in over MCP."""

    def __init__(
        self, db: Database, config: AppConfig, client_pool=None, scheduler_manager=None
    ) -> None:
        self._db = db
        self._config = config
        self._client_pool = client_pool
        self._scheduler_manager = scheduler_manager
        self._initialized = False

    def initialize(self) -> None:
        self._initialized = True

    @property
    def available(self) -> bool:
        """True when the Codex SDK is installed and the Codex CLI is authenticated."""
        from src.services.provider_adapters import codex_available

        return codex_available()

    def _config_path(self) -> str:
        # The MCP subprocess re-loads config to open the same DB. AppConfig does
        # not carry its source path, so honour an explicit override and fall back
        # to the project-standard config.yaml used by every other entrypoint.
        import os

        return os.environ.get("TG_CONFIG_PATH", "").strip() or "config.yaml"

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
        del thread_id, stats, session_id

        from openai_codex import AsyncCodex, Sandbox

        from src.agent.manager import _embed_history_in_prompt

        model_id = model or CODEX_DEFAULT_MODEL
        full_prompt = _embed_history_in_prompt(history_msgs or [], prompt)
        with_pool = self._client_pool is not None
        codex_config = _project_mcp_server_config(self._config_path(), with_pool=with_pool)

        full_text_parts: list[str] = []
        usage: dict | None = None
        turn_error: str | None = None

        async def _drain() -> dict | None:
            nonlocal usage, turn_error
            async with AsyncCodex() as codex:
                thread = await codex.thread_start(
                    base_instructions=system_prompt or None,
                    config=codex_config,
                    model=model_id,
                    # read_only, not workspace_write: the agent's real work
                    # (DB/Telegram/search) runs in the out-of-process mcp-server
                    # subprocess, which is unaffected by Codex's own sandbox. The
                    # Codex process itself never needs to write the project tree,
                    # so don't grant it write access to whatever CWD the web
                    # process started in.
                    sandbox=Sandbox.read_only,
                )
                handle = await thread.turn(full_prompt)
                async for note in handle.stream():
                    method = getattr(note, "method", None)
                    payload = getattr(note, "payload", None)
                    if method == NOTE_AGENT_MESSAGE_DELTA:
                        text = getattr(payload, "delta", None)
                        if text:
                            full_text_parts.append(text)
                            chunk = safe_json_dumps({"text": text}, ensure_ascii=False)
                            await queue.put(f"data: {chunk}\n\n")
                    elif method == NOTE_ITEM_STARTED:
                        event = _tool_start_event(payload)
                        if event:
                            await queue.put(f"data: {safe_json_dumps(event, ensure_ascii=False)}\n\n")
                    elif method == NOTE_ITEM_COMPLETED:
                        event = _tool_end_event(payload)
                        if event:
                            await queue.put(f"data: {safe_json_dumps(event, ensure_ascii=False)}\n\n")
                    elif method == NOTE_TOKEN_USAGE_UPDATED:
                        usage = _usage_from_payload(payload) or usage
                    elif method == NOTE_TURN_COMPLETED:
                        # Terminal event — stop draining rather than waiting for
                        # the async iterator to close on its own. But a failed
                        # turn (auth expiry, model error, MCP-server crash) is
                        # delivered *inside* turn/completed as Turn.status=failed
                        # + Turn.error — record it so the caller surfaces an
                        # error frame instead of a "successful" empty response.
                        turn_error = _turn_error_message(payload)
                        break
            return usage

        # Hard total-turn deadline, mirroring ClaudeSdkBackend's total_timeout.
        # Without it a stalled Codex subprocess (auth expiry, model overload)
        # would block this coroutine — and the SSE stream — until the client or
        # server gives up. On timeout, surface an error frame to the queue.
        total_timeout = self._config.agent.total_timeout
        try:
            await asyncio.wait_for(_drain(), timeout=total_timeout)
        except (TimeoutError, asyncio.TimeoutError):
            logger.error("Codex turn exceeded total_timeout=%ss", total_timeout)
            error_payload = safe_json_dumps(
                {"error": f"Codex не ответил за {total_timeout}с (таймаут)"},
                ensure_ascii=False,
            )
            await queue.put(f"data: {error_payload}\n\n")
            return

        # A failed turn is not a success: surface the error frame the drain
        # loop captured from turn/completed (status=failed) instead of a done
        # payload whose full_text would otherwise be an empty "" answer.
        if turn_error is not None:
            logger.error("Codex turn failed: %s", turn_error)
            error_payload = safe_json_dumps({"error": turn_error}, ensure_ascii=False)
            await queue.put(f"data: {error_payload}\n\n")
            return

        full_text = "".join(full_text_parts)
        done_payload = safe_json_dumps(
            {
                "done": True,
                "full_text": full_text,
                "backend": "codex",
                "model": model_id,
                "usage": usage or {},
            },
            ensure_ascii=False,
        )
        await queue.put(f"data: {done_payload}\n\n")


def _turn_error_message(payload) -> str | None:
    """Error message when a ``turn/completed`` reports a failed turn, else None.

    Shape (openai_codex ``TurnCompletedNotification``): ``payload.turn`` is a
    ``Turn`` whose ``status`` is a ``TurnStatus`` enum (``failed`` when the turn
    crashed — auth expiry, model error, MCP-server crash) and whose ``error`` is
    a ``TurnError`` (``.message``) populated only on failure. ``status`` is
    compared via its ``.value`` so a real enum and a plain-string fake both work;
    any status other than ``failed`` (completed / interrupted / inProgress) is a
    non-failure and returns None.
    """
    turn = getattr(payload, "turn", None)
    if turn is None:
        return None
    status = getattr(turn, "status", None)
    status_value = getattr(status, "value", status)
    if status_value != "failed":
        return None
    error = getattr(turn, "error", None)
    message = getattr(error, "message", None)
    return str(message) if message else "Codex turn failed"


def _mcp_tool_item(payload):
    """Return the ``McpToolCallThreadItem`` from an item notification, or None.

    ``item/started`` / ``item/completed`` payloads wrap a ``ThreadItem`` whose
    ``root`` is the concrete item; we only care about MCP tool calls (they carry
    a ``tool`` name and a ``server``).
    """
    item = getattr(payload, "item", None)
    inner = getattr(item, "root", None) if item is not None else None
    inner = inner if inner is not None else item
    if inner is not None and getattr(inner, "tool", None) and getattr(inner, "server", None):
        return inner
    return None


def _tool_start_event(payload) -> dict | None:
    """SSE ``tool_start`` event when an MCP tool call begins, else None."""
    item = _mcp_tool_item(payload)
    if item is None:
        return None
    return {"type": "tool_start", "tool": str(item.tool)}


def _tool_end_event(payload) -> dict | None:
    """SSE ``tool_end`` event when an MCP tool call finishes, else None."""
    item = _mcp_tool_item(payload)
    if item is None:
        return None
    duration = getattr(item, "duration_ms", None)
    return {
        "type": "tool_end",
        "tool": str(item.tool),
        "duration": round(duration / 1000, 1) if isinstance(duration, (int, float)) else 0,
        "is_error": getattr(item, "error", None) is not None,
        "summary": str(getattr(item, "server", "")),
    }


def _usage_from_payload(payload) -> dict | None:
    """Extract the latest token usage from a ``thread/tokenUsage/updated`` payload.

    Shape (openai_codex): ``payload.token_usage.last`` is a ``TokenUsageBreakdown``
    with ``input_tokens`` / ``output_tokens`` / ``total_tokens`` / etc.
    """
    token_usage = getattr(payload, "token_usage", None)
    breakdown = getattr(token_usage, "last", None) if token_usage is not None else None
    if breakdown is None:
        return None
    out: dict = {}
    for attr in (
        "input_tokens",
        "output_tokens",
        "total_tokens",
        "cached_input_tokens",
        "reasoning_output_tokens",
    ):
        value = getattr(breakdown, attr, None)
        if value is not None:
            out[attr] = value
    return out or None
