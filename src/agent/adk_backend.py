"""Google ADK (Agent Development Kit) agent backend.

A fourth agent engine alongside ``ClaudeSdkBackend``, ``DeepagentsBackend`` and
``CodexSdkBackend``. Like the others it streams output, reports token usage and
uses the project's tools. The transport mirrors Codex: ADK connects to the
project tool registry as an *external* stdio MCP server. We point ADK's
``McpToolset`` at ``python -m src.main mcp-server`` (the standalone entrypoint),
which serves the same ``build_agent_tool_registry`` the in-process path uses —
one tool registry, several transports.

ADK runs the Gemini family of models, so it needs a Google/Gemini API key
(``GOOGLE_API_KEY`` / ``GOOGLE_GENAI_API_KEY`` / ``GEMINI_API_KEY``); without one
the backend reports itself unavailable. Every ``google.adk`` import is lazy so
this module loads without the SDK installed (the optional ``[adk]`` extra).
"""

from __future__ import annotations

import asyncio
import logging
import os
import sys

from src.config import AppConfig
from src.database import Database
from src.utils.json import safe_json_dumps

logger = logging.getLogger(__name__)

ADK_DEFAULT_MODEL = "gemini-2.5-flash"
# Name the project MCP server is registered under inside ADK's toolset.
PROJECT_MCP_SERVER_NAME = "telegram_db"
# App/user identifiers for ADK's in-memory session — single-tenant, so constant.
ADK_APP_NAME = "tg_content_factory"
ADK_USER_ID = "web"
# Environment variables ADK / google-genai accept for the API key, in priority
# order. The first non-empty one makes the backend available.
ADK_API_KEY_ENV_VARS: tuple[str, ...] = (
    "GOOGLE_API_KEY",
    "GOOGLE_GENAI_API_KEY",
    "GEMINI_API_KEY",
)


def _adk_sdk_installed() -> bool:
    """True when the ``google.adk`` SDK is importable in this environment."""
    import importlib.util

    return importlib.util.find_spec("google.adk") is not None


def _adk_api_key() -> str:
    """Return the first configured Google/Gemini API key, or "" if none set."""
    for var in ADK_API_KEY_ENV_VARS:
        value = os.environ.get(var, "").strip()
        if value:
            return value
    return ""


class AdkSdkBackend:
    """Run the Google ADK agent engine with the project's tools wired in over MCP."""

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
        """True when the ADK SDK is installed and a Google/Gemini API key is set."""
        return _adk_sdk_installed() and bool(_adk_api_key())

    def _config_path(self) -> str:
        # The MCP subprocess re-loads config to open the same DB. AppConfig does
        # not carry its source path, so honour an explicit override and fall back
        # to the project-standard config.yaml used by every other entrypoint.
        return os.environ.get("TG_CONFIG_PATH", "").strip() or "config.yaml"

    def _mcp_server_args(self) -> list[str]:
        """Args for the stdio MCP subprocess that serves the project tool registry."""
        args = ["-m", "src.main", "--config", self._config_path(), "mcp-server"]
        if self._client_pool is None:
            args.append("--no-pool")
        return args

    def _build_agent(self, *, system_prompt: str, model_id: str):
        """Construct the ADK ``LlmAgent`` wired to the project MCP toolset."""
        from google.adk.agents import Agent
        from google.adk.tools.mcp_tool import McpToolset
        from google.adk.tools.mcp_tool.mcp_session_manager import StdioConnectionParams
        from mcp import StdioServerParameters

        toolset = McpToolset(
            connection_params=StdioConnectionParams(
                server_params=StdioServerParameters(
                    command=sys.executable,
                    args=self._mcp_server_args(),
                ),
                timeout=self._config.agent.permission_timeout,
            )
        )
        return Agent(
            name=PROJECT_MCP_SERVER_NAME,
            model=model_id,
            description="Telegram content factory assistant",
            instruction=system_prompt or "",
            tools=[toolset],
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
        del thread_id, stats, session_id

        from google.adk.agents.run_config import RunConfig, StreamingMode
        from google.adk.runners import InMemoryRunner
        from google.genai import types

        from src.agent.manager import _embed_history_in_prompt

        model_id = model or ADK_DEFAULT_MODEL
        full_prompt = _embed_history_in_prompt(history_msgs or [], prompt)

        full_text_parts: list[str] = []
        usage: dict | None = None

        async def _drain() -> dict | None:
            nonlocal usage
            agent = self._build_agent(system_prompt=system_prompt, model_id=model_id)
            runner = InMemoryRunner(agent=agent, app_name=ADK_APP_NAME)
            try:
                session = await runner.session_service.create_session(
                    app_name=ADK_APP_NAME, user_id=ADK_USER_ID
                )
                message = types.Content(role="user", parts=[types.Part(text=full_prompt)])
                run_config = RunConfig(streaming_mode=StreamingMode.SSE)
                async for event in runner.run_async(
                    user_id=ADK_USER_ID,
                    session_id=session.id,
                    new_message=message,
                    run_config=run_config,
                ):
                    await self._handle_event(event, queue, full_text_parts)
                    event_usage = _usage_from_event(event)
                    if event_usage:
                        usage = event_usage
            finally:
                # InMemoryRunner spawns the MCP subprocess via McpToolset; close it
                # so a finished/cancelled turn does not leak the child process.
                await runner.close()
            return usage

        # Hard total-turn deadline, mirroring the other backends' total_timeout.
        # Without it a stalled ADK turn (model overload, MCP subprocess hang)
        # would block this coroutine — and the SSE stream — indefinitely.
        total_timeout = self._config.agent.total_timeout
        try:
            await asyncio.wait_for(_drain(), timeout=total_timeout)
        except (TimeoutError, asyncio.TimeoutError):
            logger.error("ADK turn exceeded total_timeout=%ss", total_timeout)
            error_payload = safe_json_dumps(
                {"error": f"ADK не ответил за {total_timeout}с (таймаут)"},
                ensure_ascii=False,
            )
            await queue.put(f"data: {error_payload}\n\n")
            return

        full_text = "".join(full_text_parts)
        done_payload = safe_json_dumps(
            {
                "done": True,
                "full_text": full_text,
                "backend": "adk",
                "model": model_id,
                "usage": usage or {},
            },
            ensure_ascii=False,
        )
        await queue.put(f"data: {done_payload}\n\n")

    async def _handle_event(
        self,
        event,
        queue: asyncio.Queue[str | None],
        full_text_parts: list[str],
    ) -> None:
        """Map one ADK event to the project's SSE frames (text deltas + tool events)."""
        content = getattr(event, "content", None)
        parts = getattr(content, "parts", None) if content is not None else None
        if not parts:
            return
        for part in parts:
            text = getattr(part, "text", None)
            if text:
                full_text_parts.append(text)
                chunk = safe_json_dumps({"text": text}, ensure_ascii=False)
                await queue.put(f"data: {chunk}\n\n")
            function_call = getattr(part, "function_call", None)
            if function_call is not None and getattr(function_call, "name", None):
                start = {"type": "tool_start", "tool": str(function_call.name)}
                await queue.put(f"data: {safe_json_dumps(start, ensure_ascii=False)}\n\n")
            function_response = getattr(part, "function_response", None)
            if function_response is not None and getattr(function_response, "name", None):
                end = {
                    "type": "tool_end",
                    "tool": str(function_response.name),
                    "duration": 0,
                    "is_error": False,
                    "summary": PROJECT_MCP_SERVER_NAME,
                }
                await queue.put(f"data: {safe_json_dumps(end, ensure_ascii=False)}\n\n")


def _usage_from_event(event) -> dict | None:
    """Extract token usage from an ADK event's ``usage_metadata``, or None.

    Shape (google-genai ``GenerateContentResponseUsageMetadata``):
    ``prompt_token_count`` / ``candidates_token_count`` / ``total_token_count``.
    Mapped onto the same keys the other backends report so the UI is uniform.
    """
    usage_metadata = getattr(event, "usage_metadata", None)
    if usage_metadata is None:
        return None
    out: dict = {}
    prompt_tokens = getattr(usage_metadata, "prompt_token_count", None)
    if prompt_tokens is not None:
        out["input_tokens"] = prompt_tokens
    candidate_tokens = getattr(usage_metadata, "candidates_token_count", None)
    if candidate_tokens is not None:
        out["output_tokens"] = candidate_tokens
    total_tokens = getattr(usage_metadata, "total_token_count", None)
    if total_tokens is not None:
        out["total_tokens"] = total_tokens
    return out or None
