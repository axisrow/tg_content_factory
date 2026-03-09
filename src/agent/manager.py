from __future__ import annotations

import asyncio
import json
import logging
import os
from collections.abc import AsyncGenerator
from contextlib import suppress

from claude_agent_sdk import AssistantMessage, ClaudeAgentOptions, ResultMessage, TextBlock, query

from src.database import Database

logger = logging.getLogger(__name__)

_SYSTEM_PROMPT = (
    "Ты — аналитический ассистент для работы с данными из Telegram-каналов.\n"
    "Используй search_messages для поиска сообщений и get_channels для списка каналов.\n"
    "Основной use-case: анализ вопросов и ответов из каналов для создания учебного курса.\n"
    "Отвечай на русском языке. Будь точным и структурированным."
)

_ALLOWED_TOOLS = ["mcp__telegram_db__search_messages", "mcp__telegram_db__get_channels"]


class AgentManager:
    def __init__(self, db: Database) -> None:
        self._db = db
        self._server = None
        self._active_tasks: set[asyncio.Task] = set()

    def initialize(self) -> None:
        from src.agent.tools import make_mcp_server

        self._server = make_mcp_server(self._db)
        logger.info("AgentManager initialized (claude-agent-sdk)")

    @property
    def available(self) -> bool:
        return bool(
            os.environ.get("ANTHROPIC_API_KEY") or os.environ.get("CLAUDE_CODE_OAUTH_TOKEN")
        )

    def _build_prompt(self, history: list[dict], message: str) -> str:
        lines = []
        for msg in history:
            role_label = "User" if msg["role"] == "user" else "Assistant"
            lines.append(f"{role_label}: {msg['content']}")
        lines.append(f"User: {message}")
        return "\n".join(lines)

    async def chat_stream(self, thread_id: int, message: str) -> AsyncGenerator[str, None]:
        """Async generator yielding raw SSE lines: data: <json>\\n\\n"""
        history = await self._db.get_agent_messages(thread_id)
        # Callers must save the user message before calling chat_stream, so history[-1]
        # is always the user's message we're about to answer — exclude it from context.
        assert not history or history[-1]["role"] == "user", (
            "Expected last DB message to be the user message just saved"
        )
        prompt = self._build_prompt(history[:-1], message)

        extra: dict = {}
        if agent_model := os.environ.get("AGENT_MODEL"):
            extra["model"] = agent_model
        options = ClaudeAgentOptions(
            system_prompt=_SYSTEM_PROMPT,
            mcp_servers={"telegram_db": self._server},
            allowed_tools=_ALLOWED_TOOLS,
            **extra,
        )

        queue: asyncio.Queue[str | None] = asyncio.Queue()

        async def _run_query() -> None:
            draining = False
            try:
                full_text = ""
                async for msg in query(prompt=prompt, options=options):
                    if draining:
                        continue  # drain to StopAsyncIteration to avoid cleanup Task
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
                                {"done": True, "full_text": full_text}, ensure_ascii=False
                            )
                            await queue.put(f"data: {done_payload}\n\n")
                            # NO return — let the loop exhaust naturally
                    except asyncio.CancelledError:
                        draining = True  # switch to drain mode, keep iterating
            except Exception as e:
                logger.exception("Agent chat error for thread %d", thread_id)
                err_payload = json.dumps({"error": f"Ошибка агента: {e}"}, ensure_ascii=False)
                await queue.put(f"data: {err_payload}\n\n")
            finally:
                await queue.put(None)  # sentinel

        task = asyncio.create_task(_run_query())
        self._active_tasks.add(task)
        task.add_done_callback(self._active_tasks.discard)
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

    async def close_all(self) -> None:
        tasks = list(self._active_tasks)
        for t in tasks:
            t.cancel()
        for t in tasks:
            with suppress(asyncio.CancelledError):
                await t
