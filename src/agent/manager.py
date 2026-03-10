from __future__ import annotations

import asyncio
import json
import logging
import os
import shutil
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
        self._active_tasks: dict[int, asyncio.Task] = {}  # thread_id → task

    def initialize(self) -> None:
        from src.agent.tools import make_mcp_server

        os.environ.setdefault("CLAUDE_CODE_STREAM_CLOSE_TIMEOUT", "300000")
        self._server = make_mcp_server(self._db)
        logger.info("AgentManager initialized (claude-agent-sdk)")

    @property
    def available(self) -> bool:
        return bool(
            os.environ.get("ANTHROPIC_API_KEY") or os.environ.get("CLAUDE_CODE_OAUTH_TOKEN")
        )

    def _build_prompt(
        self, history: list[dict], message: str
    ) -> tuple[str, dict]:
        """Build prompt from history + message, truncating oldest if over budget.

        Returns (prompt_text, stats) where stats contains:
          total_msgs, kept_msgs, total_chars, prompt_chars.
        """
        user_part = f"<user>\n{message}\n</user>"
        budget = 100_000 * 4  # ~100k tokens in chars — safe limit with CLI overhead
        used = len(user_part)

        total_msgs = len(history)
        # Take messages from most recent, drop oldest if over budget
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
            "kept_msgs": len(kept) - 1,  # exclude current user message
            "total_chars": sum(len(m["content"]) for m in history) + len(message),
            "prompt_chars": len(prompt),
        }
        return prompt, stats

    async def estimate_prompt_tokens(self, thread_id: int, message: str) -> int:
        """Estimate prompt token count (~4 chars per token) before saving."""
        history = await self._db.get_agent_messages(thread_id)
        prompt, _stats = self._build_prompt(history, message)
        return len(prompt) // 4

    async def chat_stream(
        self, thread_id: int, message: str, model: str | None = None
    ) -> AsyncGenerator[str, None]:
        """Async generator yielding raw SSE lines: data: <json>\\n\\n"""
        history = await self._db.get_agent_messages(thread_id)
        # Callers must save the user message before calling chat_stream, so history[-1]
        # is always the user's message we're about to answer — exclude it from context.
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

        queue: asyncio.Queue[str | None] = asyncio.Queue()

        async def _run_query() -> None:
            last_err: Exception | None = None
            for attempt in range(2):
                draining = False
                full_text = ""
                try:
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
                                    {"done": True, "full_text": full_text},
                                    ensure_ascii=False,
                                )
                                await queue.put(f"data: {done_payload}\n\n")
                                # NO return — let the loop exhaust naturally
                        except asyncio.CancelledError:
                            draining = True  # switch to drain mode, keep iterating
                    break  # success
                except Exception as e:
                    if attempt == 0 and "Control request timeout" in str(e):
                        logger.warning(
                            "Agent init timeout, retrying (thread %d)", thread_id
                        )
                        last_err = e
                        continue
                    last_err = e
                    break
            if last_err is not None:
                if stderr_lines:
                    logger.error(
                        "claude-cli stderr dump (thread %d):\n%s",
                        thread_id,
                        "\n".join(stderr_lines),
                    )
                else:
                    logger.error(
                        "claude-cli failed with no stderr (thread %d): %s. "
                        "Prompt was %d chars (~%dK tokens). "
                        "If prompt > 400K chars, likely 'prompt too long' error.",
                        thread_id,
                        last_err,
                        stats["prompt_chars"],
                        stats["prompt_chars"] // 4000,
                    )
                logger.exception("Agent chat error for thread %d", thread_id)
                err_payload = json.dumps(
                    {"error": f"Ошибка агента: {last_err}"}, ensure_ascii=False
                )
                await queue.put(f"data: {err_payload}\n\n")
            await queue.put(None)  # sentinel

        task = asyncio.create_task(_run_query())
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
        """Cancel an active stream for the given thread. Returns True if cancelled."""
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
        for t in tasks:
            t.cancel()
        for t in tasks:
            with suppress(asyncio.CancelledError):
                await t
