from __future__ import annotations

import asyncio
import logging
from typing import Any

from src.config import LLMConfig
from src.database import Database
from src.database.bundles import SearchBundle
from src.models import SearchParams, SearchResult

logger = logging.getLogger(__name__)

_SYSTEM_PROMPT = """Ты — ассистент для поиска по Telegram-постам.
Используй инструмент search_posts_tool для поиска по базе.
Анализируй результаты и давай краткое резюме на русском языке.
Выдели ключевые находки и укажи ссылки на каналы."""


class AISearchEngine:
    def __init__(self, config: LLMConfig, search: SearchBundle | Database):
        self._config = config
        if isinstance(search, Database):
            search = SearchBundle.from_database(search)
        self._search = search
        # deepagents.create_deep_agent returns a langgraph CompiledStateGraph;
        # kept as Any so the runtime hasattr-guard in _run_agent_sync isn't
        # second-guessed by the static type.
        self._agent: Any = None
        self._init_error: str | None = None

    @property
    def enabled(self) -> bool:
        return self._config.enabled and bool(self._config.api_key)

    def initialize(self) -> None:
        if not self.enabled:
            logger.info("AI search disabled (llm.enabled=false or no API key)")
            return

        try:
            from deepagents import create_deep_agent
        except ImportError as exc:
            logger.warning("deepagents not installed, AI search unavailable: %s", exc)
            return

        search = self._search

        def search_posts_tool(query: str) -> str:
            """Search collected posts in the database."""
            import concurrent.futures

            try:
                asyncio.get_running_loop()
                with concurrent.futures.ThreadPoolExecutor() as executor:
                    coro = search.search_messages(SearchParams(query=query, limit=20))
                    future = executor.submit(asyncio.run, coro)
                    page = future.result()
            except RuntimeError:
                page = asyncio.run(search.search_messages(SearchParams(query=query, limit=20)))

            messages = page.messages
            if not messages:
                return f"No results found for: {query}"

            # total is a lower bound when has_more is set (#766) — keep the
            # volume signal for the LLM so it knows the query is broad.
            total_display = f"{page.total}+" if page.has_more else str(page.total)
            lines = [f"Found {total_display} results for '{query}'. Top results:"]
            for m in messages:
                text_preview = (m.text or "")[:200]
                lines.append(f"- [{m.date}] Channel {m.channel_id}: {text_preview}")
            return "\n".join(lines)

        model_str = f"{self._config.provider}:{self._config.model}"
        try:
            self._agent = create_deep_agent(
                model=model_str,
                tools=[search_posts_tool],
                system_prompt=_SYSTEM_PROMPT,
            )
            logger.info("AI search agent initialized with model %s", model_str)
        except ImportError as exc:
            logger.warning("AI search dependency missing: %s", exc)
        except Exception as e:
            logger.error("Failed to initialize AI search: %s", e)
            self._init_error = str(e)

    @staticmethod
    def _extract_result_text(result: object) -> str:
        """Extract the final assistant text from a deepagents/langgraph result.

        `create_deep_agent` returns a langgraph ``CompiledStateGraph`` whose
        ``invoke`` yields ``{"messages": [...]}`` — the last message carries the
        answer as ``content`` (a plain string or a list of content blocks). This
        mirrors ``DeepagentsBackend._extract_result_text``.
        """
        if isinstance(result, dict):
            messages = result.get("messages") or []
            if messages:
                last_message = messages[-1]
                content = getattr(last_message, "content", None)
                if content is None and isinstance(last_message, dict):
                    content = last_message.get("content")
                if isinstance(content, str):
                    return content
                if isinstance(content, list):
                    return "\n".join(
                        block.get("text", "") if isinstance(block, dict) else str(block)
                        for block in content
                    ).strip()
            return str(result)
        return str(result)

    def _run_agent_sync(self, query: str) -> str:
        """Invoke the agent and normalize its response to text.

        ``create_deep_agent`` returns a langgraph ``CompiledStateGraph`` that
        exposes ``.invoke`` (not ``.run``); the hasattr-guard keeps the older
        ``.run`` shape working for backends/fakes that still provide it.
        """
        agent = self._agent
        if hasattr(agent, "run"):
            result = agent.run(query)
        else:
            result = agent.invoke({"messages": [{"role": "user", "content": query}]})
        return self._extract_result_text(result)

    async def search(self, query: str) -> SearchResult:
        """Run AI-powered search."""
        if not self._agent:
            # Fallback to basic local search
            page = await self._search.search_messages(SearchParams(query=query, limit=20))
            return SearchResult(
                messages=page.messages,
                total=page.total,
                has_more=page.has_more,
                query=query,
                ai_summary="AI search is not available. Showing local results.",
            )

        try:
            summary = await asyncio.to_thread(self._run_agent_sync, query)

            # Also get raw messages for display
            page = await self._search.search_messages(SearchParams(query=query, limit=20))

            return SearchResult(
                messages=page.messages,
                total=page.total,
                has_more=page.has_more,
                query=query,
                ai_summary=summary,
            )
        except Exception as e:
            logger.error("AI search error: %s", e)
            page = await self._search.search_messages(SearchParams(query=query, limit=20))
            return SearchResult(
                messages=page.messages,
                total=page.total,
                has_more=page.has_more,
                query=query,
                ai_summary=f"AI search error: {e}. Showing local results.",
            )
