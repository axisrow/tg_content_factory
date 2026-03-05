from __future__ import annotations

import asyncio
import logging

from src.config import LLMConfig
from src.database import Database
from src.models import SearchResult

logger = logging.getLogger(__name__)

_SYSTEM_PROMPT = """Ты — ассистент для поиска по Telegram-постам.
Используй инструмент search_posts_tool для поиска по базе.
Анализируй результаты и давай краткое резюме на русском языке.
Выдели ключевые находки и укажи ссылки на каналы."""


class AISearchEngine:
    def __init__(self, config: LLMConfig, db: Database):
        self._config = config
        self._db = db
        self._agent = None

    @property
    def enabled(self) -> bool:
        return self._config.enabled and bool(self._config.api_key)

    def initialize(self) -> None:
        if not self.enabled:
            logger.info("AI search disabled (llm.enabled=false or no API key)")
            return

        try:
            from deepagents import create_deep_agent

            db = self._db

            def search_posts_tool(query: str) -> str:
                """Search collected posts in the database."""
                loop = asyncio.get_event_loop()
                if loop.is_running():
                    import concurrent.futures

                    with concurrent.futures.ThreadPoolExecutor() as executor:
                        future = executor.submit(asyncio.run, db.search_messages(query, limit=20))
                        messages, total = future.result()
                else:
                    messages, total = asyncio.run(db.search_messages(query, limit=20))

                if not messages:
                    return f"No results found for: {query}"

                lines = [f"Found {total} results for '{query}'. Top results:"]
                for m in messages:
                    text_preview = (m.text or "")[:200]
                    lines.append(
                        f"- [{m.date}] Channel {m.channel_id}: {text_preview}"
                    )
                return "\n".join(lines)

            model_str = f"{self._config.provider}:{self._config.model}"
            self._agent = create_deep_agent(
                model=model_str,
                tools=[search_posts_tool],
                system_prompt=_SYSTEM_PROMPT,
            )
            logger.info("AI search agent initialized with model %s", model_str)
        except ImportError:
            logger.warning("deepagents not installed, AI search unavailable")
        except Exception as e:
            logger.error("Failed to initialize AI search: %s", e)

    async def search(self, query: str) -> SearchResult:
        """Run AI-powered search."""
        if not self._agent:
            # Fallback to basic local search
            messages, total = await self._db.search_messages(query, limit=20)
            return SearchResult(
                messages=messages,
                total=total,
                query=query,
                ai_summary="AI search is not available. Showing local results.",
            )

        try:
            response = await asyncio.to_thread(self._agent.run, query)
            summary = str(response)

            # Also get raw messages for display
            messages, total = await self._db.search_messages(query, limit=20)

            return SearchResult(
                messages=messages,
                total=total,
                query=query,
                ai_summary=summary,
            )
        except Exception as e:
            logger.error("AI search error: %s", e)
            messages, total = await self._db.search_messages(query, limit=20)
            return SearchResult(
                messages=messages,
                total=total,
                query=query,
                ai_summary=f"AI search error: {e}. Showing local results.",
            )
