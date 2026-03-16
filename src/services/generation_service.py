from __future__ import annotations

from datetime import datetime
from typing import Any, Awaitable, Callable, Dict, List, Optional

from src.agent.prompt_template import DEFAULT_AGENT_PROMPT_TEMPLATE, render_prompt_template
from src.models import Message, SearchResult
from src.search.engine import SearchEngine


class GenerationService:
    """Simple provider-agnostic RAG generation service.

    This core focuses on retrieval assembly and prompt rendering. It intentionally
    keeps provider integration pluggable via `provider_callable` so that unit
    tests can mock providers and later integration can wire AgentProviderService.
    """

    def __init__(
        self,
        search_engine: SearchEngine,
        provider_callable: Optional[Callable[..., Awaitable[str]]] = None,
        default_prompt_template: str = DEFAULT_AGENT_PROMPT_TEMPLATE,
    ) -> None:
        self._search = search_engine
        self._provider = provider_callable
        self._default_prompt = default_prompt_template

    async def _collect_context(self, query: str, limit: int = 8) -> List[Message]:
        """Retrieve context messages using the SearchEngine hybrid search."""
        result: SearchResult = await self._search.search_hybrid(query, limit=limit)
        return result.messages

    def _build_source_messages(self, messages: List[Message]) -> str:
        parts: List[str] = []
        for m in messages:
            text = (m.text or "").strip()
            if not text:
                continue
            header = m.channel_title or m.channel_username or ""
            when = m.date.isoformat() if isinstance(m.date, datetime) else str(m.date)
            parts.append(f"[{header}] {text} (id:{m.message_id} date:{when})")
        return "\n\n".join(parts)

    async def generate(
        self,
        query: str,
        prompt_template: Optional[str] = None,
        limit: int = 8,
        model: Optional[str] = None,
        max_tokens: int = 256,
        temperature: float = 0.0,
        provider_override: Optional[str] = None,
        stream: bool = False,
        provider_callable: Optional[Callable[..., Awaitable[str]]] = None,
    ) -> Dict[str, Any]:
        """Generate a draft from `query` using retrieval-augmented generation.

        Returns a dict containing the final prompt, generated_text, and citations.
        """
        if prompt_template is None:
            prompt_template = self._default_prompt

        messages = await self._collect_context(query, limit=limit)
        source_messages = self._build_source_messages(messages)

        rendered_prompt = render_prompt_template(
            prompt_template, {"source_messages": source_messages}
        )

        # Determine which provider callable to use (call argument overrides constructor)
        provider = provider_callable or self._provider
        if provider is None:
            raise RuntimeError("No provider callable configured for generation")

        # Provider contract: await provider(prompt=..., model=..., max_tokens=...,
        # temperature=..., stream=...)
        generated_text = await provider(
            prompt=rendered_prompt,
            model=model,
            max_tokens=max_tokens,
            temperature=temperature,
            stream=stream,
        )

        citations = [
            {
                "channel_title": (m.channel_title or m.channel_username or ""),
                "message_id": m.message_id,
                "text": (m.text or "")[:512],
                "date": m.date.isoformat() if isinstance(m.date, datetime) else str(m.date),
            }
            for m in messages
        ]

        return {
            "prompt": rendered_prompt,
            "generated_text": generated_text,
            "citations": citations,
        }
