from __future__ import annotations

from datetime import datetime
from typing import Any, AsyncIterator, Awaitable, Callable, Dict, List, Optional

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
        """Retrieve context messages using the SearchEngine.

        Uses hybrid (semantic+FTS) search when embeddings are available, falls back to
        FTS/LIKE local search otherwise so that pipelines still work without a vector backend.
        """
        import logging

        if getattr(self._search, "semantic_available", True):
            result: SearchResult = await self._search.search_hybrid(query, limit=limit)
        else:
            logging.getLogger(__name__).warning(
                "Semantic search unavailable, falling back to FTS local search for context retrieval"
            )
            result = await self._search.search_local(query, limit=limit)
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

    async def generate_stream(
        self,
        query: str,
        prompt_template: Optional[str] = None,
        limit: int = 8,
        model: Optional[str] = None,
        max_tokens: int = 256,
        temperature: float = 0.0,
        provider_override: Optional[str] = None,
        provider_callable: Optional[Callable[..., Awaitable[str]]] = None,
    ) -> AsyncIterator[Dict[str, Any]]:
        """Async generator that yields partial generation updates when the
        provider supports streaming. Each yield is a dict with keys:
          - prompt
          - generated_text (accumulated so far)
          - delta (latest chunk)
          - citations
        If the provider does not stream, a single final yield is produced.
        """
        if prompt_template is None:
            prompt_template = self._default_prompt

        messages = await self._collect_context(query, limit=limit)
        source_messages = self._build_source_messages(messages)

        rendered_prompt = render_prompt_template(
            prompt_template, {"source_messages": source_messages}
        )

        provider = provider_callable or self._provider
        if provider is None:
            raise RuntimeError("No provider callable configured for generation")

        # Citations baked from retrieved messages
        citations = [
            {
                "channel_title": (m.channel_title or m.channel_username or ""),
                "message_id": m.message_id,
                "text": (m.text or "")[:512],
                "date": m.date.isoformat() if isinstance(m.date, datetime) else str(m.date),
            }
            for m in messages
        ]

        # Call the provider with stream=True. The provider may return:
        #  - an async generator directly (async def with yield)
        #  - a coroutine that resolves to an async generator (async def that returns an async generator)
        #  - a coroutine that resolves to a final string
        #  - a synchronous iterator or string
        try:
            maybe = provider(
                prompt=rendered_prompt,
                model=model,
                max_tokens=max_tokens,
                temperature=temperature,
                stream=True,
            )
        except Exception:
            raise

        # If it's awaitable (a coroutine), await it to get the concrete result
        result = maybe
        if hasattr(maybe, "__await__"):
            result = await maybe  # type: ignore

        # If it's an async iterable (async generator), iterate and yield
        if hasattr(result, "__aiter__"):
            buffer = ""
            async for chunk in result:  # type: ignore
                if isinstance(chunk, dict):
                    delta = (
                        chunk.get("text")
                        or chunk.get("content")
                        or chunk.get("generated_text")
                        or str(chunk)
                    )
                else:
                    delta = str(chunk)
                buffer += delta
                yield {
                    "prompt": rendered_prompt,
                    "generated_text": buffer,
                    "delta": delta,
                    "citations": citations,
                }
            return

        # If it's a synchronous iterator (list/iter), iterate synchronously
        if hasattr(result, "__iter__") and not isinstance(result, (str, bytes)):
            buffer = ""
            for chunk in result:  # type: ignore
                delta = (
                    str(chunk)
                    if not isinstance(chunk, dict)
                    else (
                        chunk.get("text")
                        or chunk.get("content")
                        or chunk.get("generated_text")
                        or str(chunk)
                    )
                )
                buffer += delta
                yield {
                    "prompt": rendered_prompt,
                    "generated_text": buffer,
                    "delta": delta,
                    "citations": citations,
                }
            return

        # Otherwise it's a final scalar result
        final_text = str(result)
        yield {
            "prompt": rendered_prompt,
            "generated_text": final_text,
            "delta": final_text,
            "citations": citations,
        }

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
        When `stream=True` this method will consume the streaming generator and
        return the final result (use `generate_stream` to consume updates).
        """
        if prompt_template is None:
            prompt_template = self._default_prompt

        if stream:
            last: Optional[Dict[str, Any]] = None
            async for update in self.generate_stream(
                query=query,
                prompt_template=prompt_template,
                limit=limit,
                model=model,
                max_tokens=max_tokens,
                temperature=temperature,
                provider_override=provider_override,
                provider_callable=provider_callable,
            ):
                last = update
            if last is None:
                # No output produced
                messages = await self._collect_context(query, limit=limit)
                return {
                    "prompt": render_prompt_template(
                        prompt_template, {"source_messages": self._build_source_messages(messages)}
                    ),
                    "generated_text": "",
                    "citations": [],
                }
            return {
                "prompt": last["prompt"],
                "generated_text": last["generated_text"],
                "citations": last.get("citations", []),
            }

        # Non-streaming path (preserve existing behaviour)
        messages = await self._collect_context(query, limit=limit)
        source_messages = self._build_source_messages(messages)

        rendered_prompt = render_prompt_template(
            prompt_template, {"source_messages": source_messages}
        )

        provider = provider_callable or self._provider
        if provider is None:
            raise RuntimeError("No provider callable configured for generation")

        # Provider contract: await provider(prompt=..., model=..., max_tokens=..., temperature=...)
        generated_text = await provider(
            prompt=rendered_prompt,
            model=model,
            max_tokens=max_tokens,
            temperature=temperature,
            stream=False,
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
