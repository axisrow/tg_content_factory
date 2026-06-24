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
    tests can mock providers and later integration can wire RuntimeProviderRegistry.
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
        # Resolve the index-aware semantic gate once; None for search backends that don't expose it
        # (e.g. test doubles) so _collect_context falls back to the legacy semantic_available check.
        self._search_has_semantic_index: Callable[[], Awaitable[bool]] | None = getattr(
            search_engine, "has_semantic_index", None
        )

    async def _collect_context(
        self, query: str, limit: int = 8, channel_id: int | None = None
    ) -> List[Message]:
        """Retrieve context messages using the SearchEngine.

        Uses hybrid (semantic+FTS) search when embeddings are available, falls back to
        FTS/LIKE local search otherwise so that pipelines still work without a vector backend.
        """
        import logging

        # Gate on an actual semantic index (embeddings indexed), not merely numpy being importable:
        # search_hybrid embeds the query via an external provider before checking the index, so a
        # never-indexed (LLM-only) deployment must use local search to avoid an external embedding call.
        has_index = self._search_has_semantic_index
        if has_index is not None:
            use_hybrid = await has_index()
        else:
            use_hybrid = getattr(self._search, "semantic_available", True)
        if use_hybrid:
            try:
                result: SearchResult = await self._search.search_hybrid(
                    query, channel_id=channel_id, limit=limit
                )
                return result.messages
            except RuntimeError as exc:
                logging.getLogger(__name__).warning(
                    "Hybrid search failed (%s), falling back to FTS local search for context retrieval",
                    exc,
                )
        else:
            logging.getLogger(__name__).warning(
                "Semantic search unavailable or not indexed, falling back to FTS local search for context retrieval"
            )
        result = await self._search.search_local(query, channel_id=channel_id, limit=limit)
        return result.messages

    @staticmethod
    def _extract_delta(chunk: Any) -> str:
        """Coerce a streaming chunk into its text delta, mapping "empty" to "".

        Returns ``""`` for ``None`` and for dict chunks whose text fields are all
        absent/``None`` so callers can skip them — this prevents the literal
        ``"None"`` (from ``str(None)``) from leaking into the generated text and
        avoids emitting do-nothing deltas (issue #1034).
        """
        if chunk is None:
            return ""
        if isinstance(chunk, dict):
            text = chunk.get("text") or chunk.get("content") or chunk.get("generated_text")
            return text if isinstance(text, str) else ""
        return str(chunk)

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

    @staticmethod
    def _build_citations(messages: List[Message]) -> List[Dict[str, Any]]:
        """Bake citations from retrieved messages, deduplicated in retrieval order.

        Hybrid + FTS retrieval can surface the same message twice, which used to
        emit duplicate citations (issue #1034). Dedup on ``(channel_id,
        message_id)`` — ``message_id`` is only unique per channel, so the channel
        must be part of the key to keep same-id messages from different channels
        distinct. First occurrence wins, preserving retrieval order.
        """
        citations: List[Dict[str, Any]] = []
        seen: set[tuple[int | None, int]] = set()
        for m in messages:
            key = (m.channel_id, m.message_id)
            if key in seen:
                continue
            seen.add(key)
            citations.append(
                {
                    "channel_title": (m.channel_title or m.channel_username or ""),
                    "message_id": m.message_id,
                    "text": (m.text or "")[:512],
                    "date": m.date.isoformat() if isinstance(m.date, datetime) else str(m.date),
                }
            )
        return citations

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
        channel_id: int | None = None,
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

        messages = await self._collect_context(query, limit=limit, channel_id=channel_id)
        source_messages = self._build_source_messages(messages)

        rendered_prompt = render_prompt_template(
            prompt_template, {"source_messages": source_messages}
        )

        provider = provider_callable or self._provider
        if provider is None:
            raise RuntimeError("No provider callable configured for generation")

        # Citations baked from retrieved messages (deduplicated, see #1034)
        citations = self._build_citations(messages)

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
            import logging

            buffer = ""
            try:
                async for chunk in result:  # type: ignore
                    delta = self._extract_delta(chunk)
                    if not delta:
                        # Empty/None chunks carry no text — skip so they neither
                        # pollute the buffer (``str(None)`` → ``"None"``) nor emit
                        # a do-nothing delta downstream (issue #1034).
                        continue
                    buffer += delta
                    yield {
                        "prompt": rendered_prompt,
                        "generated_text": buffer,
                        "delta": delta,
                        "citations": citations,
                    }
            except Exception as exc:
                # Provider closed the connection mid-stream / timed out. Return the
                # partial text gracefully instead of losing it (owner decision,
                # issue #1034). A trailing update carries the accumulated buffer
                # plus the error markers so consumers can persist what arrived.
                logging.getLogger(__name__).warning(
                    "Streaming provider failed mid-stream (%s); returning partial text", exc
                )
                yield {
                    "prompt": rendered_prompt,
                    "generated_text": buffer,
                    "delta": "",
                    "citations": citations,
                    "partial": True,
                    "stream_error": str(exc) or exc.__class__.__name__,
                }
            return

        # If it's a synchronous iterator (list/iter), iterate synchronously
        if hasattr(result, "__iter__") and not isinstance(result, (str, bytes)):
            buffer = ""
            for chunk in result:  # type: ignore
                delta = self._extract_delta(chunk)
                if not delta:
                    continue
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
        channel_id: int | None = None,
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
                channel_id=channel_id,
            ):
                last = update
            if last is None:
                # No output produced
                messages = await self._collect_context(query, limit=limit, channel_id=channel_id)
                return {
                    "prompt": render_prompt_template(
                        prompt_template, {"source_messages": self._build_source_messages(messages)}
                    ),
                    "generated_text": "",
                    "citations": [],
                }
            if last.get("stream_error"):
                # The stream ended on a mid-stream provider failure. generate_stream
                # surfaces partial text gracefully for interactive consumers, but this
                # aggregating API must NOT report a truncated run as success — raise so
                # callers (e.g. ContentGenerationService) mark the run failed rather
                # than persisting partial text as a completed generation (issue #1034,
                # cycle-review).
                raise RuntimeError(f"Streaming generation failed: {last['stream_error']}")
            return {
                "prompt": last["prompt"],
                "generated_text": last["generated_text"],
                "citations": last.get("citations", []),
            }

        # Non-streaming path (preserve existing behaviour)
        messages = await self._collect_context(query, limit=limit, channel_id=channel_id)
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

        citations = self._build_citations(messages)

        return {
            "prompt": rendered_prompt,
            "generated_text": generated_text,
            "citations": citations,
        }
