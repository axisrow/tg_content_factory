"""Live (opt-in) provider smoke for the RAG generation path (issue #1034).

These tests make REAL LLM calls and are billed, so they are skipped unless both
``RUN_REAL_PROVIDER_SMOKE=1`` and a provider API key are set — the same gate the
existing provider smoke uses (``test_provider_runtime_integration.py``). They
prove the fake-provider streaming/RAG flow holds against a real provider; the
fake-provider tests in ``test_generation_service_streaming_edge_cases.py`` and
``test_generation_service_rag_context.py`` are the mandatory CI coverage.

Live runs are owner-consent only and should target a safe model with a tiny
``max_tokens`` budget — no Telegram side effects are involved here, this only
exercises the generation service against the provider HTTP API.
"""
from __future__ import annotations

import os
from datetime import datetime, timezone

import pytest

from src.models import Message, SearchResult
from src.services.generation_service import GenerationService
from src.services.provider_service import RuntimeProviderRegistry as RuntimeProviderService

_LIVE_GATE = os.environ.get("RUN_REAL_PROVIDER_SMOKE") != "1" or not os.environ.get("ZAI_API_KEY")
_LIVE_REASON = "Set RUN_REAL_PROVIDER_SMOKE=1 and ZAI_API_KEY to run live generation smoke."


class _StaticSearchEngine:
    """Returns a fixed context message so retrieval is deterministic for the smoke."""

    def __init__(self, messages):
        self._messages = messages

    async def search_hybrid(self, query: str, **kwargs) -> SearchResult:
        return SearchResult(messages=self._messages, total=len(self._messages), query=query)


def _context_message() -> Message:
    return Message(
        id=1,
        channel_id=10,
        message_id=42,
        sender_id=None,
        sender_name="Author",
        text="The capital of France is Paris.",
        date=datetime.now(timezone.utc),
        collected_at=None,
        channel_title="Facts",
        channel_username="facts",
    )


@pytest.mark.real_provider_smoke
@pytest.mark.skipif(_LIVE_GATE, reason=_LIVE_REASON)
@pytest.mark.anyio
async def test_live_generation_non_stream_smoke():
    """A real provider returns non-empty generated text plus deduped citations."""
    service = RuntimeProviderService(env={"ZAI_API_KEY": os.environ["ZAI_API_KEY"]})
    provider = service.get_provider_callable("zai")

    gen = GenerationService(_StaticSearchEngine([_context_message()]), provider_callable=provider)
    out = await gen.generate(
        query="What is the capital of France?",
        prompt_template="Context:\n{source_messages}\n\nAnswer briefly.",
        model="glm-5-turbo",
        max_tokens=32,
        temperature=0,
    )

    assert isinstance(out["generated_text"], str)
    assert out["generated_text"].strip()
    assert len(out["citations"]) == 1


@pytest.mark.real_provider_smoke
@pytest.mark.skipif(_LIVE_GATE, reason=_LIVE_REASON)
@pytest.mark.anyio
async def test_live_generation_stream_smoke():
    """The streaming path accumulates a non-empty final text against a real provider."""
    service = RuntimeProviderService(env={"ZAI_API_KEY": os.environ["ZAI_API_KEY"]})
    provider = service.get_provider_callable("zai")

    gen = GenerationService(_StaticSearchEngine([_context_message()]), provider_callable=provider)

    last = None
    async for update in gen.generate_stream(
        query="What is the capital of France?",
        prompt_template="Context:\n{source_messages}\n\nAnswer briefly.",
        model="glm-5-turbo",
        max_tokens=32,
        temperature=0,
    ):
        last = update

    assert last is not None
    assert last["generated_text"].strip()
    # A clean live stream must not be flagged partial (the mid-stream-break path).
    assert last.get("partial") in (None, False)
