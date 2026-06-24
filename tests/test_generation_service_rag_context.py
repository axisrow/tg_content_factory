"""RAG-context coverage for ``GenerationService`` and ``resolve_retrieval_scope`` (issue #1034).

Tier-2 bug-hunt, retrieval side:
  * citation deduplication — the same message returned twice used to produce
    duplicate citation entries (bug, fixed here);
  * null ``Message`` fields (``channel_title=None``/``channel_username=None``)
    must degrade gracefully — regression guard, proven by mutation;
  * ``resolve_retrieval_scope`` must fail closed (#1077): a scope lookup error
    raises ``PipelineScopeError`` (never silently widens to all channels), and a
    multi-source pipeline legitimately leaves ``channel_id=None`` — regression
    guards (cf. #1037/#1077 fail-closed).
"""
from __future__ import annotations

from datetime import datetime, timezone

import pytest

from src.models import (
    ContentPipeline,
    Message,
    PipelineGenerationBackend,
    PipelinePublishMode,
    SearchResult,
)
from src.services.generation_service import GenerationService
from src.services.pipeline_service import resolve_retrieval_scope


class DummySearchEngine:
    def __init__(self, messages):
        self._messages = messages

    async def search_hybrid(self, query: str, **kwargs) -> SearchResult:
        return SearchResult(messages=self._messages, total=len(self._messages), query=query)


def _msg(
    message_id: int = 42,
    *,
    channel_id: int = 10,
    title: str | None = "C",
    username: str | None = "u",
    text: str = "hi",
) -> Message:
    return Message(
        id=1,
        channel_id=channel_id,
        message_id=message_id,
        sender_id=None,
        sender_name="A",
        text=text,
        date=datetime.now(timezone.utc),
        collected_at=None,
        channel_title=title,
        channel_username=username,
    )


async def _provider(**kwargs) -> str:
    return "ok"


# ---------------------------------------------------------------------------
# Bug C: citation deduplication
# ---------------------------------------------------------------------------


async def test_citations_deduplicated_for_repeated_message():
    """The same (channel_id, message_id) returned twice yields ONE citation.

    Pre-fix retrieval that returned the same message twice (hybrid+FTS overlap)
    produced two identical citation entries. The exact count assertion (``== 1``)
    is deliberate — a ``>= 1`` would pass on the buggy two-citation output.
    """
    same = [_msg(message_id=42), _msg(message_id=42)]
    svc = GenerationService(DummySearchEngine(same), provider_callable=_provider)

    out = await svc.generate(query="q", prompt_template="Use {source_messages}")

    assert len(out["citations"]) == 1, "duplicate message must not duplicate the citation"
    assert out["citations"][0]["message_id"] == 42


async def test_citations_keep_distinct_messages_in_order():
    """Distinct messages stay distinct and keep retrieval order (no over-dedup).

    Guards against a fix that collapses *different* messages — mutating the dedup
    key to a constant would shrink this to one citation and fail.
    """
    msgs = [_msg(message_id=1), _msg(message_id=2), _msg(message_id=3)]
    svc = GenerationService(DummySearchEngine(msgs), provider_callable=_provider)

    out = await svc.generate(query="q", prompt_template="Use {source_messages}")

    assert [c["message_id"] for c in out["citations"]] == [1, 2, 3]


async def test_citations_distinguish_same_id_across_channels():
    """Same message_id in different channels are NOT collapsed.

    message_id is only unique per channel, so the dedup key must include
    channel_id — otherwise cross-channel citations would be lost.
    """
    msgs = [_msg(message_id=7, channel_id=100), _msg(message_id=7, channel_id=200)]
    svc = GenerationService(DummySearchEngine(msgs), provider_callable=_provider)

    out = await svc.generate(query="q", prompt_template="Use {source_messages}")

    assert len(out["citations"]) == 2


async def test_streaming_citations_also_deduplicated():
    """The streaming path bakes the same citations, so dedup must apply there too."""
    same = [_msg(message_id=42), _msg(message_id=42)]

    async def streaming_provider(prompt: str = "", **kwargs):
        async def _gen():
            yield "chunk"

        return _gen()

    svc = GenerationService(DummySearchEngine(same), provider_callable=streaming_provider)

    last = None
    async for update in svc.generate_stream(query="q", prompt_template="Use {source_messages}"):
        last = update

    assert last is not None
    assert len(last["citations"]) == 1


# ---------------------------------------------------------------------------
# Regression guard: null Message fields degrade gracefully
# ---------------------------------------------------------------------------


async def test_null_channel_fields_do_not_crash():
    """A message with channel_title=None AND channel_username=None is handled.

    The ``or ""`` fallbacks in citation/source building keep this graceful;
    mutating them away would surface ``None`` into the citation and (for the
    prompt) crash the f-string join. The assertions pin both behaviours.
    """
    msg = _msg(title=None, username=None, text="content here")
    svc = GenerationService(DummySearchEngine([msg]), provider_callable=_provider)

    out = await svc.generate(query="q", prompt_template="Use {source_messages}")

    assert out["citations"][0]["channel_title"] == ""
    assert "content here" in out["prompt"]


# ---------------------------------------------------------------------------
# Regression guards: resolve_retrieval_scope fails closed
# ---------------------------------------------------------------------------


def _pipeline() -> ContentPipeline:
    return ContentPipeline(
        id=7,
        name="MyPipe",
        prompt_template="p",
        llm_model="m",
        generation_backend=PipelineGenerationBackend.CHAIN,
        publish_mode=PipelinePublishMode.MODERATED,
    )


class _Source:
    def __init__(self, channel_id: int) -> None:
        self.channel_id = channel_id


async def test_scope_lookup_error_fails_closed():
    """A failing list_sources must FAIL CLOSED with PipelineScopeError — never
    degrade to channel_id=None (which is unscoped retrieval across ALL channels,
    a content-isolation breach). #1077 changes the old swallow-and-widen
    behaviour this test previously documented."""
    from src.services.pipeline_service import PipelineScopeError

    async def failing_list_sources(pipeline_id: int):
        raise RuntimeError("DB scope lookup failed")

    with pytest.raises(PipelineScopeError):
        await resolve_retrieval_scope(_pipeline(), failing_list_sources)


async def test_scope_single_source_sets_channel():
    """A single source scopes retrieval to that channel (positive control)."""

    async def one_source(pipeline_id: int):
        return [_Source(555)]

    scope = await resolve_retrieval_scope(_pipeline(), one_source)

    assert scope.channel_id == 555


async def test_scope_multi_source_fails_closed():
    """Multiple sources must leave channel_id=None — no cross-channel leak.

    Fail-closed scoping (cf. #1037/#1077): with >1 source there is no single
    channel to scope to, so retrieval must stay unscoped rather than silently
    pick one source's channel.
    """

    async def two_sources(pipeline_id: int):
        return [_Source(111), _Source(222)]

    scope = await resolve_retrieval_scope(_pipeline(), two_sources)

    assert scope.channel_id is None


async def test_scope_no_list_sources_returns_query_only():
    """Without a list_sources callable, scope is query-only (channel_id=None)."""
    scope = await resolve_retrieval_scope(_pipeline(), None)

    assert scope.query == "MyPipe"
    assert scope.channel_id is None
