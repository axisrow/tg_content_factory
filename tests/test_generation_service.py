from datetime import datetime, timezone

import pytest

from src.models import Message, SearchResult
from src.services.generation_service import GenerationService


class DummySearchEngine:
    def __init__(self, messages):
        self._messages = messages

    async def search_hybrid(self, query: str, **kwargs) -> SearchResult:
        return SearchResult(messages=self._messages, total=len(self._messages), query=query)


async def fake_provider(**kwargs):
    # simple fake provider that echoes a predictable string
    return "GENERATED: " + (kwargs.get("prompt") or "")[:40]


async def test_generation_service_basic():
    msg = Message(
        id=1,
        channel_id=10,
        message_id=42,
        sender_id=None,
        sender_name="Alice",
        text="Hello world from test",
        date=datetime.now(timezone.utc),
        collected_at=None,
        channel_title="TestChannel",
        channel_username="testchan",
    )

    engine = DummySearchEngine([msg])
    service = GenerationService(search_engine=engine, provider_callable=fake_provider)

    out = await service.generate(
        query="test query", prompt_template="Use {source_messages}", limit=1
    )

    assert "GENERATED:" in out["generated_text"]
    assert "Hello world from test" in out["prompt"]
    assert isinstance(out["citations"], list)


async def test_generation_service_empty_messages():
    """Test generation with no messages."""
    engine = DummySearchEngine([])
    service = GenerationService(search_engine=engine, provider_callable=fake_provider)

    out = await service.generate(query="test", prompt_template="Use {source_messages}")

    assert "GENERATED:" in out["generated_text"]
    assert out["citations"] == []


async def test_generation_service_message_with_no_text():
    """Test generation when message has no text."""
    msg = Message(
        id=1,
        channel_id=10,
        message_id=42,
        sender_id=None,
        sender_name="Alice",
        text="",  # Empty text
        date=datetime.now(timezone.utc),
        collected_at=None,
        channel_title="TestChannel",
    )

    engine = DummySearchEngine([msg])
    service = GenerationService(search_engine=engine, provider_callable=fake_provider)

    out = await service.generate(query="test", prompt_template="Use {source_messages}")

    # Empty messages should be skipped
    assert "GENERATED:" in out["generated_text"]


async def test_generation_service_no_provider():
    """Test generation without provider raises error."""
    engine = DummySearchEngine([])
    service = GenerationService(search_engine=engine, provider_callable=None)

    with pytest.raises(RuntimeError, match="No provider callable"):
        await service.generate(query="test")


class DummySearchEngineWithFallback:
    """Search engine where semantic is unavailable — simulates issue #270."""

    def __init__(self, messages):
        self._messages = messages
        self._hybrid_called = False
        self._local_called = False

    @property
    def semantic_available(self) -> bool:
        return False

    async def search_hybrid(self, query: str, **kwargs) -> SearchResult:
        self._hybrid_called = True
        raise RuntimeError("Semantic search is unavailable: sqlite-vec extension is not loaded")

    async def search_local(self, query: str, **kwargs) -> SearchResult:
        self._local_called = True
        return SearchResult(messages=self._messages, total=len(self._messages), query=query)


async def test_generation_service_fallback_to_local_when_no_semantic():
    """Issue #270: _collect_context must fall back to FTS when semantic is unavailable."""
    from datetime import datetime, timezone

    msg = Message(
        id=1,
        channel_id=10,
        message_id=1,
        sender_id=None,
        sender_name="Alice",
        text="Test message",
        date=datetime.now(timezone.utc),
        channel_title="Chan",
    )
    engine = DummySearchEngineWithFallback([msg])
    service = GenerationService(search_engine=engine, provider_callable=fake_provider)

    out = await service.generate(query="test query", prompt_template="Use {source_messages}")

    assert engine._local_called, "Should have called search_local as fallback"
    assert not engine._hybrid_called, "Should NOT have called search_hybrid when semantic unavailable"
    assert "GENERATED:" in out["generated_text"]


async def test_generation_service_uses_hybrid_when_semantic_available():
    """When semantic is available, _collect_context should use search_hybrid."""

    class EngineWithSemantic:
        @property
        def semantic_available(self):
            return True

        async def search_hybrid(self, query, **kwargs):
            return SearchResult(messages=[], total=0, query=query)

        async def search_local(self, query, **kwargs):
            raise AssertionError("search_local should not be called when semantic is available")

    engine = EngineWithSemantic()
    service = GenerationService(search_engine=engine, provider_callable=fake_provider)
    out = await service.generate(query="test")
    # Should not raise, hybrid was used
    assert "GENERATED:" in out["generated_text"]


async def test_generation_service_uses_default_prompt():
    """Test generation uses default prompt template."""
    engine = DummySearchEngine([])
    service = GenerationService(
        search_engine=engine,
        provider_callable=fake_provider,
        default_prompt_template="DEFAULT: {source_messages}",
    )

    out = await service.generate(query="test")

    assert "DEFAULT:" in out["prompt"]


async def test_generation_service_with_stream_flag():
    """Test generation with stream=True consumes stream."""
    msg = Message(
        id=1,
        channel_id=10,
        message_id=42,
        sender_id=None,
        sender_name="Alice",
        text="Streaming test",
        date=datetime.now(timezone.utc),
        collected_at=None,
        channel_title="StreamChannel",
    )

    engine = DummySearchEngine([msg])

    async def streaming_provider(**kwargs):
        return "STREAMED: " + (kwargs.get("prompt") or "")[:20]

    service = GenerationService(search_engine=engine, provider_callable=streaming_provider)

    out = await service.generate(query="test", stream=True)

    assert "STREAMED:" in out["generated_text"]


async def test_generation_service_sync_iterator_provider():
    """Test generation with provider that returns a sync iterator."""

    def sync_iterator_provider(**kwargs):
        return iter(["chunk1", "chunk2", "chunk3"])

    engine = DummySearchEngine([])
    service = GenerationService(search_engine=engine, provider_callable=sync_iterator_provider)

    chunks = []
    async for chunk in service.generate_stream(query="test"):
        chunks.append(chunk)

    assert len(chunks) == 3
    assert chunks[-1]["generated_text"] == "chunk1chunk2chunk3"


async def test_generation_service_provider_exception():
    """Test generation handles provider exception."""

    async def failing_provider(**kwargs):
        raise ValueError("Provider error")

    engine = DummySearchEngine([])
    service = GenerationService(search_engine=engine, provider_callable=failing_provider)

    with pytest.raises(ValueError, match="Provider error"):
        await service.generate(query="test")


async def test_generation_service_citation_truncation():
    """Test that citations truncate long text."""
    long_text = "x" * 1000
    msg = Message(
        id=1,
        channel_id=10,
        message_id=42,
        sender_id=None,
        sender_name="Alice",
        text=long_text,
        date=datetime.now(timezone.utc),
        collected_at=None,
        channel_title="TestChannel",
    )

    engine = DummySearchEngine([msg])
    service = GenerationService(search_engine=engine, provider_callable=fake_provider)

    out = await service.generate(query="test")

    # Citation text should be truncated to 512 chars
    assert len(out["citations"][0]["text"]) == 512
