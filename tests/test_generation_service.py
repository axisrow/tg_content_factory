from datetime import datetime

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
        date=datetime.utcnow(),
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
