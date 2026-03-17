import asyncio
from datetime import datetime, timezone

from src.models import Message, SearchResult
from src.services.generation_service import GenerationService


class DummySearchEngine:
    def __init__(self, messages):
        self._messages = messages

    async def search_hybrid(self, query: str, **kwargs) -> SearchResult:
        return SearchResult(messages=self._messages, total=len(self._messages), query=query)


async def streaming_provider(prompt: str = "", **kwargs):
    async def _gen():
        # emit three chunks with small awaits to simulate async streaming
        yield "part1 "
        await asyncio.sleep(0)
        yield "part2 "
        await asyncio.sleep(0)
        yield "end"

    return _gen()


async def test_generate_streaming():
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
    svc = GenerationService(search_engine=engine, provider_callable=streaming_provider)

    parts = []
    async for update in svc.generate_stream(
        query="q", prompt_template="Use {source_messages}", limit=1
    ):
        parts.append(update.get("delta"))

    assert "".join(parts) == "part1 part2 end"

    # ensure generate(stream=True) returns final concatenated text
    res = await svc.generate(
        query="q", prompt_template="Use {source_messages}", limit=1, stream=True
    )
    assert res["generated_text"] == "part1 part2 end"
