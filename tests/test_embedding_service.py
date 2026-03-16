from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import AsyncMock

import pytest

from src.models import Message
from src.services.embedding_service import LAST_EMBEDDED_ID_SETTING, EmbeddingService


class FakeEmbeddings:
    async def aembed_documents(self, texts: list[str]) -> list[list[float]]:
        return [self._vectorize(text) for text in texts]

    async def aembed_query(self, query: str) -> list[float]:
        return self._vectorize(query)

    @staticmethod
    def _vectorize(text: str) -> list[float]:
        lowered = text.lower()
        if "crypto" in lowered or "bitcoin" in lowered:
            return [1.0, 0.0]
        if "weather" in lowered:
            return [0.0, 1.0]
        return [0.5, 0.5]


@pytest.mark.asyncio
async def test_embedding_service_indexes_pending_messages_incrementally(db, monkeypatch):
    if not db.vec_available:
        pytest.skip("sqlite-vec extension is unavailable in this environment")

    await db.insert_messages_batch(
        [
            Message(
                channel_id=-100123,
                message_id=1,
                text="Crypto market update",
                date=datetime.now(timezone.utc),
            ),
            Message(
                channel_id=-100123,
                message_id=2,
                text="",
                date=datetime.now(timezone.utc),
            ),
            Message(
                channel_id=-100123,
                message_id=3,
                text="Weather report for today",
                date=datetime.now(timezone.utc),
            ),
        ]
    )

    monkeypatch.setattr(
        EmbeddingService,
        "_get_embeddings",
        AsyncMock(return_value=FakeEmbeddings()),
    )

    service = EmbeddingService(db)
    first_batch = await service.index_pending_messages(batch_size=1, max_batches=1)
    second_batch = await service.index_pending_messages(batch_size=10)

    assert first_batch == 1
    assert second_batch == 1
    assert await db.get_setting(LAST_EMBEDDED_ID_SETTING) is not None
    assert await db.repos.messages.get_embedding_dimensions() == 2

    query_embedding = await service.embed_query("bitcoin rally")
    messages, total = await db.search_semantic_messages(query_embedding, limit=5)

    assert total >= 1
    assert messages[0].text == "Crypto market update"


@pytest.mark.asyncio
async def test_hybrid_search_fuses_keyword_and_semantic_candidates(db):
    if not db.vec_available:
        pytest.skip("sqlite-vec extension is unavailable in this environment")

    await db.insert_messages_batch(
        [
            Message(
                channel_id=-100321,
                message_id=1,
                text="keyword only match",
                date=datetime.now(timezone.utc),
            ),
            Message(
                channel_id=-100321,
                message_id=2,
                text="bitcoin outlook without keyword",
                date=datetime.now(timezone.utc),
            ),
            Message(
                channel_id=-100321,
                message_id=3,
                text="weather bulletin",
                date=datetime.now(timezone.utc),
            ),
        ]
    )

    rows = await db.execute_fetchall("SELECT id, text FROM messages ORDER BY id")
    ids_by_text = {row["text"]: int(row["id"]) for row in rows}
    await db.repos.messages.upsert_message_embeddings(
        [
            (ids_by_text["keyword only match"], [0.0, 1.0]),
            (ids_by_text["bitcoin outlook without keyword"], [1.0, 0.0]),
            (ids_by_text["weather bulletin"], [0.2, 0.2]),
        ]
    )

    messages, total = await db.search_hybrid_messages("keyword", [1.0, 0.0], limit=10)
    texts = [message.text for message in messages]

    assert total >= 2
    assert "keyword only match" in texts
    assert "bitcoin outlook without keyword" in texts
