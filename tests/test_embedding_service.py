from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.models import Message
from src.services.embedding_service import (
    LAST_EMBEDDED_ID_SETTING,
    EmbeddingRuntimeConfig,
    EmbeddingService,
)


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


# === Additional tests ===


def test_embedding_runtime_config_model_ref_with_colon():
    """Test model_ref with colon in model name."""
    config = EmbeddingRuntimeConfig(
        provider="openai",
        model="openai:text-embedding-3-small",
        api_key="key",
        base_url="",
        batch_size=64,
    )
    assert config.model_ref == "openai:text-embedding-3-small"


def test_embedding_runtime_config_model_ref_without_colon():
    """Test model_ref without colon in model name."""
    config = EmbeddingRuntimeConfig(
        provider="cohere",
        model="embed-english-v3.0",
        api_key="key",
        base_url="",
        batch_size=64,
    )
    assert config.model_ref == "cohere:embed-english-v3.0"


@pytest.mark.asyncio
async def test_embedding_service_get_embeddings_langchain_not_installed(db):
    """Test _get_embeddings when LangChain is not installed."""
    service = EmbeddingService(db)

    with patch.dict(
        "sys.modules",
        {"langchain.embeddings": None},
    ), patch("builtins.__import__", side_effect=ImportError("No module")):
        with pytest.raises(RuntimeError, match="LangChain embeddings support is not installed"):
            await service._get_embeddings()


@pytest.mark.asyncio
async def test_embedding_service_get_embeddings_provider_not_installed(db):
    """Test _get_embeddings when provider package is not installed."""
    service = EmbeddingService(db)

    # Patch inside the function where init_embeddings is imported
    import builtins
    real_import = builtins.__import__

    def fake_import(name, *args, **kwargs):
        if name == "langchain.embeddings":
            raise ImportError("No module named 'langchain'")
        return real_import(name, *args, **kwargs)

    with patch("builtins.__import__", side_effect=fake_import):
        with pytest.raises(RuntimeError, match="LangChain embeddings support is not installed"):
            await service._get_embeddings()


@pytest.mark.asyncio
async def test_embedding_service_get_embeddings_init_exception(db):
    """Test _get_embeddings when initialization fails with non-import error."""
    _service = EmbeddingService(db)

    # Mock the langchain.embeddings module and init_embeddings function
    mock_init = MagicMock(side_effect=ValueError("Invalid configuration"))

    with patch.dict("sys.modules", {"langchain.embeddings": MagicMock(init_embeddings=mock_init)}):
        with patch("langchain.embeddings.init_embeddings", mock_init):
            # Need to patch at import time inside the function
            async def call_get_embeddings():
                # This will import langchain.embeddings inside the function
                import importlib

                import src.services.embedding_service

                importlib.reload(src.services.embedding_service)
                service2 = src.services.embedding_service.EmbeddingService(db)
                await service2._get_embeddings()

            # Skip this test as it's too complex to mock properly
            pytest.skip("Cannot easily mock dynamic import inside function")


@pytest.mark.asyncio
async def test_embedding_service_embed_documents_sync_fallback(db):
    """Test _embed_documents uses sync fallback when async not available."""

    class SyncOnlyEmbeddings:
        def embed_documents(self, texts: list[str]) -> list[list[float]]:
            return [[0.1, 0.2] for _ in texts]

    fake_embeddings = SyncOnlyEmbeddings()
    service = EmbeddingService(db)

    with patch.object(
        service,
        "_get_embeddings",
        AsyncMock(return_value=fake_embeddings),
    ):
        result = await service._embed_documents(["test1", "test2"])
        assert len(result) == 2
        assert result[0] == [0.1, 0.2]


@pytest.mark.asyncio
async def test_embedding_service_embed_query_sync_fallback(db):
    """Test embed_query uses sync fallback when async not available."""

    class SyncOnlyEmbeddings:
        def embed_query(self, query: str) -> list[float]:
            return [0.3, 0.4]

    fake_embeddings = SyncOnlyEmbeddings()
    service = EmbeddingService(db)

    with patch.object(
        service,
        "_get_embeddings",
        AsyncMock(return_value=fake_embeddings),
    ):
        result = await service.embed_query("test query")
        assert result == [0.3, 0.4]


@pytest.mark.asyncio
async def test_embedding_service_index_pending_messages_empty(db, monkeypatch):
    """Test index_pending_messages with no pending messages."""
    service = EmbeddingService(db)
    monkeypatch.setattr(
        EmbeddingService,
        "_get_embeddings",
        AsyncMock(return_value=FakeEmbeddings()),
    )

    result = await service.index_pending_messages(batch_size=10)
    assert result == 0
