"""Tests for LocalSearch numpy fallback and hybrid paths."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.models import Message, SearchResult
from src.search.local_search import LocalSearch


def _mock_search_bundle(**overrides):
    bundle = MagicMock()
    bundle.search_messages = AsyncMock(return_value=([], 0))
    bundle.vec_available = overrides.get("vec_available", False)
    bundle.numpy_available = overrides.get("numpy_available", True)
    return bundle


def _mock_embedding_service():
    svc = MagicMock()
    svc.embed_query = AsyncMock(return_value=[0.1, 0.2, 0.3])
    return svc


def _make_message(msg_id=1, channel_id=100, text="hello", date="2026-01-01T00:00:00"):
    return Message(
        id=msg_id,
        channel_id=channel_id,
        message_id=msg_id * 10,
        text=text,
        date=date,
        sender_id=1,
    )


async def test_search_basic():
    bundle = _mock_search_bundle()
    ls = LocalSearch(bundle)
    result = await ls.search("test")
    assert isinstance(result, SearchResult)
    assert result.total == 0


async def test_search_with_channel_filter():
    bundle = _mock_search_bundle()
    ls = LocalSearch(bundle)
    await ls.search("test", channel_id=100)
    bundle.search_messages.assert_called_once()


async def test_search_semantic_no_embedding_service():
    bundle = _mock_search_bundle()
    ls = LocalSearch(bundle, embedding_service=None)
    with pytest.raises(RuntimeError, match="unavailable"):
        await ls.search_semantic("test")


async def test_search_semantic_with_vec():
    bundle = _mock_search_bundle(vec_available=True)
    embedding = _mock_embedding_service()
    bundle.messages = MagicMock()
    bundle.messages.search_semantic_messages = AsyncMock(return_value=([], 0))
    ls = LocalSearch(bundle, embedding_service=embedding)
    result = await ls.search_semantic("test")
    assert result.total == 0


async def test_search_semantic_numpy_no_numpy():
    bundle = _mock_search_bundle(numpy_available=False, vec_available=False)
    embedding = _mock_embedding_service()
    ls = LocalSearch(bundle, embedding_service=embedding)
    with pytest.raises(RuntimeError, match="unavailable"):
        await ls.search_semantic("test")


async def test_search_semantic_numpy_empty_index():
    bundle = _mock_search_bundle(vec_available=False, numpy_available=True)
    embedding = _mock_embedding_service()

    mock_index = MagicMock()
    mock_index.size = 0

    ls = LocalSearch(bundle, embedding_service=embedding)
    ls._numpy_index = mock_index
    ls._numpy_index_loaded = True

    # _ensure_numpy_index returns the index from self._numpy_index
    with patch.object(ls, "_ensure_numpy_index", return_value=mock_index):
        # Need to mock _db.execute for filtered channels query
        mock_cursor = MagicMock()
        mock_cursor.fetchall = AsyncMock(return_value=[])
        bundle.messages._db = MagicMock()
        bundle.messages._db.execute = AsyncMock(return_value=mock_cursor)

        # Make _ensure_numpy_index an async mock returning the index
        ls._ensure_numpy_index = AsyncMock(return_value=mock_index)

        result = await ls.search_semantic("test")
        assert result.total == 0


async def test_invalidate_numpy_index():
    bundle = _mock_search_bundle()
    ls = LocalSearch(bundle)
    ls._numpy_index = MagicMock()
    ls._numpy_index_loaded = True
    ls.invalidate_numpy_index()
    assert ls._numpy_index is None
    assert ls._numpy_index_loaded is False


async def test_search_hybrid_no_embedding():
    bundle = _mock_search_bundle()
    ls = LocalSearch(bundle, embedding_service=None)
    with pytest.raises(RuntimeError, match="Hybrid search is unavailable"):
        await ls.search_hybrid("test")


async def test_search_hybrid_with_embedding():
    bundle = _mock_search_bundle()
    embedding = _mock_embedding_service()
    bundle.messages = MagicMock()
    bundle.messages.search_hybrid_messages = AsyncMock(return_value=([], 0))
    ls = LocalSearch(bundle, embedding_service=embedding)
    result = await ls.search_hybrid("test")
    assert result.total == 0


async def test_search_hybrid_with_filters():
    bundle = _mock_search_bundle()
    embedding = _mock_embedding_service()
    bundle.messages = MagicMock()
    bundle.messages.search_hybrid_messages = AsyncMock(return_value=([], 0))
    ls = LocalSearch(bundle, embedding_service=embedding)
    result = await ls.search_hybrid(
        "test", channel_id=1, date_from="2026-01-01", date_to="2026-12-31",
        limit=10, offset=5, min_length=10, max_length=100,
    )
    assert result.total == 0
