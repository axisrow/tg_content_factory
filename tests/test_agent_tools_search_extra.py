"""Tests for src/agent/tools/search.py — index_messages, Telegram/hybrid search, _render paths."""
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.database import Database
from tests.agent_tools_helpers import _get_tool_handlers, _text


@pytest.fixture
def mock_db():
    return MagicMock(spec=Database)


@pytest.fixture
def mock_pool():
    pool = MagicMock()
    pool.get_native_client_by_phone = AsyncMock(return_value=None)
    return pool


# ---------------------------------------------------------------------------
# index_messages tool  (lines 123-130)
# ---------------------------------------------------------------------------


class TestIndexMessagesTool:
    @pytest.mark.asyncio
    async def test_success(self, mock_db):
        with patch(
            "src.services.embedding_service.EmbeddingService"
        ) as mock_cls:
            mock_instance = MagicMock()
            mock_instance.index_pending_messages = AsyncMock(return_value=42)
            mock_cls.return_value = mock_instance

            handlers = _get_tool_handlers(mock_db)
            result = await handlers["index_messages"]({})

        text = _text(result)
        assert "42" in text

    @pytest.mark.asyncio
    async def test_error_returns_text(self, mock_db):
        with patch(
            "src.services.embedding_service.EmbeddingService"
        ) as mock_cls:
            mock_instance = MagicMock()
            mock_instance.index_pending_messages = AsyncMock(
                side_effect=RuntimeError("disk full")
            )
            mock_cls.return_value = mock_instance

            handlers = _get_tool_handlers(mock_db)
            result = await handlers["index_messages"]({})

        text = _text(result)
        assert "Ошибка индексации" in text
        assert "disk full" in text


# ---------------------------------------------------------------------------
# search_telegram tool  (lines 170-180)
# ---------------------------------------------------------------------------


class TestSearchTelegramTool:
    @pytest.mark.asyncio
    async def test_no_pool_returns_error(self, mock_db):
        handlers = _get_tool_handlers(mock_db, client_pool=None)
        result = await handlers["search_telegram"]({"query": "test"})
        assert "требует Telegram-клиент" in _text(result)

    @pytest.mark.asyncio
    async def test_with_results(self, mock_db, mock_pool):
        fake_result = SimpleNamespace(
            query="cats",
            messages=[
                SimpleNamespace(channel_id=100, message_id=1, text="Cats are great", date="2025-01-01")
            ],
            total=1,
            error=None,
        )

        with (
            patch("src.services.embedding_service.EmbeddingService"),
            patch("src.search.engine.SearchEngine") as mock_se,
        ):
            se_instance = mock_se.return_value
            se_instance.search_telegram = AsyncMock(return_value=fake_result)

            handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
            result = await handlers["search_telegram"]({"query": "cats", "limit": 10})

        text = _text(result)
        assert "Найдено 1" in text
        assert "Cats are great" in text

    @pytest.mark.asyncio
    async def test_empty_result(self, mock_db, mock_pool):
        fake_result = SimpleNamespace(
            query="nothing",
            messages=[],
            total=0,
            error=None,
        )

        with (
            patch("src.services.embedding_service.EmbeddingService"),
            patch("src.search.engine.SearchEngine") as mock_se,
        ):
            se_instance = mock_se.return_value
            se_instance.search_telegram = AsyncMock(return_value=fake_result)

            handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
            result = await handlers["search_telegram"]({"query": "nothing"})

        text = _text(result)
        assert "Ничего не найдено" in text

    @pytest.mark.asyncio
    async def test_result_with_error_field(self, mock_db, mock_pool):
        fake_result = SimpleNamespace(
            query="fail",
            messages=[],
            total=0,
            error="Premium required",
        )

        with (
            patch("src.services.embedding_service.EmbeddingService"),
            patch("src.search.engine.SearchEngine") as mock_se,
        ):
            se_instance = mock_se.return_value
            se_instance.search_telegram = AsyncMock(return_value=fake_result)

            handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
            result = await handlers["search_telegram"]({"query": "fail"})

        text = _text(result)
        assert "Premium required" in text

    @pytest.mark.asyncio
    async def test_exception_returns_error_text(self, mock_db, mock_pool):
        with (
            patch("src.services.embedding_service.EmbeddingService"),
            patch("src.search.engine.SearchEngine") as mock_se,
        ):
            se_instance = mock_se.return_value
            se_instance.search_telegram = AsyncMock(side_effect=Exception("timeout"))

            handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
            result = await handlers["search_telegram"]({"query": "cats"})

        text = _text(result)
        assert "Ошибка Telegram-поиска" in text
        assert "timeout" in text


# ---------------------------------------------------------------------------
# search_my_chats tool  (lines 195-205)
# ---------------------------------------------------------------------------


class TestSearchMyChatsTool:
    @pytest.mark.asyncio
    async def test_no_pool_returns_error(self, mock_db):
        handlers = _get_tool_handlers(mock_db, client_pool=None)
        result = await handlers["search_my_chats"]({"query": "test"})
        assert "требует Telegram-клиент" in _text(result)

    @pytest.mark.asyncio
    async def test_with_results(self, mock_db, mock_pool):
        fake_result = SimpleNamespace(
            query="hello",
            messages=[
                SimpleNamespace(channel_id=0, message_id=5, text="Hello world", date="2025-03-01")
            ],
            total=1,
            error=None,
        )

        with (
            patch("src.services.embedding_service.EmbeddingService"),
            patch("src.search.engine.SearchEngine") as mock_se,
        ):
            se_instance = mock_se.return_value
            se_instance.search_my_chats = AsyncMock(return_value=fake_result)

            handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
            result = await handlers["search_my_chats"]({"query": "hello", "limit": 20})

        text = _text(result)
        assert "Найдено 1" in text
        assert "Hello world" in text

    @pytest.mark.asyncio
    async def test_error_field_rendered(self, mock_db, mock_pool):
        fake_result = SimpleNamespace(
            query="x",
            messages=[],
            total=0,
            error="Not authenticated",
        )

        with (
            patch("src.services.embedding_service.EmbeddingService"),
            patch("src.search.engine.SearchEngine") as mock_se,
        ):
            se_instance = mock_se.return_value
            se_instance.search_my_chats = AsyncMock(return_value=fake_result)

            handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
            result = await handlers["search_my_chats"]({"query": "x"})

        text = _text(result)
        assert "Not authenticated" in text

    @pytest.mark.asyncio
    async def test_exception_returns_error_text(self, mock_db, mock_pool):
        with (
            patch("src.services.embedding_service.EmbeddingService"),
            patch("src.search.engine.SearchEngine") as mock_se,
        ):
            se_instance = mock_se.return_value
            se_instance.search_my_chats = AsyncMock(side_effect=Exception("network"))

            handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
            result = await handlers["search_my_chats"]({"query": "test"})

        text = _text(result)
        assert "Ошибка поиска по чатам" in text


# ---------------------------------------------------------------------------
# search_in_channel tool  (lines 225-240)
# ---------------------------------------------------------------------------


class TestSearchInChannelTool:
    @pytest.mark.asyncio
    async def test_no_pool_returns_error(self, mock_db):
        handlers = _get_tool_handlers(mock_db, client_pool=None)
        result = await handlers["search_in_channel"]({"channel_id": 100, "query": "test"})
        assert "требует Telegram-клиент" in _text(result)

    @pytest.mark.asyncio
    async def test_missing_channel_id(self, mock_db, mock_pool):
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["search_in_channel"]({"query": "test"})
        assert "channel_id обязателен" in _text(result)

    @pytest.mark.asyncio
    async def test_with_results(self, mock_db, mock_pool):
        fake_result = SimpleNamespace(
            query="news",
            messages=[
                SimpleNamespace(channel_id=100, message_id=10, text="Breaking news", date="2025-06-01")
            ],
            total=1,
            error=None,
        )

        with (
            patch("src.services.embedding_service.EmbeddingService"),
            patch("src.search.engine.SearchEngine") as mock_se,
        ):
            se_instance = mock_se.return_value
            se_instance.search_in_channel = AsyncMock(return_value=fake_result)

            handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
            result = await handlers["search_in_channel"](
                {"channel_id": 100, "query": "news", "limit": 10}
            )

        text = _text(result)
        assert "Breaking news" in text

    @pytest.mark.asyncio
    async def test_error_field_rendered(self, mock_db, mock_pool):
        fake_result = SimpleNamespace(
            query="x",
            messages=[],
            total=0,
            error="Channel not found",
        )

        with (
            patch("src.services.embedding_service.EmbeddingService"),
            patch("src.search.engine.SearchEngine") as mock_se,
        ):
            se_instance = mock_se.return_value
            se_instance.search_in_channel = AsyncMock(return_value=fake_result)

            handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
            result = await handlers["search_in_channel"](
                {"channel_id": 999, "query": "x"}
            )

        text = _text(result)
        assert "Channel not found" in text

    @pytest.mark.asyncio
    async def test_exception_returns_error_text(self, mock_db, mock_pool):
        with (
            patch("src.services.embedding_service.EmbeddingService"),
            patch("src.search.engine.SearchEngine") as mock_se,
        ):
            se_instance = mock_se.return_value
            se_instance.search_in_channel = AsyncMock(side_effect=Exception("flood"))

            handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
            result = await handlers["search_in_channel"](
                {"channel_id": 100, "query": "test"}
            )

        text = _text(result)
        assert "Ошибка поиска в канале" in text


# ---------------------------------------------------------------------------
# search_hybrid tool  (lines 263-283)
# ---------------------------------------------------------------------------


class TestSearchHybridTool:
    @pytest.mark.asyncio
    async def test_with_results(self, mock_db, mock_pool):
        fake_result = SimpleNamespace(
            query="ai",
            messages=[
                SimpleNamespace(channel_id=100, message_id=1, text="AI is future", date="2025-01-01")
            ],
            total=1,
            error=None,
        )

        with (
            patch("src.services.embedding_service.EmbeddingService"),
            patch("src.search.engine.SearchEngine") as mock_se,
        ):
            se_instance = mock_se.return_value
            se_instance.search_hybrid = AsyncMock(return_value=fake_result)

            handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
            result = await handlers["search_hybrid"](
                {"query": "ai", "limit": 5, "channel_id": 100}
            )

        text = _text(result)
        assert "Найдено 1" in text
        assert "AI is future" in text
        se_instance.search_hybrid.assert_awaited_once()
        call_kwargs = se_instance.search_hybrid.await_args.kwargs
        assert call_kwargs["channel_id"] == 100
        assert call_kwargs["limit"] == 5

    @pytest.mark.asyncio
    async def test_empty_result(self, mock_db, mock_pool):
        fake_result = SimpleNamespace(
            query="nothing",
            messages=[],
            total=0,
            error=None,
        )

        with (
            patch("src.services.embedding_service.EmbeddingService"),
            patch("src.search.engine.SearchEngine") as mock_se,
        ):
            se_instance = mock_se.return_value
            se_instance.search_hybrid = AsyncMock(return_value=fake_result)

            handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
            result = await handlers["search_hybrid"]({"query": "nothing"})

        text = _text(result)
        assert "Ничего не найдено" in text

    @pytest.mark.asyncio
    async def test_error_field_rendered(self, mock_db, mock_pool):
        fake_result = SimpleNamespace(
            query="x",
            messages=[],
            total=0,
            error="Index not built",
        )

        with (
            patch("src.services.embedding_service.EmbeddingService"),
            patch("src.search.engine.SearchEngine") as mock_se,
        ):
            se_instance = mock_se.return_value
            se_instance.search_hybrid = AsyncMock(return_value=fake_result)

            handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
            result = await handlers["search_hybrid"]({"query": "x"})

        text = _text(result)
        assert "Index not built" in text

    @pytest.mark.asyncio
    async def test_exception_returns_error_text(self, mock_db, mock_pool):
        with (
            patch("src.services.embedding_service.EmbeddingService"),
            patch("src.search.engine.SearchEngine") as mock_se,
        ):
            se_instance = mock_se.return_value
            se_instance.search_hybrid = AsyncMock(side_effect=Exception("OOM"))

            handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
            result = await handlers["search_hybrid"]({"query": "test"})

        text = _text(result)
        assert "Ошибка гибридного поиска" in text
        assert "OOM" in text

    @pytest.mark.asyncio
    async def test_optional_filters_passed(self, mock_db, mock_pool):
        fake_result = SimpleNamespace(
            query="test",
            messages=[],
            total=0,
            error=None,
        )

        with (
            patch("src.services.embedding_service.EmbeddingService"),
            patch("src.search.engine.SearchEngine") as mock_se,
        ):
            se_instance = mock_se.return_value
            se_instance.search_hybrid = AsyncMock(return_value=fake_result)

            handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
            await handlers["search_hybrid"]({
                "query": "test",
                "limit": 10,
                "channel_id": 200,
                "date_from": "2025-01-01",
                "date_to": "2025-12-31",
                "min_length": 50,
                "max_length": 500,
            })

        call_kwargs = se_instance.search_hybrid.await_args.kwargs
        assert call_kwargs["channel_id"] == 200
        assert call_kwargs["date_from"] == "2025-01-01"
        assert call_kwargs["date_to"] == "2025-12-31"
        assert call_kwargs["min_length"] == 50
        assert call_kwargs["max_length"] == 500

    @pytest.mark.asyncio
    async def test_no_client_pool_hybrid_still_works(self, mock_db):
        """search_hybrid does not require client_pool (local DB only)."""
        fake_result = SimpleNamespace(
            query="local",
            messages=[],
            total=0,
            error=None,
        )

        with (
            patch("src.services.embedding_service.EmbeddingService"),
            patch("src.search.engine.SearchEngine") as mock_se,
        ):
            se_instance = mock_se.return_value
            se_instance.search_hybrid = AsyncMock(return_value=fake_result)

            handlers = _get_tool_handlers(mock_db, client_pool=None)
            result = await handlers["search_hybrid"]({"query": "local"})

        text = _text(result)
        assert "Ничего не найдено" in text


# ---------------------------------------------------------------------------
# search_messages with optional filters  (lines 60-68 deeper coverage)
# ---------------------------------------------------------------------------


class TestSearchMessagesOptionalFilters:
    @pytest.mark.asyncio
    async def test_all_optional_filters_passed(self, mock_db):
        mock_db.search_messages = AsyncMock(return_value=([], 0))
        handlers = _get_tool_handlers(mock_db)

        await handlers["search_messages"]({
            "query": "test",
            "limit": 5,
            "channel_id": 200,
            "date_from": "2025-01-01",
            "date_to": "2025-12-31",
            "min_length": 10,
            "max_length": 100,
        })

        call_kwargs = mock_db.search_messages.await_args.kwargs
        assert call_kwargs["channel_id"] == 200
        assert call_kwargs["date_from"] == "2025-01-01"
        assert call_kwargs["date_to"] == "2025-12-31"
        assert call_kwargs["min_length"] == 10
        assert call_kwargs["max_length"] == 100
        assert call_kwargs["limit"] == 5

    @pytest.mark.asyncio
    async def test_channel_id_as_string_converted(self, mock_db):
        mock_db.search_messages = AsyncMock(return_value=([], 0))
        handlers = _get_tool_handlers(mock_db)

        await handlers["search_messages"]({
            "query": "test",
            "channel_id": "300",
        })

        call_kwargs = mock_db.search_messages.await_args.kwargs
        assert call_kwargs["channel_id"] == 300
