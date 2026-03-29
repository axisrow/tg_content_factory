"""Tests for agent tools: search_queries.py."""
from __future__ import annotations

import pytest

from tests.agent_tools_helpers import _get_tool_handlers, _text


@pytest.fixture
async def sq_handlers(db):
    """Tool handlers backed by a real in-memory DB."""
    return _get_tool_handlers(db)


async def _add_query(db, query="test query", interval_minutes=30, is_active=True):
    """Helper: add a search query via the service and return its id."""
    from src.services.search_query_service import SearchQueryService

    svc = SearchQueryService(db)
    sq_id = await svc.add(query, interval_minutes=interval_minutes)
    if not is_active:
        await svc.toggle(sq_id)  # toggle off since created as active
    return sq_id


class TestListSearchQueriesTool:
    @pytest.mark.asyncio
    async def test_empty_returns_not_found(self, sq_handlers):
        result = await sq_handlers["list_search_queries"]({"active_only": False})
        assert "не найдены" in _text(result)

    @pytest.mark.asyncio
    async def test_lists_all_when_active_only_false(self, db, sq_handlers):
        await _add_query(db, "query1", is_active=True)
        await _add_query(db, "query2", is_active=False)
        result = await sq_handlers["list_search_queries"]({"active_only": False})
        text = _text(result)
        assert "query1" in text
        assert "query2" in text
        assert "Поисковые запросы (2)" in text

    @pytest.mark.asyncio
    async def test_active_only_filters_inactive(self, db, sq_handlers):
        await _add_query(db, "active_query", is_active=True)
        await _add_query(db, "inactive_query", is_active=False)
        result = await sq_handlers["list_search_queries"]({"active_only": True})
        text = _text(result)
        assert "active_query" in text
        assert "inactive_query" not in text

    @pytest.mark.asyncio
    async def test_shows_status_labels(self, db, sq_handlers):
        await _add_query(db, "myquery", is_active=True)
        result = await sq_handlers["list_search_queries"]({})
        text = _text(result)
        assert "активен" in text


class TestGetSearchQueryTool:
    @pytest.mark.asyncio
    async def test_missing_sq_id_returns_error(self, sq_handlers):
        result = await sq_handlers["get_search_query"]({})
        assert "sq_id обязателен" in _text(result)

    @pytest.mark.asyncio
    async def test_not_found_returns_error(self, sq_handlers):
        result = await sq_handlers["get_search_query"]({"sq_id": 9999})
        assert "не найден" in _text(result)
        assert "9999" in _text(result)

    @pytest.mark.asyncio
    async def test_found_shows_fields(self, db, sq_handlers):
        sq_id = await _add_query(db, "find this")
        result = await sq_handlers["get_search_query"]({"sq_id": sq_id})
        text = _text(result)
        assert "find this" in text
        assert f"id: {sq_id}" in text
        assert "is_active" in text
        assert "interval_minutes" in text


class TestAddSearchQueryTool:
    @pytest.mark.asyncio
    async def test_missing_query_returns_error(self, sq_handlers):
        result = await sq_handlers["add_search_query"]({"confirm": True})
        assert "query обязателен" in _text(result)

    @pytest.mark.asyncio
    async def test_no_confirm_returns_gate(self, sq_handlers):
        result = await sq_handlers["add_search_query"]({"query": "hello"})
        assert "confirm=true" in _text(result)

    @pytest.mark.asyncio
    async def test_with_query_and_confirm_creates(self, sq_handlers):
        result = await sq_handlers["add_search_query"]({"query": "new query", "confirm": True})
        text = _text(result)
        assert "создан" in text
        assert "id=" in text

    @pytest.mark.asyncio
    async def test_with_custom_interval_creates(self, sq_handlers):
        result = await sq_handlers["add_search_query"](
            {"query": "custom interval", "interval_minutes": 120, "confirm": True}
        )
        assert "создан" in _text(result)


class TestEditSearchQueryTool:
    @pytest.mark.asyncio
    async def test_missing_sq_id_returns_error(self, sq_handlers):
        result = await sq_handlers["edit_search_query"]({"confirm": True})
        assert "sq_id обязателен" in _text(result)

    @pytest.mark.asyncio
    async def test_no_confirm_returns_gate(self, sq_handlers):
        result = await sq_handlers["edit_search_query"]({"sq_id": 1})
        assert "confirm=true" in _text(result)

    @pytest.mark.asyncio
    async def test_not_found_returns_error(self, sq_handlers):
        result = await sq_handlers["edit_search_query"]({"sq_id": 9999, "confirm": True})
        assert "не найден" in _text(result)

    @pytest.mark.asyncio
    async def test_found_updates_query(self, db, sq_handlers):
        sq_id = await _add_query(db, "original")
        result = await sq_handlers["edit_search_query"](
            {"sq_id": sq_id, "query": "updated", "confirm": True}
        )
        assert "обновлён" in _text(result)

    @pytest.mark.asyncio
    async def test_updates_interval(self, db, sq_handlers):
        sq_id = await _add_query(db, "some query")
        result = await sq_handlers["edit_search_query"](
            {"sq_id": sq_id, "interval_minutes": 90, "confirm": True}
        )
        assert "обновлён" in _text(result)


class TestDeleteSearchQueryTool:
    @pytest.mark.asyncio
    async def test_missing_sq_id_returns_error(self, sq_handlers):
        result = await sq_handlers["delete_search_query"]({})
        assert "sq_id обязателен" in _text(result)

    @pytest.mark.asyncio
    async def test_no_confirm_returns_gate(self, sq_handlers):
        result = await sq_handlers["delete_search_query"]({"sq_id": 1})
        assert "confirm=true" in _text(result)

    @pytest.mark.asyncio
    async def test_with_confirm_deletes(self, db, sq_handlers):
        sq_id = await _add_query(db, "to delete")
        result = await sq_handlers["delete_search_query"]({"sq_id": sq_id, "confirm": True})
        text = _text(result)
        assert "удалён" in text
        assert str(sq_id) in text


class TestToggleSearchQueryTool:
    @pytest.mark.asyncio
    async def test_missing_sq_id_returns_error(self, sq_handlers):
        result = await sq_handlers["toggle_search_query"]({})
        assert "sq_id обязателен" in _text(result)

    @pytest.mark.asyncio
    async def test_not_found_returns_error(self, sq_handlers):
        result = await sq_handlers["toggle_search_query"]({"sq_id": 9999})
        assert "не найден" in _text(result)

    @pytest.mark.asyncio
    async def test_active_query_gets_deactivated(self, db, sq_handlers):
        sq_id = await _add_query(db, "active one", is_active=True)
        result = await sq_handlers["toggle_search_query"]({"sq_id": sq_id})
        text = _text(result)
        assert "деактивирован" in text
        assert str(sq_id) in text

    @pytest.mark.asyncio
    async def test_inactive_query_gets_activated(self, db, sq_handlers):
        sq_id = await _add_query(db, "inactive one", is_active=False)
        result = await sq_handlers["toggle_search_query"]({"sq_id": sq_id})
        text = _text(result)
        assert "активирован" in text
        assert str(sq_id) in text


class TestRunSearchQueryTool:
    @pytest.mark.asyncio
    async def test_missing_sq_id_returns_error(self, sq_handlers):
        result = await sq_handlers["run_search_query"]({})
        assert "sq_id обязателен" in _text(result)

    @pytest.mark.asyncio
    async def test_run_returns_count(self, db, sq_handlers):
        sq_id = await _add_query(db, "search term")
        result = await sq_handlers["run_search_query"]({"sq_id": sq_id})
        text = _text(result)
        assert "выполнен" in text
        assert "совпадений" in text
        assert str(sq_id) in text

    @pytest.mark.asyncio
    async def test_run_nonexistent_returns_zero_matches(self, sq_handlers):
        """run_search_query with a nonexistent id returns 0 matches (service is lenient)."""
        result = await sq_handlers["run_search_query"]({"sq_id": 9999})
        text = _text(result)
        assert "выполнен" in text
        assert "0 совпадений" in text
