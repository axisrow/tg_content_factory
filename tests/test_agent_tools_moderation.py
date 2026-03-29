"""Tests for agent tools: moderation.py."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from tests.agent_tools_helpers import _get_tool_handlers, _text


def _make_run(run_id=1, pipeline_id=1, status="pending", moderation_status="pending", text="Test text"):
    r = MagicMock()
    r.id = run_id
    r.pipeline_id = pipeline_id
    r.status = status
    r.moderation_status = moderation_status
    r.generated_text = text
    r.created_at = "2025-01-01T12:00:00"
    r.updated_at = "2025-01-01T12:00:00"
    return r


class TestListPendingModerationTool:
    @pytest.mark.asyncio
    async def test_empty_returns_not_found(self, mock_db):
        mock_db.repos.generation_runs.list_pending_moderation = AsyncMock(return_value=[])
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["list_pending_moderation"]({})
        assert "Нет черновиков на модерации" in _text(result)

    @pytest.mark.asyncio
    async def test_with_runs_shows_preview(self, mock_db):
        run = _make_run(run_id=1, text="Sample generated text for preview")
        mock_db.repos.generation_runs.list_pending_moderation = AsyncMock(return_value=[run])
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["list_pending_moderation"]({})
        text = _text(result)
        assert "На модерации (1 шт.)" in text
        assert "run_id=1" in text

    @pytest.mark.asyncio
    async def test_with_pipeline_filter(self, mock_db):
        mock_db.repos.generation_runs.list_pending_moderation = AsyncMock(return_value=[])
        handlers = _get_tool_handlers(mock_db)
        await handlers["list_pending_moderation"]({"pipeline_id": 5, "limit": 10})
        mock_db.repos.generation_runs.list_pending_moderation.assert_called_once_with(
            pipeline_id=5, limit=10
        )

    @pytest.mark.asyncio
    async def test_error_returns_text(self, mock_db):
        mock_db.repos.generation_runs.list_pending_moderation = AsyncMock(
            side_effect=Exception("db error")
        )
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["list_pending_moderation"]({})
        assert "Ошибка получения очереди модерации" in _text(result)


class TestViewModerationRunTool:
    @pytest.mark.asyncio
    async def test_missing_run_id_returns_error(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["view_moderation_run"]({})
        assert "run_id обязателен" in _text(result)

    @pytest.mark.asyncio
    async def test_run_not_found(self, mock_db):
        mock_db.repos.generation_runs.get = AsyncMock(return_value=None)
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["view_moderation_run"]({"run_id": 999})
        assert "не найден" in _text(result)

    @pytest.mark.asyncio
    async def test_run_found_shows_text(self, mock_db):
        run = _make_run(run_id=1, text="Full text content")
        mock_db.repos.generation_runs.get = AsyncMock(return_value=run)
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["view_moderation_run"]({"run_id": 1})
        text = _text(result)
        assert "Run id=1" in text
        assert "Full text content" in text


class TestApproveRunTool:
    @pytest.mark.asyncio
    async def test_missing_run_id(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["approve_run"]({})
        assert "run_id обязателен" in _text(result)

    @pytest.mark.asyncio
    async def test_run_not_found(self, mock_db):
        mock_db.repos.generation_runs.get = AsyncMock(return_value=None)
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["approve_run"]({"run_id": 999})
        assert "не найден" in _text(result)

    @pytest.mark.asyncio
    async def test_approve_success(self, mock_db):
        run = _make_run(run_id=1)
        mock_db.repos.generation_runs.get = AsyncMock(return_value=run)
        mock_db.repos.generation_runs.set_moderation_status = AsyncMock()
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["approve_run"]({"run_id": 1})
        assert "одобрен" in _text(result)
        mock_db.repos.generation_runs.set_moderation_status.assert_called_once_with(1, "approved")


class TestRejectRunTool:
    @pytest.mark.asyncio
    async def test_reject_success(self, mock_db):
        run = _make_run(run_id=2)
        mock_db.repos.generation_runs.get = AsyncMock(return_value=run)
        mock_db.repos.generation_runs.set_moderation_status = AsyncMock()
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["reject_run"]({"run_id": 2})
        assert "отклонён" in _text(result)
        mock_db.repos.generation_runs.set_moderation_status.assert_called_once_with(2, "rejected")


class TestBulkApproveRunsTool:
    @pytest.mark.asyncio
    async def test_requires_confirm(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["bulk_approve_runs"]({"run_ids": "1,2,3"})
        assert "confirm=true" in _text(result).lower()

    @pytest.mark.asyncio
    async def test_invalid_run_ids(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["bulk_approve_runs"]({"run_ids": "a,b,c", "confirm": True})
        assert "должны быть числами" in _text(result)

    @pytest.mark.asyncio
    async def test_empty_run_ids(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["bulk_approve_runs"]({"run_ids": "", "confirm": True})
        assert "run_ids пуст" in _text(result)

    @pytest.mark.asyncio
    async def test_bulk_approve_success(self, mock_db):
        mock_db.repos.generation_runs.set_moderation_status = AsyncMock()
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["bulk_approve_runs"]({"run_ids": "1,2,3", "confirm": True})
        assert "Одобрено 3 run(s)" in _text(result)


class TestBulkRejectRunsTool:
    @pytest.mark.asyncio
    async def test_bulk_reject_success(self, mock_db):
        mock_db.repos.generation_runs.set_moderation_status = AsyncMock()
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["bulk_reject_runs"]({"run_ids": "5,6", "confirm": True})
        assert "Отклонено 2 run(s)" in _text(result)
