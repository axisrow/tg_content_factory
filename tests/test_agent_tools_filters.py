"""Tests for agent tools: filters.py."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from tests.agent_tools_helpers import _get_tool_handlers, _text


def _make_purge_result(purged=2, deleted=50):
    r = MagicMock()
    r.purged_count = purged
    r.total_messages_deleted = deleted
    return r


class TestAnalyzeFiltersTool:
    @pytest.mark.asyncio
    async def test_empty_report(self, mock_db):
        """Empty report returns appropriate message."""
        report = MagicMock()
        report.results = []
        with patch("src.filters.analyzer.ChannelAnalyzer") as mock_analyzer:
            mock_analyzer.return_value.analyze_all = AsyncMock(return_value=report)
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["analyze_filters"]({})
        assert "Нет каналов для анализа" in _text(result)

    @pytest.mark.asyncio
    async def test_report_with_flagged_channels(self, mock_db):
        """Flagged channels are listed with their flags."""
        r = MagicMock()
        r.should_filter = True
        r.title = "SpamChan"
        r.channel_id = 100
        r.flags = ["low_uniqueness", "spam"]
        report = MagicMock()
        report.results = [r]
        with patch("src.filters.analyzer.ChannelAnalyzer") as mock_analyzer:
            mock_analyzer.return_value.analyze_all = AsyncMock(return_value=report)
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["analyze_filters"]({})
        text = _text(result)
        assert "SpamChan" in text
        assert "low_uniqueness" in text
        assert "spam" in text
        assert "1 рекомендовано" in text

    @pytest.mark.asyncio
    async def test_report_shows_all_beyond_30(self, mock_db):
        """More than 30 flagged channels are all shown without truncation."""

        def _make_result(i):
            r = MagicMock()
            r.should_filter = True
            r.title = f"Chan{i}"
            r.channel_id = i
            r.flags = ["low_uniqueness"]
            return r

        report = MagicMock()
        report.results = [_make_result(i) for i in range(35)]
        with patch("src.filters.analyzer.ChannelAnalyzer") as mock_analyzer:
            mock_analyzer.return_value.analyze_all = AsyncMock(return_value=report)
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["analyze_filters"]({})
        text = _text(result)
        assert "35 рекомендовано" in text
        assert "и ещё" not in text

    @pytest.mark.asyncio
    async def test_report_non_flagged_not_listed(self, mock_db):
        """Non-flagged channels don't appear in the flagged list."""
        ok = MagicMock()
        ok.should_filter = False
        ok.title = "GoodChan"
        ok.channel_id = 1
        ok.flags = []
        report = MagicMock()
        report.results = [ok]
        with patch("src.filters.analyzer.ChannelAnalyzer") as mock_analyzer:
            mock_analyzer.return_value.analyze_all = AsyncMock(return_value=report)
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["analyze_filters"]({})
        text = _text(result)
        assert "GoodChan" not in text
        assert "0 рекомендовано" in text

    @pytest.mark.asyncio
    async def test_error_returns_text(self, mock_db):
        with patch("src.filters.analyzer.ChannelAnalyzer") as mock_analyzer:
            mock_analyzer.return_value.analyze_all = AsyncMock(side_effect=RuntimeError("DB fail"))
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["analyze_filters"]({})
        assert "Ошибка анализа фильтров" in _text(result)
        assert "DB fail" in _text(result)


class TestApplyFiltersTool:
    @pytest.mark.asyncio
    async def test_no_confirm_returns_gate(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["apply_filters"]({})
        assert "confirm=true" in _text(result)

    @pytest.mark.asyncio
    async def test_with_confirm_applies_and_returns_count(self, mock_db):
        report = MagicMock()
        with patch("src.filters.analyzer.ChannelAnalyzer") as mock_analyzer:
            inst = mock_analyzer.return_value
            inst.analyze_all = AsyncMock(return_value=report)
            inst.apply_filters = AsyncMock(return_value=3)
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["apply_filters"]({"confirm": True})
        text = _text(result)
        assert "3 каналов" in text
        assert "Фильтры применены" in text

    @pytest.mark.asyncio
    async def test_error_returns_text(self, mock_db):
        with patch("src.filters.analyzer.ChannelAnalyzer") as mock_analyzer:
            mock_analyzer.return_value.analyze_all = AsyncMock(side_effect=Exception("oops"))
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["apply_filters"]({"confirm": True})
        assert "Ошибка применения фильтров" in _text(result)


class TestResetFiltersTool:
    @pytest.mark.asyncio
    async def test_no_confirm_returns_gate(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["reset_filters"]({})
        assert "confirm=true" in _text(result)

    @pytest.mark.asyncio
    async def test_with_confirm_resets_and_returns_count(self, mock_db):
        with patch("src.filters.analyzer.ChannelAnalyzer") as mock_analyzer:
            mock_analyzer.return_value.reset_filters = AsyncMock(return_value=7)
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["reset_filters"]({"confirm": True})
        text = _text(result)
        assert "7 каналов" in text
        assert "разблокированы" in text

    @pytest.mark.asyncio
    async def test_error_returns_text(self, mock_db):
        with patch("src.filters.analyzer.ChannelAnalyzer") as mock_analyzer:
            mock_analyzer.return_value.reset_filters = AsyncMock(side_effect=Exception("nope"))
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["reset_filters"]({"confirm": True})
        assert "Ошибка сброса фильтров" in _text(result)


class TestToggleChannelFilterTool:
    @pytest.mark.asyncio
    async def test_missing_pk_returns_error(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["toggle_channel_filter"]({})
        assert "pk обязателен" in _text(result)

    @pytest.mark.asyncio
    async def test_channel_not_found(self, mock_db):
        mock_db.get_channel_by_pk = AsyncMock(return_value=None)
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["toggle_channel_filter"]({"pk": 999})
        assert "не найден" in _text(result)
        assert "999" in _text(result)

    @pytest.mark.asyncio
    async def test_filtered_false_becomes_filtered(self, mock_db):
        ch = MagicMock()
        ch.is_filtered = False
        ch.title = "NewsChan"
        mock_db.get_channel_by_pk = AsyncMock(return_value=ch)
        mock_db.set_channel_filtered = AsyncMock()
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["toggle_channel_filter"]({"pk": 1})
        text = _text(result)
        assert "NewsChan" in text
        assert "отфильтрован" in text
        mock_db.set_channel_filtered.assert_awaited_once_with(1, True)

    @pytest.mark.asyncio
    async def test_filtered_true_becomes_unblocked(self, mock_db):
        ch = MagicMock()
        ch.is_filtered = True
        ch.title = "SpamChan"
        mock_db.get_channel_by_pk = AsyncMock(return_value=ch)
        mock_db.set_channel_filtered = AsyncMock()
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["toggle_channel_filter"]({"pk": 2})
        text = _text(result)
        assert "SpamChan" in text
        assert "разблокирован" in text
        mock_db.set_channel_filtered.assert_awaited_once_with(2, False)

    @pytest.mark.asyncio
    async def test_error_returns_text(self, mock_db):
        mock_db.get_channel_by_pk = AsyncMock(side_effect=Exception("db error"))
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["toggle_channel_filter"]({"pk": 1})
        assert "Ошибка переключения фильтра" in _text(result)


class TestPurgeFilteredChannelsTool:
    @pytest.mark.asyncio
    async def test_no_confirm_returns_gate(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["purge_filtered_channels"]({})
        assert "confirm=true" in _text(result)

    @pytest.mark.asyncio
    async def test_with_pks_and_confirm_purges_by_pks(self, mock_db):
        purge_result = _make_purge_result(purged=2, deleted=40)
        with patch("src.services.filter_deletion_service.FilterDeletionService") as mock_svc:
            mock_svc.return_value.purge_channels_by_pks = AsyncMock(return_value=purge_result)
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["purge_filtered_channels"]({"pks": "1,2", "confirm": True})
        text = _text(result)
        assert "2 каналов" in text
        assert "40 сообщений" in text
        mock_svc.return_value.purge_channels_by_pks.assert_awaited_once_with([1, 2])

    @pytest.mark.asyncio
    async def test_empty_pks_and_confirm_purges_all(self, mock_db):
        purge_result = _make_purge_result(purged=5, deleted=200)
        with patch("src.services.filter_deletion_service.FilterDeletionService") as mock_svc:
            mock_svc.return_value.purge_all_filtered = AsyncMock(return_value=purge_result)
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["purge_filtered_channels"]({"pks": "", "confirm": True})
        text = _text(result)
        assert "5 каналов" in text
        assert "200 сообщений" in text
        mock_svc.return_value.purge_all_filtered.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_error_returns_text(self, mock_db):
        with patch("src.services.filter_deletion_service.FilterDeletionService") as mock_svc:
            mock_svc.return_value.purge_all_filtered = AsyncMock(side_effect=Exception("disk full"))
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["purge_filtered_channels"]({"confirm": True})
        assert "Ошибка очистки каналов" in _text(result)


class TestHardDeleteChannelsTool:
    @pytest.mark.asyncio
    async def test_empty_pks_returns_error(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["hard_delete_channels"]({})
        assert "pks обязателен" in _text(result)

    @pytest.mark.asyncio
    async def test_no_confirm_returns_gate(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["hard_delete_channels"]({"pks": "1,2"})
        assert "confirm=true" in _text(result)

    @pytest.mark.asyncio
    async def test_with_pks_and_confirm_deletes(self, mock_db):
        del_result = _make_purge_result(purged=2, deleted=0)
        with patch("src.services.filter_deletion_service.FilterDeletionService") as mock_svc:
            mock_svc.return_value.hard_delete_channels_by_pks = AsyncMock(return_value=del_result)
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["hard_delete_channels"]({"pks": "3,4", "confirm": True})
        text = _text(result)
        assert "2 каналов" in text
        assert "безвозвратно" in text
        mock_svc.return_value.hard_delete_channels_by_pks.assert_awaited_once_with([3, 4])

    @pytest.mark.asyncio
    async def test_error_returns_text(self, mock_db):
        with patch("src.services.filter_deletion_service.FilterDeletionService") as mock_svc:
            mock_svc.return_value.hard_delete_channels_by_pks = AsyncMock(side_effect=Exception("err"))
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["hard_delete_channels"]({"pks": "1", "confirm": True})
        assert "Ошибка удаления каналов" in _text(result)
