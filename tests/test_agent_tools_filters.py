"""Tests for agent tools: filters.py."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.filters.models import ChannelFilterResult
from tests.agent_tools_helpers import _get_tool_handlers, _text


def _make_purge_result(purged=2, deleted=50, errors=None):
    r = MagicMock()
    r.purged_count = purged
    r.total_messages_deleted = deleted
    r.errors = errors or []
    return r


class TestAnalyzeFiltersTool:
    @pytest.mark.anyio
    async def test_empty_report(self, mock_db):
        """Empty report returns appropriate message."""
        report = MagicMock()
        report.results = []
        with patch("src.filters.analyzer.ChannelAnalyzer") as mock_analyzer:
            mock_analyzer.return_value.analyze_all = AsyncMock(return_value=report)
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["analyze_filters"]({})
        assert "Нет каналов для анализа" in _text(result)

    @pytest.mark.anyio
    async def test_report_with_flagged_channels(self, mock_db):
        """Flagged channels are listed with their flags."""
        r = ChannelFilterResult(
            channel_id=100,
            title="SpamChan",
            flags=["low_uniqueness", "spam"],
            is_filtered=True,
        )
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

    @pytest.mark.anyio
    async def test_report_shows_all_beyond_30(self, mock_db):
        """More than 30 flagged channels are all shown without truncation."""

        def _make_result(i):
            return ChannelFilterResult(
                channel_id=i,
                title=f"Chan{i}",
                flags=["low_uniqueness"],
                is_filtered=True,
            )

        report = MagicMock()
        report.results = [_make_result(i) for i in range(35)]
        with patch("src.filters.analyzer.ChannelAnalyzer") as mock_analyzer:
            mock_analyzer.return_value.analyze_all = AsyncMock(return_value=report)
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["analyze_filters"]({})
        text = _text(result)
        assert "35 рекомендовано" in text
        assert "и ещё" not in text

    @pytest.mark.anyio
    async def test_report_non_flagged_not_listed(self, mock_db):
        """Non-flagged channels don't appear in the flagged list."""
        ok = ChannelFilterResult(channel_id=1, title="GoodChan", is_filtered=False)
        report = MagicMock()
        report.results = [ok]
        with patch("src.filters.analyzer.ChannelAnalyzer") as mock_analyzer:
            mock_analyzer.return_value.analyze_all = AsyncMock(return_value=report)
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["analyze_filters"]({})
        text = _text(result)
        assert "GoodChan" not in text
        assert "0 рекомендовано" in text

    @pytest.mark.anyio
    async def test_error_returns_text(self, mock_db):
        with patch("src.filters.analyzer.ChannelAnalyzer") as mock_analyzer:
            mock_analyzer.return_value.analyze_all = AsyncMock(side_effect=RuntimeError("DB fail"))
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["analyze_filters"]({})
        assert "Ошибка анализа фильтров" in _text(result)
        assert "DB fail" in _text(result)

    @pytest.mark.anyio
    async def test_quick_string_false_runs_full_analysis(self, mock_db):
        """quick="false" (JSON-string from a string-serializing backend) → FULL analysis.

        Bug class #1115 / #1238: a raw ``bool(args.get("quick"))`` treats the
        string ``"false"`` as truthy, silently forcing the sampled fast path.
        ``arg_bool`` must coerce it to False so ``analyze_all`` runs full.
        """
        report = MagicMock()
        report.results = []
        with patch("src.filters.analyzer.ChannelAnalyzer") as mock_analyzer:
            mock_analyzer.return_value.analyze_all = AsyncMock(return_value=report)
            handlers = _get_tool_handlers(mock_db)
            await handlers["analyze_filters"]({"quick": "false"})
        mock_analyzer.return_value.analyze_all.assert_awaited_once_with(quick=False, sample_size=None)

    @pytest.mark.anyio
    async def test_quick_string_true_runs_sampled_analysis(self, mock_db):
        """quick="true" (string) must still enable the sampled fast path."""
        report = MagicMock()
        report.results = []
        with patch("src.filters.analyzer.ChannelAnalyzer") as mock_analyzer:
            mock_analyzer.return_value.analyze_all = AsyncMock(return_value=report)
            handlers = _get_tool_handlers(mock_db)
            await handlers["analyze_filters"]({"quick": "true"})
        mock_analyzer.return_value.analyze_all.assert_awaited_once_with(quick=True, sample_size=None)


class TestApplyFiltersTool:
    @pytest.mark.anyio
    async def test_no_confirm_returns_gate(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["apply_filters"]({})
        assert "confirm=true" in _text(result)

    @pytest.mark.anyio
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

    @pytest.mark.anyio
    async def test_error_returns_text(self, mock_db):
        with patch("src.filters.analyzer.ChannelAnalyzer") as mock_analyzer:
            mock_analyzer.return_value.analyze_all = AsyncMock(side_effect=Exception("oops"))
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["apply_filters"]({"confirm": True})
        assert "Ошибка применения фильтров" in _text(result)


class TestResetFiltersTool:
    @pytest.mark.anyio
    async def test_no_confirm_returns_gate(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["reset_filters"]({})
        assert "confirm=true" in _text(result)

    @pytest.mark.anyio
    async def test_with_confirm_resets_and_returns_count(self, mock_db):
        with patch("src.filters.analyzer.ChannelAnalyzer") as mock_analyzer:
            mock_analyzer.return_value.reset_filters = AsyncMock(return_value=7)
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["reset_filters"]({"confirm": True})
        text = _text(result)
        assert "7 каналов" in text
        assert "разблокированы" in text

    @pytest.mark.anyio
    async def test_error_returns_text(self, mock_db):
        with patch("src.filters.analyzer.ChannelAnalyzer") as mock_analyzer:
            mock_analyzer.return_value.reset_filters = AsyncMock(side_effect=Exception("nope"))
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["reset_filters"]({"confirm": True})
        assert "Ошибка сброса фильтров" in _text(result)


class TestToggleChannelFilterTool:
    @pytest.mark.anyio
    async def test_missing_pk_returns_error(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["toggle_channel_filter"]({})
        assert "pk обязателен" in _text(result)

    @pytest.mark.anyio
    async def test_channel_not_found(self, mock_db):
        mock_db.get_channel_by_pk = AsyncMock(return_value=None)
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["toggle_channel_filter"]({"pk": 999})
        assert "не найден" in _text(result)
        assert "999" in _text(result)

    @pytest.mark.anyio
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

    @pytest.mark.anyio
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

    @pytest.mark.anyio
    async def test_error_returns_text(self, mock_db):
        mock_db.get_channel_by_pk = AsyncMock(side_effect=Exception("db error"))
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["toggle_channel_filter"]({"pk": 1})
        assert "Ошибка переключения фильтра" in _text(result)


class TestPurgeFilteredChannelsTool:
    @pytest.mark.anyio
    async def test_no_confirm_returns_gate(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["purge_filtered_channels"]({})
        assert "confirm=true" in _text(result)

    @pytest.mark.anyio
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

    @pytest.mark.anyio
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

    @pytest.mark.anyio
    async def test_error_returns_text(self, mock_db):
        with patch("src.services.filter_deletion_service.FilterDeletionService") as mock_svc:
            mock_svc.return_value.purge_all_filtered = AsyncMock(side_effect=Exception("disk full"))
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["purge_filtered_channels"]({"confirm": True})
        assert "Ошибка очистки каналов" in _text(result)

    @pytest.mark.anyio
    async def test_partial_failure_surfaces_errors(self, mock_db):
        """A partial purge failure must be reported to the agent, not hidden (#676 review)."""
        purge_result = _make_purge_result(purged=1, deleted=10, errors=["pk=2: DB error"])
        with patch("src.services.filter_deletion_service.FilterDeletionService") as mock_svc:
            mock_svc.return_value.purge_channels_by_pks = AsyncMock(return_value=purge_result)
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["purge_filtered_channels"]({"pks": "1,2", "confirm": True})
        text = _text(result)
        assert "Ошибки" in text
        assert "pk=2: DB error" in text


class TestHardDeleteChannelsTool:
    @pytest.mark.anyio
    async def test_empty_pks_returns_error(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["hard_delete_channels"]({})
        assert "pks обязателен" in _text(result)

    @pytest.mark.anyio
    async def test_no_confirm_returns_gate(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["hard_delete_channels"]({"pks": "1,2"})
        assert "confirm=true" in _text(result)

    @pytest.mark.anyio
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

    @pytest.mark.anyio
    async def test_error_returns_text(self, mock_db):
        with patch("src.services.filter_deletion_service.FilterDeletionService") as mock_svc:
            mock_svc.return_value.hard_delete_channels_by_pks = AsyncMock(side_effect=Exception("err"))
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["hard_delete_channels"]({"pks": "1", "confirm": True})
        assert "Ошибка удаления каналов" in _text(result)

    @pytest.mark.anyio
    async def test_real_service_path_actually_deletes(self, db):
        """Regression #1290: the tool built FilterDeletionService without
        channel_service, so every call raised RuntimeError and was reported as
        'Ошибка удаления'. Uses the real service/DB — mocking the service (as
        the tests above do) is exactly what hid the bug."""
        from src.models import Channel

        pk = await db.add_channel(Channel(channel_id=-1001, title="Doomed"))
        await db.set_channel_filtered(pk, True)

        handlers = _get_tool_handlers(db)
        result = await handlers["hard_delete_channels"]({"pks": str(pk), "confirm": True})

        text = _text(result)
        assert "Ошибка удаления каналов" not in text
        assert "1 каналов удалено безвозвратно" in text
        assert await db.get_channel_by_pk(pk) is None


class TestPurgeChannelMessagesTool:
    @pytest.mark.anyio
    async def test_missing_channel_id(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["purge_channel_messages"]({})
        assert "channel_id обязателен" in _text(result)

    @pytest.mark.anyio
    async def test_no_confirm_returns_gate(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["purge_channel_messages"]({"channel_id": 100})
        assert "confirm=true" in _text(result).lower()

    @pytest.mark.anyio
    async def test_with_confirm_purges(self, mock_db):
        mock_db.delete_messages_for_channel = AsyncMock(return_value=42)
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["purge_channel_messages"]({"channel_id": 100, "confirm": True})
        assert "42" in _text(result)
        mock_db.delete_messages_for_channel.assert_called_once_with(100)

    @pytest.mark.anyio
    async def test_error_returns_text(self, mock_db):
        mock_db.delete_messages_for_channel = AsyncMock(side_effect=Exception("boom"))
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["purge_channel_messages"]({"channel_id": 100, "confirm": True})
        assert "Ошибка очистки сообщений канала" in _text(result)
