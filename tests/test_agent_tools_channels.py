"""Tests for agent tools: channels.py (CRUD + tags)."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.models import ChannelStats
from tests.agent_tools_helpers import _get_tool_handlers, _text


def _make_channel(
    pk=1,
    channel_id=100,
    title="TestChan",
    username="testchan",
    is_active=True,
    is_filtered=False,
    channel_type="channel",
):
    ch = MagicMock()
    ch.id = pk
    ch.channel_id = channel_id
    ch.title = title
    ch.username = username
    ch.is_active = is_active
    ch.is_filtered = is_filtered
    ch.channel_type = channel_type
    ch.needs_review = False
    return ch


class TestGetChannelStatsTool:
    @pytest.mark.anyio
    async def test_empty(self, mock_db):
        mock_db.get_latest_stats_for_all = AsyncMock(return_value={})
        mock_db.get_channels = AsyncMock(return_value=[])
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["get_channel_stats"]({})
        assert "пока не собрана" in _text(result)

    @pytest.mark.anyio
    async def test_with_named_and_unnamed_channels(self, mock_db):
        mock_db.get_latest_stats_for_all = AsyncMock(
            return_value={
                100: ChannelStats(channel_id=100, subscriber_count=10, avg_views=2.5),
                200: ChannelStats(channel_id=200, subscriber_count=0, avg_views=0.0),
            }
        )
        mock_db.get_channels = AsyncMock(
            return_value=[
                _make_channel(channel_id=100, title="Named", username="named"),
                _make_channel(channel_id=200, title=None, username=None),
            ]
        )
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["get_channel_stats"]({})
        text = _text(result)
        assert "Named" in text
        assert "@named" in text
        assert "channel_id=100" in text
        assert "Без названия" in text
        assert "channel_id=200" in text
        assert "subscribers=0" in text
        assert "avg_views=0.0" in text


class TestAddChannelTool:
    @pytest.mark.anyio
    async def test_missing_identifier_returns_error(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["add_channel"]({})
        assert "identifier обязателен" in _text(result)

    @pytest.mark.anyio
    async def test_no_confirm_returns_gate(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["add_channel"]({"identifier": "@testchan"})
        assert "confirm=true" in _text(result)

    @pytest.mark.anyio
    async def test_with_confirm_adds_channel(self, mock_db):
        with patch("src.services.channel_service.ChannelService") as mock_svc:
            mock_svc.return_value.add_by_identifier = AsyncMock(return_value=True)
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["add_channel"]({"identifier": "@mychan", "confirm": True})
        assert "успешно добавлен" in _text(result)

    @pytest.mark.anyio
    async def test_already_exists_returns_message(self, mock_db):
        with patch("src.services.channel_service.ChannelService") as mock_svc:
            mock_svc.return_value.add_by_identifier = AsyncMock(return_value=False)
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["add_channel"]({"identifier": "@existing", "confirm": True})
        assert "уже существует" in _text(result)

    @pytest.mark.anyio
    async def test_error_returns_text(self, mock_db):
        with patch("src.services.channel_service.ChannelService") as mock_svc:
            mock_svc.return_value.add_by_identifier = AsyncMock(side_effect=Exception("API error"))
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["add_channel"]({"identifier": "@broken", "confirm": True})
        assert "Ошибка добавления канала" in _text(result)


class TestDeleteChannelTool:
    @pytest.mark.anyio
    async def test_missing_pk_returns_error(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["delete_channel"]({})
        assert "pk обязателен" in _text(result)

    @pytest.mark.anyio
    async def test_no_confirm_returns_gate(self, mock_db):
        mock_db.get_channel_by_pk = AsyncMock(return_value=_make_channel())
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["delete_channel"]({"pk": 1})
        assert "confirm=true" in _text(result)

    @pytest.mark.anyio
    async def test_with_confirm_deletes_channel(self, mock_db):
        ch = _make_channel(title="DeleteMe")
        mock_db.get_channel_by_pk = AsyncMock(return_value=ch)
        with patch("src.services.channel_service.ChannelService") as mock_svc:
            mock_svc.return_value.delete = AsyncMock()
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["delete_channel"]({"pk": 1, "confirm": True})
        assert "удалён" in _text(result)
        assert "DeleteMe" in _text(result)

    @pytest.mark.anyio
    async def test_error_returns_text(self, mock_db):
        mock_db.get_channel_by_pk = AsyncMock(return_value=_make_channel())
        with patch("src.services.channel_service.ChannelService") as mock_svc:
            mock_svc.return_value.delete = AsyncMock(side_effect=Exception("fk constraint"))
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["delete_channel"]({"pk": 1, "confirm": True})
        assert "Ошибка удаления канала" in _text(result)


class TestToggleChannelTool:
    @pytest.mark.anyio
    async def test_missing_pk_returns_error(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["toggle_channel"]({})
        assert "pk обязателен" in _text(result)

    @pytest.mark.anyio
    async def test_active_channel_gets_deactivated(self, mock_db):
        ch_after = _make_channel(is_active=False, title="MyChan")
        with patch("src.services.channel_service.ChannelService") as mock_svc:
            mock_svc.return_value.toggle = AsyncMock()
            mock_db.get_channel_by_pk = AsyncMock(return_value=ch_after)
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["toggle_channel"]({"pk": 1})
        assert "неактивен" in _text(result)
        assert "MyChan" in _text(result)

    @pytest.mark.anyio
    async def test_error_returns_text(self, mock_db):
        with patch("src.services.channel_service.ChannelService") as mock_svc:
            mock_svc.return_value.toggle = AsyncMock(side_effect=Exception("not found"))
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["toggle_channel"]({"pk": 1})
        assert "Ошибка переключения канала" in _text(result)


class TestImportChannelsTool:
    @pytest.mark.anyio
    async def test_missing_text_returns_error(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["import_channels"]({})
        assert "text обязателен" in _text(result)

    @pytest.mark.anyio
    async def test_no_identifiers_returns_error(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["import_channels"]({"text": "hello world nothing here"})
        assert "Не удалось распознать" in _text(result)

    @pytest.mark.anyio
    async def test_no_confirm_returns_gate(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["import_channels"]({"text": "@chan1 @chan2"})
        assert "confirm=true" in _text(result)

    @pytest.mark.anyio
    async def test_with_confirm_imports_channels(self, mock_db):
        with patch("src.services.channel_service.ChannelService") as mock_svc:
            mock_svc.return_value.add_by_identifier = AsyncMock(return_value=True)
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["import_channels"]({"text": "@chan1 @chan2", "confirm": True})
        text = _text(result)
        assert "Импорт завершён" in text
        assert "2/2" in text

    @pytest.mark.anyio
    async def test_partial_failure_reported(self, mock_db):
        call_count = 0

        async def flaky_add(ident):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return True
            raise Exception("API error")

        with patch("src.services.channel_service.ChannelService") as mock_svc:
            mock_svc.return_value.add_by_identifier = flaky_add
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["import_channels"]({"text": "@chan1 @chan2", "confirm": True})
        text = _text(result)
        assert "Ошибки" in text


# ===========================================================================
# channels.py (tags)
# ===========================================================================


class TestListTagsTool:
    @pytest.mark.anyio
    async def test_empty_returns_not_found(self, mock_db):
        mock_db.repos.channels.list_all_tags = AsyncMock(return_value=[])
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["list_tags"]({})
        assert "Теги не найдены" in _text(result)

    @pytest.mark.anyio
    async def test_with_tags(self, mock_db):
        mock_db.repos.channels.list_all_tags = AsyncMock(return_value=["news", "tech", "fun"])
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["list_tags"]({})
        text = _text(result)
        assert "Теги (3)" in text
        assert "news" in text


class TestCreateTagTool:
    @pytest.mark.anyio
    async def test_missing_name(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["create_tag"]({})
        assert "name обязателен" in _text(result)

    @pytest.mark.anyio
    async def test_requires_confirm(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["create_tag"]({"name": "newtag"})
        assert "confirm=true" in _text(result).lower()

    @pytest.mark.anyio
    async def test_creates_tag(self, mock_db):
        mock_db.repos.channels.create_tag = AsyncMock()
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["create_tag"]({"name": "newtag", "confirm": True})
        assert "создан" in _text(result)
        mock_db.repos.channels.create_tag.assert_called_once_with("newtag")


class TestDeleteTagTool:
    @pytest.mark.anyio
    async def test_requires_confirm(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["delete_tag"]({"name": "oldtag"})
        assert "confirm=true" in _text(result).lower()

    @pytest.mark.anyio
    async def test_deletes_tag(self, mock_db):
        mock_db.repos.channels.delete_tag = AsyncMock()
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["delete_tag"]({"name": "oldtag", "confirm": True})
        assert "удалён" in _text(result)
        mock_db.repos.channels.delete_tag.assert_called_once_with("oldtag")


class TestSetChannelTagsTool:
    @pytest.mark.anyio
    async def test_missing_pk(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["set_channel_tags"]({"tags": "news,tech"})
        assert "pk обязателен" in _text(result)

    @pytest.mark.anyio
    async def test_channel_not_found(self, mock_db):
        mock_db.get_channel_by_pk = AsyncMock(return_value=None)
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["set_channel_tags"]({"pk": 999, "tags": "news"})
        assert "не найден" in _text(result)

    @pytest.mark.anyio
    async def test_sets_tags(self, mock_db):
        ch = _make_channel(pk=1, title="TestChan")
        mock_db.get_channel_by_pk = AsyncMock(return_value=ch)
        mock_db.repos.channels.set_channel_tags = AsyncMock()
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["set_channel_tags"]({"pk": 1, "tags": "news,tech"})
        assert "обновлены" in _text(result)
        mock_db.repos.channels.set_channel_tags.assert_called_once_with(1, ["news", "tech"])

    @pytest.mark.anyio
    async def test_clears_tags(self, mock_db):
        ch = _make_channel(pk=1, title="TestChan")
        mock_db.get_channel_by_pk = AsyncMock(return_value=ch)
        mock_db.repos.channels.set_channel_tags = AsyncMock()
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["set_channel_tags"]({"pk": 1, "tags": ""})
        assert "очищены" in _text(result)


class TestGetChannelTagsTool:
    @pytest.mark.anyio
    async def test_missing_pk(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["get_channel_tags"]({})
        assert "pk обязателен" in _text(result)

    @pytest.mark.anyio
    async def test_channel_not_found(self, mock_db):
        mock_db.get_channel_by_pk = AsyncMock(return_value=None)
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["get_channel_tags"]({"pk": 999})
        assert "не найден" in _text(result)

    @pytest.mark.anyio
    async def test_no_tags(self, mock_db):
        mock_db.get_channel_by_pk = AsyncMock(return_value=_make_channel(title="TestChan"))
        mock_db.repos.channels.get_channel_tags = AsyncMock(return_value=[])
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["get_channel_tags"]({"pk": 1})
        assert "нет тегов" in _text(result)

    @pytest.mark.anyio
    async def test_with_tags(self, mock_db):
        mock_db.get_channel_by_pk = AsyncMock(return_value=_make_channel(title="TestChan"))
        mock_db.repos.channels.get_channel_tags = AsyncMock(return_value=["news", "tech"])
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["get_channel_tags"]({"pk": 1})
        text = _text(result)
        assert "TestChan" in text
        assert "news" in text


class TestListDialogsForImportTool:
    @pytest.mark.anyio
    async def test_empty(self, mock_db):
        with patch("src.services.channel_service.ChannelService") as mock_svc:
            mock_svc.return_value.get_dialogs_with_added_flags = AsyncMock(return_value=[])
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["list_dialogs_for_import"]({})
        assert "не найдены" in _text(result)

    @pytest.mark.anyio
    async def test_with_dialogs_shows_added_flag(self, mock_db):
        dialogs = [
            {"channel_id": 100, "title": "Already", "username": "a", "already_added": True},
            {"channel_id": 200, "title": "New", "username": None, "already_added": False},
        ]
        with patch("src.services.channel_service.ChannelService") as mock_svc:
            mock_svc.return_value.get_dialogs_with_added_flags = AsyncMock(return_value=dialogs)
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["list_dialogs_for_import"]({})
        text = _text(result)
        assert "уже добавлен" in text
        assert "новый" in text
        assert "Already" in text


class TestAddChannelsBulkTool:
    @pytest.mark.anyio
    async def test_missing_ids(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["add_channels_bulk"]({})
        assert "channel_ids" in _text(result)

    @pytest.mark.anyio
    async def test_no_confirm_returns_gate(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["add_channels_bulk"]({"channel_ids": "100,200"})
        assert "confirm=true" in _text(result).lower()

    @pytest.mark.anyio
    async def test_with_confirm_adds(self, mock_db):
        with patch("src.services.channel_service.ChannelService") as mock_svc:
            mock_svc.return_value.add_bulk_by_dialog_ids = AsyncMock()
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["add_channels_bulk"]({"channel_ids": "100, 200", "confirm": True})
        assert "Обработано 2" in _text(result)
        mock_svc.return_value.add_bulk_by_dialog_ids.assert_called_once_with(["100", "200"])

    @pytest.mark.anyio
    async def test_with_confirm_reports_skipped_ids(self, mock_db):
        with patch("src.services.channel_service.ChannelService") as mock_svc:
            mock_svc.return_value.add_bulk_by_dialog_ids = AsyncMock(
                return_value={"processed": 1, "skipped": 1, "skipped_ids": ["200"]}
            )
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["add_channels_bulk"]({"channel_ids": "100, 200", "confirm": True})
        text = _text(result)
        assert "Обработано 1 из 2" in text
        assert "Пропущено 1: 200" in text

    @pytest.mark.anyio
    async def test_error_returns_text(self, mock_db):
        with patch("src.services.channel_service.ChannelService") as mock_svc:
            mock_svc.return_value.add_bulk_by_dialog_ids = AsyncMock(side_effect=Exception("boom"))
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["add_channels_bulk"]({"channel_ids": "100", "confirm": True})
        assert "Ошибка массового добавления" in _text(result)


class TestRefreshChannelTypesTool:
    @pytest.mark.anyio
    async def test_resolves_with_signal_gone_and_deactivates_gone_channel(self, mock_db):
        """Parity with CLI/worker (#858 review): the agent refresh-types tool must pass
        signal_gone=True and deactivate a definitively-gone channel via the {"gone": True}
        sentinel — the old dead `if info is False` branch never fired."""
        gone_ch = _make_channel(pk=1, channel_id=111, title="Gone", username="gone")
        live_ch = _make_channel(pk=2, channel_id=222, title="Live", username="live")
        mock_db.get_channels = AsyncMock(return_value=[gone_ch, live_ch])
        mock_db.set_channel_active = AsyncMock()
        mock_db.set_channel_type = AsyncMock()
        pool = MagicMock()
        pool.resolve_channel = AsyncMock(
            side_effect=[{"gone": True}, {"channel_type": "channel"}]
        )

        handlers = _get_tool_handlers(mock_db, client_pool=pool)
        result = await handlers["refresh_channel_types"]({"confirm": True})
        text = _text(result)

        # signal_gone=True is passed for every channel
        assert all(c.kwargs.get("signal_gone") is True for c in pool.resolve_channel.await_args_list)
        # gone channel deactivated + marked unavailable; live channel typed
        mock_db.set_channel_active.assert_awaited_once_with(1, False)
        mock_db.set_channel_type.assert_any_await(111, "unavailable")
        mock_db.set_channel_type.assert_any_await(222, "channel")
        assert "деактивировано: 1" in text

    @pytest.mark.anyio
    async def test_forbidden_channel_is_not_deactivated(self, mock_db):
        """A channel that resolves to None (e.g. ChannelForbidden access error) is counted as
        failed and left active — never deactivated (#858 review)."""
        ch = _make_channel(pk=1, channel_id=111, title="Private", username=None)
        mock_db.get_channels = AsyncMock(return_value=[ch])
        mock_db.set_channel_active = AsyncMock()
        mock_db.set_channel_type = AsyncMock()
        pool = MagicMock()
        pool.resolve_channel = AsyncMock(return_value=None)  # forbidden -> None, not gone

        handlers = _get_tool_handlers(mock_db, client_pool=pool)
        result = await handlers["refresh_channel_types"]({"confirm": True})

        mock_db.set_channel_active.assert_not_awaited()
        assert "не удалось: 1" in _text(result)

    @pytest.mark.anyio
    async def test_uncertain_channel_is_quarantined_not_deactivated(self, mock_db):
        """#875 redesign: a {"review": ...} verdict (cache-miss vs deleted, uncertain) must
        flag the channel for review and NOT deactivate it."""
        ch = _make_channel(pk=1, channel_id=111, title="Maybe", username="maybe")
        mock_db.get_channels = AsyncMock(return_value=[ch])
        mock_db.set_channel_active = AsyncMock()
        mock_db.repos.channels.set_channel_review = AsyncMock()
        pool = MagicMock()
        pool.resolve_channel = AsyncMock(
            return_value={"review": True, "reason": "numeric_unresolved"}
        )

        handlers = _get_tool_handlers(mock_db, client_pool=pool)
        result = await handlers["refresh_channel_types"]({"confirm": True})

        mock_db.set_channel_active.assert_not_awaited()
        mock_db.repos.channels.set_channel_review.assert_awaited_once_with(1, "numeric_unresolved")
        assert "на ревью: 1" in _text(result)


class TestChannelReviewTools:
    @pytest.mark.anyio
    async def test_list_channels_for_review_empty(self, mock_db):
        mock_db.repos.channels.list_channels_for_review = AsyncMock(return_value=[])
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["list_channels_for_review"]({})
        assert "Нет каналов" in _text(result)

    @pytest.mark.anyio
    async def test_list_channels_for_review_lists(self, mock_db):
        ch = _make_channel(pk=7, channel_id=111, title="Maybe", username="maybe")
        ch.review_reason = "numeric_unresolved"
        mock_db.repos.channels.list_channels_for_review = AsyncMock(return_value=[ch])
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["list_channels_for_review"]({})
        text = _text(result)
        assert "pk=7" in text
        assert "Maybe" in text
        assert "numeric_unresolved" in text

    @pytest.mark.anyio
    async def test_review_keep_missing_pk_returns_error(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["review_keep_channel"]({})
        assert "pk обязателен" in _text(result)

    @pytest.mark.anyio
    async def test_review_keep_clears_flag(self, mock_db):
        ch = _make_channel(pk=3, title="KeepMe")
        mock_db.get_channel_by_pk = AsyncMock(return_value=ch)
        mock_db.repos.channels.clear_channel_review = AsyncMock()
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["review_keep_channel"]({"pk": 3})
        mock_db.repos.channels.clear_channel_review.assert_awaited_once_with(3)
        assert "снят с ревью" in _text(result)

    @pytest.mark.anyio
    async def test_confirm_dead_requires_confirmation(self, mock_db):
        mock_db.get_channel_by_pk = AsyncMock(return_value=_make_channel(title="Dead"))
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["confirm_channel_dead"]({"pk": 1})
        assert "confirm=true" in _text(result)

    @pytest.mark.anyio
    async def test_confirm_dead_deactivates_and_clears(self, mock_db):
        ch = _make_channel(pk=4, channel_id=444, title="Dead")
        mock_db.get_channel_by_pk = AsyncMock(return_value=ch)
        mock_db.set_channel_active = AsyncMock()
        mock_db.set_channel_type = AsyncMock()
        mock_db.repos.channels.clear_channel_review = AsyncMock()
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["confirm_channel_dead"]({"pk": 4, "confirm": True})
        mock_db.set_channel_active.assert_awaited_once_with(4, False)
        mock_db.set_channel_type.assert_awaited_once_with(444, "unavailable")
        mock_db.repos.channels.clear_channel_review.assert_awaited_once_with(4)
        assert "деактивирован" in _text(result)
