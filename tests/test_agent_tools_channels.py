"""Tests for agent tools: channels.py (CRUD + tags)."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

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
    return ch


class TestAddChannelTool:
    @pytest.mark.asyncio
    async def test_missing_identifier_returns_error(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["add_channel"]({})
        assert "identifier обязателен" in _text(result)

    @pytest.mark.asyncio
    async def test_no_confirm_returns_gate(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["add_channel"]({"identifier": "@testchan"})
        assert "confirm=true" in _text(result)

    @pytest.mark.asyncio
    async def test_with_confirm_adds_channel(self, mock_db):
        with patch("src.services.channel_service.ChannelService") as mock_svc:
            mock_svc.return_value.add_by_identifier = AsyncMock(return_value=True)
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["add_channel"]({"identifier": "@mychan", "confirm": True})
        assert "успешно добавлен" in _text(result)

    @pytest.mark.asyncio
    async def test_already_exists_returns_message(self, mock_db):
        with patch("src.services.channel_service.ChannelService") as mock_svc:
            mock_svc.return_value.add_by_identifier = AsyncMock(return_value=False)
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["add_channel"]({"identifier": "@existing", "confirm": True})
        assert "уже существует" in _text(result)

    @pytest.mark.asyncio
    async def test_error_returns_text(self, mock_db):
        with patch("src.services.channel_service.ChannelService") as mock_svc:
            mock_svc.return_value.add_by_identifier = AsyncMock(side_effect=Exception("API error"))
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["add_channel"]({"identifier": "@broken", "confirm": True})
        assert "Ошибка добавления канала" in _text(result)


class TestDeleteChannelTool:
    @pytest.mark.asyncio
    async def test_missing_pk_returns_error(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["delete_channel"]({})
        assert "pk обязателен" in _text(result)

    @pytest.mark.asyncio
    async def test_no_confirm_returns_gate(self, mock_db):
        mock_db.get_channel_by_pk = AsyncMock(return_value=_make_channel())
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["delete_channel"]({"pk": 1})
        assert "confirm=true" in _text(result)

    @pytest.mark.asyncio
    async def test_with_confirm_deletes_channel(self, mock_db):
        ch = _make_channel(title="DeleteMe")
        mock_db.get_channel_by_pk = AsyncMock(return_value=ch)
        with patch("src.services.channel_service.ChannelService") as mock_svc:
            mock_svc.return_value.delete = AsyncMock()
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["delete_channel"]({"pk": 1, "confirm": True})
        assert "удалён" in _text(result)
        assert "DeleteMe" in _text(result)

    @pytest.mark.asyncio
    async def test_error_returns_text(self, mock_db):
        mock_db.get_channel_by_pk = AsyncMock(return_value=_make_channel())
        with patch("src.services.channel_service.ChannelService") as mock_svc:
            mock_svc.return_value.delete = AsyncMock(side_effect=Exception("fk constraint"))
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["delete_channel"]({"pk": 1, "confirm": True})
        assert "Ошибка удаления канала" in _text(result)


class TestToggleChannelTool:
    @pytest.mark.asyncio
    async def test_missing_pk_returns_error(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["toggle_channel"]({})
        assert "pk обязателен" in _text(result)

    @pytest.mark.asyncio
    async def test_active_channel_gets_deactivated(self, mock_db):
        ch_after = _make_channel(is_active=False, title="MyChan")
        with patch("src.services.channel_service.ChannelService") as mock_svc:
            mock_svc.return_value.toggle = AsyncMock()
            mock_db.get_channel_by_pk = AsyncMock(return_value=ch_after)
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["toggle_channel"]({"pk": 1})
        assert "неактивен" in _text(result)
        assert "MyChan" in _text(result)

    @pytest.mark.asyncio
    async def test_error_returns_text(self, mock_db):
        with patch("src.services.channel_service.ChannelService") as mock_svc:
            mock_svc.return_value.toggle = AsyncMock(side_effect=Exception("not found"))
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["toggle_channel"]({"pk": 1})
        assert "Ошибка переключения канала" in _text(result)


class TestImportChannelsTool:
    @pytest.mark.asyncio
    async def test_missing_text_returns_error(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["import_channels"]({})
        assert "text обязателен" in _text(result)

    @pytest.mark.asyncio
    async def test_no_identifiers_returns_error(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["import_channels"]({"text": "hello world nothing here"})
        assert "Не удалось распознать" in _text(result)

    @pytest.mark.asyncio
    async def test_no_confirm_returns_gate(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["import_channels"]({"text": "@chan1 @chan2"})
        assert "confirm=true" in _text(result)

    @pytest.mark.asyncio
    async def test_with_confirm_imports_channels(self, mock_db):
        with patch("src.services.channel_service.ChannelService") as mock_svc:
            mock_svc.return_value.add_by_identifier = AsyncMock(return_value=True)
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["import_channels"]({"text": "@chan1 @chan2", "confirm": True})
        text = _text(result)
        assert "Импорт завершён" in text
        assert "2/2" in text

    @pytest.mark.asyncio
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
    @pytest.mark.asyncio
    async def test_empty_returns_not_found(self, mock_db):
        mock_db.repos.channels.list_all_tags = AsyncMock(return_value=[])
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["list_tags"]({})
        assert "Теги не найдены" in _text(result)

    @pytest.mark.asyncio
    async def test_with_tags(self, mock_db):
        mock_db.repos.channels.list_all_tags = AsyncMock(return_value=["news", "tech", "fun"])
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["list_tags"]({})
        text = _text(result)
        assert "Теги (3)" in text
        assert "news" in text


class TestCreateTagTool:
    @pytest.mark.asyncio
    async def test_missing_name(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["create_tag"]({})
        assert "name обязателен" in _text(result)

    @pytest.mark.asyncio
    async def test_requires_confirm(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["create_tag"]({"name": "newtag"})
        assert "confirm=true" in _text(result).lower()

    @pytest.mark.asyncio
    async def test_creates_tag(self, mock_db):
        mock_db.repos.channels.create_tag = AsyncMock()
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["create_tag"]({"name": "newtag", "confirm": True})
        assert "создан" in _text(result)
        mock_db.repos.channels.create_tag.assert_called_once_with("newtag")


class TestDeleteTagTool:
    @pytest.mark.asyncio
    async def test_requires_confirm(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["delete_tag"]({"name": "oldtag"})
        assert "confirm=true" in _text(result).lower()

    @pytest.mark.asyncio
    async def test_deletes_tag(self, mock_db):
        mock_db.repos.channels.delete_tag = AsyncMock()
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["delete_tag"]({"name": "oldtag", "confirm": True})
        assert "удалён" in _text(result)
        mock_db.repos.channels.delete_tag.assert_called_once_with("oldtag")


class TestSetChannelTagsTool:
    @pytest.mark.asyncio
    async def test_missing_pk(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["set_channel_tags"]({"tags": "news,tech"})
        assert "pk обязателен" in _text(result)

    @pytest.mark.asyncio
    async def test_channel_not_found(self, mock_db):
        mock_db.get_channel_by_pk = AsyncMock(return_value=None)
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["set_channel_tags"]({"pk": 999, "tags": "news"})
        assert "не найден" in _text(result)

    @pytest.mark.asyncio
    async def test_sets_tags(self, mock_db):
        ch = _make_channel(pk=1, title="TestChan")
        mock_db.get_channel_by_pk = AsyncMock(return_value=ch)
        mock_db.repos.channels.set_channel_tags = AsyncMock()
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["set_channel_tags"]({"pk": 1, "tags": "news,tech"})
        assert "обновлены" in _text(result)
        mock_db.repos.channels.set_channel_tags.assert_called_once_with(1, ["news", "tech"])

    @pytest.mark.asyncio
    async def test_clears_tags(self, mock_db):
        ch = _make_channel(pk=1, title="TestChan")
        mock_db.get_channel_by_pk = AsyncMock(return_value=ch)
        mock_db.repos.channels.set_channel_tags = AsyncMock()
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["set_channel_tags"]({"pk": 1, "tags": ""})
        assert "очищены" in _text(result)
