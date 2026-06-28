"""Tests for agent tools: dialogs.py MCP tools."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from tests.agent_tools_helpers import _get_tool_handlers, _text


class SnapshotClientPool:
    def __init__(self):
        self.clients = {"+79001234567": object()}

    def connected_phones(self):
        return set(self.clients)


class TestDialogsToolSearchDialogs:
    @pytest.mark.anyio
    async def test_no_pool(self, mock_db):
        handlers = _get_tool_handlers(mock_db, client_pool=None)
        result = await handlers["search_dialogs"]({"phone": "+7123456"})
        assert "CLI-режиме" in _text(result)

    @pytest.mark.anyio
    async def test_empty_dialogs(self, mock_db):
        mock_db.get_accounts = AsyncMock(
            return_value=[SimpleNamespace(phone="+79001234567", is_primary=True)]
        )
        mock_db.get_setting = AsyncMock(return_value=None)
        mock_pool = MagicMock()
        ch_svc = MagicMock()
        ch_svc.get_my_dialogs = AsyncMock(return_value=[])
        with patch("src.services.channel_service.ChannelService", return_value=ch_svc):
            handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
            result = await handlers["search_dialogs"]({"phone": "+79001234567"})
        assert "не найдены" in _text(result)

    @pytest.mark.anyio
    async def test_with_dialogs(self, mock_db):
        mock_db.get_accounts = AsyncMock(
            return_value=[SimpleNamespace(phone="+79001234567", is_primary=True)]
        )
        mock_db.get_setting = AsyncMock(return_value=None)
        mock_pool = MagicMock()
        dialogs = [
            {"title": "My Channel", "channel_id": 111, "channel_type": "channel"},
            {"title": "My Group", "channel_id": 222, "channel_type": "group"},
        ]
        ch_svc = MagicMock()
        ch_svc.get_my_dialogs = AsyncMock(return_value=dialogs)
        with patch("src.services.channel_service.ChannelService", return_value=ch_svc):
            handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
            result = await handlers["search_dialogs"]({"phone": "+79001234567"})
        text = _text(result)
        assert "My Channel" in text
        assert "id=111" in text

    @pytest.mark.anyio
    async def test_numeric_query_can_match_title(self, mock_db):
        mock_db.get_accounts = AsyncMock(
            return_value=[SimpleNamespace(phone="+79001234567", is_primary=True)]
        )
        mock_db.get_setting = AsyncMock(return_value=None)
        mock_pool = MagicMock()
        dialogs = [
            {"title": "802", "channel_id": 111802, "channel_type": "group"},
            {"title": "Other", "channel_id": 802, "channel_type": "group"},
        ]
        ch_svc = MagicMock()
        ch_svc.get_my_dialogs = AsyncMock(return_value=dialogs)
        with patch("src.services.channel_service.ChannelService", return_value=ch_svc):
            handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
            result = await handlers["search_dialogs"]({
                "phone": "+79001234567",
                "search": "802",
            })
        text = _text(result)
        assert "802" in text
        assert "id=111802" in text
        assert "id=802" in text

    @pytest.mark.anyio
    async def test_empty_phone_uses_available_connected_account(self, mock_db):
        future = datetime.now(timezone.utc) + timedelta(hours=1)
        mock_db.get_accounts = AsyncMock(return_value=[
            SimpleNamespace(
                phone="+79000000001",
                is_primary=True,
                is_active=True,
                flood_wait_until=future,
            ),
            SimpleNamespace(
                phone="+79000000002",
                is_primary=False,
                is_active=True,
                flood_wait_until=None,
            ),
        ])
        mock_db.get_setting = AsyncMock(return_value=None)
        mock_pool = MagicMock()
        mock_pool.clients = {"+79000000001": object(), "+79000000002": object()}
        dialogs = [{"title": "Available", "channel_id": 222, "channel_type": "channel"}]
        ch_svc = MagicMock()
        ch_svc.get_my_dialogs = AsyncMock(return_value=dialogs)
        with patch("src.services.channel_service.ChannelService", return_value=ch_svc):
            handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
            result = await handlers["search_dialogs"]({"phone": None})
        text = _text(result)
        assert "Available" in text
        ch_svc.get_my_dialogs.assert_awaited_once_with("+79000000002")

    @pytest.mark.anyio
    async def test_snapshot_mode_marks_cached_source(self, mock_db):
        mock_db.get_accounts = AsyncMock(return_value=[
            SimpleNamespace(
                phone="+79001234567",
                is_primary=True,
                is_active=True,
                flood_wait_until=None,
            )
        ])
        mock_db.get_setting = AsyncMock(return_value=None)
        ch_svc = MagicMock()
        ch_svc.get_my_dialogs = AsyncMock(return_value=[
            {"title": "Cached Snapshot", "channel_id": 10, "channel_type": "channel"}
        ])

        with patch("src.services.channel_service.ChannelService", return_value=ch_svc):
            handlers = _get_tool_handlers(mock_db, client_pool=SnapshotClientPool())
            result = await handlers["search_dialogs"]({"phone": "+79001234567"})

        text = _text(result)
        assert "Cached Snapshot" in text
        assert "worker snapshot / cached" in text


class TestDialogsToolRefreshDialogs:
    @pytest.mark.anyio
    async def test_no_pool(self, mock_db):
        handlers = _get_tool_handlers(mock_db, client_pool=None)
        result = await handlers["refresh_dialogs"]({"phone": "+7123456"})
        assert "CLI-режиме" in _text(result)

    @pytest.mark.anyio
    async def test_refresh_success(self, mock_db):
        mock_db.get_accounts = AsyncMock(
            return_value=[SimpleNamespace(phone="+79001234567", is_primary=True)]
        )
        mock_db.get_setting = AsyncMock(return_value=None)
        mock_pool = MagicMock()
        ch_svc = MagicMock()
        ch_svc.get_my_dialogs = AsyncMock(
            return_value=[{"title": "X", "channel_id": 1, "channel_type": "channel"}]
        )
        with patch("src.services.channel_service.ChannelService", return_value=ch_svc):
            handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
            result = await handlers["refresh_dialogs"]({"phone": "+79001234567"})
        text = _text(result)
        assert "обновлены" in text
        assert "1" in text

    @pytest.mark.anyio
    async def test_snapshot_mode_is_live_runtime_limited(self, mock_db):
        handlers = _get_tool_handlers(mock_db, client_pool=SnapshotClientPool())
        result = await handlers["refresh_dialogs"]({"phone": "+79001234567"})
        text = _text(result)
        assert "live Telegram runtime" in text
        assert "snapshot" in text


class TestDialogsToolLeaveDialogs:
    @pytest.mark.anyio
    async def test_no_pool(self, mock_db):
        handlers = _get_tool_handlers(mock_db, client_pool=None)
        result = await handlers["leave_dialogs"]({"phone": "+7123456", "dialog_ids": "1,2"})
        assert "CLI-режиме" in _text(result)

    @pytest.mark.anyio
    async def test_missing_dialog_ids(self, mock_db):
        mock_db.get_accounts = AsyncMock(
            return_value=[SimpleNamespace(phone="+79001234567", is_primary=True)]
        )
        mock_db.get_setting = AsyncMock(return_value=None)
        mock_pool = MagicMock()
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["leave_dialogs"](
            {"phone": "+79001234567", "dialog_ids": "", "confirm": True}
        )
        assert "dialog_ids обязателен" in _text(result)

    @pytest.mark.anyio
    async def test_requires_confirmation(self, mock_db):
        mock_db.get_accounts = AsyncMock(
            return_value=[SimpleNamespace(phone="+79001234567", is_primary=True)]
        )
        mock_db.get_setting = AsyncMock(return_value=None)
        mock_pool = MagicMock()
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["leave_dialogs"](
            {"phone": "+79001234567", "dialog_ids": "1,2", "confirm": False}
        )
        assert "Подтвердите" in _text(result)

    @pytest.mark.anyio
    async def test_resolves_channel_type_per_dialog(self, mock_db):
        """leave_dialogs must resolve each dialog's channel_type (audit #837/7) so
        DMs/bots/legacy-groups route to the right Peer instead of PeerChannel."""
        mock_db.get_accounts = AsyncMock(
            return_value=[SimpleNamespace(phone="+79001234567", is_primary=True)]
        )
        mock_db.get_setting = AsyncMock(return_value=None)
        ch_svc = MagicMock()
        ch_svc.get_my_dialogs = AsyncMock(
            return_value=[
                {"channel_id": 777, "channel_type": "bot", "title": "B"},
                {"channel_id": -100123, "channel_type": "channel", "title": "C"},
            ]
        )
        action = MagicMock()
        action.leave_dialogs = AsyncMock(
            return_value=SimpleNamespace(success_count=2, results={777: True, -100123: True})
        )
        with (
            patch("src.services.channel_service.ChannelService", return_value=ch_svc),
            patch("src.agent.tools.dialogs.TelegramActionService", return_value=action),
        ):
            handlers = _get_tool_handlers(mock_db, client_pool=MagicMock())
            await handlers["leave_dialogs"](
                {"phone": "+79001234567", "dialog_ids": "777,-100123,999", "confirm": True}
            )
        passed = dict(action.leave_dialogs.await_args.kwargs["dialogs"])
        assert passed[777] == "bot"
        assert passed[-100123] == "channel"
        # An id absent from the dialog listing (stale/empty cache, or a manually supplied id)
        # must fall back to "channel" -> PeerChannel, NOT a guessed "dm" -> PeerUser. Channel
        # ids here are bare-positive, so guessing "dm" for an unknown positive id would try to
        # remove the wrong peer (the same numeric id as a user). Regression for #842 review.
        assert passed[999] == "channel"


class TestDialogsToolDeleteDialogs:
    @pytest.mark.anyio
    async def test_no_pool(self, mock_db):
        handlers = _get_tool_handlers(mock_db, client_pool=None)
        result = await handlers["delete_dialogs"]({"phone": "+7123456", "dialog_ids": "1,2"})
        assert "CLI-режиме" in _text(result)

    @pytest.mark.anyio
    async def test_requires_confirmation(self, mock_db):
        mock_db.get_accounts = AsyncMock(
            return_value=[SimpleNamespace(phone="+79001234567", is_primary=True)]
        )
        mock_db.get_setting = AsyncMock(return_value=None)
        handlers = _get_tool_handlers(mock_db, client_pool=MagicMock())
        result = await handlers["delete_dialogs"](
            {"phone": "+79001234567", "dialog_ids": "1,2", "confirm": False}
        )
        assert "Подтвердите" in _text(result)

    @pytest.mark.anyio
    async def test_resolves_channel_type_per_dialog(self, mock_db):
        """delete_dialogs resolves each dialog's channel_type so the backend picks
        the right TL request (DeleteChannel/DeleteChat/delete_dialog)."""
        mock_db.get_accounts = AsyncMock(
            return_value=[SimpleNamespace(phone="+79001234567", is_primary=True)]
        )
        mock_db.get_setting = AsyncMock(return_value=None)
        ch_svc = MagicMock()
        ch_svc.get_my_dialogs = AsyncMock(
            return_value=[
                {"channel_id": 777, "channel_type": "group", "title": "G"},
                {"channel_id": -100123, "channel_type": "channel", "title": "C"},
            ]
        )
        action = MagicMock()
        action.delete_dialogs = AsyncMock(
            return_value=SimpleNamespace(success_count=2, results={777: True, -100123: True})
        )
        with (
            patch("src.services.channel_service.ChannelService", return_value=ch_svc),
            patch("src.agent.tools.dialogs.TelegramActionService", return_value=action),
        ):
            handlers = _get_tool_handlers(mock_db, client_pool=MagicMock())
            await handlers["delete_dialogs"](
                {"phone": "+79001234567", "dialog_ids": "777,-100123,999", "confirm": True}
            )
        passed = dict(action.delete_dialogs.await_args.kwargs["dialogs"])
        assert passed[777] == "group"
        assert passed[-100123] == "channel"
        # Unknown id falls back to "channel" (parity with leave).
        assert passed[999] == "channel"


JOIN_TOOL_NAMES = ("join_channel", "join_chat", "subscribe_channel")


class TestDialogsToolJoinChannel:
    @pytest.mark.anyio
    @pytest.mark.parametrize("tool_name", JOIN_TOOL_NAMES)
    async def test_no_pool(self, mock_db, tool_name):
        handlers = _get_tool_handlers(mock_db, client_pool=None)
        result = await handlers[tool_name]({"phone": "+7123456", "target": "@prog_ai"})
        assert "CLI-режиме" in _text(result)

    @pytest.mark.anyio
    @pytest.mark.parametrize("tool_name", JOIN_TOOL_NAMES)
    async def test_missing_target(self, mock_db, tool_name):
        mock_db.get_accounts = AsyncMock(
            return_value=[SimpleNamespace(phone="+79001234567", is_primary=True)]
        )
        mock_db.get_setting = AsyncMock(return_value=None)
        handlers = _get_tool_handlers(mock_db, client_pool=MagicMock())
        result = await handlers[tool_name]({"phone": "+79001234567", "confirm": True})
        assert "target обязателен" in _text(result)

    @pytest.mark.anyio
    @pytest.mark.parametrize("tool_name", JOIN_TOOL_NAMES)
    async def test_requires_confirmation(self, mock_db, tool_name):
        mock_db.get_accounts = AsyncMock(
            return_value=[SimpleNamespace(phone="+79001234567", is_primary=True)]
        )
        mock_db.get_setting = AsyncMock(return_value=None)
        handlers = _get_tool_handlers(mock_db, client_pool=MagicMock())
        result = await handlers[tool_name](
            {"phone": "+79001234567", "target": "@prog_ai", "confirm": False}
        )
        assert "Подтвердите" in _text(result)

    @pytest.mark.anyio
    @pytest.mark.parametrize("tool_name", JOIN_TOOL_NAMES)
    async def test_with_confirm_success(self, mock_db, tool_name):
        mock_db.get_accounts = AsyncMock(
            return_value=[SimpleNamespace(phone="+79001234567", is_primary=True)]
        )
        mock_db.get_setting = AsyncMock(return_value=None)
        mock_client = AsyncMock()
        mock_client.get_entity = AsyncMock(return_value="entity")
        mock_client.join_channel = AsyncMock()
        mock_pool = MagicMock()
        mock_pool.get_native_client_by_phone = AsyncMock(return_value=(mock_client, "+79001234567"))
        mock_pool.release_client = AsyncMock()

        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers[tool_name](
            {"phone": "+79001234567", "target": "@prog_ai", "confirm": True}
        )

        text = _text(result)
        assert "подписан/вступил" in text
        mock_client.join_channel.assert_awaited_once_with("entity")


class TestDialogsToolGetForumTopics:
    @pytest.mark.anyio
    async def test_missing_channel_id(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["get_forum_topics"]({})
        assert "channel_id обязателен" in _text(result)

    @pytest.mark.anyio
    async def test_empty_topics(self, mock_db):
        mock_db.get_forum_topics = AsyncMock(return_value=[])
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["get_forum_topics"]({"channel_id": 123})
        assert "не найдены" in _text(result)

    @pytest.mark.anyio
    async def test_with_topics(self, mock_db):
        topics = [
            {"topic_id": 1, "title": "General"},
            {"topic_id": 2, "title": "Off-topic"},
        ]
        mock_db.get_forum_topics = AsyncMock(return_value=topics)
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["get_forum_topics"]({"channel_id": 123})
        text = _text(result)
        assert "General" in text
        assert "Off-topic" in text
        assert "id=1" in text

    @pytest.mark.anyio
    async def test_error(self, mock_db):
        mock_db.get_forum_topics = AsyncMock(side_effect=Exception("no access"))
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["get_forum_topics"]({"channel_id": 123})
        assert "Ошибка" in _text(result)


class TestDialogsToolClearDialogCache:
    @pytest.mark.anyio
    async def test_requires_confirmation(self, mock_db):
        mock_db.get_setting = AsyncMock(return_value=None)
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["clear_dialog_cache"]({"phone": "+79001234567", "confirm": False})
        assert "Подтвердите" in _text(result)

    @pytest.mark.anyio
    async def test_clears_for_phone(self, mock_db):
        mock_db.get_setting = AsyncMock(return_value=None)
        mock_db.repos = MagicMock()
        mock_db.repos.dialog_cache.clear_dialogs = AsyncMock()
        mock_pool = MagicMock()
        mock_pool.invalidate_dialogs_cache = MagicMock()
        handlers = _get_tool_handlers(mock_db, client_pool=mock_pool)
        result = await handlers["clear_dialog_cache"]({"phone": "+79001234567", "confirm": True})
        assert "очищен" in _text(result)
        mock_db.repos.dialog_cache.clear_dialogs.assert_awaited_once_with("+79001234567")

    @pytest.mark.anyio
    async def test_clears_all_when_no_phone(self, mock_db):
        mock_db.get_setting = AsyncMock(return_value=None)
        mock_db.repos = MagicMock()
        mock_db.repos.dialog_cache.clear_all_dialogs = AsyncMock()
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["clear_dialog_cache"]({"phone": "", "confirm": True})
        assert "очищен" in _text(result)
        mock_db.repos.dialog_cache.clear_all_dialogs.assert_awaited_once()

    @pytest.mark.anyio
    async def test_no_phone_requires_explicit_phone_when_acl_configured(self, mock_db):
        mock_db.get_setting = AsyncMock(return_value='{"phone": {"clear_dialog_cache": true}}')
        mock_db.repos = MagicMock()
        mock_db.repos.dialog_cache.clear_all_dialogs = AsyncMock()
        handlers = _get_tool_handlers(mock_db)

        result = await handlers["clear_dialog_cache"]({"phone": "", "confirm": True})

        assert "Укажите phone" in _text(result)
        mock_db.repos.dialog_cache.clear_all_dialogs.assert_not_awaited()


class TestDialogsToolGetCacheStatus:
    @pytest.mark.anyio
    async def test_empty_cache(self, mock_db):
        mock_db.repos = MagicMock()
        mock_db.repos.dialog_cache.get_all_phones = AsyncMock(return_value=[])
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["get_cache_status"]({})
        assert "пуст" in _text(result)

    @pytest.mark.anyio
    async def test_with_cache_entries(self, mock_db):
        from datetime import datetime, timezone

        mock_db.repos = MagicMock()
        mock_db.repos.dialog_cache.get_all_phones = AsyncMock(return_value=["+79001234567"])
        mock_db.repos.dialog_cache.count_dialogs = AsyncMock(return_value=42)
        mock_db.repos.dialog_cache.get_cached_at = AsyncMock(
            return_value=datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        )
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["get_cache_status"]({})
        text = _text(result)
        assert "+79001234567" in text
        assert "42" in text
        assert "2026-01-01" in text

    @pytest.mark.anyio
    async def test_error(self, mock_db):
        mock_db.repos = MagicMock()
        mock_db.repos.dialog_cache.get_all_phones = AsyncMock(side_effect=Exception("cache err"))
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["get_cache_status"]({})
        assert "Ошибка" in _text(result)
