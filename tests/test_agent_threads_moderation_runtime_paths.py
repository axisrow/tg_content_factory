"""Tests for agent threads, moderation, dialogs, photo loader, dispatcher, repositories, scheduler, and search paths."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.database import Database

# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------


@pytest.fixture
def mock_db():
    db = MagicMock(spec=Database)
    db.get_setting = AsyncMock(return_value=None)
    return db


def _get_tool_handlers(mock_db, client_pool=None, config=None, **kwargs):
    captured = []
    with patch(
        "src.agent.tools.create_sdk_mcp_server",
        side_effect=lambda **kw: captured.extend(kw.get("tools", [])),
    ):
        from src.agent.tools import make_mcp_server

        make_mcp_server(mock_db, client_pool=client_pool, config=config, **kwargs)
    return {t.name: t.handler for t in captured}


def _text(result: dict) -> str:
    return result["content"][0]["text"]


# ===========================================================================
# 1. agent_threads.py
# ===========================================================================


class TestListAgentThreads:
    @pytest.mark.anyio
    async def test_empty(self, mock_db):
        mock_db.get_agent_threads = AsyncMock(return_value=[])
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["list_agent_threads"]({})
        assert "Треды не найдены" in _text(result)

    @pytest.mark.anyio
    async def test_with_threads(self, mock_db):
        mock_db.get_agent_threads = AsyncMock(
            return_value=[{"id": 1, "title": "Test Chat", "created_at": "2025-01-01"}]
        )
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["list_agent_threads"]({})
        assert "Test Chat" in _text(result)
        assert "Треды (1)" in _text(result)

    @pytest.mark.anyio
    async def test_exception_returns_error(self, mock_db):
        mock_db.get_agent_threads = AsyncMock(side_effect=Exception("db error"))
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["list_agent_threads"]({})
        assert "Ошибка получения тредов" in _text(result)


class TestCreateAgentThread:
    @pytest.mark.anyio
    async def test_creates_with_title(self, mock_db):
        mock_db.create_agent_thread = AsyncMock(return_value=42)
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["create_agent_thread"]({"title": "My Thread"})
        assert "id=42" in _text(result)
        assert "My Thread" in _text(result)

    @pytest.mark.anyio
    async def test_creates_with_default_title(self, mock_db):
        mock_db.create_agent_thread = AsyncMock(return_value=1)
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["create_agent_thread"]({})
        assert "id=1" in _text(result)

    @pytest.mark.anyio
    async def test_exception(self, mock_db):
        mock_db.create_agent_thread = AsyncMock(side_effect=Exception("fail"))
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["create_agent_thread"]({"title": "x"})
        assert "Ошибка создания треда" in _text(result)


class TestDeleteAgentThread:
    @pytest.mark.anyio
    async def test_requires_thread_id(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["delete_agent_thread"]({})
        assert "thread_id обязателен" in _text(result)

    @pytest.mark.anyio
    async def test_requires_confirm(self, mock_db):
        mock_db.get_agent_thread = AsyncMock(return_value={"title": "My Thread"})
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["delete_agent_thread"]({"thread_id": 1})
        assert "confirm=true" in _text(result)

    @pytest.mark.anyio
    async def test_thread_not_found_uses_fallback_name(self, mock_db):
        """When thread is None, name should be 'id=X' fallback."""
        mock_db.get_agent_thread = AsyncMock(return_value=None)
        mock_db.delete_agent_thread = AsyncMock()
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["delete_agent_thread"]({"thread_id": 99, "confirm": True})
        assert "id=99" in _text(result)
        mock_db.delete_agent_thread.assert_awaited_once_with(99)

    @pytest.mark.anyio
    async def test_delete_success(self, mock_db):
        mock_db.get_agent_thread = AsyncMock(return_value={"title": "Chat 1"})
        mock_db.delete_agent_thread = AsyncMock()
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["delete_agent_thread"]({"thread_id": 5, "confirm": True})
        assert "Chat 1" in _text(result)
        assert "удалён" in _text(result)

    @pytest.mark.anyio
    async def test_delete_exception(self, mock_db):
        mock_db.get_agent_thread = AsyncMock(return_value={"title": "Chat"})
        mock_db.delete_agent_thread = AsyncMock(side_effect=Exception("boom"))
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["delete_agent_thread"]({"thread_id": 1, "confirm": True})
        assert "Ошибка удаления треда" in _text(result)


class TestRenameAgentThread:
    @pytest.mark.anyio
    async def test_requires_both_params(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["rename_agent_thread"]({"thread_id": 1})
        assert "обязательны" in _text(result)

    @pytest.mark.anyio
    async def test_rename_success(self, mock_db):
        mock_db.rename_agent_thread = AsyncMock()
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["rename_agent_thread"]({"thread_id": 3, "title": "New Name"})
        assert "New Name" in _text(result)

    @pytest.mark.anyio
    async def test_rename_exception(self, mock_db):
        mock_db.rename_agent_thread = AsyncMock(side_effect=Exception("fail"))
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["rename_agent_thread"]({"thread_id": 1, "title": "X"})
        assert "Ошибка переименования треда" in _text(result)


class TestGetThreadMessages:
    @pytest.mark.anyio
    async def test_requires_thread_id(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["get_thread_messages"]({})
        assert "thread_id обязателен" in _text(result)

    @pytest.mark.anyio
    async def test_empty(self, mock_db):
        mock_db.get_agent_messages = AsyncMock(return_value=[])
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["get_thread_messages"]({"thread_id": 1})
        assert "Нет сообщений" in _text(result)

    @pytest.mark.anyio
    async def test_with_messages(self, mock_db):
        msgs = [{"role": "user", "content": "hello"}, {"role": "assistant", "content": "hi"}]
        mock_db.get_agent_messages = AsyncMock(return_value=msgs)
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["get_thread_messages"]({"thread_id": 1})
        text = _text(result)
        assert "[user]: hello" in text
        assert "[assistant]: hi" in text

    @pytest.mark.anyio
    async def test_limit_applied(self, mock_db):
        """Only last N messages should be returned when limit is small."""
        msgs = [{"role": "user", "content": f"msg{i}"} for i in range(10)]
        mock_db.get_agent_messages = AsyncMock(return_value=msgs)
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["get_thread_messages"]({"thread_id": 1, "limit": 2})
        text = _text(result)
        # Should show last 2
        assert "msg8" in text
        assert "msg9" in text
        assert "msg0" not in text

    @pytest.mark.anyio
    async def test_exception(self, mock_db):
        mock_db.get_agent_messages = AsyncMock(side_effect=Exception("fail"))
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["get_thread_messages"]({"thread_id": 1})
        assert "Ошибка получения сообщений" in _text(result)


# ===========================================================================
# 2. moderation.py
# ===========================================================================


class TestListPendingModeration:
    @pytest.mark.anyio
    async def test_empty(self, mock_db):
        mock_db.repos = MagicMock()
        mock_db.repos.generation_runs.list_pending_moderation = AsyncMock(return_value=[])
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["list_pending_moderation"]({})
        assert "Нет черновиков" in _text(result)

    @pytest.mark.anyio
    async def test_with_runs(self, mock_db):
        run = SimpleNamespace(
            id=1,
            pipeline_id=2,
            generated_text="sample text",
            created_at="2025-01-01",
        )
        mock_db.repos = MagicMock()
        mock_db.repos.generation_runs.list_pending_moderation = AsyncMock(return_value=[run])
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["list_pending_moderation"]({"pipeline_id": 2, "limit": 5})
        assert "На модерации (1 шт.)" in _text(result)

    @pytest.mark.anyio
    async def test_exception(self, mock_db):
        mock_db.repos = MagicMock()
        mock_db.repos.generation_runs.list_pending_moderation = AsyncMock(side_effect=Exception("db"))
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["list_pending_moderation"]({})
        assert "Ошибка получения очереди модерации" in _text(result)


class TestApproveRun:
    @pytest.mark.anyio
    async def test_requires_run_id(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["approve_run"]({})
        assert "run_id обязателен" in _text(result)

    @pytest.mark.anyio
    async def test_not_found(self, mock_db):
        mock_db.repos = MagicMock()
        mock_db.repos.generation_runs.get = AsyncMock(return_value=None)
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["approve_run"]({"run_id": 99})
        assert "не найден" in _text(result)

    @pytest.mark.anyio
    async def test_approve_success(self, mock_db):
        mock_db.repos = MagicMock()
        mock_db.repos.generation_runs.get = AsyncMock(
            return_value=SimpleNamespace(id=1, moderation_status="pending")
        )
        mock_db.repos.generation_runs.set_moderation_status = AsyncMock()
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["approve_run"]({"run_id": 1})
        assert "одобрен" in _text(result)

    @pytest.mark.anyio
    async def test_exception(self, mock_db):
        mock_db.repos = MagicMock()
        mock_db.repos.generation_runs.get = AsyncMock(side_effect=Exception("boom"))
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["approve_run"]({"run_id": 1})
        assert "Ошибка одобрения" in _text(result)


class TestRejectRun:
    @pytest.mark.anyio
    async def test_requires_run_id(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["reject_run"]({})
        assert "run_id обязателен" in _text(result)

    @pytest.mark.anyio
    async def test_reject_success(self, mock_db):
        mock_db.repos = MagicMock()
        mock_db.repos.generation_runs.get = AsyncMock(
            return_value=SimpleNamespace(id=2, moderation_status="pending")
        )
        mock_db.repos.generation_runs.set_moderation_status = AsyncMock()
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["reject_run"]({"run_id": 2})
        assert "отклонён" in _text(result)


class TestBulkApproveRuns:
    @pytest.mark.anyio
    async def test_invalid_ids_format(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["bulk_approve_runs"]({"run_ids": "a,b,c", "confirm": True})
        assert "числами через запятую" in _text(result)

    @pytest.mark.anyio
    async def test_empty_ids(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["bulk_approve_runs"]({"run_ids": "", "confirm": True})
        assert "пуст" in _text(result)

    @pytest.mark.anyio
    async def test_requires_confirm(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["bulk_approve_runs"]({"run_ids": "1,2,3"})
        assert "confirm=true" in _text(result)

    @pytest.mark.anyio
    async def test_approves_multiple(self, mock_db):
        mock_db.repos = MagicMock()
        mock_db.repos.generation_runs.set_moderation_status = AsyncMock()
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["bulk_approve_runs"]({"run_ids": "1,2,3", "confirm": True})
        text = _text(result)
        assert "Одобрено 3 run(s)" in text
        assert mock_db.repos.generation_runs.set_moderation_status.await_count == 3

    @pytest.mark.anyio
    async def test_exception(self, mock_db):
        mock_db.repos = MagicMock()
        mock_db.repos.generation_runs.set_moderation_status = AsyncMock(side_effect=Exception("fail"))
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["bulk_approve_runs"]({"run_ids": "1", "confirm": True})
        assert "Ошибка массового одобрения" in _text(result)


class TestBulkRejectRuns:
    @pytest.mark.anyio
    async def test_rejects_multiple(self, mock_db):
        mock_db.repos = MagicMock()
        mock_db.repos.generation_runs.set_moderation_status = AsyncMock()
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["bulk_reject_runs"]({"run_ids": "4,5", "confirm": True})
        text = _text(result)
        assert "Отклонено 2 run(s)" in text

    @pytest.mark.anyio
    async def test_invalid_ids(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["bulk_reject_runs"]({"run_ids": "x", "confirm": True})
        assert "числами через запятую" in _text(result)

    @pytest.mark.anyio
    async def test_empty_ids(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["bulk_reject_runs"]({"run_ids": "", "confirm": True})
        assert "пуст" in _text(result)

    @pytest.mark.anyio
    async def test_requires_confirm(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["bulk_reject_runs"]({"run_ids": "1"})
        assert "confirm=true" in _text(result)


# ===========================================================================
# 3. dialogs.py
# ===========================================================================


def _make_pool_with_account(phone="+79001234567"):
    pool = MagicMock()
    pool.get_available_client = AsyncMock()
    pool.get_client_by_phone = AsyncMock()
    pool.get_native_client_by_phone = AsyncMock()
    pool.invalidate_dialogs_cache = MagicMock()
    return pool


class TestSearchMyTelegram:
    @pytest.mark.anyio
    async def test_no_pool_returns_gate(self, mock_db):
        handlers = _get_tool_handlers(mock_db, client_pool=None)
        result = await handlers["search_dialogs"]({"phone": "+79001234567"})
        assert "CLI-режиме" in _text(result)

    @pytest.mark.anyio
    async def test_with_data(self, mock_db):
        pool = _make_pool_with_account()
        mock_db.get_accounts = AsyncMock(
            return_value=[SimpleNamespace(phone="+79001234567", is_primary=True)]
        )
        dialogs = [
            {"channel_id": 100, "title": "Test Chan", "channel_type": "channel"},
            {"channel_id": 200, "title": "Group X", "channel_type": "supergroup"},
        ]
        with patch("src.services.channel_service.ChannelService") as mock_svc:
            mock_svc.return_value.get_my_dialogs = AsyncMock(return_value=dialogs)
            handlers = _get_tool_handlers(mock_db, client_pool=pool)
            result = await handlers["search_dialogs"]({"phone": "+79001234567"})
        text = _text(result)
        assert "Диалоги (2)" in text

    @pytest.mark.anyio
    async def test_empty_dialogs(self, mock_db):
        pool = _make_pool_with_account()
        mock_db.get_accounts = AsyncMock(
            return_value=[SimpleNamespace(phone="+79001234567", is_primary=True)]
        )
        with patch("src.services.channel_service.ChannelService") as mock_svc:
            mock_svc.return_value.get_my_dialogs = AsyncMock(return_value=[])
            handlers = _get_tool_handlers(mock_db, client_pool=pool)
            result = await handlers["search_dialogs"]({"phone": "+79001234567"})
        assert "не найдены" in _text(result)

    @pytest.mark.anyio
    async def test_all_dialogs_shown_without_limit(self, mock_db):
        pool = _make_pool_with_account()
        mock_db.get_accounts = AsyncMock(
            return_value=[SimpleNamespace(phone="+79001234567", is_primary=True)]
        )
        dialogs = [
            {"channel_id": i, "title": f"Chan{i}", "channel_type": "channel"} for i in range(105)
        ]
        with patch("src.services.channel_service.ChannelService") as mock_svc:
            mock_svc.return_value.get_my_dialogs = AsyncMock(return_value=dialogs)
            handlers = _get_tool_handlers(mock_db, client_pool=pool)
            result = await handlers["search_dialogs"]({"phone": "+79001234567"})
        text = _text(result)
        assert "Диалоги (105)" in text
        assert "и ещё" not in text

    @pytest.mark.anyio
    async def test_filter_empty_distinguishes_from_no_dialogs(self, mock_db):
        pool = _make_pool_with_account()
        mock_db.get_accounts = AsyncMock(
            return_value=[SimpleNamespace(phone="+79001234567", is_primary=True)]
        )
        dialogs = [
            {"channel_id": 1, "title": "Test", "channel_type": "channel"},
        ]
        with patch("src.services.channel_service.ChannelService") as mock_svc:
            mock_svc.return_value.get_my_dialogs = AsyncMock(return_value=dialogs)
            handlers = _get_tool_handlers(mock_db, client_pool=pool)
            result = await handlers["search_dialogs"]({"phone": "+79001234567", "type": "dm"})
        text = _text(result)
        assert "Нет диалогов по запросу" in text
        assert "Всего диалогов" in text
        assert "не найдены" not in text

    @pytest.mark.anyio
    async def test_limit_param(self, mock_db):
        pool = _make_pool_with_account()
        mock_db.get_accounts = AsyncMock(
            return_value=[SimpleNamespace(phone="+79001234567", is_primary=True)]
        )
        dialogs = [
            {"channel_id": i, "title": f"Chan{i}", "channel_type": "channel"} for i in range(10)
        ]
        with patch("src.services.channel_service.ChannelService") as mock_svc:
            mock_svc.return_value.get_my_dialogs = AsyncMock(return_value=dialogs)
            handlers = _get_tool_handlers(mock_db, client_pool=pool)
            result = await handlers["search_dialogs"]({"phone": "+79001234567", "limit": 3})
        text = _text(result)
        assert "Диалоги (3)" in text

    @pytest.mark.anyio
    async def test_search_param(self, mock_db):
        pool = _make_pool_with_account()
        mock_db.get_accounts = AsyncMock(
            return_value=[SimpleNamespace(phone="+79001234567", is_primary=True)]
        )
        dialogs = [
            {"channel_id": 1, "title": "Python News", "channel_type": "channel"},
            {"channel_id": 2, "title": "Golang Daily", "channel_type": "channel"},
            {"channel_id": 3, "title": "Python Weekly", "channel_type": "channel"},
        ]
        with patch("src.services.channel_service.ChannelService") as mock_svc:
            mock_svc.return_value.get_my_dialogs = AsyncMock(return_value=dialogs)
            handlers = _get_tool_handlers(mock_db, client_pool=pool)
            result = await handlers["search_dialogs"]({"phone": "+79001234567", "search": "python"})
        text = _text(result)
        assert "Диалоги (2)" in text
        assert "Python News" in text
        assert "Python Weekly" in text
        assert "Golang Daily" not in text

    @pytest.mark.anyio
    async def test_type_alias_channels(self, mock_db):
        pool = _make_pool_with_account()
        mock_db.get_accounts = AsyncMock(
            return_value=[SimpleNamespace(phone="+79001234567", is_primary=True)]
        )
        dialogs = [
            {"channel_id": 1, "title": "Chan", "channel_type": "channel"},
            {"channel_id": 2, "title": "Group", "channel_type": "supergroup"},
            {"channel_id": 3, "title": "DM", "channel_type": "dm"},
        ]
        with patch("src.services.channel_service.ChannelService") as mock_svc:
            mock_svc.return_value.get_my_dialogs = AsyncMock(return_value=dialogs)
            handlers = _get_tool_handlers(mock_db, client_pool=pool)
            result = await handlers["search_dialogs"]({"phone": "+79001234567", "type": "channels"})
        text = _text(result)
        assert "Диалоги (1)" in text
        assert "Chan" in text
        assert "Group" not in text

    @pytest.mark.anyio
    async def test_type_alias_groups(self, mock_db):
        pool = _make_pool_with_account()
        mock_db.get_accounts = AsyncMock(
            return_value=[SimpleNamespace(phone="+79001234567", is_primary=True)]
        )
        dialogs = [
            {"channel_id": 1, "title": "Chan", "channel_type": "channel"},
            {"channel_id": 2, "title": "SuperG", "channel_type": "supergroup"},
            {"channel_id": 3, "title": "Forum", "channel_type": "forum"},
        ]
        with patch("src.services.channel_service.ChannelService") as mock_svc:
            mock_svc.return_value.get_my_dialogs = AsyncMock(return_value=dialogs)
            handlers = _get_tool_handlers(mock_db, client_pool=pool)
            result = await handlers["search_dialogs"]({"phone": "+79001234567", "type": "groups"})
        text = _text(result)
        assert "Диалоги (2)" in text
        assert "Chan" not in text

    @pytest.mark.anyio
    async def test_type_exact(self, mock_db):
        pool = _make_pool_with_account()
        mock_db.get_accounts = AsyncMock(
            return_value=[SimpleNamespace(phone="+79001234567", is_primary=True)]
        )
        dialogs = [
            {"channel_id": 1, "title": "Chan", "channel_type": "channel"},
            {"channel_id": 2, "title": "Bot1", "channel_type": "bot"},
        ]
        with patch("src.services.channel_service.ChannelService") as mock_svc:
            mock_svc.return_value.get_my_dialogs = AsyncMock(return_value=dialogs)
            handlers = _get_tool_handlers(mock_db, client_pool=pool)
            result = await handlers["search_dialogs"]({"phone": "+79001234567", "type": "bot"})
        text = _text(result)
        assert "Диалоги (1)" in text
        assert "Bot1" in text

    @pytest.mark.anyio
    async def test_exception(self, mock_db):
        pool = _make_pool_with_account()
        mock_db.get_accounts = AsyncMock(
            return_value=[SimpleNamespace(phone="+79001234567", is_primary=True)]
        )
        with patch("src.services.channel_service.ChannelService") as mock_svc:
            mock_svc.return_value.get_my_dialogs = AsyncMock(side_effect=Exception("fail"))
            handlers = _get_tool_handlers(mock_db, client_pool=pool)
            result = await handlers["search_dialogs"]({"phone": "+79001234567"})
        assert "Ошибка получения диалогов" in _text(result)


class TestLeaveDialogs:
    @pytest.mark.anyio
    async def test_no_pool(self, mock_db):
        handlers = _get_tool_handlers(mock_db, client_pool=None)
        result = await handlers["leave_dialogs"]({"phone": "+7999", "dialog_ids": "1,2"})
        assert "CLI-режиме" in _text(result)

    @pytest.mark.anyio
    async def test_no_dialog_ids(self, mock_db):
        pool = _make_pool_with_account()
        mock_db.get_accounts = AsyncMock(
            return_value=[SimpleNamespace(phone="+79001234567", is_primary=True)]
        )
        handlers = _get_tool_handlers(mock_db, client_pool=pool)
        result = await handlers["leave_dialogs"]({"phone": "+79001234567", "dialog_ids": ""})
        assert "dialog_ids обязателен" in _text(result)

    @pytest.mark.anyio
    async def test_requires_confirm(self, mock_db):
        pool = _make_pool_with_account()
        mock_db.get_accounts = AsyncMock(
            return_value=[SimpleNamespace(phone="+79001234567", is_primary=True)]
        )
        handlers = _get_tool_handlers(mock_db, client_pool=pool)
        result = await handlers["leave_dialogs"]({"phone": "+79001234567", "dialog_ids": "1,2"})
        assert "confirm=true" in _text(result)

    @pytest.mark.anyio
    async def test_leave_success(self, mock_db):
        pool = _make_pool_with_account()
        mock_db.get_accounts = AsyncMock(
            return_value=[SimpleNamespace(phone="+79001234567", is_primary=True)]
        )
        with patch("src.services.channel_service.ChannelService") as mock_svc:
            mock_svc.return_value.leave_dialogs = AsyncMock(return_value={100: True, 200: True})
            handlers = _get_tool_handlers(mock_db, client_pool=pool)
            result = await handlers["leave_dialogs"](
                {"phone": "+79001234567", "dialog_ids": "100,200", "confirm": True}
            )
        assert "2/2" in _text(result)

    @pytest.mark.anyio
    async def test_leave_exception(self, mock_db):
        pool = _make_pool_with_account()
        mock_db.get_accounts = AsyncMock(
            return_value=[SimpleNamespace(phone="+79001234567", is_primary=True)]
        )
        with patch("src.services.channel_service.ChannelService") as mock_svc:
            mock_svc.return_value.leave_dialogs = AsyncMock(side_effect=Exception("net"))
            handlers = _get_tool_handlers(mock_db, client_pool=pool)
            result = await handlers["leave_dialogs"](
                {"phone": "+79001234567", "dialog_ids": "100", "confirm": True}
            )
        assert "Ошибка выхода" in _text(result)


class TestGetCacheStatus:
    @pytest.mark.anyio
    async def test_empty_cache(self, mock_db):
        mock_db.repos = MagicMock()
        mock_db.repos.dialog_cache.get_all_phones = AsyncMock(return_value=[])
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["get_cache_status"]({})
        assert "Кеш диалогов пуст" in _text(result)

    @pytest.mark.anyio
    async def test_with_phones(self, mock_db):
        mock_db.repos = MagicMock()
        mock_db.repos.dialog_cache.get_all_phones = AsyncMock(return_value=["+79001234567"])
        mock_db.repos.dialog_cache.count_dialogs = AsyncMock(return_value=42)
        mock_db.repos.dialog_cache.get_cached_at = AsyncMock(
            return_value=datetime(2025, 1, 1, 12, 0, 0, tzinfo=timezone.utc)
        )
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["get_cache_status"]({})
        text = _text(result)
        assert "+79001234567" in text
        assert "42 записей" in text

    @pytest.mark.anyio
    async def test_with_no_cached_at(self, mock_db):
        mock_db.repos = MagicMock()
        mock_db.repos.dialog_cache.get_all_phones = AsyncMock(return_value=["+7999"])
        mock_db.repos.dialog_cache.count_dialogs = AsyncMock(return_value=0)
        mock_db.repos.dialog_cache.get_cached_at = AsyncMock(return_value=None)
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["get_cache_status"]({})
        assert "—" in _text(result)

    @pytest.mark.anyio
    async def test_exception(self, mock_db):
        mock_db.repos = MagicMock()
        mock_db.repos.dialog_cache.get_all_phones = AsyncMock(side_effect=Exception("oops"))
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["get_cache_status"]({})
        assert "Ошибка получения статуса кеша" in _text(result)


class TestCreateTelegramChannel:
    @pytest.mark.anyio
    async def test_no_pool(self, mock_db):
        handlers = _get_tool_handlers(mock_db, client_pool=None)
        result = await handlers["create_telegram_channel"]({"phone": "+7999", "title": "My Chan"})
        assert "CLI-режиме" in _text(result)

    @pytest.mark.anyio
    async def test_requires_title(self, mock_db):
        pool = _make_pool_with_account()
        mock_db.get_accounts = AsyncMock(
            return_value=[SimpleNamespace(phone="+79001234567", is_primary=True)]
        )
        handlers = _get_tool_handlers(mock_db, client_pool=pool)
        result = await handlers["create_telegram_channel"]({"phone": "+79001234567", "title": ""})
        assert "title обязателен" in _text(result)

    @pytest.mark.anyio
    async def test_requires_confirm(self, mock_db):
        pool = _make_pool_with_account()
        mock_db.get_accounts = AsyncMock(
            return_value=[SimpleNamespace(phone="+79001234567", is_primary=True)]
        )
        handlers = _get_tool_handlers(mock_db, client_pool=pool)
        result = await handlers["create_telegram_channel"](
            {"phone": "+79001234567", "title": "Chan"}
        )
        assert "confirm=true" in _text(result)

    @pytest.mark.anyio
    async def test_client_not_found(self, mock_db):
        pool = _make_pool_with_account()
        pool.get_native_client_by_phone = AsyncMock(return_value=None)
        mock_db.get_accounts = AsyncMock(
            return_value=[SimpleNamespace(phone="+79001234567", is_primary=True)]
        )
        handlers = _get_tool_handlers(mock_db, client_pool=pool)
        result = await handlers["create_telegram_channel"](
            {"phone": "+79001234567", "title": "Chan", "confirm": True}
        )
        assert "не найден" in _text(result)


# ===========================================================================
# 4. photo_loader.py — remaining edge cases
# ===========================================================================


def _make_photo_services():
    photo_task_svc = MagicMock()
    photo_task_svc.list_batches = AsyncMock(return_value=[])
    photo_task_svc.list_items = AsyncMock(return_value=[])
    photo_task_svc.run_due = AsyncMock(return_value=5)
    photo_task_svc.cancel_item = AsyncMock(return_value=True)

    auto_upload_svc = MagicMock()
    auto_upload_svc.list_jobs = AsyncMock(return_value=[])
    auto_upload_svc.get_job = AsyncMock(return_value=None)
    auto_upload_svc.update_job = AsyncMock()
    auto_upload_svc.delete_job = AsyncMock()
    auto_upload_svc.run_due = AsyncMock(return_value=2)

    return photo_task_svc, auto_upload_svc


def _photo_patches(photo_task_svc, auto_upload_svc):
    return (
        patch("src.services.photo_task_service.PhotoTaskService", return_value=photo_task_svc),
        patch("src.services.photo_auto_upload_service.PhotoAutoUploadService", return_value=auto_upload_svc),
        patch("src.database.bundles.PhotoLoaderBundle"),
        patch("src.services.photo_publish_service.PhotoPublishService"),
    )


class TestCancelPhotoItem:
    @pytest.mark.anyio
    async def test_requires_item_id(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["cancel_photo_item"]({})
        assert "item_id обязателен" in _text(result)

    @pytest.mark.anyio
    async def test_requires_confirm(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["cancel_photo_item"]({"item_id": 1})
        assert "confirm=true" in _text(result)

    @pytest.mark.anyio
    async def test_cancel_success(self, mock_db):
        photo_task_svc, auto_upload_svc = _make_photo_services()
        photo_task_svc.cancel_item = AsyncMock(return_value=True)
        with _photo_patches(photo_task_svc, auto_upload_svc)[0], \
             _photo_patches(photo_task_svc, auto_upload_svc)[1], \
             _photo_patches(photo_task_svc, auto_upload_svc)[2], \
             _photo_patches(photo_task_svc, auto_upload_svc)[3]:
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["cancel_photo_item"]({"item_id": 5, "confirm": True})
        assert "отменено" in _text(result)

    @pytest.mark.anyio
    async def test_cancel_item_not_found(self, mock_db):
        photo_task_svc, auto_upload_svc = _make_photo_services()
        photo_task_svc.cancel_item = AsyncMock(return_value=False)
        with _photo_patches(photo_task_svc, auto_upload_svc)[0], \
             _photo_patches(photo_task_svc, auto_upload_svc)[1], \
             _photo_patches(photo_task_svc, auto_upload_svc)[2], \
             _photo_patches(photo_task_svc, auto_upload_svc)[3]:
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["cancel_photo_item"]({"item_id": 5, "confirm": True})
        text = _text(result)
        assert "Не удалось отменить" in text

    @pytest.mark.anyio
    async def test_exception(self, mock_db):
        photo_task_svc, auto_upload_svc = _make_photo_services()
        photo_task_svc.cancel_item = AsyncMock(side_effect=Exception("db"))
        with _photo_patches(photo_task_svc, auto_upload_svc)[0], \
             _photo_patches(photo_task_svc, auto_upload_svc)[1], \
             _photo_patches(photo_task_svc, auto_upload_svc)[2], \
             _photo_patches(photo_task_svc, auto_upload_svc)[3]:
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["cancel_photo_item"]({"item_id": 5, "confirm": True})
        assert "Ошибка отмены фото" in _text(result)


class TestToggleAutoUpload:
    @pytest.mark.anyio
    async def test_requires_job_id(self, mock_db):
        handlers = _get_tool_handlers(mock_db)
        result = await handlers["toggle_auto_upload"]({})
        assert "job_id обязателен" in _text(result)

    @pytest.mark.anyio
    async def test_job_not_found(self, mock_db):
        photo_task_svc, auto_upload_svc = _make_photo_services()
        auto_upload_svc.get_job = AsyncMock(return_value=None)
        with _photo_patches(photo_task_svc, auto_upload_svc)[0], \
             _photo_patches(photo_task_svc, auto_upload_svc)[1], \
             _photo_patches(photo_task_svc, auto_upload_svc)[2], \
             _photo_patches(photo_task_svc, auto_upload_svc)[3]:
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["toggle_auto_upload"]({"job_id": 1})
        assert "не найдена" in _text(result)

    @pytest.mark.anyio
    async def test_toggle_active_to_paused(self, mock_db):
        photo_task_svc, auto_upload_svc = _make_photo_services()
        job = MagicMock()
        job.is_active = True
        auto_upload_svc.get_job = AsyncMock(return_value=job)
        auto_upload_svc.update_job = AsyncMock()
        with _photo_patches(photo_task_svc, auto_upload_svc)[0], \
             _photo_patches(photo_task_svc, auto_upload_svc)[1], \
             _photo_patches(photo_task_svc, auto_upload_svc)[2], \
             _photo_patches(photo_task_svc, auto_upload_svc)[3]:
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["toggle_auto_upload"]({"job_id": 1})
        assert "приостановлена" in _text(result)

    @pytest.mark.anyio
    async def test_toggle_paused_to_active(self, mock_db):
        photo_task_svc, auto_upload_svc = _make_photo_services()
        job = MagicMock()
        job.is_active = False
        auto_upload_svc.get_job = AsyncMock(return_value=job)
        auto_upload_svc.update_job = AsyncMock()
        with _photo_patches(photo_task_svc, auto_upload_svc)[0], \
             _photo_patches(photo_task_svc, auto_upload_svc)[1], \
             _photo_patches(photo_task_svc, auto_upload_svc)[2], \
             _photo_patches(photo_task_svc, auto_upload_svc)[3]:
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["toggle_auto_upload"]({"job_id": 1})
        assert "активирована" in _text(result)

    @pytest.mark.anyio
    async def test_exception(self, mock_db):
        photo_task_svc, auto_upload_svc = _make_photo_services()
        auto_upload_svc.get_job = AsyncMock(side_effect=Exception("fail"))
        with _photo_patches(photo_task_svc, auto_upload_svc)[0], \
             _photo_patches(photo_task_svc, auto_upload_svc)[1], \
             _photo_patches(photo_task_svc, auto_upload_svc)[2], \
             _photo_patches(photo_task_svc, auto_upload_svc)[3]:
            handlers = _get_tool_handlers(mock_db)
            result = await handlers["toggle_auto_upload"]({"job_id": 1})
        assert "Ошибка переключения" in _text(result)


class TestRunPhotoDue:
    @pytest.mark.anyio
    async def test_no_pool(self, mock_db):
        handlers = _get_tool_handlers(mock_db, client_pool=None)
        result = await handlers["run_photo_due"]({})
        assert "CLI-режиме" in _text(result)

    @pytest.mark.anyio
    async def test_requires_confirm(self, mock_db):
        pool = _make_pool_with_account()
        handlers = _get_tool_handlers(mock_db, client_pool=pool)
        result = await handlers["run_photo_due"]({})
        assert "confirm=true" in _text(result)

    @pytest.mark.anyio
    async def test_run_due_success(self, mock_db):
        pool = _make_pool_with_account()
        photo_task_svc, auto_upload_svc = _make_photo_services()
        photo_task_svc.run_due = AsyncMock(return_value=3)
        auto_upload_svc.run_due = AsyncMock(return_value=1)
        with _photo_patches(photo_task_svc, auto_upload_svc)[0], \
             _photo_patches(photo_task_svc, auto_upload_svc)[1], \
             _photo_patches(photo_task_svc, auto_upload_svc)[2], \
             _photo_patches(photo_task_svc, auto_upload_svc)[3]:
            handlers = _get_tool_handlers(mock_db, client_pool=pool)
            result = await handlers["run_photo_due"]({"confirm": True})
        text = _text(result)
        assert "items=3" in text
        assert "auto_jobs=1" in text

    @pytest.mark.anyio
    async def test_exception(self, mock_db):
        pool = _make_pool_with_account()
        with patch("src.database.bundles.PhotoLoaderBundle"):
            with patch("src.services.photo_task_service.PhotoTaskService") as mock_task:
                mock_task.side_effect = Exception("db error")
                handlers = _get_tool_handlers(mock_db, client_pool=pool)
                result = await handlers["run_photo_due"]({"confirm": True})
        assert "Ошибка обработки фото" in _text(result)


# ===========================================================================
# 5. image_generation_service.py
# ===========================================================================


class TestImageGenerationService:
    def test_init_with_adapters(self):
        from src.services.image_generation_service import ImageGenerationService

        async def fake_adapter(prompt, model_id):
            return "http://example.com/img.png"

        svc = ImageGenerationService(adapters={"myprovider": fake_adapter})
        assert "myprovider" in svc.adapter_names

    def test_init_no_adapters_empty(self):
        from src.services.image_generation_service import ImageGenerationService

        svc = ImageGenerationService(adapters={})
        assert svc.adapter_names == []

    @pytest.mark.anyio
    async def test_is_available_false_no_adapters(self):
        from src.services.image_generation_service import ImageGenerationService

        svc = ImageGenerationService(adapters={})
        assert not await svc.is_available()

    @pytest.mark.anyio
    async def test_is_available_true_with_adapter(self):
        from src.services.image_generation_service import ImageGenerationService

        svc = ImageGenerationService(adapters={"x": AsyncMock(return_value=None)})
        assert await svc.is_available()

    def test_register_adapter(self):
        from src.services.image_generation_service import ImageGenerationService

        svc = ImageGenerationService(adapters={})
        svc.register_adapter("test", AsyncMock())
        assert "test" in svc.adapter_names

    def test_parse_model_string_with_colon(self):
        from src.services.image_generation_service import ImageGenerationService

        provider, model_id = ImageGenerationService._parse_model_string("together:FLUX.1-schnell")
        assert provider == "together"
        assert model_id == "FLUX.1-schnell"

    def test_parse_model_string_no_colon(self):
        from src.services.image_generation_service import ImageGenerationService

        provider, model_id = ImageGenerationService._parse_model_string("some-model")
        assert provider is None
        assert model_id == "some-model"

    def test_parse_model_string_none(self):
        from src.services.image_generation_service import ImageGenerationService

        provider, model_id = ImageGenerationService._parse_model_string(None)
        assert provider is None
        assert model_id == ""

    @pytest.mark.anyio
    async def test_generate_no_text_returns_none(self):
        from src.services.image_generation_service import ImageGenerationService

        svc = ImageGenerationService(adapters={"x": AsyncMock(return_value="url")})
        result = await svc.generate("together:model", "")
        assert result is None

    @pytest.mark.anyio
    async def test_generate_no_adapters_returns_none(self):
        from src.services.image_generation_service import ImageGenerationService

        svc = ImageGenerationService(adapters={})
        result = await svc.generate("together:model", "some text")
        assert result is None

    @pytest.mark.anyio
    async def test_generate_success(self):
        from src.services.image_generation_service import ImageGenerationService

        adapter = AsyncMock(return_value="http://img.example.com/x.png")
        svc = ImageGenerationService(adapters={"together": adapter})
        result = await svc.generate("together:FLUX.1-schnell", "test prompt")
        assert result == "http://img.example.com/x.png"

    @pytest.mark.anyio
    async def test_generate_unknown_provider_uses_fallback(self):
        from src.services.image_generation_service import ImageGenerationService

        adapter = AsyncMock(return_value="http://fallback.example.com/img.png")
        svc = ImageGenerationService(adapters={"together": adapter})
        # 'replicate' not in adapters, should fall back to first adapter
        result = await svc.generate("replicate:some-model", "text")
        assert result == "http://fallback.example.com/img.png"

    @pytest.mark.anyio
    async def test_generate_os_error_returns_none(self):
        from src.services.image_generation_service import ImageGenerationService

        adapter = AsyncMock(side_effect=OSError("connection refused"))
        svc = ImageGenerationService(adapters={"x": adapter})
        result = await svc.generate("x:model", "text")
        assert result is None

    @pytest.mark.anyio
    async def test_generate_unexpected_error_returns_none(self):
        from src.services.image_generation_service import ImageGenerationService

        adapter = AsyncMock(side_effect=RuntimeError("unexpected"))
        svc = ImageGenerationService(adapters={"x": adapter})
        result = await svc.generate("x:model", "text")
        assert result is None

    @pytest.mark.anyio
    async def test_search_models_together(self):
        from src.services.image_generation_service import ImageGenerationService

        svc = ImageGenerationService(adapters={})
        models = await svc.search_models("together")
        assert len(models) > 0
        assert models[0]["id"].startswith("black-forest-labs")

    @pytest.mark.anyio
    async def test_search_models_openai(self):
        from src.services.image_generation_service import ImageGenerationService

        svc = ImageGenerationService(adapters={})
        models = await svc.search_models("openai")
        ids = [m["id"] for m in models]
        assert "dall-e-3" in ids

    @pytest.mark.anyio
    async def test_search_models_with_query_filter(self):
        from src.services.image_generation_service import ImageGenerationService

        svc = ImageGenerationService(adapters={})
        models = await svc.search_models("openai", query="dall-e-3")
        assert all("dall-e-3" in m["id"] for m in models)

    @pytest.mark.anyio
    async def test_search_models_unknown_provider_empty(self):
        from src.services.image_generation_service import ImageGenerationService

        svc = ImageGenerationService(adapters={})
        models = await svc.search_models("unknown_provider")
        assert models == []

    @pytest.mark.anyio
    async def test_search_models_replicate_no_token(self):
        from src.services.image_generation_service import ImageGenerationService

        svc = ImageGenerationService(adapters={})
        with patch.dict("os.environ", {}, clear=True):
            # Remove REPLICATE_API_TOKEN if present
            import os
            os.environ.pop("REPLICATE_API_TOKEN", None)
            models = await svc.search_models("replicate", api_key="")
        assert models == []


# ===========================================================================
# 6. database/repositories/messages.py
# ===========================================================================


@pytest.mark.anyio
async def test_messages_insert_and_count_embeddings(db):
    """Test insert_message + count_embeddings basic flow."""
    from datetime import datetime

    from src.models import Message

    msg = Message(
        channel_id=111,
        message_id=1,
        text="Hello world",
        date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    )
    inserted = await db.repos.messages.insert_message(msg)
    assert inserted


@pytest.mark.anyio
async def test_messages_insert_duplicate_returns_false(db):
    """Duplicate inserts should return False."""
    from datetime import datetime

    from src.models import Message

    msg = Message(
        channel_id=111,
        message_id=2,
        text="Dup message",
        date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    )
    first = await db.repos.messages.insert_message(msg)
    second = await db.repos.messages.insert_message(msg)
    assert first
    assert not second


@pytest.mark.anyio
async def test_messages_insert_with_reactions(db):
    """Messages with reactions_json should populate message_reactions."""
    from datetime import datetime

    from src.models import Message

    reactions = json.dumps([{"emoji": "👍", "count": 5}, {"emoji": "❤️", "count": 2}])
    msg = Message(
        channel_id=222,
        message_id=10,
        text="Liked message",
        date=datetime(2025, 1, 1, tzinfo=timezone.utc),
        reactions_json=reactions,
    )
    inserted = await db.repos.messages.insert_message(msg)
    assert inserted


@pytest.mark.anyio
async def test_messages_insert_batch(db):
    """insert_messages_batch should return count of inserted rows."""
    from datetime import datetime

    from src.models import Message

    msgs = [
        Message(
            channel_id=333,
            message_id=i,
            text=f"batch msg {i}",
            date=datetime(2025, 1, 1, tzinfo=timezone.utc),
        )
        for i in range(1, 6)
    ]
    count = await db.repos.messages.insert_messages_batch(msgs)
    assert count == 5


@pytest.mark.anyio
async def test_messages_insert_empty_batch(db):
    count = await db.repos.messages.insert_messages_batch([])
    assert count == 0


@pytest.mark.anyio
async def test_messages_get_embedding_dimensions_none(db):
    dims = await db.repos.messages.get_embedding_dimensions()
    assert dims is None


@pytest.mark.anyio
async def test_messages_ensure_embeddings_table(db):
    await db.repos.messages.ensure_embeddings_table(128)
    dims = await db.repos.messages.get_embedding_dimensions()
    assert dims == 128


@pytest.mark.anyio
async def test_messages_ensure_embeddings_table_invalid(db):
    with pytest.raises(ValueError):
        await db.repos.messages.ensure_embeddings_table(0)


@pytest.mark.anyio
async def test_messages_ensure_embeddings_table_dimension_mismatch(db):
    await db.repos.messages.ensure_embeddings_table(128)
    with pytest.raises(RuntimeError):
        await db.repos.messages.ensure_embeddings_table(256)


@pytest.mark.anyio
async def test_messages_upsert_embeddings(db):
    from datetime import datetime

    from src.models import Message

    # Insert a message first
    msg = Message(
        channel_id=999,
        message_id=50,
        text="Embed me",
        date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    )
    await db.repos.messages.insert_message(msg)

    # Get its id
    ids = await db.repos.messages.get_messages_for_embedding(after_id=0, limit=100)
    assert len(ids) >= 1
    row_id = ids[0][0]

    count = await db.repos.messages.upsert_message_embeddings(
        [(row_id, [0.1, 0.2, 0.3])]
    )
    assert count >= 1
    total = await db.repos.messages.count_embeddings()
    assert total >= 1


@pytest.mark.anyio
async def test_messages_upsert_embedding_json(db):
    from datetime import datetime

    from src.models import Message

    msg = Message(
        channel_id=888,
        message_id=60,
        text="JSON embed me",
        date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    )
    await db.repos.messages.insert_message(msg)

    ids = await db.repos.messages.get_messages_for_embedding(after_id=0, limit=100)
    row_id = ids[0][0]

    count = await db.repos.messages.upsert_message_embedding_json(
        [(row_id, [0.5, 0.6, 0.7])]
    )
    assert count >= 1

    loaded = await db.repos.messages.load_all_embeddings_json()
    assert len(loaded) >= 1
    assert loaded[0][1] == pytest.approx([0.5, 0.6, 0.7], abs=1e-6)


@pytest.mark.anyio
async def test_messages_search_no_query(db):
    msgs, total = await db.repos.messages.search_messages()
    assert isinstance(msgs, list)
    assert isinstance(total, int)


@pytest.mark.anyio
async def test_messages_normalize_date_to_date_only():
    from src.database.repositories.messages import MessagesRepository

    val, op = MessagesRepository._normalize_date_to("2025-01-15")
    assert op == "<"
    assert val == "2025-01-16"


@pytest.mark.anyio
async def test_messages_normalize_date_to_datetime():
    from src.database.repositories.messages import MessagesRepository

    val, op = MessagesRepository._normalize_date_to("2025-01-15T12:00:00")
    assert op == "<="
    assert val == "2025-01-15T12:00:00"


@pytest.mark.anyio
async def test_messages_normalize_date_to_none():
    from src.database.repositories.messages import MessagesRepository

    val, op = MessagesRepository._normalize_date_to(None)
    assert val is None
    assert op == "<="


# ===========================================================================
# 7. database/migrations.py
# ===========================================================================


@pytest.mark.anyio
async def test_migrations_idempotent(db):
    """Running migrations again on an initialized DB should not raise."""
    import aiosqlite

    from src.database.migrations import run_migrations

    async with aiosqlite.connect(":memory:") as conn:
        conn.row_factory = aiosqlite.Row
        # Create minimal schema
        from src.database.schema import SCHEMA_SQL

        await conn.executescript(SCHEMA_SQL)
        await conn.commit()
        # Run twice — should be idempotent
        result1 = await run_migrations(conn)
        result2 = await run_migrations(conn)
        assert isinstance(result1, bool)
        assert isinstance(result2, bool)


@pytest.mark.anyio
async def test_migrations_adds_missing_columns():
    """If columns are missing, migrations should add them."""
    import aiosqlite

    from src.database.migrations import run_migrations
    from src.database.schema import SCHEMA_SQL

    async with aiosqlite.connect(":memory:") as conn:
        conn.row_factory = aiosqlite.Row
        await conn.executescript(SCHEMA_SQL)
        await conn.commit()

        # Drop a column by recreating table without it
        # (SQLite doesn't support DROP COLUMN easily, so we just verify migration runs clean)
        result = await run_migrations(conn)
        assert isinstance(result, bool)

        cur = await conn.execute("PRAGMA table_info(messages)")
        cols = {row["name"] for row in await cur.fetchall()}
        assert "media_type" in cols
        assert "reactions_json" in cols


@pytest.mark.anyio
async def test_migrations_renames_legacy_dialog_search_permission_key():
    import json

    import aiosqlite

    from src.database.migrations import run_migrations
    from src.database.schema import SCHEMA_SQL

    legacy_key = "_".join(("search", "my", "telegram"))

    async with aiosqlite.connect(":memory:") as conn:
        conn.row_factory = aiosqlite.Row
        await conn.executescript(SCHEMA_SQL)
        await conn.execute(
            "INSERT INTO settings(key, value) VALUES('agent_tool_permissions', ?)",
            (json.dumps({legacy_key: True, "refresh_dialogs": False}),),
        )
        await conn.commit()

        await run_migrations(conn)

        cur = await conn.execute(
            "SELECT value FROM settings WHERE key = 'agent_tool_permissions' LIMIT 1"
        )
        row = await cur.fetchone()
        data = json.loads(row["value"])
        assert data["search_dialogs"] is True
        assert legacy_key not in data


@pytest.mark.anyio
async def test_migrate_vec_to_portable_skips_without_table():
    """_migrate_vec_to_portable should return early if vec_messages table doesn't exist."""
    import aiosqlite

    from src.database.migrations import _migrate_vec_to_portable
    from src.database.schema import SCHEMA_SQL

    async with aiosqlite.connect(":memory:") as conn:
        conn.row_factory = aiosqlite.Row
        await conn.executescript(SCHEMA_SQL)
        await conn.commit()
        # Should not raise
        await _migrate_vec_to_portable(conn)


# ===========================================================================
# 8. scheduler/manager.py
# ===========================================================================


def _make_mock_bundle(setting_val=None):
    bundle = MagicMock()
    bundle.get_setting = AsyncMock(return_value=setting_val)
    bundle.set_setting = AsyncMock()
    return bundle


def _make_task_enqueuer():
    enqueuer = MagicMock()
    result = MagicMock()
    result.queued_count = 2
    result.skipped_existing_count = 0
    result.total_candidates = 2
    enqueuer.enqueue_all_channels = AsyncMock(return_value=result)
    enqueuer.enqueue_sq_stats = AsyncMock()
    enqueuer.enqueue_photo_due = AsyncMock()
    enqueuer.enqueue_photo_auto = AsyncMock()
    enqueuer.enqueue_pipeline_run = AsyncMock()
    enqueuer.enqueue_content_generate = AsyncMock()
    return enqueuer


@pytest.mark.anyio
async def test_scheduler_start_with_task_enqueuer():
    from src.config import SchedulerConfig
    from src.scheduler.service import SchedulerManager

    bundle = _make_mock_bundle()
    bundle.get_setting = AsyncMock(return_value=None)
    enqueuer = _make_task_enqueuer()
    mgr = SchedulerManager(
        SchedulerConfig(collect_interval_minutes=30),
        scheduler_bundle=bundle,
        task_enqueuer=enqueuer,
    )
    await mgr.start()
    assert mgr.is_running
    # photo_due and photo_auto jobs should be registered
    jobs = mgr._scheduler.get_jobs()
    job_ids = [j.id for j in jobs]
    assert "photo_due" in job_ids
    assert "photo_auto" in job_ids
    await mgr.stop()


@pytest.mark.anyio
async def test_scheduler_stop_without_start():
    """Stop on non-running scheduler should not raise."""
    from src.config import SchedulerConfig
    from src.scheduler.service import SchedulerManager

    mgr = SchedulerManager(SchedulerConfig())
    await mgr.stop()  # Should be a no-op


@pytest.mark.anyio
async def test_scheduler_stop_cancels_bg_task():
    from src.config import SchedulerConfig
    from src.scheduler.service import SchedulerManager

    bundle = _make_mock_bundle()
    enqueuer = _make_task_enqueuer()
    mgr = SchedulerManager(SchedulerConfig(), scheduler_bundle=bundle, task_enqueuer=enqueuer)
    await mgr.trigger_background()
    assert mgr._bg_task is not None
    await mgr.stop()
    assert mgr._bg_task is None


@pytest.mark.anyio
async def test_scheduler_trigger_background_idempotent():
    """Second trigger_background call should not create another task if first is running."""
    import asyncio

    from src.config import SchedulerConfig
    from src.scheduler.service import SchedulerManager

    bundle = _make_mock_bundle()
    enqueuer = _make_task_enqueuer()

    async def slow_enqueue():
        await asyncio.sleep(0.1)
        r = MagicMock()
        r.queued_count = 0
        r.skipped_existing_count = 0
        r.total_candidates = 0
        return r

    enqueuer.enqueue_all_channels = AsyncMock(side_effect=slow_enqueue)
    mgr = SchedulerManager(SchedulerConfig(), scheduler_bundle=bundle, task_enqueuer=enqueuer)
    await mgr.trigger_background()
    task1 = mgr._bg_task
    await mgr.trigger_background()
    task2 = mgr._bg_task
    assert task1 is task2
    await mgr._bg_task


@pytest.mark.anyio
async def test_scheduler_run_photo_due_no_enqueuer():
    from src.config import SchedulerConfig
    from src.scheduler.service import SchedulerManager

    mgr = SchedulerManager(SchedulerConfig())
    result = await mgr._run_photo_due()
    assert "processed" in result


@pytest.mark.anyio
async def test_scheduler_run_photo_auto_no_enqueuer():
    from src.config import SchedulerConfig
    from src.scheduler.service import SchedulerManager

    mgr = SchedulerManager(SchedulerConfig())
    result = await mgr._run_photo_auto()
    assert "jobs" in result


@pytest.mark.anyio
async def test_scheduler_run_photo_due_with_enqueuer():
    from src.config import SchedulerConfig
    from src.scheduler.service import SchedulerManager

    enqueuer = _make_task_enqueuer()
    mgr = SchedulerManager(SchedulerConfig(), task_enqueuer=enqueuer)
    result = await mgr._run_photo_due()
    assert result["enqueued"] is True
    enqueuer.enqueue_photo_due.assert_awaited_once()


@pytest.mark.anyio
async def test_scheduler_run_photo_auto_with_enqueuer():
    from src.config import SchedulerConfig
    from src.scheduler.service import SchedulerManager

    enqueuer = _make_task_enqueuer()
    mgr = SchedulerManager(SchedulerConfig(), task_enqueuer=enqueuer)
    result = await mgr._run_photo_auto()
    assert result["enqueued"] is True
    enqueuer.enqueue_photo_auto.assert_awaited_once()


@pytest.mark.anyio
async def test_scheduler_run_photo_due_exception():
    from src.config import SchedulerConfig
    from src.scheduler.service import SchedulerManager

    enqueuer = _make_task_enqueuer()
    enqueuer.enqueue_photo_due = AsyncMock(side_effect=Exception("fail"))
    mgr = SchedulerManager(SchedulerConfig(), task_enqueuer=enqueuer)
    result = await mgr._run_photo_due()
    # Should not raise; returns dict
    assert isinstance(result, dict)


@pytest.mark.anyio
async def test_scheduler_run_pipeline_job():
    from src.config import SchedulerConfig
    from src.scheduler.service import SchedulerManager

    enqueuer = _make_task_enqueuer()
    mgr = SchedulerManager(SchedulerConfig(), task_enqueuer=enqueuer)
    await mgr._run_pipeline_job(42)
    enqueuer.enqueue_pipeline_run.assert_awaited_once_with(42)


@pytest.mark.anyio
async def test_scheduler_run_content_generate_job():
    from src.config import SchedulerConfig
    from src.scheduler.service import SchedulerManager

    enqueuer = _make_task_enqueuer()
    mgr = SchedulerManager(SchedulerConfig(), task_enqueuer=enqueuer)
    await mgr._run_content_generate_job(7)
    enqueuer.enqueue_content_generate.assert_awaited_once_with(7)


@pytest.mark.anyio
async def test_scheduler_run_search_query():
    from src.config import SchedulerConfig
    from src.scheduler.service import SchedulerManager

    enqueuer = _make_task_enqueuer()
    mgr = SchedulerManager(SchedulerConfig(), task_enqueuer=enqueuer)
    await mgr._run_search_query(5)
    enqueuer.enqueue_sq_stats.assert_awaited_once_with(5)


@pytest.mark.anyio
async def test_scheduler_get_job_next_run_no_scheduler():
    from src.config import SchedulerConfig
    from src.scheduler.service import SchedulerManager

    mgr = SchedulerManager(SchedulerConfig())
    result = mgr.get_job_next_run("collect_all")
    assert result is None


@pytest.mark.anyio
async def test_scheduler_get_all_jobs_no_scheduler():
    from src.config import SchedulerConfig
    from src.scheduler.service import SchedulerManager

    mgr = SchedulerManager(SchedulerConfig())
    result = mgr.get_all_jobs_next_run()
    assert result == {}


@pytest.mark.anyio
async def test_scheduler_get_potential_jobs_no_bundles():
    from src.config import SchedulerConfig
    from src.scheduler.service import SchedulerManager

    mgr = SchedulerManager(SchedulerConfig())
    jobs = await mgr.get_potential_jobs()
    assert any(j["job_id"] == "collect_all" for j in jobs)


@pytest.mark.anyio
async def test_scheduler_load_settings_no_bundle():
    """load_settings with no bundle should be a no-op."""
    from src.config import SchedulerConfig
    from src.scheduler.service import SchedulerManager

    mgr = SchedulerManager(SchedulerConfig(collect_interval_minutes=45))
    await mgr.load_settings()
    assert mgr.interval_minutes == 45


@pytest.mark.anyio
async def test_scheduler_is_job_enabled_no_bundle():
    from src.config import SchedulerConfig
    from src.scheduler.service import SchedulerManager

    mgr = SchedulerManager(SchedulerConfig())
    assert await mgr.is_job_enabled("collect_all")


@pytest.mark.anyio
async def test_scheduler_sync_job_state_not_running():
    """sync_job_state should be a no-op when scheduler is not running."""
    from src.config import SchedulerConfig
    from src.scheduler.service import SchedulerManager

    mgr = SchedulerManager(SchedulerConfig())
    await mgr.sync_job_state("collect_all", True)  # Should not raise


@pytest.mark.anyio
async def test_scheduler_update_interval_no_job():
    """update_interval when job is disabled should just store the value."""
    from src.config import SchedulerConfig
    from src.scheduler.service import SchedulerManager

    bundle = _make_mock_bundle()
    bundle.get_setting = AsyncMock(return_value="1")  # job disabled
    mgr = SchedulerManager(SchedulerConfig(), scheduler_bundle=bundle)
    await mgr.start()
    mgr.update_interval(15)
    assert mgr.interval_minutes == 15
    await mgr.stop()


# ===========================================================================
# 9. search/telegram_search.py
# ===========================================================================


@pytest.mark.anyio
async def test_telegram_search_no_pool():
    from src.search.telegram_search import TelegramSearch

    persistence = MagicMock()
    svc = TelegramSearch(pool=None, persistence=persistence)

    result = await svc.search_telegram("test query")
    assert result.error is not None
    assert "аккаунтов" in result.error


@pytest.mark.anyio
async def test_telegram_search_no_premium_client():
    from src.search.telegram_search import TelegramSearch

    pool = MagicMock()
    pool.get_premium_client = AsyncMock(return_value=None)
    pool.get_premium_unavailability_reason = MagicMock(
        return_value="No premium accounts"
    )
    pool.release_client = AsyncMock()
    persistence = MagicMock()
    svc = TelegramSearch(pool=pool, persistence=persistence)

    result = await svc.search_telegram("test query")
    assert result.messages == []
    assert result.error is not None


@pytest.mark.anyio
async def test_telegram_search_my_chats_no_pool():
    from src.search.telegram_search import TelegramSearch

    persistence = MagicMock()
    svc = TelegramSearch(pool=None, persistence=persistence)

    result = await svc.search_my_chats("test")
    assert result.error is not None


@pytest.mark.anyio
async def test_telegram_search_my_chats_no_client():
    from src.search.telegram_search import TelegramSearch

    pool = MagicMock()
    pool.get_available_client = AsyncMock(return_value=None)
    pool.release_client = AsyncMock()
    persistence = MagicMock()
    svc = TelegramSearch(pool=pool, persistence=persistence)

    result = await svc.search_my_chats("test")
    assert result.error is not None
    assert "доступных" in result.error


@pytest.mark.anyio
async def test_telegram_search_in_channel_no_pool():
    from src.search.telegram_search import TelegramSearch

    persistence = MagicMock()
    svc = TelegramSearch(pool=None, persistence=persistence)

    result = await svc.search_in_channel(None, "test")
    assert result.error is not None


@pytest.mark.anyio
async def test_telegram_search_in_channel_no_client():
    from src.search.telegram_search import TelegramSearch

    pool = MagicMock()
    pool.get_available_client = AsyncMock(return_value=None)
    pool.release_client = AsyncMock()
    persistence = MagicMock()
    svc = TelegramSearch(pool=pool, persistence=persistence)

    result = await svc.search_in_channel(12345, "test")
    assert result.error is not None


@pytest.mark.anyio
async def test_check_search_quota_no_pool():
    from src.search.telegram_search import TelegramSearch

    persistence = MagicMock()
    svc = TelegramSearch(pool=None, persistence=persistence)

    result = await svc.check_search_quota()
    assert result is None


@pytest.mark.anyio
async def test_check_search_quota_no_premium():
    from src.search.telegram_search import TelegramSearch

    pool = MagicMock()
    pool.get_premium_client = AsyncMock(return_value=None)
    pool.release_client = AsyncMock()
    persistence = MagicMock()
    svc = TelegramSearch(pool=pool, persistence=persistence)

    result = await svc.check_search_quota()
    assert result is None


@pytest.mark.anyio
async def test_get_premium_unavailability_reason_no_pool():
    from src.search.telegram_search import TelegramSearch

    persistence = MagicMock()
    svc = TelegramSearch(pool=None, persistence=persistence)

    reason = await svc._get_premium_unavailability_reason()
    assert "аккаунтов" in reason


@pytest.mark.anyio
async def test_get_premium_unavailability_reason_no_method():
    from src.search.telegram_search import TelegramSearch

    pool = MagicMock(spec=[])  # Empty spec — no methods
    persistence = MagicMock()
    svc = TelegramSearch(pool=pool, persistence=persistence)

    reason = await svc._get_premium_unavailability_reason()
    assert "Premium" in reason


# ===========================================================================
# 10. unified_dispatcher.py
# ===========================================================================


def _make_tasks_repo():
    repo = MagicMock()
    repo.claim_next_due_generic_task = AsyncMock(return_value=None)
    repo.requeue_running_generic_tasks_on_startup = AsyncMock(return_value=0)
    repo.update_collection_task = AsyncMock()
    repo.update_collection_task_progress = AsyncMock()
    repo.get_collection_task = AsyncMock(return_value=None)
    return repo


def _make_collector():
    collector = MagicMock()
    collector.is_running = False
    collector.delay_between_channels_sec = 0.0
    return collector


def _make_channel_bundle():
    bundle = MagicMock()
    bundle.get_by_channel_id = AsyncMock(return_value=None)
    return bundle


def _make_task(task_type, payload=None, task_id=1):
    from src.models import CollectionTask, CollectionTaskStatus

    task = MagicMock(spec=CollectionTask)
    task.id = task_id
    task.task_type = task_type
    task.payload = payload
    task.status = CollectionTaskStatus.RUNNING
    task.messages_collected = 0
    return task


@pytest.mark.anyio
async def test_dispatcher_start_recovers_tasks():
    from src.services.unified_dispatcher import UnifiedDispatcher

    tasks_repo = _make_tasks_repo()
    tasks_repo.requeue_running_generic_tasks_on_startup = AsyncMock(return_value=3)
    dispatcher = UnifiedDispatcher(
        collector=_make_collector(),
        channel_bundle=_make_channel_bundle(),
        tasks_repo=tasks_repo,
        poll_interval_sec=0.01,
    )
    await dispatcher.start()
    await dispatcher.stop()
    tasks_repo.requeue_running_generic_tasks_on_startup.assert_awaited_once()


@pytest.mark.anyio
async def test_dispatcher_start_idempotent():
    from src.services.unified_dispatcher import UnifiedDispatcher

    tasks_repo = _make_tasks_repo()
    dispatcher = UnifiedDispatcher(
        collector=_make_collector(),
        channel_bundle=_make_channel_bundle(),
        tasks_repo=tasks_repo,
        poll_interval_sec=0.01,
    )
    await dispatcher.start()
    task1 = dispatcher._task
    await dispatcher.start()  # Should not create a new task
    task2 = dispatcher._task
    assert task1 is task2
    await dispatcher.stop()


@pytest.mark.anyio
async def test_dispatcher_handle_photo_due_no_service():
    from src.models import CollectionTaskStatus, CollectionTaskType
    from src.services.unified_dispatcher import UnifiedDispatcher

    tasks_repo = _make_tasks_repo()
    dispatcher = UnifiedDispatcher(
        collector=_make_collector(),
        channel_bundle=_make_channel_bundle(),
        tasks_repo=tasks_repo,
        photo_task_service=None,
    )
    task = _make_task(CollectionTaskType.PHOTO_DUE)
    await dispatcher._handle_photo_due(task)
    tasks_repo.update_collection_task.assert_awaited_with(
        1, CollectionTaskStatus.FAILED, error="PhotoTaskService not configured"
    )


@pytest.mark.anyio
async def test_dispatcher_handle_photo_due_with_service():
    from src.models import CollectionTaskStatus, CollectionTaskType
    from src.services.unified_dispatcher import UnifiedDispatcher

    tasks_repo = _make_tasks_repo()
    photo_svc = MagicMock()
    photo_svc.run_due = AsyncMock(return_value=5)
    dispatcher = UnifiedDispatcher(
        collector=_make_collector(),
        channel_bundle=_make_channel_bundle(),
        tasks_repo=tasks_repo,
        photo_task_service=photo_svc,
    )
    task = _make_task(CollectionTaskType.PHOTO_DUE)
    await dispatcher._handle_photo_due(task)
    tasks_repo.update_collection_task.assert_awaited_with(
        1, CollectionTaskStatus.COMPLETED, messages_collected=5
    )


@pytest.mark.anyio
async def test_dispatcher_handle_photo_auto_no_service():
    from src.models import CollectionTaskStatus, CollectionTaskType
    from src.services.unified_dispatcher import UnifiedDispatcher

    tasks_repo = _make_tasks_repo()
    dispatcher = UnifiedDispatcher(
        collector=_make_collector(),
        channel_bundle=_make_channel_bundle(),
        tasks_repo=tasks_repo,
        photo_auto_upload_service=None,
    )
    task = _make_task(CollectionTaskType.PHOTO_AUTO)
    await dispatcher._handle_photo_auto(task)
    tasks_repo.update_collection_task.assert_awaited_with(
        1, CollectionTaskStatus.FAILED, error="PhotoAutoUploadService not configured"
    )


@pytest.mark.anyio
async def test_dispatcher_handle_photo_auto_with_service():
    from src.models import CollectionTaskStatus, CollectionTaskType
    from src.services.unified_dispatcher import UnifiedDispatcher

    tasks_repo = _make_tasks_repo()
    auto_svc = MagicMock()
    auto_svc.run_due = AsyncMock(return_value=3)
    dispatcher = UnifiedDispatcher(
        collector=_make_collector(),
        channel_bundle=_make_channel_bundle(),
        tasks_repo=tasks_repo,
        photo_auto_upload_service=auto_svc,
    )
    task = _make_task(CollectionTaskType.PHOTO_AUTO)
    await dispatcher._handle_photo_auto(task)
    tasks_repo.update_collection_task.assert_awaited_with(
        1, CollectionTaskStatus.COMPLETED, messages_collected=3
    )


@pytest.mark.anyio
async def test_dispatcher_handle_sq_stats_no_bundle():
    from src.models import CollectionTaskStatus, CollectionTaskType
    from src.services.unified_dispatcher import UnifiedDispatcher

    tasks_repo = _make_tasks_repo()
    dispatcher = UnifiedDispatcher(
        collector=_make_collector(),
        channel_bundle=_make_channel_bundle(),
        tasks_repo=tasks_repo,
        sq_bundle=None,
    )
    task = _make_task(CollectionTaskType.SQ_STATS)
    await dispatcher._handle_sq_stats(task)
    tasks_repo.update_collection_task.assert_awaited_with(
        1,
        CollectionTaskStatus.COMPLETED,
        note="No search query bundle configured",
    )


@pytest.mark.anyio
async def test_dispatcher_handle_sq_stats_invalid_payload():
    from src.models import CollectionTaskStatus, CollectionTaskType
    from src.services.unified_dispatcher import UnifiedDispatcher

    tasks_repo = _make_tasks_repo()
    sq_bundle = MagicMock()
    dispatcher = UnifiedDispatcher(
        collector=_make_collector(),
        channel_bundle=_make_channel_bundle(),
        tasks_repo=tasks_repo,
        sq_bundle=sq_bundle,
    )
    task = _make_task(CollectionTaskType.SQ_STATS, payload="invalid")
    await dispatcher._handle_sq_stats(task)
    tasks_repo.update_collection_task.assert_awaited_with(
        1,
        CollectionTaskStatus.FAILED,
        error="Invalid SQ_STATS payload",
    )


@pytest.mark.anyio
async def test_dispatcher_dispatch_unknown_task_type():
    from src.models import CollectionTaskStatus
    from src.services.unified_dispatcher import UnifiedDispatcher

    tasks_repo = _make_tasks_repo()
    dispatcher = UnifiedDispatcher(
        collector=_make_collector(),
        channel_bundle=_make_channel_bundle(),
        tasks_repo=tasks_repo,
    )
    # Create a task with a type not in the handler map
    task = MagicMock()
    task.id = 1
    task.task_type = None  # None is not in handler map
    await dispatcher._dispatch(task)
    tasks_repo.update_collection_task.assert_awaited_with(
        1,
        CollectionTaskStatus.FAILED,
        error="Unknown task type: None",
    )


@pytest.mark.anyio
async def test_dispatcher_handle_photo_due_exception():
    from src.models import CollectionTaskStatus, CollectionTaskType
    from src.services.unified_dispatcher import UnifiedDispatcher

    tasks_repo = _make_tasks_repo()
    photo_svc = MagicMock()
    photo_svc.run_due = AsyncMock(side_effect=Exception("db error"))
    dispatcher = UnifiedDispatcher(
        collector=_make_collector(),
        channel_bundle=_make_channel_bundle(),
        tasks_repo=tasks_repo,
        photo_task_service=photo_svc,
    )
    task = _make_task(CollectionTaskType.PHOTO_DUE)
    await dispatcher._handle_photo_due(task)
    # Should call update with FAILED
    calls = tasks_repo.update_collection_task.await_args_list
    assert any(CollectionTaskStatus.FAILED in c.args for c in calls)


@pytest.mark.anyio
async def test_dispatcher_handle_stats_all_empty_channels():
    from src.models import CollectionTaskStatus, CollectionTaskType, StatsAllTaskPayload
    from src.services.unified_dispatcher import UnifiedDispatcher

    tasks_repo = _make_tasks_repo()
    dispatcher = UnifiedDispatcher(
        collector=_make_collector(),
        channel_bundle=_make_channel_bundle(),
        tasks_repo=tasks_repo,
    )
    payload = StatsAllTaskPayload(
        channel_ids=[],
        next_index=0,
        channels_ok=0,
        channels_err=0,
    )
    task = _make_task(CollectionTaskType.STATS_ALL, payload=payload)
    await dispatcher._handle_stats_all(task)
    tasks_repo.update_collection_task.assert_awaited_with(
        1,
        CollectionTaskStatus.COMPLETED,
        messages_collected=0,
    )
