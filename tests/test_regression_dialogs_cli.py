"""Tests for Telegram dialogs CLI actions and tool error paths."""

from __future__ import annotations

import argparse
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.config import AppConfig
from src.database import Database

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_mock_db():
    db = MagicMock(spec=Database)
    db.repos = MagicMock()
    db._db_path = ":memory:"
    db._session_encryption_secret = None
    return db


def _make_pool_with_clients(phones=None):
    phones = phones or ["+1111"]
    pool = MagicMock()
    pool.clients = {p: MagicMock() for p in phones}
    pool.get_native_client_by_phone = AsyncMock(return_value=None)
    pool.get_available_client = AsyncMock(return_value=None)
    pool.get_forum_topics = AsyncMock(return_value=[])
    pool.invalidate_dialogs_cache = MagicMock()
    pool.disconnect_all = AsyncMock()
    pool._dialogs_cache = {}
    pool._dialogs_cache_ttl_sec = 300
    return pool




def _text(result) -> str:
    """Extract text from tool result payload."""
    if isinstance(result, dict):
        return result["content"][0]["text"]
    if hasattr(result, "content"):
        return result.content[0].text if hasattr(result.content[0], "text") else str(result.content[0])
    return str(result)




# ===========================================================================
# 1. cli/commands/test.py — read/write check functions
# ===========================================================================




class TestMyTelegramActions:
    """Test dialogs CLI actions by mocking pool/db and calling run()."""

    def _run_action(self, action, pool=None, db=None, extra_args=None):
        from src.cli.commands import dialogs

        pool = pool or _make_pool_with_clients()
        db = db or _make_mock_db()
        db.get_forum_topics = AsyncMock(return_value=[])
        db.close = AsyncMock()
        db.repos.dialog_cache = MagicMock()
        db.repos.dialog_cache.clear_dialogs = AsyncMock()
        db.repos.dialog_cache.clear_all_dialogs = AsyncMock()
        db.repos.dialog_cache.get_all_phones = AsyncMock(return_value=[])
        db.repos.dialog_cache.count_dialogs = AsyncMock(return_value=0)
        db.repos.dialog_cache.get_cached_at = AsyncMock(return_value=None)

        args_dict = {
            "config": "config.yaml",
            "dialogs_action": action,
            "phone": "+1111",
            "yes": True,
        }
        if extra_args:
            args_dict.update(extra_args)
        args = argparse.Namespace(**args_dict)

        config = AppConfig()

        async def fake_init_db(cfg):
            return config, db

        async def fake_init_pool(cfg, d):
            return cfg, pool

        with (
            patch("src.cli.commands.dialogs.runtime.init_db", side_effect=fake_init_db),
            patch("src.cli.commands.dialogs.runtime.init_pool", side_effect=fake_init_pool),
        ):
            dialogs.run(args)

    def test_refresh_action(self, capsys):
        pool = _make_pool_with_clients()
        db = _make_mock_db()
        db.close = AsyncMock()
        with patch(
            "src.cli.commands.dialogs.ChannelService.get_my_dialogs",
            new_callable=AsyncMock,
            return_value=[{"channel_id": 1, "title": "t"}],
        ):
            self._run_action("refresh", pool=pool, db=db)
        out = capsys.readouterr().out
        assert "refreshed" in out.lower() or "1 total" in out.lower()

    def test_list_action(self, capsys):
        pool = _make_pool_with_clients()
        db = _make_mock_db()
        db.close = AsyncMock()
        with patch(
            "src.cli.commands.dialogs.ChannelService.get_my_dialogs",
            new_callable=AsyncMock,
            return_value=[
                {
                    "channel_type": "channel",
                    "title": "Test",
                    "username": "test_ch",
                    "already_added": True,
                    "channel_id": 1,
                },
            ],
        ):
            self._run_action("list", pool=pool, db=db)
        out = capsys.readouterr().out
        assert "Test" in out

    def test_list_no_dialogs(self, capsys):
        pool = _make_pool_with_clients()
        db = _make_mock_db()
        db.close = AsyncMock()
        with patch(
            "src.cli.commands.dialogs.ChannelService.get_my_dialogs",
            new_callable=AsyncMock,
            return_value=[],
        ):
            self._run_action("list", pool=pool, db=db)
        out = capsys.readouterr().out
        assert "No dialogs" in out

    def test_list_no_accounts(self, capsys):
        pool = _make_pool_with_clients()
        pool.clients = {}
        self._run_action("list", pool=pool)
        out = capsys.readouterr().out
        assert "No connected" in out

    def test_list_phone_not_connected(self, capsys):
        pool = _make_pool_with_clients(["+2222"])
        self._run_action("list", pool=pool, extra_args={"phone": "+9999"})
        out = capsys.readouterr().out
        assert "not connected" in out

    def test_topics_action(self, capsys):
        pool = _make_pool_with_clients()
        pool.get_forum_topics = AsyncMock(return_value=[
            {"id": 1, "title": "General", "icon_emoji_id": None, "date": "2025-01-01"},
        ])
        self._run_action("topics", pool=pool, extra_args={"channel_id": 123})
        out = capsys.readouterr().out
        assert "General" in out

    def test_topics_empty(self, capsys):
        pool = _make_pool_with_clients()
        pool.get_forum_topics = AsyncMock(return_value=[])
        db = _make_mock_db()
        db.close = AsyncMock()
        db.get_forum_topics = AsyncMock(return_value=[])
        self._run_action("topics", pool=pool, db=db, extra_args={"channel_id": 123})
        out = capsys.readouterr().out
        assert "No forum topics" in out

    def test_send_action(self, capsys):
        pool = _make_pool_with_clients()
        mock_client = AsyncMock()
        pool.get_native_client_by_phone = AsyncMock(return_value=(mock_client, "+1111"))
        self._run_action("send", pool=pool, extra_args={"recipient": "@user", "text": "hello"})
        out = capsys.readouterr().out
        assert "sent" in out.lower()

    def test_send_no_client(self, capsys):
        pool = _make_pool_with_clients()
        pool.get_native_client_by_phone = AsyncMock(return_value=None)
        self._run_action("send", pool=pool, extra_args={"recipient": "@user", "text": "hello"})
        out = capsys.readouterr().out
        assert "unavailable" in out.lower()

    def test_forward_action(self, capsys):
        pool = _make_pool_with_clients()
        mock_client = AsyncMock()
        pool.get_native_client_by_phone = AsyncMock(return_value=(mock_client, "+1111"))
        self._run_action(
            "forward", pool=pool,
            extra_args={"from_chat": "@a", "to_chat": "@b", "message_ids": ["1,2"]},
        )
        out = capsys.readouterr().out
        assert "forwarded" in out.lower()

    def test_forward_no_valid_ids(self, capsys):
        pool = _make_pool_with_clients()
        self._run_action(
            "forward", pool=pool,
            extra_args={"from_chat": "@a", "to_chat": "@b", "message_ids": ["abc"]},
        )
        out = capsys.readouterr().out
        assert "No valid" in out

    def test_edit_message_action(self, capsys):
        pool = _make_pool_with_clients()
        mock_client = AsyncMock()
        pool.get_native_client_by_phone = AsyncMock(return_value=(mock_client, "+1111"))
        self._run_action(
            "edit-message", pool=pool,
            extra_args={"chat_id": "@ch", "message_id": 42, "text": "new text"},
        )
        out = capsys.readouterr().out
        assert "edited" in out.lower()

    def test_delete_message_action(self, capsys):
        pool = _make_pool_with_clients()
        mock_client = AsyncMock()
        pool.get_native_client_by_phone = AsyncMock(return_value=(mock_client, "+1111"))
        self._run_action(
            "delete-message", pool=pool,
            extra_args={"chat_id": "@ch", "message_ids": ["1,2,3"]},
        )
        out = capsys.readouterr().out
        assert "deleted" in out.lower()

    def test_pin_message_action(self, capsys):
        pool = _make_pool_with_clients()
        mock_client = AsyncMock()
        pool.get_native_client_by_phone = AsyncMock(return_value=(mock_client, "+1111"))
        self._run_action(
            "pin-message", pool=pool,
            extra_args={"chat_id": "@ch", "message_id": 42, "notify": False},
        )
        out = capsys.readouterr().out
        assert "pinned" in out.lower()

    def test_unpin_message_action(self, capsys):
        pool = _make_pool_with_clients()
        mock_client = AsyncMock()
        pool.get_native_client_by_phone = AsyncMock(return_value=(mock_client, "+1111"))
        self._run_action(
            "unpin-message", pool=pool,
            extra_args={"chat_id": "@ch", "message_id": None},
        )
        out = capsys.readouterr().out
        assert "unpinned" in out.lower()

    def test_download_media_action(self, capsys):
        pool = _make_pool_with_clients()
        mock_client = AsyncMock()
        mock_client.download_media = AsyncMock(return_value="/tmp/file.jpg")

        async def fake_iter(*a, **kw):
            yield SimpleNamespace(id=1)

        mock_client.iter_messages = fake_iter
        pool.get_native_client_by_phone = AsyncMock(return_value=(mock_client, "+1111"))
        self._run_action(
            "download-media", pool=pool,
            extra_args={"chat_id": "@ch", "message_id": 1, "output_dir": "/tmp"},
        )
        out = capsys.readouterr().out
        assert "downloaded" in out.lower() or "file" in out.lower()

    def test_participants_action(self, capsys):
        pool = _make_pool_with_clients()
        mock_client = AsyncMock()
        p1 = SimpleNamespace(id=1, first_name="A", last_name="B", username="ab")
        mock_client.get_participants = AsyncMock(return_value=[p1])
        pool.get_native_client_by_phone = AsyncMock(return_value=(mock_client, "+1111"))
        self._run_action(
            "participants", pool=pool,
            extra_args={"chat_id": "@ch", "limit": 10, "search": ""},
        )
        out = capsys.readouterr().out
        assert "Total" in out

    def test_participants_empty(self, capsys):
        pool = _make_pool_with_clients()
        mock_client = AsyncMock()
        mock_client.get_participants = AsyncMock(return_value=[])
        pool.get_native_client_by_phone = AsyncMock(return_value=(mock_client, "+1111"))
        self._run_action(
            "participants", pool=pool,
            extra_args={"chat_id": "@ch", "limit": 10, "search": ""},
        )
        out = capsys.readouterr().out
        assert "No participants" in out

    def test_edit_admin_action(self, capsys):
        pool = _make_pool_with_clients()
        mock_client = AsyncMock()
        pool.get_native_client_by_phone = AsyncMock(return_value=(mock_client, "+1111"))
        self._run_action(
            "edit-admin", pool=pool,
            extra_args={"chat_id": "@ch", "user_id": "@u", "is_admin": True, "title": "mod"},
        )
        out = capsys.readouterr().out
        assert "updated" in out.lower()

    def test_edit_permissions_action(self, capsys):
        pool = _make_pool_with_clients()
        mock_client = AsyncMock()
        pool.get_native_client_by_phone = AsyncMock(return_value=(mock_client, "+1111"))
        self._run_action(
            "edit-permissions", pool=pool,
            extra_args={
                "chat_id": "@ch", "user_id": "@u",
                "send_messages": "true", "send_media": None, "until_date": None,
            },
        )
        out = capsys.readouterr().out
        assert "updated" in out.lower()

    def test_edit_permissions_no_flags(self, capsys):
        pool = _make_pool_with_clients()
        mock_client = AsyncMock()
        pool.get_native_client_by_phone = AsyncMock(return_value=(mock_client, "+1111"))
        self._run_action(
            "edit-permissions", pool=pool,
            extra_args={
                "chat_id": "@ch", "user_id": "@u",
                "send_messages": None, "send_media": None, "until_date": None,
            },
        )
        out = capsys.readouterr().out
        assert "specify" in out.lower() or "error" in out.lower()

    def test_kick_action(self, capsys):
        pool = _make_pool_with_clients()
        mock_client = AsyncMock()
        pool.get_native_client_by_phone = AsyncMock(return_value=(mock_client, "+1111"))
        self._run_action("kick", pool=pool, extra_args={"chat_id": "@ch", "user_id": "@u"})
        out = capsys.readouterr().out
        assert "kicked" in out.lower()

    def test_broadcast_stats_action(self, capsys):
        pool = _make_pool_with_clients()
        mock_client = AsyncMock()
        mock_stats = SimpleNamespace(
            followers=SimpleNamespace(current=100, previous=90),
            views_per_post=None, shares_per_post=None,
            reactions_per_post=None, forwards_per_post=None,
            period=SimpleNamespace(min_date="2025-01-01", max_date="2025-01-31"),
            enabled_notifications=SimpleNamespace(current=80),
        )
        mock_client.get_broadcast_stats = AsyncMock(return_value=mock_stats)
        pool.get_native_client_by_phone = AsyncMock(return_value=(mock_client, "+1111"))
        self._run_action("broadcast-stats", pool=pool, extra_args={"chat_id": "@ch"})
        out = capsys.readouterr().out
        assert "followers" in out.lower() or "100" in out

    def test_archive_action(self, capsys):
        pool = _make_pool_with_clients()
        mock_client = AsyncMock()
        pool.get_native_client_by_phone = AsyncMock(return_value=(mock_client, "+1111"))
        self._run_action("archive", pool=pool, extra_args={"chat_id": "@ch"})
        out = capsys.readouterr().out
        assert "archived" in out.lower()

    def test_unarchive_action(self, capsys):
        pool = _make_pool_with_clients()
        mock_client = AsyncMock()
        pool.get_native_client_by_phone = AsyncMock(return_value=(mock_client, "+1111"))
        self._run_action("unarchive", pool=pool, extra_args={"chat_id": "@ch"})
        out = capsys.readouterr().out
        assert "unarchived" in out.lower()

    def test_mark_read_action(self, capsys):
        pool = _make_pool_with_clients()
        mock_client = AsyncMock()
        pool.get_native_client_by_phone = AsyncMock(return_value=(mock_client, "+1111"))
        self._run_action("mark-read", pool=pool, extra_args={"chat_id": "@ch", "max_id": None})
        out = capsys.readouterr().out
        assert "marked" in out.lower() or "read" in out.lower()

    def test_cache_clear_with_phone(self, capsys):
        pool = _make_pool_with_clients()
        self._run_action("cache-clear", pool=pool, extra_args={"phone": "+1111"})
        out = capsys.readouterr().out
        assert "cleared" in out.lower()

    def test_cache_clear_all(self, capsys):
        pool = _make_pool_with_clients()
        self._run_action("cache-clear", pool=pool, extra_args={"phone": None})
        out = capsys.readouterr().out
        assert "cleared" in out.lower()

    def test_cache_status_empty(self, capsys):
        pool = _make_pool_with_clients()
        pool._dialogs_cache = {}
        db = _make_mock_db()
        db.close = AsyncMock()
        db.repos.dialog_cache = MagicMock()
        db.repos.dialog_cache.get_all_phones = AsyncMock(return_value=[])
        self._run_action("cache-status", pool=pool, db=db)
        out = capsys.readouterr().out
        assert "no cached" in out.lower() or "cached" in out.lower()


# ===========================================================================
# 3. agent/tools/messaging.py — tool handlers (via make_mcp_server)
# ===========================================================================




class TestMyTelegramToolErrors:
    @pytest.fixture
    def mytg_setup(self):
        mock_db = MagicMock(spec=Database)
        mock_db.repos = MagicMock()
        from src.models import Account
        mock_db.get_accounts = AsyncMock(return_value=[
            Account(id=1, phone="+1111", session_string="s", is_active=True, is_primary=True),
        ])
        mock_db.get_setting = AsyncMock(return_value=None)
        mock_db.repos.settings = MagicMock()
        mock_db.repos.settings.get = AsyncMock(return_value=None)
        mock_db.repos.tool_permissions = MagicMock()
        mock_db.repos.tool_permissions.get_by_phone = AsyncMock(return_value=None)
        pool = MagicMock()
        pool.get_native_client_by_phone = AsyncMock(return_value=None)
        pool.invalidate_dialogs_cache = MagicMock()

        captured_tools = []
        with patch(
            "src.agent.tools.create_sdk_mcp_server",
            side_effect=lambda **kw: captured_tools.extend(kw.get("tools", [])),
        ):
            from src.agent.tools import make_mcp_server
            make_mcp_server(mock_db, client_pool=pool)
        return {t.name: t.handler for t in captured_tools if hasattr(t, "handler")}, mock_db, pool

    async def test_list_dialogs_exception(self, mytg_setup):
        handlers, _, _ = mytg_setup
        with patch("src.services.channel_service.ChannelService.get_my_dialogs",
                    new_callable=AsyncMock, side_effect=RuntimeError("fail")):
            result = await handlers["search_dialogs"]({"phone": "+1111"})
            assert "ошибка" in _text(result).lower()

    async def test_refresh_dialogs_exception(self, mytg_setup):
        handlers, _, _ = mytg_setup
        with patch("src.services.channel_service.ChannelService.get_my_dialogs",
                    new_callable=AsyncMock, side_effect=RuntimeError("fail")):
            result = await handlers["refresh_dialogs"]({"phone": "+1111"})
            assert "ошибка" in _text(result).lower()

    async def test_create_channel_no_title(self, mytg_setup):
        handlers, _, _ = mytg_setup
        result = await handlers["create_telegram_channel"]({
            "phone": "+1111", "title": "", "confirm": True,
        })
        assert "обязател" in _text(result).lower()

    async def test_create_channel_client_none(self, mytg_setup):
        handlers, _, _ = mytg_setup
        result = await handlers["create_telegram_channel"]({
            "phone": "+1111", "title": "Test", "confirm": True,
        })
        assert "не найден" in _text(result).lower()

    async def test_create_channel_exception(self, mytg_setup):
        handlers, _, pool = mytg_setup
        mock_client = AsyncMock(side_effect=RuntimeError("net"))
        pool.get_native_client_by_phone = AsyncMock(return_value=(mock_client, "+1111"))
        result = await handlers["create_telegram_channel"]({
            "phone": "+1111", "title": "Test", "confirm": True,
        })
        assert "ошибка" in _text(result).lower()

    async def test_leave_dialogs_exception(self, mytg_setup):
        handlers, _, pool = mytg_setup
        pool.leave_channels = AsyncMock(side_effect=RuntimeError("fail"))
        result = await handlers["leave_dialogs"]({
            "phone": "+1111", "dialog_ids": "1,2", "confirm": True,
        })
        assert "ошибка" in _text(result).lower()

    async def test_get_forum_topics_exception(self, mytg_setup):
        handlers, mock_db, _ = mytg_setup
        mock_db.get_forum_topics = AsyncMock(side_effect=RuntimeError("fail"))
        result = await handlers["get_forum_topics"]({"channel_id": 123})
        assert "ошибка" in _text(result).lower()

    async def test_clear_dialog_cache_exception(self, mytg_setup):
        handlers, mock_db, _ = mytg_setup
        mock_db.repos.dialog_cache = MagicMock()
        mock_db.repos.dialog_cache.clear_dialogs = AsyncMock(side_effect=RuntimeError("fail"))
        result = await handlers["clear_dialog_cache"]({
            "phone": "+1111", "confirm": True,
        })
        assert "ошибка" in _text(result).lower()


# ===========================================================================
# 19. agent/tools/photo_loader.py — error paths
# ===========================================================================




class TestMyTelegramNoAccounts:
    """Cover the 'no connected accounts' and 'client unavailable' branches."""

    def _run_action(self, action, pool=None, db=None, extra_args=None):
        from src.cli.commands import dialogs

        pool = pool or _make_pool_with_clients()
        db = db or _make_mock_db()
        db.get_forum_topics = AsyncMock(return_value=[])
        db.close = AsyncMock()
        db.repos.dialog_cache = MagicMock()
        db.repos.dialog_cache.clear_dialogs = AsyncMock()
        db.repos.dialog_cache.clear_all_dialogs = AsyncMock()
        db.repos.dialog_cache.get_all_phones = AsyncMock(return_value=[])
        db.repos.dialog_cache.count_dialogs = AsyncMock(return_value=0)
        db.repos.dialog_cache.get_cached_at = AsyncMock(return_value=None)

        args_dict = {
            "config": "config.yaml",
            "dialogs_action": action,
            "phone": None,
            "yes": True,
        }
        if extra_args:
            args_dict.update(extra_args)
        args = argparse.Namespace(**args_dict)

        config = AppConfig()

        async def fake_init_db(cfg):
            return config, db

        async def fake_init_pool(cfg, d):
            return cfg, pool

        with (
            patch("src.cli.commands.dialogs.runtime.init_db", side_effect=fake_init_db),
            patch("src.cli.commands.dialogs.runtime.init_pool", side_effect=fake_init_pool),
        ):
            dialogs.run(args)

    def test_delete_no_accounts(self, capsys):
        pool = MagicMock()
        pool.clients = {}
        pool.disconnect_all = AsyncMock()
        db = _make_mock_db()
        db.close = AsyncMock()
        self._run_action("delete-message", pool=pool, db=db,
                         extra_args={"chat_id": "@ch", "message_ids": ["1"]})
        assert "no connected" in capsys.readouterr().out.lower()

    def test_pin_no_accounts(self, capsys):
        pool = MagicMock()
        pool.clients = {}
        pool.disconnect_all = AsyncMock()
        db = _make_mock_db()
        db.close = AsyncMock()
        self._run_action("pin-message", pool=pool, db=db,
                         extra_args={"chat_id": "@ch", "message_id": 1})
        assert "no connected" in capsys.readouterr().out.lower()

    def test_unpin_no_accounts(self, capsys):
        pool = MagicMock()
        pool.clients = {}
        pool.disconnect_all = AsyncMock()
        db = _make_mock_db()
        db.close = AsyncMock()
        self._run_action("unpin-message", pool=pool, db=db,
                         extra_args={"chat_id": "@ch", "message_id": 1})
        assert "no connected" in capsys.readouterr().out.lower()

    def test_download_no_accounts(self, capsys):
        pool = MagicMock()
        pool.clients = {}
        pool.disconnect_all = AsyncMock()
        db = _make_mock_db()
        db.close = AsyncMock()
        self._run_action("download-media", pool=pool, db=db,
                         extra_args={"chat_id": "@ch", "message_id": 1})
        assert "no connected" in capsys.readouterr().out.lower()

    def test_participants_no_accounts(self, capsys):
        pool = MagicMock()
        pool.clients = {}
        pool.disconnect_all = AsyncMock()
        db = _make_mock_db()
        db.close = AsyncMock()
        self._run_action("participants", pool=pool, db=db,
                         extra_args={"chat_id": "@ch", "limit": 10, "search": ""})
        assert "no connected" in capsys.readouterr().out.lower()

    def test_edit_admin_no_accounts(self, capsys):
        pool = MagicMock()
        pool.clients = {}
        pool.disconnect_all = AsyncMock()
        db = _make_mock_db()
        db.close = AsyncMock()
        self._run_action("edit-admin", pool=pool, db=db,
                         extra_args={"chat_id": "@ch", "user_id": "@u", "is_admin": True, "title": None})
        assert "no connected" in capsys.readouterr().out.lower()

    def test_edit_permissions_no_accounts(self, capsys):
        pool = MagicMock()
        pool.clients = {}
        pool.disconnect_all = AsyncMock()
        db = _make_mock_db()
        db.close = AsyncMock()
        self._run_action("edit-permissions", pool=pool, db=db,
                         extra_args={
                             "chat_id": "@ch", "user_id": "@u",
                             "send_messages": False, "send_media": None, "until_date": None,
                         })
        assert "no connected" in capsys.readouterr().out.lower()

    def test_kick_no_accounts(self, capsys):
        pool = MagicMock()
        pool.clients = {}
        pool.disconnect_all = AsyncMock()
        db = _make_mock_db()
        db.close = AsyncMock()
        self._run_action("kick", pool=pool, db=db,
                         extra_args={"chat_id": "@ch", "user_id": "@u"})
        assert "no connected" in capsys.readouterr().out.lower()

    def test_broadcast_stats_no_accounts(self, capsys):
        pool = MagicMock()
        pool.clients = {}
        pool.disconnect_all = AsyncMock()
        db = _make_mock_db()
        db.close = AsyncMock()
        self._run_action("broadcast-stats", pool=pool, db=db,
                         extra_args={"chat_id": "@ch"})
        assert "no connected" in capsys.readouterr().out.lower()

    def test_archive_no_accounts(self, capsys):
        pool = MagicMock()
        pool.clients = {}
        pool.disconnect_all = AsyncMock()
        db = _make_mock_db()
        db.close = AsyncMock()
        self._run_action("archive", pool=pool, db=db,
                         extra_args={"chat_id": "@ch"})
        assert "no connected" in capsys.readouterr().out.lower()

    def test_unarchive_no_accounts(self, capsys):
        pool = MagicMock()
        pool.clients = {}
        pool.disconnect_all = AsyncMock()
        db = _make_mock_db()
        db.close = AsyncMock()
        self._run_action("unarchive", pool=pool, db=db,
                         extra_args={"chat_id": "@ch"})
        assert "no connected" in capsys.readouterr().out.lower()

    def test_mark_read_no_accounts(self, capsys):
        pool = MagicMock()
        pool.clients = {}
        pool.disconnect_all = AsyncMock()
        db = _make_mock_db()
        db.close = AsyncMock()
        self._run_action("mark-read", pool=pool, db=db,
                         extra_args={"chat_id": "@ch", "max_id": None})
        assert "no connected" in capsys.readouterr().out.lower()


# ===========================================================================
# 26. cli/commands/dialogs.py — additional action branches
# ===========================================================================




class TestMyTelegramMoreBranches:
    def _run_action(self, action, pool=None, db=None, extra_args=None):
        from src.cli.commands import dialogs

        pool = pool or _make_pool_with_clients()
        db = db or _make_mock_db()
        db.get_forum_topics = AsyncMock(return_value=[])
        db.close = AsyncMock()
        db.repos.dialog_cache = MagicMock()
        db.repos.dialog_cache.clear_dialogs = AsyncMock()
        db.repos.dialog_cache.clear_all_dialogs = AsyncMock()
        db.repos.dialog_cache.get_all_phones = AsyncMock(return_value=[])
        db.repos.dialog_cache.count_dialogs = AsyncMock(return_value=0)
        db.repos.dialog_cache.get_cached_at = AsyncMock(return_value=None)

        args_dict = {
            "config": "config.yaml",
            "dialogs_action": action,
            "phone": "+1111",
            "yes": True,
        }
        if extra_args:
            args_dict.update(extra_args)
        args = argparse.Namespace(**args_dict)

        config = AppConfig()

        async def fake_init_db(cfg):
            return config, db

        async def fake_init_pool(cfg, d):
            return cfg, pool

        with (
            patch("src.cli.commands.dialogs.runtime.init_db", side_effect=fake_init_db),
            patch("src.cli.commands.dialogs.runtime.init_pool", side_effect=fake_init_pool),
        ):
            dialogs.run(args)

    def test_forward_phone_not_connected(self, capsys):
        pool = _make_pool_with_clients(["+2222"])  # only +2222 connected
        db = _make_mock_db()
        db.close = AsyncMock()
        self._run_action(
            "forward", pool=pool, db=db,
            extra_args={
                "phone": "+1111",  # not in pool.clients
                "from_chat": "@a", "to_chat": "@b",
                "message_ids": ["1"], "yes": True,
            },
        )
        out = capsys.readouterr().out
        assert "not connected" in out.lower()

    def test_edit_message_phone_not_connected(self, capsys):
        pool = _make_pool_with_clients(["+2222"])
        db = _make_mock_db()
        db.close = AsyncMock()
        self._run_action(
            "edit-message", pool=pool, db=db,
            extra_args={
                "phone": "+1111",
                "chat_id": "@ch", "message_id": 1, "text": "x", "yes": True,
            },
        )
        out = capsys.readouterr().out
        assert "not connected" in out.lower()

    def test_delete_message_phone_not_connected(self, capsys):
        pool = _make_pool_with_clients(["+2222"])
        db = _make_mock_db()
        db.close = AsyncMock()
        self._run_action(
            "delete-message", pool=pool, db=db,
            extra_args={
                "phone": "+1111",
                "chat_id": "@ch", "message_ids": ["1"], "yes": True,
            },
        )
        out = capsys.readouterr().out
        assert "not connected" in out.lower()

    def test_pin_message_phone_not_connected(self, capsys):
        pool = _make_pool_with_clients(["+2222"])
        db = _make_mock_db()
        db.close = AsyncMock()
        self._run_action(
            "pin-message", pool=pool, db=db,
            extra_args={
                "phone": "+1111",
                "chat_id": "@ch", "message_id": 1, "yes": True,
            },
        )
        out = capsys.readouterr().out
        assert "not connected" in out.lower()

    def test_unpin_message_phone_not_connected(self, capsys):
        pool = _make_pool_with_clients(["+2222"])
        db = _make_mock_db()
        db.close = AsyncMock()
        self._run_action(
            "unpin-message", pool=pool, db=db,
            extra_args={
                "phone": "+1111",
                "chat_id": "@ch", "message_id": 1, "yes": True,
            },
        )
        out = capsys.readouterr().out
        assert "not connected" in out.lower()

    def test_download_media_phone_not_connected(self, capsys):
        pool = _make_pool_with_clients(["+2222"])
        db = _make_mock_db()
        db.close = AsyncMock()
        self._run_action(
            "download-media", pool=pool, db=db,
            extra_args={
                "phone": "+1111",
                "chat_id": "@ch", "message_id": 1, "yes": True,
            },
        )
        out = capsys.readouterr().out
        assert "not connected" in out.lower()

    def test_participants_phone_not_connected(self, capsys):
        pool = _make_pool_with_clients(["+2222"])
        db = _make_mock_db()
        db.close = AsyncMock()
        self._run_action(
            "participants", pool=pool, db=db,
            extra_args={
                "phone": "+1111",
                "chat_id": "@ch", "limit": 10, "search": "", "yes": True,
            },
        )
        out = capsys.readouterr().out
        assert "not connected" in out.lower()

    def test_edit_admin_phone_not_connected(self, capsys):
        pool = _make_pool_with_clients(["+2222"])
        db = _make_mock_db()
        db.close = AsyncMock()
        self._run_action(
            "edit-admin", pool=pool, db=db,
            extra_args={
                "phone": "+1111",
                "chat_id": "@ch", "user_id": "@u",
                "is_admin": True, "title": None, "yes": True,
            },
        )
        out = capsys.readouterr().out
        assert "not connected" in out.lower()

    def test_edit_permissions_phone_not_connected(self, capsys):
        pool = _make_pool_with_clients(["+2222"])
        db = _make_mock_db()
        db.close = AsyncMock()
        self._run_action(
            "edit-permissions", pool=pool, db=db,
            extra_args={
                "phone": "+1111",
                "chat_id": "@ch", "user_id": "@u",
                "send_messages": False, "send_media": None,
                "until_date": None, "yes": True,
            },
        )
        out = capsys.readouterr().out
        assert "not connected" in out.lower()

    def test_kick_phone_not_connected(self, capsys):
        pool = _make_pool_with_clients(["+2222"])
        db = _make_mock_db()
        db.close = AsyncMock()
        self._run_action(
            "kick", pool=pool, db=db,
            extra_args={
                "phone": "+1111",
                "chat_id": "@ch", "user_id": "@u", "yes": True,
            },
        )
        out = capsys.readouterr().out
        assert "not connected" in out.lower()

    def test_broadcast_stats_phone_not_connected(self, capsys):
        pool = _make_pool_with_clients(["+2222"])
        db = _make_mock_db()
        db.close = AsyncMock()
        self._run_action(
            "broadcast-stats", pool=pool, db=db,
            extra_args={
                "phone": "+1111",
                "chat_id": "@ch", "yes": True,
            },
        )
        out = capsys.readouterr().out
        assert "not connected" in out.lower()

    def test_archive_phone_not_connected(self, capsys):
        pool = _make_pool_with_clients(["+2222"])
        db = _make_mock_db()
        db.close = AsyncMock()
        self._run_action(
            "archive", pool=pool, db=db,
            extra_args={
                "phone": "+1111",
                "chat_id": "@ch", "yes": True,
            },
        )
        out = capsys.readouterr().out
        assert "not connected" in out.lower()

    def test_unarchive_phone_not_connected(self, capsys):
        pool = _make_pool_with_clients(["+2222"])
        db = _make_mock_db()
        db.close = AsyncMock()
        self._run_action(
            "unarchive", pool=pool, db=db,
            extra_args={
                "phone": "+1111",
                "chat_id": "@ch", "yes": True,
            },
        )
        out = capsys.readouterr().out
        assert "not connected" in out.lower()

    def test_mark_read_phone_not_connected(self, capsys):
        pool = _make_pool_with_clients(["+2222"])
        db = _make_mock_db()
        db.close = AsyncMock()
        self._run_action(
            "mark-read", pool=pool, db=db,
            extra_args={
                "phone": "+1111",
                "chat_id": "@ch", "max_id": None, "yes": True,
            },
        )
        out = capsys.readouterr().out
        assert "not connected" in out.lower()


# ===========================================================================
# 28. Messaging tools — final edge cases for 90%
# ===========================================================================




class TestMyTelegramToolPhoneGates:
    @pytest.fixture
    def mytg_phone_err(self):
        mock_db = MagicMock(spec=Database)
        mock_db.repos = MagicMock()
        mock_db.get_accounts = AsyncMock(return_value=[])  # no accounts
        mock_db.repos.settings = MagicMock()
        mock_db.repos.settings.get = AsyncMock(return_value=None)
        mock_db.repos.tool_permissions = MagicMock()
        mock_db.repos.tool_permissions.get_by_phone = AsyncMock(return_value=None)
        pool = MagicMock()
        pool.get_native_client_by_phone = AsyncMock(return_value=None)
        pool.invalidate_dialogs_cache = MagicMock()

        captured_tools = []
        with patch(
            "src.agent.tools.create_sdk_mcp_server",
            side_effect=lambda **kw: captured_tools.extend(kw.get("tools", [])),
        ):
            from src.agent.tools import make_mcp_server
            make_mcp_server(mock_db, client_pool=pool)
        return {t.name: t.handler for t in captured_tools if hasattr(t, "handler")}, mock_db

    async def test_list_dialogs_phone_err(self, mytg_phone_err):
        handlers, _ = mytg_phone_err
        r = await handlers["search_dialogs"]({"phone": ""})
        assert "аккаунт" in _text(r).lower()

    async def test_refresh_dialogs_phone_err(self, mytg_phone_err):
        handlers, _ = mytg_phone_err
        r = await handlers["refresh_dialogs"]({"phone": ""})
        assert "аккаунт" in _text(r).lower()

    async def test_leave_dialogs_phone_err(self, mytg_phone_err):
        handlers, _ = mytg_phone_err
        r = await handlers["leave_dialogs"]({"phone": "", "dialog_ids": "1", "confirm": True})
        assert "аккаунт" in _text(r).lower()

    async def test_create_channel_phone_err(self, mytg_phone_err):
        handlers, _ = mytg_phone_err
        r = await handlers["create_telegram_channel"]({
            "phone": "", "title": "Test", "confirm": True,
        })
        assert "аккаунт" in _text(r).lower()


# ---------------------------------------------------------------------------
# === COVERAGE PUSH BATCH 2 ===
# Target: push 6 modules to 90%+ coverage
# ---------------------------------------------------------------------------


# ---- collection_queue.py coverage (lines 86, 97-98, 136, 142-146, 175-181) ----


