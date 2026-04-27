"""Tests for dialogs CLI and web action paths."""

from __future__ import annotations

import asyncio
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.config import AppConfig
from tests.helpers import build_web_app, cli_ns, make_auth_client

# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

_PHONE = "+79001234567"
_SVC_DIALOGS = "src.services.channel_service.ChannelService.get_my_dialogs"
_SVC_LEAVE = "src.services.channel_service.ChannelService.leave_dialogs"

_FAKE_DIALOGS = [
    {
        "channel_id": -100111,
        "title": "My Channel",
        "username": "mychan",
        "channel_type": "channel",
        "deactivate": False,
        "is_own": False,
    },
    {
        "channel_id": -100222,
        "title": "My Group",
        "username": None,
        "channel_type": "supergroup",
        "deactivate": False,
        "is_own": False,
    },
]


def _mock_pool(*, clients=None, native_result=None, get_forum_topics=None):
    """Build a mock ClientPool with common defaults."""
    mock_client = AsyncMock()
    pool = MagicMock()
    pool.clients = clients if clients is not None else {_PHONE: mock_client}
    pool.get_native_client_by_phone = AsyncMock(
        return_value=(mock_client, _PHONE) if native_result is None else native_result,
    )
    pool.invalidate_dialogs_cache = MagicMock()
    pool.get_forum_topics = AsyncMock(
        return_value=get_forum_topics if get_forum_topics is not None else [],
    )
    pool.disconnect_all = AsyncMock()
    pool._dialogs_cache = {}
    pool._dialogs_cache_ttl_sec = 60.0
    return pool, mock_client


def _run_cli(action, pool, db, extra_ns=None):
    """Run a CLI action with mocked runtime."""
    ns_kwargs = {"dialogs_action": action}
    if extra_ns:
        ns_kwargs.update(extra_ns)
    ns = cli_ns(**ns_kwargs)

    async def fake_init_db(_):
        return AppConfig(), db

    async def fake_init_pool(config, database):
        from src.telegram.auth import TelegramAuth
        return TelegramAuth(0, ""), pool

    with (
        patch("src.cli.runtime.init_db", side_effect=fake_init_db),
        patch("src.cli.runtime.init_pool", side_effect=fake_init_pool),
    ):
        from src.cli.commands.dialogs import run
        run(ns)


# =========================================================================
# CLI Tests
# =========================================================================


class TestCliList:
    """CLI dialogs list."""

    def test_list_no_accounts(self, cli_db, capsys):
        pool, _ = _mock_pool(clients={})
        _run_cli("list", pool, cli_db)
        assert "No connected accounts." in capsys.readouterr().out

    def test_list_phone_not_connected(self, cli_db, capsys):
        pool, _ = _mock_pool()
        _run_cli("list", pool, cli_db, {"phone": "+000"})
        assert "not connected" in capsys.readouterr().out

    def test_list_no_dialogs(self, cli_db, capsys):
        pool, _ = _mock_pool()
        with patch(_SVC_DIALOGS, new_callable=AsyncMock, return_value=[]):
            _run_cli("list", pool, cli_db, {"phone": _PHONE})
        assert "No dialogs found." in capsys.readouterr().out

    def test_list_with_dialogs(self, cli_db, capsys):
        pool, _ = _mock_pool()
        with patch(_SVC_DIALOGS, new_callable=AsyncMock, return_value=_FAKE_DIALOGS):
            _run_cli("list", pool, cli_db, {"phone": _PHONE})
        out = capsys.readouterr().out
        assert "My Channel" in out
        assert "@mychan" in out
        assert "My Group" in out

    def test_list_default_phone(self, cli_db, capsys):
        pool, _ = _mock_pool()
        with patch(_SVC_DIALOGS, new_callable=AsyncMock, return_value=_FAKE_DIALOGS):
            _run_cli("list", pool, cli_db, {"phone": None})
        assert "My Channel" in capsys.readouterr().out

    def test_list_dialog_without_username(self, cli_db, capsys):
        dialogs = [
            {
                "channel_id": -100222,
                "title": "No Username Group",
                "channel_type": "supergroup",
                "deactivate": False,
                "is_own": False,
                "already_added": False,
            }
        ]
        pool, _ = _mock_pool()
        with patch(_SVC_DIALOGS, new_callable=AsyncMock, return_value=dialogs):
            _run_cli("list", pool, cli_db, {"phone": _PHONE})
        out = capsys.readouterr().out
        assert "No Username Group" in out


class TestCliRefresh:
    """CLI dialogs refresh."""

    def test_refresh_no_accounts(self, cli_db, capsys):
        pool, _ = _mock_pool(clients={})
        _run_cli("refresh", pool, cli_db, {"phone": None})
        assert "No connected accounts." in capsys.readouterr().out

    def test_refresh_phone_not_connected(self, cli_db, capsys):
        pool, _ = _mock_pool()
        _run_cli("refresh", pool, cli_db, {"phone": "+000"})
        assert "not connected" in capsys.readouterr().out

    def test_refresh_ok(self, cli_db, capsys):
        pool, _ = _mock_pool()
        with patch(_SVC_DIALOGS, new_callable=AsyncMock, return_value=_FAKE_DIALOGS):
            _run_cli("refresh", pool, cli_db, {"phone": _PHONE})
        assert "Dialogs refreshed: 2 total." in capsys.readouterr().out


class TestCliCacheClear:
    """CLI dialogs cache-clear."""

    def test_cache_clear_single_phone(self, cli_db, capsys):
        pool, _ = _mock_pool()
        pool.invalidate_dialogs_cache = MagicMock()
        asyncio.run(cli_db.repos.dialog_cache.replace_dialogs(_PHONE, _FAKE_DIALOGS))
        _run_cli("cache-clear", pool, cli_db, {"phone": _PHONE})
        out = capsys.readouterr().out
        assert f"Cache cleared for {_PHONE}." in out
        pool.invalidate_dialogs_cache.assert_called_once_with(_PHONE)

    def test_cache_clear_all(self, cli_db, capsys):
        pool, _ = _mock_pool()
        pool.invalidate_dialogs_cache = MagicMock()
        _run_cli("cache-clear", pool, cli_db, {"phone": None})
        out = capsys.readouterr().out
        assert "Cache cleared for all accounts." in out
        pool.invalidate_dialogs_cache.assert_called_once_with()


class TestCliCacheStatus:
    """CLI dialogs cache-status."""

    def test_cache_status_no_cache(self, cli_db, capsys):
        pool, _ = _mock_pool()
        _run_cli("cache-status", pool, cli_db)
        assert "No cached dialogs." in capsys.readouterr().out

    def test_cache_status_with_db_entries(self, cli_db, capsys):
        pool, _ = _mock_pool()
        asyncio.run(cli_db.repos.dialog_cache.replace_dialogs(_PHONE, _FAKE_DIALOGS))
        _run_cli("cache-status", pool, cli_db)
        out = capsys.readouterr().out
        assert _PHONE in out
        assert "DB entries" in out


class TestCliSend:
    """CLI dialogs send."""

    def test_send_no_accounts(self, cli_db, capsys):
        pool, _ = _mock_pool(clients={})
        _run_cli("send", pool, cli_db, {"phone": None, "recipient": "@user", "text": "hi", "yes": True})
        assert "No connected accounts." in capsys.readouterr().out

    def test_send_not_connected(self, cli_db, capsys):
        pool, _ = _mock_pool()
        _run_cli("send", pool, cli_db, {"phone": "+000", "recipient": "@user", "text": "hi", "yes": True})
        assert "not connected" in capsys.readouterr().out

    def test_send_confirmed(self, cli_db, capsys):
        pool, client = _mock_pool()
        client.get_entity = AsyncMock(return_value=SimpleNamespace(id=123))
        client.send_message = AsyncMock()
        _run_cli("send", pool, cli_db, {"phone": _PHONE, "recipient": "@user", "text": "hello", "yes": True})
        assert "Message sent" in capsys.readouterr().out

    def test_send_abort(self, cli_db, capsys):
        pool, client = _mock_pool()
        with patch("builtins.input", return_value="n"):
            _run_cli("send", pool, cli_db, {"phone": _PHONE, "recipient": "@user", "text": "hello", "yes": False})
        assert "Aborted." in capsys.readouterr().out

    def test_send_confirm_yes(self, cli_db, capsys):
        pool, client = _mock_pool()
        client.get_entity = AsyncMock(return_value=SimpleNamespace(id=123))
        client.send_message = AsyncMock()
        with patch("builtins.input", return_value="y"):
            _run_cli("send", pool, cli_db, {"phone": _PHONE, "recipient": "@user", "text": "hello", "yes": False})
        assert "Message sent" in capsys.readouterr().out

    def test_send_client_unavailable(self, cli_db, capsys):
        pool, _ = _mock_pool(native_result=None)
        pool.get_native_client_by_phone = AsyncMock(return_value=None)
        _run_cli("send", pool, cli_db, {"phone": _PHONE, "recipient": "@user", "text": "hi", "yes": True})
        assert "unavailable" in capsys.readouterr().out

    def test_send_error(self, cli_db, capsys):
        pool, client = _mock_pool()
        client.get_entity = AsyncMock(side_effect=RuntimeError("entity error"))
        _run_cli("send", pool, cli_db, {"phone": _PHONE, "recipient": "@user", "text": "hi", "yes": True})
        assert "Error sending message" in capsys.readouterr().out

    def test_send_long_text_preview(self, cli_db, capsys):
        pool, client = _mock_pool()
        client.get_entity = AsyncMock(return_value=SimpleNamespace(id=1))
        client.send_message = AsyncMock()
        long_text = "x" * 300
        with patch("builtins.input", return_value="y"):
            _run_cli("send", pool, cli_db, {"phone": _PHONE, "recipient": "@u", "text": long_text, "yes": False})
        out = capsys.readouterr().out
        assert "..." in out


class TestCliForward:
    """CLI dialogs forward."""

    def test_forward_no_accounts(self, cli_db, capsys):
        pool, _ = _mock_pool(clients={})
        _run_cli("forward", pool, cli_db, {
            "phone": None, "from_chat": "A", "to_chat": "B", "message_ids": ["1,2"], "yes": True,
        })
        assert "No connected accounts." in capsys.readouterr().out

    def test_forward_no_valid_ids(self, cli_db, capsys):
        pool, _ = _mock_pool()
        _run_cli("forward", pool, cli_db, {
            "phone": _PHONE, "from_chat": "A", "to_chat": "B", "message_ids": ["abc"], "yes": True,
        })
        assert "No valid message IDs" in capsys.readouterr().out

    def test_forward_ok(self, cli_db, capsys):
        pool, client = _mock_pool()
        client.get_entity = AsyncMock(return_value=SimpleNamespace(id=1))
        client.forward_messages = AsyncMock()
        _run_cli("forward", pool, cli_db, {
            "phone": _PHONE, "from_chat": "A", "to_chat": "B", "message_ids": ["1,2"], "yes": True,
        })
        assert "Forwarded 2 message(s)" in capsys.readouterr().out

    def test_forward_abort(self, cli_db, capsys):
        pool, _ = _mock_pool()
        with patch("builtins.input", return_value="n"):
            _run_cli("forward", pool, cli_db, {
                "phone": _PHONE, "from_chat": "A", "to_chat": "B", "message_ids": ["1"], "yes": False,
            })
        assert "Aborted." in capsys.readouterr().out

    def test_forward_client_unavailable(self, cli_db, capsys):
        pool, _ = _mock_pool()
        pool.get_native_client_by_phone = AsyncMock(return_value=None)
        _run_cli("forward", pool, cli_db, {
            "phone": _PHONE, "from_chat": "A", "to_chat": "B", "message_ids": ["1"], "yes": True,
        })
        assert "unavailable" in capsys.readouterr().out

    def test_forward_error(self, cli_db, capsys):
        pool, client = _mock_pool()
        client.get_entity = AsyncMock(side_effect=RuntimeError("err"))
        _run_cli("forward", pool, cli_db, {
            "phone": _PHONE, "from_chat": "A", "to_chat": "B", "message_ids": ["1"], "yes": True,
        })
        assert "Error forwarding" in capsys.readouterr().out


class TestCliEditMessage:
    """CLI dialogs edit-message."""

    def test_edit_message_ok(self, cli_db, capsys):
        pool, client = _mock_pool()
        client.get_entity = AsyncMock(return_value=SimpleNamespace(id=1))
        client.edit_message = AsyncMock()
        _run_cli("edit-message", pool, cli_db, {
            "phone": _PHONE, "chat_id": "@ch", "message_id": 42, "text": "new text", "yes": True,
        })
        assert "edited" in capsys.readouterr().out

    def test_edit_message_abort(self, cli_db, capsys):
        pool, _ = _mock_pool()
        with patch("builtins.input", return_value="n"):
            _run_cli("edit-message", pool, cli_db, {
                "phone": _PHONE, "chat_id": "@ch", "message_id": 42, "text": "new", "yes": False,
            })
        assert "Aborted." in capsys.readouterr().out

    def test_edit_message_client_unavailable(self, cli_db, capsys):
        pool, _ = _mock_pool()
        pool.get_native_client_by_phone = AsyncMock(return_value=None)
        _run_cli("edit-message", pool, cli_db, {
            "phone": _PHONE, "chat_id": "@ch", "message_id": 42, "text": "t", "yes": True,
        })
        assert "unavailable" in capsys.readouterr().out

    def test_edit_message_error(self, cli_db, capsys):
        pool, client = _mock_pool()
        client.get_entity = AsyncMock(side_effect=RuntimeError("oops"))
        _run_cli("edit-message", pool, cli_db, {
            "phone": _PHONE, "chat_id": "@ch", "message_id": 42, "text": "t", "yes": True,
        })
        assert "Error editing" in capsys.readouterr().out

    def test_edit_message_no_accounts(self, cli_db, capsys):
        pool, _ = _mock_pool(clients={})
        _run_cli("edit-message", pool, cli_db, {
            "phone": None, "chat_id": "@ch", "message_id": 42, "text": "t", "yes": True,
        })
        assert "No connected accounts." in capsys.readouterr().out


class TestCliDeleteMessage:
    """CLI dialogs delete-message."""

    def test_delete_message_ok(self, cli_db, capsys):
        pool, client = _mock_pool()
        client.get_entity = AsyncMock(return_value=SimpleNamespace(id=1))
        client.delete_messages = AsyncMock()
        _run_cli("delete-message", pool, cli_db, {
            "phone": _PHONE, "chat_id": "@ch", "message_ids": ["1,2"], "yes": True,
        })
        assert "Deleted 2 message(s)" in capsys.readouterr().out

    def test_delete_message_no_valid_ids(self, cli_db, capsys):
        pool, _ = _mock_pool()
        _run_cli("delete-message", pool, cli_db, {
            "phone": _PHONE, "chat_id": "@ch", "message_ids": ["abc"], "yes": True,
        })
        assert "No valid message IDs" in capsys.readouterr().out

    def test_delete_message_abort(self, cli_db, capsys):
        pool, _ = _mock_pool()
        with patch("builtins.input", return_value="n"):
            _run_cli("delete-message", pool, cli_db, {
                "phone": _PHONE, "chat_id": "@ch", "message_ids": ["1"], "yes": False,
            })
        assert "Aborted." in capsys.readouterr().out

    def test_delete_message_error(self, cli_db, capsys):
        pool, client = _mock_pool()
        client.get_entity = AsyncMock(side_effect=RuntimeError("err"))
        _run_cli("delete-message", pool, cli_db, {
            "phone": _PHONE, "chat_id": "@ch", "message_ids": ["1"], "yes": True,
        })
        assert "Error deleting" in capsys.readouterr().out


class TestCliPinUnpin:
    """CLI dialogs pin-message / unpin-message."""

    def test_pin_ok(self, cli_db, capsys):
        pool, client = _mock_pool()
        client.get_entity = AsyncMock(return_value=SimpleNamespace(id=1))
        client.pin_message = AsyncMock()
        _run_cli("pin-message", pool, cli_db, {
            "phone": _PHONE, "chat_id": "@ch", "message_id": 10, "notify": False, "yes": True,
        })
        assert "pinned" in capsys.readouterr().out

    def test_pin_abort(self, cli_db, capsys):
        pool, _ = _mock_pool()
        with patch("builtins.input", return_value="n"):
            _run_cli("pin-message", pool, cli_db, {
                "phone": _PHONE, "chat_id": "@ch", "message_id": 10, "notify": False, "yes": False,
            })
        assert "Aborted." in capsys.readouterr().out

    def test_pin_client_unavailable(self, cli_db, capsys):
        pool, _ = _mock_pool()
        pool.get_native_client_by_phone = AsyncMock(return_value=None)
        _run_cli("pin-message", pool, cli_db, {
            "phone": _PHONE, "chat_id": "@ch", "message_id": 10, "notify": False, "yes": True,
        })
        assert "unavailable" in capsys.readouterr().out

    def test_pin_error(self, cli_db, capsys):
        pool, client = _mock_pool()
        client.get_entity = AsyncMock(side_effect=RuntimeError("err"))
        _run_cli("pin-message", pool, cli_db, {
            "phone": _PHONE, "chat_id": "@ch", "message_id": 10, "notify": False, "yes": True,
        })
        assert "Error pinning" in capsys.readouterr().out

    def test_unpin_ok(self, cli_db, capsys):
        pool, client = _mock_pool()
        client.get_entity = AsyncMock(return_value=SimpleNamespace(id=1))
        client.unpin_message = AsyncMock()
        _run_cli("unpin-message", pool, cli_db, {
            "phone": _PHONE, "chat_id": "@ch", "message_id": 10, "yes": True,
        })
        assert "unpinned" in capsys.readouterr().out

    def test_unpin_all(self, cli_db, capsys):
        pool, client = _mock_pool()
        client.get_entity = AsyncMock(return_value=SimpleNamespace(id=1))
        client.unpin_message = AsyncMock()
        _run_cli("unpin-message", pool, cli_db, {
            "phone": _PHONE, "chat_id": "@ch", "message_id": None, "yes": True,
        })
        assert "unpinned" in capsys.readouterr().out

    def test_unpin_abort(self, cli_db, capsys):
        pool, _ = _mock_pool()
        with patch("builtins.input", return_value="n"):
            _run_cli("unpin-message", pool, cli_db, {
                "phone": _PHONE, "chat_id": "@ch", "message_id": None, "yes": False,
            })
        assert "Aborted." in capsys.readouterr().out

    def test_unpin_error(self, cli_db, capsys):
        pool, client = _mock_pool()
        client.get_entity = AsyncMock(side_effect=RuntimeError("err"))
        _run_cli("unpin-message", pool, cli_db, {
            "phone": _PHONE, "chat_id": "@ch", "message_id": None, "yes": True,
        })
        assert "Error unpinning" in capsys.readouterr().out


class TestCliParticipants:
    """CLI dialogs participants."""

    def test_participants_ok(self, cli_db, capsys):
        pool, client = _mock_pool()
        p1 = SimpleNamespace(id=1, first_name="Alice", last_name="B", username="alice")
        p2 = SimpleNamespace(id=2, first_name="Bob", last_name=None, username=None)
        client.get_entity = AsyncMock(return_value=SimpleNamespace(id=1))
        client.get_participants = AsyncMock(return_value=[p1, p2])
        _run_cli("participants", pool, cli_db, {
            "phone": _PHONE, "chat_id": "@ch", "limit": 200, "search": "",
        })
        out = capsys.readouterr().out
        assert "Alice" in out
        assert "Bob" in out
        assert "Total: 2" in out

    def test_participants_empty(self, cli_db, capsys):
        pool, client = _mock_pool()
        client.get_entity = AsyncMock(return_value=SimpleNamespace(id=1))
        client.get_participants = AsyncMock(return_value=[])
        _run_cli("participants", pool, cli_db, {
            "phone": _PHONE, "chat_id": "@ch", "limit": 200, "search": "",
        })
        assert "No participants found." in capsys.readouterr().out

    def test_participants_error(self, cli_db, capsys):
        pool, client = _mock_pool()
        client.get_entity = AsyncMock(side_effect=RuntimeError("err"))
        _run_cli("participants", pool, cli_db, {
            "phone": _PHONE, "chat_id": "@ch", "limit": 200, "search": "",
        })
        assert "Error fetching participants" in capsys.readouterr().out

    def test_participants_no_accounts(self, cli_db, capsys):
        pool, _ = _mock_pool(clients={})
        _run_cli("participants", pool, cli_db, {
            "phone": None, "chat_id": "@ch", "limit": 200, "search": "",
        })
        assert "No connected accounts." in capsys.readouterr().out


class TestCliEditAdmin:
    """CLI dialogs edit-admin."""

    def test_edit_admin_ok(self, cli_db, capsys):
        pool, client = _mock_pool()
        client.get_entity = AsyncMock(return_value=SimpleNamespace(id=1))
        client.edit_admin = AsyncMock()
        _run_cli("edit-admin", pool, cli_db, {
            "phone": _PHONE, "chat_id": "@ch", "user_id": "@u", "is_admin": True, "title": "Boss", "yes": True,
        })
        assert "Admin rights updated" in capsys.readouterr().out

    def test_edit_admin_abort(self, cli_db, capsys):
        pool, _ = _mock_pool()
        with patch("builtins.input", return_value="n"):
            _run_cli("edit-admin", pool, cli_db, {
                "phone": _PHONE, "chat_id": "@ch", "user_id": "@u", "is_admin": True, "title": None, "yes": False,
            })
        assert "Aborted." in capsys.readouterr().out

    def test_edit_admin_error(self, cli_db, capsys):
        pool, client = _mock_pool()
        client.get_entity = AsyncMock(side_effect=RuntimeError("err"))
        _run_cli("edit-admin", pool, cli_db, {
            "phone": _PHONE, "chat_id": "@ch", "user_id": "@u", "is_admin": True, "title": None, "yes": True,
        })
        assert "Error editing admin" in capsys.readouterr().out


class TestCliEditPermissions:
    """CLI dialogs edit-permissions."""

    def test_edit_permissions_ok(self, cli_db, capsys):
        pool, client = _mock_pool()
        client.get_entity = AsyncMock(return_value=SimpleNamespace(id=1))
        client.edit_permissions = AsyncMock()
        _run_cli("edit-permissions", pool, cli_db, {
            "phone": _PHONE, "chat_id": "@ch", "user_id": "@u",
            "send_messages": "true", "send_media": None, "until_date": None, "yes": True,
        })
        assert "Permissions updated" in capsys.readouterr().out

    def test_edit_permissions_no_flags(self, cli_db, capsys):
        pool, _ = _mock_pool()
        _run_cli("edit-permissions", pool, cli_db, {
            "phone": _PHONE, "chat_id": "@ch", "user_id": "@u",
            "send_messages": None, "send_media": None, "until_date": None, "yes": True,
        })
        assert "specify at least one flag" in capsys.readouterr().out

    def test_edit_permissions_with_until_date(self, cli_db, capsys):
        pool, client = _mock_pool()
        client.get_entity = AsyncMock(return_value=SimpleNamespace(id=1))
        client.edit_permissions = AsyncMock()
        _run_cli("edit-permissions", pool, cli_db, {
            "phone": _PHONE, "chat_id": "@ch", "user_id": "@u",
            "send_messages": "true", "send_media": "false", "until_date": "2025-12-31", "yes": True,
        })
        assert "Permissions updated" in capsys.readouterr().out

    def test_edit_permissions_error(self, cli_db, capsys):
        pool, client = _mock_pool()
        client.get_entity = AsyncMock(side_effect=RuntimeError("err"))
        _run_cli("edit-permissions", pool, cli_db, {
            "phone": _PHONE, "chat_id": "@ch", "user_id": "@u",
            "send_messages": "true", "send_media": None, "until_date": None, "yes": True,
        })
        assert "Error editing permissions" in capsys.readouterr().out


class TestCliKick:
    """CLI dialogs kick."""

    def test_kick_ok(self, cli_db, capsys):
        pool, client = _mock_pool()
        client.get_entity = AsyncMock(return_value=SimpleNamespace(id=1))
        client.kick_participant = AsyncMock()
        _run_cli("kick", pool, cli_db, {
            "phone": _PHONE, "chat_id": "@ch", "user_id": "@u", "yes": True,
        })
        assert "kicked" in capsys.readouterr().out

    def test_kick_abort(self, cli_db, capsys):
        pool, _ = _mock_pool()
        with patch("builtins.input", return_value="n"):
            _run_cli("kick", pool, cli_db, {
                "phone": _PHONE, "chat_id": "@ch", "user_id": "@u", "yes": False,
            })
        assert "Aborted." in capsys.readouterr().out

    def test_kick_error(self, cli_db, capsys):
        pool, client = _mock_pool()
        client.get_entity = AsyncMock(side_effect=RuntimeError("err"))
        _run_cli("kick", pool, cli_db, {
            "phone": _PHONE, "chat_id": "@ch", "user_id": "@u", "yes": True,
        })
        assert "Error kicking" in capsys.readouterr().out


class TestCliArchiveUnarchiveMarkRead:
    """CLI dialogs archive, unarchive, mark-read."""

    def test_archive_ok(self, cli_db, capsys):
        pool, client = _mock_pool()
        client.get_entity = AsyncMock(return_value=SimpleNamespace(id=1))
        client.edit_folder = AsyncMock()
        _run_cli("archive", pool, cli_db, {"phone": _PHONE, "chat_id": "@ch"})
        assert "archived" in capsys.readouterr().out

    def test_archive_error(self, cli_db, capsys):
        pool, client = _mock_pool()
        client.get_entity = AsyncMock(side_effect=RuntimeError("err"))
        _run_cli("archive", pool, cli_db, {"phone": _PHONE, "chat_id": "@ch"})
        assert "Error archiving" in capsys.readouterr().out

    def test_archive_no_accounts(self, cli_db, capsys):
        pool, _ = _mock_pool(clients={})
        _run_cli("archive", pool, cli_db, {"phone": None, "chat_id": "@ch"})
        assert "No connected accounts." in capsys.readouterr().out

    def test_archive_client_unavailable(self, cli_db, capsys):
        pool, _ = _mock_pool()
        pool.get_native_client_by_phone = AsyncMock(return_value=None)
        _run_cli("archive", pool, cli_db, {"phone": _PHONE, "chat_id": "@ch"})
        assert "unavailable" in capsys.readouterr().out

    def test_unarchive_ok(self, cli_db, capsys):
        pool, client = _mock_pool()
        client.get_entity = AsyncMock(return_value=SimpleNamespace(id=1))
        client.edit_folder = AsyncMock()
        _run_cli("unarchive", pool, cli_db, {"phone": _PHONE, "chat_id": "@ch"})
        assert "unarchived" in capsys.readouterr().out

    def test_unarchive_error(self, cli_db, capsys):
        pool, client = _mock_pool()
        client.get_entity = AsyncMock(side_effect=RuntimeError("err"))
        _run_cli("unarchive", pool, cli_db, {"phone": _PHONE, "chat_id": "@ch"})
        assert "Error unarchiving" in capsys.readouterr().out

    def test_mark_read_ok(self, cli_db, capsys):
        pool, client = _mock_pool()
        client.get_entity = AsyncMock(return_value=SimpleNamespace(id=1))
        client.send_read_acknowledge = AsyncMock()
        _run_cli("mark-read", pool, cli_db, {"phone": _PHONE, "chat_id": "@ch", "max_id": None})
        assert "marked as read" in capsys.readouterr().out

    def test_mark_read_with_max_id(self, cli_db, capsys):
        pool, client = _mock_pool()
        client.get_entity = AsyncMock(return_value=SimpleNamespace(id=1))
        client.send_read_acknowledge = AsyncMock()
        _run_cli("mark-read", pool, cli_db, {"phone": _PHONE, "chat_id": "@ch", "max_id": 100})
        assert "marked as read" in capsys.readouterr().out

    def test_mark_read_error(self, cli_db, capsys):
        pool, client = _mock_pool()
        client.get_entity = AsyncMock(side_effect=RuntimeError("err"))
        _run_cli("mark-read", pool, cli_db, {"phone": _PHONE, "chat_id": "@ch", "max_id": None})
        assert "Error marking" in capsys.readouterr().out


class TestCliTopics:
    """CLI dialogs topics."""

    def test_topics_from_pool(self, cli_db, capsys):
        topics = [{"id": 1, "title": "General", "icon_emoji_id": None, "date": "2025-01-01"}]
        pool, _ = _mock_pool(get_forum_topics=topics)
        _run_cli("topics", pool, cli_db, {"channel_id": -100111})
        out = capsys.readouterr().out
        assert "General" in out

    def test_topics_from_db_fallback(self, cli_db, capsys):
        pool, _ = _mock_pool(get_forum_topics=[])
        topics = [{"id": 2, "title": "DB Topic", "icon_emoji_id": 42, "date": None}]
        with patch.object(cli_db, "get_forum_topics", new_callable=AsyncMock, return_value=topics):
            _run_cli("topics", pool, cli_db, {"channel_id": -100111})
        out = capsys.readouterr().out
        assert "DB Topic" in out

    def test_topics_not_found(self, cli_db, capsys):
        pool, _ = _mock_pool(get_forum_topics=[])
        with patch.object(cli_db, "get_forum_topics", new_callable=AsyncMock, return_value=[]):
            _run_cli("topics", pool, cli_db, {"channel_id": -100111})
        assert "No forum topics found" in capsys.readouterr().out


class TestCliDownloadMedia:
    """CLI dialogs download-media."""

    def test_download_media_ok(self, cli_db, capsys):
        pool, client = _mock_pool()
        client.get_entity = AsyncMock(return_value=SimpleNamespace(id=1))
        msg = SimpleNamespace(id=1, media=True)

        async def _iter_messages(entity, ids):
            yield msg

        client.iter_messages = MagicMock(return_value=_iter_messages(None, None))
        client.download_media = AsyncMock(return_value="/tmp/photo.jpg")
        _run_cli("download-media", pool, cli_db, {
            "phone": _PHONE, "chat_id": "@ch", "message_id": 1, "output_dir": "/tmp",
        })
        assert "Downloaded: /tmp/photo.jpg" in capsys.readouterr().out

    def test_download_media_no_media(self, cli_db, capsys):
        pool, client = _mock_pool()
        client.get_entity = AsyncMock(return_value=SimpleNamespace(id=1))
        msg = SimpleNamespace(id=1, media=None)

        async def _iter_messages(entity, ids):
            yield msg

        client.iter_messages = MagicMock(return_value=_iter_messages(None, None))
        client.download_media = AsyncMock(return_value=None)
        _run_cli("download-media", pool, cli_db, {
            "phone": _PHONE, "chat_id": "@ch", "message_id": 1, "output_dir": "/tmp",
        })
        assert "No media" in capsys.readouterr().out

    def test_download_media_message_not_found(self, cli_db, capsys):
        pool, client = _mock_pool()
        client.get_entity = AsyncMock(return_value=SimpleNamespace(id=1))

        async def _iter_messages(entity, ids):
            return
            yield  # make it an async generator

        client.iter_messages = MagicMock(return_value=_iter_messages(None, None))
        _run_cli("download-media", pool, cli_db, {
            "phone": _PHONE, "chat_id": "@ch", "message_id": 1, "output_dir": "/tmp",
        })
        assert "not found" in capsys.readouterr().out

    def test_download_media_error(self, cli_db, capsys):
        pool, client = _mock_pool()
        client.get_entity = AsyncMock(side_effect=RuntimeError("err"))
        _run_cli("download-media", pool, cli_db, {
            "phone": _PHONE, "chat_id": "@ch", "message_id": 1, "output_dir": "/tmp",
        })
        assert "Error downloading media" in capsys.readouterr().out


class TestCliBroadcastStats:
    """CLI dialogs broadcast-stats."""

    def test_broadcast_stats_ok(self, cli_db, capsys):
        pool, client = _mock_pool()
        client.get_entity = AsyncMock(return_value=SimpleNamespace(id=1))
        stats = SimpleNamespace(
            followers=SimpleNamespace(current=1000, previous=900),
            views_per_post=SimpleNamespace(current=500, previous=400),
            shares_per_post=100,
            reactions_per_post=None,
            forwards_per_post=None,
            period=SimpleNamespace(min_date="2025-01-01", max_date="2025-01-31"),
            enabled_notifications=0.8,
        )
        client.get_broadcast_stats = AsyncMock(return_value=stats)
        _run_cli("broadcast-stats", pool, cli_db, {
            "phone": _PHONE, "chat_id": "@ch",
        })
        out = capsys.readouterr().out
        assert "followers: 1000" in out
        assert "period:" in out
        assert "enabled_notifications" in out

    def test_broadcast_stats_error(self, cli_db, capsys):
        pool, client = _mock_pool()
        client.get_entity = AsyncMock(side_effect=RuntimeError("err"))
        _run_cli("broadcast-stats", pool, cli_db, {
            "phone": _PHONE, "chat_id": "@ch",
        })
        assert "Error fetching broadcast stats" in capsys.readouterr().out

    def test_broadcast_stats_no_accounts(self, cli_db, capsys):
        pool, _ = _mock_pool(clients={})
        _run_cli("broadcast-stats", pool, cli_db, {
            "phone": None, "chat_id": "@ch",
        })
        assert "No connected accounts." in capsys.readouterr().out


class TestCliLeave:
    """CLI dialogs leave."""

    def test_leave_ok(self, cli_db, capsys):
        pool, _ = _mock_pool()
        with (
            patch(_SVC_DIALOGS, new_callable=AsyncMock, return_value=_FAKE_DIALOGS),
            patch(_SVC_LEAVE, new_callable=AsyncMock, return_value={-100111: True}),
        ):
            _run_cli("leave", pool, cli_db, {
                "phone": _PHONE, "dialog_ids": ["-100111"], "yes": True,
            })
        out = capsys.readouterr().out
        assert "1 left" in out

    def test_leave_abort(self, cli_db, capsys):
        pool, _ = _mock_pool()
        with (
            patch(_SVC_DIALOGS, new_callable=AsyncMock, return_value=_FAKE_DIALOGS),
            patch("builtins.input", return_value="n"),
        ):
            _run_cli("leave", pool, cli_db, {
                "phone": _PHONE, "dialog_ids": ["-100111"], "yes": False,
            })
        assert "Aborted." in capsys.readouterr().out

    def test_leave_invalid_ids(self, cli_db, capsys):
        pool, _ = _mock_pool()
        _run_cli("leave", pool, cli_db, {
            "phone": _PHONE, "dialog_ids": ["abc"], "yes": True,
        })
        out = capsys.readouterr().out
        assert "No valid dialog IDs" in out

    def test_leave_partial_invalid_ids(self, cli_db, capsys):
        pool, _ = _mock_pool()
        with (
            patch(_SVC_DIALOGS, new_callable=AsyncMock, return_value=_FAKE_DIALOGS),
            patch(_SVC_LEAVE, new_callable=AsyncMock, return_value={-100111: True}),
        ):
            _run_cli("leave", pool, cli_db, {
                "phone": _PHONE, "dialog_ids": ["-100111,abc"], "yes": True,
            })
        out = capsys.readouterr().out
        assert "Invalid dialog ID" in out
        assert "1 left" in out

    def test_leave_no_accounts(self, cli_db, capsys):
        pool, _ = _mock_pool(clients={})
        _run_cli("leave", pool, cli_db, {
            "phone": None, "dialog_ids": ["1"], "yes": True,
        })
        assert "No connected accounts." in capsys.readouterr().out


class TestCliCreateChannel:
    """CLI dialogs create-channel."""

    def test_create_channel_ok(self, cli_db, capsys):
        pool, _ = _mock_pool()
        mock_client = MagicMock()
        channel = SimpleNamespace(id=12345, username="newchan")
        mock_result = SimpleNamespace(chats=[channel])
        mock_client.__call__ = AsyncMock(return_value=mock_result)
        pool.clients = {_PHONE: mock_client}

        # Replace the client's __call__ to handle CreateChannelRequest
        async def fake_call(req):
            return mock_result

        mock_client.side_effect = fake_call

        with patch("src.cli.commands.dialogs.pool", pool, create=True):
            _run_cli("create-channel", pool, cli_db, {
                "phone": _PHONE, "title": "New Channel", "about": "test", "username": "",
            })
        out = capsys.readouterr().out
        assert "Created channel" in out

    def test_create_channel_no_accounts(self, cli_db, capsys):
        pool, _ = _mock_pool(clients={})
        _run_cli("create-channel", pool, cli_db, {
            "phone": None, "title": "Ch", "about": "", "username": "",
        })
        assert "No connected accounts." in capsys.readouterr().out


# =========================================================================
# Web Tests
# =========================================================================


async def _build_web_app(db, real_pool_harness_factory, *, with_account=True):
    """Build a web app for dialogs route tests."""
    from tests.helpers import AsyncIterMessages, FakeCliTelethonClient

    config = AppConfig()
    config.telegram.api_id = 12345
    config.telegram.api_hash = "test_hash"
    config.web.password = "testpass"

    harness = real_pool_harness_factory()
    if with_account:
        harness.queue_cli_client(
            phone=_PHONE,
            client=FakeCliTelethonClient(
                iter_dialogs_factory=lambda: AsyncIterMessages([]),
                entity_resolver=lambda _peer: MagicMock(),
            ),
        )
        await harness.connect_account(
            _PHONE,
            session_string="test_session",
            is_primary=True,
        )

    app, db = await build_web_app(config, harness, db=db)
    return app, db, harness


@pytest.fixture
async def web_client(db, real_pool_harness_factory):
    app, db, harness = await _build_web_app(db, real_pool_harness_factory)
    async with make_auth_client(app) as c:
        yield c, app
    await app.state.collection_queue.shutdown()
    await app.state.pool.disconnect_all()


class TestWebPage:
    """GET /dialogs/."""

    @pytest.mark.asyncio
    async def test_page_renders(self, web_client):
        c, app = web_client
        resp = await c.get("/dialogs/")
        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_page_with_phone(self, web_client):
        c, app = web_client
        phone_encoded = "%2B79001234567"
        resp = await c.get(f"/dialogs/?phone={phone_encoded}")
        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_page_invalid_phone_shows_no_dialogs(self, web_client):
        c, app = web_client
        resp = await c.get("/dialogs/?phone=%2B00000")
        assert resp.status_code == 200


class TestWebRefresh:
    """POST /dialogs/refresh."""

    @pytest.mark.asyncio
    async def test_refresh_ok(self, web_client):
        c, app = web_client
        resp = await c.post("/dialogs/refresh", data={"phone": _PHONE})
        assert resp.status_code == 200


class TestWebCacheStatus:
    """GET /dialogs/cache-status."""

    @pytest.mark.asyncio
    async def test_cache_status_empty(self, web_client):
        c, app = web_client
        resp = await c.get("/dialogs/cache-status")
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list)


class TestWebCacheClear:
    """POST /dialogs/cache-clear."""

    @pytest.mark.asyncio
    async def test_cache_clear_all(self, web_client):
        c, app = web_client
        resp = await c.post("/dialogs/cache-clear", data={"phone": ""})
        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_cache_clear_single(self, web_client):
        c, app = web_client
        resp = await c.post("/dialogs/cache-clear", data={"phone": _PHONE})
        assert resp.status_code == 200


async def _assert_enqueued(app, expected_type: str, expected_payload_subset: dict | None = None):
    """Verify at least one telegram_commands row exists with given type and payload."""
    db = app.state.db
    commands = await db.repos.telegram_commands.list_commands(limit=20)
    matches = [c for c in commands if c.command_type == expected_type]
    assert matches, (
        f"expected a command of type {expected_type!r}, got {[c.command_type for c in commands]}"
    )
    if expected_payload_subset:
        latest = matches[0]
        for k, v in expected_payload_subset.items():
            assert latest.payload.get(k) == v, (
                f"payload[{k!r}] expected {v!r}, got {latest.payload.get(k)!r}"
            )


class TestWebSend:
    """POST /dialogs/send — queued-command model."""

    @pytest.mark.asyncio
    async def test_send_missing_fields(self, web_client):
        c, app = web_client
        resp = await c.post("/dialogs/send", data={"phone": _PHONE, "recipient": "", "text": ""})
        assert resp.status_code == 200
        assert "error=missing_fields" in str(resp.url)

    @pytest.mark.asyncio
    async def test_send_enqueues(self, web_client):
        c, app = web_client
        resp = await c.post(
            "/dialogs/send",
            data={"phone": _PHONE, "recipient": "@u", "text": "hi"},
            follow_redirects=False,
        )
        assert resp.status_code == 303
        assert "command_id=" in resp.headers["location"]
        await _assert_enqueued(app, "dialogs.send", {"phone": _PHONE, "recipient": "@u", "text": "hi"})


class TestWebEditMessage:
    """POST /dialogs/edit-message — queued-command model."""

    @pytest.mark.asyncio
    async def test_edit_missing_fields(self, web_client):
        c, app = web_client
        resp = await c.post("/dialogs/edit-message", data={
            "phone": _PHONE, "chat_id": "", "message_id": "", "text": "",
        })
        assert "error=missing_fields" in str(resp.url)

    @pytest.mark.asyncio
    async def test_edit_enqueues(self, web_client):
        c, app = web_client
        resp = await c.post(
            "/dialogs/edit-message",
            data={"phone": _PHONE, "chat_id": "@ch", "message_id": "42", "text": "new"},
            follow_redirects=False,
        )
        assert resp.status_code == 303
        assert "command_id=" in resp.headers["location"]
        await _assert_enqueued(app, "dialogs.edit_message", {"phone": _PHONE, "chat_id": "@ch"})


class TestWebDeleteMessage:
    """POST /dialogs/delete-message — queued-command model."""

    @pytest.mark.asyncio
    async def test_delete_missing_fields(self, web_client):
        c, app = web_client
        resp = await c.post("/dialogs/delete-message", data={
            "phone": _PHONE, "chat_id": "", "message_ids": "",
        })
        assert "error=missing_fields" in str(resp.url)

    @pytest.mark.asyncio
    async def test_delete_invalid_ids(self, web_client):
        c, app = web_client
        resp = await c.post("/dialogs/delete-message", data={
            "phone": _PHONE, "chat_id": "@ch", "message_ids": "abc",
        })
        assert "error=invalid_ids" in str(resp.url)

    @pytest.mark.asyncio
    async def test_delete_enqueues(self, web_client):
        c, app = web_client
        resp = await c.post(
            "/dialogs/delete-message",
            data={"phone": _PHONE, "chat_id": "@ch", "message_ids": "1,2"},
            follow_redirects=False,
        )
        assert resp.status_code == 303
        assert "command_id=" in resp.headers["location"]
        await _assert_enqueued(app, "dialogs.delete_message", {"phone": _PHONE, "chat_id": "@ch"})


class TestWebForwardMessages:
    """POST /dialogs/forward-messages — queued-command model."""

    @pytest.mark.asyncio
    async def test_forward_missing_fields(self, web_client):
        c, app = web_client
        resp = await c.post("/dialogs/forward-messages", data={
            "phone": _PHONE, "from_chat": "", "to_chat": "", "message_ids": "",
        })
        assert "error=missing_fields" in str(resp.url)

    @pytest.mark.asyncio
    async def test_forward_invalid_ids(self, web_client):
        c, app = web_client
        resp = await c.post("/dialogs/forward-messages", data={
            "phone": _PHONE, "from_chat": "@a", "to_chat": "@b", "message_ids": "xyz",
        })
        assert "error=invalid_ids" in str(resp.url)

    @pytest.mark.asyncio
    async def test_forward_enqueues(self, web_client):
        c, app = web_client
        resp = await c.post(
            "/dialogs/forward-messages",
            data={"phone": _PHONE, "from_chat": "@a", "to_chat": "@b", "message_ids": "1,2"},
            follow_redirects=False,
        )
        assert resp.status_code == 303
        assert "command_id=" in resp.headers["location"]
        await _assert_enqueued(app, "dialogs.forward_messages", {"phone": _PHONE})


class TestWebPinMessage:
    """POST /dialogs/pin-message — queued-command model."""

    @pytest.mark.asyncio
    async def test_pin_missing_fields(self, web_client):
        c, app = web_client
        resp = await c.post("/dialogs/pin-message", data={
            "phone": "", "chat_id": "", "message_id": "",
        })
        assert "error=missing_fields" in str(resp.url)

    @pytest.mark.asyncio
    async def test_pin_enqueues(self, web_client):
        c, app = web_client
        resp = await c.post(
            "/dialogs/pin-message",
            data={"phone": _PHONE, "chat_id": "@ch", "message_id": "10", "notify": "1"},
            follow_redirects=False,
        )
        assert resp.status_code == 303
        assert "command_id=" in resp.headers["location"]
        await _assert_enqueued(app, "dialogs.pin_message", {"phone": _PHONE, "chat_id": "@ch"})


class TestWebUnpinMessage:
    """POST /dialogs/unpin-message — queued-command model."""

    @pytest.mark.asyncio
    async def test_unpin_missing_fields(self, web_client):
        c, app = web_client
        resp = await c.post("/dialogs/unpin-message", data={
            "phone": "", "chat_id": "",
        })
        assert "error=missing_fields" in str(resp.url)

    @pytest.mark.asyncio
    async def test_unpin_enqueues(self, web_client):
        c, app = web_client
        resp = await c.post(
            "/dialogs/unpin-message",
            data={"phone": _PHONE, "chat_id": "@ch", "message_id": "10"},
            follow_redirects=False,
        )
        assert resp.status_code == 303
        assert "command_id=" in resp.headers["location"]
        await _assert_enqueued(app, "dialogs.unpin_message", {"phone": _PHONE, "chat_id": "@ch"})

    @pytest.mark.asyncio
    async def test_unpin_all_enqueues(self, web_client):
        c, app = web_client
        resp = await c.post(
            "/dialogs/unpin-message",
            data={"phone": _PHONE, "chat_id": "@ch"},
            follow_redirects=False,
        )
        assert resp.status_code == 303
        assert "command_id=" in resp.headers["location"]


class TestWebParticipants:
    """GET /dialogs/participants — queued-command model.

    Returns cached snapshot if available, otherwise enqueues a command and
    responds 202.
    """

    @pytest.mark.asyncio
    async def test_participants_missing_params(self, web_client):
        c, app = web_client
        resp = await c.get("/dialogs/participants")
        assert resp.status_code == 400

    @pytest.mark.asyncio
    async def test_participants_enqueues_when_no_snapshot(self, web_client):
        c, app = web_client
        phone_enc = _PHONE.replace("+", "%2B")
        resp = await c.get(f"/dialogs/participants?phone={phone_enc}&chat_id=@ch")
        # no snapshot yet → 202 Accepted with queued status
        assert resp.status_code == 202
        body = resp.json()
        assert body.get("status") == "queued"
        assert "command_id" in body
        await _assert_enqueued(app, "dialogs.participants", {"phone": _PHONE, "chat_id": "@ch"})


class TestWebArchive:
    """POST /dialogs/archive and /unarchive — queued-command model."""

    @pytest.mark.asyncio
    async def test_archive_missing_fields(self, web_client):
        c, app = web_client
        resp = await c.post("/dialogs/archive", data={"phone": "", "chat_id": ""})
        assert "error=missing_fields" in str(resp.url)

    @pytest.mark.asyncio
    async def test_archive_enqueues(self, web_client):
        c, app = web_client
        resp = await c.post(
            "/dialogs/archive",
            data={"phone": _PHONE, "chat_id": "@ch"},
            follow_redirects=False,
        )
        assert resp.status_code == 303
        assert "command_id=" in resp.headers["location"]
        await _assert_enqueued(app, "dialogs.archive", {"phone": _PHONE, "chat_id": "@ch"})

    @pytest.mark.asyncio
    async def test_unarchive_enqueues(self, web_client):
        c, app = web_client
        resp = await c.post(
            "/dialogs/unarchive",
            data={"phone": _PHONE, "chat_id": "@ch"},
            follow_redirects=False,
        )
        assert resp.status_code == 303
        assert "command_id=" in resp.headers["location"]
        await _assert_enqueued(app, "dialogs.unarchive", {"phone": _PHONE, "chat_id": "@ch"})

    @pytest.mark.asyncio
    async def test_unarchive_missing_fields(self, web_client):
        c, app = web_client
        resp = await c.post("/dialogs/unarchive", data={"phone": "", "chat_id": ""})
        assert "error=missing_fields" in str(resp.url)


class TestWebMarkRead:
    """POST /dialogs/mark-read — queued-command model."""

    @pytest.mark.asyncio
    async def test_mark_read_missing_fields(self, web_client):
        c, app = web_client
        resp = await c.post("/dialogs/mark-read", data={"phone": "", "chat_id": ""})
        assert "error=missing_fields" in str(resp.url)

    @pytest.mark.asyncio
    async def test_mark_read_enqueues(self, web_client):
        c, app = web_client
        resp = await c.post(
            "/dialogs/mark-read",
            data={"phone": _PHONE, "chat_id": "@ch"},
            follow_redirects=False,
        )
        assert resp.status_code == 303
        assert "command_id=" in resp.headers["location"]
        await _assert_enqueued(app, "dialogs.mark_read", {"phone": _PHONE, "chat_id": "@ch"})

    @pytest.mark.asyncio
    async def test_mark_read_with_max_id_enqueues(self, web_client):
        c, app = web_client
        resp = await c.post(
            "/dialogs/mark-read",
            data={"phone": _PHONE, "chat_id": "@ch", "max_id": "100"},
            follow_redirects=False,
        )
        assert resp.status_code == 303
        await _assert_enqueued(app, "dialogs.mark_read", {"phone": _PHONE, "max_id": 100})


class TestWebEditAdmin:
    """POST /dialogs/edit-admin — queued-command model."""

    @pytest.mark.asyncio
    async def test_edit_admin_missing_fields(self, web_client):
        c, app = web_client
        resp = await c.post("/dialogs/edit-admin", data={
            "phone": "", "chat_id": "", "user_id": "",
        })
        assert "error=missing_fields" in str(resp.url)

    @pytest.mark.asyncio
    async def test_edit_admin_enqueues(self, web_client):
        c, app = web_client
        resp = await c.post(
            "/dialogs/edit-admin",
            data={
                "phone": _PHONE, "chat_id": "@ch", "user_id": "@u",
                "is_admin": "1", "title": "Boss",
            },
            follow_redirects=False,
        )
        assert resp.status_code == 303
        assert "command_id=" in resp.headers["location"]
        await _assert_enqueued(app, "dialogs.edit_admin", {"phone": _PHONE, "chat_id": "@ch"})


class TestWebEditPermissions:
    """POST /dialogs/edit-permissions — queued-command model."""

    @pytest.mark.asyncio
    async def test_edit_permissions_no_flags(self, web_client):
        c, app = web_client
        resp = await c.post("/dialogs/edit-permissions", data={
            "phone": _PHONE, "chat_id": "@ch", "user_id": "@u",
        })
        assert "error=no_permission_flags" in str(resp.url)

    @pytest.mark.asyncio
    async def test_edit_permissions_missing_fields(self, web_client):
        c, app = web_client
        resp = await c.post("/dialogs/edit-permissions", data={
            "phone": "", "chat_id": "", "user_id": "", "send_messages": "true",
        })
        assert "error=missing_fields" in str(resp.url)

    @pytest.mark.asyncio
    async def test_edit_permissions_enqueues(self, web_client):
        c, app = web_client
        resp = await c.post(
            "/dialogs/edit-permissions",
            data={
                "phone": _PHONE, "chat_id": "@ch", "user_id": "@u",
                "send_messages": "true",
            },
            follow_redirects=False,
        )
        assert resp.status_code == 303
        assert "command_id=" in resp.headers["location"]
        await _assert_enqueued(app, "dialogs.edit_permissions", {"phone": _PHONE, "chat_id": "@ch"})


class TestWebKick:
    """POST /dialogs/kick — queued-command model."""

    @pytest.mark.asyncio
    async def test_kick_missing_fields(self, web_client):
        c, app = web_client
        resp = await c.post("/dialogs/kick", data={
            "phone": "", "chat_id": "", "user_id": "",
        })
        assert "error=missing_fields" in str(resp.url)

    @pytest.mark.asyncio
    async def test_kick_enqueues(self, web_client):
        c, app = web_client
        resp = await c.post(
            "/dialogs/kick",
            data={"phone": _PHONE, "chat_id": "@ch", "user_id": "@u"},
            follow_redirects=False,
        )
        assert resp.status_code == 303
        assert "command_id=" in resp.headers["location"]
        await _assert_enqueued(app, "dialogs.kick", {"phone": _PHONE, "chat_id": "@ch"})


class TestWebBroadcastStats:
    """GET /dialogs/broadcast-stats — queued-command model."""

    @pytest.mark.asyncio
    async def test_broadcast_stats_missing_params(self, web_client):
        c, app = web_client
        resp = await c.get("/dialogs/broadcast-stats")
        assert resp.status_code == 400

    @pytest.mark.asyncio
    async def test_broadcast_stats_enqueues_when_no_snapshot(self, web_client):
        c, app = web_client
        phone_enc = _PHONE.replace("+", "%2B")
        resp = await c.get(f"/dialogs/broadcast-stats?phone={phone_enc}&chat_id=@ch")
        # no snapshot yet → 202 Accepted with queued status
        assert resp.status_code == 202
        body = resp.json()
        assert body.get("status") == "queued"
        assert "command_id" in body
        await _assert_enqueued(app, "dialogs.broadcast_stats", {"phone": _PHONE, "chat_id": "@ch"})


class TestWebLeave:
    """POST /dialogs/leave — queued-command model."""

    @pytest.mark.asyncio
    async def test_leave_enqueues(self, web_client):
        c, app = web_client
        resp = await c.post(
            "/dialogs/leave",
            data={"phone": _PHONE, "channel_ids": ["-100111:channel", "-100222:supergroup"]},
            follow_redirects=False,
        )
        assert resp.status_code == 303
        assert "command_id=" in resp.headers["location"]
        await _assert_enqueued(app, "dialogs.leave", {"phone": _PHONE})


class TestWebDownloadMedia:
    """POST /dialogs/download-media — queued-command model."""

    @pytest.mark.asyncio
    async def test_download_missing_fields(self, web_client):
        c, app = web_client
        resp = await c.post("/dialogs/download-media", data={
            "phone": "", "chat_id": "", "message_id": "",
        })
        assert "error=missing_fields" in str(resp.url)

    @pytest.mark.asyncio
    async def test_download_enqueues(self, web_client):
        c, app = web_client
        resp = await c.post(
            "/dialogs/download-media",
            data={"phone": _PHONE, "chat_id": "@ch", "message_id": "1"},
            follow_redirects=False,
        )
        assert resp.status_code == 303
        assert "command_id=" in resp.headers["location"]
        await _assert_enqueued(app, "dialogs.download_media", {"phone": _PHONE, "chat_id": "@ch"})


class TestWebCreateChannel:
    """GET and POST /dialogs/create-channel — queued-command model."""

    @pytest.mark.asyncio
    async def test_create_channel_page(self, web_client):
        c, app = web_client
        resp = await c.get("/dialogs/create-channel")
        assert resp.status_code == 200

    @pytest.mark.asyncio
    async def test_create_channel_enqueues(self, web_client):
        c, app = web_client
        resp = await c.post(
            "/dialogs/create-channel",
            data={"phone": _PHONE, "title": "Test", "about": "", "username": ""},
            follow_redirects=False,
        )
        assert resp.status_code == 303
        assert "command_id=" in resp.headers["location"]
        await _assert_enqueued(
            app, "dialogs.create_channel", {"phone": _PHONE, "title": "Test"},
        )
