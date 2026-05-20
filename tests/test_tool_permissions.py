"""Tests for agent tool permission system — 3-layer gates and permission storage."""

from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.agent.tools._registry import (
    normalize_phone,
    require_confirmation,
    require_phone_permission,
    require_pool,
    resolve_phone,
)
from src.agent.tools.permissions import (
    MCP_PREFIX,
    TOOL_CATEGORIES,
    _is_per_phone_format,
    build_template_context,
    filter_allowed_tools,
    load_tool_permissions,
    load_tool_permissions_all_phones,
    load_tool_permissions_union,
    save_tool_permissions,
)
from src.database import Database


@pytest.fixture
def mock_db():
    return MagicMock(spec=Database)


def _text(result: dict) -> str:
    return result["content"][0]["text"]


# ---------------------------------------------------------------------------
# _registry.normalize_phone
# ---------------------------------------------------------------------------


class TestNormalizePhone:
    def test_adds_plus_prefix(self):
        assert normalize_phone("79990001111") == "+79990001111"

    def test_preserves_existing_plus(self):
        assert normalize_phone("+79990001111") == "+79990001111"

    def test_strips_whitespace(self):
        assert normalize_phone("  +79990001111  ") == "+79990001111"

    def test_empty_string(self):
        assert normalize_phone("") == ""


# ---------------------------------------------------------------------------
# _registry.require_confirmation
# ---------------------------------------------------------------------------


class TestRequireConfirmation:
    def test_confirm_true_returns_none(self):
        assert require_confirmation("удалит канал", {"confirm": True}) is None

    def test_confirm_false_returns_warning(self):
        result = require_confirmation("удалит канал", {"confirm": False})
        assert result is not None
        assert "Подтвердите" in _text(result)

    def test_confirm_missing_returns_warning(self):
        result = require_confirmation("удалит канал", {})
        assert result is not None
        assert "Подтвердите" in _text(result)

    def test_action_description_in_text(self):
        result = require_confirmation("выйдет из 3 диалогов", {})
        assert "выйдет из 3 диалогов" in _text(result)

    def test_response_format(self):
        result = require_confirmation("test", {})
        assert isinstance(result, dict)
        assert result["content"][0]["type"] == "text"
        assert isinstance(result["content"][0]["text"], str)


# ---------------------------------------------------------------------------
# _registry.require_pool
# ---------------------------------------------------------------------------


class TestRequirePool:
    def test_pool_present_returns_none(self):
        assert require_pool(MagicMock()) is None

    def test_pool_none_returns_error(self):
        result = require_pool(None)
        assert "требует Telegram-клиент" in _text(result)

    def test_custom_action_in_text(self):
        result = require_pool(None, action="Выход из диалогов")
        assert "Выход из диалогов" in _text(result)


# ---------------------------------------------------------------------------
# _registry.resolve_phone
# ---------------------------------------------------------------------------


class TestResolvePhone:
    async def test_phone_provided(self, mock_db):
        phone, err = await resolve_phone(mock_db, "79990001111")
        assert phone == "+79990001111"
        assert err is None

    async def test_empty_phone_uses_primary(self, mock_db):
        mock_db.get_accounts = AsyncMock(
            return_value=[SimpleNamespace(phone="+79990001111", is_primary=True)]
        )
        phone, err = await resolve_phone(mock_db, "")
        assert phone == "+79990001111"
        assert err is None

    async def test_empty_phone_no_accounts(self, mock_db):
        mock_db.get_accounts = AsyncMock(return_value=[])
        phone, err = await resolve_phone(mock_db, "")
        assert phone == ""
        assert err is not None
        assert "нет подключённых" in _text(err)

    async def test_empty_phone_db_error(self, mock_db):
        mock_db.get_accounts = AsyncMock(side_effect=Exception("DB down"))
        phone, err = await resolve_phone(mock_db, "")
        assert phone == ""
        assert err is not None


# ---------------------------------------------------------------------------
# _registry.require_phone_permission
# ---------------------------------------------------------------------------


class TestRequirePhonePermission:
    async def test_no_setting_allows(self, mock_db):
        mock_db.get_setting = AsyncMock(return_value=None)
        assert await require_phone_permission(mock_db, "+79990001111", "leave_dialogs") is None

    async def test_db_error_blocks(self, mock_db):
        mock_db.get_setting = AsyncMock(side_effect=Exception("DB down"))
        result = await require_phone_permission(mock_db, "+79990001111", "leave_dialogs")
        assert result is not None
        assert "заблокировано" in _text(result)

    async def test_malformed_json_blocks(self, mock_db):
        mock_db.get_setting = AsyncMock(return_value="not json{{{")
        result = await require_phone_permission(mock_db, "+79990001111", "leave_dialogs")
        assert result is not None
        assert "поврежд" in _text(result)

    async def test_phone_in_allowed(self, mock_db):
        perms = {"+79990001111": {"leave_dialogs": True}}
        mock_db.get_setting = AsyncMock(return_value=json.dumps(perms))
        assert await require_phone_permission(mock_db, "+79990001111", "leave_dialogs") is None

    async def test_phone_not_in_allowed_read_denied(self, mock_db):
        # When a per-phone ACL exists, a phone absent from the dict must be
        # denied even for READ tools. require_phone_permission is only called
        # from phone-binded tools that touch the live Telegram client, so a
        # missing READ entry on read_messages / download_media / get_participants
        # would otherwise leak live data from an unauthorized account.
        perms = {"+79990002222": {"search_messages": True}}
        mock_db.get_setting = AsyncMock(return_value=json.dumps(perms))
        result = await require_phone_permission(mock_db, "+79990001111", "search_messages")
        assert result is not None
        assert "не разрешён" in _text(result)

    async def test_phone_not_in_allowed_write_denied(self, mock_db):
        # WRITE/DELETE: phone outside the saved ACL must be denied once any ACL exists.
        perms = {"+79990002222": {"leave_dialogs": True}}
        mock_db.get_setting = AsyncMock(return_value=json.dumps(perms))
        result = await require_phone_permission(mock_db, "+79990001111", "leave_dialogs")
        assert result is not None
        assert "не разрешён" in _text(result)

    async def test_phone_explicitly_denied(self, mock_db):
        perms = {
            "+79990001111": {"leave_dialogs": False},
            "+79990002222": {"leave_dialogs": True},
        }
        mock_db.get_setting = AsyncMock(return_value=json.dumps(perms))
        result = await require_phone_permission(mock_db, "+79990001111", "leave_dialogs")
        text = _text(result)
        assert "не разрешён" in text
        assert "+79990002222" in text

    async def test_read_tool_missing_from_per_phone_acl_denies(self, mock_db):
        # When per-phone ACL exists but no phone allows a given READ tool,
        # deny — this prevents the absent-phone READ bypass for live Telegram
        # tools (read_messages, download_media, get_participants…).
        perms = {"+79990001111": {"some_other_tool": True}}
        mock_db.get_setting = AsyncMock(return_value=json.dumps(perms))
        result = await require_phone_permission(mock_db, "+79990001111", "read_messages")
        assert result is not None
        assert "не разрешён" in _text(result)

    async def test_write_tool_not_in_saved_denies(self, mock_db):
        # WRITE/DELETE missing from saved ACL must deny — closes the new-tool bypass.
        perms = {"+79990001111": {"some_other_tool": True}}
        mock_db.get_setting = AsyncMock(return_value=json.dumps(perms))
        result = await require_phone_permission(mock_db, "+79990001111", "leave_dialogs")
        assert result is not None
        assert "не разрешён" in _text(result)

    async def test_send_reaction_blocked_for_restrictive_legacy_acl(self, mock_db):
        # Regression for PR #568 + #562 audit: previously saved restrictive ACL must not
        # implicitly grant a newly-introduced WRITE action like send_reaction.
        perms = {"+79990001111": {"search_messages": True, "pin_message": True}}
        mock_db.get_setting = AsyncMock(return_value=json.dumps(perms))
        result = await require_phone_permission(mock_db, "+79990001111", "send_reaction")
        assert result is not None
        assert "не разрешён" in _text(result)

    @pytest.mark.parametrize(
        "live_read_tool",
        ["read_messages", "download_media", "get_participants", "get_broadcast_stats"],
    )
    async def test_absent_phone_denied_for_live_telegram_read(self, mock_db, live_read_tool):
        # Round-3 Codex finding: phone-binded READ tools (read_messages,
        # download_media, get_participants, get_broadcast_stats) all hit the
        # live Telegram client. A newly added account absent from the per-phone
        # ACL must NOT inherit live-read access.
        perms = {"+79990001111": {live_read_tool: True}}
        mock_db.get_setting = AsyncMock(return_value=json.dumps(perms))
        # +79990002222 is absent → must deny
        result = await require_phone_permission(mock_db, "+79990002222", live_read_tool)
        assert result is not None
        assert "не разрешён" in _text(result)

    async def test_tool_missing_from_saved_denies(self, mock_db):
        """Tool not explicitly saved for a phone defaults to denied."""
        perms = {
            "+79990001111": {"send_photos_now": True},
            "+79990002222": {"some_other_tool": True},  # send_photos_now missing → denied
        }
        mock_db.get_setting = AsyncMock(return_value=json.dumps(perms))
        result = await require_phone_permission(mock_db, "+79990002222", "send_photos_now")
        assert result is not None
        assert "не разрешён" in _text(result)

    async def test_empty_phone_hint(self, mock_db):
        perms = {"+79990001111": {"leave_dialogs": True}}
        mock_db.get_setting = AsyncMock(return_value=json.dumps(perms))
        result = await require_phone_permission(mock_db, "", "leave_dialogs")
        text = _text(result)
        assert "укажи параметр phone" in text
        assert "+79990001111" in text

    async def test_legacy_flat_format_known_tool_allows(self, mock_db):
        """Legacy flat format must not crash; explicitly listed tool stays allowed."""
        perms = {"leave_dialogs": True, "search_messages": False}
        mock_db.get_setting = AsyncMock(return_value=json.dumps(perms))
        assert await require_phone_permission(mock_db, "+79990001111", "leave_dialogs") is None

    @pytest.mark.parametrize(
        "phone_bound_read_tool",
        ["read_messages", "download_media", "get_participants", "get_broadcast_stats", "resolve_entity"],
    )
    async def test_legacy_flat_missing_phone_bound_read_denies(self, mock_db, phone_bound_read_tool):
        """Codex round 11 regression: a legacy flat ACL that does not list
        the phone-bound READ tool must NOT fall through to the historical
        READ-permissive default. Otherwise existing flat ACLs from older
        installs allow live Telegram reads on any phone with no per-tool
        grant — a hard auth-boundary bypass."""
        perms = {"search_messages": True}  # flat, no phone-bound READ entries
        mock_db.get_setting = AsyncMock(return_value=json.dumps(perms))
        result = await require_phone_permission(mock_db, "+79990001111", phone_bound_read_tool)
        assert result is not None, f"flat ACL leaked {phone_bound_read_tool}"
        assert "не разрешён" in _text(result)

    async def test_legacy_flat_phone_bound_read_explicit_true_allows(self, mock_db):
        """Legacy flat ACL must still honour an explicit True grant for a
        phone-bound READ tool — admin opted in for everyone."""
        perms = {"read_messages": True}
        mock_db.get_setting = AsyncMock(return_value=json.dumps(perms))
        assert await require_phone_permission(mock_db, "+79990001111", "read_messages") is None

    async def test_legacy_flat_missing_write_key_denies(self, mock_db):
        """Legacy flat format with no entry for a WRITE tool must deny (fail-closed)."""
        perms = {"search_messages": True}
        mock_db.get_setting = AsyncMock(return_value=json.dumps(perms))
        result = await require_phone_permission(mock_db, "+79990001111", "leave_dialogs")
        assert result is not None
        assert "не разрешён" in _text(result)


# ---------------------------------------------------------------------------
# permissions.load_tool_permissions
# ---------------------------------------------------------------------------


class TestLoadToolPermissions:
    async def test_no_setting_returns_defaults(self, mock_db):
        mock_db.get_setting = AsyncMock(return_value=None)
        result = await load_tool_permissions(mock_db)
        assert all(v is True for v in result.values())
        assert set(result.keys()) == set(TOOL_CATEGORIES.keys())

    async def test_malformed_json_returns_defaults(self, mock_db):
        mock_db.get_setting = AsyncMock(return_value="{bad json")
        result = await load_tool_permissions(mock_db)
        assert all(v is True for v in result.values())

    async def test_flat_format(self, mock_db):
        saved = {"search_messages": False}
        mock_db.get_setting = AsyncMock(return_value=json.dumps(saved))
        result = await load_tool_permissions(mock_db)
        assert result["search_messages"] is False
        assert result["list_channels"] is True

    async def test_per_phone_matching(self, mock_db):
        saved = {"+79990001111": {"search_messages": False, "leave_dialogs": False}}
        mock_db.get_setting = AsyncMock(return_value=json.dumps(saved))
        result = await load_tool_permissions(mock_db, phone="+79990001111")
        assert result["search_messages"] is False
        assert result["leave_dialogs"] is False
        assert result["list_channels"] is True  # default

    async def test_per_phone_not_matching_fully_fail_closed(self, mock_db):
        # When a per-phone ACL exists but the requested phone is absent,
        # everything is denied (Codex round 4): allowing READ here used to
        # reopen the absent-phone live-READ leak through the settings save
        # path. Admin must explicitly opt-in each account.
        saved = {"+79990001111": {"search_messages": False}}
        mock_db.get_setting = AsyncMock(return_value=json.dumps(saved))
        result = await load_tool_permissions(mock_db, phone="+79990009999")
        assert all(v is False for v in result.values())

    async def test_per_phone_no_phone_uses_primary(self, mock_db):
        saved = {"+79990001111": {"search_messages": False}}
        mock_db.get_setting = AsyncMock(return_value=json.dumps(saved))
        mock_db.get_accounts = AsyncMock(
            return_value=[SimpleNamespace(phone="+79990001111", is_primary=True)]
        )
        result = await load_tool_permissions(mock_db, phone=None)
        assert result["search_messages"] is False

    async def test_per_phone_no_accounts_fully_fail_closed(self, mock_db):
        # Per-phone ACL exists but no primary account to resolve → no phone
        # matches saved entries.  Fully fail-closed (Codex round 4).
        saved = {"+79990001111": {"search_messages": False}}
        mock_db.get_setting = AsyncMock(return_value=json.dumps(saved))
        mock_db.get_accounts = AsyncMock(return_value=[])
        result = await load_tool_permissions(mock_db, phone=None)
        assert all(v is False for v in result.values())

    async def test_per_phone_missing_write_key_returns_false(self, mock_db):
        # WRITE/DELETE tools missing from a saved per-phone entry must come back as False.
        saved = {"+79990001111": {"search_messages": True}}
        mock_db.get_setting = AsyncMock(return_value=json.dumps(saved))
        result = await load_tool_permissions(mock_db, phone="+79990001111")
        assert result["search_messages"] is True
        assert result["send_reaction"] is False  # WRITE missing → False
        assert result["leave_dialogs"] is False  # DELETE missing → False
        assert result["list_channels"] is True  # non-phone-bound READ → True

    async def test_per_phone_missing_phone_bound_read_returns_false(self, mock_db):
        # Codex round 10 regression: phone-bound READ tools (read_messages,
        # download_media, get_participants, resolve_entity, …) missing from a
        # saved per-phone entry must default to False, not True. Otherwise
        # the settings UI renders them pre-checked for an existing phone and
        # saving the tab silently persists explicit grants — bypassing the
        # phone-gate's "explicit True only" execution semantics.
        saved = {"+79990001111": {"pin_message": True}}
        mock_db.get_setting = AsyncMock(return_value=json.dumps(saved))
        result = await load_tool_permissions(mock_db, phone="+79990001111")
        # phone-bound READ — must NOT default to True
        assert result["read_messages"] is False
        assert result["download_media"] is False
        assert result["get_participants"] is False
        assert result["get_broadcast_stats"] is False
        assert result["resolve_entity"] is False
        # non-phone-bound READ — keeps permissive default (DB-only, no
        # phone-gate, so the existing admin's DB work is not blocked)
        assert result["list_channels"] is True
        assert result["search_messages"] is True
        assert result["get_analytics_summary"] is True


# ---------------------------------------------------------------------------
# permissions.load_tool_permissions_all_phones
# ---------------------------------------------------------------------------


class TestLoadToolPermissionsAllPhones:
    async def test_no_setting_defaults_for_all(self, mock_db):
        mock_db.get_setting = AsyncMock(return_value=None)
        accounts = [SimpleNamespace(phone="+7111"), SimpleNamespace(phone="+7222")]
        result = await load_tool_permissions_all_phones(mock_db, accounts)
        assert set(result.keys()) == {"+7111", "+7222"}
        assert all(v is True for v in result["+7111"].values())

    async def test_per_phone_format(self, mock_db):
        # READ tools missing from a per-phone ACL still default to enabled;
        # WRITE/DELETE tools missing from a per-phone ACL default to disabled.
        saved = {"+7111": {"search_messages": False}, "+7222": {"leave_dialogs": False}}
        mock_db.get_setting = AsyncMock(return_value=json.dumps(saved))
        accounts = [SimpleNamespace(phone="+7111"), SimpleNamespace(phone="+7222")]
        result = await load_tool_permissions_all_phones(mock_db, accounts)
        assert result["+7111"]["search_messages"] is False
        assert result["+7111"]["list_channels"] is True  # READ missing → default True
        assert result["+7111"]["leave_dialogs"] is False  # DELETE missing → default False
        assert result["+7222"]["leave_dialogs"] is False
        assert result["+7222"]["list_channels"] is True

    async def test_legacy_flat_applies_to_all(self, mock_db):
        saved = {"search_messages": False}
        mock_db.get_setting = AsyncMock(return_value=json.dumps(saved))
        accounts = [SimpleNamespace(phone="+7111"), SimpleNamespace(phone="+7222")]
        result = await load_tool_permissions_all_phones(mock_db, accounts)
        assert result["+7111"]["search_messages"] is False
        assert result["+7222"]["search_messages"] is False

    async def test_per_phone_existing_account_phone_bound_read_disabled(self, mock_db):
        # Codex round 10 regression for the all-phones loader: an existing
        # phone with a sparse ACL must render phone-bound READ as disabled,
        # so opening + saving the settings tab cannot silently grant live
        # Telegram READ access (read_messages, download_media, …).
        saved = {"+7111": {"pin_message": True}}
        mock_db.get_setting = AsyncMock(return_value=json.dumps(saved))
        accounts = [SimpleNamespace(phone="+7111")]
        result = await load_tool_permissions_all_phones(mock_db, accounts)
        # phone-bound READ tools for +7111 — render fail-closed
        assert result["+7111"]["read_messages"] is False
        assert result["+7111"]["download_media"] is False
        assert result["+7111"]["get_participants"] is False
        assert result["+7111"]["resolve_entity"] is False
        # phone-bound WRITE explicitly granted
        assert result["+7111"]["pin_message"] is True
        # non-phone-bound READ stays permissive (DB-only)
        assert result["+7111"]["list_channels"] is True
        assert result["+7111"]["get_analytics_summary"] is True

    async def test_per_phone_unsaved_account_fully_fail_closed(self, mock_db):
        # An account absent from a per-phone ACL renders fully fail-closed
        # in the settings UI (Codex round 4). Showing READ pre-checked here
        # used to reopen the absent-phone live-READ leak: admin saving the
        # tab persisted READ=true and bypassed require_phone_permission's
        # absent-deny path. Admin must explicitly opt-in each tool per
        # account.
        saved = {"+7111": {"send_message": True, "leave_dialogs": True}}
        mock_db.get_setting = AsyncMock(return_value=json.dumps(saved))
        accounts = [
            SimpleNamespace(phone="+7111"),
            SimpleNamespace(phone="+7999"),  # not in saved ACL
        ]
        result = await load_tool_permissions_all_phones(mock_db, accounts)
        # Existing account keeps its explicit grants
        assert result["+7111"]["send_message"] is True
        assert result["+7111"]["leave_dialogs"] is True
        # Unsaved account: every tool denied — no pre-checked surprises in UI
        assert all(v is False for v in result["+7999"].values())


# ---------------------------------------------------------------------------
# permissions.load_tool_permissions_union
# ---------------------------------------------------------------------------


class TestLoadToolPermissionsUnion:
    async def test_no_setting_returns_defaults(self, mock_db):
        mock_db.get_setting = AsyncMock(return_value=None)
        result = await load_tool_permissions_union(mock_db)
        assert all(v is True for v in result.values())

    async def test_flat_format(self, mock_db):
        saved = {"search_messages": False}
        mock_db.get_setting = AsyncMock(return_value=json.dumps(saved))
        result = await load_tool_permissions_union(mock_db)
        assert result["search_messages"] is False
        assert result["list_channels"] is True

    async def test_per_phone_union_any_allows(self, mock_db):
        saved = {
            "+7111": {"send_photos_now": False, "leave_dialogs": False},
            "+7222": {"send_photos_now": True, "leave_dialogs": False},
        }
        mock_db.get_setting = AsyncMock(return_value=json.dumps(saved))
        result = await load_tool_permissions_union(mock_db)
        assert result["send_photos_now"] is True  # +7222 allows
        assert result["leave_dialogs"] is False  # both deny

    async def test_per_phone_all_deny(self, mock_db):
        saved = {
            "+7111": {"send_photos_now": False},
            "+7222": {"send_photos_now": False},
        }
        mock_db.get_setting = AsyncMock(return_value=json.dumps(saved))
        result = await load_tool_permissions_union(mock_db)
        assert result["send_photos_now"] is False

    async def test_per_phone_union_phone_binded_missing_keys_false(self, mock_db):
        # Phone-bound tools must follow require_phone_permission's "explicit
        # True only" semantics in the union (Codex round 5). Otherwise the
        # agent advertises tools the runtime gate will deny — e.g. resolve_entity
        # added after the ACL was saved would appear available but every call
        # would be blocked.
        saved = {
            "+7111": {"search_messages": True},
            "+7222": {"pin_message": True},
        }
        mock_db.get_setting = AsyncMock(return_value=json.dumps(saved))
        result = await load_tool_permissions_union(mock_db)
        # Explicit grants pass through
        assert result["pin_message"] is True  # phone-bound, explicit
        # Phone-bound tools without an explicit True anywhere are hidden
        assert result["send_reaction"] is False  # phone-bound WRITE
        assert result["leave_dialogs"] is False  # phone-bound DELETE
        assert result["resolve_entity"] is False  # phone-bound READ (Codex round 5)
        assert result["read_messages"] is False  # phone-bound READ
        # Non-phone-bound tools keep permissive defaults under per-phone ACLs:
        # DB-only READ has no phone-gate to deny it.
        assert result["list_channels"] is True  # not phone-bound
        assert result["get_analytics_summary"] is True  # not phone-bound

    async def test_missing_write_key_in_legacy_flat_false(self, mock_db):
        # Same fail-closed semantics for legacy flat ACL.
        saved = {"search_messages": True}
        mock_db.get_setting = AsyncMock(return_value=json.dumps(saved))
        result = await load_tool_permissions_union(mock_db)
        assert result["search_messages"] is True
        assert result["send_reaction"] is False
        assert result["leave_dialogs"] is False
        assert result["list_channels"] is True

    async def test_resolve_entity_hidden_for_legacy_per_phone_acl_without_it(self, mock_db):
        # Codex round 5 regression: an ACL saved before resolve_entity was
        # registered must NOT advertise resolve_entity through the union — the
        # phone-gate would deny every call. visibility tracks execution.
        saved = {
            "+7111": {"pin_message": True, "search_messages": True},
            "+7222": {"send_message": True},
        }
        mock_db.get_setting = AsyncMock(return_value=json.dumps(saved))
        result = await load_tool_permissions_union(mock_db)
        assert result["resolve_entity"] is False
        # Other phone-bound READs missing from saved are also hidden
        assert result["read_messages"] is False
        assert result["download_media"] is False
        assert result["get_participants"] is False


# ---------------------------------------------------------------------------
# permissions.save_tool_permissions
# ---------------------------------------------------------------------------


class TestSaveToolPermissions:
    async def test_flat_save(self, mock_db):
        mock_db.set_setting = AsyncMock()
        perms = {"search_messages": True, "leave_dialogs": False}
        await save_tool_permissions(mock_db, perms, phone=None)
        mock_db.set_setting.assert_awaited_once()
        saved_json = mock_db.set_setting.await_args[0][1]
        assert json.loads(saved_json) == perms

    async def test_per_phone_new_entry(self, mock_db):
        mock_db.get_setting = AsyncMock(return_value=json.dumps({"+7111": {"a": True}}))
        mock_db.get_accounts = AsyncMock(return_value=[
            SimpleNamespace(phone="+7111"), SimpleNamespace(phone="+7222"),
        ])
        mock_db.set_setting = AsyncMock()
        await save_tool_permissions(mock_db, {"b": True}, phone="+7222")
        saved = json.loads(mock_db.set_setting.await_args[0][1])
        assert saved["+7111"] == {"a": True}
        assert saved["+7222"] == {"b": True}

    async def test_per_phone_overwrite(self, mock_db):
        mock_db.get_setting = AsyncMock(return_value=json.dumps({"+7111": {"a": True}}))
        mock_db.get_accounts = AsyncMock(return_value=[SimpleNamespace(phone="+7111")])
        mock_db.set_setting = AsyncMock()
        await save_tool_permissions(mock_db, {"a": False}, phone="+7111")
        saved = json.loads(mock_db.set_setting.await_args[0][1])
        assert saved["+7111"] == {"a": False}

    async def test_per_phone_does_not_materialize_unsaved_accounts(self, mock_db):
        """Saving for one phone must NOT seed entries for other accounts.

        Materializing absent phones at save time used to reopen the
        absent-phone leak (Codex round 4): a previously-denied phone would
        become present in saved with READ=true and bypass the per-phone
        absent-deny path in require_phone_permission. Admin must explicitly
        open each account's tab and save it to grant any access.
        """
        mock_db.get_setting = AsyncMock(return_value=None)
        mock_db.get_accounts = AsyncMock(return_value=[
            SimpleNamespace(phone="+7111"), SimpleNamespace(phone="+7222"),
        ])
        mock_db.set_setting = AsyncMock()
        await save_tool_permissions(mock_db, {"send_photos_now": False}, phone="+7111")
        saved = json.loads(mock_db.set_setting.await_args[0][1])
        # Only +7111 was materialized; +7222 stays absent → require_phone_permission denies it.
        assert list(saved.keys()) == ["+7111"]
        assert saved["+7111"] == {"send_photos_now": False}

    async def test_per_phone_migrates_legacy_flat(self, mock_db):
        mock_db.get_setting = AsyncMock(return_value=json.dumps({"search_messages": True}))
        mock_db.get_accounts = AsyncMock(return_value=[SimpleNamespace(phone="+7111")])
        mock_db.set_setting = AsyncMock()
        await save_tool_permissions(mock_db, {"leave_dialogs": False}, phone="+7111")
        saved = json.loads(mock_db.set_setting.await_args[0][1])
        # Legacy flat discarded, only per-phone entry remains
        assert "+7111" in saved
        assert "search_messages" not in saved

    async def test_save_does_not_grant_live_read_to_absent_phone(self, mock_db):
        """End-to-end regression for Codex round 4: saving permissions for
        phone A must NOT silently grant live-READ access to phone B.

        Before this fix, save_tool_permissions seeded every account absent
        from saved with READ=true defaults, so an admin saving a restrictive
        ACL for phone A would inadvertently materialize phone B with
        read_messages=true, defeating require_phone_permission's
        absent-phone deny path.
        """
        # Start with empty ACL (no saved setting)
        stored: dict[str, str] = {}

        async def fake_get_setting(key: str) -> str | None:
            return stored.get(key)

        async def fake_set_setting(key: str, value: str) -> None:
            stored[key] = value

        mock_db.get_setting = AsyncMock(side_effect=fake_get_setting)
        mock_db.set_setting = AsyncMock(side_effect=fake_set_setting)
        mock_db.get_accounts = AsyncMock(return_value=[
            SimpleNamespace(phone="+7111"), SimpleNamespace(phone="+7222"),
        ])
        # Admin saves a restrictive ACL for +7111
        await save_tool_permissions(mock_db, {"pin_message": True}, phone="+7111")
        # +7222 was NOT touched — it must still be denied for live READ
        result = await require_phone_permission(mock_db, "+7222", "read_messages")
        assert result is not None
        assert "не разрешён" in _text(result)
        result_dl = await require_phone_permission(mock_db, "+7222", "download_media")
        assert result_dl is not None
        assert "не разрешён" in _text(result_dl)


# ---------------------------------------------------------------------------
# permissions.filter_allowed_tools
# ---------------------------------------------------------------------------


class TestFilterAllowedTools:
    def test_all_allowed(self):
        tools = ["mcp__telegram_db__search_messages", "mcp__telegram_db__list_channels"]
        perms = {"search_messages": True, "list_channels": True}
        assert filter_allowed_tools(tools, perms) == tools

    def test_one_disabled(self):
        tools = ["mcp__telegram_db__search_messages", "mcp__telegram_db__list_channels"]
        perms = {"search_messages": False, "list_channels": True}
        assert filter_allowed_tools(tools, perms) == ["mcp__telegram_db__list_channels"]

    def test_all_disabled(self):
        tools = ["mcp__telegram_db__search_messages", "mcp__telegram_db__list_channels"]
        perms = {"search_messages": False, "list_channels": False}
        assert filter_allowed_tools(tools, perms) == []

    def test_unknown_tool_denied(self):
        tools = ["mcp__telegram_db__unknown_tool"]
        perms = {"search_messages": True}
        assert filter_allowed_tools(tools, perms) == []

    def test_strips_mcp_prefix(self):
        tools = ["mcp__telegram_db__leave_dialogs"]
        perms = {"leave_dialogs": True}
        assert filter_allowed_tools(tools, perms) == ["mcp__telegram_db__leave_dialogs"]


# ---------------------------------------------------------------------------
# permissions._is_per_phone_format
# ---------------------------------------------------------------------------


class TestIsPerPhoneFormat:
    def test_empty_dict(self):
        assert _is_per_phone_format({}) is False

    def test_flat_format(self):
        assert _is_per_phone_format({"search_messages": True}) is False

    def test_per_phone_format(self):
        assert _is_per_phone_format({"+7111": {"search_messages": True}}) is True


# ---------------------------------------------------------------------------
# permissions.build_template_context
# ---------------------------------------------------------------------------


class TestBuildTemplateContext:
    def test_categories_keys(self):
        perms = {name: True for name in TOOL_CATEGORIES}
        ctx = build_template_context(perms)
        assert set(ctx["tool_permission_categories"].keys()) == {"read", "write", "delete"}
        for cat_list in ctx["tool_permission_categories"].values():
            assert len(cat_list) > 0

    def test_modules_non_empty(self):
        perms = {name: True for name in TOOL_CATEGORIES}
        ctx = build_template_context(perms)
        assert len(ctx["tool_permission_modules"]) > 0

    def test_disabled_tool_reflected(self):
        perms = {name: True for name in TOOL_CATEGORIES}
        perms["leave_dialogs"] = False
        ctx = build_template_context(perms)
        # Find leave_dialogs in categories
        delete_tools = ctx["tool_permission_categories"]["delete"]
        leave = next(t for t in delete_tools if t["name"] == "leave_dialogs")
        assert leave["enabled"] is False

    def test_dialogs_module_uses_canonical_name(self):
        perms = {name: True for name in TOOL_CATEGORIES}
        ctx = build_template_context(perms)
        module_names = {module["name"] for module in ctx["tool_permission_modules"]}
        assert "Диалоги" in module_names
        assert "Мой Telegram" not in module_names


# ---------------------------------------------------------------------------
# permissions.get_all_allowed_tools
# ---------------------------------------------------------------------------


class TestGetAllAllowedTools:
    """Ensure get_all_allowed_tools() derives from TOOL_CATEGORIES."""

    def test_length_matches_categories(self):
        from src.agent.tools.permissions import get_all_allowed_tools

        assert len(get_all_allowed_tools()) == len(TOOL_CATEGORIES)

    def test_all_prefixed(self):
        from src.agent.tools.permissions import BUILTIN_TOOLS, get_all_allowed_tools

        for tool in get_all_allowed_tools():
            if tool in BUILTIN_TOOLS:
                assert tool in TOOL_CATEGORIES, f"{tool} not in TOOL_CATEGORIES"
            else:
                assert tool.startswith(MCP_PREFIX), f"{tool} missing prefix"
                bare = tool.removeprefix(MCP_PREFIX)
                assert bare in TOOL_CATEGORIES, f"{bare} not in TOOL_CATEGORIES"


# ---------------------------------------------------------------------------
# End-to-end: 3-layer permission chain on leave_dialogs
# ---------------------------------------------------------------------------


def _get_tool_handlers(mock_db, client_pool=None, config=None):
    captured_tools = []
    with patch(
        "src.agent.tools.create_sdk_mcp_server",
        side_effect=lambda **kwargs: captured_tools.extend(kwargs.get("tools", [])),
    ):
        from src.agent.tools import make_mcp_server

        make_mcp_server(mock_db, client_pool=client_pool, config=config)
    return {t.name: t.handler for t in captured_tools}


class TestEndToEndPermissionFlow:
    """Integration tests: pool gate → phone permission → confirmation → business logic."""

    async def test_allowed_and_confirmed(self, mock_db):
        mock_db.get_setting = AsyncMock(return_value=None)
        pool = MagicMock()
        pool.leave_channels = AsyncMock(return_value={123: True})
        handlers = _get_tool_handlers(mock_db, client_pool=pool)
        result = await handlers["leave_dialogs"]({
            "phone": "+79990001111",
            "dialog_ids": "123",
            "confirm": True,
        })
        assert "покинут" in _text(result)

    async def test_no_confirm_returns_warning(self, mock_db):
        mock_db.get_setting = AsyncMock(return_value=None)
        pool = MagicMock()
        handlers = _get_tool_handlers(mock_db, client_pool=pool)
        result = await handlers["leave_dialogs"]({
            "phone": "+79990001111",
            "dialog_ids": "123",
        })
        assert "Подтвердите" in _text(result)

    async def test_retry_with_confirm_succeeds(self, mock_db):
        mock_db.get_setting = AsyncMock(return_value=None)
        pool = MagicMock()
        handlers = _get_tool_handlers(mock_db, client_pool=pool)
        # First call — no confirm
        r1 = await handlers["leave_dialogs"]({"phone": "+79990001111", "dialog_ids": "123"})
        assert "Подтвердите" in _text(r1)
        # Retry with confirm
        pool.leave_channels = AsyncMock(return_value={123: True})
        r2 = await handlers["leave_dialogs"]({
            "phone": "+79990001111",
            "dialog_ids": "123",
            "confirm": True,
        })
        assert "покинут" in _text(r2)

    async def test_phone_not_allowed(self, mock_db):
        # Phone explicitly set to False in perms should be denied
        perms = {
            "+79990001111": {"leave_dialogs": False},
            "+79990002222": {"leave_dialogs": True},
        }
        mock_db.get_setting = AsyncMock(return_value=json.dumps(perms))
        pool = MagicMock()
        handlers = _get_tool_handlers(mock_db, client_pool=pool)
        result = await handlers["leave_dialogs"]({
            "phone": "+79990001111",
            "dialog_ids": "123",
            "confirm": True,
        })
        assert "не разрешён" in _text(result)

    async def test_no_pool_cli_mode(self, mock_db):
        handlers = _get_tool_handlers(mock_db, client_pool=None)
        result = await handlers["leave_dialogs"]({
            "phone": "+79990001111",
            "dialog_ids": "123",
            "confirm": True,
        })
        assert "требует Telegram-клиент" in _text(result)


# ---------------------------------------------------------------------------
# Static registry completeness: every tool that calls require_phone_permission
# (directly or via prepare_telegram_tool) must be classified in
# TOOL_CATEGORIES, otherwise it cannot be enabled through the settings UI and
# the fail-closed deny path locks admins out (Codex round 4 finding).
# ---------------------------------------------------------------------------


def _collect_phone_binded_tool_names() -> set[str]:
    """Scan src/agent/tools for tool names passed to phone-binded gates."""
    import re as _re
    from pathlib import Path as _Path

    root = _Path(__file__).resolve().parent.parent / "src" / "agent" / "tools"
    patterns = [
        _re.compile(r'require_phone_permission\([^)]*?["\']([a-zA-Z_]+)["\']', _re.DOTALL),
        _re.compile(r'prepare_telegram_tool\([^)]*?tool_name=["\']([a-zA-Z_]+)["\']', _re.DOTALL),
    ]
    found: set[str] = set()
    for path in root.rglob("*.py"):
        text = path.read_text(encoding="utf-8")
        for pat in patterns:
            for match in pat.finditer(text):
                found.add(match.group(1))
    return found


def test_every_phone_binded_tool_is_registered_in_tool_categories():
    """Each tool that calls require_phone_permission / prepare_telegram_tool
    must appear in TOOL_CATEGORIES.  Otherwise the absent-from-ACL deny path
    locks it out and the settings UI offers no toggle to re-enable it.
    Codex round 4 caught resolve_entity in exactly this state.
    """
    binded = _collect_phone_binded_tool_names()
    missing = binded - set(TOOL_CATEGORIES.keys())
    assert not missing, (
        f"These phone-binded tools are missing from TOOL_CATEGORIES "
        f"(add them with the right ToolCategory + a MODULE_GROUPS entry): {sorted(missing)}"
    )


def test_phone_binded_tools_constant_matches_static_scan():
    """The PHONE_BINDED_TOOLS constant in permissions.py drives the union
    visibility logic and MUST match what the static scan finds in
    src/agent/tools.  If they drift, the agent will either advertise tools
    that the runtime phone-gate denies (Codex round 5 finding) or hide
    callable ones from the agent.
    """
    from src.agent.tools.permissions import PHONE_BINDED_TOOLS

    scanned = _collect_phone_binded_tool_names()
    extra_in_constant = set(PHONE_BINDED_TOOLS) - scanned
    missing_from_constant = scanned - set(PHONE_BINDED_TOOLS)
    assert not extra_in_constant, (
        f"PHONE_BINDED_TOOLS lists tools no longer phone-bound in source: "
        f"{sorted(extra_in_constant)}"
    )
    assert not missing_from_constant, (
        f"PHONE_BINDED_TOOLS is missing newly phone-bound tools: "
        f"{sorted(missing_from_constant)}. Add them to the constant in "
        f"src/agent/tools/permissions.py so the union visibility matches the gate."
    )
