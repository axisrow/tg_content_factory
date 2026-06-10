"""Tests for account management routes."""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.models import Account
from src.security import SessionCipher


@pytest.mark.anyio
async def test_toggle_account_enqueues_command(route_client, base_app):
    """Web toggle only enqueues `accounts.toggle`; worker reconciles the pool."""
    app, db, pool = base_app
    accounts = await db.get_accounts(active_only=False)
    assert len(accounts) > 0
    acc = accounts[0]

    resp = await route_client.post(f"/settings/{acc.id}/toggle", follow_redirects=False)
    assert resp.status_code in (303, 302)
    location = resp.headers.get("location", "")
    assert "/settings" in location
    assert "account_toggle_queued" in location
    assert "command_id=" in location

    if hasattr(pool, "add_client"):
        pool.add_client.assert_not_called()
    pool.remove_client.assert_not_called()

    commands = await db.repos.telegram_commands.list_commands(limit=1)
    assert commands[0].command_type == "accounts.toggle"
    assert commands[0].payload == {"account_id": acc.id}


@pytest.mark.anyio
async def test_delete_account_removes_row_and_enqueues_cleanup(route_client, base_app):
    """Web delete removes the DB row immediately and enqueues live pool cleanup."""
    app, db, pool = base_app
    await db.add_account(Account(phone="+9999999999", session_string="session_del"))
    accounts = await db.get_accounts(active_only=False)
    to_delete = next(a for a in accounts if a.phone == "+9999999999")

    resp = await route_client.post(f"/settings/{to_delete.id}/delete", follow_redirects=False)
    assert resp.status_code in (303, 302)
    location = resp.headers.get("location", "")
    assert "/settings" in location
    assert "account_deleted" in location
    assert "command_id=" in location

    pool.remove_client.assert_not_called()
    remaining = await db.get_accounts(active_only=False)
    assert not any(a.phone == "+9999999999" for a in remaining)

    commands = await db.repos.telegram_commands.list_commands(limit=1)
    assert commands[0].command_type == "accounts.delete"
    assert commands[0].payload == {"account_id": to_delete.id, "phone": "+9999999999"}


@pytest.mark.anyio
async def test_delete_primary_account_promotes_next_visible_account(route_client, base_app):
    """Deleting the primary account updates the remaining visible primary immediately."""
    _, db, _ = base_app
    assert db.db is not None
    await db.db.execute("UPDATE accounts SET is_primary = 1 WHERE phone = ?", ("+1234567890",))
    await db.db.commit()
    await db.add_account(Account(phone="+9999999997", session_string="session_next"))
    await db.add_account(Account(phone="+9999999996", session_string="session_later"))

    accounts = await db.get_account_summaries(active_only=False)
    to_delete = next(a for a in accounts if a.phone == "+1234567890")

    resp = await route_client.post(f"/settings/{to_delete.id}/delete", follow_redirects=False)
    assert resp.status_code in (303, 302)
    assert "account_deleted" in resp.headers.get("location", "")

    remaining = await db.get_account_summaries(active_only=False)
    assert not any(a.id == to_delete.id for a in remaining)
    assert remaining[0].phone == "+9999999997"
    assert remaining[0].is_primary is True
    assert all(a.is_primary is False for a in remaining[1:])


@pytest.mark.anyio
async def test_delete_account_works_when_session_key_is_wrong(route_client, base_app):
    """Deleting an account is a recovery action and must not require session decryption."""
    _, db, _ = base_app
    encrypted = SessionCipher("correct-session-key").encrypt("session_del")
    await db.add_account(Account(phone="+9999999998", session_string=encrypted, is_primary=True))
    db._accounts._session_cipher = SessionCipher("wrong-session-key")

    accounts = await db.get_account_summaries(active_only=False)
    to_delete = next(a for a in accounts if a.phone == "+9999999998")

    resp = await route_client.post(f"/settings/{to_delete.id}/delete", follow_redirects=False)
    assert resp.status_code in (303, 302)
    assert "account_deleted" in resp.headers.get("location", "")

    remaining = await db.get_account_summaries(active_only=False)
    assert not any(a.phone == "+9999999998" for a in remaining)
    promoted = next(a for a in remaining if a.phone == "+1234567890")
    assert promoted.is_primary is True

    commands = await db.repos.telegram_commands.list_commands(limit=1)
    assert commands[0].command_type == "accounts.delete"
    assert commands[0].payload == {"account_id": to_delete.id, "phone": "+9999999998"}


@pytest.mark.anyio
async def test_delete_notification_account_with_explicit_replacement(route_client, base_app):
    """Deleting the notification account with notify_to reassigns the setting."""
    _, db, _ = base_app
    await db.add_account(Account(phone="+9999999991", session_string="s1"))
    await db.add_account(Account(phone="+9999999992", session_string="s2"))
    await db.set_setting("notification_account_phone", "+9999999991")

    accounts = await db.get_account_summaries(active_only=False)
    to_delete = next(a for a in accounts if a.phone == "+9999999991")

    resp = await route_client.post(
        f"/settings/{to_delete.id}/delete",
        data={"notify_to": "+9999999992"},
        follow_redirects=False,
    )
    assert resp.status_code in (303, 302)
    assert "account_deleted_notify_reassigned" in resp.headers.get("location", "")
    assert await db.get_setting("notification_account_phone") == "+9999999992"


@pytest.mark.anyio
async def test_delete_notification_account_auto_reassigns_to_single_remaining(route_client, base_app):
    """With exactly one other account left, the setting moves to it automatically."""
    _, db, _ = base_app
    await db.add_account(Account(phone="+9999999991", session_string="s1"))
    await db.set_setting("notification_account_phone", "+9999999991")

    accounts = await db.get_account_summaries(active_only=False)
    to_delete = next(a for a in accounts if a.phone == "+9999999991")

    resp = await route_client.post(f"/settings/{to_delete.id}/delete", follow_redirects=False)
    assert resp.status_code in (303, 302)
    assert "account_deleted_notify_reassigned" in resp.headers.get("location", "")
    assert await db.get_setting("notification_account_phone") == "+1234567890"


@pytest.mark.anyio
async def test_delete_notification_account_clears_setting_without_choice(route_client, base_app):
    """Several accounts remain but no notify_to given — setting falls back to Primary."""
    _, db, _ = base_app
    await db.add_account(Account(phone="+9999999991", session_string="s1"))
    await db.add_account(Account(phone="+9999999992", session_string="s2"))
    await db.set_setting("notification_account_phone", "+9999999991")

    accounts = await db.get_account_summaries(active_only=False)
    to_delete = next(a for a in accounts if a.phone == "+9999999991")

    resp = await route_client.post(f"/settings/{to_delete.id}/delete", follow_redirects=False)
    assert resp.status_code in (303, 302)
    assert "account_deleted_notify_cleared" in resp.headers.get("location", "")
    assert (await db.get_setting("notification_account_phone") or "") == ""


@pytest.mark.anyio
async def test_delete_notification_account_invalid_replacement_aborts(route_client, base_app):
    """Unknown notify_to phone aborts the deletion entirely."""
    _, db, _ = base_app
    await db.add_account(Account(phone="+9999999991", session_string="s1"))
    await db.add_account(Account(phone="+9999999992", session_string="s2"))
    await db.set_setting("notification_account_phone", "+9999999991")

    accounts = await db.get_account_summaries(active_only=False)
    to_delete = next(a for a in accounts if a.phone == "+9999999991")

    resp = await route_client.post(
        f"/settings/{to_delete.id}/delete",
        data={"notify_to": "+0000000000"},
        follow_redirects=False,
    )
    assert resp.status_code in (303, 302)
    assert "error=invalid_notify_account" in resp.headers.get("location", "")

    remaining = await db.get_account_summaries(active_only=False)
    assert any(a.phone == "+9999999991" for a in remaining)
    assert await db.get_setting("notification_account_phone") == "+9999999991"
    commands = await db.repos.telegram_commands.list_commands(limit=1)
    assert commands == []


@pytest.mark.anyio
async def test_delete_other_account_keeps_notification_setting(route_client, base_app):
    """Deleting a non-notification account must not touch the setting."""
    _, db, _ = base_app
    await db.add_account(Account(phone="+9999999991", session_string="s1"))
    await db.set_setting("notification_account_phone", "+1234567890")

    accounts = await db.get_account_summaries(active_only=False)
    to_delete = next(a for a in accounts if a.phone == "+9999999991")

    resp = await route_client.post(f"/settings/{to_delete.id}/delete", follow_redirects=False)
    assert resp.status_code in (303, 302)
    location = resp.headers.get("location", "")
    assert "msg=account_deleted&" in location
    assert await db.get_setting("notification_account_phone") == "+1234567890"


@pytest.mark.anyio
async def test_delete_notification_account_invalidates_notifier_cache(route_client, base_app):
    """Reassignment must drop the notifier's cached target; a kept setting must not."""
    app, db, _ = base_app
    notifier = MagicMock()
    app.state.notifier = notifier
    await db.add_account(Account(phone="+9999999991", session_string="s1"))
    await db.set_setting("notification_account_phone", "+9999999991")

    accounts = await db.get_account_summaries(active_only=False)
    to_delete = next(a for a in accounts if a.phone == "+9999999991")
    resp = await route_client.post(f"/settings/{to_delete.id}/delete", follow_redirects=False)
    assert resp.status_code in (303, 302)
    notifier.invalidate_me_cache.assert_called_once()

    notifier.invalidate_me_cache.reset_mock()
    await db.add_account(Account(phone="+9999999992", session_string="s2"))
    accounts = await db.get_account_summaries(active_only=False)
    other = next(a for a in accounts if a.phone == "+9999999992")
    resp = await route_client.post(f"/settings/{other.id}/delete", follow_redirects=False)
    assert resp.status_code in (303, 302)
    notifier.invalidate_me_cache.assert_not_called()


@pytest.mark.anyio
async def test_settings_page_renders_notify_reassign_choices(route_client, base_app):
    """Delete form of the notification account offers reassign choices when ≥2 others remain."""
    _, db, _ = base_app
    await db.add_account(Account(phone="+9999999991", session_string="s1"))
    await db.add_account(Account(phone="+9999999992", session_string="s2"))
    await db.set_setting("notification_account_phone", "+9999999991")

    resp = await route_client.get("/settings")
    assert resp.status_code == 200
    html = resp.text
    assert "data-notify-reassign" in html
    assert html.count("data-notify-reassign") == 2  # desktop table + mobile card
    assert 'name="notify_to"' in html
    # the deleted-account phone itself must not be offered as a replacement
    assert "+9999999992" in html


@pytest.mark.anyio
async def test_settings_page_no_reassign_attr_for_regular_accounts(route_client, base_app):
    """Without a notification account at risk the delete forms keep plain data-confirm."""
    _, db, _ = base_app
    await db.add_account(Account(phone="+9999999991", session_string="s1"))

    resp = await route_client.get("/settings")
    assert resp.status_code == 200
    assert "data-notify-reassign" not in resp.text


@pytest.mark.anyio
async def test_delete_account_missing_redirects_without_command(route_client, base_app):
    app, db, pool = base_app
    resp = await route_client.post("/settings/999999/delete", follow_redirects=False)
    assert resp.status_code in (303, 302)
    assert "error=invalid_account" in resp.headers.get("location", "")

    commands = await db.repos.telegram_commands.list_commands(limit=1)
    assert commands == []


@pytest.mark.anyio
async def test_flood_status_empty(route_client, base_app):
    """Flood status returns JSON with no active floods."""
    app, db, pool = base_app
    resp = await route_client.get("/settings/flood-status")
    assert resp.status_code == 200
    data = resp.json()
    assert isinstance(data, list)
    for item in data:
        assert "phone" in item
        assert "flood_wait_until" in item
        assert "remaining_seconds" in item


@pytest.mark.anyio
async def test_flood_status_active_flood(route_client, base_app):
    """Flood status shows active flood wait."""
    app, db, pool = base_app
    accounts = await db.get_accounts(active_only=False)
    acc = accounts[0]
    future = datetime.now(timezone.utc) + timedelta(hours=1)
    await db.update_account_flood(acc.phone, future)

    resp = await route_client.get("/settings/flood-status")
    assert resp.status_code == 200
    data = resp.json()
    flooded = [item for item in data if item["phone"] == acc.phone]
    assert len(flooded) == 1
    assert flooded[0]["flood_wait_until"] != "ok"
    assert flooded[0]["remaining_seconds"] > 0


@pytest.mark.anyio
async def test_flood_status_expired_flood(route_client, base_app):
    """Flood status shows ok for expired flood wait."""
    app, db, pool = base_app
    accounts = await db.get_accounts(active_only=False)
    acc = accounts[0]
    past = datetime.now(timezone.utc) - timedelta(hours=1)
    await db.update_account_flood(acc.phone, past)

    resp = await route_client.get("/settings/flood-status")
    assert resp.status_code == 200
    data = resp.json()
    entry = [item for item in data if item["phone"] == acc.phone]
    assert len(entry) == 1
    assert entry[0]["flood_wait_until"] == "ok"
    assert entry[0]["remaining_seconds"] == 0


@pytest.mark.anyio
async def test_flood_clear_success(route_client, base_app):
    """Flood clear resets flood wait."""
    app, db, pool = base_app
    accounts = await db.get_accounts(active_only=False)
    acc = accounts[0]
    future = datetime.now(timezone.utc) + timedelta(hours=1)
    await db.update_account_flood(acc.phone, future)

    resp = await route_client.post(f"/settings/{acc.id}/flood-clear", follow_redirects=False)
    assert resp.status_code in (303, 302)
    assert "/settings" in resp.headers.get("location", "")


@pytest.mark.anyio
async def test_flood_clear_not_found(route_client, base_app):
    """Flood clear for non-existent account redirects."""
    resp = await route_client.post("/settings/99999/flood-clear", follow_redirects=False)
    assert resp.status_code in (303, 302)
    assert "error=account_not_found" in resp.headers.get("location", "")


@pytest.mark.anyio
async def test_account_info_json(route_client, base_app):
    """GET /settings/{id}/info returns account summary and live diagnostics."""
    _, db, _ = base_app
    accounts = await db.get_account_summaries(active_only=False)
    acc = accounts[0]
    with patch("src.web.routes.accounts.get_live_account_info_text", AsyncMock(return_value="live ok")) as mock_live:
        resp = await route_client.get(f"/settings/{acc.id}/info")
    assert resp.status_code == 200
    data = resp.json()
    assert data["id"] == acc.id
    assert data["phone"] == acc.phone
    assert data["live_info"] == "live ok"
    assert "session_string" not in data
    runtime_arg, phone_arg = mock_live.await_args.args
    assert runtime_arg.db is db
    assert phone_arg == acc.phone


@pytest.mark.anyio
async def test_account_info_json_without_live_runtime(route_client, base_app):
    """GET /settings/{id}/info returns no-live diagnostic instead of failing."""
    _, db, _ = base_app
    accounts = await db.get_account_summaries(active_only=False)
    acc = accounts[0]
    with patch("src.web.routes.accounts.deps.get_pool", side_effect=RuntimeError("missing pool")):
        resp = await route_client.get(f"/settings/{acc.id}/info")

    assert resp.status_code == 200
    data = resp.json()
    assert data["id"] == acc.id
    assert "live Telegram runtime unavailable" in data["live_info"]
    assert "session_string" not in data


@pytest.mark.anyio
async def test_account_info_not_found(route_client, base_app):
    resp = await route_client.get("/settings/99999/info")
    assert resp.status_code == 404
