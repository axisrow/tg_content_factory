"""Tests for dialogs CLI and web action paths."""
# Web-panel tests split out of tests/test_dialogs_cli_and_web_actions.py (#1720 wave 3).


from __future__ import annotations

from unittest.mock import MagicMock

import pytest
from webui_helpers import build_web_app, make_auth_client

from tg_agent.config import AppConfig

# ---------------------------------------------------------------------------
# Shared helpers
# ---------------------------------------------------------------------------

_PHONE = "+79001234567"

_SVC_DIALOGS = "tg_agent.services.channel_service.ChannelService.get_my_dialogs"

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
                iter_dialogs_factory=lambda *a, **kw: AsyncIterMessages([]),
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

    @pytest.mark.anyio
    async def test_page_renders(self, web_client):
        c, app = web_client
        resp = await c.get("/dialogs/")
        assert resp.status_code == 200

    @pytest.mark.anyio
    async def test_page_with_phone(self, web_client):
        c, app = web_client
        phone_encoded = "%2B79001234567"
        resp = await c.get(f"/dialogs/?phone={phone_encoded}")
        assert resp.status_code == 200

    @pytest.mark.anyio
    async def test_page_invalid_phone_shows_no_dialogs(self, web_client):
        c, app = web_client
        resp = await c.get("/dialogs/?phone=%2B00000")
        assert resp.status_code == 200


class TestWebRefresh:
    """POST /dialogs/refresh."""

    @pytest.mark.anyio
    async def test_refresh_ok(self, web_client):
        c, app = web_client
        resp = await c.post("/dialogs/refresh", data={"phone": _PHONE})
        assert resp.status_code == 200


class TestWebCacheStatus:
    """GET /dialogs/cache-status."""

    @pytest.mark.anyio
    async def test_cache_status_empty(self, web_client):
        c, app = web_client
        resp = await c.get("/dialogs/cache-status")
        assert resp.status_code == 200
        data = resp.json()
        assert isinstance(data, list)


class TestWebCacheClear:
    """POST /dialogs/cache-clear."""

    @pytest.mark.anyio
    async def test_cache_clear_all(self, web_client):
        c, app = web_client
        resp = await c.post("/dialogs/cache-clear", data={"phone": ""})
        assert resp.status_code == 200

    @pytest.mark.anyio
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

    @pytest.mark.anyio
    async def test_send_missing_fields(self, web_client):
        c, app = web_client
        resp = await c.post("/dialogs/send", data={"phone": _PHONE, "recipient": "", "text": ""})
        assert resp.status_code == 200
        assert "error=missing_fields" in str(resp.url)

    @pytest.mark.anyio
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

    @pytest.mark.anyio
    async def test_edit_missing_fields(self, web_client):
        c, app = web_client
        resp = await c.post("/dialogs/edit-message", data={
            "phone": _PHONE, "chat_id": "", "message_id": "", "text": "",
        })
        assert "error=missing_fields" in str(resp.url)

    @pytest.mark.anyio
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

    @pytest.mark.anyio
    async def test_delete_missing_fields(self, web_client):
        c, app = web_client
        resp = await c.post("/dialogs/delete-message", data={
            "phone": _PHONE, "chat_id": "", "message_ids": "",
        })
        assert "error=missing_fields" in str(resp.url)

    @pytest.mark.anyio
    async def test_delete_invalid_ids(self, web_client):
        c, app = web_client
        resp = await c.post("/dialogs/delete-message", data={
            "phone": _PHONE, "chat_id": "@ch", "message_ids": "abc",
        })
        assert "error=invalid_ids" in str(resp.url)

    @pytest.mark.anyio
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

    @pytest.mark.anyio
    async def test_forward_missing_fields(self, web_client):
        c, app = web_client
        resp = await c.post("/dialogs/forward-messages", data={
            "phone": _PHONE, "from_chat": "", "to_chat": "", "message_ids": "",
        })
        assert "error=missing_fields" in str(resp.url)

    @pytest.mark.anyio
    async def test_forward_invalid_ids(self, web_client):
        c, app = web_client
        resp = await c.post("/dialogs/forward-messages", data={
            "phone": _PHONE, "from_chat": "@a", "to_chat": "@b", "message_ids": "xyz",
        })
        assert "error=invalid_ids" in str(resp.url)

    @pytest.mark.anyio
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

    @pytest.mark.anyio
    async def test_pin_missing_fields(self, web_client):
        c, app = web_client
        resp = await c.post("/dialogs/pin-message", data={
            "phone": "", "chat_id": "", "message_id": "",
        })
        assert "error=missing_fields" in str(resp.url)

    @pytest.mark.anyio
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

    @pytest.mark.anyio
    async def test_unpin_missing_fields(self, web_client):
        c, app = web_client
        resp = await c.post("/dialogs/unpin-message", data={
            "phone": "", "chat_id": "",
        })
        assert "error=missing_fields" in str(resp.url)

    @pytest.mark.anyio
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

    @pytest.mark.anyio
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

    @pytest.mark.anyio
    async def test_participants_missing_params(self, web_client):
        c, app = web_client
        resp = await c.get("/dialogs/participants")
        assert resp.status_code == 400

    @pytest.mark.anyio
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


class TestWebReadHistory:
    """GET /dialogs/history — queued-command model, snapshot-first like participants."""

    @pytest.mark.anyio
    async def test_read_history_missing_params(self, web_client):
        c, app = web_client
        resp = await c.get("/dialogs/history")
        assert resp.status_code == 400

    @pytest.mark.anyio
    async def test_read_history_enqueues_when_no_snapshot(self, web_client):
        c, app = web_client
        phone_enc = _PHONE.replace("+", "%2B")
        resp = await c.get(f"/dialogs/history?phone={phone_enc}&chat_id=@ch")
        assert resp.status_code == 202
        body = resp.json()
        assert body.get("status") == "queued"
        assert "command_id" in body
        await _assert_enqueued(app, "dialogs.read_history", {"phone": _PHONE, "chat_id": "@ch"})

    @pytest.mark.anyio
    async def test_read_history_returns_cached_snapshot(self, web_client):
        c, app = web_client
        from tg_agent.models import RuntimeSnapshot

        payload = {"messages": [{"id": 1, "text": "hi"}], "total": 1, "dialog_id": -100777}
        await app.state.db.repos.runtime_snapshots.upsert_snapshot(
            RuntimeSnapshot(
                snapshot_type="dialogs_history",
                scope=f"dialogs_history:{_PHONE}:@ch:0",
                payload=payload,
            )
        )
        phone_enc = _PHONE.replace("+", "%2B")
        resp = await c.get(f"/dialogs/history?phone={phone_enc}&chat_id=@ch")
        assert resp.status_code == 200
        assert resp.json() == payload


class TestWebArchive:
    """POST /dialogs/archive and /unarchive — queued-command model."""

    @pytest.mark.anyio
    async def test_archive_missing_fields(self, web_client):
        c, app = web_client
        resp = await c.post("/dialogs/archive", data={"phone": "", "chat_id": ""})
        assert "error=missing_fields" in str(resp.url)

    @pytest.mark.anyio
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

    @pytest.mark.anyio
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

    @pytest.mark.anyio
    async def test_unarchive_missing_fields(self, web_client):
        c, app = web_client
        resp = await c.post("/dialogs/unarchive", data={"phone": "", "chat_id": ""})
        assert "error=missing_fields" in str(resp.url)


class TestWebMarkRead:
    """POST /dialogs/mark-read — queued-command model."""

    @pytest.mark.anyio
    async def test_mark_read_missing_fields(self, web_client):
        c, app = web_client
        resp = await c.post("/dialogs/mark-read", data={"phone": "", "chat_id": ""})
        assert "error=missing_fields" in str(resp.url)

    @pytest.mark.anyio
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

    @pytest.mark.anyio
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

    @pytest.mark.anyio
    async def test_edit_admin_missing_fields(self, web_client):
        c, app = web_client
        resp = await c.post("/dialogs/edit-admin", data={
            "phone": "", "chat_id": "", "user_id": "",
        })
        assert "error=missing_fields" in str(resp.url)

    @pytest.mark.anyio
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

    @pytest.mark.anyio
    async def test_edit_permissions_no_flags(self, web_client):
        c, app = web_client
        resp = await c.post("/dialogs/edit-permissions", data={
            "phone": _PHONE, "chat_id": "@ch", "user_id": "@u",
        })
        assert "error=no_permission_flags" in str(resp.url)

    @pytest.mark.anyio
    async def test_edit_permissions_missing_fields(self, web_client):
        c, app = web_client
        resp = await c.post("/dialogs/edit-permissions", data={
            "phone": "", "chat_id": "", "user_id": "", "send_messages": "true",
        })
        assert "error=missing_fields" in str(resp.url)

    @pytest.mark.anyio
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

    @pytest.mark.anyio
    async def test_kick_missing_fields(self, web_client):
        c, app = web_client
        resp = await c.post("/dialogs/kick", data={
            "phone": "", "chat_id": "", "user_id": "",
        })
        assert "error=missing_fields" in str(resp.url)

    @pytest.mark.anyio
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

    @pytest.mark.anyio
    async def test_broadcast_stats_missing_params(self, web_client):
        c, app = web_client
        resp = await c.get("/dialogs/broadcast-stats")
        assert resp.status_code == 400

    @pytest.mark.anyio
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

    @pytest.mark.anyio
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

    @pytest.mark.anyio
    async def test_download_missing_fields(self, web_client):
        c, app = web_client
        resp = await c.post("/dialogs/download-media", data={
            "phone": "", "chat_id": "", "message_id": "",
        })
        assert "error=missing_fields" in str(resp.url)

    @pytest.mark.anyio
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

    @pytest.mark.anyio
    async def test_create_channel_page(self, web_client):
        c, app = web_client
        resp = await c.get("/dialogs/create-channel")
        assert resp.status_code == 200

    @pytest.mark.anyio
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
