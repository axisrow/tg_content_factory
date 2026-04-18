"""Unit tests for TelegramCommandDispatcher handlers (path safety, session hygiene)."""

from __future__ import annotations

import asyncio
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.database import Database
from src.models import Account
from src.services import telegram_command_dispatcher as mod
from src.services.telegram_command_dispatcher import TelegramCommandDispatcher


class _FakeClient:
    def __init__(self, download_return: str | None):
        self._download_return = download_return
        self.last_file: str | None = None

    async def get_entity(self, chat_id):
        return MagicMock(id=chat_id)

    def iter_messages(self, entity, ids):
        msg = MagicMock()

        async def _gen():
            yield msg

        return _gen()

    async def download_media(self, msg, file: str):
        self.last_file = file
        return self._download_return


@pytest.mark.asyncio
async def test_download_media_creates_output_dir(tmp_path, monkeypatch):
    """output_dir must be created (mkdir) and returned path must be inside it."""
    db = Database(str(tmp_path / "test.db"))
    await db.initialize()
    try:
        root = tmp_path / "app"
        fake_dispatcher_py = root / "src" / "services" / "telegram_command_dispatcher.py"
        fake_dispatcher_py.parent.mkdir(parents=True)
        fake_dispatcher_py.touch()

        monkeypatch.setattr(mod, "__file__", str(fake_dispatcher_py))

        expected_dir = root / "data" / "downloads"
        expected_file = expected_dir / "file.jpg"
        # Pre-create the file; actual mkdir happens inside the handler.
        expected_dir.mkdir(parents=True)
        expected_file.touch()

        pool = MagicMock()
        pool.release_client = AsyncMock()
        dispatcher = TelegramCommandDispatcher(db, pool)
        dispatcher._get_client = AsyncMock(
            return_value=(_FakeClient(download_return=str(expected_file)), "+123")
        )

        result = await dispatcher._handle_dialogs_download_media(
            {"phone": "+123", "chat_id": 1, "message_id": 42}
        )
        assert Path(result["path"]).resolve() == expected_file.resolve()
        assert expected_dir.exists()
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_download_media_rejects_path_escape(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "test.db"))
    await db.initialize()
    try:
        root = tmp_path / "app"
        fake_dispatcher_py = root / "src" / "services" / "telegram_command_dispatcher.py"
        fake_dispatcher_py.parent.mkdir(parents=True)
        fake_dispatcher_py.touch()
        monkeypatch.setattr(mod, "__file__", str(fake_dispatcher_py))

        # Returned path lives OUTSIDE root/data/downloads.
        escape_file = tmp_path / "evil.bin"
        escape_file.touch()

        pool = MagicMock()
        pool.release_client = AsyncMock()
        dispatcher = TelegramCommandDispatcher(db, pool)
        dispatcher._get_client = AsyncMock(
            return_value=(_FakeClient(download_return=str(escape_file)), "+123")
        )

        with pytest.raises(RuntimeError, match="path_escape"):
            await dispatcher._handle_dialogs_download_media(
                {"phone": "+123", "chat_id": 1, "message_id": 42}
            )
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_accounts_connect_reads_session_from_db(tmp_path):
    """accounts.connect handler must NOT accept session_string from payload;
    it reads it from the accounts table."""
    db = Database(str(tmp_path / "test.db"))
    await db.initialize()
    try:
        await db.add_account(
            Account(phone="+777", session_string="SECRET_SESSION", is_primary=True)
        )

        pool = MagicMock()
        pool.add_client = AsyncMock()
        pool.get_client_by_phone = AsyncMock(return_value=None)

        dispatcher = TelegramCommandDispatcher(db, pool)
        result = await dispatcher._handle_accounts_connect({"phone": "+777"})

        assert result["phone"] == "+777"
        pool.add_client.assert_awaited_once_with("+777", "SECRET_SESSION")
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_accounts_connect_raises_for_unknown_phone(tmp_path):
    db = Database(str(tmp_path / "test.db"))
    await db.initialize()
    try:
        pool = MagicMock()
        pool.add_client = AsyncMock()
        dispatcher = TelegramCommandDispatcher(db, pool)
        with pytest.raises(RuntimeError, match="account_not_found"):
            await dispatcher._handle_accounts_connect({"phone": "+000"})
        pool.add_client.assert_not_awaited()
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_notification_service_uses_config_prefixes(tmp_path):
    """dispatcher._notification_service must propagate bot prefixes from AppConfig."""
    from src.config import AppConfig

    db = Database(str(tmp_path / "test.db"))
    await db.initialize()
    try:
        config = AppConfig()
        config.notifications.bot_name_prefix = "CustomName"
        config.notifications.bot_username_prefix = "custom_"

        pool = MagicMock()
        dispatcher = TelegramCommandDispatcher(db, pool, config)
        svc = dispatcher._notification_service()
        assert svc._bot_name_prefix == "CustomName"
        assert svc._bot_username_prefix == "custom_"
    finally:
        await db.close()


@pytest.mark.asyncio
async def test_notification_service_without_config_uses_defaults(tmp_path):
    db = Database(str(tmp_path / "test.db"))
    await db.initialize()
    try:
        pool = MagicMock()
        dispatcher = TelegramCommandDispatcher(db, pool)
        svc = dispatcher._notification_service()
        # Defaults from notification_service module
        assert svc._bot_name_prefix == "LeadHunter"
        assert svc._bot_username_prefix == "leadhunter_"
    finally:
        await db.close()


# --- Additional handler tests using mock DB/pool ---


def _mock_db():
    db = MagicMock()
    db.get_accounts = AsyncMock(return_value=[])
    db.add_account = AsyncMock()
    db.get_setting = AsyncMock(return_value=None)
    db.set_account_active = AsyncMock()
    db.delete_account = AsyncMock()
    db.update_account_premium = AsyncMock()
    db.get_channel_by_pk = AsyncMock(return_value=None)
    db.get_channels = AsyncMock(return_value=[])
    db.add_channel = AsyncMock()
    db.set_channel_active = AsyncMock()
    db.set_channel_type = AsyncMock()
    db.update_channel_full_meta = AsyncMock()
    db.upsert_forum_topics = AsyncMock()
    db.repos = MagicMock()
    db.repos.telegram_commands = MagicMock()
    db.repos.telegram_commands.claim_next_command = AsyncMock(return_value=None)
    db.repos.telegram_commands.update_command = AsyncMock()
    db.repos.dialog_cache = MagicMock()
    db.repos.dialog_cache.replace_dialogs = AsyncMock()
    db.repos.dialog_cache.clear_dialogs = AsyncMock()
    db.repos.dialog_cache.clear_all_dialogs = AsyncMock()
    db.repos.runtime_snapshots = MagicMock()
    db.repos.runtime_snapshots.upsert_snapshot = AsyncMock()
    db.repos.generation_runs = MagicMock()
    db.repos.generation_runs.get = AsyncMock(return_value=None)
    return db


def _mock_pool():
    pool = MagicMock()
    pool.get_native_client_by_phone = AsyncMock(return_value=None)
    pool.get_dialogs_for_phone = AsyncMock(return_value=[])
    pool.leave_channels = AsyncMock(return_value={})
    pool.add_client = AsyncMock()
    pool.remove_client = AsyncMock()
    pool.get_client_by_phone = AsyncMock(return_value=None)
    pool.release_client = AsyncMock()
    pool.resolve_channel = AsyncMock(return_value=None)
    pool.fetch_channel_meta = AsyncMock(return_value=None)
    pool.get_forum_topics = AsyncMock(return_value=[])
    return pool


def _dispatcher(db=None, pool=None, **kw):
    return TelegramCommandDispatcher(db or _mock_db(), pool or _mock_pool(), **kw)


def _client_mock():
    c = MagicMock()
    c.get_entity = AsyncMock(return_value="entity")
    return c


async def test_dispatch_unknown():
    d = _dispatcher()
    with pytest.raises(RuntimeError, match="Unsupported"):
        await d._dispatch("no_such", {})


async def test_dispatch_routes():
    d = _dispatcher()
    d._handle_test_cmd = AsyncMock(return_value={"ok": True})
    r = await d._dispatch("test_cmd", {})
    assert r == {"ok": True}


async def test_dispatch_dots_to_underscores():
    d = _dispatcher()
    d._handle_dialogs_send = AsyncMock(return_value={})
    await d._dispatch("dialogs.send", {"phone": "+1"})
    d._handle_dialogs_send.assert_awaited_once()


async def test_get_client_unavailable():
    d = _dispatcher()
    with pytest.raises(RuntimeError, match="client unavailable"):
        await d._get_client("+1234")


async def test_get_client_ok():
    pool = _mock_pool()
    c = _client_mock()
    pool.get_native_client_by_phone.return_value = (c, "+1")
    d = _dispatcher(pool=pool)
    assert await d._get_client("+1") == (c, "+1")


async def test_auth_send_code_not_configured():
    d = _dispatcher()
    with pytest.raises(RuntimeError, match="auth_not_configured"):
        await d._handle_auth_send_code({"phone": "+1"})


async def test_auth_send_code_ok():
    auth = MagicMock(is_configured=True)
    auth.send_code = AsyncMock(return_value={"phone_code_hash": "h"})
    d = _dispatcher(auth=auth)
    r = await d._handle_auth_send_code({"phone": "+1"})
    assert r["phone_code_hash"] == "h"


async def test_auth_resend_code_not_configured():
    d = _dispatcher()
    with pytest.raises(RuntimeError, match="auth_not_configured"):
        await d._handle_auth_resend_code({"phone": "+1"})


async def test_auth_verify_code():
    auth = MagicMock(is_configured=True)
    auth.verify_code = AsyncMock(return_value="session123")
    db = _mock_db()
    pool = _mock_pool()
    from src.models import Account

    db.get_accounts.return_value = [Account(id=1, phone="+1", session_string="session123")]
    pool.get_client_by_phone = AsyncMock(return_value=None)
    d = _dispatcher(db=db, pool=pool, auth=auth)
    r = await d._handle_auth_verify_code({"phone": "+1", "code": "123", "phone_code_hash": "h"})
    assert r["result"]["phone"] == "+1"


async def test_scheduler_reconcile_no_scheduler():
    d = _dispatcher()
    with pytest.raises(RuntimeError, match="scheduler_unavailable"):
        await d._handle_scheduler_reconcile({})


async def test_scheduler_reconcile_off():
    db = _mock_db()
    db.get_setting.return_value = "0"
    s = MagicMock(is_running=False)
    s.stop = AsyncMock()
    s.load_settings = AsyncMock()
    d = _dispatcher(db=db, scheduler=s)
    r = await d._handle_scheduler_reconcile({})
    assert r["running"] is False


async def test_scheduler_reconcile_on():
    db = _mock_db()
    db.get_setting.return_value = "1"
    s = MagicMock(is_running=False, interval_minutes=30)
    s.stop = AsyncMock()
    s.start = AsyncMock()
    s.load_settings = AsyncMock()
    d = _dispatcher(db=db, scheduler=s)
    r = await d._handle_scheduler_reconcile({})
    assert r["running"] is True
    assert r["interval_minutes"] == 30


async def test_scheduler_trigger_warm():
    s = MagicMock()
    s.trigger_warm_background = AsyncMock()
    d = _dispatcher(scheduler=s)
    r = await d._handle_scheduler_trigger_warm({})
    assert r["started"] is True


async def test_dialogs_refresh():
    db = _mock_db()
    pool = _mock_pool()
    pool.get_dialogs_for_phone.return_value = [{"id": 1}, {"id": 2}]
    d = _dispatcher(db=db, pool=pool)
    r = await d._handle_dialogs_refresh({"phone": "+1"})
    assert r["dialogs_count"] == 2


async def test_dialogs_cache_clear_phone():
    db = _mock_db()
    pool = _mock_pool()
    pool.invalidate_dialogs_cache = MagicMock()
    d = _dispatcher(db=db, pool=pool)
    r = await d._handle_dialogs_cache_clear({"phone": "+1"})
    db.repos.dialog_cache.clear_dialogs.assert_awaited_once_with("+1")


async def test_dialogs_cache_clear_all():
    db = _mock_db()
    pool = _mock_pool()
    pool.invalidate_dialogs_cache = MagicMock()
    d = _dispatcher(db=db, pool=pool)
    await d._handle_dialogs_cache_clear({})
    db.repos.dialog_cache.clear_all_dialogs.assert_awaited_once()


async def test_dialogs_leave():
    pool = _mock_pool()
    pool.leave_channels.return_value = {(-100, "Ch1"): True, (-200, "Ch2"): False}
    d = _dispatcher(pool=pool)
    r = await d._handle_dialogs_leave({
        "phone": "+1", "dialogs": [{"dialog_id": -100, "title": "Ch1"}, {"dialog_id": -200, "title": "Ch2"}],
    })
    assert r["left"] == 1
    assert r["failed"] == 1


async def test_dialogs_send():
    pool = _mock_pool()
    c = _client_mock()
    msg = MagicMock(id=42)
    c.send_message = AsyncMock(return_value=msg)
    pool.get_native_client_by_phone.return_value = (c, "+1")
    d = _dispatcher(pool=pool)
    r = await d._handle_dialogs_send({"phone": "+1", "recipient": "-100", "text": "hi"})
    assert r["message_id"] == 42


async def test_dialogs_edit_message():
    pool = _mock_pool()
    c = _client_mock()
    c.edit_message = AsyncMock()
    pool.get_native_client_by_phone.return_value = (c, "+1")
    d = _dispatcher(pool=pool)
    await d._handle_dialogs_edit_message({"phone": "+1", "chat_id": -100, "message_id": 1, "text": "x"})
    pool.release_client.assert_awaited()


async def test_dialogs_delete_message():
    pool = _mock_pool()
    c = _client_mock()
    c.delete_messages = AsyncMock()
    pool.get_native_client_by_phone.return_value = (c, "+1")
    d = _dispatcher(pool=pool)
    r = await d._handle_dialogs_delete_message({"phone": "+1", "chat_id": -100, "message_ids": [1, 2]})
    assert r["deleted"] == 2


async def test_dialogs_forward():
    pool = _mock_pool()
    c = _client_mock()
    c.forward_messages = AsyncMock()
    pool.get_native_client_by_phone.return_value = (c, "+1")
    d = _dispatcher(pool=pool)
    r = await d._handle_dialogs_forward_messages({
        "phone": "+1", "from_chat": -100, "to_chat": -200, "message_ids": [1],
    })
    assert r["forwarded"] == 1


async def test_dialogs_pin_unpin():
    pool = _mock_pool()
    c = _client_mock()
    c.pin_message = AsyncMock()
    c.unpin_message = AsyncMock()
    pool.get_native_client_by_phone.return_value = (c, "+1")
    d = _dispatcher(pool=pool)
    await d._handle_dialogs_pin_message({"phone": "+1", "chat_id": -100, "message_id": 1})
    await d._handle_dialogs_unpin_message({"phone": "+1", "chat_id": -100, "message_id": 1})


async def test_dialogs_mark_read():
    pool = _mock_pool()
    c = _client_mock()
    c.send_read_acknowledge = AsyncMock()
    pool.get_native_client_by_phone.return_value = (c, "+1")
    d = _dispatcher(pool=pool)
    r = await d._handle_dialogs_mark_read({"phone": "+1", "chat_id": -100})
    assert r["phone"] == "+1"


async def test_dialogs_kick():
    pool = _mock_pool()
    c = _client_mock()
    c.kick_participant = AsyncMock()
    pool.get_native_client_by_phone.return_value = (c, "+1")
    d = _dispatcher(pool=pool)
    r = await d._handle_dialogs_kick({"phone": "+1", "chat_id": -100, "user_id": 42})
    assert r["phone"] == "+1"


async def test_dialogs_archive_unarchive():
    pool = _mock_pool()
    c = _client_mock()
    c.edit_folder = AsyncMock()
    pool.get_native_client_by_phone.return_value = (c, "+1")
    d = _dispatcher(pool=pool)
    assert (await d._handle_dialogs_archive({"phone": "+1", "chat_id": -100}))["folder_id"] == 1
    assert (await d._handle_dialogs_unarchive({"phone": "+1", "chat_id": -100}))["folder_id"] == 0


async def test_dialogs_edit_admin():
    pool = _mock_pool()
    c = _client_mock()
    c.edit_admin = AsyncMock()
    pool.get_native_client_by_phone.return_value = (c, "+1")
    d = _dispatcher(pool=pool)
    await d._handle_dialogs_edit_admin({"phone": "+1", "chat_id": -100, "user_id": 42, "is_admin": True})


async def test_dialogs_edit_permissions():
    pool = _mock_pool()
    c = _client_mock()
    c.edit_permissions = AsyncMock()
    pool.get_native_client_by_phone.return_value = (c, "+1")
    d = _dispatcher(pool=pool)
    await d._handle_dialogs_edit_permissions({
        "phone": "+1", "chat_id": -100, "user_id": 42,
        "send_messages": True, "until_date": "2026-12-31T00:00:00",
    })


async def test_dialogs_participants():
    db = _mock_db()
    pool = _mock_pool()
    c = _client_mock()
    p = MagicMock(id=1, first_name="A", last_name="B", username="u")
    c.get_participants = AsyncMock(return_value=[p])
    pool.get_native_client_by_phone.return_value = (c, "+1")
    d = _dispatcher(db=db, pool=pool)
    r = await d._handle_dialogs_participants({"phone": "+1", "chat_id": -100, "limit": 10, "search": ""})
    assert r["total"] == 1


async def test_dialogs_participants_search_no_cache():
    db = _mock_db()
    pool = _mock_pool()
    c = _client_mock()
    c.get_participants = AsyncMock(return_value=[])
    pool.get_native_client_by_phone.return_value = (c, "+1")
    d = _dispatcher(db=db, pool=pool)
    r = await d._handle_dialogs_participants({"phone": "+1", "chat_id": -100, "search": "x"})
    assert "participants" in r
    db.repos.runtime_snapshots.upsert_snapshot.assert_not_awaited()


async def test_channels_add_identifier():
    pool = _mock_pool()
    db = _mock_db()
    pool.resolve_channel.return_value = {"channel_id": -100, "title": "T", "username": "t", "channel_type": "channel"}
    pool.fetch_channel_meta.return_value = {"about": "a", "linked_chat_id": None, "has_comments": False}
    d = _dispatcher(db=db, pool=pool)
    r = await d._handle_channels_add_identifier({"identifier": "@t"})
    assert r["channel_id"] == -100


async def test_channels_add_identifier_fail():
    pool = _mock_pool()
    pool.resolve_channel.return_value = None
    d = _dispatcher(pool=pool)
    with pytest.raises(RuntimeError, match="resolve failed"):
        await d._handle_channels_add_identifier({"identifier": "@x"})


async def test_channels_collect_stats_no_collector():
    d = _dispatcher()
    with pytest.raises(RuntimeError, match="collector_unavailable"):
        await d._handle_channels_collect_stats({"channel_pk": 1})


async def test_channels_collect_stats_not_found():
    d = _dispatcher(collector=MagicMock())
    with pytest.raises(RuntimeError, match="channel_not_found"):
        await d._handle_channels_collect_stats({"channel_pk": 999})


async def test_channels_refresh_types():
    from src.models import Channel

    db = _mock_db()
    pool = _mock_pool()
    db.get_channels.return_value = [Channel(id=1, channel_id=-100, title="T", username="t")]
    pool.resolve_channel.return_value = {"channel_id": -100, "channel_type": "supergroup"}
    d = _dispatcher(db=db, pool=pool)
    r = await d._handle_channels_refresh_types({})
    assert r["updated"] == 1


async def test_channels_refresh_meta():
    from src.models import Channel

    db = _mock_db()
    pool = _mock_pool()
    db.get_channels.return_value = [Channel(id=1, channel_id=-100, title="T")]
    pool.fetch_channel_meta.return_value = {"about": "a", "linked_chat_id": None, "has_comments": False}
    d = _dispatcher(db=db, pool=pool)
    r = await d._handle_channels_refresh_meta({})
    assert r["updated"] == 1


async def test_channels_import_batch():
    pool = _mock_pool()
    pool.resolve_channel.return_value = {"channel_id": -100, "title": "T", "username": "t", "channel_type": "channel"}
    d = _dispatcher(pool=pool)
    r = await d._handle_channels_import_batch({"identifiers": ["@t"]})
    assert r["added"] == 1


async def test_channels_import_batch_existing():
    from src.models import Channel

    db = _mock_db()
    pool = _mock_pool()
    db.get_channels.return_value = [Channel(id=1, channel_id=-100, title="T")]
    pool.resolve_channel.return_value = {"channel_id": -100, "title": "T", "username": "t", "channel_type": "channel"}
    d = _dispatcher(db=db, pool=pool)
    r = await d._handle_channels_import_batch({"identifiers": ["@t"]})
    assert r["skipped"] == 1


async def test_channels_import_batch_fail():
    pool = _mock_pool()
    pool.resolve_channel.return_value = None
    d = _dispatcher(pool=pool)
    r = await d._handle_channels_import_batch({"identifiers": ["@x"]})
    assert r["failed"] == 1


async def test_accounts_toggle():
    from src.models import Account

    db = _mock_db()
    pool = _mock_pool()
    db.get_accounts.return_value = [Account(id=1, phone="+1", session_string="s", is_active=True)]
    d = _dispatcher(db=db, pool=pool)
    r = await d._handle_accounts_toggle({"account_id": 1})
    assert r["is_active"] is False


async def test_accounts_toggle_not_found():
    d = _dispatcher()
    with pytest.raises(RuntimeError, match="account_not_found"):
        await d._handle_accounts_toggle({"account_id": 999})


async def test_accounts_delete():
    from src.models import Account

    db = _mock_db()
    pool = _mock_pool()
    db.get_accounts.return_value = [Account(id=1, phone="+1", session_string="s")]
    d = _dispatcher(db=db, pool=pool)
    r = await d._handle_accounts_delete({"account_id": 1})
    assert r["deleted"] is True


async def test_forum_topics_refresh():
    db = _mock_db()
    pool = _mock_pool()
    pool.get_forum_topics.return_value = [{"id": 1, "title": "Topic"}]
    d = _dispatcher(db=db, pool=pool)
    r = await d._handle_agent_forum_topics_refresh({"channel_id": -100})
    assert r["count"] == 1


async def test_start_stop():
    d = _dispatcher()
    await d.start()
    assert d._task is not None
    await d.stop()
    assert d._task is None


async def test_start_idempotent():
    d = _dispatcher()
    await d.start()
    t = d._task
    await d.start()
    assert d._task is t
    await d.stop()


async def test_stop_no_task():
    d = _dispatcher()
    await d.stop()


async def test_run_loop_exception():
    from src.models import TelegramCommand

    db = _mock_db()
    pool = _mock_pool()
    cmd = TelegramCommand(id=1, command_type="bad_type", payload={})
    db.repos.telegram_commands.claim_next_command.return_value = cmd
    d = _dispatcher(db=db, pool=pool)
    # Set stop after first iteration so loop exits after handling the command
    original_claim = db.repos.telegram_commands.claim_next_command

    async def claim_once():
        result = await original_claim()
        d._stop_event.set()
        db.repos.telegram_commands.claim_next_command.return_value = None
        return result

    db.repos.telegram_commands.claim_next_command = claim_once
    await d._run_loop()
    update_call = db.repos.telegram_commands.update_command.call_args
    assert update_call is not None
    from src.models import TelegramCommandStatus
    assert update_call[1]["status"] == TelegramCommandStatus.FAILED


# --- broadcast_stats ---


async def test_dialogs_broadcast_stats():
    db = _mock_db()
    pool = _mock_pool()
    c = _client_mock()
    stats = MagicMock()
    stats.followers = MagicMock(current=100, previous=90)
    stats.views_per_post = MagicMock(current=50.0, previous=40.0)
    stats.shares_per_post = None
    stats.reactions_per_post = None
    stats.forwards_per_post = None
    period = MagicMock()
    period.min_date = None
    period.max_date = None
    stats.period = period
    stats.enabled_notifications = 0.75
    c.get_broadcast_stats = AsyncMock(return_value=stats)
    pool.get_native_client_by_phone.return_value = (c, "+1")
    d = _dispatcher(db=db, pool=pool)
    r = await d._handle_dialogs_broadcast_stats({"phone": "+1", "chat_id": -100})
    assert r["phone"] == "+1"
    assert "scope" in r


# --- create_channel ---


async def test_dialogs_create_channel():
    pool = _mock_pool()
    c = AsyncMock()
    channel_obj = MagicMock()
    channel_obj.id = 12345
    channel_obj.username = ""
    result_mock = MagicMock()
    result_mock.chats = [channel_obj]
    c.return_value = result_mock
    c.get_entity = AsyncMock(return_value="entity")
    pool.get_native_client_by_phone.return_value = (c, "+1")
    d = _dispatcher(pool=pool)
    with patch("telethon.tl.functions.channels.CreateChannelRequest") as MockReq, \
         patch("telethon.tl.functions.channels.UpdateUsernameRequest"):
        MockReq.return_value = "req"
        r = await d._handle_dialogs_create_channel({"phone": "+1", "title": "Test Channel", "about": "desc"})
    assert r["phone"] == "+1"


# --- notifications ---


async def test_notifications_setup_bot():
    db = _mock_db()
    pool = _mock_pool()
    d = _dispatcher(db=db, pool=pool)
    mock_notif = MagicMock()
    mock_bot = MagicMock(bot_username="test_bot", bot_id=42)
    mock_notif.setup_bot = AsyncMock(return_value=mock_bot)
    with patch.object(type(d), "_notification_service", return_value=mock_notif):
        r = await d._handle_notifications_setup_bot({})
    assert r["bot_username"] == "test_bot"


async def test_notifications_delete_bot():
    db = _mock_db()
    pool = _mock_pool()
    d = _dispatcher(db=db, pool=pool)
    mock_notif = MagicMock()
    mock_notif.teardown_bot = AsyncMock()
    with patch.object(type(d), "_notification_service", return_value=mock_notif):
        r = await d._handle_notifications_delete_bot({})
    assert r["deleted"] is True


async def test_notifications_test():
    db = _mock_db()
    pool = _mock_pool()
    d = _dispatcher(db=db, pool=pool)
    with patch.object(mod, "Notifier") as MockNotifier, \
         patch("src.database.bundles.NotificationBundle") as MockBundle:
        MockBundle.from_database.return_value = MagicMock()
        mock_instance = MagicMock()
        mock_instance.notify = AsyncMock(return_value=True)
        MockNotifier.return_value = mock_instance
        r = await d._handle_notifications_test({})
    assert r["sent"] is True


async def test_notifications_test_failed():
    db = _mock_db()
    pool = _mock_pool()
    d = _dispatcher(db=db, pool=pool)
    with patch.object(mod, "Notifier") as MockNotifier, \
         patch("src.database.bundles.NotificationBundle") as MockBundle:
        MockBundle.from_database.return_value = MagicMock()
        mock_instance = MagicMock()
        mock_instance.notify = AsyncMock(return_value=False)
        MockNotifier.return_value = mock_instance
        with pytest.raises(RuntimeError, match="notification_test_failed"):
            await d._handle_notifications_test({})


# --- photo handlers ---


async def test_photo_send_now():
    db = _mock_db()
    pool = _mock_pool()
    d = _dispatcher(db=db, pool=pool)
    mock_svc = MagicMock()
    mock_item = MagicMock(id=1, batch_id=10)
    mock_svc.send_now = AsyncMock(return_value=mock_item)
    with patch.object(type(d), "_photo_task_service", return_value=mock_svc):
        r = await d._handle_photo_send_now({
            "phone": "+1", "target_dialog_id": -100, "target_title": "T",
            "file_paths": ["/img.jpg"], "mode": "separate",
        })
    assert r["item_id"] == 1


async def test_photo_schedule_send():
    db = _mock_db()
    pool = _mock_pool()
    d = _dispatcher(db=db, pool=pool)
    mock_svc = MagicMock()
    mock_item = MagicMock(id=2, batch_id=20)
    mock_svc.schedule_send = AsyncMock(return_value=mock_item)
    with patch.object(type(d), "_photo_task_service", return_value=mock_svc):
        r = await d._handle_photo_schedule_send({
            "phone": "+1", "target_dialog_id": -100, "file_paths": ["/img.jpg"],
            "schedule_at": "2026-06-01T00:00:00",
        })
    assert r["item_id"] == 2


async def test_photo_run_due():
    db = _mock_db()
    pool = _mock_pool()
    d = _dispatcher(db=db, pool=pool)
    mock_task_svc = MagicMock()
    mock_task_svc.run_due = AsyncMock(return_value=3)
    mock_auto_svc = MagicMock()
    mock_auto_svc.run_due = AsyncMock(return_value=1)
    with patch.object(type(d), "_photo_task_service", return_value=mock_task_svc), \
         patch.object(type(d), "_photo_auto_upload_service", return_value=mock_auto_svc):
        r = await d._handle_photo_run_due({})
    assert r["processed_items"] == 3
    assert r["processed_jobs"] == 1


# --- moderation_publish_run ---


async def test_moderation_publish_run_not_found():
    db = _mock_db()
    pool = _mock_pool()
    d = _dispatcher(db=db, pool=pool)
    with pytest.raises(RuntimeError, match="run_not_found"):
        await d._handle_moderation_publish_run({"run_id": 999, "pipeline_id": 1})


# --- run_loop success path ---


async def test_run_loop_success():
    from src.models import TelegramCommand, TelegramCommandStatus

    db = _mock_db()
    pool = _mock_pool()
    cmd = TelegramCommand(id=2, command_type="dialogs.cache_clear", payload={})
    db.repos.telegram_commands.claim_next_command.return_value = cmd

    d = _dispatcher(db=db, pool=pool)

    async def claim_once():
        d._stop_event.set()
        db.repos.telegram_commands.claim_next_command.return_value = None
        return cmd

    db.repos.telegram_commands.claim_next_command = claim_once
    await d._run_loop()
    update_call = db.repos.telegram_commands.update_command.call_args
    assert update_call is not None
    assert update_call[1]["status"] == TelegramCommandStatus.SUCCEEDED


# --- run_loop cancelled path ---


async def test_run_loop_cancelled():
    from src.models import TelegramCommand, TelegramCommandStatus

    db = _mock_db()
    pool = _mock_pool()
    cmd = TelegramCommand(id=3, command_type="dialogs.cache_clear", payload={})

    d = _dispatcher(db=db, pool=pool)

    call_count = 0
    original_claim = db.repos.telegram_commands.claim_next_command

    async def claim_once():
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return cmd
        return None

    db.repos.telegram_commands.claim_next_command = claim_once

    async def dispatch_then_cancel(command_type, payload):
        d._stop_event.set()
        raise asyncio.CancelledError()

    d._dispatch = dispatch_then_cancel

    with pytest.raises(asyncio.CancelledError):
        await d._run_loop()

    update_call = db.repos.telegram_commands.update_command.call_args
    assert update_call is not None
    assert update_call[1]["status"] == TelegramCommandStatus.PENDING


# ============================================================
# Additional tests for uncovered handler paths
# ============================================================


# --- _handle_auth_send_code: auth is None ---


async def test_auth_send_code_auth_none():
    d = _dispatcher(auth=None)
    with pytest.raises(RuntimeError, match="auth_not_configured"):
        await d._handle_auth_send_code({"phone": "+1"})


# --- _handle_auth_resend_code: happy path ---


async def test_auth_resend_code_ok():
    auth = MagicMock(is_configured=True)
    auth.resend_code = AsyncMock(return_value={"phone_code_hash": "h2"})
    d = _dispatcher(auth=auth)
    r = await d._handle_auth_resend_code({"phone": "+1"})
    assert r["phone_code_hash"] == "h2"
    assert r["phone"] == "+1"


# --- _handle_auth_verify_code: first account is_primary ---


async def test_auth_verify_code_first_account_primary():
    auth = MagicMock(is_configured=True)
    auth.verify_code = AsyncMock(return_value="session_new")
    db = _mock_db()
    pool = _mock_pool()
    # First call returns empty (for add_account logic), second call returns the
    # newly added account (for _handle_accounts_connect which looks it up).
    new_account = Account(id=2, phone="+1", session_string="session_new", is_primary=True)
    db.get_accounts = AsyncMock(side_effect=[[], [new_account]])
    pool.get_client_by_phone = AsyncMock(return_value=None)
    d = _dispatcher(db=db, pool=pool, auth=auth)
    r = await d._handle_auth_verify_code({"phone": "+1", "code": "123", "phone_code_hash": "h"})
    add_call = db.add_account.call_args
    assert add_call[0][0].is_primary is True
    assert r["result"]["phone"] == "+1"


# --- _handle_auth_verify_code: with 2fa password ---


async def test_auth_verify_code_with_2fa():
    auth = MagicMock(is_configured=True)
    auth.verify_code = AsyncMock(return_value="session_2fa")
    db = _mock_db()
    pool = _mock_pool()
    new_account = Account(id=2, phone="+1", session_string="session_2fa", is_primary=True)
    db.get_accounts = AsyncMock(side_effect=[[], [new_account]])
    pool.get_client_by_phone = AsyncMock(return_value=None)
    d = _dispatcher(db=db, pool=pool, auth=auth)
    r = await d._handle_auth_verify_code({
        "phone": "+1", "code": "123", "phone_code_hash": "h", "password_2fa": "mypass",
    })
    auth.verify_code.assert_awaited_once_with("+1", "123", "h", "mypass")
    assert r["result"]["phone"] == "+1"


# --- _handle_scheduler_reconcile: autostart=1 and is_running=True (stop then restart) ---


async def test_scheduler_reconcile_on_was_running():
    db = _mock_db()
    db.get_setting.return_value = "1"
    s = MagicMock(is_running=True, interval_minutes=15)
    s.stop = AsyncMock()
    s.start = AsyncMock()
    s.load_settings = AsyncMock()
    d = _dispatcher(db=db, scheduler=s)
    r = await d._handle_scheduler_reconcile({})
    assert r["running"] is True
    assert r["interval_minutes"] == 15
    s.stop.assert_awaited_once()
    s.load_settings.assert_awaited_once()
    s.start.assert_awaited_once()


# --- _handle_scheduler_trigger_warm: no scheduler ---


async def test_scheduler_trigger_warm_no_scheduler():
    d = _dispatcher()
    with pytest.raises(RuntimeError, match="scheduler_unavailable"):
        await d._handle_scheduler_trigger_warm({})


# --- _handle_dialogs_cache_clear: pool without invalidate_dialogs_cache ---


async def test_dialogs_cache_clear_no_invalidate_attr():
    db = _mock_db()
    pool = _mock_pool()
    # Remove invalidate_dialogs_cache attribute
    del pool.invalidate_dialogs_cache
    d = _dispatcher(db=db, pool=pool)
    r = await d._handle_dialogs_cache_clear({"phone": "+1"})
    db.repos.dialog_cache.clear_dialogs.assert_awaited_once_with("+1")
    assert r["phone"] == "+1"


# --- _handle_dialogs_cache_clear: no phone clears all ---


async def test_dialogs_cache_clear_no_phone_clears_all():
    db = _mock_db()
    pool = _mock_pool()
    d = _dispatcher(db=db, pool=pool)
    r = await d._handle_dialogs_cache_clear({"phone": ""})
    db.repos.dialog_cache.clear_all_dialogs.assert_awaited_once()
    assert r["phone"] == ""


# --- _handle_channels_refresh_types: resolve returns False (unavailable) ---


async def test_channels_refresh_types_unavailable():
    from src.models import Channel

    db = _mock_db()
    pool = _mock_pool()
    db.get_channels.return_value = [Channel(id=1, channel_id=-100, title="T", username="t")]
    pool.resolve_channel.return_value = False
    d = _dispatcher(db=db, pool=pool)
    r = await d._handle_channels_refresh_types({})
    assert r["failed"] == 1
    db.set_channel_active.assert_awaited_once_with(1, False)
    db.set_channel_type.assert_awaited_once_with(-100, "unavailable")


# --- _handle_channels_refresh_types: resolve returns None ---


async def test_channels_refresh_types_resolve_none():
    from src.models import Channel

    db = _mock_db()
    pool = _mock_pool()
    db.get_channels.return_value = [Channel(id=1, channel_id=-100, title="T", username="t")]
    pool.resolve_channel.return_value = None
    d = _dispatcher(db=db, pool=pool)
    r = await d._handle_channels_refresh_types({})
    assert r["failed"] == 1


# --- _handle_channels_refresh_types: resolve raises exception ---


async def test_channels_refresh_types_resolve_exception():
    from src.models import Channel

    db = _mock_db()
    pool = _mock_pool()
    db.get_channels.return_value = [Channel(id=1, channel_id=-100, title="T", username="t")]
    pool.resolve_channel.side_effect = Exception("net error")
    d = _dispatcher(db=db, pool=pool)
    r = await d._handle_channels_refresh_types({})
    assert r["failed"] == 1


# --- _handle_channels_refresh_types: channel without username uses channel_id ---


async def test_channels_refresh_types_no_username():
    from src.models import Channel

    db = _mock_db()
    pool = _mock_pool()
    db.get_channels.return_value = [Channel(id=1, channel_id=-100, title="T", username=None)]
    pool.resolve_channel.return_value = {"channel_id": -100, "channel_type": "channel"}
    d = _dispatcher(db=db, pool=pool)
    r = await d._handle_channels_refresh_types({})
    assert r["updated"] == 1
    pool.resolve_channel.assert_awaited_once_with("-100")


# --- _handle_channels_refresh_types: resolve returns info with None channel_type ---


async def test_channels_refresh_types_null_type():
    from src.models import Channel

    db = _mock_db()
    pool = _mock_pool()
    db.get_channels.return_value = [Channel(id=1, channel_id=-100, title="T", username="t")]
    pool.resolve_channel.return_value = {"channel_id": -100, "channel_type": None}
    d = _dispatcher(db=db, pool=pool)
    r = await d._handle_channels_refresh_types({})
    assert r["failed"] == 1


# --- _handle_channels_refresh_meta: meta is None ---


async def test_channels_refresh_meta_none():
    from src.models import Channel

    db = _mock_db()
    pool = _mock_pool()
    db.get_channels.return_value = [Channel(id=1, channel_id=-100, title="T")]
    pool.fetch_channel_meta.return_value = None
    d = _dispatcher(db=db, pool=pool)
    r = await d._handle_channels_refresh_meta({})
    assert r["failed"] == 1
    assert r["updated"] == 0


# --- _handle_channels_refresh_meta: fetch raises exception ---


async def test_channels_refresh_meta_exception():
    from src.models import Channel

    db = _mock_db()
    pool = _mock_pool()
    db.get_channels.return_value = [Channel(id=1, channel_id=-100, title="T")]
    pool.fetch_channel_meta.side_effect = Exception("timeout")
    d = _dispatcher(db=db, pool=pool)
    r = await d._handle_channels_refresh_meta({})
    assert r["failed"] == 1


# --- _handle_channels_collect_stats: with collector and channel found ---


async def test_channels_collect_stats_ok():
    from src.models import Channel

    db = _mock_db()
    ch = Channel(id=1, channel_id=-100, title="Test")
    db.get_channel_by_pk.return_value = ch
    collector = MagicMock()
    collector.collect_channel_stats = AsyncMock(return_value={"subscribers": 500})
    d = _dispatcher(db=db, collector=collector)
    r = await d._handle_channels_collect_stats({"channel_pk": 1})
    assert r["channel_id"] == -100
    assert r["collected"] is True


# --- _handle_channels_collect_stats: collector returns None ---


async def test_channels_collect_stats_empty():
    from src.models import Channel

    db = _mock_db()
    ch = Channel(id=1, channel_id=-100, title="Test")
    db.get_channel_by_pk.return_value = ch
    collector = MagicMock()
    collector.collect_channel_stats = AsyncMock(return_value=None)
    d = _dispatcher(db=db, collector=collector)
    r = await d._handle_channels_collect_stats({"channel_pk": 1})
    assert r["collected"] is False


# --- _handle_channels_add_identifier: no meta ---


async def test_channels_add_identifier_no_meta():
    pool = _mock_pool()
    db = _mock_db()
    pool.resolve_channel.return_value = {
        "channel_id": -200, "title": "T", "username": "t", "channel_type": "channel",
    }
    pool.fetch_channel_meta.return_value = None
    d = _dispatcher(db=db, pool=pool)
    r = await d._handle_channels_add_identifier({"identifier": "@t"})
    assert r["channel_id"] == -200
    db.add_channel.assert_awaited_once()


# --- _handle_channels_add_identifier: deactivate flag ---


async def test_channels_add_identifier_deactivate():
    pool = _mock_pool()
    db = _mock_db()
    pool.resolve_channel.return_value = {
        "channel_id": -300, "title": "T", "username": "t",
        "channel_type": "channel", "deactivate": True,
    }
    pool.fetch_channel_meta.return_value = {"about": "a", "linked_chat_id": None, "has_comments": False}
    d = _dispatcher(db=db, pool=pool)
    r = await d._handle_channels_add_identifier({"identifier": "@t"})
    added_ch = db.add_channel.call_args[0][0]
    assert added_ch.is_active is False


# --- _handle_channels_import_batch: resolve raises exception ---


async def test_channels_import_batch_resolve_exception():
    pool = _mock_pool()
    pool.resolve_channel.side_effect = Exception("net fail")
    d = _dispatcher(pool=pool)
    r = await d._handle_channels_import_batch({"identifiers": ["@x"]})
    assert r["failed"] == 1
    assert r["details"][0]["status"] == "failed"


# --- _handle_channels_import_batch: empty identifiers ---


async def test_channels_import_batch_empty():
    d = _dispatcher()
    r = await d._handle_channels_import_batch({"identifiers": []})
    assert r["added"] == 0
    assert r["skipped"] == 0
    assert r["failed"] == 0


# --- _handle_dialogs_create_channel: with username ---


async def test_dialogs_create_channel_with_username():
    pool = _mock_pool()
    c = AsyncMock()
    channel_obj = MagicMock()
    channel_obj.id = 12345
    channel_obj.username = ""
    result_mock = MagicMock()
    result_mock.chats = [channel_obj]
    c.return_value = result_mock
    c.get_entity = AsyncMock(return_value="entity")
    pool.get_native_client_by_phone.return_value = (c, "+1")
    d = _dispatcher(pool=pool)
    with patch("telethon.tl.functions.channels.CreateChannelRequest") as MockReq, \
         patch("telethon.tl.functions.channels.UpdateUsernameRequest") as MockUpdate:
        MockReq.return_value = "req"
        r = await d._handle_dialogs_create_channel({
            "phone": "+1", "title": "Test Channel", "about": "desc", "username": "my_channel",
        })
    assert r["channel_username"] == "my_channel"
    assert "t.me/my_channel" in r["invite_link"]


# --- _handle_dialogs_create_channel: username update fails gracefully ---


async def test_dialogs_create_channel_username_fails():
    pool = _mock_pool()
    c = AsyncMock()
    channel_obj = MagicMock()
    channel_obj.id = 12345
    channel_obj.username = ""
    result_mock = MagicMock()
    result_mock.chats = [channel_obj]

    # First call = CreateChannelRequest, second call = UpdateUsernameRequest (fails)
    c.side_effect = [result_mock, Exception("username taken")]
    c.get_entity = AsyncMock(return_value="entity")
    pool.get_native_client_by_phone.return_value = (c, "+1")
    d = _dispatcher(pool=pool)
    with patch("telethon.tl.functions.channels.CreateChannelRequest") as MockReq, \
         patch("telethon.tl.functions.channels.UpdateUsernameRequest"):
        MockReq.return_value = "req"
        r = await d._handle_dialogs_create_channel({
            "phone": "+1", "title": "Test Channel", "username": "taken_name",
        })
    # Username remains empty since update failed
    assert r["channel_username"] == ""
    assert r["invite_link"] == ""


# --- _handle_dialogs_create_channel: no chats in result ---


async def test_dialogs_create_channel_no_chats():
    pool = _mock_pool()
    c = AsyncMock()
    result_mock = MagicMock()
    result_mock.chats = []
    c.return_value = result_mock
    c.get_entity = AsyncMock(return_value="entity")
    pool.get_native_client_by_phone.return_value = (c, "+1")
    d = _dispatcher(pool=pool)
    with patch("telethon.tl.functions.channels.CreateChannelRequest") as MockReq:
        MockReq.return_value = "req"
        with pytest.raises(RuntimeError, match="Telegram returned empty response"):
            await d._handle_dialogs_create_channel({"phone": "+1", "title": "Test"})


# --- _handle_dialogs_edit_admin: with title ---


async def test_dialogs_edit_admin_with_title():
    pool = _mock_pool()
    c = _client_mock()
    c.edit_admin = AsyncMock()
    pool.get_native_client_by_phone.return_value = (c, "+1")
    d = _dispatcher(pool=pool)
    await d._handle_dialogs_edit_admin({
        "phone": "+1", "chat_id": -100, "user_id": 42, "is_admin": True, "title": "Editor",
    })
    c.edit_admin.assert_awaited_once_with("entity", "entity", is_admin=True, title="Editor")


# --- _handle_dialogs_edit_admin: default is_admin=False ---


async def test_dialogs_edit_admin_default_not_admin():
    pool = _mock_pool()
    c = _client_mock()
    c.edit_admin = AsyncMock()
    pool.get_native_client_by_phone.return_value = (c, "+1")
    d = _dispatcher(pool=pool)
    await d._handle_dialogs_edit_admin({"phone": "+1", "chat_id": -100, "user_id": 42})
    c.edit_admin.assert_awaited_once_with("entity", "entity", is_admin=False)


# --- _handle_dialogs_edit_permissions: with send_media, no until_date ---


async def test_dialogs_edit_permissions_send_media():
    pool = _mock_pool()
    c = _client_mock()
    c.edit_permissions = AsyncMock()
    pool.get_native_client_by_phone.return_value = (c, "+1")
    d = _dispatcher(pool=pool)
    await d._handle_dialogs_edit_permissions({
        "phone": "+1", "chat_id": -100, "user_id": 42,
        "send_messages": True, "send_media": False,
    })
    c.edit_permissions.assert_awaited_once()
    kwargs = c.edit_permissions.call_args[1]
    assert kwargs["send_messages"] is True
    assert kwargs["send_media"] is False
    assert "until_date" not in kwargs


# --- _handle_dialogs_edit_permissions: no extra fields ---


async def test_dialogs_edit_permissions_minimal():
    pool = _mock_pool()
    c = _client_mock()
    c.edit_permissions = AsyncMock()
    pool.get_native_client_by_phone.return_value = (c, "+1")
    d = _dispatcher(pool=pool)
    await d._handle_dialogs_edit_permissions({"phone": "+1", "chat_id": -100, "user_id": 42})
    c.edit_permissions.assert_awaited_once_with("entity", "entity")


# --- _handle_dialogs_unpin_message: no message_id (None) ---


async def test_dialogs_unpin_message_no_id():
    pool = _mock_pool()
    c = _client_mock()
    c.unpin_message = AsyncMock()
    pool.get_native_client_by_phone.return_value = (c, "+1")
    d = _dispatcher(pool=pool)
    r = await d._handle_dialogs_unpin_message({"phone": "+1", "chat_id": -100})
    c.unpin_message.assert_awaited_once_with("entity", None)
    assert r["phone"] == "+1"


# --- _handle_dialogs_mark_read: with max_id ---


async def test_dialogs_mark_read_with_max_id():
    pool = _mock_pool()
    c = _client_mock()
    c.send_read_acknowledge = AsyncMock()
    pool.get_native_client_by_phone.return_value = (c, "+1")
    d = _dispatcher(pool=pool)
    await d._handle_dialogs_mark_read({"phone": "+1", "chat_id": -100, "max_id": 500})
    c.send_read_acknowledge.assert_awaited_once_with("entity", max_id=500)


# --- _handle_dialogs_participants: no search (caches snapshot) ---


async def test_dialogs_participants_cache_no_search():
    db = _mock_db()
    pool = _mock_pool()
    c = _client_mock()
    p = MagicMock(id=10, first_name="X", last_name="Y", username="z")
    c.get_participants = AsyncMock(return_value=[p])
    pool.get_native_client_by_phone.return_value = (c, "+1")
    d = _dispatcher(db=db, pool=pool)
    r = await d._handle_dialogs_participants({"phone": "+1", "chat_id": -100, "limit": 50})
    assert r["total"] == 1
    assert "participants" not in r
    db.repos.runtime_snapshots.upsert_snapshot.assert_awaited_once()


# --- _handle_dialogs_broadcast_stats: empty stats fallback to raw ---


async def test_dialogs_broadcast_stats_raw_fallback():
    db = _mock_db()
    pool = _mock_pool()
    c = _client_mock()
    stats = MagicMock()
    stats.followers = None
    stats.views_per_post = None
    stats.shares_per_post = None
    stats.reactions_per_post = None
    stats.forwards_per_post = None
    stats.period = None
    stats.enabled_notifications = None
    stats.__str__ = lambda self: "raw_stats_data"
    c.get_broadcast_stats = AsyncMock(return_value=stats)
    pool.get_native_client_by_phone.return_value = (c, "+1")
    d = _dispatcher(db=db, pool=pool)
    r = await d._handle_dialogs_broadcast_stats({"phone": "+1", "chat_id": -100})
    assert r["phone"] == "+1"
    snap_call = db.repos.runtime_snapshots.upsert_snapshot.call_args
    snap_payload = snap_call[0][0].payload
    assert "raw" in snap_payload["stats"]


# --- _handle_dialogs_broadcast_stats: attr with no .current (str fallback) ---


async def test_dialogs_broadcast_stats_str_value():
    db = _mock_db()
    pool = _mock_pool()
    c = _client_mock()
    stats = MagicMock()
    stats.followers = "some_string_value"  # no .current attr
    stats.views_per_post = None
    stats.shares_per_post = None
    stats.reactions_per_post = None
    stats.forwards_per_post = None
    stats.period = None
    stats.enabled_notifications = None
    c.get_broadcast_stats = AsyncMock(return_value=stats)
    pool.get_native_client_by_phone.return_value = (c, "+1")
    d = _dispatcher(db=db, pool=pool)
    r = await d._handle_dialogs_broadcast_stats({"phone": "+1", "chat_id": -100})
    snap_call = db.repos.runtime_snapshots.upsert_snapshot.call_args
    snap_payload = snap_call[0][0].payload
    assert snap_payload["stats"]["followers"] == "some_string_value"


# --- _handle_dialogs_broadcast_stats: with period dates ---


async def test_dialogs_broadcast_stats_with_period():
    from datetime import datetime

    db = _mock_db()
    pool = _mock_pool()
    c = _client_mock()
    stats = MagicMock()
    stats.followers = None
    stats.views_per_post = None
    stats.shares_per_post = None
    stats.reactions_per_post = None
    stats.forwards_per_post = None
    period = MagicMock()
    period.min_date = datetime(2026, 1, 1)
    period.max_date = datetime(2026, 12, 31)
    stats.period = period
    stats.enabled_notifications = None
    c.get_broadcast_stats = AsyncMock(return_value=stats)
    pool.get_native_client_by_phone.return_value = (c, "+1")
    d = _dispatcher(db=db, pool=pool)
    r = await d._handle_dialogs_broadcast_stats({"phone": "+1", "chat_id": -100})
    snap_call = db.repos.runtime_snapshots.upsert_snapshot.call_args
    snap_payload = snap_call[0][0].payload
    assert "period" in snap_payload["stats"]
    assert snap_payload["stats"]["period"]["min_date"] == "2026-01-01T00:00:00"


# --- _handle_accounts_connect: with premium fetch ---


async def test_accounts_connect_with_premium():
    from src.models import Account

    db = _mock_db()
    pool = _mock_pool()
    db.get_accounts.return_value = [Account(id=1, phone="+1", session_string="s", is_primary=True)]
    pool.add_client = AsyncMock()

    session_mock = MagicMock()
    me = MagicMock()
    me.premium = True
    session_mock.fetch_me = AsyncMock(return_value=me)
    pool.get_client_by_phone = AsyncMock(return_value=(session_mock, "+1"))

    d = _dispatcher(db=db, pool=pool)
    r = await d._handle_accounts_connect({"phone": "+1"})
    assert r["is_premium"] is True
    db.update_account_premium.assert_awaited_once_with("+1", True)
    pool.release_client.assert_awaited_once_with("+1")


# --- _handle_accounts_toggle: activate inactive account ---


async def test_accounts_toggle_activate():
    from src.models import Account

    db = _mock_db()
    pool = _mock_pool()
    db.get_accounts.return_value = [Account(id=2, phone="+2", session_string="s", is_active=False)]
    d = _dispatcher(db=db, pool=pool)
    r = await d._handle_accounts_toggle({"account_id": 2})
    assert r["is_active"] is True
    pool.add_client.assert_awaited_once_with("+2", "s")


# --- _handle_accounts_toggle: add_client failure ---


async def test_accounts_toggle_deactivate_remove_failure():
    from src.models import Account

    db = _mock_db()
    pool = _mock_pool()
    db.get_accounts.return_value = [Account(id=1, phone="+1", session_string="s", is_active=True)]
    pool.remove_client = AsyncMock(side_effect=Exception("cannot remove"))
    d = _dispatcher(db=db, pool=pool)
    # Should not raise; exception is caught and logged
    r = await d._handle_accounts_toggle({"account_id": 1})
    assert r["is_active"] is False


# --- _handle_accounts_toggle: activate failure ---


async def test_accounts_toggle_activate_add_failure():
    from src.models import Account

    db = _mock_db()
    pool = _mock_pool()
    db.get_accounts.return_value = [Account(id=2, phone="+2", session_string="s", is_active=False)]
    pool.add_client = AsyncMock(side_effect=Exception("cannot add"))
    d = _dispatcher(db=db, pool=pool)
    # Should not raise; exception is caught and logged
    r = await d._handle_accounts_toggle({"account_id": 2})
    assert r["is_active"] is True


# --- _handle_accounts_delete: remove_client failure ---


async def test_accounts_delete_remove_failure():
    from src.models import Account

    db = _mock_db()
    pool = _mock_pool()
    db.get_accounts.return_value = [Account(id=1, phone="+1", session_string="s")]
    pool.remove_client = AsyncMock(side_effect=Exception("err"))
    d = _dispatcher(db=db, pool=pool)
    r = await d._handle_accounts_delete({"account_id": 1})
    assert r["deleted"] is True
    db.delete_account.assert_awaited_once_with(1)


# --- _handle_accounts_delete: account not found (still deletes by id) ---


async def test_accounts_delete_not_found():
    db = _mock_db()
    pool = _mock_pool()
    db.get_accounts.return_value = []
    d = _dispatcher(db=db, pool=pool)
    r = await d._handle_accounts_delete({"account_id": 99})
    assert r["deleted"] is True
    pool.remove_client.assert_not_awaited()
    db.delete_account.assert_awaited_once_with(99)


# --- _handle_moderation_publish_run: pipeline not found ---


async def test_moderation_publish_run_pipeline_not_found():
    db = _mock_db()
    pool = _mock_pool()
    run_mock = MagicMock()
    db.repos.generation_runs.get.return_value = run_mock
    d = _dispatcher(db=db, pool=pool)
    with patch("src.services.pipeline_service.PipelineService") as MockPS:
        MockPS.return_value.get = AsyncMock(return_value=None)
        with pytest.raises(RuntimeError, match="pipeline_invalid"):
            await d._handle_moderation_publish_run({"run_id": 1, "pipeline_id": 1})


# --- _handle_moderation_publish_run: success ---


async def test_moderation_publish_run_success():
    db = _mock_db()
    pool = _mock_pool()
    run_mock = MagicMock()
    db.repos.generation_runs.get.return_value = run_mock
    pipeline_mock = MagicMock()
    d = _dispatcher(db=db, pool=pool)
    with patch("src.services.pipeline_service.PipelineService") as MockPS, \
         patch("src.services.publish_service.PublishService") as MockPubSvc:
        MockPS.return_value.get = AsyncMock(return_value=pipeline_mock)
        pub_result = MagicMock(success=True)
        MockPubSvc.return_value.publish_run = AsyncMock(return_value=[pub_result])
        r = await d._handle_moderation_publish_run({"run_id": 1, "pipeline_id": 1})
    assert r["run_id"] == 1
    assert r["published"] == 1


# --- _handle_moderation_publish_run: publish fails ---


async def test_moderation_publish_run_publish_fails():
    db = _mock_db()
    pool = _mock_pool()
    run_mock = MagicMock()
    db.repos.generation_runs.get.return_value = run_mock
    pipeline_mock = MagicMock()
    d = _dispatcher(db=db, pool=pool)
    with patch("src.services.pipeline_service.PipelineService") as MockPS, \
         patch("src.services.publish_service.PublishService") as MockPubSvc:
        MockPS.return_value.get = AsyncMock(return_value=pipeline_mock)
        pub_result = MagicMock(success=False)
        MockPubSvc.return_value.publish_run = AsyncMock(return_value=[pub_result])
        with pytest.raises(RuntimeError, match="pipeline_run_failed"):
            await d._handle_moderation_publish_run({"run_id": 1, "pipeline_id": 1})


# --- _handle_moderation_publish_run: empty results ---


async def test_moderation_publish_run_empty_results():
    db = _mock_db()
    pool = _mock_pool()
    run_mock = MagicMock()
    db.repos.generation_runs.get.return_value = run_mock
    pipeline_mock = MagicMock()
    d = _dispatcher(db=db, pool=pool)
    with patch("src.services.pipeline_service.PipelineService") as MockPS, \
         patch("src.services.publish_service.PublishService") as MockPubSvc:
        MockPS.return_value.get = AsyncMock(return_value=pipeline_mock)
        MockPubSvc.return_value.publish_run = AsyncMock(return_value=[])
        with pytest.raises(RuntimeError, match="pipeline_run_failed"):
            await d._handle_moderation_publish_run({"run_id": 1, "pipeline_id": 1})


# --- _handle_agent_forum_topics_refresh: empty topics ---


async def test_forum_topics_refresh_empty():
    db = _mock_db()
    pool = _mock_pool()
    pool.get_forum_topics.return_value = []
    d = _dispatcher(db=db, pool=pool)
    r = await d._handle_agent_forum_topics_refresh({"channel_id": -100})
    assert r["count"] == 0
    # No DB writes for empty topics
    db.upsert_forum_topics.assert_not_awaited()


# --- _handle_dialogs_download_media: message not found ---


async def test_download_media_message_not_found(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "test.db"))
    await db.initialize()
    try:
        root = tmp_path / "app"
        fake_dispatcher_py = root / "src" / "services" / "telegram_command_dispatcher.py"
        fake_dispatcher_py.parent.mkdir(parents=True)
        fake_dispatcher_py.touch()
        monkeypatch.setattr(mod, "__file__", str(fake_dispatcher_py))

        pool = MagicMock()
        pool.release_client = AsyncMock()

        client = MagicMock()

        async def empty_iter(*a, **kw):
            return
            yield  # make it an async generator  # noqa: unreachable

        client.iter_messages = lambda entity, ids: empty_iter()
        client.get_entity = AsyncMock(return_value=MagicMock())

        dispatcher = TelegramCommandDispatcher(db, pool)
        dispatcher._get_client = AsyncMock(return_value=(client, "+123"))

        with pytest.raises(RuntimeError, match="message_not_found"):
            await dispatcher._handle_dialogs_download_media(
                {"phone": "+123", "chat_id": 1, "message_id": 42}
            )
    finally:
        await db.close()


# --- _handle_dialogs_download_media: no media in message ---


async def test_download_media_no_media(tmp_path, monkeypatch):
    db = Database(str(tmp_path / "test.db"))
    await db.initialize()
    try:
        root = tmp_path / "app"
        fake_dispatcher_py = root / "src" / "services" / "telegram_command_dispatcher.py"
        fake_dispatcher_py.parent.mkdir(parents=True)
        fake_dispatcher_py.touch()
        monkeypatch.setattr(mod, "__file__", str(fake_dispatcher_py))

        pool = MagicMock()
        pool.release_client = AsyncMock()

        client = MagicMock()

        msg = MagicMock()

        async def single_msg(*a, **kw):
            yield msg

        client.iter_messages = lambda entity, ids: single_msg()
        client.download_media = AsyncMock(return_value=None)
        client.get_entity = AsyncMock(return_value=MagicMock())

        dispatcher = TelegramCommandDispatcher(db, pool)
        dispatcher._get_client = AsyncMock(return_value=(client, "+123"))

        with pytest.raises(RuntimeError, match="no_media"):
            await dispatcher._handle_dialogs_download_media(
                {"phone": "+123", "chat_id": 1, "message_id": 42}
            )
    finally:
        await db.close()


# --- _run_loop: sleep when no command ---


async def test_run_loop_sleeps_on_no_command():
    db = _mock_db()
    pool = _mock_pool()
    d = _dispatcher(db=db, pool=pool)

    claim_count = 0

    async def claim_none_then_stop():
        nonlocal claim_count
        claim_count += 1
        if claim_count >= 2:
            d._stop_event.set()
        return None

    db.repos.telegram_commands.claim_next_command = claim_none_then_stop

    with patch("src.services.telegram_command_dispatcher.asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
        await d._run_loop()
    # asyncio.sleep should have been called (once for each None command)
    assert mock_sleep.await_count >= 1


# --- _handle_dialogs_leave: empty dialogs list ---


async def test_dialogs_leave_empty():
    pool = _mock_pool()
    pool.leave_channels.return_value = {}
    d = _dispatcher(pool=pool)
    r = await d._handle_dialogs_leave({"phone": "+1", "dialogs": []})
    assert r["left"] == 0
    assert r["failed"] == 0


# --- _handle_dialogs_pin_message: with notify=True ---


async def test_dialogs_pin_with_notify():
    pool = _mock_pool()
    c = _client_mock()
    c.pin_message = AsyncMock()
    pool.get_native_client_by_phone.return_value = (c, "+1")
    d = _dispatcher(pool=pool)
    await d._handle_dialogs_pin_message({"phone": "+1", "chat_id": -100, "message_id": 1, "notify": True})
    c.pin_message.assert_awaited_once_with("entity", 1, notify=True)


# --- _notification_target_service ---


async def test_notification_target_service():
    db = _mock_db()
    pool = _mock_pool()
    d = _dispatcher(db=db, pool=pool)
    with patch("src.services.telegram_command_dispatcher.NotificationTargetService") as MockNTS, \
         patch("src.database.bundles.NotificationBundle") as MockBundle:
        MockBundle.from_database.return_value = MagicMock()
        svc = d._notification_target_service()
        MockNTS.assert_called_once()


# --- _handle_dialogs_send: message without id attr ---


async def test_dialogs_send_message_no_id():
    pool = _mock_pool()
    c = _client_mock()
    c.send_message = AsyncMock(return_value="not_a_message_object")
    pool.get_native_client_by_phone.return_value = (c, "+1")
    d = _dispatcher(pool=pool)
    r = await d._handle_dialogs_send({"phone": "+1", "recipient": "-100", "text": "hi"})
    assert r["message_id"] is None
    pool.release_client.assert_awaited_once_with("+1")


# --- _handle_dialogs_edit_permissions: with until_date ---


async def test_dialogs_edit_permissions_with_until_date():
    pool = _mock_pool()
    c = _client_mock()
    c.edit_permissions = AsyncMock()
    pool.get_native_client_by_phone.return_value = (c, "+1")
    d = _dispatcher(pool=pool)
    await d._handle_dialogs_edit_permissions({
        "phone": "+1", "chat_id": -100, "user_id": 42,
        "until_date": "2026-12-31T23:59:59",
    })
    kwargs = c.edit_permissions.call_args[1]
    assert "until_date" in kwargs
