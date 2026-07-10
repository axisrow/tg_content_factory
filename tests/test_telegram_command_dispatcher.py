"""Unit tests for TelegramCommandDispatcher handlers (path safety, session hygiene)."""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.database import Database
from src.models import Account, AccountSessionStatus, AccountSummary, TelegramCommand, TelegramCommandStatus
from src.services import telegram_command_dispatcher as mod
from src.services.telegram_command_dispatcher import TelegramCommandDispatcher, TelegramCommandRetryLaterError
from src.telegram.flood_wait import FloodWaitInfo, HandledFloodWaitError


def _download_media_path_checks(actual_path: str, expected_file: Path, expected_dir: Path) -> tuple[bool, bool]:
    return Path(actual_path).resolve() == expected_file.resolve(), expected_dir.exists()


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


@pytest.mark.anyio
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
        pool.get_native_client_by_phone = AsyncMock(
            return_value=(_FakeClient(download_return=str(expected_file)), "+123")
        )
        dispatcher = TelegramCommandDispatcher(db, pool)

        result = await dispatcher._handle_dialogs_download_media(
            {"phone": "+123", "chat_id": 1, "message_id": 42}
        )
        path_matches, dir_exists = await asyncio.to_thread(
            _download_media_path_checks,
            result["path"],
            expected_file,
            expected_dir,
        )
        assert path_matches
        assert dir_exists
    finally:
        await db.close()


@pytest.mark.anyio
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
        pool.get_native_client_by_phone = AsyncMock(
            return_value=(_FakeClient(download_return=str(escape_file)), "+123")
        )
        dispatcher = TelegramCommandDispatcher(db, pool)

        with pytest.raises(RuntimeError, match="path_escape"):
            await dispatcher._handle_dialogs_download_media(
                {"phone": "+123", "chat_id": 1, "message_id": 42}
            )
    finally:
        await db.close()


@pytest.mark.anyio
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


@pytest.mark.anyio
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


@pytest.mark.anyio
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


@pytest.mark.anyio
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
    db.get_account_summaries = AsyncMock(return_value=[])
    db.add_account = AsyncMock()
    db.get_setting = AsyncMock(return_value=None)
    db.set_account_active = AsyncMock()
    db.delete_account = AsyncMock()
    db.update_account_premium = AsyncMock()
    db.get_channel_by_pk = AsyncMock(return_value=None)
    db.get_channel_by_channel_id = AsyncMock(return_value=None)
    db.get_channels = AsyncMock(return_value=[])
    db.add_channel = AsyncMock()
    db.create_stats_task = AsyncMock(return_value=123)
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


async def test_notifications_invalidate_cache_calls_worker_notifier():
    # #832: the queued command must clear the worker's shared Notifier me-cache.
    notifier = MagicMock()
    d = _dispatcher(notifier=notifier)
    result = await d._dispatch("notifications.invalidate_cache", {})
    assert result == {"invalidated": True}
    notifier.invalidate_me_cache.assert_called_once_with()


async def test_notifications_invalidate_cache_noop_without_notifier():
    # No worker Notifier (e.g. web-only container) → no crash, still acknowledges.
    d = _dispatcher()
    result = await d._dispatch("notifications.invalidate_cache", {})
    assert result == {"invalidated": True}


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
    assert r["result"]["phone_code_hash"] == "h"
    assert r["result"]["phone"] == "+1"


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
    await d._handle_dialogs_cache_clear({"phone": "+1"})
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


async def test_dialogs_delete():
    pool = _mock_pool()
    pool.delete_dialogs = AsyncMock(return_value={-100: True, -200: False})
    d = _dispatcher(pool=pool)
    r = await d._handle_dialogs_delete({
        "phone": "+1",
        "dialogs": [
            {"dialog_id": -100, "channel_type": "channel"},
            {"dialog_id": -200, "channel_type": "supergroup"},
        ],
    })
    assert r["deleted"] == 1
    assert r["failed"] == 1
    # channel_type must be forwarded verbatim (not dropped like the old leave title bug)
    pool.delete_dialogs.assert_awaited_once_with(
        "+1", [(-100, "channel"), (-200, "supergroup")]
    )


async def test_dialogs_send():
    pool = _mock_pool()
    c = _client_mock()
    msg = MagicMock(id=42)
    c.send_message = AsyncMock(return_value=msg)
    pool.get_native_client_by_phone.return_value = (c, "+1")
    d = _dispatcher(pool=pool)
    r = await d._handle_dialogs_send({"phone": "+1", "recipient": "-100", "text": "hi"})
    assert r["message_id"] == 42


async def test_dialogs_join():
    pool = _mock_pool()
    d = _dispatcher(pool=pool)
    result = MagicMock(phone="+1", target="@chan", via_invite=False)
    with patch.object(mod, "TelegramActionService") as svc_cls:
        svc_cls.return_value.join_dialog = AsyncMock(return_value=result)
        r = await d._handle_dialogs_join({"phone": "+1", "target": "@chan"})
    assert r == {"phone": "+1", "target": "@chan", "via_invite": False}


async def test_dialogs_resolve():
    pool = _mock_pool()
    pool.resolve_any_entity = AsyncMock(return_value={"id": 123, "type": "channel"})
    d = _dispatcher(pool=pool)
    r = await d._handle_dialogs_resolve({"phone": "+1", "identifier": "@user"})
    assert r["entity"]["id"] == 123


async def test_dialogs_resolve_not_found_raises():
    pool = _mock_pool()
    pool.resolve_any_entity = AsyncMock(return_value=None)
    d = _dispatcher(pool=pool)
    with pytest.raises(RuntimeError, match=r"resolve failed: '@nope' not found"):
        await d._handle_dialogs_resolve({"identifier": "@nope"})


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


async def test_dialogs_react_waits_while_pool_is_warming():
    pool = _mock_pool()
    pool.is_warming = MagicMock(return_value=True)
    d = _dispatcher(pool=pool)

    with pytest.raises(TelegramCommandRetryLaterError, match="warm-up"):
        await d._handle_dialogs_react({"phone": "+1", "chat_id": -100, "message_id": 1, "emoji": "👍"})

    pool.get_native_client_by_phone.assert_not_awaited()


async def test_dialogs_react_waits_when_account_is_flooded():
    db = _mock_db()
    db.get_account_summaries.return_value = [
        AccountSummary(
            id=1,
            phone="+1",
            session_status=AccountSessionStatus.OK,
            is_active=True,
            flood_wait_until=datetime.now(timezone.utc) + timedelta(minutes=1),
        )
    ]
    pool = _mock_pool()
    pool.is_warming = MagicMock(return_value=False)
    d = _dispatcher(db=db, pool=pool)

    with pytest.raises(TelegramCommandRetryLaterError, match="flood-waited"):
        await d._handle_dialogs_react({"phone": "+1", "chat_id": -100, "message_id": 1, "emoji": "👍"})

    pool.get_native_client_by_phone.assert_not_awaited()


async def test_reaction_min_interval_uses_configured_setting():
    db = _mock_db()
    db.get_setting.return_value = "5"
    pool = _mock_pool()
    pool.is_warming = MagicMock(return_value=False)
    d = _dispatcher(db=db, pool=pool)
    # A reaction just went out for this phone — only ~5s spacing should be enforced now.
    d._last_reaction_at_monotonic["+1"] = time.monotonic()

    with pytest.raises(TelegramCommandRetryLaterError, match="waiting") as exc_info:
        await d._ensure_reaction_can_run("+1")

    payload = exc_info.value.result_payload
    assert payload["state"] == "waiting_rate_limit"
    # 5s window (not the old 30s): remaining is at most the configured interval.
    assert payload["retry_after_sec"] <= 5


async def test_reaction_min_interval_clamps_below_floor():
    db = _mock_db()
    db.get_setting.return_value = "0"
    pool = _mock_pool()
    pool.is_warming = MagicMock(return_value=False)
    d = _dispatcher(db=db, pool=pool)

    # 0 is clamped up to the 1s floor, so the interval reader never returns 0.
    assert await d._reaction_min_interval() == mod.REACTION_MIN_INTERVAL_FLOOR_SEC


async def test_reaction_min_interval_falls_back_on_garbage():
    db = _mock_db()
    db.get_setting.return_value = "abc"
    pool = _mock_pool()
    pool.is_warming = MagicMock(return_value=False)
    d = _dispatcher(db=db, pool=pool)

    assert await d._reaction_min_interval() == mod.DEFAULT_REACTION_MIN_INTERVAL_SEC


async def test_reaction_min_interval_clamps_above_ceiling():
    db = _mock_db()
    db.get_setting.return_value = "9999"
    pool = _mock_pool()
    pool.is_warming = MagicMock(return_value=False)
    d = _dispatcher(db=db, pool=pool)

    assert await d._reaction_min_interval() == mod.REACTION_MIN_INTERVAL_CEILING_SEC


async def test_run_loop_requeues_handled_flood_wait():
    db = _mock_db()
    command = TelegramCommand(
        id=9,
        command_type="dialogs.react",
        payload={"phone": "+1", "chat_id": -100, "message_id": 1, "emoji": "👍"},
    )
    db.repos.telegram_commands.claim_next_command = AsyncMock(return_value=command)
    d = _dispatcher(db=db)
    info = FloodWaitInfo(
        operation="telegram_send_reaction",
        phone="+1",
        wait_seconds=21,
        next_available_at_utc=datetime.now(timezone.utc) + timedelta(seconds=21),
        detail="Flood wait 21s for +1",
    )
    d._dispatch = AsyncMock(side_effect=HandledFloodWaitError(info))

    async def _update_and_stop(*args, **kwargs):
        d._stop_event.set()

    db.repos.telegram_commands.update_command = AsyncMock(side_effect=_update_and_stop)

    await d._run_loop()

    db.repos.telegram_commands.update_command.assert_awaited_once()
    kwargs = db.repos.telegram_commands.update_command.await_args.kwargs
    assert kwargs["status"] == TelegramCommandStatus.PENDING
    assert kwargs["run_after"] > datetime.now(timezone.utc)
    assert kwargs["result_payload"]["state"] == "waiting_flood_wait"


async def test_run_loop_marks_invalid_reaction_failed_without_calling_telegram():
    db = _mock_db()
    command = TelegramCommand(
        id=10,
        command_type="dialogs.react",
        payload={"phone": "+1", "chat_id": -100, "message_id": 1, "emoji": "✅"},
    )
    db.repos.telegram_commands.claim_next_command = AsyncMock(return_value=command)
    pool = _mock_pool()
    d = _dispatcher(db=db, pool=pool)

    async def _update_and_stop(*args, **kwargs):
        d._stop_event.set()

    db.repos.telegram_commands.update_command = AsyncMock(side_effect=_update_and_stop)

    await d._run_loop()

    pool.get_native_client_by_phone.assert_not_awaited()
    kwargs = db.repos.telegram_commands.update_command.await_args.kwargs
    assert kwargs["status"] == TelegramCommandStatus.FAILED
    assert kwargs["result_payload"]["state"] == "invalid_reaction"
    assert kwargs["result_payload"]["emoji"] == "✅"


# ---------------------------------------------------------------------------
# Per-phone reaction rate-limit: real enforcement, key consistency, and the
# memory-growth guard (#1030, epic #1024 tier-1).
# ---------------------------------------------------------------------------


def _react_db(*, min_interval: str = "30"):
    """A mock DB whose only relevant behaviour is the reaction-interval setting
    and an empty account list (so the flood-wait gate is a no-op)."""
    db = _mock_db()
    db.get_setting.return_value = min_interval
    db.get_account_summaries.return_value = []
    db.get_accounts.return_value = []
    return db


def _patch_reaction_service(acquired_phone: str):
    """Patch TelegramActionService in the dialogs mixin so send_reaction reports
    ``acquired_phone`` (the phone the pool actually handed out, which the pool is
    free to normalise) regardless of the requested phone.

    Returns ``(patcher, send_reaction_mock)``; the patcher is already started and
    the caller must ``.stop()`` it when done.
    """
    svc_patch = patch("src.services.dispatcher.dialogs_mixin.TelegramActionService")
    svc_cls = svc_patch.start()
    send_reaction = AsyncMock(return_value=MagicMock(phone=acquired_phone))
    svc_cls.return_value.send_reaction = send_reaction
    return svc_patch, send_reaction


async def test_reaction_rate_limit_enforced_after_a_successful_reaction():
    """A second reaction within the interval is delayed, not sent (#1030).

    The existing suite seeds ``_last_reaction_at_monotonic`` by hand; this drives
    the real handler so the success path that records the timestamp is exercised
    end-to-end. The first reaction goes out; the second (immediate) one must be
    bounced to PENDING with run_after instead of hitting Telegram again.
    """
    db = _react_db(min_interval="30")
    pool = _mock_pool()
    pool.is_warming = MagicMock(return_value=False)
    d = _dispatcher(db=db, pool=pool)

    svc_patch, send_reaction = _patch_reaction_service(acquired_phone="+1")
    try:
        payload = {"phone": "+1", "chat_id": -100, "message_id": 1, "emoji": "👍"}
        await d._handle_dialogs_react(payload)
        assert send_reaction.await_count == 1

        with pytest.raises(TelegramCommandRetryLaterError, match="rate limit"):
            await d._handle_dialogs_react({**payload, "message_id": 2})
        # The blocked reaction must NOT have reached Telegram.
        assert send_reaction.await_count == 1
    finally:
        svc_patch.stop()


async def test_reaction_rate_limit_keyed_by_requested_phone_not_normalized():
    """Rate-limit must hold even when the pool normalises the phone (#1030).

    Bug: ``_handle_dialogs_react`` recorded the last-reaction time under the
    phone the *pool returned* (``result.phone``), while ``_ensure_reaction_can_run``
    reads under the phone from the *payload*. When the pool hands back a
    normalised phone (``"+1"`` requested, ``"1"`` acquired), the write and the
    read land on different keys, so the gate never sees the prior reaction and
    the next one fires immediately — defeating the per-phone FloodWait guard.
    """
    db = _react_db(min_interval="30")
    pool = _mock_pool()
    pool.is_warming = MagicMock(return_value=False)
    d = _dispatcher(db=db, pool=pool)

    # Pool acquires under a normalised phone that differs from the request.
    svc_patch, send_reaction = _patch_reaction_service(acquired_phone="1")
    try:
        payload = {"phone": "+1", "chat_id": -100, "message_id": 1, "emoji": "👍"}
        await d._handle_dialogs_react(payload)
        assert send_reaction.await_count == 1

        # Same requested phone, immediately again — must be rate-limited.
        with pytest.raises(TelegramCommandRetryLaterError, match="rate limit"):
            await d._handle_dialogs_react({**payload, "message_id": 2})
        assert send_reaction.await_count == 1, (
            "second reaction slipped past the rate-limit because the timestamp "
            "was keyed by the normalised acquired phone, not the requested one"
        )
    finally:
        svc_patch.stop()


async def test_reaction_timestamps_do_not_grow_unbounded():
    """Stale per-phone reaction timestamps are pruned (#1030 memory leak).

    ``_last_reaction_at_monotonic`` is an in-memory dict keyed by phone with no
    eviction: every distinct phone that ever reacted left a permanent entry, so
    a long-lived worker reacting across many accounts grows it without bound.
    Entries older than the rate-limit window carry no information (the gate would
    let that phone react anyway), so they must be evicted. This is not a DB
    ledger — pruning a stale monotonic timestamp changes no idempotency state.
    """
    db = _react_db(min_interval="30")
    pool = _mock_pool()
    pool.is_warming = MagicMock(return_value=False)
    d = _dispatcher(db=db, pool=pool)

    now = time.monotonic()
    # 500 phones reacted long ago (well past any sane interval ceiling).
    for i in range(500):
        d._last_reaction_at_monotonic[f"+{i}"] = now - 100_000
    # One phone reacted just now — its entry is still meaningful.
    d._last_reaction_at_monotonic["+recent"] = now

    svc_patch, _ = _patch_reaction_service(acquired_phone="+fresh")
    try:
        await d._handle_dialogs_react(
            {"phone": "+fresh", "chat_id": -100, "message_id": 1, "emoji": "👍"}
        )
    finally:
        svc_patch.stop()

    tracked = d._last_reaction_at_monotonic
    assert len(tracked) < 500, (
        f"reaction timestamp map is not pruned (size={len(tracked)}); stale "
        "per-phone entries accumulate forever"
    )
    # The recently-active phone and the just-reacted one are retained.
    assert "+recent" in tracked
    assert "+fresh" in tracked


async def test_reaction_bookkeeping_failure_does_not_fail_an_already_sent_reaction():
    """Post-send rate-limit bookkeeping must not fail a sent reaction (#1030).

    ``_handle_dialogs_react`` sends the reaction (an irreversible Telegram side
    effect) *before* recording it. Recording now reads the live interval setting
    from the DB to prune stale entries; if that read raises after the reaction
    already went out, the exception would bubble to ``_run_loop`` and the command
    would be persisted FAILED — so a retry/recovery re-sends a reaction Telegram
    already saw. The bookkeeping is best-effort: a settings read that blows up
    must not undo a completed send. The in-memory timestamp is still recorded so
    the rate-limit gate keeps working.
    """
    db = _react_db(min_interval="30")
    # The interval read (used only for pruning) fails after the send succeeded.
    db.get_setting.side_effect = RuntimeError("settings DB unavailable")
    pool = _mock_pool()
    pool.is_warming = MagicMock(return_value=False)
    d = _dispatcher(db=db, pool=pool)

    svc_patch, send_reaction = _patch_reaction_service(acquired_phone="+1")
    try:
        result = await d._handle_dialogs_react(
            {"phone": "+1", "chat_id": -100, "message_id": 1, "emoji": "👍"}
        )
    finally:
        svc_patch.stop()

    # The reaction was sent once and the handler returned success, not an error.
    assert send_reaction.await_count == 1
    assert result == {"phone": "+1"}
    # The timestamp is still recorded so the gate enforces spacing next time.
    assert "+1" in d._last_reaction_at_monotonic


# ---------------------------------------------------------------------------
# Phone-bound isolation: a command for account X must run on X's client, two
# commands for different phones must run on different clients, and a command
# with no phone must fail loudly rather than silently mis-route (#1030).
# ---------------------------------------------------------------------------


class _PhoneRoutingClient:
    """A per-phone fake Telethon client that records the phone it belongs to."""

    def __init__(self, phone: str):
        self.phone = phone
        self.sent: list[tuple[object, str]] = []

    async def get_entity(self, identifier):
        return MagicMock(id=identifier, _client_phone=self.phone)

    async def send_message(self, entity, text):
        # Tag every send with the owning phone so the test can assert isolation.
        self.sent.append((entity, text))
        return MagicMock(id=len(self.sent), _client_phone=self.phone)


class _PhoneRoutingPool:
    """Pool double that hands back a *distinct* client per phone.

    A real class (not MagicMock) so ``explicit_pool_method`` recognises
    ``get_native_client_by_phone`` as implemented and ``TelegramActionService``
    routes through it — proving the requested phone selects the right client.
    """

    def __init__(self, clients: dict[str, _PhoneRoutingClient]):
        self._clients = clients
        self.acquired: list[str] = []
        self.released: list[str] = []

    async def get_native_client_by_phone(self, phone, *, wait_for_flood=False):
        client = self._clients.get(phone)
        if client is None:
            return None
        self.acquired.append(phone)
        return client, phone

    def release_client(self, phone):
        self.released.append(phone)


async def test_dialogs_send_routes_to_the_clients_own_phone():
    """A send for +1 must go out on +1's client, never another account's (#1030)."""
    client_a = _PhoneRoutingClient("+1")
    client_b = _PhoneRoutingClient("+2")
    pool = _PhoneRoutingPool({"+1": client_a, "+2": client_b})
    d = _dispatcher(pool=pool)

    result = await d._handle_dialogs_send({"phone": "+1", "recipient": -100, "text": "hi"})

    assert result["phone"] == "+1"
    assert len(client_a.sent) == 1, "the +1 client must be the one that sent"
    assert client_b.sent == [], "the +2 client must not have been touched"
    assert pool.released == ["+1"], "the acquired client must be released"


async def test_concurrent_sends_for_distinct_phones_use_distinct_clients():
    """Two sends for different phones at once each hit their own client (#1030).

    Guards against the dispatcher cross-wiring accounts under concurrency — a
    +1 command sending through +2's session would post to the wrong account.
    """
    client_a = _PhoneRoutingClient("+1")
    client_b = _PhoneRoutingClient("+2")
    pool = _PhoneRoutingPool({"+1": client_a, "+2": client_b})
    d = _dispatcher(pool=pool)

    await asyncio.gather(
        d._handle_dialogs_send({"phone": "+1", "recipient": -100, "text": "from-1"}),
        d._handle_dialogs_send({"phone": "+2", "recipient": -200, "text": "from-2"}),
    )

    assert [text for _, text in client_a.sent] == ["from-1"]
    assert [text for _, text in client_b.sent] == ["from-2"]
    assert sorted(pool.acquired) == ["+1", "+2"]


async def test_dialogs_send_missing_phone_fails_loudly():
    """A phone-bound command with no phone must raise, not run on a random client.

    The handler reads ``payload["phone"]`` directly, so a missing phone is a
    hard KeyError — surfaced to ``_run_loop`` as a FAILED command rather than
    silently sending from whichever account the pool happens to pick (#1030).
    """
    pool = _PhoneRoutingPool({"+1": _PhoneRoutingClient("+1")})
    d = _dispatcher(pool=pool)

    with pytest.raises(KeyError):
        await d._handle_dialogs_send({"recipient": -100, "text": "orphan"})

    assert pool.acquired == [], "no client should have been acquired without a phone"


async def test_run_loop_marks_missing_phone_command_failed_not_silent():
    """A command with no phone is recorded FAILED, never silently dropped (#1030).

    The poll loop must convert the handler's KeyError into a FAILED status (so an
    operator sees the bad command) instead of letting it kill the loop or pass
    unnoticed.
    """
    db = _mock_db()
    command = TelegramCommand(
        id=11,
        command_type="dialogs.send",
        payload={"recipient": -100, "text": "no phone here"},
    )
    db.repos.telegram_commands.claim_next_command = AsyncMock(return_value=command)
    d = _dispatcher(db=db, pool=_PhoneRoutingPool({}))

    async def _update_and_stop(*args, **kwargs):
        d._stop_event.set()

    db.repos.telegram_commands.update_command = AsyncMock(side_effect=_update_and_stop)

    await d._run_loop()

    db.repos.telegram_commands.update_command.assert_awaited_once()
    kwargs = db.repos.telegram_commands.update_command.await_args.kwargs
    assert kwargs["status"] == TelegramCommandStatus.FAILED
    assert "phone" in kwargs["error"]


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
    added = db.add_channel.await_args.args[0]
    assert added.about == "a"
    db.create_stats_task.assert_awaited_once()


async def test_channels_add_identifier_fail():
    pool = _mock_pool()
    pool.resolve_channel.return_value = None
    d = _dispatcher(pool=pool)
    with pytest.raises(RuntimeError, match=r"resolve failed: '@x' not found"):
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
    db = _mock_db()
    pool.resolve_channel.return_value = {"channel_id": -100, "title": "T", "username": "t", "channel_type": "channel"}
    pool.fetch_channel_meta.return_value = {"about": "a", "linked_chat_id": None, "has_comments": False}
    d = _dispatcher(db=db, pool=pool)
    r = await d._handle_channels_import_batch({"identifiers": ["@t"]})
    assert r["added"] == 1
    added = db.add_channel.await_args.args[0]
    assert added.about == "a"
    db.create_stats_task.assert_awaited_once()


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
    db = _mock_db()
    pool = _mock_pool()
    db.get_accounts.side_effect = AssertionError("delete must not decrypt sessions")
    d = _dispatcher(db=db, pool=pool)
    r = await d._handle_accounts_delete({"account_id": 1, "phone": "+1"})
    assert r["deleted"] is False
    assert r["client_removed"] is True
    pool.remove_client.assert_awaited_once_with("+1")
    db.delete_account.assert_not_awaited()
    db.get_accounts.assert_not_awaited()


async def test_accounts_delete_legacy_payload_uses_summary_fallback():
    db = _mock_db()
    pool = _mock_pool()
    db.get_accounts.side_effect = AssertionError("delete must not decrypt sessions")
    db.get_account_summaries.return_value = [Account(id=1, phone="+1", session_string="")]
    d = _dispatcher(db=db, pool=pool)
    r = await d._handle_accounts_delete({"account_id": 1})
    assert r["deleted"] is True
    assert r["client_removed"] is True
    db.get_account_summaries.assert_awaited_once_with(active_only=False)
    pool.remove_client.assert_awaited_once_with("+1")
    db.delete_account.assert_awaited_once_with(1)
    db.get_accounts.assert_not_awaited()


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
    c.create_channel = AsyncMock(return_value=result_mock)
    c.update_channel_username = AsyncMock()
    pool.get_native_client_by_phone.return_value = (c, "+1")
    d = _dispatcher(pool=pool)
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
    with patch.object(mod, "Notifier") as mock_notifier, \
         patch("src.database.bundles.NotificationBundle") as mock_bundle:
        mock_bundle.from_database.return_value = MagicMock()
        mock_instance = MagicMock()
        mock_instance.notify = AsyncMock(return_value=True)
        mock_notifier.return_value = mock_instance
        r = await d._handle_notifications_test({})
    assert r["sent"] is True
    mock_instance.notify.assert_awaited_once_with("✅ Тест уведомлений: соединение установлено")


async def test_notifications_test_custom_text():
    db = _mock_db()
    pool = _mock_pool()
    d = _dispatcher(db=db, pool=pool)
    with patch.object(mod, "Notifier") as mock_notifier, \
         patch("src.database.bundles.NotificationBundle") as mock_bundle:
        mock_bundle.from_database.return_value = MagicMock()
        mock_instance = MagicMock()
        mock_instance.notify = AsyncMock(return_value=True)
        mock_notifier.return_value = mock_instance
        r = await d._handle_notifications_test({"text": "hello world"})
    assert r["sent"] is True
    mock_instance.notify.assert_awaited_once_with("hello world")


async def test_notifications_test_failed():
    db = _mock_db()
    pool = _mock_pool()
    d = _dispatcher(db=db, pool=pool)
    with patch.object(mod, "Notifier") as mock_notifier, \
         patch("src.database.bundles.NotificationBundle") as mock_bundle:
        mock_bundle.from_database.return_value = MagicMock()
        mock_instance = MagicMock()
        mock_instance.notify = AsyncMock(return_value=False)
        mock_notifier.return_value = mock_instance
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


async def test_photo_run_due_dry_run_returns_plan_without_sending():
    """dry_run payload previews auto jobs, serializes them, and never runs the item path."""
    from src.models import PhotoSendMode
    from src.services.photo_auto_upload_service import PhotoAutoPreview

    db = _mock_db()
    pool = _mock_pool()
    d = _dispatcher(db=db, pool=pool)
    mock_task_svc = MagicMock()
    mock_task_svc.run_due = AsyncMock(return_value=99)
    preview = PhotoAutoPreview(
        job_id=4,
        target_dialog_id=-100777,
        target_title="Chan",
        target_type="channel",
        send_mode=PhotoSendMode.ALBUM,
        files=["/x/1.jpg", "/x/2.jpg"],
    )
    mock_auto_svc = MagicMock()
    mock_auto_svc.run_due = AsyncMock(return_value=[preview])
    with patch.object(type(d), "_photo_task_service", return_value=mock_task_svc), \
         patch.object(type(d), "_photo_auto_upload_service", return_value=mock_auto_svc):
        r = await d._handle_photo_run_due({"dry_run": True})
    assert r["dry_run"] is True
    assert r["jobs"] == [
        {
            "job_id": 4,
            "target_dialog_id": -100777,
            "target_title": "Chan",
            "target_type": "channel",
            "send_mode": "album",
            "files": ["/x/1.jpg", "/x/2.jpg"],
        }
    ]
    mock_auto_svc.run_due.assert_awaited_once_with(dry_run=True)
    mock_task_svc.run_due.assert_not_awaited()


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


async def test_run_loop_success_update_busy_does_not_kill_loop():
    from src.database import DatabaseBusyError
    from src.models import TelegramCommand

    db = _mock_db()
    pool = _mock_pool()
    cmd = TelegramCommand(id=4, command_type="dialogs.cache_clear", payload={})
    d = _dispatcher(db=db, pool=pool)

    calls = 0

    async def claim_once_then_stop():
        nonlocal calls
        calls += 1
        if calls == 1:
            return cmd
        d._stop_event.set()
        return None

    db.repos.telegram_commands.claim_next_command = claim_once_then_stop
    db.repos.telegram_commands.update_command = AsyncMock(
        side_effect=[
            DatabaseBusyError("Database is busy. Retry the request in a few seconds."),
            None,
        ]
    )
    d._dispatch = AsyncMock(return_value={"result": {}, "payload_update": None})

    with patch.object(mod.asyncio, "sleep", new_callable=AsyncMock) as mock_sleep:
        await d._run_loop()

    assert calls == 2
    assert db.repos.telegram_commands.update_command.await_count == 2
    mock_sleep.assert_any_await(mod.COMMAND_STATUS_UPDATE_BUSY_RETRY_INITIAL_SEC)


async def test_run_loop_cancelled_reraises_when_update_busy():
    from src.database import DatabaseBusyError
    from src.models import TelegramCommand

    db = _mock_db()
    pool = _mock_pool()
    cmd = TelegramCommand(id=5, command_type="dialogs.cache_clear", payload={})
    d = _dispatcher(db=db, pool=pool)
    db.repos.telegram_commands.claim_next_command = AsyncMock(return_value=cmd)
    db.repos.telegram_commands.update_command = AsyncMock(
        side_effect=DatabaseBusyError("Database is busy. Retry the request in a few seconds.")
    )
    d._dispatch = AsyncMock(side_effect=asyncio.CancelledError())

    with pytest.raises(asyncio.CancelledError):
        await d._run_loop()

    db.repos.telegram_commands.update_command.assert_awaited_once()


# --- характеризующие тесты _update_command_safely (#1132: ручной цикл → tenacity) ---
# Фиксируют точный контракт retry до/после замены реализации: экспоненциальную
# лестницу задержек с потолком, retry_busy=False и заглатывание прочих ошибок.


def _busy_error():
    from src.database import DatabaseBusyError

    return DatabaseBusyError("Database is busy. Retry the request in a few seconds.")


async def test_update_command_safely_busy_backoff_doubles_and_caps():
    """Лестница задержек: INITIAL, 2×, 4×… с потолком MAX; попыток — до успеха."""
    db = _mock_db()
    d = _dispatcher(db=db, pool=_mock_pool())
    failures = 6
    db.repos.telegram_commands.update_command = AsyncMock(
        side_effect=[_busy_error()] * failures + [None]
    )

    with patch.object(mod.asyncio, "sleep", new_callable=AsyncMock) as mock_sleep:
        await d._update_command_safely(
            7, status=TelegramCommandStatus.SUCCEEDED, log_action="succeeded"
        )

    expected = []
    delay = mod.COMMAND_STATUS_UPDATE_BUSY_RETRY_INITIAL_SEC
    for _ in range(failures):
        expected.append(delay)
        delay = min(delay * 2, mod.COMMAND_STATUS_UPDATE_BUSY_RETRY_MAX_SEC)

    assert [c.args[0] for c in mock_sleep.await_args_list] == expected
    assert db.repos.telegram_commands.update_command.await_count == failures + 1


async def test_update_command_safely_busy_without_retry_is_single_shot():
    """retry_busy=False: одна попытка, без sleep, busy заглатывается."""
    db = _mock_db()
    d = _dispatcher(db=db, pool=_mock_pool())
    db.repos.telegram_commands.update_command = AsyncMock(side_effect=_busy_error())

    with patch.object(mod.asyncio, "sleep", new_callable=AsyncMock) as mock_sleep:
        await d._update_command_safely(
            7,
            status=TelegramCommandStatus.FAILED,
            log_action="failed",
            retry_busy=False,
        )

    db.repos.telegram_commands.update_command.assert_awaited_once()
    mock_sleep.assert_not_awaited()


async def test_update_command_safely_swallows_generic_error_without_retry():
    """Не-busy ошибка: одна попытка, без sleep, наружу не пробрасывается."""
    db = _mock_db()
    d = _dispatcher(db=db, pool=_mock_pool())
    db.repos.telegram_commands.update_command = AsyncMock(side_effect=RuntimeError("boom"))

    with patch.object(mod.asyncio, "sleep", new_callable=AsyncMock) as mock_sleep:
        await d._update_command_safely(
            7, status=TelegramCommandStatus.SUCCEEDED, log_action="succeeded"
        )

    db.repos.telegram_commands.update_command.assert_awaited_once()
    mock_sleep.assert_not_awaited()


# ============================================================
# Additional tests for handler edge paths
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
    assert r["result"]["phone_code_hash"] == "h2"
    assert r["result"]["phone"] == "+1"


# --- _handle_auth_verify_code: first account is_primary ---


async def test_auth_verify_code_first_account_primary():
    auth = MagicMock(is_configured=True)
    auth.verify_code = AsyncMock(return_value="session_new")
    db = _mock_db()
    pool = _mock_pool()
    new_account = Account(id=2, phone="+1", session_string="session_new", is_primary=True)
    db.get_account_summaries = AsyncMock(return_value=[])
    db.get_live_usable_accounts = AsyncMock(return_value=[new_account])
    db.get_accounts = AsyncMock(side_effect=AssertionError("auth verify must not decrypt unrelated sessions"))
    pool.get_client_by_phone = AsyncMock(return_value=None)
    d = _dispatcher(db=db, pool=pool, auth=auth)
    r = await d._handle_auth_verify_code({"phone": "+1", "code": "123", "phone_code_hash": "h"})
    add_call = db.add_account.call_args
    assert add_call[0][0].is_primary is True
    assert r["result"]["phone"] == "+1"


async def test_auth_verify_code_uses_summaries_when_unrelated_session_degraded():
    auth = MagicMock(is_configured=True)
    auth.verify_code = AsyncMock(return_value="session_new")
    db = _mock_db()
    pool = _mock_pool()
    db.get_account_summaries = AsyncMock(
        return_value=[
            AccountSummary(
                id=1,
                phone="+bad",
                session_status=AccountSessionStatus.DECRYPT_FAILED,
            )
        ]
    )
    db.get_live_usable_accounts = AsyncMock(
        return_value=[Account(id=2, phone="+1", session_string="session_new")]
    )
    db.get_accounts = AsyncMock(side_effect=AssertionError("must not decrypt summaries"))
    pool.get_client_by_phone = AsyncMock(return_value=None)
    d = _dispatcher(db=db, pool=pool, auth=auth)

    await d._handle_auth_verify_code({"phone": "+1", "code": "123", "phone_code_hash": "h"})

    add_call = db.add_account.call_args
    assert add_call[0][0].is_primary is False
    db.get_accounts.assert_not_awaited()


# --- _handle_auth_verify_code: with 2fa password ---


async def test_auth_verify_code_with_2fa():
    auth = MagicMock(is_configured=True)
    auth.verify_code = AsyncMock(return_value="session_2fa")
    db = _mock_db()
    pool = _mock_pool()
    new_account = Account(id=2, phone="+1", session_string="session_2fa", is_primary=True)
    db.get_account_summaries = AsyncMock(return_value=[])
    db.get_live_usable_accounts = AsyncMock(return_value=[new_account])
    db.get_accounts = AsyncMock(side_effect=AssertionError("auth verify must not decrypt unrelated sessions"))
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
    # Definitive not-found now returns the {"gone": True} sentinel, not False
    # (which resolve_channel never returned — audit #835/8).
    pool.resolve_channel.return_value = {"gone": True}
    d = _dispatcher(db=db, pool=pool)
    r = await d._handle_channels_refresh_types({})
    assert r["deactivated"] == 1
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
    pool.resolve_channel.assert_awaited_once_with(
        "-100", signal_gone=True, numeric_fallback="-100"
    )


# --- _handle_channels_refresh_types: stale @username must not deactivate a live channel ---


async def test_channels_refresh_types_passes_numeric_fallback():
    """#858 review: refresh-types resolves by @username but must pass the numeric
    channel_id as fallback so a stale username can't deactivate a live channel."""
    from src.models import Channel

    db = _mock_db()
    pool = _mock_pool()
    db.get_channels.return_value = [Channel(id=1, channel_id=-100, title="T", username="oldname")]
    pool.resolve_channel.return_value = {"channel_id": -100, "channel_type": "channel"}
    d = _dispatcher(db=db, pool=pool)
    r = await d._handle_channels_refresh_types({})
    assert r["updated"] == 1
    pool.resolve_channel.assert_awaited_once_with(
        "oldname", signal_gone=True, numeric_fallback="-100"
    )


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
    await d._handle_channels_add_identifier({"identifier": "@t"})
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
    c.create_channel = AsyncMock(return_value=result_mock)
    c.update_channel_username = AsyncMock()
    pool.get_native_client_by_phone.return_value = (c, "+1")
    d = _dispatcher(pool=pool)
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

    c.create_channel = AsyncMock(return_value=result_mock)
    c.update_channel_username = AsyncMock(side_effect=Exception("username taken"))
    pool.get_native_client_by_phone.return_value = (c, "+1")
    d = _dispatcher(pool=pool)
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
    c.create_channel = AsyncMock(return_value=result_mock)
    c.update_channel_username = AsyncMock()
    pool.get_native_client_by_phone.return_value = (c, "+1")
    d = _dispatcher(pool=pool)
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
    await d._handle_dialogs_broadcast_stats({"phone": "+1", "chat_id": -100})
    snap_call = db.repos.runtime_snapshots.upsert_snapshot.call_args
    snap_payload = snap_call[0][0].payload
    assert snap_payload["stats"]["followers"] == "some_string_value"


# --- _handle_dialogs_broadcast_stats: with period dates ---


async def test_dialogs_broadcast_stats_with_period():
    from datetime import datetime, timezone

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
    period.min_date = datetime(2026, 1, 1, tzinfo=timezone.utc).replace(tzinfo=None)
    period.max_date = datetime(2026, 12, 31, tzinfo=timezone.utc).replace(tzinfo=None)
    stats.period = period
    stats.enabled_notifications = None
    c.get_broadcast_stats = AsyncMock(return_value=stats)
    pool.get_native_client_by_phone.return_value = (c, "+1")
    d = _dispatcher(db=db, pool=pool)
    await d._handle_dialogs_broadcast_stats({"phone": "+1", "chat_id": -100})
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
    db = _mock_db()
    pool = _mock_pool()
    db.get_accounts.side_effect = AssertionError("delete must not decrypt sessions")
    pool.remove_client = AsyncMock(side_effect=Exception("err"))
    d = _dispatcher(db=db, pool=pool)
    r = await d._handle_accounts_delete({"account_id": 1, "phone": "+1"})
    assert r["deleted"] is False
    assert r["client_removed"] is True
    db.delete_account.assert_not_awaited()
    db.get_accounts.assert_not_awaited()


# --- _handle_accounts_delete: account not found (still deletes by id) ---


async def test_accounts_delete_not_found():
    db = _mock_db()
    pool = _mock_pool()
    db.get_accounts.side_effect = AssertionError("delete must not decrypt sessions")
    db.get_account_summaries.return_value = []
    d = _dispatcher(db=db, pool=pool)
    r = await d._handle_accounts_delete({"account_id": 99})
    assert r["deleted"] is True
    pool.remove_client.assert_not_awaited()
    db.delete_account.assert_awaited_once_with(99)
    db.get_accounts.assert_not_awaited()


# --- _handle_moderation_publish_run: pipeline not found ---


async def test_moderation_publish_run_pipeline_not_found():
    db = _mock_db()
    pool = _mock_pool()
    run_mock = MagicMock()
    db.repos.generation_runs.get.return_value = run_mock
    d = _dispatcher(db=db, pool=pool)
    with patch("src.services.pipeline_service.PipelineService") as mock_ps:
        mock_ps.return_value.get = AsyncMock(return_value=None)
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
    with patch("src.services.pipeline_service.PipelineService") as mock_ps, \
         patch("src.services.publish_service.PublishService") as mock_pubsvc:
        mock_ps.return_value.get = AsyncMock(return_value=pipeline_mock)
        pub_result = MagicMock(success=True)
        mock_pubsvc.return_value.publish_run = AsyncMock(return_value=[pub_result])
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
    with patch("src.services.pipeline_service.PipelineService") as mock_ps, \
         patch("src.services.publish_service.PublishService") as mock_pubsvc:
        mock_ps.return_value.get = AsyncMock(return_value=pipeline_mock)
        pub_result = MagicMock(success=False)
        mock_pubsvc.return_value.publish_run = AsyncMock(return_value=[pub_result])
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
    with patch("src.services.pipeline_service.PipelineService") as mock_ps, \
         patch("src.services.publish_service.PublishService") as mock_pubsvc:
        mock_ps.return_value.get = AsyncMock(return_value=pipeline_mock)
        mock_pubsvc.return_value.publish_run = AsyncMock(return_value=[])
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
            yield  # make it an async generator

        client.iter_messages = lambda entity, ids: empty_iter()
        client.get_entity = AsyncMock(return_value=MagicMock())
        pool.get_native_client_by_phone = AsyncMock(return_value=(client, "+123"))

        dispatcher = TelegramCommandDispatcher(db, pool)

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
        pool.get_native_client_by_phone = AsyncMock(return_value=(client, "+123"))

        dispatcher = TelegramCommandDispatcher(db, pool)

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
    # The loop runs twice (claim_count hits 2 and sets _stop_event); each None
    # claim triggers a sleep, so exactly 2 sleeps occur.
    assert mock_sleep.await_count == 2


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
    with patch("src.services.telegram_command_dispatcher.NotificationTargetService") as mock_nts, \
         patch("src.database.bundles.NotificationBundle") as mock_bundle:
        mock_bundle.from_database.return_value = MagicMock()
        d._notification_target_service()
        mock_nts.assert_called_once()


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


# --- _handle_search_telegram: proxies live Telegram search to the worker (#643) ---


async def test_search_telegram_handler_routes_premium():
    from src.models import Message, SearchResult

    engine = MagicMock()
    msg = Message(
        channel_id=-100, message_id=1, text="hit",
        date=datetime(2025, 1, 1, tzinfo=timezone.utc),
    )
    engine.search_telegram = AsyncMock(return_value=SearchResult(messages=[msg], total=1, query="q"))
    d = _dispatcher(search_engine=engine)

    out = await d._handle_search_telegram({"mode": "telegram", "query": "q", "limit": 25})

    engine.search_telegram.assert_awaited_once_with("q", limit=25)
    assert out["result"]["total"] == 1
    assert out["result"]["messages"][0]["text"] == "hit"
    # result_payload must round-trip back into a SearchResult
    assert SearchResult.model_validate(out["result"]).messages[0].message_id == 1


async def test_search_telegram_handler_routes_my_chats_and_channel():
    from src.models import SearchResult

    engine = MagicMock()
    engine.search_my_chats = AsyncMock(return_value=SearchResult(messages=[], total=0, query="q"))
    engine.search_in_channel = AsyncMock(return_value=SearchResult(messages=[], total=0, query="q"))
    d = _dispatcher(search_engine=engine)

    await d._handle_search_telegram({"mode": "my_chats", "query": "q", "limit": 10})
    engine.search_my_chats.assert_awaited_once_with("q", limit=10)

    await d._handle_search_telegram({"mode": "channel", "query": "q", "limit": 10, "channel_id": -100500})
    engine.search_in_channel.assert_awaited_once_with(-100500, "q", limit=10)


async def test_search_telegram_handler_without_engine_raises():
    d = _dispatcher(search_engine=None)
    with pytest.raises(RuntimeError, match="Search engine unavailable"):
        await d._handle_search_telegram({"mode": "telegram", "query": "q"})


async def test_run_loop_logs_search_telegram_command_context(caplog):
    from src.models import SearchResult

    db = _mock_db()
    pool = _mock_pool()
    engine = MagicMock()
    engine.search_telegram = AsyncMock(return_value=SearchResult(messages=[], total=0, query="q"))
    command = TelegramCommand(
        id=44,
        command_type="search.telegram",
        payload={"mode": "telegram", "query": "q", "limit": 5},
    )
    dispatcher = _dispatcher(db=db, pool=pool, search_engine=engine)

    async def claim_once():
        dispatcher._stop_event.set()
        return command

    db.repos.telegram_commands.claim_next_command = claim_once
    caplog.set_level(logging.INFO, logger="src.services.telegram_command_dispatcher")

    await dispatcher._run_loop()

    assert "telegram_search_command start command_id=44" in caplog.text
    assert "telegram_search_command success command_id=44" in caplog.text
    assert "query_hash=" in caplog.text
    update_call = db.repos.telegram_commands.update_command.call_args
    assert update_call is not None
    assert update_call[1]["status"] == TelegramCommandStatus.SUCCEEDED


async def test_collection_pause_sets_setting_and_pauses():
    db = _mock_db()
    db.set_setting = AsyncMock()
    queue = MagicMock()
    d = _dispatcher(db=db, collection_queue=queue)
    result = await d._handle_collection_pause({})
    db.set_setting.assert_awaited_once_with("collection_queue_paused", "1")
    queue.pause.assert_called_once_with()
    assert result == {"paused": True}


async def test_collection_resume_sets_setting_and_resumes():
    db = _mock_db()
    db.set_setting = AsyncMock()
    queue = MagicMock()
    d = _dispatcher(db=db, collection_queue=queue)
    result = await d._handle_collection_resume({})
    db.set_setting.assert_awaited_once_with("collection_queue_paused", "0")
    queue.resume.assert_called_once_with()
    assert result == {"paused": False}


async def test_collection_pause_without_queue_still_sets_setting():
    db = _mock_db()
    db.set_setting = AsyncMock()
    d = _dispatcher(db=db, collection_queue=None)
    result = await d._handle_collection_pause({})
    db.set_setting.assert_awaited_once_with("collection_queue_paused", "1")
    assert result == {"paused": True}


async def test_run_loop_survives_busy_error_from_claim():
    """A transient DatabaseBusyError from claim_next_command must NOT kill the
    dispatcher coroutine (regression: "Task exception was never retrieved").
    The loop must back off and continue claiming commands.
    """
    from src.database import DatabaseBusyError

    db = _mock_db()
    pool = _mock_pool()
    d = _dispatcher(db=db, pool=pool)

    calls = 0

    async def claim_busy_then_stop():
        nonlocal calls
        calls += 1
        if calls == 1:
            raise DatabaseBusyError("Database is busy. Retry the request in a few seconds.")
        d._stop_event.set()
        return None

    db.repos.telegram_commands.claim_next_command = claim_busy_then_stop

    with patch.object(mod.asyncio, "sleep", new_callable=AsyncMock):
        await d._run_loop()

    assert calls == 2


# ── channels.refresh_types deactivation (audit #835/8) ─────────────────────────


class TestRefreshTypesDeactivation:
    @pytest.mark.anyio
    async def test_deactivates_gone_quarantines_review_skips_transient_updates_ok(self):
        from types import SimpleNamespace

        db = MagicMock()
        ch_gone = SimpleNamespace(id=1, channel_id=111, username="gone", title="Gone",
                                  needs_review=False)
        ch_transient = SimpleNamespace(id=2, channel_id=222, username="slow", title="Slow",
                                       needs_review=False)
        ch_ok = SimpleNamespace(id=3, channel_id=333, username="ok", title="Ok",
                                needs_review=False)
        ch_review = SimpleNamespace(id=4, channel_id=444, username="maybe", title="Maybe",
                                    needs_review=False)
        db.get_channels = AsyncMock(return_value=[ch_gone, ch_transient, ch_ok, ch_review])
        db.set_channel_active = AsyncMock()
        db.set_channel_type = AsyncMock()
        db.repos.channels.set_channel_review = AsyncMock()
        db.repos.channels.clear_channel_review = AsyncMock()
        pool = MagicMock()
        pool.resolve_channel = AsyncMock(
            side_effect=[
                {"gone": True},               # definitive not-found -> deactivate
                None,                          # transient failure -> skip, stay active
                {"channel_type": "channel"},   # resolved -> update
                {"review": True, "reason": "numeric_unresolved"},  # uncertain -> quarantine
            ]
        )

        d = TelegramCommandDispatcher(db, pool)
        result = await d._handle_channels_refresh_types({})

        assert result == {"updated": 1, "failed": 1, "deactivated": 1, "quarantined": 1}
        db.set_channel_active.assert_awaited_once_with(1, False)
        # uncertain channel must be quarantined, NOT deactivated
        db.repos.channels.set_channel_review.assert_awaited_once_with(4, "numeric_unresolved")
        assert all(call.args[0] != 4 for call in db.set_channel_active.await_args_list)
        # transient channel must NOT be deactivated
        assert all(call.args[0] != 2 for call in db.set_channel_active.await_args_list)


# ── _unwrap_result_payload (audit #838/9, #838/4) ──────────────────────────────


class TestUnwrapResultPayload:
    def test_uses_envelope_when_present(self):
        assert TelegramCommandDispatcher._unwrap_result_payload({"result": {"a": 1}}) == {"a": 1}

    def test_preserves_flat_dict(self):
        # ~40 handlers return a flat dict; it must be persisted, not dropped to {}.
        flat = {"phone": "+7", "scope": "all", "total": 3, "participants": [{"id": 1}]}
        assert TelegramCommandDispatcher._unwrap_result_payload(flat) == flat

    def test_excludes_reserved_payload_update(self):
        out = TelegramCommandDispatcher._unwrap_result_payload(
            {"updated": 2, "payload_update": {"x": 1}}
        )
        assert out == {"updated": 2}

    def test_non_dict_returns_empty(self):
        assert TelegramCommandDispatcher._unwrap_result_payload(None) == {}

    def test_envelope_non_dict_inner_returns_empty(self):
        assert TelegramCommandDispatcher._unwrap_result_payload({"result": "x"}) == {}
