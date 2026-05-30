from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone

import pytest

from src.database import Database
from src.models import TelegramCommand, TelegramCommandStatus
from tests.agent_tools_helpers import _get_tool_handlers, _text


async def _open_db(tmp_path) -> Database:
    db = Database(str(tmp_path / "test.db"))
    await db.initialize()
    return db


async def _create_reaction(
    db: Database,
    *,
    phone: str = "+1",
    message_id: int = 1,
    emoji: str = "👍",
    status: TelegramCommandStatus = TelegramCommandStatus.PENDING,
    run_after=None,
    result_payload=None,
    error: str | None = None,
) -> int:
    command_id = await db.repos.telegram_commands.create_command(
        TelegramCommand(
            command_type="dialogs.react",
            payload={"phone": phone, "chat_id": "5832576119", "message_id": message_id, "emoji": emoji},
            status=TelegramCommandStatus.PENDING,
            requested_by="test",
            run_after=run_after,
            result_payload=result_payload,
        )
    )
    if status != TelegramCommandStatus.PENDING or error is not None:
        await db.repos.telegram_commands.update_command(
            command_id,
            status=status,
            payload={"phone": phone, "chat_id": "5832576119", "message_id": message_id, "emoji": emoji},
            result_payload=result_payload,
            error=error,
            run_after=run_after,
        )
    return command_id


@pytest.mark.anyio
async def test_get_telegram_queue_status_shows_reaction_summary(tmp_path):
    db = await _open_db(tmp_path)
    try:
        await _create_reaction(db, message_id=1, status=TelegramCommandStatus.SUCCEEDED)
        await _create_reaction(db, message_id=2, status=TelegramCommandStatus.RUNNING, emoji="🔥")
        await _create_reaction(db, message_id=3, emoji="🎉")
        handlers = _get_tool_handlers(db)

        result = await handlers["get_telegram_queue_status"]({})
        text = _text(result)

        assert "Очередь Telegram-заданий: Всего: 3" in text
        assert "Реакции: Всего: 3" in text
        assert "Выполнено: 1" in text
        assert "Выполняется: 1" in text
        assert "Ждёт: 1" in text
        assert "reaction 🎉 в чат 5832576119, сообщение 3" in text
    finally:
        await db.close()


@pytest.mark.anyio
async def test_get_telegram_queue_status_shows_flood_wait_reason(tmp_path):
    db = await _open_db(tmp_path)
    try:
        run_after = datetime.now(timezone.utc) + timedelta(minutes=2)
        await _create_reaction(
            db,
            run_after=run_after,
            result_payload={"state": "waiting_flood_wait", "phone": "+1"},
            error="Flood wait 30s for +1",
        )
        handlers = _get_tool_handlers(db)

        result = await handlers["get_telegram_queue_status"]({"command_type": "dialogs.react"})
        text = _text(result)

        assert "Ожидание реакций: 1 из-за flood-wait." in text
        assert "ждёт до" in text
        assert "из-за flood-wait" in text
    finally:
        await db.close()


@pytest.mark.anyio
async def test_get_telegram_queue_status_does_not_create_commands(tmp_path):
    db = await _open_db(tmp_path)
    try:
        await _create_reaction(db)
        before = await db.repos.telegram_commands.list_commands(limit=10)
        handlers = _get_tool_handlers(db)

        await handlers["get_telegram_queue_status"]({})

        after = await db.repos.telegram_commands.list_commands(limit=10)
        assert [item.id for item in after] == [item.id for item in before]
    finally:
        await db.close()


@pytest.mark.anyio
async def test_get_telegram_queue_status_filters_by_command_type(tmp_path):
    db = await _open_db(tmp_path)
    try:
        await _create_reaction(db, message_id=1)
        await db.repos.telegram_commands.create_command(
            TelegramCommand(
                command_type="dialogs.send_message",
                payload={"phone": "+1", "recipient": "@chat", "text": "hello"},
                requested_by="test",
            )
        )
        handlers = _get_tool_handlers(db)

        result = await handlers["get_telegram_queue_status"]({"command_type": "dialogs.react"})
        text = _text(result)

        assert "dialogs.react" in text
        assert "dialogs.send_message" not in text
        assert "Очередь Telegram-заданий: Всего: 1" in text
    finally:
        await db.close()


@pytest.mark.anyio
async def test_get_telegram_queue_status_requires_phone_when_acl_is_phone_scoped(tmp_path):
    db = await _open_db(tmp_path)
    try:
        await db.set_setting(
            "agent_tool_permissions",
            json.dumps({
                "+1": {"get_telegram_queue_status": True},
                "+2": {"get_telegram_queue_status": False},
            }),
        )
        await _create_reaction(db, phone="+1", message_id=1)
        await _create_reaction(db, phone="+2", message_id=2)
        handlers = _get_tool_handlers(db)

        missing_phone = await handlers["get_telegram_queue_status"]({})
        assert "укажи параметр phone" in _text(missing_phone)

        allowed_phone = await handlers["get_telegram_queue_status"]({"phone": "+1"})
        text = _text(allowed_phone)
        assert "сообщение 1" in text
        assert "сообщение 2" not in text

        denied_phone = await handlers["get_telegram_queue_status"]({"phone": "+2"})
        assert "не разрешён" in _text(denied_phone)
    finally:
        await db.close()


@pytest.mark.anyio
async def test_cancel_telegram_command_gates_on_target_command_phone(tmp_path):
    """Regression: single-command cancel must be gated on the command's own phone.

    cancel_telegram_command takes a bare command_id (no phone arg), so without a
    lookup-then-check a phone-restricted agent could enumerate sequential
    autoincrement IDs and cancel another account's command. The tool now reads
    the target command's payload phone and gates on it.
    """
    db = await _open_db(tmp_path)
    try:
        await db.set_setting(
            "agent_tool_permissions",
            json.dumps({
                "+1": {"cancel_telegram_command": True},
                "+2": {"cancel_telegram_command": False},
            }),
        )
        cmd1 = await _create_reaction(db, phone="+1", message_id=1)
        cmd2 = await _create_reaction(db, phone="+2", message_id=2)
        handlers = _get_tool_handlers(db)

        # Agent allowed on +1 tries to cancel +2's command → denied, +2 untouched.
        denied = await handlers["cancel_telegram_command"]({"command_id": cmd2, "confirm": True})
        assert "не разрешён" in _text(denied)
        assert (await db.repos.telegram_commands.get_command(cmd2)).status == TelegramCommandStatus.PENDING

        # Its own command goes through.
        allowed = await handlers["cancel_telegram_command"]({"command_id": cmd1, "confirm": True})
        assert "отменено" in _text(allowed)
        assert (await db.repos.telegram_commands.get_command(cmd1)).status == TelegramCommandStatus.CANCELLED

        # Unknown id is reported as not-found, not a crash.
        missing = await handlers["cancel_telegram_command"]({"command_id": 999999, "confirm": True})
        assert "не найдено" in _text(missing)
    finally:
        await db.close()


@pytest.mark.anyio
async def test_clear_pending_requires_phone_when_acl_is_phone_scoped(tmp_path):
    """Regression: unscoped bulk-cancel must be gated under a per-phone ACL.

    clear_pending_telegram_commands is in PHONE_BINDED_TOOLS. A phone-restricted
    agent calling it with no phone would otherwise bulk-cancel pending commands
    across ALL accounts, because the ACL check used to sit under `if phone:`.
    """
    db = await _open_db(tmp_path)
    try:
        await db.set_setting(
            "agent_tool_permissions",
            json.dumps({
                "+1": {"clear_pending_telegram_commands": True},
                "+2": {"clear_pending_telegram_commands": False},
            }),
        )
        await _create_reaction(db, phone="+1", message_id=1)
        await _create_reaction(db, phone="+2", message_id=2)
        handlers = _get_tool_handlers(db)

        # No phone + confirm: must be blocked asking for phone, NOT cancel all.
        missing_phone = await handlers["clear_pending_telegram_commands"]({"confirm": True})
        assert "укажи параметр phone" in _text(missing_phone)

        # Both commands are still PENDING — nothing was cancelled.
        for command_id in (1, 2):
            cmd = await db.repos.telegram_commands.get_command(command_id)
            assert cmd.status == TelegramCommandStatus.PENDING

        # Denied phone is rejected outright.
        denied = await handlers["clear_pending_telegram_commands"]({"phone": "+2", "confirm": True})
        assert "не разрешён" in _text(denied)

        # Allowed phone goes through and cancels only that account's commands.
        allowed = await handlers["clear_pending_telegram_commands"]({"phone": "+1", "confirm": True})
        assert "отмен" in _text(allowed).lower()
        assert (await db.repos.telegram_commands.get_command(1)).status == TelegramCommandStatus.CANCELLED
        assert (await db.repos.telegram_commands.get_command(2)).status == TelegramCommandStatus.PENDING
    finally:
        await db.close()


@pytest.mark.anyio
async def test_get_telegram_queue_status_clamps_limit_to_100(tmp_path):
    db = await _open_db(tmp_path)
    try:
        for message_id in range(105):
            await _create_reaction(db, message_id=message_id)
        handlers = _get_tool_handlers(db)

        result = await handlers["get_telegram_queue_status"]({"limit": 500})
        text = _text(result)

        assert "Последние задания (100):" in text
    finally:
        await db.close()
