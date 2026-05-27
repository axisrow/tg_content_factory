from __future__ import annotations

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
