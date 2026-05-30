from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from src.models import TelegramCommand
from src.services.telegram_command_service import TelegramCommandService


@pytest.fixture
def mock_db():
    db = MagicMock()
    db.repos = MagicMock()
    db.repos.telegram_commands = MagicMock()
    db.repos.telegram_commands.find_active_by_type = AsyncMock(return_value=None)
    db.repos.telegram_commands.create_command = AsyncMock(return_value=1)
    db.repos.telegram_commands.get_command = AsyncMock(return_value=None)
    db.repos.telegram_commands.cancel_command = AsyncMock(return_value=False)
    db.repos.telegram_commands.cancel_pending_commands = AsyncMock(return_value=0)
    return db


@pytest.fixture
def service(mock_db):
    return TelegramCommandService(mock_db)


async def test_enqueue_creates_new_command(service, mock_db):
    result = await service.enqueue("COLLECT", payload={"channel_id": -100})

    assert result == 1
    mock_db.repos.telegram_commands.find_active_by_type.assert_awaited_once_with(
        "COLLECT", payload={"channel_id": -100}
    )
    mock_db.repos.telegram_commands.create_command.assert_awaited_once()


async def test_enqueue_returns_existing_when_deduplicate(service, mock_db):
    existing = TelegramCommand(id=42, command_type="COLLECT", payload={"channel_id": -100})
    mock_db.repos.telegram_commands.find_active_by_type.return_value = existing

    result = await service.enqueue("COLLECT", payload={"channel_id": -100})

    assert result == 42
    mock_db.repos.telegram_commands.create_command.assert_not_awaited()


async def test_enqueue_skips_dedup_when_disabled(service, mock_db):
    result = await service.enqueue("COLLECT", payload={"channel_id": -100}, deduplicate=False)

    assert result == 1
    mock_db.repos.telegram_commands.find_active_by_type.assert_not_awaited()
    mock_db.repos.telegram_commands.create_command.assert_awaited_once()


async def test_enqueue_creates_new_when_existing_has_no_id(service, mock_db):
    existing = TelegramCommand(command_type="COLLECT", payload={"channel_id": -100})
    mock_db.repos.telegram_commands.find_active_by_type.return_value = existing

    result = await service.enqueue("COLLECT", payload={"channel_id": -100})

    assert result == 1
    mock_db.repos.telegram_commands.create_command.assert_awaited_once()


async def test_enqueue_with_requested_by(service, mock_db):
    await service.enqueue("COLLECT", payload={"channel_id": -100}, requested_by="admin")

    call_args = mock_db.repos.telegram_commands.create_command.call_args
    cmd = call_args[0][0]
    assert cmd.requested_by == "admin"


async def test_get_returns_command(service, mock_db):
    cmd = TelegramCommand(id=1, command_type="COLLECT", payload={})
    mock_db.repos.telegram_commands.get_command.return_value = cmd

    result = await service.get(1)

    assert result == cmd
    mock_db.repos.telegram_commands.get_command.assert_awaited_once_with(1)


async def test_get_returns_none_when_not_found(service, mock_db):
    result = await service.get(999)

    assert result is None
    mock_db.repos.telegram_commands.get_command.assert_awaited_once_with(999)


async def test_cancel_delegates_to_repo(service, mock_db):
    mock_db.repos.telegram_commands.cancel_command.return_value = True
    result = await service.cancel(42)
    assert result is True
    mock_db.repos.telegram_commands.cancel_command.assert_awaited_once_with(42)


async def test_cancel_returns_false_when_not_pending(service, mock_db):
    mock_db.repos.telegram_commands.cancel_command.return_value = False
    assert await service.cancel(42) is False


async def test_cancel_pending_passes_filters(service, mock_db):
    mock_db.repos.telegram_commands.cancel_pending_commands.return_value = 7
    result = await service.cancel_pending(command_type="dialogs.react", phone="+79990001111")
    assert result == 7
    mock_db.repos.telegram_commands.cancel_pending_commands.assert_awaited_once_with(
        command_type="dialogs.react", phone="+79990001111"
    )


async def test_cancel_pending_unfiltered(service, mock_db):
    await service.cancel_pending()
    mock_db.repos.telegram_commands.cancel_pending_commands.assert_awaited_once_with(
        command_type=None, phone=None
    )
