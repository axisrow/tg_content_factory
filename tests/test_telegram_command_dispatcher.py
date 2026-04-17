"""Unit tests for TelegramCommandDispatcher handlers (path safety, session hygiene)."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

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
