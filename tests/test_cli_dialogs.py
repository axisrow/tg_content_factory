"""Tests for CLI dialogs commands."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.cli.commands.dialogs import run_with_dependencies
from src.config import AppConfig
from tests.helpers import cli_ns as _ns

pytestmark = pytest.mark.aiosqlite_serial


def _mock_pool():
    pool = MagicMock()
    pool.clients = {"+1234567890": MagicMock()}
    pool.disconnect_all = AsyncMock()
    pool.get_forum_topics = AsyncMock(return_value=[])
    pool.get_native_client_by_phone = AsyncMock(return_value=None)
    pool.get_dialogs_for_phone = AsyncMock(return_value=[
        {"channel_id": 100111, "title": "My Channel", "username": "mychan",
         "channel_type": "channel", "already_added": False},
    ])
    pool.leave_channels = AsyncMock(return_value={100111: True})
    pool.delete_dialogs = AsyncMock(return_value={100111: True})
    return pool


def _run(args, pool, cli_db):
    """Run CLI command with mocked pool."""
    config = AppConfig()

    async def fake_init_db(_):
        return config, cli_db

    async def fake_init_pool(_, __):
        from src.telegram.auth import TelegramAuth
        return TelegramAuth(0, ""), pool

    with patch("src.cli.commands.dialogs.runtime.init_db", side_effect=fake_init_db), \
         patch("src.cli.commands.dialogs.runtime.init_pool", side_effect=fake_init_pool):
        run_with_dependencies(args)


def test_cli_dialogs_list(cli_db, capsys):
    """Test `dialogs list` command prints dialog table."""
    import asyncio

    pool = _mock_pool()

    # channel_service.get_my_dialogs() now reads from dialog_cache by default
    # (live pool calls only happen on --refresh, owned by the worker).
    async def _seed():
        await cli_db.repos.dialog_cache.replace_dialogs(
            "+1234567890",
            [
                {
                    "channel_id": 100111,
                    "title": "My Channel",
                    "username": "mychan",
                    "channel_type": "channel",
                    "is_dm": False,
                }
            ],
        )

    asyncio.run(_seed())

    _run(_ns(dialogs_action="list", phone="+1234567890"), pool, cli_db)
    out = capsys.readouterr().out
    assert "My Channel" in out
    assert "mychan" in out


def test_cli_dialogs_list_no_accounts(cli_db, capsys):
    """Test `dialogs list` with no connected accounts."""
    pool = _mock_pool()
    pool.clients = {}
    _run(_ns(dialogs_action="list", phone=None), pool, cli_db)
    out = capsys.readouterr().out
    assert "No connected accounts" in out


def _seed_account(db, phone, *, is_primary=False):
    from src.models import Account

    return db.add_account(Account(phone=phone, session_string="sess", is_primary=is_primary))


def test_cli_dialogs_list_defaults_to_db_primary(cli_db, capsys):
    """Without --phone the DB primary account wins, not the sorted-first one (#1480).

    Sorted lexicographically '+10000000001' comes first, but '+90000000009' is
    primary; the dialog cache is seeded only for the primary, so the old
    sorted-first pick printed 'No dialogs found.'"""
    import asyncio

    pool = _mock_pool()
    pool.clients = {"+10000000001": MagicMock(), "+90000000009": MagicMock()}

    async def _seed():
        await _seed_account(cli_db, "+10000000001")
        await _seed_account(cli_db, "+90000000009", is_primary=True)
        await cli_db.repos.dialog_cache.replace_dialogs(
            "+90000000009",
            [
                {
                    "channel_id": 100111,
                    "title": "Primary Channel",
                    "username": "primchan",
                    "channel_type": "channel",
                    "is_dm": False,
                }
            ],
        )

    asyncio.run(_seed())
    _run(_ns(dialogs_action="list", phone=None), pool, cli_db)
    out = capsys.readouterr().out
    assert "Primary Channel" in out


def test_cli_dialogs_list_falls_back_to_connected_when_primary_down(cli_db, capsys):
    """Primary in DB but not connected -> first connected phone is used."""
    import asyncio

    pool = _mock_pool()
    pool.clients = {"+10000000001": MagicMock()}

    async def _seed():
        await _seed_account(cli_db, "+90000000009", is_primary=True)
        await cli_db.repos.dialog_cache.replace_dialogs(
            "+10000000001",
            [
                {
                    "channel_id": 100222,
                    "title": "Fallback Channel",
                    "username": "fbchan",
                    "channel_type": "channel",
                    "is_dm": False,
                }
            ],
        )

    asyncio.run(_seed())
    _run(_ns(dialogs_action="list", phone=None), pool, cli_db)
    out = capsys.readouterr().out
    assert "Fallback Channel" in out


def test_cli_dialogs_list_phone_not_connected(cli_db, capsys):
    """Test `dialogs list` with phone that is not connected."""
    pool = _mock_pool()
    _run(_ns(dialogs_action="list", phone="+9999999999"), pool, cli_db)
    out = capsys.readouterr().out
    assert "not connected" in out


def test_cli_dialogs_refresh(cli_db, capsys):
    """Test `dialogs refresh` command."""
    pool = _mock_pool()
    _run(_ns(dialogs_action="refresh", phone="+1234567890"), pool, cli_db)
    out = capsys.readouterr().out
    assert "refreshed" in out.lower()


def test_cli_dialogs_refresh_no_accounts(cli_db, capsys):
    """Test `dialogs refresh` with no connected accounts."""
    pool = _mock_pool()
    pool.clients = {}
    _run(_ns(dialogs_action="refresh", phone=None), pool, cli_db)
    out = capsys.readouterr().out
    assert "No connected accounts" in out


def test_cli_dialogs_leave(cli_db, capsys):
    """Test `dialogs leave` with auto-confirm."""
    pool = _mock_pool()
    _run(
        _ns(dialogs_action="leave", phone="+1234567890",
            dialog_ids=["100111"], yes=True),
        pool, cli_db,
    )
    out = capsys.readouterr().out
    assert "left" in out


def test_cli_dialogs_delete(cli_db, capsys):
    """Test `dialogs delete` with auto-confirm."""
    pool = _mock_pool()
    _run(
        _ns(dialogs_action="delete", phone="+1234567890",
            dialog_ids=["100111"], yes=True),
        pool, cli_db,
    )
    out = capsys.readouterr().out
    assert "deleted" in out
    pool.delete_dialogs.assert_awaited_once()


def test_cli_dialogs_join(cli_db, capsys):
    """Test `dialogs join` with auto-confirm."""
    pool = _mock_pool()
    client = AsyncMock()
    client.get_entity = AsyncMock(return_value="entity")
    client.join_channel = AsyncMock()
    pool.get_native_client_by_phone = AsyncMock(return_value=(client, "+1234567890"))
    pool.release_client = AsyncMock()

    _run(
        _ns(dialogs_action="join", phone="+1234567890", target="@prog_ai", yes=True),
        pool,
        cli_db,
    )

    out = capsys.readouterr().out
    assert "Joined/subscribed" in out
    client.join_channel.assert_awaited_once_with("entity")


def test_cli_dialogs_topics(cli_db, capsys):
    """Test `dialogs topics` with no topics."""
    pool = _mock_pool()
    pool.get_forum_topics = AsyncMock(return_value=[])
    _run(_ns(dialogs_action="topics", channel_id=100111), pool, cli_db)
    out = capsys.readouterr().out
    assert "No forum topics" in out


def test_cli_dialogs_topics_with_data(cli_db, capsys):
    """Test `dialogs topics` returns topic list."""
    pool = _mock_pool()
    pool.get_forum_topics = AsyncMock(return_value=[
        {"id": 1, "title": "General", "icon_emoji_id": None, "date": "2025-01-01"},
    ])
    _run(_ns(dialogs_action="topics", channel_id=100111), pool, cli_db)
    out = capsys.readouterr().out
    assert "General" in out


def test_cli_dialogs_send_no_client(cli_db, capsys):
    """Test `dialogs send` when client unavailable."""
    pool = _mock_pool()
    pool.get_native_client_by_phone = AsyncMock(return_value=None)
    _run(
        _ns(dialogs_action="send", phone="+1234567890",
            recipient="100111", text="hello", yes=True),
        pool, cli_db,
    )
    out = capsys.readouterr().out
    assert "unavailable" in out.lower()


def test_cli_dialogs_archive_history_runs_backfill(cli_db, capsys):
    """#1453: `dialogs archive-history` зовёт бэкфилл и печатает итог+разбивку."""
    pool = _mock_pool()
    with (
        patch("src.cli.commands.dialogs.serve_is_running", return_value=False),
        patch(
            "src.cli.commands.dialogs.backfill_account",
            new_callable=AsyncMock,
            return_value={"dialogs": 2, "archived": 5, "errors": 1},
        ) as fake_backfill,
    ):
        _run(_ns(dialogs_action="archive-history", phone="+1234567890", chat_id=None), pool, cli_db)

    out = capsys.readouterr().out
    assert "archived_now=5" in out
    assert "errors=1" in out
    assert "incoming=0" in out  # пустой cli_db — счётчики из архива
    assert fake_backfill.await_args.args[2] == "+1234567890"
    assert fake_backfill.await_args.kwargs["chat_ids"] is None


def test_cli_dialogs_archive_history_refuses_running_worker(cli_db, capsys):
    """Второй MTProto-коннект на сессии живого воркера = silent brick — отказ."""
    pool = _mock_pool()
    with (
        patch("src.cli.commands.dialogs.serve_is_running", return_value=True),
        patch("src.cli.commands.dialogs.backfill_account", new_callable=AsyncMock) as fake_backfill,
    ):
        _run(_ns(dialogs_action="archive-history", phone="+1234567890", chat_id=None), pool, cli_db)

    out = capsys.readouterr().out
    assert "stop it first" in out
    fake_backfill.assert_not_awaited()


def test_cli_dialogs_archive_history_single_chat(cli_db, capsys):
    """--chat-id сужает прогон до одного диалога."""
    pool = _mock_pool()
    with (
        patch("src.cli.commands.dialogs.serve_is_running", return_value=False),
        patch(
            "src.cli.commands.dialogs.backfill_account",
            new_callable=AsyncMock,
            return_value={"dialogs": 1, "archived": 3, "errors": 0},
        ) as fake_backfill,
    ):
        _run(_ns(dialogs_action="archive-history", phone="+1234567890", chat_id="4242"), pool, cli_db)

    assert fake_backfill.await_args.kwargs["chat_ids"] == {4242}


def test_cli_dialogs_archive_history_rejects_non_numeric_chat_id(cli_db, capsys):
    """Не-числовой --chat-id — дружелюбное сообщение, а не traceback (ревью #1455)."""
    pool = _mock_pool()
    with (
        patch("src.cli.commands.dialogs.serve_is_running", return_value=False),
        patch("src.cli.commands.dialogs.backfill_account", new_callable=AsyncMock) as fake_backfill,
    ):
        _run(_ns(dialogs_action="archive-history", phone="+1234567890", chat_id="abc"), pool, cli_db)

    out = capsys.readouterr().out
    assert "Invalid --chat-id" in out
    fake_backfill.assert_not_awaited()


def test_cli_dialogs_archive_history_incomplete_gate(cli_db, capsys):
    """Насыщенный гейт помечает прогон незавершённым — CLI не печатает «готово» молча."""
    pool = _mock_pool()
    with (
        patch("src.cli.commands.dialogs.serve_is_running", return_value=False),
        patch(
            "src.cli.commands.dialogs.backfill_account",
            new_callable=AsyncMock,
            return_value={"dialogs": 2, "archived": 1, "errors": 0, "incomplete": True},
        ),
    ):
        _run(_ns(dialogs_action="archive-history", phone="+1234567890", chat_id=None), pool, cli_db)

    out = capsys.readouterr().out
    assert "НЕ ЗАВЕРШЁН" in out
    assert "продолжится с курсоров" in out
