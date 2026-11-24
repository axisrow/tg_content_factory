"""CLI-тесты группы `ping` (#1415): CRUD — DB-only, run-due — на фейковом пуле.

Паттерн — как в test_dm_reply_drafts: `impl` закрывает БД в finally, каждый
вызов начинается с переоткрытия fixture-инстанса; `runtime.init_db` (и для
run-due `runtime.init_pool`) патчится на cli_db.
"""

from __future__ import annotations

import asyncio
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import typer

from tg_agent.cli.commands import ping as ping_cli
from tg_agent.models import CollectionTaskType, DialogMessage, MessagesResult
from tg_agent.services.telegram_actions import TelegramActionService

NOW = datetime(2026, 10, 17, 12, 0, tzinfo=timezone.utc)


def _reopen(db):
    """Переоткрыть закрытый fixture-инстанс для инспекции (паттерн handoff-тестов)."""
    if db._connection.db is None:
        asyncio.run(db.initialize())


def _run_ping_impl(coro_factory, cli_db, capsys, *, config_path="cfg"):
    """impl закрывает БД в finally — каждый вызов начинает с переоткрытия."""
    _reopen(cli_db)

    async def fake_init_db(_):
        return None, cli_db

    with patch("tg_agent.cli.commands.ping.runtime.init_db", side_effect=fake_init_db):
        ping_cli.run_async(coro_factory())
    return capsys.readouterr().out


def _monitors(db):
    _reopen(db)
    return asyncio.run(db.repos.ping_monitors.list())


def test_cli_create_writes_monitor(cli_db, capsys):
    out = _run_ping_impl(
        lambda: ping_cli.create_impl(
            "cfg",
            phone="+1234",
            dialog_id=777,
            username=None,
            text="привет, как ты?",
            days=14,
            title="Иван",
        ),
        cli_db,
        capsys,
    )

    assert "Created ping monitor #1" in out
    monitors = _monitors(cli_db)
    assert len(monitors) == 1
    assert monitors[0].phone == "+1234"
    assert monitors[0].dialog_id == 777
    assert monitors[0].dialog_title == "Иван"
    assert monitors[0].silence_threshold_days == 14
    assert monitors[0].nudge_text == "привет, как ты?"


def test_cli_create_rejects_duplicate(cli_db, capsys):
    def _create():
        return ping_cli.create_impl(
            "cfg", phone="+1234", dialog_id=777, username=None, text="ping", days=30, title=None,
        )

    _run_ping_impl(_create, cli_db, capsys)
    with pytest.raises(typer.Exit):
        _run_ping_impl(_create, cli_db, capsys)
    out = capsys.readouterr().out
    assert "already monitored" in out
    assert len(_monitors(cli_db)) == 1


def test_cli_create_requires_dialog_or_username(cli_db, capsys):
    with pytest.raises(typer.Exit):
        _run_ping_impl(
            lambda: ping_cli.create_impl(
                "cfg", phone="+1234", dialog_id=None, username=None,
                text="ping", days=30, title=None,
            ),
            cli_db,
            capsys,
        )
    assert _monitors(cli_db) == []


def test_cli_create_resolves_username_via_pool(cli_db, capsys):
    pool = MagicMock()
    pool.resolve_any_entity = AsyncMock(
        return_value={"channel_id": 555, "title": "@friend", "channel_type": "dm"}
    )
    pool.disconnect_all = AsyncMock()

    async def fake_init_db(_):
        return None, cli_db

    async def fake_init_pool(_config, _db):
        return None, pool

    _reopen(cli_db)
    with (
        patch("tg_agent.cli.commands.ping.runtime.init_db", side_effect=fake_init_db),
        patch("tg_agent.cli.commands.ping.runtime.init_pool", side_effect=fake_init_pool),
    ):
        ping_cli.run_async(
            ping_cli.create_impl(
                "cfg", phone="+1234", dialog_id=None, username="@friend",
                text="ping", days=30, title=None,
            )
        )

    out = capsys.readouterr().out
    assert "Created ping monitor #1" in out
    monitors = _monitors(cli_db)
    assert monitors[0].dialog_id == 555
    assert monitors[0].dialog_title == "@friend"
    pool.disconnect_all.assert_awaited_once()


def test_cli_list_prints_monitors(cli_db, capsys):
    _run_ping_impl(
        lambda: ping_cli.create_impl(
            "cfg", phone="+1234", dialog_id=777, username=None, text="ping", days=30, title=None,
        ),
        cli_db,
        capsys,
    )

    out = _run_ping_impl(
        lambda: ping_cli.list_impl("cfg", active_only=False), cli_db, capsys
    )

    assert "+1234" in out
    assert "777" in out


def test_cli_update_changes_fields(cli_db, capsys):
    def _create():
        return ping_cli.create_impl(
            "cfg", phone="+1234", dialog_id=777, username=None, text="old", days=30, title=None,
        )

    _run_ping_impl(_create, cli_db, capsys)
    out = _run_ping_impl(
        lambda: ping_cli.update_impl(
            "cfg", monitor_id=1, text="новый текст", days=7, active=False, inactive=True,
        ),
        cli_db,
        capsys,
    )

    assert "Updated ping monitor #1" in out
    monitor = _monitors(cli_db)[0]
    assert monitor.nudge_text == "новый текст"
    assert monitor.silence_threshold_days == 7
    assert monitor.is_active is False


def test_cli_update_unknown_monitor_exits(cli_db, capsys):
    with pytest.raises(typer.Exit):
        _run_ping_impl(
            lambda: ping_cli.update_impl(
                "cfg", monitor_id=99, text=None, days=None, active=False, inactive=False,
            ),
            cli_db,
            capsys,
        )
    assert "not found" in capsys.readouterr().out


def test_cli_delete_removes_monitor(cli_db, capsys):
    def _create():
        return ping_cli.create_impl(
            "cfg", phone="+1234", dialog_id=777, username=None, text="ping", days=30, title=None,
        )

    _run_ping_impl(_create, cli_db, capsys)
    out = _run_ping_impl(
        lambda: ping_cli.delete_impl("cfg", monitor_id=1), cli_db, capsys
    )

    assert "Deleted ping monitor #1" in out
    assert _monitors(cli_db) == []


def test_cli_run_due_invokes_service_and_reports(cli_db, capsys):
    def _create():
        return ping_cli.create_impl(
            "cfg", phone="+1234", dialog_id=777, username=None, text="ping", days=30, title=None,
        )

    _run_ping_impl(_create, cli_db, capsys)

    pool = MagicMock()
    pool.disconnect_all = AsyncMock()
    actions = MagicMock(spec=TelegramActionService)
    actions.read_history = AsyncMock(
        return_value=MessagesResult(
            phone="+1234",
            dialog_id=777,
            messages=[
                DialogMessage(
                    id=1, dialog_id=777, out=True, date=NOW - timedelta(days=40), text="..."
                )
            ],
        )
    )
    actions.send_message = AsyncMock()

    async def fake_init_db(_):
        return None, cli_db

    async def fake_init_pool(_config, _db):
        return None, pool

    _reopen(cli_db)
    with (
        patch("tg_agent.cli.commands.ping.runtime.init_db", side_effect=fake_init_db),
        patch("tg_agent.cli.commands.ping.runtime.init_pool", side_effect=fake_init_pool),
        patch(
            "tg_agent.cli.commands.ping.worker_handoff.serve_is_running", return_value=False
        ),
        # run_due_impl импортирует TelegramActionService локально — патчим источник.
        patch("tg_agent.services.telegram_actions.TelegramActionService", return_value=actions),
    ):
        ping_cli.run_async(ping_cli.run_due_impl("cfg"))

    out = capsys.readouterr().out
    assert "Checked 1 ping monitor(s)." in out
    actions.send_message.assert_awaited_once_with(
        phone="+1234", recipient=777, text="ping", is_user=True, source="ping", ref="1"
    )
    pool.disconnect_all.assert_awaited_once()


def test_cli_run_due_in_a_gateway_project_respects_the_dialog_owner(cli_db, capsys):
    """Gateway project, no daemon: automatic replies in the dialog belong to another project."""
    def _create():
        return ping_cli.create_impl(
            "cfg", phone="+1234", dialog_id=777, username=None, text="ping", days=30, title=None,
        )

    _run_ping_impl(_create, cli_db, capsys)

    relay = MagicMock()
    relay.call = AsyncMock(return_value=False)
    pool = MagicMock(relay=relay)
    pool.disconnect_all = AsyncMock()
    actions = MagicMock(spec=TelegramActionService)
    actions.read_history = AsyncMock(
        return_value=MessagesResult(
            phone="+1234",
            dialog_id=777,
            messages=[DialogMessage(id=1, dialog_id=777, out=True, date=NOW - timedelta(days=40), text="...")],
        )
    )
    actions.send_message = AsyncMock()

    async def fake_init_db(_):
        return SimpleNamespace(gateway={"project_id": "project-a"}), cli_db

    async def fake_init_pool(_config, _db):
        return None, pool

    _reopen(cli_db)
    with (
        patch("tg_agent.cli.commands.ping.runtime.init_db", side_effect=fake_init_db),
        patch("tg_agent.cli.commands.ping.runtime.init_pool", side_effect=fake_init_pool),
        patch("tg_agent.cli.commands.ping.worker_handoff.serve_is_running", return_value=False),
        patch("tg_agent.services.telegram_actions.TelegramActionService", return_value=actions),
        patch("tg_agent.gateway.catalog.gateway_account_id", AsyncMock(return_value="account-p")),
    ):
        ping_cli.run_async(ping_cli.run_due_impl("cfg"))

    actions.send_message.assert_not_awaited()
    relay.call.assert_awaited_once_with("reply_owner.check", account_id="account-p", dialog_id=777)


def test_cli_run_due_hands_off_to_running_worker(cli_db, capsys):
    """Живой демон: run-due ставит PING_DUE задачу, своего пула не открывает."""
    def _create():
        return ping_cli.create_impl(
            "cfg", phone="+1234", dialog_id=777, username=None, text="ping", days=30, title=None,
        )

    _run_ping_impl(_create, cli_db, capsys)

    pool = MagicMock()
    pool.disconnect_all = AsyncMock()

    async def fake_init_db(_):
        return None, cli_db

    async def fake_init_pool(_config, _db):
        raise AssertionError("must not open a pool while the daemon owns the sessions")

    _reopen(cli_db)
    with (
        patch("tg_agent.cli.commands.ping.runtime.init_db", side_effect=fake_init_db),
        patch("tg_agent.cli.commands.ping.runtime.init_pool", side_effect=fake_init_pool),
        patch(
            "tg_agent.cli.commands.ping.worker_handoff.serve_is_running", return_value=True
        ),
    ):
        ping_cli.run_async(ping_cli.run_due_impl("cfg"))

    out = capsys.readouterr().out
    assert "PING_DUE task #" in out
    assert "the worker will run it" in out
    pool.disconnect_all.assert_not_awaited()
    _reopen(cli_db)
    assert asyncio.run(cli_db.repos.tasks.has_active_task(CollectionTaskType.PING_DUE))
