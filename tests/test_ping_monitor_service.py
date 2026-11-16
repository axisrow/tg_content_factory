"""Тесты PingMonitorService: затухающая прогрессия и правило тишины (#1415).

Прогрессия — чистая функция, юнит без БД. Поведение run_due — на моках бандла и
TelegramActionService (никакой живой Telegram).
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from tg_agent.models import DialogMessage, MessagesResult, PingMonitor
from tg_agent.services.ping_monitor_service import (
    PingMonitorService,
    _next_threshold_days,
)

NOW = datetime(2026, 10, 17, 12, 0, tzinfo=timezone.utc)


@pytest.fixture(autouse=True)
def _frozen_now():
    """run_due() берёт datetime.now() из своего модуля — замораживаем на NOW."""
    with patch("tg_agent.services.ping_monitor_service.datetime") as fake_dt:
        fake_dt.now.return_value = NOW
        yield


def _monitor(**overrides) -> PingMonitor:
    defaults = dict(
        id=1,
        phone="+1234",
        dialog_id=777,
        silence_threshold_days=30,
        nudge_text="привет, как ты?",
        cycle_count=0,
        is_active=True,
    )
    defaults.update(overrides)
    return PingMonitor(**defaults)


def _repo(monitors: list[PingMonitor] | None = None) -> MagicMock:
    repo = MagicMock()
    repo.create = AsyncMock(return_value=1)
    repo.list = AsyncMock(return_value=monitors or [])
    repo.get = AsyncMock(return_value=None)
    repo.update = AsyncMock()
    repo.delete = AsyncMock()
    return repo


def _actions(last_message: DialogMessage | None) -> MagicMock:
    actions = MagicMock()
    actions.read_history = AsyncMock(
        return_value=MessagesResult(
            phone="+1234",
            dialog_id=777,
            messages=[last_message] if last_message else [],
        )
    )
    actions.send_message = AsyncMock()
    return actions


def _msg(out: bool, minutes_ago: int) -> DialogMessage:
    return DialogMessage(
        id=10,
        dialog_id=777,
        out=out,
        date=NOW - timedelta(minutes=minutes_ago),
        text="...",
    )


# --- затухающая прогрессия (чистая функция) ---


@pytest.mark.parametrize(
    ("base_days", "cycle_count", "expected"),
    [
        (30, 0, 30),   # базовый порог
        (30, 1, 1),    # +1 день после 1-го пинга
        (30, 2, 7),    # +7 дней
        (30, 3, 30),   # +30 дней
        (30, 4, 30),   # далее каждые 30 дней
        (30, 10, 30),  # бессрочно
        (7, 0, 7),     # base_days параметризуем
        (7, 2, 7),     # шаг прогрессии от base не зависит
    ],
)
def test_next_threshold_days_progression(base_days, cycle_count, expected):
    assert _next_threshold_days(base_days, cycle_count) == expected


# --- CRUD-делегирование ---


async def test_crud_delegates_to_repo():
    repo = _repo()
    service = PingMonitorService(repo, MagicMock())
    monitor = _monitor()

    assert await service.create_monitor(monitor) == 1
    repo.create.assert_awaited_once_with(monitor)
    await service.get_monitor(1)
    repo.get.assert_awaited_once_with(1)
    await service.list_monitors(active_only=True)
    repo.list.assert_awaited_once_with(True)
    await service.update_monitor(1, nudge_text="новый")
    repo.update.assert_awaited_once_with(1, nudge_text="новый")
    await service.delete_monitor(1)
    repo.delete.assert_awaited_once_with(1)


# --- run_due: правило тишины ---


async def test_run_due_contact_replied_resets_cycle_without_ping():
    monitor = _monitor(cycle_count=3, silence_threshold_days=30)
    repo = _repo([monitor])
    actions = _actions(_msg(out=False, minutes_ago=5))
    service = PingMonitorService(repo, actions)

    checked = await service.run_due()

    assert checked == 1
    actions.send_message.assert_not_awaited()
    repo.update.assert_awaited_once_with(
        1,
        cycle_count=0,
        last_incoming_message_at=NOW - timedelta(minutes=5),
        last_checked_at=NOW,
        error="",
    )


async def test_run_due_outgoing_below_threshold_no_ping():
    # Моё сообщение 10 дней назад, базовый порог 30 — тишины ещё недостаточно.
    monitor = _monitor(cycle_count=0, silence_threshold_days=30)
    repo = _repo([monitor])
    actions = _actions(_msg(out=True, minutes_ago=10 * 24 * 60))
    service = PingMonitorService(repo, actions)

    checked = await service.run_due()

    assert checked == 1
    actions.send_message.assert_not_awaited()
    repo.update.assert_awaited_once_with(
        1,
        last_outgoing_message_at=NOW - timedelta(minutes=10 * 24 * 60),
        last_checked_at=NOW,
        error="",
    )


async def test_run_due_first_ping_after_base_threshold():
    monitor = _monitor(cycle_count=0, silence_threshold_days=30)
    repo = _repo([monitor])
    # Моё сообщение 31 день назад, пингов не было → порог base=30 пройден.
    actions = _actions(_msg(out=True, minutes_ago=31 * 24 * 60))
    service = PingMonitorService(repo, actions)

    checked = await service.run_due()

    assert checked == 1
    actions.send_message.assert_awaited_once_with(
        phone="+1234", recipient=777, text="привет, как ты?", is_user=True
    )
    update_kwargs = repo.update.call_args.kwargs
    assert update_kwargs["cycle_count"] == 1
    assert update_kwargs["last_nudge_sent_at"] == NOW


async def test_run_due_second_ping_one_day_after_first():
    first_ping = NOW - timedelta(days=2)
    monitor = _monitor(cycle_count=1, silence_threshold_days=30, last_nudge_sent_at=first_ping)
    repo = _repo([monitor])
    # Последнее сообщение — мой пинг двухдневной давности; цикл 1 → +1 день.
    actions = _actions(_msg(out=True, minutes_ago=2 * 24 * 60))
    service = PingMonitorService(repo, actions)

    await service.run_due()

    actions.send_message.assert_awaited_once()
    assert repo.update.call_args.kwargs["cycle_count"] == 2


async def test_run_due_second_ping_not_yet_due():
    first_ping = NOW - timedelta(hours=12)
    monitor = _monitor(cycle_count=1, silence_threshold_days=30, last_nudge_sent_at=first_ping)
    repo = _repo([monitor])
    actions = _actions(_msg(out=True, minutes_ago=12 * 60))
    service = PingMonitorService(repo, actions)

    await service.run_due()

    actions.send_message.assert_not_awaited()


async def test_run_due_reply_older_than_recent_own_message_uses_own_message_anchor():
    # Старый пинг (40 дней назад), затем моё новое сообщение 31 день назад:
    # отсчёт — от свежего сообщения, не от старой метки пинга.
    monitor = _monitor(
        cycle_count=1,
        silence_threshold_days=30,
        last_nudge_sent_at=NOW - timedelta(days=40),
    )
    repo = _repo([monitor])
    actions = _actions(_msg(out=True, minutes_ago=31 * 24 * 60))
    service = PingMonitorService(repo, actions)

    await service.run_due()

    actions.send_message.assert_awaited_once()


async def test_run_due_empty_dialog_just_checks_in():
    # Пустая история — ветка успеха: прошлая ошибка (flood wait и т.п.) очищается.
    monitor = _monitor()
    repo = _repo([monitor])
    actions = _actions(None)
    service = PingMonitorService(repo, actions)

    checked = await service.run_due()

    assert checked == 1
    actions.send_message.assert_not_awaited()
    repo.update.assert_awaited_once_with(1, last_checked_at=NOW, error="")


async def test_run_due_error_on_one_monitor_does_not_stop_others():
    failing = _monitor(id=1)
    healthy = _monitor(id=2)
    repo = _repo([failing, healthy])
    actions = MagicMock()
    call_count = {"n": 0}

    async def read_history(**kwargs):
        call_count["n"] += 1
        if call_count["n"] == 1:
            raise RuntimeError("flood wait")
        return MessagesResult(
            phone="+1234",
            dialog_id=777,
            messages=[_msg(out=True, minutes_ago=40 * 24 * 60)],
        )

    actions.read_history = AsyncMock(side_effect=read_history)
    actions.send_message = AsyncMock()
    service = PingMonitorService(repo, actions)

    checked = await service.run_due()

    assert checked == 1
    error_calls = [
        c for c in repo.update.call_args_list if c.kwargs.get("error") == "flood wait"
    ]
    assert len(error_calls) == 1
    actions.send_message.assert_awaited_once()  # здоровый монитор дошёл до пинга


async def test_run_due_send_failure_records_error_without_cycle_bump():
    monitor = _monitor()
    repo = _repo([monitor])
    actions = _actions(_msg(out=True, minutes_ago=31 * 24 * 60))
    actions.send_message = AsyncMock(side_effect=RuntimeError("cannot send"))
    service = PingMonitorService(repo, actions)

    checked = await service.run_due()

    # Ошибка записи обновления — монитор не считается проверенным, но
    # cycle_count не растёт: ретрай на следующем прогоне снова пришлёт пинг.
    assert checked == 0
    last_update = repo.update.call_args.kwargs
    assert last_update["error"] == "cannot send"
    assert "cycle_count" not in last_update
