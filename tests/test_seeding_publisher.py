"""Тесты публикатора посевов (#1757): SEED_DUE-ран, идемпотентность, рельсы.

Живых аккаунтов и живой Telegram нет: отправка — фейк с контрактом
``TelegramActionService.send_message``, хранилище — реальный SQLite (fixture
``db``). Идемпотентность = атомарный claim (``scheduled→sending``) +
guarded-исход: повторный due-прогон не может отправить реплику дважды.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest

from tg_agent.models import (
    Channel,
    Message,
    SeedingMessage,
    SeedingMessageStatus,
    SeedingThread,
    SeedingThreadStatus,
)
from tg_agent.services.seeding_publisher import (
    PER_ACCOUNT_DAILY_LIMIT,
    SeedingPublisherService,
    validate_schedule_rails,
)

CHAT_ID = -1001234
PHONE_A = "+111"
PHONE_B = "+222"


class FakeActions:
    """Контракт TelegramActionService.send_message; падает по номеру телефона."""

    def __init__(self, fail_phones: frozenset[str] = frozenset()):
        self.calls: list[dict] = []
        self.fail_phones = fail_phones
        self._next_id = 500

    async def send_message(self, *, phone, recipient, text, link_preview=False, reply_to=None):
        if phone in self.fail_phones:
            raise RuntimeError("FloodWait 30s")
        self.calls.append(
            {"phone": phone, "recipient": recipient, "text": text, "reply_to": reply_to}
        )
        self._next_id += 1
        return SimpleNamespace(phone=phone, message_id=self._next_id)


def _row(
    position: int,
    *,
    planned_at: datetime,
    phone: str = PHONE_A,
    text: str | None = None,
    reply_to_position: int | None = None,
    status: SeedingMessageStatus = SeedingMessageStatus.DRAFT,
) -> SeedingMessage:
    return SeedingMessage(
        thread_id=0,  # подменяется create_messages
        position=position,
        account_phone=phone,
        persona_name="Аня" if phone == PHONE_A else "Боря",
        text=text or f"реплика номер {position}",
        planned_at=planned_at,
        status=status,
        reply_to_position=reply_to_position,
    )


async def _make_thread(db, *, chat: int = CHAT_ID, personas: dict[str, str] | None = None) -> int:
    return await db.repos.seeding.create_thread(
        SeedingThread(chat_channel_id=chat, personas=personas or {"Аня": PHONE_A})
    )


async def _approved_thread(db, rows: list[SeedingMessage], **kwargs) -> int:
    """Тред в состоянии этапа schedule: scheduled + approved реплики."""
    thread_id = await _make_thread(db, **kwargs)
    await db.repos.seeding.create_messages(thread_id, rows)
    assert await db.repos.seeding.approve_thread(thread_id) == len(rows)
    return thread_id


async def _armed_thread(db, rows: list[SeedingMessage], **kwargs) -> int:
    """Тред, поставленный на расписание: реплики scheduled (вход SEED_DUE-рана)."""
    thread_id = await _approved_thread(db, rows, **kwargs)
    assert await db.repos.seeding.schedule_thread_messages(thread_id) == len(rows)
    return thread_id


async def _seed_chat_profile(db, *, base: datetime) -> None:
    """Канал + сообщения в часах 10..12 UTC — тихие часы = все остальные."""
    await db.repos.channels.add_channel(
        Channel(channel_id=CHAT_ID, title="Тестовый чат", channel_type="supergroup")
    )
    for i, offset_minutes in enumerate((0, 25, 65, 120)):
        await db.repos.messages.insert_message(
            Message(
                channel_id=CHAT_ID,
                message_id=i + 1,
                date=base + timedelta(minutes=offset_minutes),
                text=f"живое сообщение номер {i + 1}",
                sender_id=(i % 2) + 1,
                sender_name="Аня" if i % 2 == 0 else "Боря",
            )
        )


def _at(hour: int, minute: int = 0, second: int = 0) -> datetime:
    return datetime.now(timezone.utc).replace(
        hour=hour, minute=minute, second=second, microsecond=0
    )


# --------------------------------------------------------------------------- #
# SEED_DUE-ран: отправка, идемпотентность, reply-цепочка, переходы
# --------------------------------------------------------------------------- #


@pytest.mark.anyio
async def test_run_due_sends_and_marks_sent_with_tg_id(db):
    past = datetime.now(timezone.utc) - timedelta(minutes=5)
    thread_id = await _armed_thread(db, [_row(1, planned_at=past)])
    actions = FakeActions()

    sent = await SeedingPublisherService(db, actions).run_due()

    assert sent == 1
    assert len(actions.calls) == 1
    call = actions.calls[0]
    assert call["phone"] == PHONE_A
    assert call["recipient"] == CHAT_ID
    msg = await db.repos.seeding.list_messages_for_thread(thread_id)
    assert msg[0].status is SeedingMessageStatus.SENT
    assert msg[0].tg_message_id == 501
    assert msg[0].sent_at is not None


@pytest.mark.anyio
async def test_run_due_twice_does_not_duplicate(db):
    """Повторный due-прогон: отправленная реплика не в scheduled — фейк звался один раз."""
    past = datetime.now(timezone.utc) - timedelta(minutes=5)
    await _armed_thread(db, [_row(1, planned_at=past)])
    publisher = SeedingPublisherService(db, FakeActions())

    assert await publisher.run_due() == 1
    assert await publisher.run_due() == 0
    assert len(publisher._actions.calls) == 1


@pytest.mark.anyio
async def test_claim_moves_to_sending_and_blocks_second_claim(db):
    """Гонка двух потребителей: claim атомарен, вторая рука берёт следующую строку."""
    now = datetime.now(timezone.utc)
    past = now - timedelta(minutes=5)
    thread_id = await _armed_thread(
        db, [_row(1, planned_at=past), _row(2, planned_at=past)]
    )
    first = await db.repos.seeding.claim_next_due_message(now)
    assert first is not None and first.status is SeedingMessageStatus.SENDING
    second = await db.repos.seeding.claim_next_due_message(now)
    assert second is not None and second.id != first.id
    assert await db.repos.seeding.claim_next_due_message(now) is None
    # Future-реплика не берётся.
    (future_id,) = await db.repos.seeding.create_messages(
        thread_id,
        [_row(3, planned_at=now + timedelta(hours=1), status=SeedingMessageStatus.SCHEDULED)],
    )
    assert await db.repos.seeding.claim_next_due_message(now) is None
    assert (await db.repos.seeding.get_message(future_id)).status is SeedingMessageStatus.SCHEDULED


@pytest.mark.anyio
async def test_run_due_only_claims_due_not_future(db):
    now = datetime.now(timezone.utc)
    thread_id = await _armed_thread(
        db,
        [
            _row(1, planned_at=now - timedelta(minutes=5)),
            _row(2, planned_at=now + timedelta(hours=2)),
        ],
    )
    publisher = SeedingPublisherService(db, FakeActions())

    assert await publisher.run_due(now=now) == 1
    messages = await db.repos.seeding.list_messages_for_thread(thread_id)
    assert messages[0].status is SeedingMessageStatus.SENT
    assert messages[1].status is SeedingMessageStatus.SCHEDULED


@pytest.mark.anyio
async def test_send_failure_marks_failed_with_error_and_rest_continue(db):
    """Сбой → failed (+error_text); остальные реплики рана не прерываются."""
    now = datetime.now(timezone.utc)
    past = now - timedelta(minutes=5)
    thread_id = await _armed_thread(
        db,
        [
            _row(1, planned_at=past, phone=PHONE_B),
            _row(2, planned_at=past),
        ],
    )
    actions = FakeActions(fail_phones=frozenset({PHONE_B}))
    sent = await SeedingPublisherService(db, actions).run_due(now=now)

    assert sent == 1
    messages = await db.repos.seeding.list_messages_for_thread(thread_id)
    assert messages[0].status is SeedingMessageStatus.FAILED
    assert "FloodWait" in messages[0].error_text
    assert messages[0].tg_message_id is None
    assert messages[1].status is SeedingMessageStatus.SENT


@pytest.mark.anyio
async def test_send_failure_with_empty_message_falls_back_to_class_name(db):
    """Пустой str() исключения не даёт пустого error_text (класс исключения)."""

    class SilentError(Exception):
        pass

    now = datetime.now(timezone.utc)
    past = now - timedelta(minutes=5)
    thread_id = await _armed_thread(db, [_row(1, planned_at=past, phone=PHONE_B)])
    actions = FakeActions(fail_phones=frozenset({PHONE_B}))

    async def raise_silent(**kwargs):
        raise SilentError()

    actions.send_message = raise_silent
    await SeedingPublisherService(db, actions).run_due(now=now)

    msg = (await db.repos.seeding.list_messages_for_thread(thread_id))[0]
    assert msg.status is SeedingMessageStatus.FAILED
    assert msg.error_text == "SilentError"


@pytest.mark.anyio
async def test_reply_chain_uses_sibling_tg_message_id(db):
    now = datetime.now(timezone.utc)
    past = now - timedelta(minutes=5)
    await _armed_thread(
        db,
        [
            _row(1, planned_at=past),
            _row(2, planned_at=past, reply_to_position=1),
        ],
    )
    publisher = SeedingPublisherService(db, FakeActions())
    await publisher.run_due(now=now)

    first, second = publisher._actions.calls
    assert first["reply_to"] is None
    assert second["reply_to"] == 501  # tg_message_id первой реплики


@pytest.mark.anyio
async def test_reply_chain_degrades_without_sent_sibling(db):
    """Собеседник не отправлен — реплика уходит без reply, а не падает."""
    now = datetime.now(timezone.utc)
    past = now - timedelta(minutes=5)
    thread_id = await _armed_thread(
        db,
        [
            # Реплика 1 упадёт (фейк-сбой) — у 2 нет tg_message_id для reply.
            _row(1, planned_at=past, phone=PHONE_B),
            _row(2, planned_at=past, reply_to_position=1),
        ],
    )
    actions = FakeActions(fail_phones=frozenset({PHONE_B}))
    await SeedingPublisherService(db, actions).run_due(now=now)

    messages = await db.repos.seeding.list_messages_for_thread(thread_id)
    assert messages[0].status is SeedingMessageStatus.FAILED
    assert messages[1].status is SeedingMessageStatus.SENT
    # Ушедший вызов ровно один (реплика 2), reply нет — собеседник не отправлен.
    assert len(actions.calls) == 1
    assert actions.calls[0]["reply_to"] is None


@pytest.mark.anyio
async def test_thread_transitions_active_then_done(db):
    now = datetime.now(timezone.utc)
    past = now - timedelta(minutes=5)
    thread_id = await _armed_thread(
        db, [_row(1, planned_at=past), _row(2, planned_at=past)]
    )
    await SeedingPublisherService(db, FakeActions()).run_due(now=now)

    thread = await db.repos.seeding.get_thread(thread_id)
    assert thread.status is SeedingThreadStatus.DONE


@pytest.mark.anyio
async def test_thread_stays_scheduled_when_everything_failed(db):
    now = datetime.now(timezone.utc)
    past = now - timedelta(minutes=5)
    thread_id = await _armed_thread(db, [_row(1, planned_at=past, phone=PHONE_B)])
    actions = FakeActions(fail_phones=frozenset({PHONE_B}))
    await SeedingPublisherService(db, actions).run_due(now=now)

    thread = await db.repos.seeding.get_thread(thread_id)
    assert thread.status is SeedingThreadStatus.SCHEDULED


@pytest.mark.anyio
async def test_chain_enqueues_next_seed_due_on_remaining_planned_at(db):
    now = datetime.now(timezone.utc)
    future = now + timedelta(hours=2)
    await _armed_thread(
        db,
        [
            _row(1, planned_at=now - timedelta(minutes=5)),
            _row(2, planned_at=future),
        ],
    )
    publisher = SeedingPublisherService(db, FakeActions())
    await publisher.run_due(now=now)

    seed_tasks = await _list_seed_due_tasks(db)
    assert len(seed_tasks) == 1
    assert seed_tasks[0]["run_after"] == future.astimezone(timezone.utc).isoformat()


async def _list_seed_due_tasks(db) -> list[dict]:
    cur = await db.db.execute(
        "SELECT id, task_type, run_after, status FROM collection_tasks WHERE task_type = 'seed_due'"
    )
    return [dict(row) for row in await cur.fetchall()]


@pytest.mark.anyio
async def test_chain_skipped_when_nothing_scheduled_left(db):
    now = datetime.now(timezone.utc)
    await _armed_thread(db, [_row(1, planned_at=now - timedelta(minutes=5))])
    publisher = SeedingPublisherService(db, FakeActions())
    await publisher.run_due(now=now)

    assert await _list_seed_due_tasks(db) == []


# --------------------------------------------------------------------------- #
# reconcile старта воркера
# --------------------------------------------------------------------------- #


@pytest.mark.anyio
async def test_reconcile_fails_stuck_sending_without_resend(db):
    """Зависший sending (воркер умер с RPC в полёте) → failed, отправки нет."""
    now = datetime.now(timezone.utc)
    past = now - timedelta(minutes=5)
    thread_id = await _armed_thread(db, [_row(1, planned_at=past)])
    (msg_id,) = [
        m.id for m in await db.repos.seeding.list_messages_for_thread(thread_id)
    ]
    assert await db.repos.seeding.set_message_status(
        msg_id, SeedingMessageStatus.SENDING
    )
    actions = FakeActions()

    stuck = await SeedingPublisherService(db, actions).reconcile_on_startup()

    assert stuck == 1
    assert actions.calls == []
    msg = await db.repos.seeding.get_message(msg_id)
    assert msg.status is SeedingMessageStatus.FAILED
    assert "перезапущен" in msg.error_text


@pytest.mark.anyio
async def test_reconcile_rearms_seed_due_and_dedups(db):
    now = datetime.now(timezone.utc)
    future = now + timedelta(hours=3)
    await _armed_thread(db, [_row(1, planned_at=future)])
    publisher = SeedingPublisherService(db, FakeActions())

    await publisher.reconcile_on_startup()
    await publisher.reconcile_on_startup()

    tasks = await _list_seed_due_tasks(db)
    assert len(tasks) == 1  # dedup по активной задаче
    assert tasks[0]["status"] == "pending"


# --------------------------------------------------------------------------- #
# Рельсы безопасности (этап schedule)
# --------------------------------------------------------------------------- #


@pytest.mark.anyio
async def test_rails_happy_path_passes(db):
    base = _at(10)
    await _seed_chat_profile(db, base=base)
    thread_id = await _approved_thread(
        db,
        [
            _row(1, planned_at=_at(10, 30)),
            _row(2, planned_at=_at(10, 35), phone=PHONE_B, reply_to_position=1),
        ],
    )
    assert await validate_schedule_rails(db, thread_id, now=base) == []


@pytest.mark.anyio
async def test_rails_reject_links(db):
    base = _at(10)
    await _seed_chat_profile(db, base=base)
    thread_id = await _approved_thread(
        db, [_row(1, planned_at=_at(10, 30), text="смотри https://example.com/post")]
    )
    violations = await validate_schedule_rails(db, thread_id, now=base)
    assert len(violations) == 1
    assert "ссылки" in violations[0] and "#1" in violations[0]


@pytest.mark.anyio
async def test_rails_reject_cross_account_gap(db):
    base = _at(10)
    await _seed_chat_profile(db, base=base)
    thread_id = await _approved_thread(
        db,
        [
            _row(1, planned_at=_at(10, 30)),
            # 10 секунд после реплики 1 — меньше 20s межаккаунтного зазора.
            _row(2, planned_at=_at(10, 30, 10), phone=PHONE_B),
        ],
    )
    violations = await validate_schedule_rails(db, thread_id, now=base)
    assert len(violations) == 1
    assert "зазор" in violations[0]


@pytest.mark.anyio
async def test_rails_reject_overdue_planned_at(db):
    """Past-рельс: просроченный префикс уйдёт залпом (зазор = RPC-латентность)."""
    await _seed_chat_profile(db, base=_at(10))
    # now = 10:30; профиль активен в часах 10..12 → просроченные реплики не
    # задевают рельсу тишины, срабатывает только past-рельс.
    now = _at(10, 30)
    thread_id = await _approved_thread(
        db,
        [
            # 10 минут назад — за пределами допуска 5 минут.
            _row(1, planned_at=_at(10, 20)),
            # 2 минуты назад — в допуске: schedule с небольшим опозданием ок.
            _row(2, planned_at=_at(10, 28)),
        ],
    )
    violations = await validate_schedule_rails(db, thread_id, now=now)
    assert len(violations) == 1
    assert "в прошлом" in violations[0] and "#1" in violations[0]


@pytest.mark.anyio
async def test_rails_reject_daily_limit(db):
    base = _at(10)
    await _seed_chat_profile(db, base=base)
    rows = [
        _row(i + 1, planned_at=_at(10, 30 + i), text=f"уникальная реплика номер {i + 1}")
        for i in range(PER_ACCOUNT_DAILY_LIMIT + 1)
    ]
    thread_id = await _approved_thread(db, rows)
    violations = await validate_schedule_rails(db, thread_id, now=base)
    assert any("дневного лимита" in v for v in violations)


@pytest.mark.anyio
async def test_rails_daily_limit_counts_other_threads(db):
    base = _at(10)
    await _seed_chat_profile(db, base=base)
    other_rows = [
        _row(i + 1, planned_at=_at(11, i), text=f"чужой тред реплика {i + 1}")
        for i in range(9)
    ]
    other_id = await _approved_thread(db, other_rows)
    # Другой тред уходим в scheduled (реплики approved→scheduled), вклад в лимит остаётся.
    assert await db.repos.seeding.schedule_thread_messages(other_id) == 9

    own_rows = [
        _row(i + 1, planned_at=_at(11, 30 + i), text=f"свой тред реплика {i + 1}")
        for i in range(4)
    ]
    thread_id = await _approved_thread(db, own_rows)
    violations = await validate_schedule_rails(db, thread_id, now=base)
    assert any("дневного лимита" in v and "13" in v for v in violations)


@pytest.mark.anyio
async def test_rails_reject_quiet_hours(db):
    base = _at(10)
    await _seed_chat_profile(db, base=base)
    # +14:15 от now=10:00 → 00:15 следующего дня: всегда в будущем и в тихих
    # часах (профиль активен только 10..12 UTC), не зависит от времени прогона.
    thread_id = await _approved_thread(
        db, [_row(1, planned_at=base + timedelta(hours=14, minutes=15))]
    )
    violations = await validate_schedule_rails(db, thread_id, now=base)
    assert len(violations) == 1
    assert "тихие часы" in violations[0]


@pytest.mark.anyio
async def test_rails_require_approved_state(db):
    thread_id = await _make_thread(db)
    await db.repos.seeding.create_messages(
        thread_id, [_row(1, planned_at=_at(10, 30))]
    )
    violations = await validate_schedule_rails(db, thread_id)
    assert len(violations) == 1
    assert "approve" in violations[0]


@pytest.mark.anyio
async def test_rails_missing_thread(db):
    assert await validate_schedule_rails(db, 424242) == ["тред #424242 не найден"]
