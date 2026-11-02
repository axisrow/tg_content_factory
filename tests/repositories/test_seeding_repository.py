"""Тесты хранилища посева диалогов (#1755): миграция, guarded-переходы,
due-выборка, прогресс-агрегация."""

from datetime import datetime, timedelta, timezone

import aiosqlite
import pytest

from tg_agent.models import (
    SeedingMessage,
    SeedingMessageStatus,
    SeedingThread,
    SeedingThreadStatus,
)

EXPECTED_THREAD_COLUMNS = {
    "id",
    "chat_channel_id",
    "personas_json",
    "status",
    "composer_run_ref",
    "created_at",
    "updated_at",
}

EXPECTED_MESSAGE_COLUMNS = {
    "id",
    "thread_id",
    "position",
    "account_phone",
    "persona_name",
    "text",
    "reply_to_position",
    "planned_at",
    "sent_at",
    "tg_message_id",
    "status",
    "error_text",
    "created_at",
    "updated_at",
}


async def _make_thread(db, **kwargs) -> int:
    thread = SeedingThread(chat_channel_id=-1001234, personas={"Аня": "+111", "Боря": "+222"})
    for key, value in kwargs.items():
        setattr(thread, key, value)
    return await db.repos.seeding.create_thread(thread)


def _msg(
    position: int,
    *,
    planned_at=None,
    status=SeedingMessageStatus.DRAFT,
    reply_to_position=None,
) -> SeedingMessage:
    return SeedingMessage(
        thread_id=0,  # подменяется create_messages
        position=position,
        account_phone="+111",
        persona_name="Аня",
        text=f"реплика {position}",
        planned_at=planned_at,
        status=status,
        reply_to_position=reply_to_position,
    )


@pytest.mark.anyio
async def test_migrations_create_tables(db):
    """Свежая БД содержит обе таблицы с ожидаемым набором колонок."""
    cur = await db.db.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'seeding_%'")
    assert {row["name"] for row in await cur.fetchall()} == {"seeding_threads", "seeding_messages"}
    for table, expected in (
        ("seeding_threads", EXPECTED_THREAD_COLUMNS),
        ("seeding_messages", EXPECTED_MESSAGE_COLUMNS),
    ):
        cur = await db.db.execute(f"PRAGMA table_info({table})")
        assert {row["name"] for row in await cur.fetchall()} == expected


@pytest.mark.anyio
async def test_create_and_get_thread_roundtrip(db):
    thread_id = await _make_thread(db)
    fetched = await db.repos.seeding.get_thread(thread_id)
    assert fetched is not None
    assert fetched.chat_channel_id == -1001234
    assert fetched.personas == {"Аня": "+111", "Боря": "+222"}
    assert fetched.status is SeedingThreadStatus.DRAFT


@pytest.mark.anyio
async def test_get_missing_thread_returns_none(db):
    assert await db.repos.seeding.get_thread(99999) is None


@pytest.mark.anyio
async def test_list_threads_newest_first(db):
    first = await _make_thread(db)
    second = await _make_thread(db)
    ids = [t.id for t in await db.repos.seeding.list_threads()]
    assert ids == [second, first]


@pytest.mark.anyio
async def test_create_messages_transactional_with_positions(db):
    thread_id = await _make_thread(db)
    ids = await db.repos.seeding.create_messages(
        thread_id, [_msg(1), _msg(2, reply_to_position=1), _msg(3)]
    )
    assert ids == sorted(ids) and len(set(ids)) == 3
    messages = await db.repos.seeding.list_messages_for_thread(thread_id)
    assert [m.position for m in messages] == [1, 2, 3]
    assert messages[0].thread_id == thread_id
    assert messages[1].reply_to_position == 1


@pytest.mark.anyio
async def test_create_messages_duplicate_position_rolls_back_batch(db):
    """Дубль position откатывает весь батч — частичный сценарий не сохраняется."""
    thread_id = await _make_thread(db)
    with pytest.raises(aiosqlite.IntegrityError):
        await db.repos.seeding.create_messages(thread_id, [_msg(1), _msg(1)])
    assert await db.repos.seeding.list_messages_for_thread(thread_id) == []


@pytest.mark.anyio
async def test_thread_valid_transition_chain(db):
    thread_id = await _make_thread(db)
    repo = db.repos.seeding
    for status in (
        SeedingThreadStatus.SCHEDULED,
        SeedingThreadStatus.ACTIVE,
        SeedingThreadStatus.DONE,
    ):
        assert await repo.set_thread_status(thread_id, status) is True
        assert (await repo.get_thread(thread_id)).status is status


@pytest.mark.anyio
async def test_thread_illegal_transitions_do_not_write(db):
    thread_id = await _make_thread(db)
    repo = db.repos.seeding
    # draft → done / draft → active запрещены.
    assert await repo.set_thread_status(thread_id, SeedingThreadStatus.DONE) is False
    assert await repo.set_thread_status(thread_id, SeedingThreadStatus.ACTIVE) is False
    assert (await repo.get_thread(thread_id)).status is SeedingThreadStatus.DRAFT
    # Терминальный статус — ни одного исхода.
    await repo.set_thread_status(thread_id, SeedingThreadStatus.CANCELLED)
    assert await repo.set_thread_status(thread_id, SeedingThreadStatus.SCHEDULED) is False
    assert await repo.set_thread_status(thread_id, SeedingThreadStatus.CANCELLED) is False
    assert (await repo.get_thread(thread_id)).status is SeedingThreadStatus.CANCELLED


@pytest.mark.anyio
async def test_thread_missing_row_returns_false(db):
    assert await db.repos.seeding.set_thread_status(424242, SeedingThreadStatus.CANCELLED) is False


@pytest.mark.anyio
async def test_message_full_lifecycle_sent_requires_tg_id(db):
    thread_id = await _make_thread(db)
    (msg_id,) = await db.repos.seeding.create_messages(
        thread_id, [_msg(1, planned_at=datetime.now(timezone.utc))]
    )
    repo = db.repos.seeding
    assert await repo.set_message_status(msg_id, SeedingMessageStatus.APPROVED) is True
    assert await repo.set_message_status(msg_id, SeedingMessageStatus.SCHEDULED) is True
    # Факт отправки без id сообщения невоспроизводим — отказ.
    with pytest.raises(ValueError):
        await repo.set_message_status(msg_id, SeedingMessageStatus.SENT)
    sent_at = datetime(2026, 11, 2, 12, 0, tzinfo=timezone.utc)
    assert (
        await repo.set_message_status(
            msg_id, SeedingMessageStatus.SENT, tg_message_id=777, sent_at=sent_at
        )
        is True
    )
    msg = await repo.get_message(msg_id)
    assert msg.status is SeedingMessageStatus.SENT
    assert msg.tg_message_id == 777
    assert msg.sent_at == sent_at


@pytest.mark.anyio
async def test_message_illegal_transitions_do_not_write(db):
    thread_id = await _make_thread(db)
    (msg_id,) = await db.repos.seeding.create_messages(thread_id, [_msg(1)])
    repo = db.repos.seeding
    # draft → sent / draft → failed запрещены.
    assert await repo.set_message_status(msg_id, SeedingMessageStatus.FAILED) is False
    assert await repo.set_message_status(msg_id, SeedingMessageStatus.SENT, tg_message_id=1) is False
    assert (await repo.get_message(msg_id)).status is SeedingMessageStatus.DRAFT
    await repo.set_message_status(msg_id, SeedingMessageStatus.CANCELLED)
    # Терминальный: cancelled → scheduled/approved запрещены.
    assert await repo.set_message_status(msg_id, SeedingMessageStatus.SCHEDULED) is False
    assert await repo.set_message_status(msg_id, SeedingMessageStatus.APPROVED) is False


@pytest.mark.anyio
async def test_message_failed_replan_clears_error(db):
    thread_id = await _make_thread(db)
    (msg_id,) = await db.repos.seeding.create_messages(thread_id, [_msg(1)])
    repo = db.repos.seeding
    await repo.set_message_status(msg_id, SeedingMessageStatus.SCHEDULED)
    assert await repo.set_message_status(msg_id, SeedingMessageStatus.FAILED, error_text="FloodWait") is True
    assert (await repo.get_message(msg_id)).error_text == "FloodWait"
    # Перепланирование стирает устаревшую ошибку.
    assert await repo.set_message_status(msg_id, SeedingMessageStatus.SCHEDULED) is True
    assert (await repo.get_message(msg_id)).error_text is None


@pytest.mark.anyio
async def test_message_failed_to_cancelled_keeps_error(db):
    thread_id = await _make_thread(db)
    (msg_id,) = await db.repos.seeding.create_messages(thread_id, [_msg(1)])
    repo = db.repos.seeding
    await repo.set_message_status(msg_id, SeedingMessageStatus.SCHEDULED)
    await repo.set_message_status(msg_id, SeedingMessageStatus.FAILED, error_text="boom")
    await repo.set_message_status(msg_id, SeedingMessageStatus.CANCELLED)
    assert (await repo.get_message(msg_id)).error_text == "boom"


@pytest.mark.anyio
async def test_list_due_messages_only_scheduled_past(db):
    now = datetime.now(timezone.utc)
    past = now - timedelta(minutes=5)
    future = now + timedelta(hours=1)
    thread_id = await _make_thread(db)
    repo = db.repos.seeding
    # planned_at наследуется из create_messages: 1 — просрочена, 2 — будущее,
    # 3 — просрочена, но уже отправлена, 4 — просроченный черновик.
    ids = await repo.create_messages(
        thread_id,
        [
            _msg(1, planned_at=past, status=SeedingMessageStatus.SCHEDULED),
            _msg(2, planned_at=future, status=SeedingMessageStatus.SCHEDULED),
            _msg(3, planned_at=past, status=SeedingMessageStatus.SENT),
            _msg(4, planned_at=past),
        ],
    )
    due = await repo.list_due_messages(now)
    assert [m.id for m in due] == [ids[0]]
    assert due[0].position == 1
    # Лимит ограничивает выборку.
    (late_id,) = await repo.create_messages(
        thread_id, [_msg(5, planned_at=past - timedelta(hours=1), status=SeedingMessageStatus.SCHEDULED)]
    )
    due_all = await repo.list_due_messages(now)
    # 5 раньше 1 — упорядочивание по planned_at.
    assert [m.id for m in due_all] == [late_id, ids[0]]
    due_capped = await repo.list_due_messages(now, limit=1)
    assert [m.id for m in due_capped] == [late_id]


@pytest.mark.anyio
async def test_get_thread_progress_counts(db):
    thread_id = await _make_thread(db)
    repo = db.repos.seeding
    ids = await repo.create_messages(
        thread_id,
        [
            _msg(1, status=SeedingMessageStatus.DRAFT),
            _msg(2, status=SeedingMessageStatus.APPROVED),
            _msg(3, status=SeedingMessageStatus.SCHEDULED),
            _msg(4, status=SeedingMessageStatus.SCHEDULED),
            _msg(5, status=SeedingMessageStatus.CANCELLED),
        ],
    )
    await repo.set_message_status(ids[2], SeedingMessageStatus.SENT, tg_message_id=11)
    await repo.set_message_status(ids[3], SeedingMessageStatus.FAILED, error_text="x")
    progress = await repo.get_thread_progress(thread_id)
    assert (progress.total, progress.sent, progress.failed, progress.cancelled) == (5, 1, 1, 1)
    # Пустой тред — нули, не исключение.
    empty = await repo.get_thread_progress(await _make_thread(db))
    assert empty.total == 0 and empty.sent == 0
