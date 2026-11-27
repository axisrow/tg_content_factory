"""Agent tools DM-цикла (#1580): журнал, черновики, апрув, архив.

Fake-first: без живого Telegram — реальная ``Database(":memory:")`` с сидом
через те же репозитории, что используют web/REST (#1579) и CLI. Приёмо-
сдаточные доказательства: каждый tool возвращает реальное состояние журнала/
черновиков/архива, а approve/reject/create реально мутируют ``dm_reply_drafts``
(проверяется чтением БД после вызова), idempotent no-op на sending/sent/unconfirmed.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from tests.agent_tools_helpers import _get_tool_handlers, _text
from tg_agent.models import DmMessage, IncomingDm

_FAKE_PHONE = "+10000000001"


async def _seed_journal(
    db,
    *,
    chat_id: int = 42,
    message_id: int = 7,
    text: str = "входящее сообщение",
    minutes_ago: int = 0,
) -> int:
    received = datetime.now(timezone.utc) - timedelta(minutes=minutes_ago)
    await db.repos.incoming_dms.record(
        IncomingDm(
            phone=_FAKE_PHONE,
            chat_id=chat_id,
            message_id=message_id,
            text=text,
            message_date=received,
            received_at=received,
        ),
    )
    rows = await db.repos.incoming_dms.list(limit=1)
    return rows[0].id


async def _seed_draft(
    db, incoming_dm_id: int, *, status: str = "draft", reply_text: str = "черновик ответа"
) -> int:
    draft_id, _created = await db.repos.dm_reply_drafts.create_draft(
        incoming_dm_id=incoming_dm_id,
        phone=_FAKE_PHONE,
        chat_id=42,
        reply_text=reply_text,
    )
    if status == "approved":
        await db.repos.dm_reply_drafts.decide(draft_id, "approved")
    elif status == "rejected":
        await db.repos.dm_reply_drafts.decide(draft_id, "rejected")
    elif status == "sent":
        await db.repos.dm_reply_drafts.decide(draft_id, "approved")
        assert await db.repos.dm_reply_drafts.claim_for_send(draft_id)
        await db.repos.dm_reply_drafts.mark_sent(draft_id, 900)
    return draft_id


async def _seed_archive(
    db, *, chat_id: int = 42, message_id: int = 1, out: bool = False, text: str = "текст"
) -> None:
    await db.repos.dm_messages.record(
        DmMessage(
            phone=_FAKE_PHONE,
            chat_id=chat_id,
            message_id=message_id,
            out=out,
            text=text,
        ),
    )


@pytest.fixture
async def handlers(db):
    return _get_tool_handlers(db, client_pool=None)


# === Журнал входящих (READ) ================================================


@pytest.mark.anyio
async def test_list_incoming_dms_returns_seeded_journal(db, handlers):
    await _seed_journal(db, chat_id=42, message_id=7, text="первое сообщение")
    await _seed_journal(db, chat_id=43, message_id=8, text="второе сообщение")

    text = _text(await handlers["list_incoming_dms"]({}))

    assert "Входящие DM (2 шт." in text
    # Идентификаторы, нужные потоку черновиков (#1579).
    assert "id=" in text and "chat_id=43" in text and "message_id=8" in text
    assert "phone=+10000000001" in text
    assert "второе сообщение" in text and "первое сообщение" in text


@pytest.mark.anyio
async def test_list_incoming_dms_respects_limit_and_offset(db, handlers):
    await _seed_journal(db, chat_id=42, message_id=1, text="старое")
    await _seed_journal(db, chat_id=43, message_id=2, text="свежее")

    page = _text(await handlers["list_incoming_dms"]({"limit": 1}))
    assert "свежее" in page and "старое" not in page

    second = _text(await handlers["list_incoming_dms"]({"limit": 1, "offset": 1}))
    assert "старое" in second and "свежее" not in second


@pytest.mark.anyio
async def test_list_incoming_dms_empty_journal(db, handlers):
    text = _text(await handlers["list_incoming_dms"]({}))
    assert "пуст" in text


@pytest.mark.anyio
async def test_get_incoming_dm_returns_full_text(db, handlers):
    dm_id = await _seed_journal(db, text="полный текст сообщения")

    text = _text(await handlers["get_incoming_dm"]({"dm_id": dm_id}))

    assert "полный текст сообщения" in text
    assert "chat_id=42" in text and "message_id=7" in text


@pytest.mark.anyio
async def test_get_incoming_dm_requires_dm_id(db, handlers):
    assert "dm_id обязателен" in _text(await handlers["get_incoming_dm"]({}))


@pytest.mark.anyio
async def test_get_incoming_dm_not_found(db, handlers):
    text = _text(await handlers["get_incoming_dm"]({"dm_id": 9999}))
    assert "не найдена" in text


# === Черновики (READ + WRITE) ==============================================


@pytest.mark.anyio
async def test_list_dm_drafts_shows_statuses(db, handlers):
    # Один черновик на входящее (UNIQUE(incoming_dm_id)) — два диалога.
    dm_one = await _seed_journal(db, chat_id=42, message_id=1)
    dm_two = await _seed_journal(db, chat_id=43, message_id=2)
    approved_id = await _seed_draft(db, dm_one, status="approved")
    draft_id = await _seed_draft(db, dm_two, status="draft")

    text = _text(await handlers["list_dm_drafts"]({}))

    assert "Черновики ответов (2 шт.)" in text
    assert f"draft_id={approved_id}, status=approved" in text
    assert f"draft_id={draft_id}, status=draft" in text


@pytest.mark.anyio
async def test_list_dm_drafts_filters_by_status(db, handlers):
    # Один черновик на входящее (UNIQUE(incoming_dm_id)) — два диалога.
    dm_one = await _seed_journal(db, chat_id=42, message_id=1)
    dm_two = await _seed_journal(db, chat_id=43, message_id=2)
    await _seed_draft(db, dm_one, status="approved")
    await _seed_draft(db, dm_two, status="rejected")

    text = _text(await handlers["list_dm_drafts"]({"status": "rejected"}))

    assert "status=rejected" in text
    assert "status=approved" not in text


@pytest.mark.anyio
async def test_list_dm_drafts_empty(db, handlers):
    assert "не найдены" in _text(await handlers["list_dm_drafts"]({}))


@pytest.mark.anyio
async def test_create_dm_draft_resolves_phone_chat_from_journal(db, handlers):
    dm_id = await _seed_journal(db)

    text = _text(await handlers["create_dm_draft"]({"incoming_dm_id": dm_id, "text": "ответ агента"}))

    assert "создан" in text
    draft = (await db.repos.dm_reply_drafts.list_drafts())[0]
    assert (draft.phone, draft.chat_id, draft.status) == (_FAKE_PHONE, 42, "draft")
    assert draft.reply_text == "ответ агента"


@pytest.mark.anyio
async def test_create_dm_draft_redraft_updates_text_while_draft(db, handlers):
    dm_id = await _seed_journal(db)
    await _seed_draft(db, dm_id, reply_text="первый вариант")

    text = _text(await handlers["create_dm_draft"]({"incoming_dm_id": dm_id, "text": "второй вариант"}))

    assert "обновлён" in text
    drafts = await db.repos.dm_reply_drafts.list_drafts()
    assert len(drafts) == 1 and drafts[0].reply_text == "второй вариант"


@pytest.mark.anyio
async def test_create_dm_draft_never_touches_decided_draft(db, handlers):
    dm_id = await _seed_journal(db)
    draft_id = await _seed_draft(db, dm_id, status="approved", reply_text="одобренный текст")

    text = _text(await handlers["create_dm_draft"]({"incoming_dm_id": dm_id, "text": "подмена"}))

    assert "не изменён" in text
    draft = await db.repos.dm_reply_drafts.get(draft_id)
    assert draft.reply_text == "одобренный текст" and draft.status == "approved"


@pytest.mark.anyio
async def test_create_dm_draft_marks_journal_processed(db, handlers):
    """Паритет с `dm drafts create` (#1581): черновик разбирает журнальную строку."""
    dm_id = await _seed_journal(db)
    assert await db.repos.incoming_dms.count_unprocessed() == 1

    await handlers["create_dm_draft"]({"incoming_dm_id": dm_id, "text": "ответ агента"})

    assert await db.repos.incoming_dms.count_unprocessed() == 0


@pytest.mark.anyio
async def test_create_dm_draft_without_journal_row_asks_for_phone_chat(db, handlers):
    text = _text(await handlers["create_dm_draft"]({"incoming_dm_id": 4242, "text": "ответ"}))
    assert "не найдена" in text and "phone" in text and "chat_id" in text


@pytest.mark.anyio
async def test_create_dm_draft_requires_args(db, handlers):
    assert "обязательны" in _text(await handlers["create_dm_draft"]({}))


@pytest.mark.anyio
async def test_approve_dm_draft_mutates_db(db, handlers):
    dm_id = await _seed_journal(db)
    draft_id = await _seed_draft(db, dm_id)

    text = _text(await handlers["approve_dm_draft"]({"draft_id": draft_id}))

    assert "approved" in text
    draft = await db.repos.dm_reply_drafts.get(draft_id)
    assert draft.status == "approved"


@pytest.mark.anyio
async def test_approve_dm_draft_after_sent_is_noop(db, handlers):
    dm_id = await _seed_journal(db)
    draft_id = await _seed_draft(db, dm_id, status="sent")

    text = _text(await handlers["approve_dm_draft"]({"draft_id": draft_id}))

    assert "остался" in text
    draft = await db.repos.dm_reply_drafts.get(draft_id)
    assert draft.status == "sent"


@pytest.mark.anyio
async def test_reject_dm_draft_mutates_db(db, handlers):
    dm_id = await _seed_journal(db)
    draft_id = await _seed_draft(db, dm_id)

    text = _text(await handlers["reject_dm_draft"]({"draft_id": draft_id}))

    assert "rejected" in text
    draft = await db.repos.dm_reply_drafts.get(draft_id)
    assert draft.status == "rejected"


@pytest.mark.anyio
async def test_decide_requires_draft_id_and_reports_missing(db, handlers):
    assert "draft_id обязателен" in _text(await handlers["approve_dm_draft"]({}))
    assert "не найден" in _text(await handlers["reject_dm_draft"]({"draft_id": 777}))


# === Архив переписки (READ) ================================================


@pytest.mark.anyio
async def test_read_dm_archive_shows_both_directions(db, handlers):
    await _seed_archive(db, chat_id=42, message_id=1, out=False, text="входящее в архиве")
    await _seed_archive(db, chat_id=42, message_id=2, out=True, text="исходящее из архива")
    await _seed_archive(db, chat_id=43, message_id=3, out=False, text="чужой диалог")

    text = _text(await handlers["read_dm_archive"]({"chat_id": 42}))

    assert "Архив DM (2 шт." in text
    assert "входящее" in text and "исходящее" in text
    assert "входящее в архиве" in text and "исходящее из архива" in text
    assert "чужой диалог" not in text


@pytest.mark.anyio
async def test_read_dm_archive_filters_by_phone(db, handlers):
    await _seed_archive(db, chat_id=42, message_id=1, text="наш аккаунт")
    await db.repos.dm_messages.record(
        DmMessage(phone="+10000000002", chat_id=42, message_id=1, text="другой аккаунт"),
    )

    text = _text(await handlers["read_dm_archive"]({"phone": _FAKE_PHONE}))

    assert "наш аккаунт" in text
    assert "другой аккаунт" not in text


@pytest.mark.anyio
async def test_read_dm_archive_empty(db, handlers):
    assert "пуст" in _text(await handlers["read_dm_archive"]({}))
