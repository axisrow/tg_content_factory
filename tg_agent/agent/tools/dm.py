"""Agent tools DM-цикла (#1580): журнал входящих, черновики, апрув, архив.

Обёртки над репозиториями ``incoming_dms``/``dm_reply_drafts``/``dm_messages``
(#1427/#1429/#1453) — те же методы, что используют web/REST (#1579) и CLI;
инструменты не пере-изобретают чтение/запись. Отправка — вне tool-контракта:
апрув лишь готовит черновик, сама отправка идёт только через явную очередь
воркера (``dm.send_reply`` из CLI), поэтому двойная отправка исключена.

Приватность: тело ответа — личная переписка, как у REST-зеркала #1579;
инструменты не логируют содержимое, а идентификаторы во внешних артефактах
(issue/PR/коммиты) маскируются по общему правилу проекта.
"""

from __future__ import annotations

from typing import Annotated, Literal

from claude_agent_sdk import tool

from tg_agent.agent.tools._categories import ToolCategory, ToolMeta
from tg_agent.agent.tools._registry import _text_response

# Permission metadata (#245): single source of truth for TOOL_CATEGORIES —
# invariants in tests/test_tool_permissions_autoderive.py keep it in sync.
TOOL_GROUPS: list[tuple[str, dict[str, ToolMeta]]] = [
    ("Личные сообщения (DM)", {
        "list_incoming_dms": ToolMeta(ToolCategory.READ),
        "get_incoming_dm": ToolMeta(ToolCategory.READ),
        "list_dm_drafts": ToolMeta(ToolCategory.READ),
        "read_dm_archive": ToolMeta(ToolCategory.READ),
        "create_dm_draft": ToolMeta(ToolCategory.WRITE),
        "approve_dm_draft": ToolMeta(ToolCategory.WRITE),
        "reject_dm_draft": ToolMeta(ToolCategory.WRITE),
    }),
]

_PREVIEW_CAP = 200


def _one_line(text: str | None, cap: int = _PREVIEW_CAP) -> str:
    return ((text or "(без текста)")[:cap].replace("\n", " "))


def register(db, client_pool, embedding_service, **kwargs):
    tools = []

    # ------------------------------------------------------------------
    # READ
    # ------------------------------------------------------------------

    @tool(
        "list_incoming_dms",
        "List the incoming-DM journal (24h TTL window, fresh first). Each entry "
        "carries id/phone/chat_id/message_id — the ids the DM draft flow needs. "
        "Use get_incoming_dm for full text, then create_dm_draft.",
        {
            "limit": Annotated[int, "Максимальное количество записей"],
            "offset": Annotated[int, "Смещение страницы"],
        },
    )
    async def list_incoming_dms(args):
        try:
            limit = int(args.get("limit", 50))
            offset = int(args.get("offset", 0))
            dms = await db.repos.incoming_dms.list(limit=limit, offset=offset)
            if not dms:
                return _text_response("Журнал входящих DM пуст (окно TTL 24ч).")
            lines = [f"Входящие DM ({len(dms)} шт., свежие первыми):"]
            for dm in dms:
                processed = "разобрано" if dm.processed else "не разобрано"
                lines.append(
                    f"- id={dm.id}, chat_id={dm.chat_id}, message_id={dm.message_id}, "
                    f"phone={dm.phone}, {processed}: {_one_line(dm.text)}"
                )
            return _text_response("\n".join(lines))
        except Exception as e:
            return _text_response(f"Ошибка чтения журнала входящих DM: {e}")

    tools.append(list_incoming_dms)

    @tool(
        "get_incoming_dm",
        "Full text of one incoming-DM journal entry. dm_id from list_incoming_dms. "
        "Entries older than the 24h TTL are pruned and return not-found.",
        {"dm_id": Annotated[int, "ID записи журнала из list_incoming_dms"]},
    )
    async def get_incoming_dm(args):
        dm_id = args.get("dm_id")
        if dm_id is None:
            return _text_response("Ошибка: dm_id обязателен.")
        try:
            dm = await db.repos.incoming_dms.get(int(dm_id))
            if dm is None:
                return _text_response(
                    f"Запись журнала id={dm_id} не найдена "
                    "(вычищена prune'ом TTL 24ч или не существовала)."
                )
            date = dm.message_date.isoformat() if dm.message_date else "неизвестно"
            processed = "разобрано" if dm.processed else "не разобрано"
            return _text_response(
                f"Входящее DM id={dm.id}\n"
                f"  Откуда: chat_id={dm.chat_id}, phone={dm.phone}\n"
                f"  message_id={dm.message_id}, дата={date}, {processed}\n"
                f"\nТекст:\n{dm.text or '(без текста)'}"
            )
        except Exception as e:
            return _text_response(f"Ошибка получения записи журнала: {e}")

    tools.append(get_incoming_dm)

    @tool(
        "list_dm_drafts",
        "List reply drafts for incoming DMs (fresh first). Optional status filter: "
        "draft/approved/rejected/sending/sent/unconfirmed. "
        "Then approve_dm_draft or reject_dm_draft.",
        {
            "status": Annotated[
                str, "Фильтр по статусу (draft/approved/rejected/sending/sent/unconfirmed)"
            ],
            "limit": Annotated[int, "Максимальное количество черновиков"],
        },
    )
    async def list_dm_drafts(args):
        try:
            status = args.get("status") or None
            limit = int(args.get("limit", 50))
            drafts = await db.repos.dm_reply_drafts.list_drafts(status=status, limit=limit)
            if not drafts:
                return _text_response("Черновики ответов не найдены.")
            lines = [f"Черновики ответов ({len(drafts)} шт.):"]
            for d in drafts:
                sent = f", sent_message_id={d.sent_message_id}" if d.sent_message_id is not None else ""
                lines.append(
                    f"- draft_id={d.id}, status={d.status}, chat_id={d.chat_id}, "
                    f"incoming_dm_id={d.incoming_dm_id}{sent}: {_one_line(d.reply_text)}"
                )
            return _text_response("\n".join(lines))
        except Exception as e:
            return _text_response(f"Ошибка получения черновиков: {e}")

    tools.append(list_dm_drafts)

    @tool(
        "read_dm_archive",
        "Read the persistent DM archive (both directions, no TTL) — history "
        "backfilled by `dialogs archive-history` and written by the live "
        "listener. Filter by phone and/or chat_id; fresh first.",
        {
            "phone": Annotated[str, "Телефон аккаунта (чья переписка)"],
            "chat_id": Annotated[int, "ID диалога (собеседника)"],
            "limit": Annotated[int, "Максимальное количество сообщений"],
        },
    )
    async def read_dm_archive(args):
        try:
            phone = str(args.get("phone") or "").strip() or None
            chat_raw = args.get("chat_id")
            chat_id = int(chat_raw) if chat_raw not in (None, "") else None
            limit = int(args.get("limit", 100))
            messages = await db.repos.dm_messages.list(phone=phone, chat_id=chat_id, limit=limit)
            if not messages:
                return _text_response("Архив DM пуст по заданному фильтру.")
            lines = [f"Архив DM ({len(messages)} шт., свежие первыми):"]
            for m in messages:
                direction = "исходящее" if m.out else "входящее"
                date = m.message_date.strftime("%Y-%m-%d %H:%M") if m.message_date else "—"
                lines.append(
                    f"- [{date}] chat_id={m.chat_id} #{m.message_id} ({direction}): "
                    f"{_one_line(m.text, 300)}"
                )
            return _text_response("\n".join(lines))
        except Exception as e:
            return _text_response(f"Ошибка чтения архива DM: {e}")

    tools.append(read_dm_archive)

    # ------------------------------------------------------------------
    # WRITE
    # ------------------------------------------------------------------

    async def _decide_draft(
        args: dict, decision: Literal["approved", "rejected"]
    ) -> dict:
        draft_id = args.get("draft_id")
        if draft_id is None:
            return _text_response("Ошибка: draft_id обязателен.")
        try:
            draft = await db.repos.dm_reply_drafts.get(int(draft_id))
            if draft is None:
                return _text_response(f"Черновик #{draft_id} не найден.")
            changed = await db.repos.dm_reply_drafts.decide(int(draft_id), decision)
            if changed:
                return _text_response(f"Черновик #{draft_id}: {draft.status} → {decision}.")
            # Идемпотентный no-op: терминальные sending/sent/unconfirmed решением
            # владельца назад не переводятся — двойная отправка исключена.
            return _text_response(
                f"Черновик #{draft_id} остался в статусе '{draft.status}' "
                "(решения не применяются к sending/sent/unconfirmed)."
            )
        except Exception as e:
            return _text_response(f"Ошибка решения по черновику: {e}")

    @tool(
        "create_dm_draft",
        "Create (or re-draft, while still in draft status) a reply draft for an "
        "incoming DM. Resolves phone/chat_id from the journal row; for an "
        "already-pruned entry pass phone and chat_id explicitly. Text of a "
        "decided draft is never touched. Sending is a separate explicit step.",
        {
            "incoming_dm_id": Annotated[int, "ID записи журнала из list_incoming_dms"],
            "text": Annotated[str, "Текст ответа"],
            "phone": Annotated[str, "Телефон аккаунта, если журнальная строка уже вычищена (TTL 24ч)"],
            "chat_id": Annotated[int, "ID диалога, если журнальная строка уже вычищена (TTL 24ч)"],
        },
    )
    async def create_dm_draft(args):
        incoming_dm_id = args.get("incoming_dm_id")
        text = str(args.get("text") or "").strip()
        if incoming_dm_id is None or not text:
            return _text_response("Ошибка: incoming_dm_id и text обязательны.")
        try:
            phone = str(args.get("phone") or "").strip() or None
            chat_raw = args.get("chat_id")
            chat_id = int(chat_raw) if chat_raw not in (None, "") else None
            if phone is None or chat_id is None:
                journal = await db.repos.incoming_dms.get(int(incoming_dm_id))
                if journal is not None:
                    phone = phone or journal.phone
                    chat_id = chat_id if chat_id is not None else journal.chat_id
            if not phone or chat_id is None:
                return _text_response(
                    f"Ошибка: запись журнала id={incoming_dm_id} не найдена (TTL 24ч) — "
                    "укажите phone и chat_id явно."
                )
            draft_id, created = await db.repos.dm_reply_drafts.create_draft(
                incoming_dm_id=int(incoming_dm_id),
                phone=phone,
                chat_id=chat_id,
                reply_text=text,
            )
            # Семантика #1581 (паритет с `dm drafts create`): черновик разбирает
            # журнальную строку — unprocessed в `dm status` считает только
            # недоразобранное.
            journal = await db.repos.incoming_dms.get(int(incoming_dm_id))
            if journal is not None:
                await db.repos.incoming_dms.mark_processed(
                    journal.phone, journal.chat_id, [journal.message_id]
                )
            draft = await db.repos.dm_reply_drafts.get(draft_id)
            if draft is not None and draft.status == "draft":
                verb = "создан" if created else "обновлён"
                return _text_response(f"Черновик #{draft_id} {verb} (статус draft).")
            return _text_response(
                f"Черновик #{draft_id} в статусе "
                f"'{draft.status if draft else 'неизвестен'}' — текст не изменён "
                "(правка одобренного/отклонённого текста недопустима)."
            )
        except Exception as e:
            return _text_response(f"Ошибка создания черновика: {e}")

    tools.append(create_dm_draft)

    @tool(
        "approve_dm_draft",
        "Approve a reply draft for sending (draft → approved; no-op once "
        "sending/sent/unconfirmed, so a double send is impossible). draft_id from "
        "list_dm_drafts. Actual sending is a separate explicit queue step.",
        {"draft_id": Annotated[int, "ID черновика из list_dm_drafts"]},
    )
    async def approve_dm_draft(args):
        return await _decide_draft(args, "approved")

    tools.append(approve_dm_draft)

    @tool(
        "reject_dm_draft",
        "Reject a reply draft — a rejected draft can never be claimed for "
        "sending. draft_id from list_dm_drafts.",
        {"draft_id": Annotated[int, "ID черновика из list_dm_drafts"]},
    )
    async def reject_dm_draft(args):
        return await _decide_draft(args, "rejected")

    tools.append(reject_dm_draft)

    return tools
