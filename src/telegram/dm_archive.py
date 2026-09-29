"""Бэкфилл полной истории личных диалогов в архив `dm_messages` (#1453).

Догон #1428 читает только выше курсора `dm_catchup_cursors`, а слушатель
#1426 видит только живые события — глубокая история диалога обоим недостижима.
Бэкфилл дополняет их: живой `iter_dialogs()` пула (НЕ `dialog_cache` — урок
#1350 о молча замороженном кэше), страницы истории через готовый
`read_dialog_history_since` (flood-retry внутри), каждая страница — в архив,
обе стороны, идемпотентно по `UNIQUE(phone, chat_id, message_id)`.

Возобновляемость: курсор = `dm_messages.MAX(message_id)` диалога, поэтому
убитый посреди прогон продолжается с места обрыва, а повторный — почти
бесплатен (страница читается, вставки молчат). Инвариант `dm_history`
соблюдён: никаких connect/disconnect и обработчиков на клиенте пула.

ЗАПУСК: только при остановленном воркере. Клиент бэкфилла открывает второй
MTProto-коннект на той же сессии — рядом с живым пулом это «silent brick»
(`src/telegram/mtproto_watchdog.py`).
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Any

from telethon_floodgate import HandledFloodWaitError, TelegramRateLimitGate

from src.models import DmMessage
from src.telegram.dm_history import read_dialog_history_since

logger = logging.getLogger(__name__)

# Страница истории на запрос: бэкфилл — разовый дренаж, крупная страница
# экономит запросы; догон остаётся на 200 (#1428).
BACKFILL_PAGE_LIMIT = 500

_GATE_HISTORY_CATEGORY = "history"
_GATE_WAIT_ATTEMPTS = 2


async def _acquire_history_slot(pool: Any, phone: str) -> bool:
    """Слот гейта `history`: ждать, а не отказывать (#1417); False — насыщен.

    Тот же паттерн, что у догона (`DmCatchupService._acquire_history_slot`).
    """
    gate = getattr(pool, "_rate_limit_gate", None)
    if not isinstance(gate, TelegramRateLimitGate):
        return True  # фейки/незабинженный пул — гейтинг no-op, как в backends
    for _ in range(_GATE_WAIT_ATTEMPTS):
        retry_after = gate.try_acquire(phone, _GATE_HISTORY_CATEGORY)
        if retry_after <= 0:
            return True
        logger.info("dm_archive: history-гейт %s на %.1fs; жду", phone, retry_after)
        await asyncio.sleep(retry_after)
    return False


async def backfill_account(
    pool: Any,
    db: Any,
    phone: str,
    *,
    chat_ids: set[int] | None = None,
    progress: bool = True,
) -> dict[str, Any]:
    """Дочитать полную историю личных диалогов аккаунта в архив.

    Личные диалоги = `User`-энтитии живого `iter_dialogs()` (люди и боты;
    каналы/группы/боги — не DM, saved-заметки себе тоже мимо скоупа #1453).
    `chat_ids` сужает прогон до конкретных диалогов. Прогресс печатается
    по диалогам (CLI-путь синхронный и долгий).
    """
    session = pool.clients.get(phone)
    client = getattr(session, "raw_client", None)
    if client is None:
        raise RuntimeError(f"dm_archive: нет подключенного клиента для {phone}")
    auth = getattr(pool, "_auth", None)

    stats: dict[str, Any] = {"dialogs": 0, "archived": 0, "errors": 0}
    # Живой iter_dialogs сразу же: заполняет кэш энтитий сессии — numeric-peer
    # lookups iter_messages ниже резолвятся без второго запроса (конвенция
    # «entity cache», CLAUDE.md).
    dialogs = [
        dialog
        for dialog in await client.iter_dialogs()
        if dialog.is_user
        and (chat_ids is None or int(dialog.id) in chat_ids)
    ]
    stats["dialogs"] = len(dialogs)
    for dialog in dialogs:
        chat_id = int(dialog.id)
        cursor = await db.repos.dm_messages.max_message_id(phone, chat_id)
        name = dialog.name or str(chat_id)
        pages = 0
        while True:
            if not await _acquire_history_slot(pool, phone):
                # Гейт аккаунта насыщен: диалог остаётся на повторный прогон —
                # курсор не двигался, возобновление вернётся ровно сюда.
                logger.warning("dm_archive: гейт насыщен, стоп на %s chat %s", phone, chat_id)
                return stats
            try:
                messages = await read_dialog_history_since(
                    client,
                    api_id=auth.api_id,
                    api_hash=auth.api_hash,
                    peer=chat_id,
                    min_id=cursor,
                    limit=BACKFILL_PAGE_LIMIT,
                )
            except HandledFloodWaitError:
                logger.warning("dm_archive: flood wait на %s chat %s; диалог пропущен", phone, chat_id)
                stats["errors"] += 1
                break
            except (ValueError, TypeError) as exc:
                logger.warning("dm_archive: %s chat %s не резолвится: %s", phone, chat_id, exc)
                stats["errors"] += 1
                break
            for msg in messages:
                await db.repos.dm_messages.record(
                    DmMessage(
                        phone=phone,
                        chat_id=chat_id,
                        message_id=msg.id,
                        out=msg.out,
                        text=msg.text,
                        message_date=msg.date,
                        received_at=datetime.now(timezone.utc),
                    )
                )
            stats["archived"] += len(messages)
            pages += 1
            if len(messages) < BACKFILL_PAGE_LIMIT:
                break
            cursor = max(msg.id for msg in messages)
        if progress:
            print(
                f"dm_archive: {phone} chat {chat_id} ({name}): "
                f"страниц {pages}, всего в архиве {stats['archived']}",
                flush=True,
            )
    return stats
