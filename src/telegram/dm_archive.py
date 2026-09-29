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

from telethon_floodgate import (
    HandledFloodWaitError,
    TelegramRateLimitGate,
    run_with_flood_wait_retry,
)

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
    auth = getattr(pool, "_auth", None)
    if client is None or auth is None:
        raise RuntimeError(f"dm_archive: нет подключенного клиента для {phone}")

    stats: dict[str, Any] = {"dialogs": 0, "archived": 0, "errors": 0}

    async def _list_personal_dialogs() -> list[Any]:
        # Живой iter_dialogs сразу же: заполняет кэш энтитий сессии — numeric-peer
        # lookups iter_messages ниже резолвятся без второго запроса (конвенция
        # «entity cache», CLAUDE.md). iter_dialogs — async-итератор, не awaitable
        # (регресс боевого прогона #1455); GetDialogsRequest флудится как любой
        # запрос — транзиентные ожидания внутри run_with_flood_wait_retry
        # (второй регресс того же прогона: голый вызов упал на FloodWait 20s).
        return [
            dialog
            async for dialog in client.iter_dialogs()
            if dialog.is_user
            and (chat_ids is None or int(dialog.id) in chat_ids)
        ]

    # Листинг сам себя флудит: попыток ~21 (чанки по 100 диалогов) подряд,
    # ретрай обёртки перезапускает всё с нуля, а второй подряд FloodWait она
    # уже отдаёт наверх. Пауза по водяному знаку и заново — листинг короткий,
    # архив при этом ничего не теряет (регресс боевого прогона #1455).
    dialogs: list[Any] = []
    for listing_attempt in range(1, 4):
        try:
            dialogs = await run_with_flood_wait_retry(
                _list_personal_dialogs, operation="dm_archive_dialogs"
            )
            break
        except HandledFloodWaitError as exc:
            if listing_attempt == 3:
                raise RuntimeError(
                    "dm_archive: листинг диалогов флудится дольше трёх пауз — "
                    "перезапусти команду позже (прогон возобновляемый)"
                ) from exc
            pause = float(
                getattr(getattr(exc, "info", None), "wait_seconds", 0) or 30
            )
            logger.info(
                "dm_archive: листинг флудится, пауза %.0fs (попытка %d/3)",
                pause,
                listing_attempt,
            )
            await asyncio.sleep(pause + 1)
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
                # Маркер незавершённости: CLI не должен печатать «готово».
                logger.warning("dm_archive: гейт насыщен, стоп на %s chat %s", phone, chat_id)
                stats["incomplete"] = True
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
                inserted = await db.repos.dm_messages.record(
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
                if inserted:
                    stats["archived"] += 1  # только новые: повторы бэкфилла не «архив»
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
