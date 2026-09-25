"""Догон пропущенного входящего DM (#1428, эпик #1416 этап 2.3).

Слушатель #1426 получает только живые события; всё, что пришло, пока он не
работал (рестарт, падение, замена клиента), догоняет этот сервис: по
водяному знаку журнала (`MAX(message_id)` на диалог) дочитывает историю через
некэшированный `history_since` и дописывает журнал `incoming_dms`. Повторная
доставка безопасна: UNIQUE(phone, chat_id, message_id) делает запись
идемпотентной, а INSERT OR IGNORE не трогает `processed` уже записанных строк.

Три режима (глобаль + переопределения на аккаунт и диалог, см.
`DmCatchupSettings`): `full` — догнать, свежее ждёт черновик (processed=0);
`journal_only` — догнать в журнал на просмотр, черновики автомат не готовит
(processed=1); `ignore` — не догонять вовсе. Порог давности (`staleness_sec`)
не даёт старому входящему получить автоматический черновик: оно попадает в
журнал только «на показ» (processed=1).

Триггер — присоединение слушателя к аккаунту (`DmListener` зовёт
`schedule([phone])` на каждый attach: старт воркера, замена клиента
восстановлением/reattach). Отдельная периодика не нужна: между attach-ами
работает живой слушатель, терять нечего. Проход single-flight: новые триггеры
во время прохода сливаются в одну очередь и разбираются следующим витком.

Лимиты: перед каждой страницей истории резервируется слот гейта пула
(`history`, калибровка #1418) с семантикой «ждать, а не отказывать»; второй
подряд отказ завершает проход аккаунта — водяные знаки не тронуты, остаток
догонит следующий триггер. FloodWait внутри чтения обрабатывает
`run_with_flood_wait_retry` (tg_messenger): транзиентные ждёт, долгие
превращает в ошибку диалога, не прерывая проход.
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import Any

from pydantic import ValidationError
from telethon_floodgate import HandledFloodWaitError, TelegramRateLimitGate

from src.models import DM_CATCHUP_SETTING_KEY, DmCatchupSettings, IncomingDm
from src.telegram.dm_history import read_dialog_history_since

logger = logging.getLogger(__name__)

# Страница истории на диалог за один проход; зазор длиннее страницы (200+
# пропущенных сообщений в одном диалоге) одним проходом не покрывается —
# догоняется верхушка, более старый хвост разбирается вручную.
CATCHUP_PAGE_LIMIT = 200

_GATE_HISTORY_CATEGORY = "history"
_GATE_WAIT_ATTEMPTS = 2

# Диалоги, которые слушатель видит живьём (is_private: люди и боты) — ровно
# их и догоняем; скоуп-диалог «Saved messages» живьём incoming не даёт.
_CATCHUP_DIALOG_TYPES = ("dm", "bot")


async def load_dm_catchup_settings(db: Any) -> DmCatchupSettings:
    """Прочитать настройки догона из settings-таблицы; битый JSON — дефолты."""
    raw = await db.get_setting(DM_CATCHUP_SETTING_KEY)
    if not raw:
        return DmCatchupSettings()
    try:
        return DmCatchupSettings.model_validate_json(raw)
    except ValidationError:
        logger.warning(
            "dm_catchup: некорректная настройка %s (%r), использую дефолты",
            DM_CATCHUP_SETTING_KEY,
            raw,
        )
        return DmCatchupSettings()


def _ensure_aware(dt: datetime) -> datetime:
    # Наивное время трактуем как UTC: смешивание naive/aware в арифметике —
    # повторявшийся класс багов этого проекта.
    return dt if dt.tzinfo is not None else dt.replace(tzinfo=timezone.utc)


class DmCatchupService:
    """Догон пропущенного DM по водяным знакам журнала (воркер-only).

    Живёт рядом с `DmListener` и только в worker-контейнере — как и слушатель,
    чтобы `serve` (embedded worker) и standalone `worker` не догоняли одни и те
    же аккаунты дважды.
    """

    def __init__(self, pool: Any, db: Any):
        self._pool = pool
        self._db = db
        self._task: asyncio.Task | None = None
        self._pending: set[str] = set()
        self._last_runs: dict[str, dict[str, Any]] = {}

    # --- lifecycle / scheduling ---

    def schedule(self, phones: Any) -> None:
        """Запланировать догон аккаунтов; во время прохода — в очередь следующего."""
        self._pending.update(str(phone) for phone in phones)
        if self._task and not self._task.done():
            return
        self._task = asyncio.create_task(self._run(), name="dm_catchup")

    async def stop(self) -> None:
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        self._task = None

    async def _run(self) -> None:
        while self._pending:
            phones = sorted(self._pending)
            self._pending.clear()
            for phone in phones:
                try:
                    await self.run_for_phone(phone)
                except asyncio.CancelledError:
                    raise
                except Exception:
                    logger.exception("dm_catchup: проход по %s упал", phone)

    # --- pass ---

    async def run_for_phone(self, phone: str) -> dict[str, Any]:
        """Один проход по аккаунту: догнать все диалоги из кэша диалогов."""
        session = self._pool.clients.get(phone)
        client = getattr(session, "raw_client", None)
        if client is None:
            return {}
        settings = await load_dm_catchup_settings(self._db)
        mode, _ = settings.resolve(phone)
        stats: dict[str, Any] = {
            "dialogs": 0,
            "stored": 0,
            "already": 0,
            "skipped": 0,
            "deferred": 0,
            "errors": 0,
        }
        if mode != "ignore":
            dialogs = [
                dialog
                for dialog in await self._db.repos.dialog_cache.list_dialogs(phone)
                if dialog.get("channel_type") in _CATCHUP_DIALOG_TYPES
            ]
            stats["dialogs"] = len(dialogs)
            now = datetime.now(timezone.utc)
            for index, dialog in enumerate(dialogs):
                outcome = await self._catch_up_dialog(
                    phone, client, int(dialog["channel_id"]), settings, now, stats
                )
                if outcome == "deferred":
                    # Гейт аккаунта насыщен — оставшиеся диалоги этого прохода
                    # отказались бы так же; догонит следующий триггер.
                    stats["deferred"] += 1 + (len(dialogs) - index - 1)
                    break
        self._last_runs[phone] = {**stats, "finished_at": datetime.now(timezone.utc).isoformat()}
        return stats

    async def _catch_up_dialog(
        self,
        phone: str,
        client: Any,
        chat_id: int,
        settings: DmCatchupSettings,
        now: datetime,
        stats: dict[str, Any],
    ) -> str:
        """Догнать один диалог; вернуть исход для счётчиков прохода."""
        mode, staleness = settings.resolve(phone, chat_id)
        if mode == "ignore":
            stats["skipped"] += 1
            return "skipped"
        watermark = await self._db.repos.incoming_dms.max_message_id(phone, chat_id)
        if not await self._acquire_history_slot(phone):
            return "deferred"
        auth = getattr(self._pool, "_auth", None)
        try:
            entity = await self._pool.resolve_dialog_entity(
                client, phone, chat_id, target_type="dm"
            )
            messages = await read_dialog_history_since(
                client,
                api_id=auth.api_id,
                api_hash=auth.api_hash,
                peer=entity,
                min_id=watermark,
                limit=CATCHUP_PAGE_LIMIT,
            )
        except HandledFloodWaitError:
            # Нетранзиентный флуд: не hammer'им, диалог остаётся на следующий
            # триггер; проход продолжается с остальных диалогов.
            logger.warning("dm_catchup: flood wait на %s chat %s; отложено", phone, chat_id)
            stats["errors"] += 1
            return "flood"
        except (ValueError, TypeError) as exc:
            # Peer не резолвится даже после warm (удалённый диалог и т.п.).
            logger.warning("dm_catchup: %s chat %s не резолвится: %s", phone, chat_id, exc)
            stats["errors"] += 1
            return "error"
        for msg in messages:
            if msg.out:
                continue  # журнал — только входящие, как у живого слушателя
            stale = msg.date is None or (now - _ensure_aware(msg.date)) > timedelta(
                seconds=staleness
            )
            inserted = await self._db.repos.incoming_dms.record(
                IncomingDm(
                    phone=phone,
                    chat_id=chat_id,
                    message_id=msg.id,
                    text=msg.text,
                    message_date=msg.date,
                ),
                processed=mode == "journal_only" or stale,
            )
            if inserted:
                stats["stored"] += 1
            else:
                stats["already"] += 1
        return "ok"

    async def _acquire_history_slot(self, phone: str) -> bool:
        """Слот гейта `history`: ждать, а не отказывать (#1417); False — насыщен."""
        gate = getattr(self._pool, "_rate_limit_gate", None)
        if not isinstance(gate, TelegramRateLimitGate):
            return True  # фейки/незабинженный пул — гейтинг no-op, как в backends
        for _ in range(_GATE_WAIT_ATTEMPTS):
            retry_after = gate.try_acquire(phone, _GATE_HISTORY_CATEGORY)
            if retry_after <= 0:
                return True
            logger.info(
                "dm_catchup: history-гейт %s на %.1fs; жду", phone, retry_after
            )
            await asyncio.sleep(retry_after)
        return False

    # --- observability ---

    def status(self) -> dict[str, Any]:
        """Срез для снапшота `dm_listener_status`: живость и исход последнего прохода."""
        return {
            "running": bool(self._task and not self._task.done()),
            "last_runs": dict(self._last_runs),
        }
