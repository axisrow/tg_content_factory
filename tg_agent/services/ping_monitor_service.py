"""Дружеский пинг затихшим DM-контактам (issue #1415, MVP).

`PingMonitorService` проверяет активные мониторы и, если последнее сообщение в
диалоге — моё и оно старше порога, шлёт контакт `nudge_text` с затухающей
прогрессией повторов (+1 день, +7, +30, далее каждые 30). Ответ контакта
(последнее сообщение — не моё) сбрасывает прогрессию в 0, мониторинг
продолжается. История читается живьём через `TelegramActionService.read_history`
(тот же путь, что `dialogs read`; второе MTProto-соединение не открывается —
см. `tg_agent/telegram/dm_history.py`).

Сервис не зависит от scheduler/CLI-обвязки — только репозиторий БД + действия
Telegram; задуман для переноса в будущий пакет tg_messenger как есть.
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone

from tg_agent.database.repositories.ping_monitors import PingMonitorRepository
from tg_agent.models import PingMonitor
from tg_agent.services.telegram_actions import TelegramActionService
from tg_agent.utils.datetime import normalize_utc

logger = logging.getLogger(__name__)

# Шаги затухающей прогрессии ПОСЛЕ базового порога: цикл 1 → +1 день,
# цикл 2 → +7 дней, цикл 3 → +30 дней; далее каждые 30 дней, бессрочно.
_PROGRESSION_DAYS = [1, 7, 30]


def _next_threshold_days(base_days: int, cycle_count: int) -> int:
    """Порог тишины для следующего пинга: базовый N, потом шаги прогрессии.

    Шаг считается ОТ последнего пинга, не от начала тишины: цикл ≥ 3 замирает
    на 30 днях (последний элемент `_PROGRESSION_DAYS`).
    """
    if cycle_count == 0:
        return base_days
    step = _PROGRESSION_DAYS[min(cycle_count - 1, len(_PROGRESSION_DAYS) - 1)]
    return step


class PingMonitorService:
    def __init__(self, repo: PingMonitorRepository, actions: TelegramActionService):
        self._repo = repo
        self._actions = actions

    async def create_monitor(self, monitor: PingMonitor) -> int:
        return await self._repo.create(monitor)

    async def get_monitor(self, monitor_id: int) -> PingMonitor | None:
        return await self._repo.get(monitor_id)

    async def list_monitors(self, active_only: bool = False) -> list[PingMonitor]:
        return await self._repo.list(active_only)

    async def update_monitor(self, monitor_id: int, **fields) -> None:
        """Частично обновить монитор; kwargs — как `PingMonitorRepository.update`."""
        await self._repo.update(monitor_id, **fields)

    async def delete_monitor(self, monitor_id: int) -> None:
        await self._repo.delete(monitor_id)

    async def run_due(self) -> int:
        """Проверить все активные мониторы; вернуть число проверенных.

        Ошибка на одном мониторе пишется в его `error` и не прерывает остальные
        (как в `PhotoAutoUploadService.run_job`).
        """
        monitors = await self._repo.list(active_only=True)
        now = datetime.now(timezone.utc)
        checked = 0
        for monitor in monitors:
            try:
                await self._check_monitor(monitor, now)
            except Exception as exc:
                logger.exception(
                    "Ping monitor check failed: monitor_id=%s phone=%s dialog_id=%s",
                    monitor.id,
                    monitor.phone,
                    monitor.dialog_id,
                )
                await self._repo.update(
                    monitor.id or 0, error=str(exc), last_checked_at=now
                )
                continue
            checked += 1
        return checked

    async def _check_monitor(self, monitor: PingMonitor, now: datetime) -> None:
        monitor_id = monitor.id or 0
        result = await self._actions.read_history(
            phone=monitor.phone, chat_id=monitor.dialog_id, limit=1
        )
        last = result.messages[0] if result.messages else None
        if last is None:
            await self._repo.update(monitor_id, last_checked_at=now, error="")
            return
        last_date = normalize_utc(last.date)
        if not last.out:
            # Контакт ответил: прогрессия сбрасывается, следующая тишина снова
            # стартует с базового порога N. Пинг не шлём, мониторинг продолжается.
            await self._repo.update(
                monitor_id,
                cycle_count=0,
                last_incoming_message_at=last_date,
                last_checked_at=now,
                error="",
            )
            return
        await self._repo.update(
            monitor_id,
            last_outgoing_message_at=last_date,
            last_checked_at=now,
            error="",
        )
        # Отсчёт от последнего события исходящей стороны: свежая дата моего
        # сообщения или метка прошлого пинга (защита от гонки с кэшем истории).
        anchor_candidates = [last_date, normalize_utc(monitor.last_nudge_sent_at)]
        anchor = max(d for d in anchor_candidates if d is not None)
        threshold = _next_threshold_days(monitor.silence_threshold_days, monitor.cycle_count)
        if anchor + timedelta(days=threshold) > now:
            return
        from tg_agent.gateway.context import gateway_intent

        with gateway_intent(f"ping:{monitor_id}:{monitor.cycle_count}:{anchor.isoformat()}", automatic=True):
            await self._actions.send_message(
                phone=monitor.phone, recipient=monitor.dialog_id, text=monitor.nudge_text
            )
        await self._repo.update(
            monitor_id, cycle_count=monitor.cycle_count + 1, last_nudge_sent_at=now
        )
