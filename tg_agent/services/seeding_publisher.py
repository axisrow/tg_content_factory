"""Публикатор посевных диалогов (#1757, эпик #1751).

SEED_DUE-ран (#1757): воркер атомарно захватывает due-реплики
(``scheduled`` + ``planned_at <= now`` → ``sending``, claim-гард в
репозитории), отправляет от аккаунта реплики через общий send-механизм
(``TelegramActionService.send_message``, reply-цепочка по ``tg_message_id``
предыдущей реплики) и пишет исход guarded-переходом (``sent`` + id/``failed``
+ error). Повторный due-прогон не может задублировать: отправленная реплика
не в ``scheduled``, отправка без ``tg_message_id`` невоспроизводима.

Рельсы безопасности — ``validate_schedule_rails``, этап ``seeding schedule``
(CLI отклоняет сам, не надеясь на композер): запрет ссылок, минимальный
зазор между аккаунтами, дневной лимит реплик на аккаунт, тихие часы чата
(выученный профиль, как в композере #1756).

Автоматизации планировщика НЕТ: SEED_DUE ставится только явными действиями
владельца (``seeding schedule``) или resume-цепочкой уже апрувнутого
расписания (``_enqueue_next_run`` / reconcile старта воркера). Manual-first
до 2–3 чистых прогонов.
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any

from tg_agent.models import (
    CollectionTaskType,
    SeedingMessage,
    SeedingMessageStatus,
    SeedingThreadStatus,
)
from tg_agent.services.seeding_composer import (
    CROSS_PERSONA_MIN_GAP_SECONDS,
    LINK_RE,
    ChatStatistics,
    compute_chat_statistics,
)

if TYPE_CHECKING:
    from tg_agent.database.facade import Database

logger = logging.getLogger(__name__)

#: Дневной лимит реплик на аккаунт по UTC-дням (все треды суммарно).
#: Эпик #1751: аккаунты посева тем же пулом собирают каналы — только
#: консервативные ставки. Публичное имя: тесты пинят значение.
PER_ACCOUNT_DAILY_LIMIT = 12

#: Потолок claims за один SEED_DUE-ран (repлика отправляется синхронно,
#: цикл не должен крутиться вечно при потоке due-строк).
_RUN_CLAIM_LIMIT = 50

_RESTART_ERROR_TEXT = (
    "воркер перезапущен во время отправки; исход RPC неизвестен — "
    "сверь чат вручную и перепланируй осознанно"
)


def _error_text(exc: Exception) -> str:
    """Текст для error_text: пустой str() исключения подменяется классом."""
    return str(exc).strip()[:500] or type(exc).__name__


async def validate_schedule_rails(
    db: "Database",
    thread_id: int,
    *,
    days: int = 14,
) -> list[str]:
    """Рельсы безопасности на этапе ``seeding schedule``; пустой список = проход.

    Применяются к апрувнутому (``scheduled`` тред, ``approved`` реплики)
    сценарию с уже проставленным ``planned_at`` (#1756 add): ссылки,
    межаккаунтный зазор, дневной лимит на аккаунт (по всем тредам), тихие
    часы выученного профиля чата. CLI отклоняет запуск по непустому списку.
    """
    repo = db.repos.seeding
    thread = await repo.get_thread(thread_id)
    if thread is None:
        return [f"тред #{thread_id} не найден"]
    if thread.status is not SeedingThreadStatus.SCHEDULED:
        return [
            f"тред #{thread_id} в статусе '{thread.status.value}' — рельсы "
            "применяются после approve (тред scheduled, реплики approved)"
        ]
    messages = await repo.list_messages_for_thread(thread_id)
    if not messages:
        return ["тред без реплик — нечего планировать"]
    wrong_status = [m for m in messages if m.status is not SeedingMessageStatus.APPROVED]
    if wrong_status:
        return [
            f"реплика #{m.position} в статусе '{m.status.value}' — schedule идёт "
            "после approve (все реплики approved)"
            for m in wrong_status
        ]

    violations: list[str] = []
    ordered = sorted(messages, key=lambda m: m.position)

    # 1. Ссылки запрещены эпиком (spam-reportable) — ретроспективная проверка:
    # композер мог быть обойдён прямой записью в БД.
    for m in ordered:
        if LINK_RE.search(m.text):
            violations.append(
                f"#{m.position}: ссылки в посевах запрещены (spam-reportable) — убери URL"
            )

    # 2. Минимальный зазор между репликами разных аккаунтов.
    prev: SeedingMessage | None = None
    for m in ordered:
        if m.planned_at is None:
            violations.append(
                f"#{m.position}: planned_at не задан — расписание строит `seeding add`"
            )
            prev = m
            continue
        if (
            prev is not None
            and prev.planned_at is not None
            and prev.account_phone != m.account_phone
        ):
            gap = (m.planned_at - prev.planned_at).total_seconds()
            if gap < CROSS_PERSONA_MIN_GAP_SECONDS:
                violations.append(
                    f"#{m.position}: зазор между аккаунтами {prev.account_phone} → "
                    f"{m.account_phone} {gap:.0f}s (< {CROSS_PERSONA_MIN_GAP_SECONDS}s)"
                )
        prev = m

    # 3. Дневной лимит на аккаунт (UTC-дни planned_at, все треды суммарно).
    per_day_this: dict[str, dict[str, int]] = {}
    for m in ordered:
        if m.planned_at is None:
            continue
        day = m.planned_at.astimezone(timezone.utc).strftime("%Y-%m-%d")
        per_day_this.setdefault(m.account_phone, {})
        per_day_this[m.account_phone][day] = per_day_this[m.account_phone].get(day, 0) + 1
    for phone, days_map in sorted(per_day_this.items()):
        existing = await repo.count_account_messages_by_day(phone, exclude_thread_id=thread_id)
        for day, own in sorted(days_map.items()):
            total = existing.get(day, 0) + own
            if total > PER_ACCOUNT_DAILY_LIMIT:
                violations.append(
                    f"аккаунт {phone}, день {day}: {total} реплик "
                    f"(> дневного лимита {PER_ACCOUNT_DAILY_LIMIT}; вне треда уже "
                    f"{existing.get(day, 0)})"
                )

    # 4. Тихие часы — выученный профиль чата, тот же расчёт, что в композере.
    stats = await _learn_chat_stats(db, thread.chat_channel_id, days=days)
    for m in ordered:
        if m.planned_at is None:
            continue
        planned_utc = m.planned_at.astimezone(timezone.utc)
        if planned_utc.hour in stats.quiet_hours:
            violations.append(
                f"#{m.position}: планируемое время {planned_utc:%H:%M} UTC попадает "
                f"в тихие часы чата {stats.quiet_hours}"
            )
    return violations


async def _learn_chat_stats(db: "Database", chat_channel_id: int, *, days: int) -> ChatStatistics:
    """Профиль чата из собранных сообщений (общий ход композера и rails)."""
    messages = await db.repos.messages.get_recent_for_channels(
        [chat_channel_id], since_hours=days * 24.0
    )
    return compute_chat_statistics(
        messages, chat_channel_id=chat_channel_id, window_days=days
    )


class SeedingPublisherService:
    """SEED_DUE-исполнитель: claim → send → guarded-исход → переходы треда.

    ``actions`` — ``TelegramActionService`` (общий send-механизм с
    ``dialogs send``); в тестах — фейк с тем же контрактом ``send_message``.
    """

    def __init__(self, db: "Database", actions: Any) -> None:
        self._db = db
        self._actions = actions

    async def run_due(
        self, *, now: datetime | None = None, limit: int = _RUN_CLAIM_LIMIT
    ) -> int:
        """Отправить все due-реплики; вернуть число успешно отправленных.

        ``now`` фиксирован на старте рана: последующие реплики со сдвинутым
        ``planned_at`` забирает цепочечный SEED_DUE (``_enqueue_next_run``),
        а не тот же цикл — диалог идёт по расписанию, а не залпом.
        """
        now = now or datetime.now(timezone.utc)
        sent = 0
        touched: set[int] = set()
        for _ in range(limit):
            msg = await self._db.repos.seeding.claim_next_due_message(now)
            if msg is None:
                break
            touched.add(msg.thread_id)
            if await self._send_claimed(msg):
                sent += 1
        for thread_id in sorted(touched):
            await self._sync_thread_status(thread_id)
        await self._enqueue_next_run(dedup_active=False)
        return sent

    async def _send_claimed(self, msg: SeedingMessage) -> bool:
        assert msg.id is not None
        try:
            tg_message_id = await self._send_one(msg)
        except Exception as exc:
            logger.warning(
                "Seeding send failed for message #%s (thread %s): %s",
                msg.position,
                msg.thread_id,
                exc,
            )
            await self._db.repos.seeding.set_message_status(
                msg.id,
                SeedingMessageStatus.FAILED,
                error_text=_error_text(exc),
            )
            return False
        await self._db.repos.seeding.set_message_status(
            msg.id,
            SeedingMessageStatus.SENT,
            tg_message_id=tg_message_id,
        )
        return True

    async def _send_one(self, msg: SeedingMessage) -> int:
        thread = await self._db.repos.seeding.get_thread(msg.thread_id)
        if thread is None:
            raise RuntimeError(f"тред #{msg.thread_id} исчез между claim и отправкой")
        reply_to: int | None = None
        if msg.reply_to_position is not None:
            sibling = await self._db.repos.seeding.get_message_by_position(
                msg.thread_id, msg.reply_to_position
            )
            # Лучший разумный исход: собеседник не отправлен/упал — реплика
            # уходит без reply, а не падает вся цепочка.
            if sibling is not None and sibling.tg_message_id is not None:
                reply_to = sibling.tg_message_id
        result = await self._actions.send_message(
            phone=msg.account_phone,
            recipient=thread.chat_channel_id,
            text=msg.text,
            reply_to=reply_to,
        )
        if result.message_id is None:
            raise RuntimeError("send_message вернул сообщение без id")
        return int(result.message_id)

    async def _sync_thread_status(self, thread_id: int) -> None:
        """Переходы треда по факту рана: scheduled→active (есть отправленные),
        →done (все реплики терминальны). Guarded-запись молча пропускает
        нелегальные переходы (например, all-failed из scheduled остаётся)."""
        messages = await self._db.repos.seeding.list_messages_for_thread(thread_id)
        if not messages:
            return
        if any(m.status is SeedingMessageStatus.SENT for m in messages):
            await self._db.repos.seeding.set_thread_status(
                thread_id, SeedingThreadStatus.ACTIVE
            )
        if all(
            m.status
            in (SeedingMessageStatus.SENT, SeedingMessageStatus.FAILED, SeedingMessageStatus.CANCELLED)
            for m in messages
        ):
            await self._db.repos.seeding.set_thread_status(thread_id, SeedingThreadStatus.DONE)

    async def _enqueue_next_run(self, *, dedup_active: bool) -> int | None:
        """Цепочка уже апрувнутого расписания: следующий SEED_DUE на ближайший
        planned_at (без крона и без участия планировщика).

        ``dedup_active=True`` (reconcile старта: активной задачи быть не может)
        идёт через create_generic_task_if_inactive; ``False`` (хвост рана —
        задача этого рана ещё RUNNING, дедуп по активности подавил бы вставку)
        — через idempotency_key.

        ponytail: ключ ``seed_due_chain:<ts>`` переживает завершённую задачу —
        перепланировка ровно на тот же момент не перезакинет задачу; дыру
        закрывает reconcile старта воркера и повторный ``seeding schedule``.
        """
        next_at = await self._db.repos.seeding.earliest_scheduled_planned_at()
        if next_at is None:
            return None
        if dedup_active:
            return await self._db.repos.tasks.create_generic_task_if_inactive(
                CollectionTaskType.SEED_DUE,
                title="Отправка посевных реплик (SEED_DUE)",
                run_after=next_at,
            )
        return await self._db.repos.tasks.create_generic_task(
            CollectionTaskType.SEED_DUE,
            title="Отправка посевных реплик (SEED_DUE)",
            run_after=next_at,
            idempotency_key=f"seed_due_chain:{next_at.astimezone(timezone.utc).isoformat()}",
        )

    async def reconcile_on_startup(self) -> int:
        """reconcile старта воркера (#1757): зависшие ``sending`` → ``failed``
        (повтор мог бы задублировать — исход RPC неизвестен) + перевзвод
        цепочечного SEED_DUE на оставшееся расписание. Вернуть число упавших."""
        stuck = await self._db.repos.seeding.fail_stuck_sending(_RESTART_ERROR_TEXT)
        if stuck:
            logger.warning("Seeding reconcile: %d stuck 'sending' replicas failed on startup", stuck)
        await self._enqueue_next_run(dedup_active=True)
        return stuck
