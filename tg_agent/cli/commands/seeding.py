"""``seeding`` CLI group — композер и публикатор посевных диалогов (#1756/#1757, эпик #1751).

Композер: ``seeding stats`` (профиль реального чата — вход промпта
композера), ``seeding validate`` (валидация сценария по выученному
распределению) и запись черновиков (``thread-create``/``add``/``list``/
``show``) в таблицы ``seeding_threads``/``seeding_messages`` (#1755):
сценарий ложится строками ``status=draft`` с ``planned_at`` из кумулятива
задержек — черновое расписание видно сразу.

Публикатор (#1757): ``approve`` (явное «да» владельца) → ``schedule``
(рельсы безопасности + постановка в расписание + SEED_DUE) → воркер
рассылает due-реплики через UnifiedDispatcher; ``cancel``/``status`` —
управление и наблюдение. Автоматизации планировщика НЕТ (manual-first до
2–3 чистых прогонов). LLM = Claude Code CLI поверх этих команд
(skills/tg-agent/seeding.md, эталон — digest-воркфлоу).
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, NoReturn

import typer
from aiosqlite import IntegrityError
from pydantic import TypeAdapter, ValidationError

from tg_agent.cli import runtime
from tg_agent.cli.commands.common import apply_startup, cli_error, run_async
from tg_agent.models import (
    CollectionTaskType,
    SeedingMessage,
    SeedingMessageStatus,
    SeedingThread,
    SeedingThreadStatus,
)
from tg_agent.services.seeding_composer import (
    ChatStatistics,
    SeedingPersona,
    SeedingScenario,
    compute_chat_statistics,
    derive_planned_schedule,
    validate_scenario,
)
from tg_agent.services.seeding_publisher import validate_schedule_rails


def _parse_start_at(value: str) -> datetime:
    """ISO datetime; наивное трактуется как UTC (база planned_at)."""
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        cli_error(f"--start-at: не ISO datetime: {value!r}")
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _read_json(path: str) -> Any:
    try:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    except FileNotFoundError:
        cli_error(f"Файл не найден: {path}")
    except json.JSONDecodeError as exc:
        cli_error(f"Файл {path} — не валидный JSON: {exc}")


def _schema_error(path: str, exc: ValidationError) -> NoReturn:
    message = f"Схема в {path} нарушена — {exc.error_count()} ошибок:\n" + "\n".join(
        "  " + ".".join(str(p) for p in err["loc"]) + f": {err['msg']}"
        for err in exc.errors(include_url=False)
    )
    cli_error(message)


def _load_scenario(path: str) -> SeedingScenario:
    raw = _read_json(path)
    try:
        return SeedingScenario.model_validate(raw)
    except ValidationError as exc:
        _schema_error(path, exc)


def _load_personas(path: str) -> list[SeedingPersona]:
    raw = _read_json(path)
    try:
        personas = TypeAdapter(list[SeedingPersona]).validate_python(raw)
    except ValidationError as exc:
        _schema_error(path, exc)
    names = [p.name for p in personas]
    dup_names = sorted({n for n in names if names.count(n) > 1})
    if dup_names:
        cli_error(f"Personas: дублирующиеся имена: {dup_names}")
    if len({p.account_phone for p in personas}) != len(personas):
        cli_error("Personas: один аккаунт назначен нескольким персонам.")
    return personas


def _violations_error(violations: list[str]) -> NoReturn:
    message = f"Scenario INVALID — {len(violations)} violation(s):\n" + "\n".join(
        f"  - {item}" for item in violations
    )
    cli_error(message)


async def _load_chat_stats(config_path: str, *, chat: int, days: int) -> ChatStatistics:
    """Собранные сообщения окна → выученный профиль чата (общий ход stats/validate/add)."""
    _, db = await runtime.init_db(config_path)
    try:
        messages = await db.repos.messages.get_recent_for_channels([chat], since_hours=days * 24.0)
    finally:
        await db.close()
    return compute_chat_statistics(messages, chat_channel_id=chat, window_days=days)


async def stats_impl(config_path: str, *, chat: int, days: int) -> ChatStatistics:
    stats = await _load_chat_stats(config_path, chat=chat, days=days)
    print(stats.model_dump_json(indent=2))
    return stats


async def validate_impl(
    config_path: str,
    *,
    scenario_path: str,
    chat: int,
    days: int,
    start_at: datetime | None,
) -> list[str]:
    scenario = _load_scenario(scenario_path)
    if scenario.chat_channel_id != chat:
        cli_error(
            f"Сценарий для чата {scenario.chat_channel_id} не совпадает с --chat {chat} — "
            "валидация по чужому профилю даст неверный вердикт таймингов."
        )
    stats = await _load_chat_stats(config_path, chat=chat, days=days)
    violations = validate_scenario(scenario, stats, start_at=start_at)
    if violations:
        _violations_error(violations)
    print("Scenario OK.")
    return violations


async def thread_create_impl(config_path: str, *, chat: int, personas_path: str, ref: str | None) -> int:
    """Создать тред посева (status=draft); personas_json = «имя → phone».

    Полные стили персон (словарь/пунктуация/окно активности) остаются в
    локальном personas-файле композера — консистентность между сессиями
    держится им; в БД хранится карта персон на аккаунты (контракт #1755).
    """
    personas = _load_personas(personas_path)
    _, db = await runtime.init_db(config_path)
    try:
        thread_id = await db.repos.seeding.create_thread(
            SeedingThread(
                chat_channel_id=chat,
                personas={p.name: p.account_phone for p in personas},
                composer_run_ref=ref,
            )
        )
    finally:
        await db.close()
    print(f"Тред посева #{thread_id} создан (status=draft).")
    print(f"Дальше: seeding add scenario.json --thread {thread_id} --start-at ISO")
    return thread_id


async def add_impl(
    config_path: str,
    *,
    scenario_path: str,
    thread_id: int,
    days: int,
    start_at: datetime,
) -> list[int]:
    """Записать сценарий черновиком: строки seeding_messages со status=draft.

    Тред должен существовать и быть в draft; personas сценария — тем же
    набором, что у треда. Валидация по выученной статистике — до записи.
    """
    scenario = _load_scenario(scenario_path)
    _, db = await runtime.init_db(config_path)
    try:
        thread = await db.repos.seeding.get_thread(thread_id)
        if thread is None:
            cli_error(f"Тред #{thread_id} не найден.")
        if thread.status is not SeedingThreadStatus.DRAFT:
            cli_error(
                f"Тред #{thread_id} в статусе '{thread.status.value}' — сценарий "
                "пишется только в draft (апрув/планирование — у публикатора)."
            )
        if thread.chat_channel_id != scenario.chat_channel_id:
            cli_error(
                f"Сценарий для чата {scenario.chat_channel_id} не совпадает с тредом "
                f"#{thread_id} (чат {thread.chat_channel_id})."
            )
        scenario_map = {p.name: p.account_phone for p in scenario.personas}
        if scenario_map != thread.personas:
            cli_error(
                "Personas сценария не совпадают с тредом — персоны треда "
                f"{sorted(thread.personas)}, сценария {sorted(scenario_map)}."
            )
        messages = await db.repos.messages.get_recent_for_channels(
            [scenario.chat_channel_id], since_hours=days * 24.0
        )
        stats = compute_chat_statistics(
            messages, chat_channel_id=scenario.chat_channel_id, window_days=days
        )
        violations = validate_scenario(scenario, stats, start_at=start_at)
        if violations:
            _violations_error(violations)
        planned = derive_planned_schedule(scenario, start_at)
        ordered = sorted(scenario.messages, key=lambda m: m.position)
        rows = [
            SeedingMessage(
                thread_id=thread_id,
                position=m.position,
                account_phone=scenario_map[m.persona],
                persona_name=m.persona,
                text=m.text,
                reply_to_position=m.reply_to_position,
                planned_at=planned[i],
            )
            for i, m in enumerate(ordered)
        ]
        try:
            ids = await db.repos.seeding.create_messages(thread_id, rows)
        except IntegrityError:
            cli_error(
                f"Тред #{thread_id} уже содержит реплики на этих позициях "
                "(UNIQUE(thread_id, position)) — повторный add невозможен."
            )
    finally:
        await db.close()
    print(f"Записано {len(ids)} реплик в тред #{thread_id} (status=draft, planned_at по задержкам).")
    print(f"Просмотр: seeding show {thread_id}; апрув/планирование — в публикаторе.")
    return ids


async def list_impl(config_path: str, *, status: str | None, limit: int) -> None:
    status_enum: SeedingThreadStatus | None = None
    if status is not None:
        try:
            status_enum = SeedingThreadStatus(status)
        except ValueError:
            cli_error(
                f"--status: неизвестный статус {status!r} "
                "(допустимо: draft|scheduled|active|done|cancelled)"
            )
    _, db = await runtime.init_db(config_path)
    try:
        threads = await db.repos.seeding.list_threads(limit=limit, status=status_enum)
        if not threads:
            print("No seeding threads.")
            return
        fmt = "{:<5} {:<16} {:<10} {:<12} {:<20}"
        print(fmt.format("ID", "Chat", "Status", "Sent/Total", "Personas"))
        print("-" * 70)
        for t in threads:
            progress = await db.repos.seeding.get_thread_progress(t.id or 0)
            print(
                fmt.format(
                    str(t.id),
                    str(t.chat_channel_id),
                    t.status.value,
                    f"{progress.sent}/{progress.total}",
                    ",".join(t.personas) or "-",
                )
            )
    finally:
        await db.close()


async def show_impl(config_path: str, *, thread_id: int) -> None:
    _, db = await runtime.init_db(config_path)
    try:
        thread = await db.repos.seeding.get_thread(thread_id)
        if thread is None:
            cli_error(f"Тред #{thread_id} не найден.")
        messages = await db.repos.seeding.list_messages_for_thread(thread_id)
        progress = await db.repos.seeding.get_thread_progress(thread_id)
    finally:
        await db.close()
    personas = ", ".join(f"{name}({phone})" for name, phone in thread.personas.items()) or "-"
    print(f"Тред #{thread.id}: chat={thread.chat_channel_id} status={thread.status.value}")
    print(f"  personas: {personas}")
    if thread.composer_run_ref:
        print(f"  composer_run_ref: {thread.composer_run_ref}")
    extras = (
        f", {progress.failed} failed, {progress.cancelled} cancelled"
        if progress.failed or progress.cancelled
        else ""
    )
    print(f"  прогресс: {progress.sent}/{progress.total} sent{extras}")
    if not messages:
        print("  Реплик нет — сценарий ещё не записан (seeding add).")
        return
    for m in messages:
        planned = m.planned_at.strftime("%Y-%m-%d %H:%M") if m.planned_at else "-"
        reply = f" reply→#{m.reply_to_position}" if m.reply_to_position is not None else ""
        text = m.text[:60] + ("…" if len(m.text) > 60 else "")
        print(f"  #{m.position:>2} [{m.status.value}] {m.persona_name} @ {planned}{reply}  {text}")


seeding_app = typer.Typer(
    no_args_is_help=True,
    help="Chat seeding: composer (stats/validate/drafts, #1756) + publisher (approve/schedule/cancel, #1757)",
)


# --------------------------------------------------------------------------- #
# Публикатор (#1757): approve / schedule / cancel / status
# --------------------------------------------------------------------------- #


async def approve_impl(config_path: str, *, thread_id: int) -> int:
    """Явное «да» владельца: тред draft→scheduled, реплики draft→approved.

    Атомарно (репозиторий): наполовину апрувнутого треда не бывает. Отправка
    ещё НЕ включается — расписание ставит отдельный ``schedule``.
    """
    _, db = await runtime.init_db(config_path)
    try:
        thread = await db.repos.seeding.get_thread(thread_id)
        if thread is None:
            cli_error(f"Тред #{thread_id} не найден.")
        if thread.status is not SeedingThreadStatus.DRAFT:
            cli_error(
                f"Тред #{thread_id} в статусе '{thread.status.value}' — approve "
                "применим только к draft."
            )
        approved = await db.repos.seeding.approve_thread(thread_id)
        if approved is None:
            cli_error(
                f"Тред #{thread_id} не в draft — approve не применён (гонка или уже апрувнут)."
            )
        if approved == 0:
            cli_error(
                f"Тред #{thread_id} без реплик — нечего утверждать "
                "(запиши сценарий: seeding add)."
            )
    finally:
        await db.close()
    print(f"Тред #{thread_id} апрувнут: {approved} реплик approved, тред scheduled.")
    print("Дальше: явное расписание владельца — seeding schedule.")
    return approved


async def schedule_impl(config_path: str, *, thread_id: int, days: int) -> int:
    """Рельсы безопасности + постановка в расписание + enqueue SEED_DUE.

    Рельсы (publishing-rails #1757): ссылки запрещены, минимальный зазор
    между аккаунтами, дневной лимит на аккаунт, тихие часы чата. Нарушение —
    отклонение всей постановки (CLI не надеется на валидацию композера).
    """
    _, db = await runtime.init_db(config_path)
    try:
        violations = await validate_schedule_rails(db, thread_id, days=days)
        if violations:
            _violations_error(violations)
        scheduled = await db.repos.seeding.schedule_thread_messages(thread_id)
        if scheduled == 0:
            cli_error(
                f"Тред #{thread_id}: ни одной реплики не поставлено в расписание "
                "(approved без planned_at — расписание строит seeding add)."
            )
        next_at = await db.repos.seeding.earliest_scheduled_planned_at()
        task_id = await db.repos.tasks.create_generic_task_if_inactive(
            CollectionTaskType.SEED_DUE,
            title="Отправка посевных реплик (SEED_DUE)",
            run_after=next_at,
        )
    finally:
        await db.close()
    when = next_at.strftime("%Y-%m-%d %H:%M UTC") if next_at else "-"
    print(f"Запланировано {scheduled} реплик; первая отправка ~ {when}.")
    if task_id is None:
        print("SEED_DUE-задача уже активна — воркер подхватит расписание.")
    else:
        print(f"SEED_DUE-задача #{task_id} поставлена (воркер отправит по planned_at).")
    return scheduled


async def cancel_impl(config_path: str, *, thread_id: int) -> int:
    """Отмена треда: тред → cancelled, не-отправленные реплики → cancelled."""
    _, db = await runtime.init_db(config_path)
    try:
        thread = await db.repos.seeding.get_thread(thread_id)
        if thread is None:
            cli_error(f"Тред #{thread_id} не найден.")
        cancelled_thread, cancelled_messages = await db.repos.seeding.cancel_thread(thread_id)
    finally:
        await db.close()
    if not cancelled_thread:
        cli_error(
            f"Тред #{thread_id} в статусе '{thread.status.value}' — терминальный, отмена невозможна."
        )
    print(f"Тред #{thread_id} отменён; реплик отменено: {cancelled_messages}.")
    print("Реплики в 'sending'/'sent' не тронуты: полёт или уже свершившийся факт.")
    return cancelled_messages


async def status_impl(config_path: str, *, thread_id: int) -> None:
    """Сводка публикатора по треду: статусы реплик, due/next, SEED_DUE-задача."""
    _, db = await runtime.init_db(config_path)
    try:
        thread = await db.repos.seeding.get_thread(thread_id)
        if thread is None:
            cli_error(f"Тред #{thread_id} не найден.")
        messages = await db.repos.seeding.list_messages_for_thread(thread_id)
        progress = await db.repos.seeding.get_thread_progress(thread_id)
        counts: dict[str, int] = {}
        for m in messages:
            counts[m.status.value] = counts.get(m.status.value, 0) + 1
        now = datetime.now(timezone.utc)
        due = sum(
            1
            for m in messages
            if m.status is SeedingMessageStatus.SCHEDULED
            and m.planned_at is not None
            and m.planned_at.astimezone(timezone.utc) <= now
        )
        next_planned = min(
            (
                m.planned_at.astimezone(timezone.utc)
                for m in messages
                if m.status is SeedingMessageStatus.SCHEDULED and m.planned_at is not None
            ),
            default=None,
        )
        seed_task_active = await db.repos.tasks.has_active_task(CollectionTaskType.SEED_DUE)
    finally:
        await db.close()
    parts = ", ".join(f"{name}: {count}" for name, count in sorted(counts.items())) or "-"
    print(f"Тред #{thread.id}: chat={thread.chat_channel_id} status={thread.status.value}")
    print(f"  прогресс: {progress.sent}/{progress.total} sent, {progress.failed} failed")
    print(f"  статусы реплик: {parts}")
    print(f"  due сейчас: {due}; следующая отправка: {next_planned or '-'}")
    print(f"  SEED_DUE-задача: {'активна' if seed_task_active else 'нет'}")


@seeding_app.command("stats")
def stats_command(
    ctx: typer.Context,
    chat: int = typer.Argument(
        ..., metavar="chat-id", help="Telegram chat id (negative for groups), as in `dialogs list`"
    ),
    days: int = typer.Option(14, "--days", min=1, help="Window over collected messages"),
) -> None:
    """Learn the real-chat statistics profile (JSON; composer prompt input + validation base)."""
    apply_startup(ctx)
    run_async(stats_impl(ctx.obj.config, chat=chat, days=days))


@seeding_app.command("validate")
def validate_command(
    ctx: typer.Context,
    scenario_path: str = typer.Argument(..., metavar="scenario.json"),
    chat: int = typer.Option(..., "--chat", help="Chat id whose learned stats validate the timing"),
    days: int = typer.Option(14, "--days", min=1, help="Window over collected messages"),
    start_at: str | None = typer.Option(
        None, "--start-at", help="ISO datetime (naive = UTC) — enables the quiet-hour checks"
    ),
) -> None:
    """Validate a scenario against the learned chat profile; exit 1 on violations."""
    apply_startup(ctx)
    run_async(
        validate_impl(
            ctx.obj.config,
            scenario_path=scenario_path,
            chat=chat,
            days=days,
            start_at=_parse_start_at(start_at) if start_at else None,
        )
    )


@seeding_app.command("thread-create")
def thread_create_command(
    ctx: typer.Context,
    chat: int = typer.Option(..., "--chat", help="Telegram chat id to seed"),
    personas_path: str = typer.Option(..., "--personas", metavar="personas.json"),
    ref: str | None = typer.Option(None, "--ref", help="Composer run reference (provenance)"),
) -> None:
    """Create a draft seeding thread (personas map «name → phone»)."""
    apply_startup(ctx)
    run_async(thread_create_impl(ctx.obj.config, chat=chat, personas_path=personas_path, ref=ref))


@seeding_app.command("add")
def add_command(
    ctx: typer.Context,
    scenario_path: str = typer.Argument(..., metavar="scenario.json"),
    thread_id: int = typer.Option(..., "--thread", help="Seeding thread id from `seeding thread-create`"),
    days: int = typer.Option(14, "--days", min=1, help="Stats window for scenario validation"),
    start_at: str = typer.Option(
        ..., "--start-at", help="ISO datetime (naive = UTC): base of the planned_at schedule"
    ),
) -> None:
    """Validate the scenario and record it as draft rows with a planned schedule."""
    apply_startup(ctx)
    run_async(
        add_impl(
            ctx.obj.config,
            scenario_path=scenario_path,
            thread_id=thread_id,
            days=days,
            start_at=_parse_start_at(start_at),
        )
    )


@seeding_app.command("list")
def list_command(
    ctx: typer.Context,
    status: str | None = typer.Option(None, "--status", help="Filter by thread status"),
    limit: int = typer.Option(50, "--limit", help="Max rows"),
) -> None:
    """List seeding threads with sent/total progress (fresh first)."""
    apply_startup(ctx)
    run_async(list_impl(ctx.obj.config, status=status, limit=limit))


@seeding_app.command("show")
def show_command(
    ctx: typer.Context,
    thread_id: int = typer.Argument(..., metavar="thread-id"),
) -> None:
    """Show a seeding thread with its scheduled draft messages."""
    apply_startup(ctx)
    run_async(show_impl(ctx.obj.config, thread_id=thread_id))


@seeding_app.command("approve")
def approve_command(
    ctx: typer.Context,
    thread_id: int = typer.Argument(..., metavar="thread-id"),
) -> None:
    """Owner's explicit YES: thread draft→scheduled, messages draft→approved (no sending yet)."""
    apply_startup(ctx)
    run_async(approve_impl(ctx.obj.config, thread_id=thread_id))


@seeding_app.command("schedule")
def schedule_command(
    ctx: typer.Context,
    thread_id: int = typer.Argument(..., metavar="thread-id"),
    days: int = typer.Option(14, "--days", min=1, help="Stats window for the quiet-hours rail"),
) -> None:
    """Safety rails + put the approved scenario on the schedule (enqueues SEED_DUE)."""
    apply_startup(ctx)
    run_async(schedule_impl(ctx.obj.config, thread_id=thread_id, days=days))


@seeding_app.command("cancel")
def cancel_command(
    ctx: typer.Context,
    thread_id: int = typer.Argument(..., metavar="thread-id"),
) -> None:
    """Cancel a thread: unsent messages → cancelled (sending/sent untouched)."""
    apply_startup(ctx)
    run_async(cancel_impl(ctx.obj.config, thread_id=thread_id))


@seeding_app.command("status")
def status_command(
    ctx: typer.Context,
    thread_id: int = typer.Argument(..., metavar="thread-id"),
) -> None:
    """Publisher summary: message statuses, due-now count, next send, SEED_DUE task."""
    apply_startup(ctx)
    run_async(status_impl(ctx.obj.config, thread_id=thread_id))
