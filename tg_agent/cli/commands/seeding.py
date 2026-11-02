"""``seeding`` CLI group — композер посевных диалогов (#1756, эпик #1751).

Детерминированная часть: ``seeding stats`` (профиль реального чата — вход
промпта композера), ``seeding validate`` (валидация сценария по выученному
распределению) и запись черновиков (``thread-create``/``add``/``list``/
``show``) в таблицы ``seeding_threads``/``seeding_messages`` (#1755):
сценарий ложится строками ``status=draft`` с ``planned_at`` из кумулятива
задержек — черновое расписание видно сразу.

Апрув/планирование/отмена — жизненный цикл публикатора (отдельный
sub-issue); CLI пишет только в ``draft``. LLM = Claude Code CLI поверх
этих команд (skills/tg-agent/seeding.md, эталон — digest-воркфлоу).
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
from tg_agent.models import SeedingMessage, SeedingThread, SeedingThreadStatus
from tg_agent.services.seeding_composer import (
    ChatStatistics,
    SeedingPersona,
    SeedingScenario,
    compute_chat_statistics,
    derive_planned_schedule,
    validate_scenario,
)


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
    help="Chat seeding composer: real-chat stats, scenario validation, draft threads (#1756)",
)


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
