"""``seeding`` CLI group — композер посевных диалогов (#1756, эпик #1751).

Детерминированная часть работает сразу: ``seeding stats`` (профиль
реального чата — вход промпта композера) и ``seeding validate``
(валидация сценария по выученному распределению). Команды записи
черновиков (``thread-create``/``add``/``list``/``show``) — контракт
параллельного #1755 (``seeding_threads``/``seeding_messages`` +
``SeedingRepository``): до его мержа они валидируют вход и громко
отказывают на записи; запись включается следом поверх #1755.

LLM = Claude Code CLI поверх этих команд (skills/tg-agent/seeding.md,
эталон — digest-воркфлоу); z.ai запрещён.
"""

from __future__ import annotations

import importlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import typer
from pydantic import TypeAdapter, ValidationError

from tg_agent.cli import runtime
from tg_agent.cli.commands.common import apply_startup, run_async
from tg_agent.services.seeding_composer import (
    ChatStatistics,
    SeedingPersona,
    SeedingScenario,
    compute_chat_statistics,
    validate_scenario,
)


def _require_seeding_storage() -> None:
    """Хранение черновиков — контракт #1755; до его мержа команды отказывают."""
    # ponytail: gating-стаб — тела записи появятся в follow-up поверх #1755;
    # контракт (сценарий → строки seeding_messages со status=draft) уже стабилен.
    try:
        importlib.import_module("tg_agent.database.repositories.seeding")
    except ImportError:
        print("Seeding storage is not available: seeding_threads/seeding_messages +")
        print("SeedingRepository land with #1755; draft writes activate on top of it.")
        raise typer.Exit(1) from None


def _parse_start_at(value: str) -> datetime:
    """ISO datetime; наивное трактуется как UTC (база planned_at)."""
    try:
        parsed = datetime.fromisoformat(value)
    except ValueError:
        print(f"--start-at: не ISO datetime: {value!r}")
        raise typer.Exit(1) from None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _read_json(path: str) -> Any:
    try:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    except FileNotFoundError:
        print(f"Файл не найден: {path}")
        raise typer.Exit(1) from None
    except json.JSONDecodeError as exc:
        print(f"Файл {path} — не валидный JSON: {exc}")
        raise typer.Exit(1) from None


def _print_schema_errors(path: str, exc: ValidationError) -> None:
    print(f"Схема сценария в {path} нарушена — {exc.error_count()} ошибок:")
    for err in exc.errors(include_url=False):
        loc = ".".join(str(p) for p in err["loc"])
        print(f"  {loc}: {err['msg']}")


def _load_scenario(path: str) -> SeedingScenario:
    raw = _read_json(path)
    try:
        return SeedingScenario.model_validate(raw)
    except ValidationError as exc:
        _print_schema_errors(path, exc)
        raise typer.Exit(1) from None


def _load_personas(path: str) -> list[SeedingPersona]:
    raw = _read_json(path)
    try:
        personas = TypeAdapter(list[SeedingPersona]).validate_python(raw)
    except ValidationError as exc:
        _print_schema_errors(path, exc)
        raise typer.Exit(1) from None
    names = [p.name for p in personas]
    dup_names = sorted({n for n in names if names.count(n) > 1})
    if dup_names:
        print(f"Personas: дублирующиеся имена: {dup_names}")
        raise typer.Exit(1)
    if len({p.account_phone for p in personas}) != len(personas):
        print("Personas: один аккаунт назначен нескольким персонам.")
        raise typer.Exit(1)
    return personas


async def _load_chat_stats(config_path: str, *, chat: int, days: int) -> ChatStatistics:
    """Собранные сообщения окна → выученный профиль чата (общий ход stats/validate/add)."""
    _, db = await runtime.init_db(config_path)
    try:
        messages = await db.repos.messages.get_recent_for_channels([chat], since_hours=days * 24.0)
    finally:
        await db.close()
    return compute_chat_statistics(messages, chat_channel_id=chat, window_days=days)


async def stats_impl(config_path: str, *, chat: int, days: int) -> ChatStatistics:
    return await _load_chat_stats(config_path, chat=chat, days=days)


async def validate_impl(
    config_path: str,
    *,
    scenario_path: str,
    chat: int,
    days: int,
    start_at: datetime | None,
) -> list[str]:
    scenario = _load_scenario(scenario_path)
    stats = await _load_chat_stats(config_path, chat=chat, days=days)
    return validate_scenario(scenario, stats, start_at=start_at)


async def add_impl(
    config_path: str,
    *,
    scenario_path: str,
    thread: int,
    days: int,
    start_at: datetime,
) -> None:
    scenario = _load_scenario(scenario_path)
    stats = await _load_chat_stats(config_path, chat=scenario.chat_channel_id, days=days)
    violations = validate_scenario(scenario, stats, start_at=start_at)
    if violations:
        print(f"Scenario INVALID — {len(violations)} violation(s):")
        for item in violations:
            print(f"  - {item}")
        raise typer.Exit(1)
    _require_seeding_storage()


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
    stats = run_async(stats_impl(ctx.obj.config, chat=chat, days=days))
    print(stats.model_dump_json(indent=2))


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
    violations = run_async(
        validate_impl(
            ctx.obj.config,
            scenario_path=scenario_path,
            chat=chat,
            days=days,
            start_at=_parse_start_at(start_at) if start_at else None,
        )
    )
    if violations:
        print(f"Scenario INVALID — {len(violations)} violation(s):")
        for item in violations:
            print(f"  - {item}")
        raise typer.Exit(1)
    print("Scenario OK.")


@seeding_app.command("thread-create")
def thread_create_command(
    ctx: typer.Context,
    chat: int = typer.Option(..., "--chat", help="Telegram chat id to seed"),
    personas_path: str = typer.Option(..., "--personas", metavar="personas.json"),
    ref: str | None = typer.Option(None, "--ref", help="Composer run reference (provenance)"),
) -> None:
    """Create a draft seeding thread (write activates with the #1755 storage)."""
    apply_startup(ctx)
    _load_personas(personas_path)
    print("Personas OK (validated, not stored).")
    _require_seeding_storage()


@seeding_app.command("add")
def add_command(
    ctx: typer.Context,
    scenario_path: str = typer.Argument(..., metavar="scenario.json"),
    thread: int = typer.Option(..., "--thread", help="Seeding thread id from `seeding thread-create`"),
    days: int = typer.Option(14, "--days", min=1, help="Stats window for scenario validation"),
    start_at: str = typer.Option(
        ..., "--start-at", help="ISO datetime (naive = UTC): base of the planned_at schedule"
    ),
) -> None:
    """Validate and record a scenario as draft rows (write activates with #1755)."""
    apply_startup(ctx)
    run_async(
        add_impl(
            ctx.obj.config,
            scenario_path=scenario_path,
            thread=thread,
            days=days,
            start_at=_parse_start_at(start_at),
        )
    )


@seeding_app.command("list")
def list_command(
    ctx: typer.Context,
    status: str | None = typer.Option(None, "--status", help="Filter by thread status"),
    limit: int = typer.Option(100, "--limit", help="Max rows"),
) -> None:
    """List seeding threads with sent/total progress (read activates with #1755)."""
    apply_startup(ctx)
    _require_seeding_storage()


@seeding_app.command("show")
def show_command(
    ctx: typer.Context,
    thread: int = typer.Argument(..., metavar="thread-id"),
) -> None:
    """Show a seeding thread with its draft messages (read activates with #1755)."""
    apply_startup(ctx)
    _require_seeding_storage()
