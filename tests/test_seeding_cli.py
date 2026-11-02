"""CLI-тесты композера посевных диалогов (#1756): stats/validate/черновики.

``stats``/``validate``/запись черновиков работают на реальной SQLite
(cli_db) с засеянными сообщениями; жизненный цикл статусов #1755 тестируется
в репозиторных тестах — здесь CLI-поверхность. Живой LLM и живые аккаунты
не участвуют: сценарий — фикстура-файл, композер по контракту — внешний
Claude Code CLI.
"""

from __future__ import annotations

import asyncio
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
import typer

from tg_agent.cli.commands import seeding as seeding_cli
from tg_agent.config import AppConfig
from tg_agent.models import Channel, Message, SeedingThread, SeedingThreadStatus

CHAT_ID = -100999


def _start_at(base: datetime) -> datetime:
    """Aware-UTC старт в том же часе, где сидят первые сообщения сида."""
    return base.replace(tzinfo=timezone.utc) + timedelta(minutes=10)


def _reopen(db):
    """Переоткрыть закрытый fixture-инстанс для инспекции (паттерн handoff-тестов)."""
    if db._connection.db is None:
        asyncio.run(db.initialize())


def _seed_chat(db) -> datetime:
    """Чат с широким распределением пауз (5..240 мин) в окне сбора; вернуть базу.

    get_recent_for_channels делает INNER JOIN на channels — строка канала нужна.
    """
    base = datetime.now(timezone.utc).replace(tzinfo=None, second=0, microsecond=0) - timedelta(hours=6)
    _reopen(db)
    asyncio.run(
        db.repos.channels.add_channel(
            Channel(channel_id=CHAT_ID, title="Тестовый чат", channel_type="supergroup")
        )
    )
    for i, minute in enumerate((0, 5, 11, 58, 240)):
        msg = Message(
            channel_id=CHAT_ID,
            message_id=i + 1,
            date=base + timedelta(minutes=minute),
            text=f"тестовое сообщение номер {i + 1}",
            sender_id=(i % 2) + 1,
            sender_name="Аня" if i % 2 == 0 else "Борис",
        )
        asyncio.run(db.repos.messages.insert_message(msg))
    return base


def _run_impl(coro_factory, cli_db, capsys):
    """Запустить impl на cli_db; на успехе вернуть (результат, вывод).

    typer.Exit НЕ перехватывается — вывод тогда читает сам тест
    (readouterr внутри хелпера не успевает поглотить capture).
    """
    _reopen(cli_db)
    config = AppConfig()

    async def fake_init_db(_):
        return config, cli_db

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(seeding_cli.runtime, "init_db", fake_init_db)
        result = seeding_cli.run_async(coro_factory())
    return result, capsys.readouterr().out


def _make_thread(db, *, personas: dict[str, str] | None = None, chat_id: int = CHAT_ID) -> int:
    _reopen(db)
    return asyncio.run(
        db.repos.seeding.create_thread(
            SeedingThread(
                chat_channel_id=chat_id,
                personas=personas or {"Аня": "+79990000001", "Борис": "+79990000002"},
            )
        )
    )


def _write_json(tmp_path: Path, name: str, payload) -> str:
    path = tmp_path / name
    path.write_text(json.dumps(payload, ensure_ascii=False), encoding="utf-8")
    return str(path)


def _personas_file(tmp_path: Path) -> str:
    return _write_json(
        tmp_path,
        "personas.json",
        [
            {"name": "Аня", "account_phone": "+79990000001"},
            {"name": "Борис", "account_phone": "+79990000002"},
        ],
    )


def _scenario_dict(chat_id: int = CHAT_ID, delays: list[int] | None = None) -> dict:
    delays = delays or [400, 600]
    return {
        "chat_channel_id": chat_id,
        "personas": [
            {"name": "Аня", "account_phone": "+79990000001"},
            {"name": "Борис", "account_phone": "+79990000002"},
        ],
        "messages": [
            {"position": 1, "persona": "Аня", "text": "первая реплика", "delay_seconds": delays[0]},
            {
                "position": 2,
                "persona": "Борис",
                "text": "вторая реплика длиннее",
                "delay_seconds": delays[1],
                "reply_to_position": 1,
            },
        ],
    }


def _scenario_file(tmp_path: Path, **kwargs) -> str:
    return _write_json(tmp_path, "scenario.json", _scenario_dict(**kwargs))


# --------------------------------------------------------------------------- #
# stats / validate
# --------------------------------------------------------------------------- #


def test_stats_impl_returns_profile(cli_db, capsys):
    _seed_chat(cli_db)

    stats, _ = _run_impl(lambda: seeding_cli.stats_impl("cfg", chat=CHAT_ID, days=14), cli_db, capsys)

    assert stats.message_count == 5
    assert stats.chat_channel_id == CHAT_ID
    assert stats.sender_count == 2
    # p10 пауз 300/360/2820/10920 = 318 → машинный пол не активен
    assert stats.min_gap_seconds == 318
    assert stats.max_gap_seconds == 10920


def test_stats_command_prints_json(cli_db, capsys):
    _seed_chat(cli_db)

    _, out = _run_impl(lambda: seeding_cli.stats_impl("cfg", chat=CHAT_ID, days=14), cli_db, capsys)
    stats = json.loads(out)
    assert stats["message_count"] == 5
    assert stats["gaps_seconds"]["max"] > 0
    assert isinstance(stats["quiet_hours"], list)


def test_validate_impl_valid_scenario(cli_db, capsys, tmp_path):
    _seed_chat(cli_db)
    path = _scenario_file(tmp_path, delays=[400, 600])

    violations, _ = _run_impl(
        lambda: seeding_cli.validate_impl("cfg", scenario_path=path, chat=CHAT_ID, days=14, start_at=None),
        cli_db,
        capsys,
    )
    assert violations == []


def test_validate_impl_invalid_scenario(cli_db, capsys, tmp_path):
    _seed_chat(cli_db)
    path = _scenario_file(tmp_path, delays=[2, 300])

    with pytest.raises(typer.Exit):
        _run_impl(
            lambda: seeding_cli.validate_impl("cfg", scenario_path=path, chat=CHAT_ID, days=14, start_at=None),
            cli_db,
            capsys,
        )
    out = capsys.readouterr().err
    assert "Scenario INVALID" in out
    assert "машинно" in out


def test_validate_impl_chat_mismatch(cli_db, capsys, tmp_path):
    """Сценарий для другого чата не валидируется молча по чужому профилю (ревью #1763)."""
    _seed_chat(cli_db)
    path = _scenario_file(tmp_path, chat_id=-100888)

    with pytest.raises(typer.Exit):
        _run_impl(
            lambda: seeding_cli.validate_impl("cfg", scenario_path=path, chat=CHAT_ID, days=14, start_at=None),
            cli_db,
            capsys,
        )
    err = capsys.readouterr().err
    assert "не совпадает с --chat" in err


def test_validate_impl_schema_error_exits(cli_db, capsys, tmp_path):
    _seed_chat(cli_db)
    path = _write_json(tmp_path, "scenario.json", {"chat_channel_id": CHAT_ID, "personas": [], "messages": 5})

    with pytest.raises(typer.Exit):
        _run_impl(
            lambda: seeding_cli.validate_impl("cfg", scenario_path=path, chat=CHAT_ID, days=14, start_at=None),
            cli_db,
            capsys,
        )
    assert "Схема" in capsys.readouterr().err


def test_validate_impl_missing_file(cli_db, capsys, tmp_path):
    _seed_chat(cli_db)

    with pytest.raises(typer.Exit):
        _run_impl(
            lambda: seeding_cli.validate_impl(
                "cfg", scenario_path=str(tmp_path / "nope.json"), chat=CHAT_ID, days=14, start_at=None
            ),
            cli_db,
            capsys,
        )
    assert "не найден" in capsys.readouterr().err


# --------------------------------------------------------------------------- #
# thread-create / add — запись черновиков (#1755 storage)
# --------------------------------------------------------------------------- #


def test_thread_create_impl_writes_draft_thread(cli_db, capsys, tmp_path):
    personas_path = _personas_file(tmp_path)

    thread_id, out = _run_impl(
        lambda: seeding_cli.thread_create_impl("cfg", chat=CHAT_ID, personas_path=personas_path, ref="run-42"),
        cli_db,
        capsys,
    )

    assert f"#{thread_id}" in out
    _reopen(cli_db)
    thread = asyncio.run(cli_db.repos.seeding.get_thread(thread_id))
    assert thread.status is SeedingThreadStatus.DRAFT
    assert thread.chat_channel_id == CHAT_ID
    assert thread.personas == {"Аня": "+79990000001", "Борис": "+79990000002"}
    assert thread.composer_run_ref == "run-42"


def test_thread_create_personas_schema_error(cli_db, capsys, tmp_path):
    path = _write_json(tmp_path, "personas.json", [{"name": "Аня"}])  # нет account_phone
    _seed_chat(cli_db)

    with pytest.raises(typer.Exit):
        _run_impl(
            lambda: seeding_cli.thread_create_impl("cfg", chat=CHAT_ID, personas_path=path, ref=None),
            cli_db,
            capsys,
        )
    assert "Схема" in capsys.readouterr().err


def test_thread_create_personas_duplicate_phone(cli_db, capsys, tmp_path):
    path = _write_json(
        tmp_path,
        "personas.json",
        [
            {"name": "Аня", "account_phone": "+79990000001"},
            {"name": "Борис", "account_phone": "+79990000001"},
        ],
    )
    _seed_chat(cli_db)

    with pytest.raises(typer.Exit):
        _run_impl(
            lambda: seeding_cli.thread_create_impl("cfg", chat=CHAT_ID, personas_path=path, ref=None),
            cli_db,
            capsys,
        )
    assert "нескольким персонам" in capsys.readouterr().err


def _add(cli_db, tmp_path, thread_id, capsys, base, **kwargs):
    path = _scenario_file(tmp_path, **kwargs)
    return _run_impl(
        lambda: seeding_cli.add_impl(
            "cfg", scenario_path=path, thread_id=thread_id, days=14, start_at=_start_at(base)
        ),
        cli_db,
        capsys,
    )


def test_add_impl_writes_draft_rows(cli_db, capsys, tmp_path):
    base = _seed_chat(cli_db)
    thread_id = _make_thread(cli_db)

    ids, out = _add(cli_db, tmp_path, thread_id, capsys, base)

    assert len(ids) == 2
    assert "status=draft" in out
    _reopen(cli_db)
    rows = asyncio.run(cli_db.repos.seeding.list_messages_for_thread(thread_id))
    assert [r.position for r in rows] == [1, 2]
    assert all(r.status is not None and r.status.value == "draft" for r in rows)
    assert [r.account_phone for r in rows] == ["+79990000001", "+79990000002"]
    start = _start_at(base)
    assert rows[0].planned_at == start + timedelta(seconds=400)
    assert rows[1].planned_at == start + timedelta(seconds=1000)
    assert rows[1].reply_to_position == 1


def test_add_impl_rejects_invalid_scenario_before_write(cli_db, capsys, tmp_path):
    base = _seed_chat(cli_db)
    thread_id = _make_thread(cli_db)

    with pytest.raises(typer.Exit):
        _add(cli_db, tmp_path, thread_id, capsys, base, delays=[2, 300])

    out = capsys.readouterr().err
    assert "Scenario INVALID" in out
    _reopen(cli_db)
    assert asyncio.run(cli_db.repos.seeding.list_messages_for_thread(thread_id)) == []


def test_add_impl_unknown_thread(cli_db, capsys, tmp_path):
    base = _seed_chat(cli_db)

    with pytest.raises(typer.Exit):
        _add(cli_db, tmp_path, 999, capsys, base)
    assert "не найден" in capsys.readouterr().err


def test_add_impl_chat_mismatch(cli_db, capsys, tmp_path):
    base = _seed_chat(cli_db)
    thread_id = _make_thread(cli_db, chat_id=-100888)

    with pytest.raises(typer.Exit):
        _add(cli_db, tmp_path, thread_id, capsys, base)
    assert "не совпадает" in capsys.readouterr().err


def test_add_impl_personas_mismatch(cli_db, capsys, tmp_path):
    base = _seed_chat(cli_db)
    thread_id = _make_thread(cli_db, personas={"Аня": "+79990000001"})

    with pytest.raises(typer.Exit):
        _add(cli_db, tmp_path, thread_id, capsys, base)
    assert "не совпадают с тредом" in capsys.readouterr().err


def test_add_impl_refuses_non_draft_thread(cli_db, capsys, tmp_path):
    base = _seed_chat(cli_db)
    thread_id = _make_thread(cli_db)
    _reopen(cli_db)
    assert asyncio.run(cli_db.repos.seeding.set_thread_status(thread_id, SeedingThreadStatus.SCHEDULED))

    with pytest.raises(typer.Exit):
        _add(cli_db, tmp_path, thread_id, capsys, base)
    assert "только в draft" in capsys.readouterr().err


def test_add_impl_duplicate_positions_rejected(cli_db, capsys, tmp_path):
    base = _seed_chat(cli_db)
    thread_id = _make_thread(cli_db)
    _add(cli_db, tmp_path, thread_id, capsys, base)
    capsys.readouterr()

    with pytest.raises(typer.Exit):
        _add(cli_db, tmp_path, thread_id, capsys, base)
    assert "позициях" in capsys.readouterr().err


# --------------------------------------------------------------------------- #
# list / show
# --------------------------------------------------------------------------- #


def test_list_impl_prints_progress(cli_db, capsys, tmp_path):
    base = _seed_chat(cli_db)
    thread_id = _make_thread(cli_db)
    _add(cli_db, tmp_path, thread_id, capsys, base)
    capsys.readouterr()

    _, out = _run_impl(lambda: seeding_cli.list_impl("cfg", status=None, limit=50), cli_db, capsys)
    assert str(thread_id) in out
    assert "0/2" in out


def test_list_impl_status_filter_before_limit(cli_db, capsys):
    """--status фильтрует в SQL ДО LIMIT: старые draft-треды не прячутся (ревью #1763)."""
    _seed_chat(cli_db)
    draft_thread = _make_thread(cli_db)
    scheduled_thread = _make_thread(cli_db)
    _reopen(cli_db)
    assert asyncio.run(
        cli_db.repos.seeding.set_thread_status(scheduled_thread, SeedingThreadStatus.SCHEDULED)
    )

    _, out = _run_impl(
        lambda: seeding_cli.list_impl("cfg", status="draft", limit=1), cli_db, capsys
    )
    assert str(draft_thread) in out
    assert str(scheduled_thread) not in out

    with pytest.raises(typer.Exit):
        _run_impl(
            lambda: seeding_cli.list_impl("cfg", status="bogus", limit=50), cli_db, capsys
        )
    assert "неизвестный статус" in capsys.readouterr().err


def test_list_impl_empty(cli_db, capsys):
    _seed_chat(cli_db)

    _, out = _run_impl(lambda: seeding_cli.list_impl("cfg", status=None, limit=50), cli_db, capsys)
    assert "No seeding threads." in out


def test_show_impl_prints_thread_and_messages(cli_db, capsys, tmp_path):
    base = _seed_chat(cli_db)
    thread_id = _make_thread(cli_db)
    _add(cli_db, tmp_path, thread_id, capsys, base)
    capsys.readouterr()

    _, out = _run_impl(lambda: seeding_cli.show_impl("cfg", thread_id=thread_id), cli_db, capsys)
    assert f"Тред #{thread_id}" in out
    assert "status=draft" in out
    assert "первая реплика" in out


def test_show_impl_unknown_thread(cli_db, capsys):
    _seed_chat(cli_db)

    with pytest.raises(typer.Exit):
        _run_impl(lambda: seeding_cli.show_impl("cfg", thread_id=999), cli_db, capsys)
    assert "не найден" in capsys.readouterr().err
