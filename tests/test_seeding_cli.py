"""CLI-тесты композера посевных диалогов (#1756): stats/validate/gating.

``stats``/``validate`` работают на реальной SQLite (cli_db) с засеянными
сообщениями; команды записи черновиков проверяются как gating-стабы до
мержа #1755 (громкий отказ, exit 1). Живой LLM и живые аккаунты не
участвуют: сценарий — фикстура-файл, композер по контракту — внешний
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
from tg_agent.models import Message

CHAT_ID = -100999


def _reopen(db):
    """Переоткрыть закрытый fixture-инстанс для инспекции (паттерн handoff-тестов)."""
    if db._connection.db is None:
        asyncio.run(db.initialize())


def _seed_chat(db, count: int = 4) -> None:
    """Свежие сообщения одного чата с минутными паузами (окно сбора — сейчас)."""
    base = datetime.now(timezone.utc).replace(tzinfo=None, second=0, microsecond=0) - timedelta(hours=2)
    _reopen(db)
    for i in range(count):
        msg = Message(
            channel_id=CHAT_ID,
            message_id=i + 1,
            date=base + timedelta(minutes=i),
            text=f"тестовое сообщение номер {i + 1}",
            sender_id=(i % 2) + 1,
            sender_name="Аня" if i % 2 == 0 else "Борис",
        )
        asyncio.run(db.repos.messages.insert_message(msg))


def _run_impl(coro_factory, cli_db, capsys):
    _reopen(cli_db)
    config = AppConfig()

    async def fake_init_db(_):
        return config, cli_db

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(seeding_cli.runtime, "init_db", fake_init_db)
        return seeding_cli.run_async(coro_factory()), capsys.readouterr().out


def _write_scenario(tmp_path: Path, scenario: dict) -> str:
    path = tmp_path / "scenario.json"
    path.write_text(json.dumps(scenario, ensure_ascii=False), encoding="utf-8")
    return str(path)


def _scenario_dict(chat_id: int = CHAT_ID, delays: list[int] | None = None) -> dict:
    delays = delays or [120, 180]
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


def test_stats_impl_returns_profile(cli_db, capsys):
    _seed_chat(cli_db)

    stats, _ = _run_impl(lambda: seeding_cli.stats_impl("cfg", chat=CHAT_ID, days=14), cli_db, capsys)

    assert stats.message_count == 4
    assert stats.chat_channel_id == CHAT_ID
    assert stats.sender_count == 2
    assert stats.min_gap_seconds >= 5


def test_stats_command_prints_json(cli_db, capsys):
    _seed_chat(cli_db)

    _run_impl(lambda: seeding_cli.stats_impl("cfg", chat=CHAT_ID, days=14), cli_db, capsys)
    stats = json.loads(capsys.readouterr().out)
    assert stats["message_count"] == 4
    assert stats["gaps_seconds"]["max"] > 0
    assert isinstance(stats["quiet_hours"], list)


def test_validate_impl_valid_scenario(cli_db, capsys, tmp_path):
    _seed_chat(cli_db)
    path = _write_scenario(tmp_path, _scenario_dict(delays=[120, 300]))

    violations, out = _run_impl(
        lambda: seeding_cli.validate_impl("cfg", scenario_path=path, chat=CHAT_ID, days=14, start_at=None),
        cli_db,
        capsys,
    )
    assert violations == []
    assert out == ""


def test_validate_impl_invalid_scenario(cli_db, capsys, tmp_path):
    _seed_chat(cli_db)
    path = _write_scenario(tmp_path, _scenario_dict(delays=[2, 300]))

    with pytest.raises(typer.Exit):
        _run_impl(
            lambda: seeding_cli.validate_impl("cfg", scenario_path=path, chat=CHAT_ID, days=14, start_at=None),
            cli_db,
            capsys,
        )
    out = capsys.readouterr().out
    assert "Scenario INVALID" in out
    assert "машинно" in out


def test_validate_impl_schema_error_exits(cli_db, capsys, tmp_path):
    _seed_chat(cli_db)
    path = _write_scenario(tmp_path, {"chat_channel_id": CHAT_ID, "personas": [], "messages": "not-a-list"})

    with pytest.raises(typer.Exit):
        _run_impl(
            lambda: seeding_cli.validate_impl("cfg", scenario_path=path, chat=CHAT_ID, days=14, start_at=None),
            cli_db,
            capsys,
        )
    assert "Схема сценария" in capsys.readouterr().out


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
    assert "не найден" in capsys.readouterr().out


def test_add_impl_gated_on_storage(cli_db, capsys, tmp_path):
    """Валидный сценарий доходит до записи — и громко отказывает до #1755."""
    _seed_chat(cli_db)
    path = _write_scenario(tmp_path, _scenario_dict(delays=[120, 300]))

    with pytest.raises(typer.Exit):
        _run_impl(
            lambda: seeding_cli.add_impl(
                "cfg", scenario_path=path, thread=1, days=14, start_at=datetime(2026, 11, 2, 10, 0)
            ),
            cli_db,
            capsys,
        )
    assert "#1755" in capsys.readouterr().out


def test_add_impl_rejects_invalid_before_storage(cli_db, capsys, tmp_path):
    _seed_chat(cli_db)
    path = _write_scenario(tmp_path, _scenario_dict(delays=[2, 300]))

    with pytest.raises(typer.Exit):
        _run_impl(
            lambda: seeding_cli.add_impl(
                "cfg", scenario_path=path, thread=1, days=14, start_at=datetime(2026, 11, 2, 10, 0)
            ),
            cli_db,
            capsys,
        )
    out = capsys.readouterr().out
    assert "Scenario INVALID" in out
    assert "#1755" not in out  # gating-ошибки нет — валидация раньше записи


def test_thread_create_impl_gated(cli_db, capsys, tmp_path):
    personas_path = tmp_path / "personas.json"
    personas_path.write_text(
        json.dumps([{"name": "Аня", "account_phone": "+79990000001"}], ensure_ascii=False),
        encoding="utf-8",
    )
    _reopen(cli_db)

    with pytest.raises(typer.Exit):
        seeding_cli._load_personas(str(personas_path))
        seeding_cli._require_seeding_storage()
    assert "#1755" in capsys.readouterr().out


def test_thread_create_personas_schema_error(cli_db, capsys, tmp_path):
    personas_path = tmp_path / "personas.json"
    personas_path.write_text(json.dumps([{"name": "Аня"}]), encoding="utf-8")  # нет account_phone
    _reopen(cli_db)

    with pytest.raises(typer.Exit):
        seeding_cli._load_personas(str(personas_path))
    assert "Схема сценария" in capsys.readouterr().out


def test_thread_create_personas_duplicate_phone(cli_db, capsys, tmp_path):
    personas_path = tmp_path / "personas.json"
    personas_path.write_text(
        json.dumps(
            [
                {"name": "Аня", "account_phone": "+79990000001"},
                {"name": "Борис", "account_phone": "+79990000001"},
            ],
            ensure_ascii=False,
        ),
        encoding="utf-8",
    )
    _reopen(cli_db)

    with pytest.raises(typer.Exit):
        seeding_cli._load_personas(str(personas_path))
    assert "нескольким персонам" in capsys.readouterr().out


def test_list_and_show_gated(cli_db, capsys):
    _reopen(cli_db)
    with pytest.raises(typer.Exit):
        seeding_cli._require_seeding_storage()
    assert "#1755" in capsys.readouterr().out
