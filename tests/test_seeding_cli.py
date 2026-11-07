"""CLI-тесты посевных диалогов: композер (#1756: stats/validate/черновики) и
публикатор (#1757: approve/schedule/cancel/status).

Оба работают на реальной SQLite (cli_db) с засеянными сообщениями; жизненный
цикл статусов #1755 тестируется в репозиторных тестах, механика SEED_DUE-рана
— в test_seeding_publisher.py. Живой LLM и живые аккаунты не участвуют:
сценарий — фикстура-файл, отправка — за пределами этих тестов.
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
from tg_agent.models import (
    Channel,
    Message,
    SeedingMessage,
    SeedingThread,
    SeedingThreadStatus,
)

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


# --------------------------------------------------------------------------- #
# samples (#1793) + id чата в любой форме
# --------------------------------------------------------------------------- #

BARE_CHAT_ID = 1234567890
MARKED_CHAT_ID = -1001234567890


def _seed_conversation(db) -> None:
    """Чат под голым id (как пишет коллектор): разговор трёх + поздний обмен двоих."""
    base = datetime.now(timezone.utc).replace(tzinfo=None, second=0, microsecond=0) - timedelta(hours=5)
    _reopen(db)
    asyncio.run(
        db.repos.channels.add_channel(
            Channel(channel_id=BARE_CHAT_ID, title="Живой чат", channel_type="supergroup")
        )
    )
    rows = [
        (0, "Аня", "кто-нибудь пробовал агента в песочнице?", None),
        (70, "Боря", "я в докере гоняю", None),
        (130, "Вика", "а я на основной машине, живу опасно", None),
        (200, "Боря", None, "photo"),
        (260, "Аня", "ну это смело\nочень смело", None),
        (330, "Вика", "раз в год и палка стреляет", None),
        (400, "Боря", "вово", None),
        (7200, "Аня", "кстати", None),
        (7260, "Боря", "ага", None),
    ]
    for i, (second, sender, text, media) in enumerate(rows):
        asyncio.run(
            db.repos.messages.insert_message(
                Message(
                    channel_id=BARE_CHAT_ID,
                    message_id=i + 1,
                    date=base + timedelta(seconds=second),
                    text=text,
                    media_type=media,
                    sender_id=100 + i,
                    sender_name=sender,
                    sender_kind="user",
                    message_kind="regular",
                )
            )
        )


def test_samples_text_with_marked_id_finds_bare_stored_chat(cli_db, capsys):
    _seed_conversation(cli_db)

    samples, out = _run_impl(
        lambda: seeding_cli.samples_impl("cfg", chat=MARKED_CHAT_ID, days=1), cli_db, capsys
    )

    assert samples.qualifying_windows == 1
    assert samples.windows[0].speakers == ["Аня", "Боря", "Вика"]
    assert "=== window 1/1" in out
    assert "Боря: [photo]" in out
    assert "ну это смело" in out and "очень смело" in out
    assert "=== voices" in out
    assert "--- Боря (3 messages) ---" in out


def test_samples_json_output(cli_db, capsys):
    _seed_conversation(cli_db)

    _, out = _run_impl(
        lambda: seeding_cli.samples_impl(
            "cfg",
            chat=MARKED_CHAT_ID,
            days=1,
            per_sender=1,
            output_format=seeding_cli.SamplesFormat.json,
        ),
        cli_db,
        capsys,
    )

    payload = json.loads(out)
    assert payload["chat_channel_id"] == MARKED_CHAT_ID
    assert payload["window_gap_seconds"] == 600
    window = payload["windows"][0]
    assert window["message_count"] == 7
    assert window["messages"][0]["gap_seconds"] is None
    assert window["messages"][1]["gap_seconds"] == 70
    # Аня и Боря — по 3 текста (фото Бори не текст): равенство разрешается по имени.
    assert payload["voices"][:2] == [
        {"sender": "Аня", "message_count": 3, "samples": ["кстати"]},
        {"sender": "Боря", "message_count": 3, "samples": ["ага"]},
    ]


def test_samples_empty_chat_is_not_an_error(cli_db, capsys):
    samples, out = _run_impl(
        lambda: seeding_cli.samples_impl("cfg", chat=MARKED_CHAT_ID, days=1), cli_db, capsys
    )

    assert samples.windows == [] and samples.voices == []
    assert "No conversation windows matched" in out


def test_stats_with_marked_id_finds_bare_stored_chat(cli_db, capsys):
    """Регрессия: ``-100<bare>`` молча давал нулевой профиль (хранилище — голый id)."""
    _seed_conversation(cli_db)

    stats, _ = _run_impl(
        lambda: seeding_cli.stats_impl("cfg", chat=MARKED_CHAT_ID, days=1), cli_db, capsys
    )

    assert stats.message_count == 8
    assert stats.sender_count == 3


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


def _add(cli_db, tmp_path, thread_id, capsys, base, start=None, **kwargs):
    path = _scenario_file(tmp_path, **kwargs)
    return _run_impl(
        lambda: seeding_cli.add_impl(
            "cfg",
            scenario_path=path,
            thread_id=thread_id,
            days=14,
            start_at=start if start is not None else _start_at(base),
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


# --------------------------------------------------------------------------- #
# publisher (#1757): approve / schedule / cancel / status
# --------------------------------------------------------------------------- #


def _seed_round_clock_chat(db) -> datetime:
    """Чат с активностью в каждом UTC-часе (по 3 сообщения с паузами 5/6 мин):
    quiet_hours=[] и p10 пауз ~300s — тихий рельс не мешает schedule-тестам,
    а валидация композера на add пропускает задержки сценария 400/600s."""
    _reopen(db)
    base = datetime.now(timezone.utc).replace(tzinfo=None, minute=0, second=0, microsecond=0) - timedelta(hours=48)
    asyncio.run(
        db.repos.channels.add_channel(
            Channel(channel_id=CHAT_ID, title="Круглосуточный чат", channel_type="supergroup")
        )
    )
    message_id = 1
    for i in range(24):
        for minute in (0, 5, 11):
            msg = Message(
                channel_id=CHAT_ID,
                message_id=message_id,
                date=base + timedelta(hours=i, minutes=minute),
                text=f"сообщение номер {message_id} в час {i}",
                sender_id=(message_id % 2) + 1,
                sender_name="Аня" if message_id % 2 else "Борис",
            )
            asyncio.run(db.repos.messages.insert_message(msg))
            message_id += 1
    return base


def _future_start(minutes: int = 5) -> datetime:
    """Будущий старт посева: past-рельс публикатора отклоняет просроченные планы."""
    return datetime.now(timezone.utc) + timedelta(minutes=minutes)


def test_approve_impl_flows_draft_to_scheduled(cli_db, capsys, tmp_path):
    base = _seed_chat(cli_db)
    thread_id = _make_thread(cli_db)
    _add(cli_db, tmp_path, thread_id, capsys, base)
    capsys.readouterr()

    approved, out = _run_impl(
        lambda: seeding_cli.approve_impl("cfg", thread_id=thread_id), cli_db, capsys
    )

    assert approved == 2
    assert "апрувнут" in out and "scheduled" in out
    _reopen(cli_db)
    thread = asyncio.run(cli_db.repos.seeding.get_thread(thread_id))
    assert thread.status is SeedingThreadStatus.SCHEDULED
    rows = asyncio.run(cli_db.repos.seeding.list_messages_for_thread(thread_id))
    assert all(r.status.value == "approved" for r in rows)


def test_approve_impl_refuses_non_draft(cli_db, capsys, tmp_path):
    base = _seed_chat(cli_db)
    thread_id = _make_thread(cli_db)
    _add(cli_db, tmp_path, thread_id, capsys, base)
    capsys.readouterr()
    _run_impl(lambda: seeding_cli.approve_impl("cfg", thread_id=thread_id), cli_db, capsys)
    capsys.readouterr()

    with pytest.raises(typer.Exit):
        _run_impl(lambda: seeding_cli.approve_impl("cfg", thread_id=thread_id), cli_db, capsys)
    assert "только к draft" in capsys.readouterr().err


def test_approve_impl_unknown_thread(cli_db, capsys):
    _seed_chat(cli_db)

    with pytest.raises(typer.Exit):
        _run_impl(lambda: seeding_cli.approve_impl("cfg", thread_id=999), cli_db, capsys)
    assert "не найден" in capsys.readouterr().err


def test_approve_impl_empty_thread(cli_db, capsys):
    _seed_chat(cli_db)
    thread_id = _make_thread(cli_db)

    with pytest.raises(typer.Exit):
        _run_impl(lambda: seeding_cli.approve_impl("cfg", thread_id=thread_id), cli_db, capsys)
    assert "нечего утверждать" in capsys.readouterr().err


def _approve(cli_db, tmp_path, thread_id, capsys, base, start=None):
    _add(cli_db, tmp_path, thread_id, capsys, base, start=start)
    capsys.readouterr()
    _run_impl(lambda: seeding_cli.approve_impl("cfg", thread_id=thread_id), cli_db, capsys)
    capsys.readouterr()


def test_schedule_impl_schedules_and_enqueues_seed_due(cli_db, capsys, tmp_path):
    # Круглосуточный профиль + будущий старт: рельсы тишины и past-рельс не мешают.
    base = _seed_round_clock_chat(cli_db)
    thread_id = _make_thread(cli_db)
    _approve(cli_db, tmp_path, thread_id, capsys, base, start=_future_start())

    scheduled, out = _run_impl(
        lambda: seeding_cli.schedule_impl("cfg", thread_id=thread_id, days=14), cli_db, capsys
    )

    assert scheduled == 2
    assert "SEED_DUE" in out
    _reopen(cli_db)
    rows = asyncio.run(cli_db.repos.seeding.list_messages_for_thread(thread_id))
    assert all(r.status.value == "scheduled" for r in rows)

    async def _count_seed_due() -> int:
        cur = await cli_db.db.execute(
            "SELECT COUNT(*) AS n FROM collection_tasks WHERE task_type = 'seed_due'"
        )
        return (await cur.fetchone())["n"]

    assert asyncio.run(_count_seed_due()) == 1


def test_schedule_impl_rejects_link_text(cli_db, capsys, tmp_path):
    """Рельсы на этапе schedule: ссылка отклоняет постановку, не надеемся на композер."""
    _seed_round_clock_chat(cli_db)
    thread_id = _make_thread(cli_db)
    _reopen(cli_db)
    asyncio.run(
        cli_db.repos.seeding.create_messages(
            thread_id,
            [
                SeedingMessage(
                    thread_id=thread_id,
                    position=1,
                    account_phone="+79990000001",
                    persona_name="Аня",
                    text="гляди https://example.com",
                    planned_at=_future_start(),
                ),
            ],
        )
    )
    asyncio.run(cli_db.repos.seeding.approve_thread(thread_id))

    with pytest.raises(typer.Exit):
        _run_impl(
            lambda: seeding_cli.schedule_impl("cfg", thread_id=thread_id, days=14), cli_db, capsys
        )
    err = capsys.readouterr().err
    assert "Scenario INVALID" in err and "ссылки" in err
    _reopen(cli_db)
    rows = asyncio.run(cli_db.repos.seeding.list_messages_for_thread(thread_id))
    assert rows[0].status.value == "approved"  # постановка не случилась


def test_schedule_impl_rejects_draft_thread(cli_db, capsys, tmp_path):
    base = _seed_chat(cli_db)
    thread_id = _make_thread(cli_db)
    _add(cli_db, tmp_path, thread_id, capsys, base)
    capsys.readouterr()

    with pytest.raises(typer.Exit):
        _run_impl(
            lambda: seeding_cli.schedule_impl("cfg", thread_id=thread_id, days=14), cli_db, capsys
        )
    assert "после approve" in capsys.readouterr().err


def test_schedule_impl_rejects_cross_account_gap(cli_db, capsys, tmp_path):
    """Зазор 10s между аккаунтами < 20s — отклонение."""
    _seed_round_clock_chat(cli_db)
    thread_id = _make_thread(cli_db)
    _reopen(cli_db)
    start = _future_start()
    asyncio.run(
        cli_db.repos.seeding.create_messages(
            thread_id,
            [
                SeedingMessage(
                    thread_id=thread_id, position=1, account_phone="+79990000001",
                    persona_name="Аня", text="первая реплика", planned_at=start,
                ),
                SeedingMessage(
                    thread_id=thread_id, position=2, account_phone="+79990000002",
                    persona_name="Боря", text="вторая реплика",
                    planned_at=start + timedelta(seconds=10),
                ),
            ],
        )
    )
    asyncio.run(cli_db.repos.seeding.approve_thread(thread_id))

    with pytest.raises(typer.Exit):
        _run_impl(
            lambda: seeding_cli.schedule_impl("cfg", thread_id=thread_id, days=14), cli_db, capsys
        )
    assert "зазор" in capsys.readouterr().err


def test_cancel_impl_cancels_unsent(cli_db, capsys, tmp_path):
    base = _seed_chat(cli_db)
    thread_id = _make_thread(cli_db)
    _approve(cli_db, tmp_path, thread_id, capsys, base)

    cancelled, out = _run_impl(
        lambda: seeding_cli.cancel_impl("cfg", thread_id=thread_id), cli_db, capsys
    )

    assert cancelled == 2
    assert "отменён" in out
    _reopen(cli_db)
    thread = asyncio.run(cli_db.repos.seeding.get_thread(thread_id))
    assert thread.status is SeedingThreadStatus.CANCELLED
    rows = asyncio.run(cli_db.repos.seeding.list_messages_for_thread(thread_id))
    assert all(r.status.value == "cancelled" for r in rows)


def test_cancel_impl_refuses_terminal(cli_db, capsys, tmp_path):
    base = _seed_chat(cli_db)
    thread_id = _make_thread(cli_db)
    _approve(cli_db, tmp_path, thread_id, capsys, base)
    _run_impl(lambda: seeding_cli.cancel_impl("cfg", thread_id=thread_id), cli_db, capsys)
    capsys.readouterr()

    with pytest.raises(typer.Exit):
        _run_impl(lambda: seeding_cli.cancel_impl("cfg", thread_id=thread_id), cli_db, capsys)
    assert "терминальный" in capsys.readouterr().err


def test_cancel_impl_unknown_thread(cli_db, capsys):
    _seed_chat(cli_db)

    with pytest.raises(typer.Exit):
        _run_impl(lambda: seeding_cli.cancel_impl("cfg", thread_id=999), cli_db, capsys)
    assert "не найден" in capsys.readouterr().err


def test_status_impl_prints_summary(cli_db, capsys, tmp_path):
    base = _seed_round_clock_chat(cli_db)
    thread_id = _make_thread(cli_db)
    _approve(cli_db, tmp_path, thread_id, capsys, base, start=_future_start())
    _run_impl(
        lambda: seeding_cli.schedule_impl("cfg", thread_id=thread_id, days=14), cli_db, capsys
    )
    capsys.readouterr()

    _, out = _run_impl(
        lambda: seeding_cli.status_impl("cfg", thread_id=thread_id), cli_db, capsys
    )

    assert f"Тред #{thread_id}" in out
    assert "scheduled: 2" in out
    assert "SEED_DUE-задача: активна" in out
    assert "0/2 sent" in out


def test_status_impl_unknown_thread(cli_db, capsys):
    _seed_chat(cli_db)

    with pytest.raises(typer.Exit):
        _run_impl(lambda: seeding_cli.status_impl("cfg", thread_id=999), cli_db, capsys)
    assert "не найден" in capsys.readouterr().err
