"""Юнит-тесты композера посевных диалогов: статистика + валидация (#1756).

Чистые функции без БД/LLM/транспорта: фиксированные фикстуры сообщений,
детерминированные распределения, каждый класс нарушения валидации.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from tg_agent.models import Message
from tg_agent.services.seeding_composer import (
    ChatStatistics,
    SeedingPersona,
    SeedingScenario,
    SeedingScenarioMessage,
    compute_chat_statistics,
    derive_planned_schedule,
    validate_scenario,
)

CHAT_ID = -100999


def _naive(hour: int, minute: int = 0) -> datetime:
    """Naive-UTC (формат сбора) через fromisoformat — обход DTZ001 по конвенции репо."""
    return datetime.fromisoformat(f"2026-10-20T{hour:02d}:{minute:02d}")


def _msg(text: str, *, hour: int, minute: int = 0, sender_id: int = 1, sender_name: str = "Аня") -> Message:
    return Message(
        channel_id=CHAT_ID,
        message_id=hour * 100 + minute + len(sender_name),
        date=_naive(hour, minute),
        text=text,
        sender_id=sender_id,
        sender_name=sender_name,
    )


@pytest.fixture
def chat_messages() -> list[Message]:
    """День чата: два собеседника, паузы 300/360/3000/8940/23400s, часы 10..20."""
    return [
        _msg("привет, как дела?", hour=10, minute=0),
        _msg("норм, ты как?", hour=10, minute=5, sender_id=2, sender_name="Борис"),
        _msg("делаю релиз, вечерами свободен 🚀", hour=10, minute=11),
        _msg("ок, скинь потом статус", hour=11, minute=1, sender_id=2, sender_name="Борис"),
        _msg("какой смысл в этом флаге вообще?", hour=13, minute=30),
        _msg("понял, спасибо", hour=20, minute=0),
    ]


@pytest.fixture
def chat_stats(chat_messages: list[Message]) -> ChatStatistics:
    return compute_chat_statistics(chat_messages, chat_channel_id=CHAT_ID, window_days=14)


def test_stats_counts(chat_stats: ChatStatistics) -> None:
    assert chat_stats.message_count == 6
    assert chat_stats.sender_count == 2
    assert chat_stats.window_days == 14
    assert chat_stats.chat_channel_id == CHAT_ID


def test_stats_length_distribution() -> None:
    # ASCII-строки заведомых длин: перцентили считаются без ручного подсчёта кириллицы.
    msgs = [
        Message(
            channel_id=CHAT_ID,
            message_id=i + 1,
            date=_naive(10, i * 3),
            text=text,
        )
        for i, text in enumerate(["aaaa", "aaaaaaa", "aaaaaaaaaa"])
    ]
    stats = compute_chat_statistics(msgs, chat_channel_id=CHAT_ID, window_days=7)
    assert stats.length.p10 == pytest.approx(4.6)
    assert stats.length.p25 == pytest.approx(5.5)
    assert stats.length.p50 == pytest.approx(7.0)
    assert stats.length.mean == pytest.approx(7.0)


def test_stats_length_matches_fixture(chat_messages: list[Message], chat_stats: ChatStatistics) -> None:
    lens = sorted(len(m.text or "") for m in chat_messages)
    assert chat_stats.length.mean == pytest.approx(sum(lens) / len(lens))
    assert chat_stats.length.p50 == pytest.approx(lens[3] * 0.5 + lens[2] * 0.5)


def test_stats_gaps(chat_stats: ChatStatistics) -> None:
    # паузы: 10:00→10:05→10:11→11:01→13:30→20:00
    assert chat_stats.gaps_seconds.max == pytest.approx(23400.0)
    assert chat_stats.gaps_seconds.p10 == pytest.approx(324.0)  # 300 + 0.4 * 60
    assert chat_stats.min_gap_seconds == 324
    assert chat_stats.max_gap_seconds == 23400


def test_stats_hours_and_quiet(chat_stats: ChatStatistics) -> None:
    assert set(chat_stats.active_hour_counts) == {10, 11, 13, 20}
    assert 0 in chat_stats.quiet_hours and 9 in chat_stats.quiet_hours
    assert 10 not in chat_stats.quiet_hours


def test_stats_style_and_emoji(chat_stats: ChatStatistics) -> None:
    assert chat_stats.emoji_share == pytest.approx(1 / 6)
    assert chat_stats.top_emojis[0].emoji == "🚀"
    assert chat_stats.style.question_share == pytest.approx(3 / 6)
    assert chat_stats.style.link_share == 0.0
    assert chat_stats.style.top_tokens  # «статус», «смысл», «флаг»…


def test_stats_per_sender(chat_stats: ChatStatistics) -> None:
    top = chat_stats.per_sender[0]
    assert top.sender == "Аня"
    assert top.count == 4
    assert top.share == pytest.approx(4 / 6)


def test_stats_ignores_empty_and_service_messages() -> None:
    base = _naive(10)
    msgs = [
        Message(channel_id=CHAT_ID, message_id=1, date=base, text="текст один"),
        Message(channel_id=CHAT_ID, message_id=2, date=base + timedelta(minutes=2), text=None),
        Message(channel_id=CHAT_ID, message_id=3, date=base + timedelta(minutes=4), text="   "),
    ]
    stats = compute_chat_statistics(msgs, chat_channel_id=CHAT_ID, window_days=7)
    assert stats.message_count == 1
    assert stats.gaps_seconds.p50 == 0.0  # пауз нет
    assert stats.max_gap_seconds is None
    assert stats.quiet_hours == [h for h in range(24) if h != 10]


def _scenario(
    delays: list[int],
    *,
    texts: list[str],
    personas: list[str] | None = None,
    reply_to: dict[int, int] | None = None,
    positions: list[int] | None = None,
) -> SeedingScenario:
    personas = personas or ["Аня", "Борис"]
    positions = positions or list(range(1, len(delays) + 1))
    persona_names = [personas[i % len(personas)] for i in range(len(delays))]
    return SeedingScenario(
        chat_channel_id=CHAT_ID,
        personas=[
            SeedingPersona(name="Аня", account_phone="+79990000001"),
            SeedingPersona(name="Борис", account_phone="+79990000002"),
        ],
        messages=[
            SeedingScenarioMessage(
                position=positions[i],
                persona=persona_names[i],
                text=texts[i],
                delay_seconds=delays[i],
                reply_to_position=(reply_to or {}).get(positions[i]),
            )
            for i in range(len(delays))
        ],
    )


def _valid_delays(stats: ChatStatistics, n: int) -> list[int]:
    low = stats.min_gap_seconds + 10
    high = (stats.max_gap_seconds or low) - 10
    step = max(1, (high - low) // max(n, 1))
    return [low + step * i for i in range(n)]


def test_validate_scenario_valid(chat_stats: ChatStatistics) -> None:
    scenario = _scenario(
        _valid_delays(chat_stats, 3),
        texts=["привет, как ты", "да нормально всё", "погнали дальше, поехали"],
        reply_to={2: 1},
    )
    assert validate_scenario(scenario, chat_stats) == []


def test_validate_scenario_valid_with_quiet_hours(chat_stats: ChatStatistics) -> None:
    scenario = _scenario(
        [400, 500, 600, 700, 800],
        texts=[
            "раз",
            "два слова",
            "три слова тут",
            "четыре слова в этой реплике",
            "пять слов в этой вот реплике",
        ],
        personas=["Аня"],
        reply_to={2: 1, 3: 2, 4: 3, 5: 4},
    )
    start = datetime(2026, 10, 20, 10, 0, tzinfo=timezone.utc)
    assert validate_scenario(scenario, chat_stats, start_at=start) == []


def test_validate_no_links(chat_stats: ChatStatistics) -> None:
    scenario = _scenario(_valid_delays(chat_stats, 1), texts=["гляди https://example.com/post"], personas=["Аня"])
    assert any("ссылки" in v for v in validate_scenario(scenario, chat_stats))


def test_validate_echo_repeat(chat_stats: ChatStatistics) -> None:
    scenario = _scenario(
        _valid_delays(chat_stats, 2), texts=["одинаковый текст", "Одинаковый текст"], personas=["Аня"]
    )
    assert any("эхо-повтор" in v for v in validate_scenario(scenario, chat_stats))


def test_validate_identical_length_run(chat_stats: ChatStatistics) -> None:
    scenario = _scenario(
        _valid_delays(chat_stats, 3), texts=["abcde", "fghij", "klmno"], personas=["Аня"]
    )
    assert any("одинаковой длины" in v for v in validate_scenario(scenario, chat_stats))


def test_validate_machine_delay(chat_stats: ChatStatistics) -> None:
    scenario = _scenario([3], texts=["быстро"], personas=["Аня"])
    assert any("машинно" in v for v in validate_scenario(scenario, chat_stats))


def test_validate_gap_above_learned_max(chat_stats: ChatStatistics) -> None:
    assert chat_stats.max_gap_seconds is not None
    scenario = _scenario(
        [chat_stats.max_gap_seconds + 1], texts=["долго молчал"], personas=["Аня"]
    )
    assert any("наблюдавшейся паузы" in v for v in validate_scenario(scenario, chat_stats))


def test_validate_cross_persona_rapid(chat_stats: ChatStatistics) -> None:
    scenario = _scenario([400, 3], texts=["момент один", "момент два"], personas=["Аня", "Борис"])
    violations = validate_scenario(scenario, chat_stats)
    assert any("признак координации" in v for v in violations)


def test_validate_unknown_persona(chat_stats: ChatStatistics) -> None:
    scenario = _scenario(_valid_delays(chat_stats, 1), texts=["текст"], personas=["Гость"])
    assert any("не объявлена в personas" in v for v in validate_scenario(scenario, chat_stats))


def test_validate_reply_forward_reference(chat_stats: ChatStatistics) -> None:
    scenario = _scenario(
        _valid_delays(chat_stats, 2), texts=["вопрос", "ответ"], reply_to={1: 2}
    )
    assert any("reply_to_position=2" in v for v in validate_scenario(scenario, chat_stats))


def test_validate_positions_and_personas(chat_stats: ChatStatistics) -> None:
    scenario = _scenario(
        _valid_delays(chat_stats, 3),
        texts=["один", "два", "три"],
        positions=[1, 1, 2],
        personas=["Аня", "Аня"],
    )
    scenario = scenario.model_copy(
        update={
            "personas": [
                SeedingPersona(name="Аня", account_phone="+79990000001"),
                SeedingPersona(name="Аня", account_phone="+79990000001"),
            ]
        }
    )
    violations = validate_scenario(scenario, chat_stats)
    assert any("позиции должны быть 1..N" in v for v in violations)
    assert any("дублирующиеся имена" in v for v in violations)
    assert any("нескольким персонам" in v for v in violations)


def test_validate_empty_scenario(chat_stats: ChatStatistics) -> None:
    scenario = SeedingScenario(chat_channel_id=CHAT_ID, personas=[], messages=[])
    violations = validate_scenario(scenario, chat_stats)
    assert any("personas: пусто" in v for v in violations)
    assert any("messages: пусто" in v for v in violations)


def test_validate_text_limits(chat_stats: ChatStatistics) -> None:
    long_text = "д" * 4097
    scenario = _scenario(_valid_delays(chat_stats, 1), texts=[long_text], personas=["Аня"])
    assert any("лимита Telegram" in v for v in validate_scenario(scenario, chat_stats))


def test_validate_quiet_hour_landing(chat_stats: ChatStatistics) -> None:
    assert chat_stats.max_gap_seconds is not None
    scenario = _scenario(
        [chat_stats.max_gap_seconds // 2], texts=["ночная реплика"], personas=["Аня"]
    )
    start = datetime(2026, 10, 20, 11, 0, tzinfo=timezone.utc)
    assert any("тихие часы" in v for v in validate_scenario(scenario, chat_stats, start_at=start))


def test_validate_naive_start_at_treated_as_utc(chat_stats: ChatStatistics) -> None:
    scenario = _scenario([400], texts=["реплика"], personas=["Аня"])
    aware = datetime(2026, 10, 20, 10, 0, tzinfo=timezone.utc)
    naive = _naive(10)
    assert validate_scenario(scenario, chat_stats, start_at=naive) == validate_scenario(
        scenario, chat_stats, start_at=aware
    )


def test_derive_planned_schedule() -> None:
    scenario = _scenario([100, 200, 300], texts=["а-а-а", "б-б-б", "в-в-в"], personas=["Аня"])
    start = datetime(2026, 11, 2, 12, 0, tzinfo=timezone.utc)
    planned = derive_planned_schedule(scenario, start)
    assert planned == [
        start + timedelta(seconds=100),
        start + timedelta(seconds=300),
        start + timedelta(seconds=600),
    ]
