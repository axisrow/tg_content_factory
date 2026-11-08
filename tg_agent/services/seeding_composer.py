"""Композер посевных диалогов: статистика реального чата + валидация сценария (#1756).

Детерминированная (без LLM) половина композера из эпика #1751 (chat seeding):

- ``compute_chat_statistics`` — профиль реального чата из собранных сообщений:
  распределение длин, межсообщенные паузы, активные/тихие часы, эмодзи- и
  стилевые привычки, пер-сендерная статистика. Это вход для промпта композера
  (LLM = Claude Code CLI, workflow skills/tg-agent/seeding.md) и база
  валидации таймингов.
- ``validate_scenario`` — структурная и поведенческая валидация сценария
  перед записью черновиков: позиции/персоны/reply-цепочки + анти-паттерны
  эпика (ссылки, эхо-повторы, одинаковые длины подряд, машинные паузы,
  ночная тишина).
- ``derive_planned_schedule`` — ``planned_at`` каждой реплики из кумулятива
  задержек (черновое расписание видно сразу).
- ``build_chat_samples`` — реальная речь чата для композера (#1793): окна
  разговоров (смежные сообщения с паузой ≤ порога) дословно + «голоса» топ-
  участников (их последние сообщения). Агрегаты ``stats`` — измерения, а не
  речь; сленг, регистр и манера ответов живут только в самих сообщениях.
- ``load_chat_messages`` — общий ход чтения сообщений чата (stats/validate/
  add/samples и rails публикатора); принимает id чата в обеих формах.
- ``foreign_words`` — слова сценария, которых участники чата не писали
  (подсказка ``validate``: в слепом тесте #1793 посевы выдали именно чужие слова).

Запись черновиков (``seeding_threads``/``seeding_messages`` + переходы
статусов) — контракт параллельного #1755; CLI-команды записи включаются
после его мержа, схема сценария уже следует его колонкам.
"""

from __future__ import annotations

import bisect
import math
import re
from collections import Counter
from datetime import datetime, timedelta, timezone
from statistics import fmean
from typing import TYPE_CHECKING, Any

from pydantic import BaseModel

from tg_agent.models import Message
from tg_agent.parsers import bare_channel_id

if TYPE_CHECKING:
    from tg_agent.database.facade import Database

# Те же диапазоны, что в channel_analysis_service._EMOJI_RE (порт ai_detect_tool #781).
_EMOJI_RE = re.compile(
    "[\U0001f600-\U0001f64f\U0001f300-\U0001f5ff\U0001f680-\U0001f6ff"
    "\U0001f1e0-\U0001f1ff\U00002702-\U000027b0\U0001f900-\U0001f9ff"
    "\U0001fa00-\U0001fa6f\U0001fa70-\U0001faff\U00002600-\U000026ff]"
)

#: Ссылки в посевах запрещены эпиком (#1751): spam-reportable — риск для
#: аккаунтов сбора, которые тем же пулом и сеются. Публичное имя: rails-валидатор
#: публикатора (#1757) переиспользует тот же паттерн.
LINK_RE = re.compile(r"https?://|www\.|t\.me/", re.IGNORECASE)

#: Токены для top_tokens (слова от 4 букв; латиница+кириллица).
_TOKEN_RE = re.compile(r"[а-яёa-z]{4,}", re.IGNORECASE)

#: Мини-стоп-лист для top_tokens — только шейп-шум болтовни (это подсказка
#: промпту, не лингвистика; полный список не нужен).
_STOPWORDS = frozenset({
    "это", "что", "как", "так", "уже", "тоже", "вот", "если", "когда", "чтобы",
    "потом", "сейчас", "можно", "нужно", "быть", "есть", "было", "будет",
    "them", "then", "than", "that", "this", "have", "with", "would", "could",
})

#: Анти-паттерны эпика #1751: машинный пол паузы и минимальный зазор при
#: смене аккаунта («два аккаунта не пишут подряд с малым зазором»). Публичные:
#: rails-валидатор публикатора (#1757) применяет тот же зазор к planned_at.
_MIN_GAP_FLOOR_SECONDS = 5
CROSS_PERSONA_MIN_GAP_SECONDS = 20

_MAX_TEXT_LEN = 4096  # лимит Telegram на одно сообщение
_PER_SENDER_TOP = 8
_TOP_TOKENS = 10
_TOP_EMOJIS = 5


class LengthStats(BaseModel):
    """Распределение длины сообщения (символы, по strip()-текстам)."""

    p10: float
    p25: float
    p50: float
    p75: float
    p90: float
    mean: float


class GapStats(BaseModel):
    """Распределение межсообщенных пауз в секундах (0 = пауз нет)."""

    p10: float
    p25: float
    p50: float
    p75: float
    p90: float
    max: float


class SenderStats(BaseModel):
    """Активность одного участника реального чата (база для персон)."""

    sender: str
    count: int
    share: float
    avg_len: float


class StyleStats(BaseModel):
    """Формальность/лексика: детерминированные прокси для промпта композера."""

    question_share: float
    exclamation_share: float
    lowercase_start_share: float
    link_share: float
    avg_words: float
    top_tokens: list[str]


class EmojiStat(BaseModel):
    emoji: str
    count: int


class ChatStatistics(BaseModel):
    """Выученный профиль реального чата — вход промпта и база валидации."""

    chat_channel_id: int
    window_days: int
    message_count: int
    sender_count: int
    length: LengthStats
    gaps_seconds: GapStats
    #: 0..23 -> количество сообщений; отсутствующий час = 0 (тихий).
    active_hour_counts: dict[int, int]
    quiet_hours: list[int]
    emoji_share: float
    top_emojis: list[EmojiStat]
    style: StyleStats
    per_sender: list[SenderStats]

    @property
    def min_gap_seconds(self) -> int:
        """Нижняя граница паузы: машинный пол или быстрый хвост чата (p10)."""
        return max(_MIN_GAP_FLOOR_SECONDS, math.floor(self.gaps_seconds.p10))

    @property
    def max_gap_seconds(self) -> int | None:
        """Верхняя граница паузы: самая долгая наблюдавшаяся (None — пауз нет)."""
        if self.message_count < 2:
            return None
        return math.ceil(self.gaps_seconds.max)


class SeedingPersona(BaseModel):
    """Персона треда: имя + аккаунт + зафиксированный стиль.

    Контракт эпика: персона консистентна между сессиями — словарь,
    пунктуация и окно активности фиксируются в ``style`` (свободная форма —
    это данные для промпта композера, не для серверной логики).
    """

    name: str
    account_phone: str
    style: dict[str, Any] | None = None


class SeedingScenarioMessage(BaseModel):
    """Реплика сценария: позиция, автор-персона, текст, пауза перед отправкой."""

    position: int
    persona: str
    text: str
    delay_seconds: int
    reply_to_position: int | None = None


class SeedingScenario(BaseModel):
    """Сценарий диалога, который композер (Claude Code CLI) сдаёт через CLI.

    Поля повторяют колонки ``seeding_messages`` из #1755: ``position``,
    ``persona`` → ``persona_name``, ``delay_seconds`` → ``planned_at``
    (кумулятив от ``--start-at``), ``reply_to_position``.
    """

    chat_channel_id: int
    personas: list[SeedingPersona]
    messages: list[SeedingScenarioMessage]


def _pct(sorted_values: list[float], q: float) -> float:
    """Персентиль (0..100) с линейной интерполяцией по отсортированному списку."""
    if not sorted_values:
        return 0.0
    if len(sorted_values) == 1:
        return float(sorted_values[0])
    pos = (len(sorted_values) - 1) * q / 100.0
    lo = math.floor(pos)
    hi = math.ceil(pos)
    if lo == hi:
        return float(sorted_values[lo])
    frac = pos - lo
    return sorted_values[lo] * (1.0 - frac) + sorted_values[hi] * frac


def _dist(values: list[float]) -> dict[str, float]:
    ordered = sorted(values)
    return {
        "p10": _pct(ordered, 10),
        "p25": _pct(ordered, 25),
        "p50": _pct(ordered, 50),
        "p75": _pct(ordered, 75),
        "p90": _pct(ordered, 90),
    }


def _strip_len(msg: Message) -> int:
    return len((msg.text or "").strip())


def _sender_label(msg: Message) -> str:
    if msg.sender_name:
        return msg.sender_name
    if msg.sender_id is not None:
        return str(msg.sender_id)
    return "unknown"


def compute_chat_statistics(
    messages: list[Message], *, chat_channel_id: int, window_days: int
) -> ChatStatistics:
    """Профиль реального чата по собранным сообщениям (детерминированный, без LLM).

    Пустые/служебные сообщения (без текста) не участвуют; порядок входа не
    важен — сортировка по дате внутри.
    """
    texted = [m for m in messages if m.text and m.text.strip()]
    texted.sort(key=lambda m: m.date)
    total = len(texted)
    lengths = [float(_strip_len(m)) for m in texted]
    gaps = [
        (later.date - earlier.date).total_seconds()
        for earlier, later in zip(texted, texted[1:], strict=False)
    ]
    hour_counts: Counter[int] = Counter(m.date.hour for m in texted)

    emoji_counter: Counter[str] = Counter()
    token_counter: Counter[str] = Counter()
    questions = exclamations = lowercase_starts = links = 0
    emoji_messages = 0
    word_total = 0
    for m in texted:
        text = m.text or ""
        emojis = _EMOJI_RE.findall(text)
        if emojis:
            emoji_messages += 1
            emoji_counter.update(emojis)
        if "?" in text:
            questions += 1
        if "!" in text:
            exclamations += 1
        first = text.strip()[:1]
        if first and first.isalpha() and first.islower():
            lowercase_starts += 1
        if LINK_RE.search(text):
            links += 1
        tokens = _TOKEN_RE.findall(text)
        word_total += len(tokens)
        token_counter.update(t.lower() for t in tokens if t.lower() not in _STOPWORDS)

    by_sender: dict[str, list[float]] = {}
    for m in texted:
        by_sender.setdefault(_sender_label(m), []).append(float(_strip_len(m)))
    per_sender = [
        SenderStats(sender=name, count=len(lens), share=len(lens) / total, avg_len=fmean(lens))
        for name, lens in by_sender.items()
    ]
    per_sender.sort(key=lambda s: s.count, reverse=True)

    length_dist = _dist(lengths)
    gap_dist = _dist(gaps)
    return ChatStatistics(
        chat_channel_id=chat_channel_id,
        window_days=window_days,
        message_count=total,
        sender_count=len(by_sender),
        length=LengthStats(**length_dist, mean=fmean(lengths) if lengths else 0.0),
        gaps_seconds=GapStats(**gap_dist, max=max(gaps) if gaps else 0.0),
        active_hour_counts=dict(hour_counts),
        quiet_hours=[h for h in range(24) if hour_counts.get(h, 0) == 0],
        emoji_share=emoji_messages / total if total else 0.0,
        top_emojis=[EmojiStat(emoji=e, count=c) for e, c in emoji_counter.most_common(_TOP_EMOJIS)],
        style=StyleStats(
            question_share=questions / total if total else 0.0,
            exclamation_share=exclamations / total if total else 0.0,
            lowercase_start_share=lowercase_starts / total if total else 0.0,
            link_share=links / total if total else 0.0,
            avg_words=word_total / total if total else 0.0,
            top_tokens=[t for t, _ in token_counter.most_common(_TOP_TOKENS)],
        ),
        per_sender=per_sender[:_PER_SENDER_TOP],
    )


def _as_utc(value: datetime) -> datetime:
    """Наивные datetime трактуются как UTC (вся статистика — по UTC-датам сбора)."""
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def validate_scenario(
    scenario: SeedingScenario, stats: ChatStatistics, *, start_at: datetime | None = None
) -> list[str]:
    """Валидация сценария перед записью черновиков; пустой список = валиден.

    Структура (позиции 1..N, персоны, reply-цепочки) + анти-паттерны эпика
    #1751: ссылки (spam-reportable), эхо-повторы фраз между аккаунтами,
    три+ одинаковые длины подряд, машинные паузы (вне выученного
    распределения), ночная тишина (``start_at`` включает проверку часов).
    """
    violations: list[str] = []

    if not scenario.personas:
        violations.append("personas: пусто — нужен хотя бы один участник")
    if not scenario.messages:
        violations.append("messages: пусто — сценарий без реплик")
    names = [p.name for p in scenario.personas]
    dup_names = sorted({n for n in names if names.count(n) > 1})
    if dup_names:
        violations.append(f"personas: дублирующиеся имена: {dup_names}")
    if len({p.account_phone for p in scenario.personas}) != len(scenario.personas):
        violations.append("personas: один аккаунт назначен нескольким персонам")
    persona_set = set(names)

    ordered = sorted(scenario.messages, key=lambda m: m.position)
    positions = [m.position for m in ordered]
    if positions and positions != list(range(1, len(positions) + 1)):
        violations.append(
            f"messages: позиции должны быть 1..N без пропусков/дублей, получено {positions[:20]}"
        )

    min_gap = stats.min_gap_seconds
    max_gap = stats.max_gap_seconds
    planned = _as_utc(start_at) if start_at is not None else None

    seen_texts: set[str] = set()
    prev: SeedingScenarioMessage | None = None
    run_len = 0
    run_text_len: int | None = None
    for m in ordered:
        text = m.text.strip()
        if not text:
            violations.append(f"#{m.position}: пустой текст")
        elif len(text) > _MAX_TEXT_LEN:
            violations.append(f"#{m.position}: текст длиннее лимита Telegram ({len(text)} > {_MAX_TEXT_LEN})")
        if LINK_RE.search(text):
            violations.append(f"#{m.position}: ссылки в посевах запрещены (spam-reportable) — убери URL")
        text_key = text.lower()
        if text_key in seen_texts:
            violations.append(f"#{m.position}: эхо-повтор текста «{text[:40]}» — аккаунты не повторяют фразы")
        seen_texts.add(text_key)
        if m.persona not in persona_set:
            violations.append(f"#{m.position}: persona «{m.persona}» не объявлена в personas")
        if m.reply_to_position is not None and not 1 <= m.reply_to_position < m.position:
            violations.append(
                f"#{m.position}: reply_to_position={m.reply_to_position} не ссылается на предыдущую реплику"
            )
        if m.delay_seconds < min_gap:
            violations.append(
                f"#{m.position}: delay_seconds={m.delay_seconds} машинно (< минимальной паузы чата {min_gap}s)"
            )
        elif max_gap is not None and m.delay_seconds > max_gap:
            violations.append(
                f"#{m.position}: delay_seconds={m.delay_seconds} больше максимальной наблюдавшейся паузы {max_gap}s"
            )
        if (
            prev is not None
            and m.persona != prev.persona
            and m.delay_seconds < CROSS_PERSONA_MIN_GAP_SECONDS
        ):
            violations.append(
                f"#{m.position}: смена персоны {prev.persona} → {m.persona} с зазором "
                f"{m.delay_seconds}s (< {CROSS_PERSONA_MIN_GAP_SECONDS}s) — признак координации"
            )

        cur_len = len(text)
        if run_text_len == cur_len:
            run_len += 1
        else:
            run_len = 1
            run_text_len = cur_len
        if run_len == 3:
            violations.append(f"#{m.position}: три реплики подряд одинаковой длины ({cur_len}) — машинный след")

        if planned is not None:
            planned = planned + timedelta(seconds=m.delay_seconds)
            if planned.astimezone(timezone.utc).hour in stats.quiet_hours:
                violations.append(
                    f"#{m.position}: планируемое время {planned:%H:%M} UTC попадает в тихие часы чата "
                    f"{stats.quiet_hours}"
                )
        prev = m

    return violations


def derive_planned_schedule(scenario: SeedingScenario, start_at: datetime) -> list[datetime]:
    """Черновое расписание: planned_at каждой реплики = старт + кумулятив задержек."""
    cursor = _as_utc(start_at)
    planned: list[datetime] = []
    for m in sorted(scenario.messages, key=lambda m: m.position):
        cursor = cursor + timedelta(seconds=m.delay_seconds)
        planned.append(cursor)
    return planned


# --------------------------------------------------------------------------- #
# Чтение сообщений чата (общий ход) + реальная речь для композера (#1793)
# --------------------------------------------------------------------------- #

#: Пауза, после которой разговор считается новым: окно режется.
DEFAULT_WINDOW_GAP_SECONDS = 600

#: Отправители, которые не становятся голосами персон: посты привязанного
#: канала (триггеры обсуждения) и анонимный админ (говорит от имени чата).
_NON_PERSONA_SENDER_KINDS = frozenset({"channel", "anonymous_admin"})


def chat_id_lookup_keys(chat_channel_id: int) -> list[int]:
    """Формы id чата, под которыми его сообщения могут лежать в ``messages``.

    Хранилище — голый положительный id (``bare_channel_id``), а владелец и доки
    передают телеграмный ``-100<bare>`` (как в Telegram/Telethon). Поиск только
    по переданной форме молча давал пустой профиль; ищем по обеим.
    """
    keys = [chat_channel_id]
    bare = bare_channel_id(chat_channel_id)
    if bare != chat_channel_id:
        keys.append(bare)
    return keys


async def load_chat_messages(db: Database, chat_channel_id: int, *, days: int) -> list[Message]:
    """Собранные сообщения чата за ``days`` дней (общий ход stats/validate/add/samples/rails)."""
    return await db.repos.messages.get_recent_for_channels(
        chat_id_lookup_keys(chat_channel_id), since_hours=days * 24.0
    )


class SampleMessage(BaseModel):
    """Сообщение окна дословно: автор, время, пауза от предыдущего, текст."""

    message_id: int
    date: datetime
    #: Пауза от предыдущего сообщения окна в секундах (None — первое в окне).
    gap_seconds: int | None
    sender: str
    sender_kind: str | None
    #: Текст как есть (без крайних пробелов); пусто у медиа без подписи.
    text: str
    media_type: str | None


class ConversationWindow(BaseModel):
    """Непрерывный разговор: смежные сообщения с паузой ≤ порога."""

    start: datetime
    end: datetime
    #: Сколько сообщений в окне всего (до обрезки до --window).
    message_count: int
    #: Участники в порядке первого появления (посты канала — триггеры, не участники).
    speakers: list[str]
    messages: list[SampleMessage]
    #: Сколько сообщений окна не показано (обрезка до --window).
    truncated: int


class SenderVoice(BaseModel):
    """Голос реального участника: его последние сообщения — образец для персоны."""

    sender: str
    message_count: int
    samples: list[str]


class ChatSamples(BaseModel):
    """Реальная речь чата для композера: свежие окна разговоров + голоса участников."""

    chat_channel_id: int
    window_days: int
    #: Сообщения выборки (без служебных; текст или медиа).
    message_count: int
    window_gap_seconds: int
    #: Сколько окон прошли пороги (показаны последние из них).
    qualifying_windows: int
    windows: list[ConversationWindow]
    voices: list[SenderVoice]


def _has_text(msg: Message) -> bool:
    return bool(msg.text and msg.text.strip())


def _conversation_population(messages: list[Message]) -> list[Message]:
    """Сообщения разговора: без служебных; с текстом или медиа; по времени, затем id.

    Медиа без подписи остаются: на скрины реагируют, это часть ритма чата.
    ``_as_utc`` в ключе — смесь naive (тесты/старые строки) и aware (коллектор)
    дат не падает на сравнении.
    """
    population = [
        m for m in messages if m.message_kind != "service" and (_has_text(m) or m.media_type)
    ]
    population.sort(key=lambda m: (_as_utc(m.date), m.message_id))
    return population


def split_conversation_windows(messages: list[Message], *, gap_seconds: int) -> list[list[Message]]:
    """Разрезать упорядоченные сообщения на окна: пауза > ``gap_seconds`` — новое окно."""
    windows: list[list[Message]] = []
    current: list[Message] = []
    for m in messages:
        if current and (_as_utc(m.date) - _as_utc(current[-1].date)).total_seconds() > gap_seconds:
            windows.append(current)
            current = []
        current.append(m)
    if current:
        windows.append(current)
    return windows


def _window_speakers(window: list[Message]) -> list[str]:
    speakers: list[str] = []
    for m in window:
        label = _sender_label(m)
        if m.sender_kind != "channel" and label not in speakers:
            speakers.append(label)
    return speakers


def _sample_message(msg: Message, prev: Message | None) -> SampleMessage:
    gap = None if prev is None else int((_as_utc(msg.date) - _as_utc(prev.date)).total_seconds())
    return SampleMessage(
        message_id=msg.message_id,
        date=_as_utc(msg.date),
        gap_seconds=gap,
        sender=_sender_label(msg),
        sender_kind=msg.sender_kind,
        text=(msg.text or "").strip(),
        media_type=msg.media_type,
    )


def _to_conversation_window(window: list[Message], *, window_size: int) -> ConversationWindow:
    shown = window[:window_size]
    return ConversationWindow(
        start=_as_utc(window[0].date),
        end=_as_utc(window[-1].date),
        message_count=len(window),
        speakers=_window_speakers(window),
        messages=[_sample_message(m, shown[i - 1] if i else None) for i, m in enumerate(shown)],
        truncated=len(window) - len(shown),
    )


def _sender_voices(population: list[Message], *, senders: int, per_sender: int) -> list[SenderVoice]:
    """Топ-``senders`` участников по числу текстовых сообщений; у каждого последние ``per_sender``."""
    by_sender: dict[str, list[Message]] = {}
    for m in population:
        if m.sender_kind in _NON_PERSONA_SENDER_KINDS or not _has_text(m):
            continue
        by_sender.setdefault(_sender_label(m), []).append(m)
    ranked = sorted(by_sender.items(), key=lambda item: (-len(item[1]), item[0]))
    return [
        SenderVoice(
            sender=name,
            message_count=len(msgs),
            samples=[(m.text or "").strip() for m in msgs[-per_sender:]] if per_sender > 0 else [],
        )
        for name, msgs in ranked[:senders]
    ]


def build_chat_samples(
    messages: list[Message],
    *,
    chat_channel_id: int,
    window_days: int,
    windows: int = 4,
    window_size: int = 20,
    gap_seconds: int = DEFAULT_WINDOW_GAP_SECONDS,
    min_messages: int = 6,
    min_speakers: int = 3,
    senders: int = 4,
    per_sender: int = 10,
) -> ChatSamples:
    """Реальная речь чата для композера (#1793), детерминированно и без LLM.

    Окно проходит пороги, если в нём ≥ ``min_messages`` сообщений и
    ≥ ``min_speakers`` участников; показываются ``windows`` самых свежих из
    прошедших (хронологически), каждое — первые ``window_size`` сообщений.
    """
    population = _conversation_population(messages)
    qualifying = [
        w
        for w in split_conversation_windows(population, gap_seconds=gap_seconds)
        if len(w) >= min_messages and len(_window_speakers(w)) >= min_speakers
    ]
    recent = qualifying[-windows:] if windows > 0 else []
    return ChatSamples(
        chat_channel_id=chat_channel_id,
        window_days=window_days,
        message_count=len(population),
        window_gap_seconds=gap_seconds,
        qualifying_windows=len(qualifying),
        windows=[_to_conversation_window(w, window_size=window_size) for w in recent],
        voices=_sender_voices(population, senders=senders, per_sender=per_sender),
    )


def foreign_words(scenario: SeedingScenario, messages: list[Message]) -> dict[int, list[str]]:
    """Слова реплик, которых участники чата не писали: позиция → слова (#1793).

    В слепом тесте 08.11 посевы выдали чужие слова («бутстрап», «градиент» —
    0 раз в чате): манеру персон композер копирует, а словарь остаётся свой.
    Это подсказка, а не нарушение: у 58% живых сообщений эталонного чата есть
    слово, которого больше нигде в чате нет. Словарь — только реплики
    участников: посты канала и анонимный админ пишут новостным языком.
    """
    vocab = sorted({
        token.lower()
        for m in messages
        if m.sender_kind not in _NON_PERSONA_SENDER_KINDS and m.message_kind != "service" and _has_text(m)
        for token in _TOKEN_RE.findall(m.text or "")
    })

    def known(word: str) -> bool:
        # ponytail: основа = первые max(4, len-3) букв вместо стемминга — падежи
        # («лимита»/«лимиты») совпадают, но сленг с общей основой проскочит
        # («свежак»/«свежий»); настоящий стеммер — если промахи начнут мешать.
        stem = word[: max(4, len(word) - 3)]
        i = bisect.bisect_left(vocab, stem)
        return i < len(vocab) and vocab[i].startswith(stem)

    found: dict[int, list[str]] = {}
    for m in sorted(scenario.messages, key=lambda m: m.position):
        words = [w for w in dict.fromkeys(t.lower() for t in _TOKEN_RE.findall(m.text)) if not known(w)]
        if words:
            found[m.position] = words
    return found
