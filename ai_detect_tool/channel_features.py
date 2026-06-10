#!/usr/bin/env python3
"""Признаки уровня КАНАЛА для рейтинга AI-slop.

Общий модуль: и разведка (channel_survey.py), и будущий скоринг (channel_eval.py)
считают ОДНИ И ТЕ ЖЕ фичи отсюда, чтобы рейтинг не разъехался с разметкой.

Подход: per-message бинарная атрибуция «AI vs человек» проваливается на коротких
текстах (см. план). Поэтому мы НЕ выносим вердикт по сообщению — мы агрегируем
дешёвые сигналы ПО КАНАЛУ (канал целиком оценивается стабильнее) и считаем
эвристический slop_suspect для сортировки кандидатов.

Ключевая фича — brand_gap: AI разбавляет русский текст англ-брендами в ПРАВИЛЬНОМ
написании (OpenAI, ChatGPT), а ленивый человек пишет транслитом с опечатками
(опенай, чатгпт). Сигнал привязан к лени человека, а не к версии модели → не устаревает.

Ничего не пишет в основную БД; используется только для чтения её содержимого.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from statistics import median

# Переиспользуем готовые per-message строительные блоки из ai_detect.py
# (НЕ classify() — провалившийся бинарный вердикт, его веса вредны).
from ai_detect import _EMOJI_RE, HeuristicAnalyzer

# ---------------------------------------------------------------------------
# Регекспы brand-фичи (с учётом обеих ловушек, описанных в плане)
# ---------------------------------------------------------------------------

# Чистый англ-бренд: CamelCase либо известный бренд в правильном написании.
# 96% русских тех-постов содержат латиницу (URL, тикеры, github-ссылки) —
# поэтому ловим НЕ «латиницу вообще», а аккуратно написанные бренды.
CLEAN_BRAND_RE = re.compile(
    r"\b(?:[A-Z][a-z]+[A-Z][A-Za-z]+"                       # CamelCase: OpenAI, ChatGPT, GitHub
    r"|OpenAI|ChatGPT|Claude|Anthropic|Gemini|Midjourney"
    r"|GitHub|NVIDIA|DeepMind|Stability|Llama|Mistral)\b"
)

# Транслит-ОПЕЧАТКИ брендов — именно исковерканные формы.
# СОЗНАТЕЛЬНО без 'нейросеть/ИИ/промпт' — это нормальный русский, а не лень (ловушка).
TRANSLIT_TYPO_RE = re.compile(
    r"(опенай|чатгпт|чат\s?гпт|джипити|нейронк|клауд|гитхаб"
    r"|нвидиа|мидджорни|дипсик|промт\b)",
    re.IGNORECASE,
)

# CTA-маркеры рекламы (наличие призыва к действию в эмодзи-нагруженном посте).
CTA_RE = re.compile(
    r"(https?://|t\.me/|@\w|подписаться|подпишись|скидк|промокод|купить|заказать|реклама)",
    re.IGNORECASE,
)

# Разговорный сленг / маркеры живой речи (человеческий сигнал; наблюдаемая колонка).
SLANG_RE = re.compile(
    r"\b(щас|чё|че|норм|кек|имхо|жиза|бл\b|типа|короче|ваще|пздц|капец|офигенн|збс)\b",
    re.IGNORECASE,
)

# ---------------------------------------------------------------------------
# Языконезависимые фичи имени канала (название + username).
# Главный сигнал для спам-ферм: случайный username + эмодзи-частокол в названии.
# Работают БЕЗ чтения сообщений — только из таблицы channels.
# ---------------------------------------------------------------------------

# Проектный регекс suspicious_username (src/filters/criteria.py): заглавные латинские
# + цифра, ≥10 символов. Ловит @HS7J8NN78DZSA. Берём как сильный сигнал (→ score 1.0).
PROJECT_SUSPICIOUS_USERNAME_RE = re.compile(r"^(?=.*\d)[A-Z0-9]{10,}$")

# Эмодзи в названии — шире, чем ai_detect._EMOJI_RE: + variation selectors (FE0F),
# enclosed-alphanumeric (🈲🈵), dingbats/arrows (⚛️⏱️→), чтобы поймать китайский частокол.
_TITLE_EMOJI_RE = re.compile(
    "[\U0001f300-\U0001faff\U00002600-\U000027bf\U0001f000-\U0001f0ff"
    "\U0001f100-\U0001f2ff\U00002190-\U000021ff\U00002b00-\U00002bff"
    "\U00002300-\U000023ff\U0000fe00-\U0000fe0f]"
)

# AI-бренды в НАЗВАНИИ (для template_title — наблюдаемый сигнал жанра «генератор имён»).
AI_BRAND_TITLE_RE = re.compile(
    r"(chatgpt|gpt[-\s]?\d?o?|midjourney|nano\s?banana|deepseek|grok|gemini|claude"
    r"|dall[-\s]?e|stable\s?diffusion|flux|sora|kling|runway|openai"
    r"|нейросет|нейромереж|промпт)",
    re.IGNORECASE,
)
# Разделители-конструкторы («ChatGPT × Midjourney + GPT»).
TITLE_SEP_RE = re.compile(r"[×x✕+&|—•·/]")

# QWERTY-смежность для детекции клавиатурного набора (asdadsasddas, zxzcxzxczxc).
_QWERTY_ROWS = ("qwertyuiop", "asdfghjkl", "zxcvbnm")
_QWERTY_NEIGHBORS: dict[str, set[str]] = {}
for _row in _QWERTY_ROWS:
    for _i, _ch in enumerate(_row):
        _nb = _QWERTY_NEIGHBORS.setdefault(_ch, set())
        if _i > 0:
            _nb.add(_row[_i - 1])
        if _i < len(_row) - 1:
            _nb.add(_row[_i + 1])

_VOWELS = set("aeiouаеёиоуыэюя")


def _max_consonant_run(letters: str) -> int:
    """Максимальная серия согласных подряд (FUCTWSXN → длинная серия)."""
    run = mx = 0
    for ch in letters:
        if ch not in _VOWELS:
            run += 1
            mx = max(mx, run)
        else:
            run = 0
    return mx


def _vowel_ratio(letters: str) -> float:
    if not letters:
        return 1.0
    return sum(1 for ch in letters if ch in _VOWELS) / len(letters)


def _bigram_repeat(s: str) -> float:
    """Доля повторяющихся биграмм (asdasdasd, ndndnd → высокая)."""
    bigrams = [s[i:i + 2] for i in range(len(s) - 1)]
    if not bigrams:
        return 0.0
    return 1.0 - len(set(bigrams)) / len(bigrams)


def _kbd_walk_ratio(letters: str) -> float:
    """Доля соседних букв, смежных на QWERTY (asdfg, qwert → высокая)."""
    if len(letters) < 2:
        return 0.0
    adj = sum(
        1 for a, b in zip(letters, letters[1:])
        if b in _QWERTY_NEIGHBORS.get(a, ())
    )
    return adj / (len(letters) - 1)


def random_username_score(username: str | None) -> float:
    """Насколько username похож на случайно сгенерированный (0..1).

    Языконезависимо. Проверено: legit (techcrunch/meduzalive)=0.0-0.30,
    мусор (FUCTWSXN8M778/asdadsasddas/zxzcxzxczxc)=0.7-1.0, без ложных на legit.
    """
    if not username:
        return 0.0
    u = username.strip().lstrip("@")
    if len(u) < 6:
        return 0.0
    if PROJECT_SUSPICIOUS_USERNAME_RE.match(u):
        return 1.0                                   # сильный сигнал — короткое замыкание

    s = u.lower()
    letters = "".join(ch for ch in s if ch.isalpha())
    if len(letters) < 5:
        # почти одни цифры/символы в «username» — тоже подозрительно
        return 0.6 if any(ch.isdigit() for ch in u) else 0.0

    score = 0.0
    if _max_consonant_run(letters) >= 5:
        score += 0.45
    vr = _vowel_ratio(letters)
    if vr < 0.20:
        score += 0.35
    elif vr < 0.28:
        score += 0.15
    if _bigram_repeat(s) > 0.33:
        score += 0.30
    kw = _kbd_walk_ratio(letters)
    if kw > 0.60:
        score += 0.45
    elif kw > 0.45:
        score += 0.25
    # буквы+цифры вперемешку при низкой доле гласных (PZ2Q14432L, R4UQG9BH)
    if any(ch.isdigit() for ch in u) and vr < 0.20:
        score += 0.60
    return min(score, 1.0)


def title_emoji_count(title: str | None) -> int:
    """Число эмодзи в названии (наблюдаемая колонка)."""
    if not title:
        return 0
    return len(_TITLE_EMOJI_RE.findall(title))


def is_emoji_fence(title: str | None) -> bool:
    """Эмодзи-частокол спам-ферм (а не 2-3 осмысленных эмодзи у живого канала).

    Срабатывает при МНОГО эмодзи (≥4) ИЛИ при ПОВТОРЕ одинаковых (⚛️⚛️⚛️, 🈲🈲).
    Проверка показала: 2-3 РАЗНЫХ эмодзи — норма (🐊Seva торгует🐊, 🇹🇭✈️ Виза),
    поэтому порог ≥2 давал ложные срабатывания на живых каналах.
    """
    if not title:
        return False
    em = _TITLE_EMOJI_RE.findall(title)
    if len(em) >= 4:
        return True
    if len(em) >= 2:
        dup = 1.0 - len(set(em)) / len(em)   # доля повторов
        return dup >= 0.34                    # хотя бы один эмодзи дублируется
    return False


def template_title_score(title: str | None) -> float:
    """Жанр «название-генератор из AI-брендов» (0..1). НАБЛЮДАЕМЫЙ, НЕ в flag_count.

    ⚠️ Горит ОДИНАКОВО на slop (✨ ChatGPT × Midjourney) и на легитимных
    AI-новостниках (AI Новости — ChatGPT | Claude) — поэтому сам по себе НЕ
    отличает мусор. Используется только как контекст в аудите фильтра.
    """
    if not title:
        return 0.0
    brands = len(set(m.group(0).lower() for m in AI_BRAND_TITLE_RE.finditer(title)))
    seps = len(TITLE_SEP_RE.findall(title))
    emojis = title_emoji_count(title)
    score = 0.0
    if brands >= 2 and seps >= 1:
        score += 0.5
    if brands >= 3:
        score += 0.2
    if emojis >= 1 and _TITLE_EMOJI_RE.match(title.strip()[:2] or " "):
        score += 0.2                                 # эмодзи-префикс
    if emojis >= 3:
        score += 0.2
    words = len(title.split())
    if brands >= 2 and words <= brands + 2:
        score += 0.2                                 # название почти целиком брендовый список
    return min(score, 1.0)


# ---------------------------------------------------------------------------
# Пороги (стартовые; калибруются на pre-2022 и ground-truth)
# ---------------------------------------------------------------------------

MIN_TEXT_LEN = 120          # ниже этой длины текстовые фичи ненадёжны
AD_EMOJI_THR = 1.5          # эмодзи на 100 символов → реклама
AD_EMOJI_ABS = 6            # абсолютное число эмодзи в посте → реклама
AD_RATIO_TAG = 0.5          # доля рекламных постов канала → tag=ad
FWD_RATIO_TAG = 0.7         # доля форвардов → tag=repost
AI_TOPIC_MIN = 0.05         # clean+translit ≥ этого → канал «AI-тематический», brand_gap применим
PRE2022_CUTOFF = "2022-12-01"  # ChatGPT вышел 30.11.2022 → раньше = 100% человек


# ---------------------------------------------------------------------------
# Сырое сообщение из выборки канала
# ---------------------------------------------------------------------------

@dataclass
class SampleMsg:
    """Одно сообщение семпла канала (только нужные поля)."""

    text: str
    date: str | None = None
    forward_from_channel_id: int | None = None
    views: int | None = None
    forwards: int | None = None
    reply_count: int | None = None
    reactions_json: str | None = None


def _reaction_total(reactions_json: str | None) -> int:
    """Суммарное число реакций из reactions_json (best-effort)."""
    if not reactions_json:
        return 0
    try:
        data = json.loads(reactions_json)
    except (ValueError, TypeError):
        return 0
    total = 0
    # форма обычно [{"emoji": "👍", "count": 3}, ...] либо {"👍": 3, ...}
    if isinstance(data, list):
        for item in data:
            if isinstance(item, dict):
                total += int(item.get("count", 0) or 0)
    elif isinstance(data, dict):
        for v in data.values():
            try:
                total += int(v)
            except (ValueError, TypeError):
                continue
    return total


@dataclass
class ChannelFeatures:
    """Агрегированные фичи уровня канала."""

    channel_id: int
    title: str = ""
    username: str = ""

    # Языконезависимые фичи имени (считаются ВСЕГДА, без чтения сообщений)
    random_username_score: float = 0.0
    template_title_score: float = 0.0
    title_emoji_count: int = 0

    n_total: int = 0          # COUNT всех ru-regular постов канала
    n_sample: int = 0         # фактический размер семпла (после фильтра)
    posts_per_day: float = 0.0
    len_mean: float = 0.0
    len_median: float = 0.0
    pre2022_ratio: float = 0.0

    ad_ratio: float = 0.0
    fwd_ratio: float = 0.0

    emoji_density_mean: float = 0.0
    fmt_density_mean: float = 0.0
    list_ratio_mean: float = 0.0
    ttr_mean: float = 0.0
    burstiness_mean: float = 0.0
    slang_ratio: float = 0.0

    clean_brand_ratio: float = 0.0
    translit_typo_ratio: float = 0.0
    ai_topic: bool = False
    brand_gap: float | None = None     # None если канал не AI-тематический

    eng_views_med: float = 0.0
    eng_react_rate: float = 0.0
    eng_reply_rate: float = 0.0
    low_eng_flag: bool = False

    tag: str = ""             # "" | "ad" | "repost"

    # Флаги-подозрения (вместо единого балла — он не различал каналы, см. план).
    # Пользователь сам фильтрует по комбинации флагов в CSV.
    #
    # НАДЁЖНЫЕ (0% ложных на заведомо-человеческих pre-2022 каналах) — входят в flag_count:
    flag_clean_brands: bool = False   # высокий brand_gap: чистые англ-бренды без транслита
    flag_listy: bool = False          # много списков/буллетов (любимый формат AI)
    flag_formatted: bool = False      # высокая плотность markdown-форматирования
    flag_random_username: bool = False  # случайный username (спам-фермы), языконезависимо, 0 ложных
    #
    # НЕНАДЁЖНЫЕ / НАБЛЮДАЕМЫЕ — НЕ входят в flag_count:
    flag_emoji_fence: bool = False    # эмодзи-частокол: ловит и живых (🐊Seva торгует🐊) — наблюдаемый
    flag_template_title: bool = False  # название-генератор из брендов: горит и на legit AI-каналах
    flag_low_eng: bool = False        # low-engagement: 74% ложных — свойство старых каналов
    flag_no_slang: bool = False       # без сленга: 74% ложных — норма для новостных каналов
    flag_uniform: bool = False        # однообразие: 30% ложных — слабый признак
    flag_count: int = 0               # число НАДЁЖНЫХ флагов (B/L/F/R) для сортировки

    def as_row(self) -> dict:
        """Плоский dict для CSV (None → пустая строка, bool → 0/1, float округлён)."""
        def fmt(v: object) -> object:
            if v is None:
                return ""
            if isinstance(v, bool):
                return int(v)
            if isinstance(v, float):
                return round(v, 4)
            return v

        return {k: fmt(v) for k, v in self.__dict__.items()}


CSV_COLUMNS = [
    "channel_id", "title", "username",
    "tag", "flag_count",
    "flag_clean_brands", "flag_listy", "flag_formatted",
    "flag_random_username", "flag_emoji_fence",
    "flag_template_title", "flag_low_eng", "flag_no_slang", "flag_uniform",
    "random_username_score", "template_title_score", "title_emoji_count",
    "n_total", "n_sample", "posts_per_day", "len_mean", "len_median", "pre2022_ratio",
    "brand_gap", "clean_brand_ratio", "translit_typo_ratio", "ai_topic",
    "ad_ratio", "fwd_ratio",
    "emoji_density_mean", "fmt_density_mean", "list_ratio_mean", "ttr_mean",
    "burstiness_mean", "slang_ratio",
    "eng_views_med", "eng_react_rate", "eng_reply_rate", "low_eng_flag",
]

# Пороги срабатывания флагов (наблюдаемые из смоук-прогона; калибруются на ground-truth).
FLAG_BRAND_GAP_MIN = 0.4    # brand_gap ≥ → чистые бренды доминируют над транслитом
FLAG_LIST_MIN = 0.10        # list_ratio_mean ≥ → канал любит списки
FLAG_FMT_MIN = 0.015        # fmt_density_mean ≥ → много markdown
FLAG_NO_SLANG_MAX = 0.02    # slang_ratio ≤ → почти нет живого сленга
FLAG_UNIFORM_MAX = 0.45     # burstiness_mean ≤ → однообразные предложения
FLAG_RANDOM_USERNAME_MIN = 0.6  # random_username_score ≥ → случайный хэндл
FLAG_TEMPLATE_TITLE_MIN = 0.6   # template_title_score ≥ → название-генератор (наблюдаемый)
# flag_emoji_fence считается через is_emoji_fence() (≥4 эмодзи ИЛИ повтор одинаковых)


def _days_span(dates: list[str]) -> float:
    """Размах семпла в днях по ISO-датам (грубо, по первым 10 символам YYYY-MM-DD)."""
    iso = sorted(d[:10] for d in dates if d)
    if len(iso) < 2 or iso[0] == iso[-1]:
        return 1.0
    from datetime import date as _date
    try:
        lo = _date.fromisoformat(iso[0])
        hi = _date.fromisoformat(iso[-1])
        return max((hi - lo).days, 1)
    except ValueError:
        return 1.0


def _is_ad(text: str) -> bool:
    """Рекламный пост: высокая эмодзи-нагрузка И призыв к действию."""
    emoji_count = len(_EMOJI_RE.findall(text))
    density = emoji_count / max(len(text) / 100, 1)
    heavy = density > AD_EMOJI_THR or emoji_count >= AD_EMOJI_ABS
    return heavy and bool(CTA_RE.search(text))


def compute_channel_features(
    channel_id: int,
    title: str,
    username: str,
    n_total: int,
    sample: list[SampleMsg],
) -> ChannelFeatures:
    """Свести семпл канала в агрегированные фичи + булевы флаги-подозрения."""
    cf = ChannelFeatures(channel_id=channel_id, title=title or "", username=username or "")
    cf.n_total = n_total

    # Языконезависимые фичи имени — считаем ВСЕГДА (даже для нерусских каналов без семпла).
    _compute_name_features(cf)

    if not sample:
        _assign_flags(cf)        # проставит name-флаги даже без текста
        return cf

    cf.n_sample = len(sample)

    # -- объём / частота --
    dates = [m.date for m in sample if m.date]
    span = _days_span(dates)
    cf.posts_per_day = round(n_total / span, 3) if span else 0.0

    # -- pre-2022 доля (заведомо человеческие) --
    pre = sum(1 for m in sample if m.date and m.date[:10] < PRE2022_CUTOFF)
    cf.pre2022_ratio = pre / len(sample)

    # -- форварды / реклама --
    cf.fwd_ratio = sum(1 for m in sample if m.forward_from_channel_id is not None) / len(sample)
    cf.ad_ratio = sum(1 for m in sample if _is_ad(m.text)) / len(sample)

    # -- текстовые фичи (только на постах ≥ MIN_TEXT_LEN) --
    long_msgs = [m for m in sample if len(m.text) >= MIN_TEXT_LEN]
    emoji_d: list[float] = []
    fmt_d: list[float] = []
    list_r: list[float] = []
    ttr: list[float] = []
    burst: list[float] = []
    lengths = [len(m.text) for m in sample]
    cf.len_mean = sum(lengths) / len(lengths)
    cf.len_median = float(median(lengths))

    clean_hits = 0
    translit_hits = 0
    slang_hits = 0
    for m in long_msgs:
        f = HeuristicAnalyzer.compute_features(m.text)
        emoji_d.append(f.emoji_density)
        fmt_d.append(f.formatting_density)
        list_r.append(f.list_marker_ratio)
        ttr.append(f.type_token_ratio)
        burst.append(f.burstiness)
        has_translit = bool(TRANSLIT_TYPO_RE.search(m.text))
        if CLEAN_BRAND_RE.search(m.text) and not has_translit:
            clean_hits += 1
        if has_translit:
            translit_hits += 1
        if SLANG_RE.search(m.text):
            slang_hits += 1

    denom = len(long_msgs) or 1
    cf.emoji_density_mean = sum(emoji_d) / denom if emoji_d else 0.0
    cf.fmt_density_mean = sum(fmt_d) / denom if fmt_d else 0.0
    cf.list_ratio_mean = sum(list_r) / denom if list_r else 0.0
    cf.ttr_mean = sum(ttr) / denom if ttr else 0.0
    cf.burstiness_mean = sum(burst) / denom if burst else 0.0
    cf.slang_ratio = slang_hits / denom
    cf.clean_brand_ratio = clean_hits / denom
    cf.translit_typo_ratio = translit_hits / denom

    cf.ai_topic = (cf.clean_brand_ratio + cf.translit_typo_ratio) >= AI_TOPIC_MIN
    cf.brand_gap = (cf.clean_brand_ratio - cf.translit_typo_ratio) if cf.ai_topic else None

    # -- engagement --
    views = [float(m.views) for m in sample if m.views]
    cf.eng_views_med = float(median(views)) if views else 0.0
    react_rates = [
        _reaction_total(m.reactions_json) / m.views
        for m in sample if m.views and m.views > 0
    ]
    reply_rates = [
        (m.reply_count or 0) / m.views
        for m in sample if m.views and m.views > 0
    ]
    cf.eng_react_rate = float(median(react_rates)) if react_rates else 0.0
    cf.eng_reply_rate = float(median(reply_rates)) if reply_rates else 0.0
    # автопостинг: много постов в день при почти нулевой вовлечённости
    cf.low_eng_flag = (
        cf.posts_per_day >= 5.0
        and cf.eng_react_rate < 0.01
        and cf.eng_reply_rate < 0.005
    )

    _assign_flags(cf)
    return cf


def _compute_name_features(cf: ChannelFeatures) -> None:
    """Языконезависимые фичи имени канала (название + username). Без чтения сообщений."""
    cf.random_username_score = random_username_score(cf.username)
    cf.template_title_score = template_title_score(cf.title)
    cf.title_emoji_count = title_emoji_count(cf.title)


def _assign_flags(cf: ChannelFeatures) -> None:
    """Проставить tag (ad/repost) и булевы флаги-подозрения.

    Намеренно НЕ сводим в единый балл — на смоук-прогоне взвешенная формула
    зажимала всё у 0.5 и не различала каналы (та же болезнь, что у прошлой
    эвристики). Вместо этого — независимые флаги; пользователь фильтрует по
    комбинации (напр. «flag_clean_brands И flag_random_username»).
    """
    # Языконезависимые name-флаги — проставляем ВСЕГДА (даже для ad/нерусских/без семпла).
    # Спам-ферма может быть и рекламной — её случайное имя всё равно сигнал.
    cf.flag_random_username = cf.random_username_score >= FLAG_RANDOM_USERNAME_MIN
    cf.flag_emoji_fence = is_emoji_fence(cf.title)
    cf.flag_template_title = cf.template_title_score >= FLAG_TEMPLATE_TITLE_MIN

    if cf.ad_ratio > AD_RATIO_TAG:
        cf.tag = "ad"                # рекламу автора не детектим, но name-флаги уже стоят
    elif cf.fwd_ratio > FWD_RATIO_TAG:
        cf.tag = "repost"

    if cf.tag != "ad":
        # Надёжные текстовые флаги (0% ложных на pre-2022) — только для не-ad каналов.
        # brand_gap информативен только для AI-тематических каналов (иначе None → флаг off)
        cf.flag_clean_brands = cf.brand_gap is not None and cf.brand_gap >= FLAG_BRAND_GAP_MIN
        cf.flag_listy = cf.list_ratio_mean >= FLAG_LIST_MIN
        cf.flag_formatted = cf.fmt_density_mean >= FLAG_FMT_MIN

        # Ненадёжные флаги — считаем для наблюдения в CSV, но НЕ в flag_count.
        cf.flag_low_eng = cf.low_eng_flag
        cf.flag_no_slang = cf.slang_ratio <= FLAG_NO_SLANG_MAX and cf.n_sample > 0
        cf.flag_uniform = 0.0 < cf.burstiness_mean <= FLAG_UNIFORM_MAX

    # flag_count = надёжные флаги: B/L/F текстовые + R (random_username, 0 ложных).
    # emoji_fence/template_title ловят живых людей → наблюдаемые, НЕ в счёте.
    cf.flag_count = sum([
        cf.flag_clean_brands, cf.flag_listy, cf.flag_formatted,
        cf.flag_random_username,
    ])
