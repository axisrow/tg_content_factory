"""Детерминированный контент-рейтинг канала «полезное vs вода» (сценарий 4).

Прод-путь БЕЗ LLM (решение владельца 22.10.26): по сэмплу последних постов канала
считаются (1) water_score — доля постов со штампами/канцеляритом по Ильяхову
«Пиши, сокращай», (2) профиль авторскости own/quality_retell/rewrite — свой опыт,
пересказ с добавленной ценностью, рерайт/форварды/голые ссылки (ключи — канонический
AUTHORSHIP_KEYS из criteria.py, общий с LLM-судьёй). Итог — тир
useful/mixed/water/no_data.

Детерминированная нижняя граница: «качественный пересказ vs тупой рерайт» до конца
regex не различается — тонкую ось (quality_retell) даёт LLM-судья `channel-rate`,
который используется ТОЛЬКО для калибровки порогов, не в проде. Что словари не
разделят на калибровке — выкидываем (прецедент shapes-метрики auto_feed).
"""
from __future__ import annotations

from dataclasses import dataclass

from tg_agent.filters.criteria import (
    AUTHORSHIP_KEYS,
    AUTO_FEED_SHORT_TEXT,
    OWN_MARKER_RE,
    RETELL_PHRASES,
    REWRITE_PHRASES,
    USEFULNESS_MIN_POSTS,
    USEFULNESS_SCAN_PREFIX,
    USEFULNESS_TIER_USEFUL_ENGAGED,
    USEFULNESS_TIER_USEFUL_REWRITE,
    USEFULNESS_TIER_USEFUL_WATER,
    USEFULNESS_TIER_WATER_SCORE,
    WATER_STAMP_PHRASES,
)

#: Порядок тиров в отчёте: полезные сверху, вода ниже, «нет данных» — в самом низу.
TIER_ORDER: dict[str, int] = {"useful": 0, "mixed": 1, "water": 2, "no_data": 3}


@dataclass
class ChannelUsefulnessScore:
    """Рейтинг канала по содержимому.

    ``water_score`` — доля постов со штампами (0-100). Профиль
    own/quality_retell/rewrite — доли КЛАССИФИЦИРОВАННЫХ постов (пост без
    сигналов авторскости не тянет ни в одну сторону и в знаменатель не попадает).
    """

    n_posts: int
    n_classified: int
    water_score: float
    own_share: float
    quality_retell_share: float
    rewrite_share: float
    tier: str

    def to_dict(self) -> dict:
        return {
            "n_posts": self.n_posts,
            "n_classified": self.n_classified,
            "water_score": round(self.water_score, 1),
            "own_share": round(self.own_share, 1),
            "quality_retell_share": round(self.quality_retell_share, 1),
            "rewrite_share": round(self.rewrite_share, 1),
            "tier": self.tier,
        }


def classify_post(text: str, is_forward: bool = False) -> tuple[str | None, bool]:
    """Авторская категория поста (или None) + маркер воды.

    Категория — первый сработавший сигнал. Фид-форматы (форвард, ссылка-лид,
    шаблонный заголовок авто-фида) = rewrite ДО own-проверки: бриф «I need a
    developer» со ссылкой-лидом — не личный опыт (калибровка 22.10.26: фриланс/
    токен-фиды набирали own=100 на \bI\b). Возвращает ключи канонического
    AUTHORSHIP_KEYS — те же, что у судьи.
    """
    text = text or ""
    # Штампы/маркеры — локальные признаки начала поста: литеральный `in` по
    # lowercase-префиксу (USEFULNESS_SCAN_PREFIX) — regex по полным текстам
    # стоил бы минуты на отчёт (замер 22.10.26).
    probe = text[:USEFULNESS_SCAN_PREFIX].lower()
    watery = any(phrase in probe for phrase in WATER_STAMP_PHRASES)
    has_http = "http" in probe
    # Ссылка-лид (markdown или @-бот): O(1)-гейты дешевле фраз-скана.
    link_lead = has_http and (probe.startswith("@") or (probe.startswith("[") and "](" in probe))
    if is_forward:
        authorship: str | None = "rewrite"
    elif link_lead:
        # Фид-формат — рерайт до own-проверки: бриф «I need a developer»
        # со ссылкой-лидом — не личный опыт.
        authorship = "rewrite"
    elif len(text) < AUTO_FEED_SHORT_TEXT and (has_http or "t.me" in probe):
        # «Голая ссылка» — тот же литерал «короткого поста», что у auto_feed.
        authorship = "rewrite"
    elif any(phrase in probe for phrase in REWRITE_PHRASES):
        # Шаблонные заголовки авто-фидов — рерайт даже без ссылки в начале.
        authorship = "rewrite"
    elif OWN_MARKER_RE.search(probe):
        authorship = "own"
    elif any(phrase in probe for phrase in RETELL_PHRASES):
        authorship = "quality_retell"
    else:
        authorship = None
    return authorship, watery


def score_channel(posts: list[tuple[str, bool]]) -> ChannelUsefulnessScore:
    """Свести сэмпл постов канала `(text, is_forward)` в рейтинг — один проход."""
    watery = 0
    counts = dict.fromkeys(AUTHORSHIP_KEYS, 0)
    for text, is_forward in posts:
        authorship, is_watery = classify_post(text, is_forward)
        watery += is_watery
        if authorship is not None:
            counts[authorship] += 1
    n_posts = len(posts)
    n_classified = sum(counts.values())
    water_score = watery / n_posts * 100 if n_posts else 0.0
    shares = {
        key: (count / n_classified * 100 if n_classified else 0.0)
        for key, count in counts.items()
    }
    return ChannelUsefulnessScore(
        n_posts=n_posts,
        n_classified=n_classified,
        water_score=water_score,
        own_share=shares["own"],
        quality_retell_share=shares["quality_retell"],
        rewrite_share=shares["rewrite"],
        tier=_tier(n_posts, water_score, shares),
    )


def _tier(n_posts: int, water_score: float, shares: dict[str, float]) -> str:
    """Правила тира; пороги калиброваны по разметке судьи 22.10.26 (см. criteria)."""
    if n_posts < USEFULNESS_MIN_POSTS:
        return "no_data"
    if water_score >= USEFULNESS_TIER_WATER_SCORE:
        return "water"
    engaged = shares["own"] + shares["quality_retell"]
    if (
        water_score <= USEFULNESS_TIER_USEFUL_WATER
        and shares["rewrite"] <= USEFULNESS_TIER_USEFUL_REWRITE
        and engaged >= USEFULNESS_TIER_USEFUL_ENGAGED
    ):
        return "useful"
    return "mixed"
