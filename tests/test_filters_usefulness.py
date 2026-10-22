"""Детерминированный контент-рейтинг «полезное vs вода» (сценарий 4, 22.10.26).

Прод-путь без LLM: classify_post / score_channel / тир-правила.
Пороги калиброваны по разметке LLM-судьи 22.10.26 — читаются из criteria.py;
тесты фиксируют механику и гардируют формулу снапшотом разметки.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from tg_agent.filters.criteria import (
    USEFULNESS_MIN_POSTS,
    USEFULNESS_TIER_USEFUL_ENGAGED,
    USEFULNESS_TIER_USEFUL_REWRITE,
    USEFULNESS_TIER_USEFUL_WATER,
    USEFULNESS_TIER_WATER_SCORE,
)
from tg_agent.filters.usefulness import _tier, classify_post, score_channel


def _filler_channel(n: int = 40) -> list[tuple[str, bool]]:
    stamp = "В современном мире нейросети играют важную роль. "
    return [(stamp * 5, False) for _ in range(n)]


# --- classify_post ----------------------------------------------------------


def test_stamp_post_is_watery():
    _, watery = classify_post(
        "Не секрет, что ИИ меняет мир. Стоит отметить, что это важно."
    )
    assert watery


def test_concrete_post_is_not_watery():
    _, watery = classify_post(
        "Claude 5.5 стоит $3/млн токенов. Контекст 200k. Вот бенчмарк: 89.3%."
    )
    assert not watery


def test_forward_is_rewrite():
    authorship, _ = classify_post("я переписал этот пост", is_forward=True)
    assert authorship == "rewrite"


def test_bare_link_is_rewrite():
    authorship, _ = classify_post("https://example.com/news/123 — читайте тут")
    assert authorship == "rewrite"


def test_bare_link_hint_is_case_insensitive():
    authorship, _ = classify_post("WWW.EXAMPLE.COM/T.ME/SHORT")
    assert authorship == "rewrite"


def test_own_markers_beat_retell_markers():
    # Личное наблюдение + пересказное слово: первый сигнал — собственный опыт.
    authorship, _ = classify_post(
        "OpenAI announced a new model. Я проверил на своих задачах — мой опыт другой."
    )
    assert authorship == "own"


def test_english_bare_pronoun_is_not_own():
    # Калибровка 22.10.26: \bI\b|\bmy\b убраны — любой англ. текст набирал own=100.
    # Слабые местоимения не авторский сигнал; сильные фразы опыта — да.
    authorship, _ = classify_post("I need a production-ready AI agent for my business.")
    assert authorship is None
    authorship, _ = classify_post("We tested the model on 3 benchmarks; our tests show 2x speedup.")
    assert authorship == "own"


def test_markdown_link_lead_is_rewrite_even_with_pronouns():
    # Фриланс-брифы «[Title](url) I need a developer...» — фид, не личный опыт
    # (канал 1273446931 набирал own=100 на таких постах).
    authorship, _ = classify_post(
        "[Google Ads Lead-Gen Setup](https://freelancer.com/projects/x.html) "
        "I need a brand-new account built from the ground up."
    )
    assert authorship == "rewrite"


def test_bot_handle_lead_is_rewrite():
    # Бот-фид «@bot | [info](url)» (канал Spotify-загрузчика, judge W=95).
    authorship, _ = classify_post(
        "@Spotify_downloaderrr_bot | [info](https://open.spotify.com/track/6s08)"
    )
    assert authorship == "rewrite"


def test_feed_template_phrase_is_rewrite():
    # Шаблонные заголовки авто-фидов — рерайт даже без ссылок в начале.
    authorship, _ = classify_post(
        "🚨 **NEW PUMPFUN CALLOUT** 🪙 Symbio 💰 **Market cap:** `$9 412` 📊 Bonding curve: 57.1%"
    )
    assert authorship == "rewrite"


def test_chinese_headline_bracket_is_quality_retell():
    # Китайский новостной формат 【标题】正文 (金十数据/币快讯; judge qret=65-80).
    authorship, _ = classify_post(
        "【SemiAnalysis：2028年数据中心或面临过剩】 10 月 22 日，半导体研究机构表示，"
        "目前市场已出现一定程度的芯片供应过剩。"
    )
    assert authorship == "quality_retell"


def test_affiliate_water_stamp_is_watery():
    # Amazon-аффилиатный фид (Next Dealz, judge W=85): «Prices & promotions subject».
    _, watery = classify_post(
        "About $4.97 https://www.amazon.com/dp/B00KB5OGCM OREO BTS Sandwich Cookies "
        "__#ad____ Prices & promotions subject to change."
    )
    assert watery
    authorship, watery = classify_post(
        "About $4.97 https://www.amazon.com/dp/B00KB5OGCM OREO Cookies #ad"
    )
    assert authorship == "rewrite"


def test_coupon_feed_is_rewrite():
    # Владелец 22.10.26: купонники — хлам, не полезное. Штампы купонных фидов.
    authorship, _ = classify_post(
        "Free Udemy Course - Machine Learning Basics | 100% OFF Coupon Code"
    )
    assert authorship == "rewrite"


def test_retell_markers_use_canonical_key():
    # Ключи классификации = канонический AUTHORSHIP_KEYS (общий с судьёй).
    authorship, _ = classify_post("По данным инсайдеров, стало известно о слиянии.")
    assert authorship == "quality_retell"


def test_no_signals_is_unclassified():
    authorship, _ = classify_post("Нейросети обучаются на данных и решают задачи.")
    assert authorship is None


def test_empty_text_is_not_watery():
    authorship, watery = classify_post("")
    assert authorship is None
    assert not watery


# --- score_channel ----------------------------------------------------------


def test_score_channel_aggregates_shares():
    posts = [
        # 2 own (классифицированы), 1 quality_retell, 1 unclassified → знаменатель 3.
        ("Я протестировал новый релиз, мой опыт: работает быстрее.", False),
        ("Мне удалось собрать пайплайн за вечер.", False),
        ("По данным компании, стало известно о релизе.", False),
        ("Нейросети решают широкий круг задач.", False),
    ]
    score = score_channel(posts)
    assert score.n_posts == 4
    assert score.n_classified == 3
    assert abs(score.own_share - 200 / 3) < 0.1
    assert abs(score.quality_retell_share - 100 / 3) < 0.1
    assert score.rewrite_share == 0.0
    assert score.water_score == 0.0


def test_water_score_is_share_of_stamped_posts():
    posts = _filler_channel(n=40)
    posts.append(("Конкретика с цифрами и фактами, без штампов. " * 10, False))
    score = score_channel(posts)
    assert abs(score.water_score - 100 * 40 / 41) < 0.1


def test_forward_only_channel_is_not_water():
    # Калибровка 22.10.26: судья не считает рерайт водой (полезные агрегаторы имеют
    # rewrite 80-100 при W=5) — water-тир только по штампам, форвард-канал = mixed.
    score = score_channel([("ANY", True)] * 40)
    assert score.tier == "mixed"
    assert score.rewrite_share == 100.0


def test_to_dict_rounds_and_carries_tier():
    score = score_channel(_filler_channel(n=USEFULNESS_MIN_POSTS))
    d = score.to_dict()
    assert d["tier"] == "water"
    assert d["water_score"] == round(score.water_score, 1)


# --- tier rules (черновые пороги; границы читаются из criteria.py) -----------


def test_tier_no_data_below_min_sample():
    score = score_channel(_filler_channel(n=USEFULNESS_MIN_POSTS - 1))
    assert score.tier == "no_data"


def test_tier_water_on_high_water_score():
    score = score_channel(_filler_channel(n=USEFULNESS_MIN_POSTS))
    assert score.water_score >= USEFULNESS_TIER_WATER_SCORE
    assert score.tier == "water"


def test_tier_useful_needs_low_water_low_rewrite_and_engagement():
    # Вода бьёт всё: 100% own-постов, но со штампами → water.
    stamped_own = "Я попробовал новый агент; стоит отметить, мой опыт положительный."
    score = score_channel([(stamped_own, False)] * USEFULNESS_MIN_POSTS)
    assert score.water_score >= USEFULNESS_TIER_WATER_SCORE
    assert score.tier == "water"

    # Rewrite-доминирование не полезно: фид-канал с rew > потолка → mixed.
    bare = [(f"https://example.com/{i}", False) for i in range(USEFULNESS_MIN_POSTS)]
    score = score_channel(bare)
    assert score.rewrite_share > USEFULNESS_TIER_USEFUL_REWRITE
    assert score.tier == "mixed"

    # Чистые own + quality_retell без штампов: engaged ≥ порога → useful.
    own_post = "Мой опыт: бенчмарк 89.3%, цена $3/млн, вот мои выводы."
    retell_post = "По данным отчёта, релиз состоялся, контекст вырос вдвое."
    n_own = 12
    posts = [(own_post, False)] * n_own
    posts += [(retell_post, False)] * (USEFULNESS_MIN_POSTS - n_own)
    score = score_channel(posts)
    assert score.water_score <= USEFULNESS_TIER_USEFUL_WATER
    assert score.own_share + score.quality_retell_share >= USEFULNESS_TIER_USEFUL_ENGAGED
    assert score.tier == "useful"


def test_tier_mixed_middle():
    # Ни штампов, ни классифицированного контента: engaged=0 → не useful, не water.
    posts = [("Нейросети решают широкий круг задач.", False)] * USEFULNESS_MIN_POSTS
    assert score_channel(posts).tier == "mixed"


def test_calibration_tier_formula_reproduces_judge():
    """Регресс-гард калибровки: формула тиров на судейских долях воспроизводит
    разметку снапшота — ≥16 из 26 полезных каналов получают useful, и не больше
    одного полезного вердикта у 24 бесполезных (включая купонники-override).
    Судейские доли ≠ прод-доли на живых текстах, поэтому это floor, не точность."""
    fixture = Path(__file__).parent / "fixtures" / "usefulness_calibration.json"
    labeled = json.loads(fixture.read_text(encoding="utf-8"))["labeled_channels"]
    reproduced = 0
    useful_total = 0
    false_useful = 0
    for row in labeled:
        shares = {
            "own": row["judge_authorship"]["own"],
            "quality_retell": row["judge_authorship"]["quality_retell"],
            "rewrite": row["judge_authorship"]["rewrite"],
        }
        tier = _tier(USEFULNESS_MIN_POSTS, row["judge_water"], shares)
        verdict = row.get("owner_override") or row["judge_useful"]
        if verdict == "useful":
            useful_total += 1
            reproduced += tier == "useful"
        elif tier == "useful":
            false_useful += 1
    assert reproduced >= 16
    assert false_useful <= 1


def test_empty_channel_is_no_data():
    score = score_channel([])
    assert score.tier == "no_data"
    assert score.to_dict()["water_score"] == 0.0


# --- SQL-сэмпл (integration, :memory:) --------------------------------------


async def test_fetch_recent_post_samples(db):
    """Сэмпл: свежие (по message_id) сначала, форвард-флаг на месте."""
    now = datetime.now(tz=timezone.utc).isoformat()
    # Сэмпл channels-driven: каналы, чьи посты не внесены в channels, не видны.
    for cid in (-100, -200):
        await db.execute_write(
            "INSERT OR IGNORE INTO channels (channel_id) VALUES (?)", (cid,)
        )
    for i in range(3):
        await db.execute_write(
            "INSERT INTO messages (channel_id, message_id, text, date, message_kind) "
            "VALUES (?, ?, ?, ?, 'regular')",
            (-100, i, f"post {i}", now),
        )
    await db.execute_write(
        "INSERT INTO messages (channel_id, message_id, text, date, message_kind, "
        "forward_from_channel_id) VALUES (?, ?, ?, ?, 'regular', 42)",
        (-200, 1, "fwd", now),
    )
    # Пустой текст и нерегулярный kind в сэмпл не попадают (тот же предикат,
    # что у сэмпла судьи).
    await db.execute_write(
        "INSERT INTO messages (channel_id, message_id, text, date, message_kind) "
        "VALUES (?, ?, ?, ?, 'service')",
        (-100, 4, "pinned message", now),
    )

    samples = await db.filter_repo.fetch_recent_post_samples(sample_size=10)
    assert set(samples) == {-100, -200}
    assert [t for t, _ in samples[-100]] == ["post 2", "post 1", "post 0"]
    assert samples[-200] == [("fwd", True)]

    single = await db.filter_repo.fetch_recent_post_samples(
        channel_id=-200, sample_size=10
    )
    assert set(single) == {-200}
