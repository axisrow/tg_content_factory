"""Таргет-тесты офлайн-прототипа рейтинга авторитета (#1748); только синтетика."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from tg_agent.filters.authority_proto import (
    AUTH_TAG_PREFIX,
    Post,
    apply_tier_tags,
    assign_tiers,
    cluster_posts,
    compute_authority,
    jaccard,
    make_shingles,
    make_synthetic_fixture,
    normalize_text,
    passes_digest_gate,
)

T0 = datetime(2026, 11, 1, 8, 0, tzinfo=timezone.utc)


def _post(channel_id: int, minutes: int, text: str, message_id: int = 1) -> Post:
    return Post(channel_id, message_id, T0 + timedelta(minutes=minutes), text)


STORY = "Anthropic выпустила Claude 5.2 с режимом длинного контекста: модель держит весь репозиторий в памяти."


def test_normalize_strips_punctuation_and_case():
    assert normalize_text("⚡ Коротко: Claude-5.2! 【Новость】") == "коротко claude 5 2 новость"


def test_shingles_short_text_falls_back_to_whole_string():
    assert make_shingles("абв") == {"абв"}
    assert len(make_shingles("абвгдеёжз")) == 9 - 5 + 1


def test_jaccard_identical_disjoint_and_empty():
    a = make_shingles("первый пост канала")
    assert jaccard(a, a) == 1.0
    assert jaccard(a, make_shingles("совершенно другой текст")) == 0.0
    assert jaccard(frozenset(), frozenset()) == 0.0


def test_cluster_merges_echoes_and_separates_unrelated():
    posts = [
        _post(1, 0, STORY, 1),
        _post(2, 40, f"⚡ Коротко: {STORY} Подробности у первоисточника.", 2),
        _post(3, 90, f"【Новость】{STORY} Читать дальше.", 3),
        _post(
            4,
            10,
            "Postgres 18 принёс асинхронный ввод-вывод: чтение таблиц "
            "ускорилось в полтора раза на дисках.",
            4,
        ),
    ]
    clusters = cluster_posts(posts)
    assert len(clusters) == 2
    echo = next(c for c in clusters if len(c) == 3)
    assert [p.channel_id for p in echo] == [1, 2, 3]  # sorted by date: скуп первым


def test_scoop_and_echo_rates_on_mini_fixture():
    posts = [
        _post(1, 0, STORY, 1),
        _post(2, 40, f"⚡ Коротко: {STORY} Подробности у первоисточника.", 2),
        _post(1, 120, "Postgres 18 принёс асинхронный ввод-вывод и ускорил чтение таблиц в полтора раза.", 3),
        _post(2, 180, "Postgres 18 принёс асинхронный ввод-вывод, чтение таблиц ускорилось в полтора раза.", 4),
        _post(2, 240, "Уникальный пост канала номер два, который никто не эхает в этом окне сбора данных.", 5),
    ]
    ratings = compute_authority(posts)
    assert ratings[1].scoop_rate == 1.0
    assert ratings[2].scoops == 1 and ratings[2].participations == 3
    assert ratings[2].scoop_rate == pytest.approx(1 / 3)
    assert ratings[1].volume == 2 and ratings[2].volume == 3


def test_tie_gives_scoop_to_all_earliest():
    posts = [_post(1, 0, STORY, 1), _post(2, 0, STORY, 2)]
    ratings = compute_authority(posts)
    assert ratings[1].scoop_rate == 1.0 and ratings[2].scoop_rate == 1.0


def test_noise_classification_water_ad_short():
    # водяной штамп / реклама / механическая коротышка / два нормальных поста
    posts = [
        _post(1, 0, "Стоит отметить, что качество кода определяется культурой проверки результатов работы.", 1),
        _post(1, 10, "Подписывайся на канал про нейросети — разборы релизов каждый день! t.me/x", 2),
        _post(1, 20, "купить дёшево", 3),
        _post(1, 30, STORY, 4),
        _post(1, 40, "Postgres 18 ускорил чтение таблиц в полтора раза на обычных дисках без смены настроек.", 5),
    ]
    ratings = compute_authority(posts)
    assert ratings[1].noise_count == 3
    assert ratings[1].noise_rate == pytest.approx(3 / 5)


def test_forward_itself_is_not_noise():
    ratings = compute_authority([Post(1, 1, T0, STORY, is_forward=True)])
    assert ratings[1].noise_count == 0  # рерайт/эхо наказывает scoop_rate, не noise


def test_spammer_unique_ads_score_zero():
    spam = [
        f"Продвижение каналов №{i}: дёшево и быстро, промокод SAVE20, первый аудит бесплатно до конца недели."
        for i in range(12)
    ]
    posts = [_post(9, i * 30, s, i + 1) for i, s in enumerate(spam)]
    ratings = compute_authority(posts)
    assert ratings[9].scoop_rate == 1.0  # синглтоны — тоже скупы (допущение 1)
    assert ratings[9].noise_rate == 1.0
    assert ratings[9].score == 0.0


def test_synthetic_fixture_ground_truth():
    posts = make_synthetic_fixture()
    ratings = compute_authority(posts)
    volumes = {cid: r.volume for cid, r in ratings.items()}
    assert volumes == {1: 13, 2: 16, 3: 12, 4: 12, 5: 15}
    assert {cid: r.noise_count for cid, r in ratings.items()} == {1: 1, 2: 4, 3: 6, 4: 0, 5: 15}
    rates = {cid: r.scoop_rate for cid, r in ratings.items()}
    assert rates[1] == 1.0 and rates[4] == 1.0 and rates[5] == 1.0  # скупы/синглтоны
    assert rates[2] == pytest.approx(1 / 13)  # чистое эхо + один скуп рекламного кластера
    assert rates[3] == pytest.approx(0.5)
    assert ratings[1].score == pytest.approx(12 / 13)
    tiers = assign_tiers(ratings)
    assert tiers == {1: "A", 2: "C", 3: "B", 4: "A", 5: "C"}
    # Правило дайджеста: кластер с печатниками A+B+C проходит, A+C — нет.
    story0 = next(c for c in cluster_posts(posts) if c[0].message_id == 10_000)
    story1 = next(c for c in cluster_posts(posts) if c[0].message_id == 10_001)
    assert passes_digest_gate(tiers[p.channel_id] for p in story0)
    assert not passes_digest_gate(tiers[p.channel_id] for p in story1)


def test_tiers_quantile_assignment_and_ineligible():
    clean = [
        "Pydantic v3 ускорил валидацию моделей в три раза на горячем пути приложения.",
        "Ruff научился чинить импорты автоматически и сортировать секции без ручных правок.",
        "SQLite получила параллельные чтения без блокировки записи в WAL-режиме.",
        "Кэш диалогов переехал в отдельную таблицу и больше не растёт бесконечно.",
        "Снапшоты рантайма теперь пишутся раз в минуту вместо каждых пяти секунд.",
    ]
    spam = [
        "Промокод SAVE20: курсы английского со скидкой 30% до конца месяца!",
        "Промокод AUTO15: страховка авто за 5 минут, полис пришлём на почту.",
        "Промокод CASH10: кредит наличными без справок для новых клиентов.",
        "Промокод SUSHI25: доставка суши на первый заказ от тысячи рублей.",
        "Промокод REMIX10: ремонт квартир под ключ, смета бесплатно.",
    ]

    def _channel(cid: int, texts: list[str]) -> list[Post]:
        # Тексты заведомо непохожи (Жаккар < порога) — каждый пост это синглтон-кластер.
        return [Post(cid, i + 1, T0 + timedelta(minutes=i * 30), t) for i, t in enumerate(texts)]

    posts = _channel(1, clean[:3]) + _channel(2, clean[3:] + spam[:2]) + _channel(3, spam[2:]) + _channel(7, clean[:1])
    ratings = compute_authority(posts)
    assert ratings[1].score == pytest.approx(1.0)
    assert ratings[2].score == pytest.approx(0.5)
    assert ratings[3].score == pytest.approx(0.0)
    # Терцили по трём пригодным каналам; по умолчанию (volume >= 10) пригодных нет.
    assert assign_tiers(ratings, min_posts=2, min_participations=2) == {1: "A", 2: "B", 3: "C"}
    assert assign_tiers(ratings) == {}
    assert 7 not in assign_tiers(ratings, min_posts=2, min_participations=2)  # volume=1


def test_digest_gate_pure():
    assert passes_digest_gate(["A", "B", None, "C"])
    assert not passes_digest_gate(["B", "C"])
    assert not passes_digest_gate(["A"])
    assert passes_digest_gate(["C", "C", "C"], min_ab=0)
    assert not passes_digest_gate([])


async def test_apply_tier_tags_merges_keeps_and_replaces_auth(db):
    from tg_agent.models import Channel

    pk = await db.repos.channels.add_channel(Channel(channel_id=100, title="proto"))
    await db.repos.channels.set_channel_tags(pk, ["keep"])
    report = await apply_tier_tags(db, {100: "B"})  # dry_run по умолчанию
    assert report == [
        {"channel_id": 100, "pk": pk, "old": ["keep"], "new": ["keep", f"{AUTH_TAG_PREFIX}B"], "written": False}
    ]
    assert await db.repos.channels.get_channel_tags(pk) == ["keep"]  # dry_run не пишет
    await apply_tier_tags(db, {100: "B"}, dry_run=False)
    assert sorted(await db.repos.channels.get_channel_tags(pk)) == [f"{AUTH_TAG_PREFIX}B", "keep"]
    await apply_tier_tags(db, {100: "C"}, dry_run=False)  # протухший auth:B заменён
    assert sorted(await db.repos.channels.get_channel_tags(pk)) == [f"{AUTH_TAG_PREFIX}C", "keep"]


async def test_apply_tier_tags_unknown_channel_raises(db):
    with pytest.raises(ValueError, match="not found"):
        await apply_tier_tags(db, {404_404: "A"})
