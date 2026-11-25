"""Таргет-тесты рейтинга авторитета каналов (#1748); синтетика + :memory:-БД."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from tg_agent.filters.authority import (
    AUTH_TAG_PREFIX,
    Post,
    apply_tier_tags,
    assign_tiers,
    cluster_posts,
    collect_window_posts,
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


def test_prefix_filter_finds_threshold_pairs_and_keeps_outliers():
    """Лямма SSJ: пара с Жаккаром ≥ порога обязана сойтись префиксами (regress #1748)."""
    a = " ".join(f"слово{i:02d}" for i in range(30))
    b = " ".join(f"слово{i:02d}" for i in range(28)) + " плюс хвост совсем другой новости"
    posts = [
        _post(1, 0, a, 1),
        _post(2, 5, b, 2),
        _post(3, 10, "Совершенно посторонний текст про погоду за окном и старый трамвай.", 3),
    ]
    clusters = cluster_posts(posts)
    assert len(clusters) == 2
    echo = next(c for c in clusters if len(c) == 2)
    assert {p.channel_id for p in echo} == {1, 2}


def test_day_buckets_split_cross_midnight_echoes():
    """Допущение 13: эхо через полночь — другой кластер (другой цикл новости)."""
    late = datetime(2026, 11, 1, 23, 50, tzinfo=timezone.utc)
    posts = [
        Post(1, 1, late, STORY),
        Post(2, 2, late + timedelta(minutes=20), STORY),
        Post(3, 3, late + timedelta(minutes=40), STORY),
    ]
    clusters = cluster_posts(posts)
    assert len(clusters) == 2


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


# ---------------------------------------------------------------------------
# DB-путь: collect_window_posts → compute (промоушен #1748, :memory:-БД)
# ---------------------------------------------------------------------------


async def _seed_channel(db, channel_id: int) -> int:
    from tg_agent.models import Channel

    return await db.repos.channels.add_channel(Channel(channel_id=channel_id, title=f"ch{channel_id}"))


async def _seed_message(db, channel_id: int, message_id: int, date, text, **kwargs) -> None:
    from tg_agent.models import Message

    await db.repos.messages.insert_message(
        Message(channel_id=channel_id, message_id=message_id, date=date, text=text, **kwargs)
    )


async def test_collect_window_posts_window_forward_and_kind(db):
    now = datetime.now(timezone.utc)
    await _seed_channel(db, 200)
    await _seed_message(db, 200, 1, now - timedelta(days=1), STORY)
    await _seed_message(db, 200, 2, now - timedelta(days=40), "слишком старый пост окна")  # вне окна
    await _seed_message(  # форвард помечен is_forward
        db, 200, 3, now - timedelta(days=2), STORY, forward_from_channel_id=999
    )
    await _seed_message(db, 200, 4, now - timedelta(days=1), "участник присоединился", message_kind="service")
    posts = await collect_window_posts(db, days=30)
    assert [(p.message_id, p.is_forward) for p in posts] == [(3, True), (1, False)]  # ORDER BY date


async def test_collect_window_posts_naive_date_reads_as_utc(db):
    await _seed_channel(db, 201)
    await _seed_message(
        db, 201, 7, datetime.now() - timedelta(hours=2), STORY  # noqa: DTZ005 — наивная дата, предмет теста
    )
    posts = await collect_window_posts(db, days=30)
    assert posts[0].date.tzinfo is not None


async def test_db_to_ratings_end_to_end(db):
    now = datetime.now(timezone.utc)
    await _seed_channel(db, 300)  # скуп
    await _seed_channel(db, 400)  # эхо
    await _seed_message(db, 300, 1, now - timedelta(hours=4), STORY)
    await _seed_message(db, 400, 1, now - timedelta(hours=1), f"⚡ Коротко: {STORY} Подробности у первоисточника.")
    await _seed_message(db, 400, 2, now - timedelta(hours=2), "купить дёшево")  # механический шум
    ratings = compute_authority(await collect_window_posts(db, days=30))
    assert ratings[300].scoop_rate == 1.0 and ratings[300].volume == 1
    # «купить дёшево» — шум-синглтон: participation + скуп (допущение 1) канала 400
    assert ratings[400].scoop_rate == 0.5 and ratings[400].noise_count == 1
    assert ratings[400].participations == 2 and ratings[400].scoops == 1
    assert ratings[300].score > ratings[400].score


async def test_fetch_authority_posts_filters_filtered_channels(db):
    pk = await _seed_channel(db, 500)
    await _seed_message(db, 500, 1, datetime.now(timezone.utc) - timedelta(days=1), STORY)
    await db.repos.channels.set_channel_filtered(pk, True, origin="auto")
    assert await collect_window_posts(db, days=30) == []
