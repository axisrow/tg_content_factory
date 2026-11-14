"""Офлайн-прототип рейтинга авторитета каналов (#1748).

ПРОТОТИП (итерация 1): считает scoop_rate / noise_rate / volume по списку постов
в памяти и предлагает теги ``auth:A/B/C`` для отбора дайджеста. Ни CLI-поверхности,
ни живого Telegram, ни чтения БД здесь нет — только синтетические фикстуры;
калибровка на реальной БД — следующий ручной шаг, затем промоушен в
``analytics authority`` по иссую.

Конвейер по иссую: нормализованный текст → символьные шинглы → инвертированный
индекс → кандидаты по Жаккару → union-find кластеры; самый ранний пост кластера —
скуп, остальные — эхи.

Журнал допущений (метрики не «правда», а предложения для калибровки):

1. ``scoop_rate = скупы / кластеры с участием канала``. Кластеры включают
   синглтоны: пост, который никто не перецитировал, всё равно скуп его автора —
   «first printer wins» (принцип владельца, 2026-11-02). Следствие: уникальный
   мусор тоже даёт scoop_rate=1.0 — вниз его тянет множитель ``(1 - noise_rate)``
   в score. Альтернатива «считать только кластеры ≥2 постов, синглтоны — не
   свидетельство» сознательно не взята: канал, публикующий оригиналы, которые
   никто не эхает, остался бы вообще без тира.
2. Точный тай-стамп внутри кластера → скуп засчитывается каждому из самых
   ранних. Таймы с секундной гранулярностью редки; дробного кредита нет.
3. Почти-дубликат = Жаккар ≥ ``JACCARD_THRESHOLD`` по символьным
   k-шинглам ``SHINGLE_K`` нормализованного текста. Транзитивность union-find
   может сшить цепочку почти-дубликатов (дрейф) — для прототипа принимаем.
4. Шинглы с документной частотой > ``MAX_DF_SHARE`` корпуса исключаются из
   генерации кандидатов (guard от стоп-слов), но только когда корпус достаточно
   велик (малый cap отрезал бы легитимных кандидатов); сам Жаккар считается по
   полным множествам шинглов.
5. ``noise`` = штамп воды (детерминированный ``classify_post``, #1658) ИЛИ
   рекламный маркер (``AD_PROMO_PHRASES``, стартовый список) ИЛИ механический
   короткий пост (нормализованный текст < ``MECHANICAL_MIN_CHARS``). Пост,
   попавший в несколько категорий, считается один раз. Форвард сам по себе
   шумом не является (рерайт = эхо, эхи наказывает scoop_rate) — только те же
   три сигнала.
6. ``volume`` = число постов в окне: нормализация score и порог пригодности
   («100 постов, 3 попадания — плохо»).
7. ``score = scoop_rate * (1 - noise_rate)`` — предварительная свёртка двух
   осей в одну для тиров; при калибровке можно заменить на явные пороги по
   каждой оси.
8. Тиры: терцильные сечения (inclusive-квантили) распределения score по
   пригодным каналам (``volume >= MIN_POSTS_FOR_TIER`` и
   ``participations >= MIN_PARTICIPATIONS_FOR_TIER``); A выше сечения 2/3,
   B выше 1/3, C ниже; непригодные остаются без тега. Вырожденный случай
   (все score равны) уводит всех в C — перекалибровать на промоушене.
   Малая выборка даёт нестабильные тиры; финальные сечения решает калибровка.
9. Правило дайджеста: тема проходит, если среди её принтеров ≥
   ``DIGEST_MIN_AB_PRINTERS`` каналов тира A/B. Эхи тира C покрытием не считаются.
10. Даты: все посты должны быть одной «оснащённости» таймзоной (aware и naive
    между собой не сравниваются) — прототип не приводит их сам.
11. Теги: ``auth:A/B/C`` заменяют протухшие ``auth:*`` и сохраняют прочие теги
    (например ``keep``); запись идёт через ``ChannelsRepository.set_channel_tags``
    (он полная замена набора, поэтому слияние делает эта функция). По умолчанию
    ``dry_run=True`` — реальная запись тегов есть действие оператора и на
    промоушене должно идти от имени человека.
"""
from __future__ import annotations

import re
import statistics
import unicodedata
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from itertools import combinations
from typing import Iterable, Sequence

from tg_agent.filters.criteria import USEFULNESS_SCAN_PREFIX
from tg_agent.filters.usefulness import classify_post

#: Длина символьного шингла по нормализованному тексту.
SHINGLE_K = 5
#: Порог Жаккара для «почти-дубликата».
JACCARD_THRESHOLD = 0.35
#: Шинглы с df выше этой доли корпуса не генерируют кандидатов (стоп-слова).
MAX_DF_SHARE = 0.2
#: «Механический короткий пост»: нормализованный текст короче этого — шум.
MECHANICAL_MIN_CHARS = 40
#: Порог пригодности для тиров: минимум постов (volume).
MIN_POSTS_FOR_TIER = 10
#: Порог пригодности для тиров: минимум кластеров с участием канала.
MIN_PARTICIPATIONS_FOR_TIER = 3
#: Правило дайджеста: минимум принтеров тира A/B.
DIGEST_MIN_AB_PRINTERS = 2
#: Префикс тега тира в существующем m2m tags/channel_tags.
AUTH_TAG_PREFIX = "auth:"

#: Рекламные маркеры — литералы по lowercase-префиксу поста, в стиле classify_post
#: (см. USEFULNESS_SCAN_PREFIX). Стартовый список до калибровки: что не разделяет
#: каналы — выкидываем (прецедент shapes-метрики auto_feed).
AD_PROMO_PHRASES: tuple[str, ...] = (
    "по вопросам рекламы",
    "подписывайся",
    "подпишись",
    "промокод",
    "партнёрский материал",
    "партнерский материал",
    "рекламодатель",
    "sponsored",
    "erid:",
)

_PUNCT_RE = re.compile(r"[^\w\s]+", re.UNICODE)


@dataclass(frozen=True)
class Post:
    """Один пост окна сбора: достаточно для кластеризации и рейтинга."""

    channel_id: int
    message_id: int
    date: datetime
    text: str
    is_forward: bool = False


@dataclass(frozen=True)
class ChannelAuthority:
    """Итоговые метрики канала за окно (см. журнал допущений 1-7)."""

    channel_id: int
    volume: int
    noise_count: int
    noise_rate: float
    participations: int
    scoops: int
    scoop_rate: float
    score: float


def normalize_text(text: str) -> str:
    """Lowercase + NFKC, пунктуация/эмодзи → пробел, схлопнутые пробелы."""
    text = unicodedata.normalize("NFKC", text or "").lower()
    text = _PUNCT_RE.sub(" ", text)
    return " ".join(text.split())


def make_shingles(normalized: str, k: int = SHINGLE_K) -> frozenset[str]:
    """Символьные k-шинглы; короткий текст целиком — один шингл (нет пустого множества)."""
    if not normalized:
        return frozenset()
    if len(normalized) <= k:
        return frozenset({normalized})
    return frozenset(normalized[i : i + k] for i in range(len(normalized) - k + 1))


def jaccard(a: frozenset[str], b: frozenset[str]) -> float:
    """|A∩B| / |A∪B|; два пустых множества не «идентичны», а 0.0."""
    union = a | b
    return len(a & b) / len(union) if union else 0.0


def cluster_posts(
    posts: Sequence[Post], threshold: float = JACCARD_THRESHOLD
) -> list[list[Post]]:
    """Слить почти-дубликаты в кластеры; каждый кластер отсортирован по дате.

    # ponytail: кандидаты через инвертированный индекс — O(sum df^2); на
    # недельном окне (единицы тысяч постов) это доли секунды. Если калибровка
    # покажет иначе — MinHash/LSH.
    """
    n = len(posts)
    if n == 0:
        return []
    shingles = [make_shingles(normalize_text(p.text)) for p in posts]
    df: Counter[str] = Counter()
    for group in shingles:
        df.update(group)
    max_df = int(MAX_DF_SHARE * n)
    if max_df < 8:
        # ponytail: на малых корпусах df-cap отрежет легитимных кандидатов;
        # охранять от стоп-слов имеет смысл только когда корпус большой.
        max_df = n
    index: dict[str, list[int]] = {}
    for i, group in enumerate(shingles):
        for shingle in group:
            if df[shingle] <= max_df:
                index.setdefault(shingle, []).append(i)

    parent = list(range(n))

    def find(x: int) -> int:
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a: int, b: int) -> None:
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[max(ra, rb)] = min(ra, rb)

    for candidates in index.values():
        for a, b in combinations(candidates, 2):
            if find(a) == find(b):
                continue
            if jaccard(shingles[a], shingles[b]) >= threshold:
                union(a, b)

    buckets: dict[int, list[Post]] = {}
    for i in range(n):
        buckets.setdefault(find(i), []).append(posts[i])
    return [sorted(group, key=lambda p: (p.date, p.message_id)) for group in buckets.values()]


def _is_noise(post: Post) -> bool:
    """Шум = штамп воды, рекламный маркер или механическая коротышка (допущение 5)."""
    _, watery = classify_post(post.text, post.is_forward)
    if watery:
        return True
    probe = (post.text or "")[:USEFULNESS_SCAN_PREFIX].lower()
    if any(phrase in probe for phrase in AD_PROMO_PHRASES):
        return True
    return len(normalize_text(post.text)) < MECHANICAL_MIN_CHARS


def compute_authority(posts: Sequence[Post]) -> dict[int, ChannelAuthority]:
    """scoop/noise/volume по каналам; кластеры (включая синглтоны) — см. допущения 1-2."""
    volume: Counter[int] = Counter(p.channel_id for p in posts)
    noise: Counter[int] = Counter(p.channel_id for p in posts if _is_noise(p))
    participations: Counter[int] = Counter()
    scoops: Counter[int] = Counter()
    for cluster in cluster_posts(posts):
        members = {p.channel_id for p in cluster}
        for cid in members:
            participations[cid] += 1
        earliest = cluster[0].date
        for cid in {p.channel_id for p in cluster if p.date == earliest}:
            scoops[cid] += 1
    ratings: dict[int, ChannelAuthority] = {}
    for cid, n_posts in volume.items():
        scoop_rate = scoops[cid] / participations[cid] if participations[cid] else 0.0
        noise_rate = noise[cid] / n_posts
        ratings[cid] = ChannelAuthority(
            channel_id=cid,
            volume=n_posts,
            noise_count=noise[cid],
            noise_rate=noise_rate,
            participations=participations[cid],
            scoops=scoops[cid],
            scoop_rate=scoop_rate,
            score=scoop_rate * (1 - noise_rate),
        )
    return ratings


def assign_tiers(
    ratings: dict[int, ChannelAuthority],
    min_posts: int = MIN_POSTS_FOR_TIER,
    min_participations: int = MIN_PARTICIPATIONS_FOR_TIER,
) -> dict[int, str]:
    """Тиры A/B/C терцилями score по пригодным каналам (допущение 8); остальные без тега."""
    eligible = {
        cid: r.score
        for cid, r in ratings.items()
        if r.volume >= min_posts and r.participations >= min_participations
    }
    if not eligible:
        return {}
    if len(eligible) == 1:
        # ponytail: на одном канале распределения нет; C — заглушка до калибровки.
        return {next(iter(eligible)): "C"}
    cut_low, cut_high = statistics.quantiles(sorted(eligible.values()), n=3, method="inclusive")
    return {
        cid: "A" if score > cut_high else ("B" if score > cut_low else "C")
        for cid, score in eligible.items()
    }


def passes_digest_gate(printer_tiers: Iterable[str | None], min_ab: int = DIGEST_MIN_AB_PRINTERS) -> bool:
    """Тема проходит, если среди принтеров достаточно тиров A/B (допущение 9)."""
    return sum(1 for tier in printer_tiers if tier in ("A", "B")) >= min_ab


def make_synthetic_fixture() -> list[Post]:
    """Детерминированный офлайн-мир с известной правдой для тестов и демо.

    Каналы: 1 scoopster — 12 оригиналов (их эхают оба эхо-канала) + 1 пост со
    штампом воды; 2 echo_one — почти-копии всех 12 позже + кластер из 4
    одинаковых рекламок; 3 echo_two — почти-копии чётных историй позже + 6
    уникальных водяных постов; 4 solo — 12 оригиналов без единого эха (допущение
    1 в чистом виде); 5 spammer — 15 разных рекламок с промокодами.

    Ожидаемая правда: scoopster/solo — тиры A (scoop_rate 1.0), echo_two — B
    (эхи не скупы, половина — вода), echo_one и spammer — C (score ≈ 0).
    """
    t0 = datetime(2026, 11, 1, 8, 0, tzinfo=timezone.utc)
    stories = [
        "Anthropic выпустила Claude 5.2 с режимом длинного контекста: модель держит "
        "весь репозиторий в памяти и сама правит тесты после рефакторинга.",
        "OpenAI открыла доступ к пакетным запросам: цена вдвое ниже, ответ приходит "
        "в течение суток, лимиты очереди поднимали трижды за неделю.",
        "Google выпустила Gemini 4 для локального запуска: вес квантованной версии "
        "12 ГБ, качество близко к облачному, работает на одной 4090.",
        "Ядро Linux 6.14 получило планировщик EEVDF по умолчанию: задержки на "
        "десктопе упали вдвое, остатки старого CFS удалены полностью.",
        "Rust 1.90 стабилизировал async-генераторы: стриминг из HTTP-обработчиков "
        "пишется без сторонних крейтов, компиляцию ускорили новым кэшем.",
        "Meta открыла веса Llama 6: 400 млрд параметров, контекст 2 млн токенов, "
        "лицензия запрещает сервисам с аудиторией выше 700 млн.",
        "Apple добавила в Xcode локального агента: индексирует проект на устройстве, "
        "правки предлагает по горячей клавише, телеметрию отключили.",
        "Microsoft перевела Azure на жидкостное охлаждение: PUE упал до 1.08, "
        "старые стойки с воздушным охлаждением выводят до конца квартала.",
        "Postgres 18 принёс асинхронный ввод-вывод: чтение таблиц ускорилось "
        "в полтора раза на обычных дисках, настройки по умолчанию пересмотрели.",
        "Python 3.14 ускорил интерпретатор на 15% копированием со сборкой: проверено "
        "на стандартных бенчмарках, обратная совместимость не пострадала.",
        "Telegram открыл API для тем каналов: посты можно собирать по темам без ботов, "
        "лимит запросов подняли до тысячи в минуту.",
        "Kubernetes 1.35 упростил kubectl apply: дифф показывает реальные изменения "
        "кластера, дубли конфигов почистили, миграция автоматическая.",
    ]
    water_fillers = [
        "Стоит отметить, что каждая команда выбирает процесс разработки исходя из "
        "своих возможностей и целей на будущее.",
        "Не секрет, что в наше время качество продукта определяется не только кодом, "
        "но и культурой проверки результатов.",
        "Как известно, успешная миграция инфраструктуры требует терпения, "
        "документации и честных замеров до и после.",
        "Подводя итог, можно сказать, что автоматизация рутинных задач освобождает "
        "время для настоящей инженерии.",
        "В заключение отметим, что выбор инструментов всегда компромисс между "
        "скоростью, надёжностью и стоимостью владения.",
        "Таким образом, зрелые процессы начинаются с малых шагов, измеримых метрик "
        "и регулярного разбора инцидентов.",
    ]
    ads = [
        "Подписывайся на канал про нейросети — каждый день разборы релизов без воды и спама! t.me/echo_one",
    ] * 4
    spam = [
        "Курсы английского со скидкой 30%: промокод SAVE20 до конца месяца!",
        "Страховка авто за 5 минут: промокод AUTO15, полис пришлём на почту.",
        "Кредит наличными без справок: промокод CASH10 для новых клиентов банка.",
        "Доставка суши и пиццы: промокод SUSHI25 на первый заказ от тысячи рублей.",
        "Ремонт квартир под ключ: смета бесплатно, промокод REMIX10 на работы.",
        "Мебель на заказ по вашим чертежам: промокод WOOD5 на кухонные гарнитуры.",
        "Туры на море из вашего города: горящие путёвки, промокод SEA30 на бронь.",
        "Магазин электроники: смартфоны и ноутбуки в рассрочку, промокод GADGET7.",
        "Маникюр и брови в центре города: запись онлайн, промокод BEAUTY20.",
        "Автозапчасти в наличии: подбор по VIN бесплатно, промокод PARTS15.",
        "Фитнес-клуб рядом с метро: год без взноса, промокод GYM50 до пятницы.",
        "Детские товары со склада: подгузники и питание, промокод KIDS10.",
        "Кофе с собой: второй напиток в подарок, промокод COFFEE2 по утрам.",
        "Аренда авто посуточно: без залога, промокод DRIVE8 для новых арендаторов.",
        "Юрист онлайн: консультация бесплатно, промокод LAW2026 на подготовку иска.",
    ]
    posts: list[Post] = []
    for i, story in enumerate(stories):
        base = t0 + timedelta(hours=2 * i)
        posts.append(Post(1, 10_000 + i, base, story))
        posts.append(
            Post(2, 20_000 + i, base + timedelta(minutes=40), f"⚡ Коротко: {story} Подробности у первоисточника.")
        )
        if i % 2 == 0:
            posts.append(Post(3, 30_000 + i, base + timedelta(minutes=90), f"【Новость】{story} Читать дальше."))
    posts.append(
        Post(1, 10_100, t0 + timedelta(hours=30), (
            "В современном мире искусственный интеллект меняет индустрию разработки, и каждая команда заново "
            "придумывает процесс проверки качества кода."
        ))
    )
    for j, filler in enumerate(water_fillers):
        posts.append(Post(3, 31_000 + j, t0 + timedelta(hours=1 + 3 * j, minutes=45), filler))
    for j, ad in enumerate(ads):
        posts.append(Post(2, 21_000 + j, t0 + timedelta(hours=5, minutes=10 * j), ad))
    for i, s in enumerate(spam):
        posts.append(Post(5, 50_000 + i, t0 + timedelta(hours=1, minutes=30 * i), s))
    for i, story in enumerate(stories):
        posts.append(
            Post(4, 40_000 + i, t0 + timedelta(hours=2 * i + 2, minutes=20), f"[обзор] {story[::-1][:-8]}{story[-8:]}")
        )
    return posts


async def apply_tier_tags(
    db, tiers: dict[int, str], dry_run: bool = True
) -> list[dict]:
    """Слить тиры в теги каналов (допущение 11); вернуть отчёт по каналам.

    ``tiers`` ключованы по Telegram channel_id. Протухшие ``auth:*`` заменяются,
    прочие теги (например ``keep``) сохраняются: ``set_channel_tags`` делает
    полную замену набора, поэтому слияние здесь. ``dry_run=True`` (по умолчанию)
    только отчёт — запись тегов это действие оператора.
    """
    repo = db.repos.channels
    report: list[dict] = []
    for channel_id, tier in sorted(tiers.items()):
        channel = await repo.get_channel_by_channel_id(channel_id)
        if channel is None or channel.id is None:
            raise ValueError(f"channel_id={channel_id} not found in channels table")
        current = await repo.get_channel_tags(channel.id)
        desired = [t for t in current if not t.startswith(AUTH_TAG_PREFIX)]
        desired.append(f"{AUTH_TAG_PREFIX}{tier}")
        report.append(
            {
                "channel_id": channel_id,
                "pk": channel.id,
                "old": current,
                "new": desired,
                "written": bool(not dry_run and desired != current),
            }
        )
        if not dry_run and desired != current:
            await repo.set_channel_tags(channel.id, desired)
    return report


def main() -> int:
    """Демо на синтетической фикстуре: таблица рейтинга + примеры правила дайджеста."""
    posts = make_synthetic_fixture()
    ratings = compute_authority(posts)
    tiers = assign_tiers(ratings)
    print(f"posts={len(posts)} channels={len(ratings)} (synthetic fixture, #1748 prototype)")
    header = (
        f"{'chan':>5} {'volume':>6} {'noise':>6} {'scoop':>6} "
        f"{'parts':>6} {'s_rate':>7} {'n_rate':>7} {'score':>7} {'tier':>4}"
    )
    print(header)
    for r in sorted(ratings.values(), key=lambda r: r.score, reverse=True):
        print(
            f"{r.channel_id:>5} {r.volume:>6} {r.noise_count:>6} {r.scoops:>6} {r.participations:>6} "
            f"{r.scoop_rate:>7.2f} {r.noise_rate:>7.2f} {r.score:>7.2f} {tiers.get(r.channel_id, '-'):>4}"
        )
    multi = [c for c in cluster_posts(posts) if len({p.channel_id for p in c}) >= 2]
    for cluster in multi[:2]:
        printers = sorted({p.channel_id for p in cluster})
        gate = "PASS" if passes_digest_gate(tiers.get(cid) for cid in printers) else "SKIP"
        shown = [tiers.get(c) for c in printers]
        print(f"cluster@{cluster[0].date:%H:%M} printers={printers} tiers={shown} -> digest {gate}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
