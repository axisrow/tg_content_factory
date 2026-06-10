#!/usr/bin/env python3
"""Слепая разметка КАНАЛОВ по ДВУМ осям: полезность + жанр.

Калька с human_eval.py, но на уровне КАНАЛА. Ключевой сдвиг рамки (решение
пользователя): ось «AI vs человек» ложная — полезный AI-обзор репозитория ценнее
бесполезных человеческих постов «как я ел пирожки». Поэтому размечаем НЕ «slop vs
не-slop», а:
  • ОСЬ 1 — полезность: useful / useless (есть ли ценность для читателя).
  • ОСЬ 2 — жанр: ad (реклама товаров/услуг) / infobiz (продажа курсов/обучения/
    «успеха») / aggregator (канал-куратор: дайджесты-подборки чужого СО СВОЕЙ
    подачей) / copy (льёт чужое 1-в-1 без подачи, клон) / original (свой контент:
    блог, новости, обзоры). Оригинал vs рерайт НЕ различаем.

Название/username/флаги при разметке СКРЫТЫ — оцениваешь только по текстам постов.

Команды:
  prepare    — набрать каналы, посчитать фичи (молча), записать сессию.
  add-value  — дозаписать каналы-кандидаты в useful (0 флагов + pre-2022).
  remap      — авто-маппинг старых меток (slop/value/mixed/ad) в новые две оси.
  label      — слепая разметка двух осей (запускай сам через `! ...`).
  llm        — channel-level вердикт LLM по набору постов (две оси).
  report     — точность флагов и LLM против разметки.

Отдельная утилита дедупликации каналов-дубликатов — post_dedup.py (по хэшам постов).

Основная БД открывается ТОЛЬКО НА ЧТЕНИЕ; ничего в неё не пишется.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from pathlib import Path

import ai_detect  # переиспользуем LlmJudge (Ollama/OpenAI клиент)
from channel_features import ChannelFeatures, compute_channel_features
from channel_survey import (
    _all_channels,
    _connect_ro,
    _ru_msg_counts,
    _sample_channel,
)

TOOL_DIR = Path(__file__).resolve().parent
SESSION = TOOL_DIR / "channel_eval_session.json"

MIN_MSGS = 50          # каналы с ≥ этого числа ru-постов (иначе не на чем показывать)
SAMPLE_SIZE = 120      # сырой семпл на канал (из него отберём длинные)
MIN_POST_LEN = 300     # как human_eval.MIN_LEN — короткое не атрибутируется
MAX_POST_LEN = 2000    # слишком длинные обрежем при показе
POSTS_PER_CHANNEL = 7  # сколько постов показать на канал

# Две оси разметки.
USEFUL_LABELS = {"u": "useful", "x": "useless"}
GENRE_LABELS = {"a": "ad", "i": "infobiz", "g": "aggregator", "c": "copy", "o": "original"}

# Надёжные флаги (в flag_count) — их и оцениваем.
FLAG_ATTRS = [
    ("flag_clean_brands", "B"),
    ("flag_listy", "L"),
    ("flag_formatted", "F"),
    ("flag_random_username", "R"),
]

# Channel-level промпт для LLM-судьи: оценивает НАБОР постов канала по ДВУМ осям.
# Полезность — семантика (LLM меряет хорошо); жанр — структура канала.
LLM_CHANNEL_PROMPT = """Ты оцениваешь Telegram-канал по нескольким его постам. Дай ДВЕ независимые оценки.

ОСЬ 1 — ПОЛЕЗНОСТЬ (есть ли ценность для читателя, НЕЗАВИСИМО от того, написал человек или AI):
- useful: даёт читателю реальную пользу — конкретику, факты, разбор, рабочую информацию, экспертизу.
  ВАЖНО: полезный AI-текст (например, толковый обзор репозитория/новости) — это useful, не наказывай
  за «машинность». Полезность важнее авторства.
- useless: пустой контент ради контента — вода, общие банальности, накрутка объёма, инфоцыганские
  обещания без сути, бесполезные личные посты ни о чём, кликбейт без содержания.

ОСЬ 2 — ЖАНР (что это за канал по сути):
- ad: реклама товаров/услуг/недвижимости — цены, призывы купить/заказать/связаться, контакты.
- infobiz: инфобизнес — продажа курсов/марафонов/обучения/«успеха», вебинары, лид-магниты,
  призывы «пиши слово в комменты», «запишись на бесплатный вебинар».
- aggregator: канал-КУРАТОР — собирает чужие источники (сайты/каналы) СО СВОЕЙ подачей: дайджесты,
  подборки, ленты новостей с разных источников, обзоры со ссылками. Есть работа отбора/компоновки.
- copy: канал льёт чужое 1-в-1 БЕЗ своей подачи — голый репост/копипаста, клон чужого канала.
- original: канал производит СВОЙ авторский оригинальный контент — блог, авторские новости, обзоры,
  разборы, мнения (рерайт чужого своими словами тоже сюда — оригинал vs рерайт не различаем).

Посты канала:
{posts}

Ответь СТРОГО в JSON (одна строка):
{{"useful": "useful|useless", "genre": "ad|infobiz|aggregator|copy|original", """ \
    """"confidence": 0.0-1.0, "reason": "кратко"}}"""


# ---------------------------------------------------------------------------
# 1. prepare — набрать выборку + посчитать флаги (молча)
# ---------------------------------------------------------------------------

def _stratified_pick(feats: list[ChannelFeatures], n: int) -> list[ChannelFeatures]:
    """~n//2 подозреваемых (flag_count≥1) + ~n//2 «чистых» (0 флагов), страйдом.

    Детерминированно (без random): берём каждый k-й по отсортированному
    channel_id внутри каждой группы — равномерно по всей выборке, воспроизводимо.
    """
    def stride(pool: list[ChannelFeatures], want: int) -> list[ChannelFeatures]:
        if want <= 0 or not pool:
            return []
        if len(pool) <= want:
            return list(pool)
        step = len(pool) / want
        return [pool[int(i * step)] for i in range(want)]

    suspect = sorted((cf for cf in feats if cf.flag_count >= 1), key=lambda cf: cf.channel_id)
    clean = sorted((cf for cf in feats if cf.flag_count == 0), key=lambda cf: cf.channel_id)
    half = n // 2
    picked = stride(suspect, half) + stride(clean, n - half)
    # если одна из групп оказалась мала — добиваем из другой
    if len(picked) < n:
        chosen_ids = {cf.channel_id for cf in picked}
        rest = [cf for cf in feats if cf.channel_id not in chosen_ids]
        rest.sort(key=lambda cf: cf.channel_id)
        picked += stride(rest, n - len(picked))
    return picked[:n]


def _channel_posts(conn, channel_id: int) -> list[str]:
    """5-7 русских ДЛИННЫХ постов канала (для показа при разметке)."""
    sample = _sample_channel(conn, channel_id, SAMPLE_SIZE)
    posts = [m.text.strip() for m in sample if MIN_POST_LEN <= len(m.text.strip()) <= MAX_POST_LEN]
    return posts[:POSTS_PER_CHANNEL]


def _emoji_trash_score(cf: ChannelFeatures) -> float:
    """Загаженность эмодзи (0..1): плотность в постах + эмодзи в названии.

    Переиспользует уже посчитанные cf.emoji_density_mean (эмодзи на 100 симв) и
    cf.title_emoji_count. Нормальные каналы ≈0, инфобиз/спам-фермы льют эмодзи."""
    # emoji_density_mean обычно 0..3 (эмодзи/100симв); 2.0 уже частокол → нормируем к 1.
    dens = min(cf.emoji_density_mean / 2.0, 1.0)
    # эмодзи в названии: 0 норм, 3+ частокол → нормируем к 1.
    title = min(cf.title_emoji_count / 3.0, 1.0)
    return round(0.7 * dens + 0.3 * title, 3)


def _make_item(cf: ChannelFeatures, posts: list[str]) -> dict:
    """Один элемент сессии: канал + посты + скрытые машинные фичи + пустая метка."""
    return {
        "channel_id": cf.channel_id,
        "title": cf.title,
        "username": cf.username,
        "n_total": cf.n_total,
        "posts": posts,
        "features": {
            "flag_count": cf.flag_count,
            "flag_clean_brands": cf.flag_clean_brands,
            "flag_listy": cf.flag_listy,
            "flag_formatted": cf.flag_formatted,
            "flag_random_username": cf.flag_random_username,
            "brand_gap": cf.brand_gap,
            "tag": cf.tag,
            "emoji_density_mean": round(cf.emoji_density_mean, 3),
            "title_emoji_count": cf.title_emoji_count,
            "emoji_trash_score": _emoji_trash_score(cf),
        },
        "human": None,
    }


def cmd_prepare(args: argparse.Namespace) -> None:
    conn = _connect_ro()
    try:
        ru_counts = _ru_msg_counts(conn, MIN_MSGS)
        names = {cid: (title, username) for cid, title, username, *_ in _all_channels(conn)}
        print(f"Русских каналов с ≥{MIN_MSGS} постов: {len(ru_counts)}. Считаю флаги (молча)…",
              file=sys.stderr)

        # фичи/флаги по всем кандидатам (молча)
        feats: list[ChannelFeatures] = []
        for cid, n_total in ru_counts.items():
            title, username = names.get(cid, ("", ""))
            sample = _sample_channel(conn, cid, SAMPLE_SIZE)
            cf = compute_channel_features(cid, title, username, n_total, sample)
            feats.append(cf)

        picked = _stratified_pick(feats, args.n)

        items = []
        for cf in picked:
            posts = _channel_posts(conn, cf.channel_id)
            if len(posts) < 5:                  # мало длинных русских постов — пропускаем
                continue
            items.append(_make_item(cf, posts))
    finally:
        conn.close()

    if not items:
        raise SystemExit("Не удалось набрать каналы с длинными русскими постами")

    n_susp = sum(1 for it in items if it["features"]["flag_count"] >= 1)
    SESSION.write_text(json.dumps({"items": items}, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Готово. Сессия: {SESSION.name} ({len(items)} каналов: "
          f"{n_susp} подозреваемых + {len(items) - n_susp} чистых).", file=sys.stderr)
    print("Теперь запусти разметку:  ! python ai_detect_tool/channel_eval.py label", file=sys.stderr)


def cmd_add_value(args: argparse.Namespace) -> None:
    """Дозаписать в сессию каналы-кандидаты в value: 0 флагов + много pre-2022 постов.

    pre-2022 = заведомо живые авторы (до ChatGPT). Это обогащает эталон «нормальными»
    каналами, которых после стратификации почти не было (value=1). Уже размеченные
    каналы НЕ трогаются — только добавляются новые.
    """
    if not SESSION.exists():
        raise SystemExit("Нет сессии. Сначала: channel_eval.py prepare")
    data = json.loads(SESSION.read_text(encoding="utf-8"))
    items = data["items"]
    have = {it["channel_id"] for it in items}

    conn = _connect_ro()
    try:
        ru_counts = _ru_msg_counts(conn, MIN_MSGS)
        names = {cid: (title, username) for cid, title, username, *_ in _all_channels(conn)}
        print(f"Ищу {args.n} каналов без флагов с высокой долей pre-2022 постов…", file=sys.stderr)

        # для каждого кандидата без флагов считаем долю pre-2022 (живые авторы)
        cands: list[tuple[float, ChannelFeatures]] = []
        for cid, n_total in ru_counts.items():
            if cid in have:
                continue
            title, username = names.get(cid, ("", ""))
            sample = _sample_channel(conn, cid, SAMPLE_SIZE)
            cf = compute_channel_features(cid, title, username, n_total, sample)
            if cf.flag_count >= 1 or cf.tag:        # нужны чистые, без флагов и без ad/repost
                continue
            if cf.pre2022_ratio <= 0.0:
                continue
            cands.append((cf.pre2022_ratio, cf))

        # сортируем по доле pre-2022 (по убыванию) — самые «заведомо живые» сверху
        cands.sort(key=lambda x: (-x[0], x[1].channel_id))

        added = []
        for _ratio, cf in cands:
            if len(added) >= args.n:
                break
            posts = _channel_posts(conn, cf.channel_id)
            if len(posts) < 5:
                continue
            it = _make_item(cf, posts)
            items.append(it)
            added.append((cf, _ratio))
    finally:
        conn.close()

    if not added:
        raise SystemExit("Не нашлось новых чистых каналов с pre-2022 постами")

    SESSION.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Добавлено {len(added)} каналов-кандидатов в value (всего в сессии: {len(items)}).",
          file=sys.stderr)
    for cf, ratio in added:
        name = (cf.title or cf.username or str(cf.channel_id))[:40]
        print(f"  pre2022={ratio:.0%}  {name}", file=sys.stderr)
    print("Доразметь новые:  ! python ai_detect_tool/channel_eval.py label", file=sys.stderr)


# ---------------------------------------------------------------------------
# prepare-all — расширить сессию до ВСЕХ каналов с читаемым контентом (любой язык)
# ---------------------------------------------------------------------------

PREPARE_ALL_MIN_POSTS = 5    # каналу нужно ≥5 длинных постов, иначе судить не на чем


def _channel_posts_anylang(conn, channel_id: int) -> list[str]:
    """До 7 ДЛИННЫХ постов канала ЛЮБОГО языка (судья многоязычен).

    В отличие от _channel_posts (русская защита через _sample_channel/_is_russian),
    здесь язык НЕ фильтруем — иначе теряем ~129 нерусских каналов. Страйд по
    message_id (тот же приём, что в _sample_channel: индекс без TEMP B-TREE)."""
    lo, hi, total = conn.execute(
        "SELECT MIN(message_id), MAX(message_id), COUNT(*) FROM messages "
        "WHERE channel_id=? AND message_kind='regular' AND text IS NOT NULL "
        "AND LENGTH(text) BETWEEN ? AND ?",
        (channel_id, MIN_POST_LEN, MAX_POST_LEN),
    ).fetchone()
    if not total or lo is None:
        return []
    want = POSTS_PER_CHANNEL
    step = max(1, (hi - lo) // max(want * 3, 1))
    out: list[str] = []
    seen: set[str] = set()
    cursor = lo
    while cursor <= hi and len(out) < want:
        row = conn.execute(
            "SELECT text FROM messages WHERE channel_id=? AND message_id>=? "
            "AND message_kind='regular' AND text IS NOT NULL "
            "AND LENGTH(text) BETWEEN ? AND ? ORDER BY message_id LIMIT 1",
            (channel_id, cursor, MIN_POST_LEN, MAX_POST_LEN),
        ).fetchone()
        cursor += step
        if not row:
            continue
        t = (row[0] or "").strip()
        if t[:50] in seen:
            continue
        seen.add(t[:50])
        out.append(t)
    return out


def cmd_prepare_all(args: argparse.Namespace) -> None:
    """Расширить сессию до ВСЕХ каналов с ≥5 длинных постов (любой язык).

    Прод-прогон: набираем всех судимых (453), уже существующие НЕ пересобираем."""
    data = {"items": []}
    if SESSION.exists():
        data = json.loads(SESSION.read_text(encoding="utf-8"))
    items = data["items"]
    have = {it["channel_id"] for it in items}

    conn = _connect_ro()
    try:
        channels = _all_channels(conn)
        ru_counts = _ru_msg_counts(conn, MIN_MSGS)   # для русских — полные текстовые фичи
        print(f"Всего каналов: {len(channels)}. Набираю с ≥{PREPARE_ALL_MIN_POSTS} длинных постов…",
              file=sys.stderr)
        added = skipped = 0
        for i, (cid, title, username, *_rest) in enumerate(channels, 1):
            if cid in have:
                continue
            posts = _channel_posts_anylang(conn, cid)
            if len(posts) < PREPARE_ALL_MIN_POSTS:
                skipped += 1
                continue
            # для русских — полные фичи (brand_gap и т.д.); для прочих — name-фичи
            n_ru = ru_counts.get(cid, 0)
            if n_ru >= MIN_MSGS:
                sample = _sample_channel(conn, cid, SAMPLE_SIZE)
                cf = compute_channel_features(cid, title, username, n_ru, sample)
            else:
                cf = compute_channel_features(cid, title, username, 0, [])
            items.append(_make_item(cf, posts))
            added += 1
            if i % 100 == 0:
                print(f"  просмотрено {i}/{len(channels)}, добавлено {added}", file=sys.stderr)
    finally:
        conn.close()

    SESSION.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Добавлено {added} каналов (пропущено без контента: {skipped}). "
          f"Всего в сессии: {len(items)}.", file=sys.stderr)


# ---------------------------------------------------------------------------
# export — итоговый CSV-рейтинг (вердикты судьи + фичи)
# ---------------------------------------------------------------------------

def cmd_export(args: argparse.Namespace) -> None:
    """Выгрузить итоговый рейтинг каналов в CSV. Сортировка: useless сверху."""
    import csv
    if not SESSION.exists():
        raise SystemExit("Нет сессии.")
    data = json.loads(SESSION.read_text(encoding="utf-8"))
    items = data["items"]

    genre_order = {"infobiz": 0, "ad": 1, "copy": 2, "aggregator": 3, "original": 4}

    def sort_key(it: dict) -> tuple:
        lv = it.get("llm") or {}
        useful = lv.get("useful")
        u_rank = 0 if useful == "useless" else (1 if useful == "useful" else 2)
        g_rank = genre_order.get(lv.get("genre"), 9)
        return (u_rank, g_rank, -(lv.get("confidence") or 0))

    rows = sorted(items, key=sort_key)
    cols = ["channel_id", "title", "username", "useful", "genre", "confidence", "reason",
            "emoji_trash_score", "flag_count", "n_total"]
    with args.out.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=cols)
        w.writeheader()
        for it in rows:
            lv = it.get("llm") or {}
            feat = it.get("features") or {}
            w.writerow({
                "channel_id": it["channel_id"],
                "title": it.get("title", ""),
                "username": it.get("username", ""),
                "useful": lv.get("useful", ""),
                "genre": lv.get("genre", ""),
                "confidence": lv.get("confidence", ""),
                "reason": lv.get("reason", ""),
                "emoji_trash_score": feat.get("emoji_trash_score", ""),
                "flag_count": feat.get("flag_count", ""),
                "n_total": it.get("n_total", ""),
            })
    judged = sum(1 for it in items if (it.get("llm") or {}).get("useful"))
    print(f"CSV: {args.out} ({len(rows)} каналов, из них с вердиктом судьи: {judged})", file=sys.stderr)


# ---------------------------------------------------------------------------
# remap — авто-маппинг старых меток (slop/value/mixed/ad) в две новые оси
# ---------------------------------------------------------------------------

# slop≠useless автоматически (ключевая мысль пользователя) — спорные → needs_review.
_OLD_TO_NEW = {
    "value": {"useful": "useful", "genre": "original"},
    "ad": {"useful": None, "genre": "ad"},          # польза рекламы — доспросить
    "slop": {"useful": "useless", "genre": "original", "needs_review": True},
    "mixed": {"needs_review": True},                 # спорные — переразметить вручную
}


def cmd_remap(args: argparse.Namespace) -> None:
    """Перевести старые строковые метки human в новый формат {useful, genre}.

    slop НЕ маппится автоматически в useless без пометки needs_review — это разные
    оси (полезный AI-обзор ≠ мусор). Спорные (mixed/ad/slop) помечаются для ручной
    доразметки через label (он переспросит каналы с needs_review)."""
    if not SESSION.exists():
        raise SystemExit("Нет сессии.")
    data = json.loads(SESSION.read_text(encoding="utf-8"))
    mapped = review = 0
    for it in data["items"]:
        hum = it.get("human")
        if not isinstance(hum, str):     # уже новый формат или пусто — пропускаем
            continue
        rule = _OLD_TO_NEW.get(hum)
        if not rule or "useful" not in rule or rule.get("useful") is None or rule.get("needs_review"):
            it["human"] = None           # требует ручной доразметки
            it["_old_label"] = hum
            review += 1
        else:
            it["human"] = {"useful": rule["useful"], "genre": rule["genre"]}
            it["_old_label"] = hum
            mapped += 1
    SESSION.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Авто-смапплено: {mapped}.  Требуют ручной доразметки (slop/mixed/ad): {review}.",
          file=sys.stderr)
    print("Доразметь спорные:  ! python ai_detect_tool/channel_eval.py label", file=sys.stderr)


# ---------------------------------------------------------------------------
# 2. label — слепая разметка (название/флаги скрыты)
# ---------------------------------------------------------------------------

def _clear() -> None:
    """Очистить экран терминала между каналами (предыдущий канал исчезает)."""
    if sys.stdout.isatty():
        os.system("cls" if os.name == "nt" else "clear")
    else:
        print("\n" * 3)   # не tty (пайп/тест) — просто отступ


def _ask(prompt: str, valid: dict[str, str], extra: dict[str, str]) -> str | None:
    """Спросить один символ из valid|extra. Возвращает значение valid, либо спец-код extra."""
    keys = list(valid) + list(extra)
    hint = "  ".join(f"{k}={v}" for k, v in {**valid, **extra}.items())
    while True:
        ans = input(f"> {prompt} [{'/'.join(keys)}]: ").strip().lower()
        if ans in valid:
            return valid[ans]
        if ans in extra:
            return extra[ans]            # 'skip' / 'quit'
        print(f"  ⚠ Введи: {hint}")


def cmd_label(args: argparse.Namespace) -> None:
    if not SESSION.exists():
        raise SystemExit("Нет сессии. Сначала: channel_eval.py prepare")
    data = json.loads(SESSION.read_text(encoding="utf-8"))
    items = data["items"]

    print("\n" + "=" * 70)
    print("СЛЕПАЯ РАЗМЕТКА КАНАЛОВ — название и машинные флаги СКРЫТЫ.")
    print("Две оси: сначала ПОЛЬЗА, потом ЖАНР. Оцениваешь только по постам.")
    print("=" * 70)

    start = next((i for i, it in enumerate(items) if it.get("human") is None), len(items))
    if start == len(items):
        print("\nВсё уже размечено. Запусти report.")
        return

    extra = {"p": "skip", "q": "quit"}
    for i in range(start, len(items)):
        it = items[i]
        _clear()
        print(f"{'='*70}\nКАНАЛ [{i+1}/{len(items)}]  (постов в показе: {len(it['posts'])})\n{'='*70}")
        for j, post in enumerate(it["posts"], 1):
            print(f"\n── пост {j} {'─'*60}")
            print(post)
        print("\n" + "─" * 70)
        print("  ОСЬ 1 — ПОЛЬЗА:  [u] useful (есть ценность)   [x] useless (пусто/вода)")
        print("  ОСЬ 2 — ЖАНР:    [a] ad   [i] infobiz (курсы/успех)   "
              "[g] aggregator (куратор-дайджест)   [c] copy (клон 1-в-1)   [o] original (свой контент)")
        print("  [p] пропустить    [q] выйти и сохранить")

        useful = _ask("ПОЛЬЗА", USEFUL_LABELS, extra)
        if useful == "quit":
            break
        if useful == "skip":
            continue
        genre = _ask("ЖАНР", GENRE_LABELS, extra)
        if genre == "quit":
            break
        if genre == "skip":
            continue
        it["human"] = {"useful": useful, "genre": genre}
        SESSION.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

    done = sum(1 for it in items if it.get("human") is not None)
    print(f"\nСохранено. Размечено {done}/{len(items)} каналов.")
    print("Отчёт:  ! python ai_detect_tool/channel_eval.py report")


# ---------------------------------------------------------------------------
# 2b. llm — channel-level вердикт LLM по набору постов (закрывает recall-дыру)
# ---------------------------------------------------------------------------

_USEFUL_VALUES = {"useful", "useless"}
_GENRE_VALUES = {"ad", "infobiz", "aggregator", "copy", "original"}


def _parse_llm(raw: str) -> dict:
    """Best-effort парсинг JSON-ответа LLM по двум осям (useful + genre)."""
    raw = raw.strip()
    start = raw.find("{")
    end = raw.rfind("}")
    if start >= 0 and end > start:
        try:
            obj = json.loads(raw[start:end + 1])
            useful = str(obj.get("useful", "")).lower().strip()
            genre = str(obj.get("genre", "")).lower().strip()
            if useful in _USEFUL_VALUES or genre in _GENRE_VALUES:
                return {
                    "useful": useful if useful in _USEFUL_VALUES else None,
                    "genre": genre if genre in _GENRE_VALUES else None,
                    "confidence": float(obj.get("confidence", 0.0) or 0.0),
                    "reason": str(obj.get("reason", ""))[:200],
                }
        except (ValueError, TypeError):
            pass
    # фолбэк: ищем оси прямым вхождением
    low = raw.lower()
    useful = next((v for v in _USEFUL_VALUES if v in low), None)
    genre = next((v for v in _GENRE_VALUES if v in low), None)
    if useful or genre:
        return {"useful": useful, "genre": genre, "confidence": 0.0, "reason": "parsed-from-text"}
    return {"useful": None, "genre": None, "confidence": 0.0, "reason": "error: " + raw[:150]}


async def _judge_channels(items: list[dict], model: str | None) -> None:
    """Прогнать каждый канал через LLM, записать результат в it['llm']."""
    import logging
    # httpx логирует каждый POST на INFO — глушим, чтобы прогресс читался чисто.
    logging.getLogger("httpx").setLevel(logging.WARNING)
    judge = ai_detect.LlmJudge(model=model)
    try:
        client = await judge._get_client()
    except RuntimeError as e:
        raise SystemExit(
            f"LLM не настроен ({e}).\n"
            "Запусти Ollama (`ollama serve`) и задай OLLAMA_BASE, например:\n"
            "  ! OLLAMA_BASE=http://localhost:11434 python ai_detect_tool/channel_eval.py llm"
        )
    for i, it in enumerate(items, 1):
        posts = "\n\n".join(f"[пост {j}] {p[:800]}" for j, p in enumerate(it["posts"], 1))
        prompt = LLM_CHANNEL_PROMPT.format(posts=posts)
        try:
            resp = await client.chat.completions.create(
                model=judge._model,
                messages=[{"role": "user", "content": prompt}],
                max_tokens=400,
                temperature=0.1,
            )
            it["llm"] = _parse_llm(resp.choices[0].message.content or "")
        except Exception as e:                       # noqa: BLE001 — best-effort, копим ошибки
            it["llm"] = {"useful": None, "genre": None, "confidence": 0.0, "reason": str(e)[:150]}
        lv = it["llm"]
        sys.stderr.write(f"\r  LLM: {i}/{len(items)} ({lv.get('useful') or '?'}/{lv.get('genre') or '?'})   ")
        sys.stderr.flush()
    sys.stderr.write("\n")


def cmd_llm(args: argparse.Namespace) -> None:
    if not SESSION.exists():
        raise SystemExit("Нет сессии. Сначала: channel_eval.py prepare")
    data = json.loads(SESSION.read_text(encoding="utf-8"))
    items = data["items"]
    todo = items if args.force else [it for it in items if not it.get("llm")]
    if not todo:
        print("Все каналы уже оценены LLM (используй --force для пересчёта).", file=sys.stderr)
        return
    print(f"Оцениваю {len(todo)} каналов через LLM (channel-level)…", file=sys.stderr)
    asyncio.run(_judge_channels(todo, args.model))
    SESSION.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    done = sum(1 for it in items if it.get("llm"))
    print(f"Готово. LLM-вердиктов: {done}/{len(items)}. Отчёт: channel_eval.py report", file=sys.stderr)


# ---------------------------------------------------------------------------
# 3. report — precision/recall флагов против разметки
# ---------------------------------------------------------------------------

def _flag_marks(feat: dict) -> str:
    return "".join(letter if feat.get(attr) else "·" for attr, letter in FLAG_ATTRS)


def _prf(tp: int, fn: int, fp: int) -> tuple[float, float]:
    prec = tp / (tp + fp) if (tp + fp) else 0.0
    rec = tp / (tp + fn) if (tp + fn) else 0.0
    return prec, rec


def cmd_report(args: argparse.Namespace) -> None:
    if not SESSION.exists():
        raise SystemExit("Нет сессии.")
    data = json.loads(SESSION.read_text(encoding="utf-8"))
    items = [it for it in data["items"] if isinstance(it.get("human"), dict)]
    if not items:
        raise SystemExit("Нет размеченных каналов (новый формат). Сначала: channel_eval.py label / remap")

    has_llm = any(it.get("llm") for it in items)

    print("\n" + "=" * 88)
    print(f"ОТЧЁТ — {len(items)} каналов размечено по двум осям (твоя разметка = эталон)")
    print("=" * 88)

    # per-channel таблица (название раскрывается ТОЛЬКО в отчёте)
    llm_hdr = f"  {'LLM(польза/жанр)':<20}" if has_llm else ""
    print(f"\n{'польза':<8} {'жанр':<11} {'флаги':<6} {'flag#':<6}{llm_hdr} канал")
    print("-" * 88)
    for it in sorted(items, key=lambda x: (x["human"]["useful"], x["human"]["genre"])):
        feat, hum = it["features"], it["human"]
        name = (it["title"] or it["username"] or str(it["channel_id"]))[:34]
        llm_col = ""
        if has_llm:
            lv = it.get("llm") or {}
            lu, lg = lv.get("useful") or "—", lv.get("genre") or "—"
            um = "✓" if lu == hum["useful"] else "✗"
            gm = "✓" if lg == hum["genre"] else "✗"
            llm_col = f"  {lu+'/'+lg:<16} {um}{gm}"
        print(f"{hum['useful']:<8} {hum['genre']:<11} [{_flag_marks(feat)}] "
              f"{feat['flag_count']:<6}{llm_col} {name}")

    n_useless = sum(1 for it in items if it["human"]["useful"] == "useless")
    n_useful = sum(1 for it in items if it["human"]["useful"] == "useful")

    # ── ФЛАГИ как детектор USELESS (главная цель — найти бесполезные) ──
    print(f"\n{'─'*88}")
    print(f"ФЛАГИ как детектор USELESS (useless={n_useless}, useful={n_useful})")
    print(f"{'─'*88}")
    if n_useless and n_useful:
        detectors = [(letter + " " + attr, (lambda a: (lambda f: f.get(a)))(attr))
                     for attr, letter in FLAG_ATTRS]
        detectors.append(("flag_count≥1", lambda f: f.get("flag_count", 0) >= 1))
        print(f"\n{'детектор':<22} {'prec':>6} {'rec':>6}   conf (TP/FN/FP/TN)")
        for name, pred in detectors:
            tp = sum(1 for it in items if it["human"]["useful"] == "useless" and pred(it["features"]))
            fn = sum(1 for it in items if it["human"]["useful"] == "useless" and not pred(it["features"]))
            fp = sum(1 for it in items if it["human"]["useful"] == "useful" and pred(it["features"]))
            tn = sum(1 for it in items if it["human"]["useful"] == "useful" and not pred(it["features"]))
            prec, rec = _prf(tp, fn, fp)
            print(f"{name:<22} {prec:>6.2f} {rec:>6.2f}   {tp}/{fn}/{fp}/{tn}")
    else:
        print("⚠️  Нет обоих классов useful/useless — метрики неинформативны.")

    # ── LLM-судья по ДВУМ осям ──
    if has_llm:
        ju = [it for it in items if (it.get("llm") or {}).get("useful") in _USEFUL_VALUES]
        jg = [it for it in items if (it.get("llm") or {}).get("genre") in _GENRE_VALUES]
        print(f"\n{'─'*88}")
        print("LLM-СУДЬЯ vs твоя разметка")
        print(f"{'─'*88}")
        # ось полезности
        if ju:
            acc = sum(1 for it in ju if it["llm"]["useful"] == it["human"]["useful"]) / len(ju)
            tp = sum(1 for it in ju if it["human"]["useful"] == "useless" and it["llm"]["useful"] == "useless")
            fn = sum(1 for it in ju if it["human"]["useful"] == "useless" and it["llm"]["useful"] != "useless")
            fp = sum(1 for it in ju if it["human"]["useful"] == "useful" and it["llm"]["useful"] == "useless")
            prec, rec = _prf(tp, fn, fp)
            print(f"  ПОЛЬЗА: accuracy {acc:.2f} ({len(ju)})  |  useless: prec {prec:.2f} rec {rec:.2f} "
                  f"(TP {tp} FN {fn} FP {fp})")
        # ось жанра
        if jg:
            gacc = sum(1 for it in jg if it["llm"]["genre"] == it["human"]["genre"]) / len(jg)
            print(f"  ЖАНР:   accuracy {gacc:.2f} ({len(jg)})")
            # путаница по жанрам
            for g in GENRE_LABELS.values():
                hum_g = [it for it in jg if it["human"]["genre"] == g]
                if hum_g:
                    right = sum(1 for it in hum_g if it["llm"]["genre"] == g)
                    print(f"     {g:<11}: {right}/{len(hum_g)} верно")
        errs = [it for it in items if (it.get("llm") or {}).get("useful") is None
                and (it.get("llm") or {}).get("genre") is None]
        if errs:
            print(f"\n  ⚠ LLM не распарсил: {len(errs)}")

    # распределение
    du = {v: sum(1 for it in items if it["human"]["useful"] == v) for v in USEFUL_LABELS.values()}
    dg = {v: sum(1 for it in items if it["human"]["genre"] == v) for v in GENRE_LABELS.values()}
    print("\nПольза: " + "  ".join(f"{k}={v}" for k, v in du.items()))
    print("Жанр:   " + "  ".join(f"{k}={v}" for k, v in dg.items()))
    print()


def main() -> None:
    ap = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="command")
    p = sub.add_parser("prepare")
    p.add_argument("--n", type=int, default=20)
    pv = sub.add_parser("add-value")
    pv.add_argument("--n", type=int, default=15, help="сколько value-кандидатов добавить")
    sub.add_parser("prepare-all")
    pe = sub.add_parser("export")
    pe.add_argument("--out", type=Path, default=TOOL_DIR / "channel_rating.csv")
    sub.add_parser("remap")
    sub.add_parser("label")
    pl = sub.add_parser("llm")
    pl.add_argument("--model", default=None, help="модель Ollama (default gemma3:12b)")
    pl.add_argument("--force", action="store_true", help="пересчитать даже уже оценённые каналы")
    sub.add_parser("report")
    args = ap.parse_args()
    if args.command == "prepare":
        cmd_prepare(args)
    elif args.command == "add-value":
        cmd_add_value(args)
    elif args.command == "prepare-all":
        cmd_prepare_all(args)
    elif args.command == "export":
        cmd_export(args)
    elif args.command == "remap":
        cmd_remap(args)
    elif args.command == "label":
        cmd_label(args)
    elif args.command == "llm":
        cmd_llm(args)
    elif args.command == "report":
        cmd_report(args)
    else:
        ap.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
