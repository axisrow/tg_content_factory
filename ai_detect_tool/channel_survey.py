#!/usr/bin/env python3
"""Этап 0 — разведка каналов: рейтинг по AI-slop подозрению (read-only).

Проходит по всем каналам с достаточным числом русских постов, берёт случайный
семпл по всей истории канала, считает дешёвые агрегатные фичи (channel_features.py)
и выдаёт CSV-рейтинг, отсортированный по slop_suspect. Без разметки и без LLM.

Главная цель — автоматически находить каналы-мусорки (AI-slop), чтобы их
отфильтровать. Реклама помечается отдельным тегом `ad`, репостные каналы — `repost`.

Использование:
    python ai_detect_tool/channel_survey.py --min-msgs 50 --sample 120 \
        --out ai_detect_tool/channel_survey.csv
    python ai_detect_tool/channel_survey.py --calibrate

Калибровка (--calibrate): считает slop_suspect ТОЛЬКО на постах до декабря 2022
(ChatGPT вышел 30.11.2022 → заведомо человеческие) и показывает, какая доля их
ошибочно метится как slop при разных порогах. Это бесплатный контроль ложных
срабатываний без ручной разметки.

Основная БД открывается ТОЛЬКО НА ЧТЕНИЕ; ничего в неё не пишется.
"""

from __future__ import annotations

import argparse
import csv
import logging
import sqlite3
import sys
from pathlib import Path

from channel_features import (
    CSV_COLUMNS,
    PRE2022_CUTOFF,
    ChannelFeatures,
    SampleMsg,
    compute_channel_features,
)
from human_eval import _is_russian  # кириллическая защита поверх detected_lang='ru'

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("channel_survey")

TOOL_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = TOOL_DIR.parent
MAIN_DB = PROJECT_ROOT / "data" / "tg_search.db"
DEFAULT_OUT = TOOL_DIR / "channel_survey.csv"


def _connect_ro() -> sqlite3.Connection:
    if not MAIN_DB.exists():
        raise SystemExit(f"Основная БД не найдена: {MAIN_DB}")
    return sqlite3.connect(f"file:{MAIN_DB}?mode=ro", uri=True)


def _candidate_channels(conn: sqlite3.Connection, min_msgs: int) -> list[tuple]:
    """Каналы с ≥ min_msgs русских regular-постов + название/username из channels."""
    rows = conn.execute(
        """
        SELECT m.channel_id, COUNT(*) AS n,
               COALESCE(c.title, ''), COALESCE(c.username, '')
        FROM messages m
        LEFT JOIN channels c ON m.channel_id = c.channel_id
        WHERE m.detected_lang = 'ru'
          AND m.message_kind = 'regular'
          AND m.text IS NOT NULL
        GROUP BY m.channel_id
        HAVING COUNT(*) >= ?
        ORDER BY n DESC
        """,
        (min_msgs,),
    ).fetchall()
    return rows


def _sample_channel(
    conn: sqlite3.Connection,
    channel_id: int,
    sample_size: int,
    pre2022_only: bool = False,
) -> list[SampleMsg]:
    """Случайный семпл русских постов канала по всему диапазону message_id (страйд).

    ВАЖНО про производительность (два подвоха, оба исправлены):
    1. НЕ фильтруем `detected_lang='ru'` в SQL — иначе SQLite берёт
       idx_messages_detected_lang и сканирует все 2млн русских строк (≈50с/запрос).
    2. Сортируем/страйдим по `message_id` (а НЕ по rowid `id`) — autoindex
       (channel_id, message_id) даёт упорядоченность БЕЗ TEMP B-TREE. Сортировка по
       `id` заставляла SQLite складывать десятки тыс. строк во временный B-tree
       на каждом шаге (≈240с на канал-гигант). По message_id весь проход — 0.05с.
    Язык фильтруем в Python по полю detected_lang/кириллице из SELECT.
    """
    where_pre = f"AND m.date < '{PRE2022_CUTOFF}'" if pre2022_only else ""
    bounds = conn.execute(
        f"""
        SELECT MIN(m.message_id), MAX(m.message_id), COUNT(*)
        FROM messages m
        WHERE m.channel_id = ?
          AND m.message_kind = 'regular' AND m.text IS NOT NULL {where_pre}
        """,
        (channel_id,),
    ).fetchone()
    lo, hi, total = bounds
    if not total or lo is None:
        return []

    # страйд по message_id-диапазону: равномерно по всей истории.
    # берём с запасом (×4): часть отсеется по языку (_is_russian) и дублям.
    want = sample_size * 4
    step = max(1, (hi - lo) // max(want, 1))
    out: list[SampleMsg] = []
    seen_prefix: set[str] = set()
    cursor = lo
    while cursor <= hi and len(out) < want:
        row = conn.execute(
            f"""
            SELECT m.text, m.date, m.forward_from_channel_id, m.views,
                   m.forwards, m.reply_count, m.reactions_json, m.detected_lang
            FROM messages m
            WHERE m.channel_id = ? AND m.message_id >= ?
              AND m.message_kind = 'regular' AND m.text IS NOT NULL {where_pre}
            ORDER BY m.message_id LIMIT 1
            """,
            (channel_id, cursor),
        ).fetchone()
        cursor += step
        if not row:
            continue
        text = row[0] or ""
        # язык фильтруем в Python (не в SQL — иначе медленный индекс).
        # _is_russian — строгая кириллическая защита (отсекает немецкий/CJK даже при lang='ru').
        if not _is_russian(text):
            continue
        prefix = text[:50]
        if prefix in seen_prefix:
            continue
        seen_prefix.add(prefix)
        out.append(
            SampleMsg(
                text=text,
                date=row[1],
                forward_from_channel_id=row[2],
                views=row[3],
                forwards=row[4],
                reply_count=row[5],
                reactions_json=row[6],
            )
        )
    return out[:sample_size]


def _survey(min_msgs: int, sample_size: int, out_path: Path) -> list[ChannelFeatures]:
    conn = _connect_ro()
    try:
        candidates = _candidate_channels(conn, min_msgs)
        logger.info("Каналов-кандидатов (≥%d ru-постов): %d", min_msgs, len(candidates))
        results: list[ChannelFeatures] = []
        for i, (cid, n_total, title, username) in enumerate(candidates, 1):
            sample = _sample_channel(conn, cid, sample_size)
            cf = compute_channel_features(cid, title, username, n_total, sample)
            results.append(cf)
            if i % 25 == 0:
                logger.info("  обработано %d/%d", i, len(candidates))
    finally:
        conn.close()

    _write_csv(results, out_path)
    _print_console(results)
    return results


def _write_csv(results: list[ChannelFeatures], out_path: Path) -> None:
    # сортировка: по числу флагов (грубое подозрение), ad/repost — в конец
    def sort_key(cf: ChannelFeatures) -> tuple:
        rank = -1 if cf.tag in ("ad", "repost") else cf.flag_count
        return (-rank, cf.channel_id)

    ordered = sorted(results, key=sort_key)
    with out_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=CSV_COLUMNS)
        writer.writeheader()
        for cf in ordered:
            writer.writerow(cf.as_row())
    logger.info("CSV записан: %s (%d каналов)", out_path, len(ordered))


def _print_console(results: list[ChannelFeatures]) -> None:
    rated = [cf for cf in results if cf.tag not in ("ad", "repost")]
    rated.sort(key=lambda cf: cf.flag_count, reverse=True)
    ads = [cf for cf in results if cf.tag == "ad"]
    reposts = [cf for cf in results if cf.tag == "repost"]

    def line(cf: ChannelFeatures) -> str:
        name = (cf.title or cf.username or str(cf.channel_id))[:32]
        # надёжные флаги — заглавными (B/L/F/R/E), наблюдаемые — строчными (t/e/s/u)
        marks = "".join([
            "B" if cf.flag_clean_brands else "·",
            "L" if cf.flag_listy else "·",
            "F" if cf.flag_formatted else "·",
            "R" if cf.flag_random_username else "·",
            "E" if cf.flag_emoji_fence else "·",
            "t" if cf.flag_template_title else "·",
            "e" if cf.flag_low_eng else "·",
            "s" if cf.flag_no_slang else "·",
            "u" if cf.flag_uniform else "·",
        ])
        gap = f"{cf.brand_gap:+.2f}" if cf.brand_gap is not None else "  — "
        return f"  {cf.flag_count}  [{marks}]  {name:<32}  gap={gap} n={cf.n_total}"

    print(f"\n{'='*78}\nКАНДИДАТЫ В AI-SLOP по НАДЁЖНЫМ флагам (B/L/F/R)\n{'='*78}")
    print("  ЗАГЛАВНЫЕ (в счёте): B=чистые-бренды L=списки F=форматирование R=случайный-username")
    print("  строчные (наблюд., не в счёте): E=эмодзи-частокол t=template-title")
    print("                                  e=low-eng s=без-сленга u=однообразие")
    for cf in rated:
        if cf.flag_count >= 1:
            print(line(cf))

    clean = [cf for cf in rated if cf.flag_count == 0]
    print(f"\n{'='*78}\n«ЧИСТЫЕ» (0 флагов): {len(clean)} каналов (первые 25)\n{'='*78}")
    for cf in clean[:25]:
        print(line(cf))

    print(f"\nРеклама (tag=ad): {len(ads)}   Репостные (tag=repost): {len(reposts)}")
    print(f"Всего каналов в рейтинге: {len(rated)}")


_FLAG_NAMES = [
    ("flag_clean_brands", "чистые-бренды"),
    ("flag_listy", "списки"),
    ("flag_formatted", "форматирование"),
    ("flag_low_eng", "low-engagement"),
    ("flag_no_slang", "без-сленга"),
    ("flag_uniform", "однообразие"),
]


def _calibrate(min_msgs: int, sample_size: int) -> None:
    """Контроль ложных срабатываний на заведомо-человеческих pre-2022 постах.

    Каждый флаг, который часто срабатывает на pre-2022 (заведомо человек), —
    ненадёжный признак slop. Идеал: флаги почти не загораются на этих каналах.
    """
    conn = _connect_ro()
    try:
        candidates = _candidate_channels(conn, min_msgs)
        feats: list = []
        for cid, n_total, title, username in candidates:
            sample = _sample_channel(conn, cid, sample_size, pre2022_only=True)
            if len(sample) < 10:        # мало pre-2022 постов — пропускаем канал
                continue
            cf = compute_channel_features(cid, title, username, n_total, sample)
            if cf.tag in ("ad", "repost"):
                continue
            feats.append(cf)
    finally:
        conn.close()

    if not feats:
        print("Недостаточно pre-2022 русских постов для калибровки.")
        return

    n = len(feats)
    print(f"\n{'='*70}")
    print("КАЛИБРОВКА на заведомо-человеческих постах (date < %s)" % PRE2022_CUTOFF)
    print(f"{'='*70}")
    print(f"Каналов с ≥10 pre-2022 постами: {n}")
    print("\nДоля заведомо-человеческих каналов, на которых ЗАГОРАЕТСЯ флаг")
    print("(чем выше — тем хуже признак: он метит человека):")
    for attr, label in _FLAG_NAMES:
        rate = sum(1 for cf in feats if getattr(cf, attr)) / n
        bar = "█" * round(rate * 50)
        print(f"  {label:<16} {rate*100:5.1f}%  {bar}")
    print("\nРаспределение числа флагов на канал (0 = идеально для человека):")
    for k in range(7):
        cnt = sum(1 for cf in feats if cf.flag_count == k)
        if cnt:
            print(f"  {k} флагов: {cnt:>3} каналов  {'▓'*cnt}")


# ---------------------------------------------------------------------------
# Этап 0+: разведка по всем языкам + аудит фильтра проекта
# ---------------------------------------------------------------------------

# Надёжные slop-флаги фильтра проекта (см. src/filters/criteria.py).
STRONG_SLOP_FLAGS = {"cross_channel_spam", "chat_noise", "suspicious_username", "low_uniqueness"}
# Слабые: метят и ценные каналы (non_cyrillic = любой нерусский, не «мусор»).
WEAK_FLAGS = {"non_cyrillic", "low_subscriber_ratio", "low_subscriber_manual", "manual"}

SUSPECT_OUT = TOOL_DIR / "filter_suspects.csv"
ALL_LANGS_OUT = TOOL_DIR / "channel_survey_all.csv"


def _parse_flags(s: str | None) -> set[str]:
    if not s:
        return set()
    return {p.strip() for p in s.split(",") if p.strip()}


def _all_channels(conn: sqlite3.Connection) -> list[tuple]:
    """Все каналы из channels (быстро, без чтения сообщений)."""
    return conn.execute(
        """
        SELECT channel_id, COALESCE(title, ''), COALESCE(username, ''),
               COALESCE(is_filtered, 0), COALESCE(filter_flags, ''),
               COALESCE(channel_type, '')
        FROM channels
        ORDER BY channel_id
        """
    ).fetchall()


def _ru_msg_counts(conn: sqlite3.Connection, min_msgs: int) -> dict[int, int]:
    """channel_id → число русских regular-постов (только каналы с ≥ min_msgs)."""
    rows = _candidate_channels(conn, min_msgs)
    return {cid: n for cid, n, _t, _u in rows}


def _survey_all_langs(min_msgs: int, sample_size: int, out_path: Path) -> None:
    """Разведка ПО ВСЕМ ЯЗЫКАМ: name-фичи всем каналам, текстовые — только русским."""
    conn = _connect_ro()
    try:
        channels = _all_channels(conn)
        ru_counts = _ru_msg_counts(conn, min_msgs)
        logger.info("Всего каналов: %d, из них русских с ≥%d постов: %d",
                    len(channels), min_msgs, len(ru_counts))
        results: list[ChannelFeatures] = []
        for i, (cid, title, username, _isf, _flags, _ctype) in enumerate(channels, 1):
            n_ru = ru_counts.get(cid, 0)
            if n_ru >= min_msgs:
                sample = _sample_channel(conn, cid, sample_size)
                cf = compute_channel_features(cid, title, username, n_ru, sample)
            else:
                # нерусский / мало русского — только name-фичи (без чтения сообщений)
                cf = compute_channel_features(cid, title, username, 0, [])
            results.append(cf)
            if i % 100 == 0:
                logger.info("  обработано %d/%d", i, len(channels))
    finally:
        conn.close()

    _write_csv(results, out_path)
    _print_console(results)


def _eval_filter(suspect_out: Path) -> None:
    """Аудит фильтра проекта: is_filtered как СЛАБЫЙ эталон slop.

    Делит filter_flags на надёжные slop и ложные. Считает recall нашего
    name-детектора по надёжно-slop каналам и — главное — находит подозреваемые
    ЛОЖНЫЕ срабатывания фильтра (отфильтрован только по слабым флагам + чистое имя).
    """
    conn = _connect_ro()
    try:
        channels = _all_channels(conn)
        rows = []
        for cid, title, username, is_filtered, filter_flags, ctype in channels:
            cf = compute_channel_features(cid, title, username, 0, [])
            flags = _parse_flags(filter_flags)
            has_strong = bool(flags & STRONG_SLOP_FLAGS)
            weak_only = bool(is_filtered) and bool(flags) and not has_strong
            # name_dirty — только НАДЁЖНЫЙ random_username (emoji_fence ловит живых, исключён)
            name_dirty = cf.flag_random_username
            rows.append({
                "cf": cf, "is_filtered": bool(is_filtered), "flags": flags,
                "filter_flags": filter_flags, "channel_type": ctype,
                "has_strong": has_strong, "weak_only": weak_only, "name_dirty": name_dirty,
            })
    finally:
        conn.close()

    total = len(rows)
    n_filtered = sum(1 for r in rows if r["is_filtered"])
    n_strong = sum(1 for r in rows if r["has_strong"])
    n_weak_only = sum(1 for r in rows if r["weak_only"])

    print(f"\n{'='*70}\nАУДИТ ФИЛЬТРА ПРОЕКТА (is_filtered как слабый эталон)\n{'='*70}")
    print(f"Всего каналов: {total}   отфильтровано: {n_filtered}")
    print(f"  с надёжным slop-флагом (strong): {n_strong}")
    print(f"  отфильтровано ТОЛЬКО по слабым флагам (weak-only): {n_weak_only}")

    # recall нашего name-детектора по надёжно-slop каналам
    if n_strong:
        caught = sum(1 for r in rows if r["has_strong"] and r["name_dirty"])
        print(f"\nName-детектор ловит {caught}/{n_strong} ({100*caught/n_strong:.0f}%) "
              "надёжно-slop каналов (recall по реальному мусору)")

    # 2×2 кросс-таблица has_strong × name_dirty
    def cnt(strong: bool, dirty: bool) -> int:
        return sum(1 for r in rows if r["has_strong"] == strong and r["name_dirty"] == dirty)
    print("\n              name_dirty=да  name_dirty=нет")
    print(f"  strong=да       {cnt(True, True):>6}        {cnt(True, False):>6}")
    print(f"  strong=нет      {cnt(False, True):>6}        {cnt(False, False):>6}")

    # ГЛАВНЫЙ выход: подозреваемые ложные срабатывания фильтра
    suspects = [
        r for r in rows
        if r["weak_only"] and not r["name_dirty"] and r["cf"].template_title_score < 0.6
    ]
    suspects.sort(key=lambda r: (
        r["cf"].random_username_score + r["cf"].title_emoji_count,
        r["cf"].channel_id,
    ))
    with suspect_out.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.writer(fh)
        writer.writerow([
            "channel_id", "username", "title", "filter_flags", "channel_type",
            "random_username_score", "template_title_score", "title_emoji_count",
        ])
        for r in suspects:
            cf = r["cf"]
            writer.writerow([
                cf.channel_id, cf.username, cf.title, r["filter_flags"], r["channel_type"],
                round(cf.random_username_score, 3), round(cf.template_title_score, 3),
                cf.title_emoji_count,
            ])
    print(f"\nПодозреваемые ЛОЖНЫЕ срабатывания фильтра: {len(suspects)} → {suspect_out.name}")
    print("(отфильтрованы только по слабым флагам, имя чистое — фильтр мог ошибиться)")
    for r in suspects[:15]:
        cf = r["cf"]
        name = (cf.title or cf.username or str(cf.channel_id))[:36]
        print(f"  @{cf.username:<20} {name:<36} [{r['filter_flags']}]")

    # Бонус: фильтр ПРОПУСТИЛ мусор (наш детектор нашёл, канал не отфильтрован)
    missed = [r for r in rows if r["name_dirty"] and not r["is_filtered"]]
    print(f"\nФильтр ПРОПУСТИЛ (наш name-детектор горит, is_filtered=0): {len(missed)}")
    for r in missed[:15]:
        cf = r["cf"]
        name = (cf.title or cf.username or str(cf.channel_id))[:36]
        print(f"  @{cf.username:<20} {name:<36} ru={cf.random_username_score:.2f} "
              f"emoji={cf.title_emoji_count}")


def main() -> None:
    ap = argparse.ArgumentParser(description="Разведка каналов по AI-slop (Этап 0/0+)")
    ap.add_argument("--min-msgs", type=int, default=50,
                    help="минимум русских regular-постов у канала (default 50)")
    ap.add_argument("--sample", type=int, default=120,
                    help="размер семпла на канал (default 120)")
    ap.add_argument("--out", type=Path, default=DEFAULT_OUT,
                    help="путь к выходному CSV")
    ap.add_argument("--calibrate", action="store_true",
                    help="режим калибровки на pre-2022 человеческих постах")
    ap.add_argument("--all-langs", action="store_true",
                    help="разведка по всем языкам (name-фичи всем, текст — русским)")
    ap.add_argument("--eval-filter", action="store_true",
                    help="аудит фильтра проекта + CSV подозреваемых ложных срабатываний")
    ap.add_argument("--suspect-out", type=Path, default=SUSPECT_OUT,
                    help="путь к CSV подозреваемых ложных срабатываний фильтра")
    args = ap.parse_args()

    if args.eval_filter:
        _eval_filter(args.suspect_out)
    elif args.calibrate:
        _calibrate(args.min_msgs, args.sample)
    elif args.all_langs:
        out = args.out if args.out != DEFAULT_OUT else ALL_LANGS_OUT
        _survey_all_langs(args.min_msgs, args.sample, out)
    else:
        _survey(args.min_msgs, args.sample, args.out)


if __name__ == "__main__":
    sys.exit(main())
