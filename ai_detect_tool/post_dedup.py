#!/usr/bin/env python3
"""Утилита дедупликации постов: находит каналы, постящие ОДИНАКОВЫЙ контент.

Отдельный самостоятельный инструмент (НЕ часть жанровой разметки). Идея
пользователя: дубль текста в БД не говорит, КТО автор, поэтому это не признак
жанра — но факт «канал X и канал Y постят одно и то же» полезен сам по себе для
авто-фильтрации. Считаем нормализованный хэш каждого поста, группируем хэши по
каналам и находим каналы с высокой долей ОБЩИХ постов.

Выход:
  - CSV пар каналов с долей пересечения постов (по хэшам);
  - консоль: топ каналов-дубликатов.

Использование:
    python ai_detect_tool/post_dedup.py [--min-len 120] [--min-shared 5]
        [--min-overlap 0.3] [--out ai_detect_tool/post_dedup.csv]

Основная БД открывается ТОЛЬКО НА ЧТЕНИЕ.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import logging
import re
import sqlite3
import sys
from collections import defaultdict
from pathlib import Path

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("post_dedup")

TOOL_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = TOOL_DIR.parent
MAIN_DB = PROJECT_ROOT / "data" / "tg_search.db"
DEFAULT_OUT = TOOL_DIR / "post_dedup.csv"

# Нормализация: убираем то, что меняется при копипасте, но не меняет суть.
_URL_RE = re.compile(r"https?://\S+|t\.me/\S+|@\w+")
_NON_WORD_RE = re.compile(r"[^\w\s]", re.UNICODE)
_WS_RE = re.compile(r"\s+")


def _connect_ro() -> sqlite3.Connection:
    if not MAIN_DB.exists():
        raise SystemExit(f"Основная БД не найдена: {MAIN_DB}")
    return sqlite3.connect(f"file:{MAIN_DB}?mode=ro", uri=True)


def _normalize(text: str) -> str:
    """Свести текст к форме, устойчивой к мелким правкам при копипасте."""
    t = text.lower()
    t = _URL_RE.sub(" ", t)          # ссылки/упоминания часто подменяют на свои
    t = _NON_WORD_RE.sub(" ", t)     # пунктуация/эмодзи
    t = _WS_RE.sub(" ", t).strip()
    return t


def _post_hash(text: str) -> str | None:
    """Хэш нормализованного текста (None если после нормализации слишком коротко)."""
    norm = _normalize(text)
    if len(norm) < 60:               # слишком короткий нормализованный текст — не считаем
        return None
    return hashlib.sha1(norm.encode("utf-8")).hexdigest()[:16]


def _build_index(conn: sqlite3.Connection, min_len: int) -> tuple[dict, dict]:
    """hash → set(channel_id) и channel_id → число хэшируемых постов.

    Идём по всем regular-постам с текстом ≥ min_len. На 5М строк это пара минут.
    """
    hash_channels: dict[str, set[int]] = defaultdict(set)
    channel_posts: dict[int, int] = defaultdict(int)

    cur = conn.execute(
        "SELECT channel_id, text FROM messages "
        "WHERE message_kind='regular' AND text IS NOT NULL AND LENGTH(text) >= ?",
        (min_len,),
    )
    n = 0
    for channel_id, text in cur:
        h = _post_hash(text or "")
        if h is None:
            continue
        hash_channels[h].add(channel_id)
        channel_posts[channel_id] += 1
        n += 1
        if n % 500_000 == 0:
            logger.info("  обработано постов: %d, уникальных хэшей: %d", n, len(hash_channels))
    logger.info("Всего хэшируемых постов: %d, уникальных хэшей: %d", n, len(hash_channels))
    return hash_channels, channel_posts


def _channel_pairs(hash_channels: dict) -> dict:
    """(channel_a, channel_b) → число ОБЩИХ хэшей (a < b). Только межканальные дубли."""
    pair_shared: dict[tuple[int, int], int] = defaultdict(int)
    for chans in hash_channels.values():
        if len(chans) < 2:
            continue                 # хэш только в одном канале — не межканальный дубль
        ordered = sorted(chans)
        for i in range(len(ordered)):
            for j in range(i + 1, len(ordered)):
                pair_shared[(ordered[i], ordered[j])] += 1
    return pair_shared


def _names(conn: sqlite3.Connection) -> dict[int, tuple[str, str]]:
    return {
        cid: (title or "", username or "")
        for cid, title, username in conn.execute(
            "SELECT channel_id, COALESCE(title,''), COALESCE(username,'') FROM channels"
        )
    }


def main() -> int:
    ap = argparse.ArgumentParser(description="Поиск каналов с одинаковыми постами (по хэшам)")
    ap.add_argument("--min-len", type=int, default=120, help="мин. длина поста (default 120)")
    ap.add_argument("--min-shared", type=int, default=5,
                    help="мин. число общих постов у пары каналов (default 5)")
    ap.add_argument("--min-overlap", type=float, default=0.3,
                    help="мин. доля общих постов (от меньшего канала) для попадания в CSV (default 0.3)")
    ap.add_argument("--out", type=Path, default=DEFAULT_OUT, help="путь к CSV")
    args = ap.parse_args()

    conn = _connect_ro()
    try:
        logger.info("Индексирую посты (min_len=%d)…", args.min_len)
        hash_channels, channel_posts = _build_index(conn, args.min_len)
        logger.info("Считаю межканальные пересечения…")
        pairs = _channel_pairs(hash_channels)
        names = _names(conn)
    finally:
        conn.close()

    rows = []
    for (a, b), shared in pairs.items():
        if shared < args.min_shared:
            continue
        na, nb = channel_posts.get(a, 0), channel_posts.get(b, 0)
        denom = min(na, nb) or 1
        overlap = shared / denom     # доля общих постов от МЕНЬШЕГО канала
        if overlap < args.min_overlap:
            continue
        ta, ua = names.get(a, ("", ""))
        tb, ub = names.get(b, ("", ""))
        rows.append({
            "channel_a": a, "title_a": ta, "username_a": ua, "posts_a": na,
            "channel_b": b, "title_b": tb, "username_b": ub, "posts_b": nb,
            "shared": shared, "overlap": round(overlap, 3),
        })

    rows.sort(key=lambda r: (-r["overlap"], -r["shared"]))
    cols = ["channel_a", "title_a", "username_a", "posts_a",
            "channel_b", "title_b", "username_b", "posts_b", "shared", "overlap"]
    with args.out.open("w", newline="", encoding="utf-8") as fh:
        w = csv.DictWriter(fh, fieldnames=cols)
        w.writeheader()
        w.writerows(rows)

    logger.info("Пар каналов-дубликатов: %d → %s", len(rows), args.out)
    print(f"\n{'='*78}\nКАНАЛЫ-ДУБЛИКАТЫ (общих постов ≥{args.min_shared}, "
          f"пересечение ≥{args.min_overlap:.0%})\n{'='*78}")
    for r in rows[:30]:
        na = (r["title_a"] or r["username_a"] or str(r["channel_a"]))[:30]
        nb = (r["title_b"] or r["username_b"] or str(r["channel_b"]))[:30]
        print(f"  {r['overlap']:>5.0%}  общих={r['shared']:<4}  {na:<30} ⇄ {nb}")
    print(f"\nВсего пар: {len(rows)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
