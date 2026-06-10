#!/usr/bin/env python3
"""Blind human evaluation of the AI-detector.

Picks N real messages from the main DB, pre-computes heuristic + (optional) LLM
verdicts SILENTLY, then shows you ONLY the text. You label each a/h/? .
At the end it prints the diff: your label vs heuristic vs LLM + agreement.

You never see the machine verdicts until the final report — no leakage.

Stages:
  1. `prepare` — pick messages, compute machine verdicts, write a session file.
  2. `label`   — interactive blind labelling (run yourself via `! ...`).
  3. `report`  — compare your labels to heuristic/LLM, print metrics.

Usage:
    python ai_detect_tool/human_eval.py prepare --n 20 [--llm]
    python ai_detect_tool/human_eval.py label
    python ai_detect_tool/human_eval.py report
"""

from __future__ import annotations

import argparse
import asyncio
import json
import sqlite3
import sys
from pathlib import Path

import ai_detect  # same dir

TOOL_DIR = Path(__file__).resolve().parent
PROJECT_ROOT = TOOL_DIR.parent
MAIN_DB = PROJECT_ROOT / "data" / "tg_search.db"
SESSION = TOOL_DIR / "human_eval_session.json"


# ---------------------------------------------------------------------------
# 1. prepare — pick messages + pre-compute machine verdicts (silent)
# ---------------------------------------------------------------------------

# Кириллические буквы, которых НЕТ в русском алфавите — маркеры узбекского/
# таджикского/казахского/украинского и т.п. Пользователь их не читает, как и
# немецкий: текст с заметной долей таких букв — НЕ русский, отсекаем.
_NON_RU_CYRILLIC = set("ўқҳғҷҙҡҝңүұһәөіїєѓѕћђјљњ")


def _is_russian(text: str) -> bool:
    """Cyrillic-dominance guard on top of detected_lang='ru'.

    The project's detected_lang tag is noisy (≈1/3 of 'ru' rows are actually
    Chinese/Uzbek/English/decorative-unicode), so confirm Cyrillic dominates the
    letters. The user doesn't read German/other — these must not slip through.
    Also rejects Uzbek/Tajik/Kazakh Cyrillic (ў/қ/ҳ/ғ…), which passes the cyr>lat
    check but is unreadable to the user — same policy as German.
    """
    cyr = sum(1 for c in text if "а" <= c.lower() <= "я" or c.lower() == "ё")
    lat = sum(1 for c in text if "a" <= c.lower() <= "z")
    # reject CJK/Korean-heavy text that happens to carry a few Cyrillic words (e.g. a
    # Chinese post mentioning «Тюмень») — those fool the cyr>lat check.
    cjk = sum(1 for c in text if "一" <= c <= "鿿" or "가" <= c <= "힣")
    # reject non-Russian Cyrillic (Uzbek/Tajik/Kazakh): if such letters are a
    # non-trivial share of the Cyrillic, it's not Russian.
    non_ru = sum(1 for c in text if c.lower() in _NON_RU_CYRILLIC)
    if cyr and non_ru / cyr > 0.03:
        return False
    return cyr >= 30 and cyr > lat and cjk <= 2


# Only LONG posts — on short texts AI-vs-human attribution is basically impossible.
MIN_LEN = 300
MAX_LEN = 2000


def _pick_messages(n: int) -> list[tuple[int, int, str]]:
    """Pick n LONG real RUSSIAN posts, at most ONE per channel.

    Uses the project's own language tag `detected_lang='ru'` (the DB's fresh tail is
    mostly German channels — the user doesn't read German) AND a Cyrillic-dominance
    guard (the tag is noisy). Requires LONG posts (MIN_LEN..MAX_LEN) since short texts
    can't be attributed. De-dups by channel_id so no channel appears twice, and
    stratifies across the whole ru id-range (1.8M…10M) for topic/source diversity.
    """
    conn = sqlite3.connect(f"file:{MAIN_DB}?mode=ro", uri=True)
    lo, hi, total = conn.execute(
        """
        SELECT MIN(id), MAX(id), COUNT(*) FROM messages
        WHERE detected_lang='ru' AND message_kind='regular'
          AND LENGTH(text) BETWEEN ? AND ?
        """,
        (MIN_LEN, MAX_LEN),
    ).fetchone()
    if not total:
        conn.close()
        return []
    # walk the range in evenly-spaced buckets; in each bucket scan ru rows and take the
    # first long Cyrillic post from a NEW channel. Oversample (10x) since the guard +
    # one-per-channel de-dup drop a lot.
    buckets = max(n * 10, n)
    step = max(1, (hi - lo) // buckets)
    seen_prefix: set[str] = set()
    seen_chan: set[int] = set()
    picked: list[tuple[int, int, str]] = []
    cursor = lo
    while cursor <= hi and len(picked) < n:
        candidates = conn.execute(
            """
            SELECT id, channel_id, text FROM messages
            WHERE detected_lang='ru' AND message_kind='regular'
              AND LENGTH(text) BETWEEN ? AND ? AND id >= ?
            ORDER BY id ASC LIMIT 12
            """,
            (MIN_LEN, MAX_LEN, cursor),
        ).fetchall()
        cursor += step
        for mid, ch, t in candidates:
            t = (t or "").strip()
            key = t[:50]
            if ch in seen_chan or key in seen_prefix or len(t) < MIN_LEN or not _is_russian(t):
                continue
            seen_chan.add(ch)
            seen_prefix.add(key)
            picked.append((mid, ch, t))
            break
    conn.close()
    return picked[:n]


def _heuristic_verdict(text: str) -> tuple[int, float, float]:
    f = ai_detect.HeuristicAnalyzer.compute_features(text)
    is_ai, conf, score = ai_detect.HeuristicAnalyzer.classify(f)
    return (1 if is_ai else 0, conf, score)


async def _llm_verdicts(texts: list[str]) -> list[dict | None]:
    judge = ai_detect.LlmJudge()
    try:
        judge._resolve_config()
    except RuntimeError as e:
        print(f"LLM не настроен ({e}) — пропускаю LLM-вердикты", file=sys.stderr)
        return [None] * len(texts)
    out: list[dict | None] = []
    for i, t in enumerate(texts):
        try:
            v = await judge.judge_single(t)
            out.append({"verdict": v.verdict, "confidence": v.confidence, "reasoning": v.reasoning})
        except Exception as e:
            out.append({"verdict": "error", "confidence": 0.0, "reasoning": str(e)})
        sys.stderr.write(".")
        sys.stderr.flush()
    sys.stderr.write("\n")
    return out


def cmd_prepare(args: argparse.Namespace) -> None:
    picked = _pick_messages(args.n)
    if not picked:
        raise SystemExit("Не удалось набрать сообщения из БД")
    print(f"Набрано {len(picked)} сообщений. Считаю машинные вердикты (молча)…", file=sys.stderr)

    texts = [t for _, _, t in picked]
    heur = [_heuristic_verdict(t) for t in texts]

    llm: list[dict | None] = [None] * len(texts)
    if args.llm:
        llm = asyncio.run(_llm_verdicts(texts))

    items = []
    for (mid, ch, t), (h_ai, h_conf, h_score), lv in zip(picked, heur, llm):
        items.append({
            "message_id": mid,
            "channel_id": ch,
            "text": t,
            "heuristic": {"is_ai": h_ai, "confidence": h_conf, "score": h_score},
            "llm": lv,
            "human": None,  # filled during `label`
        })
    SESSION.write_text(json.dumps({"items": items}, ensure_ascii=False, indent=2), encoding="utf-8")
    print(f"Готово. Сессия: {SESSION.name} ({len(items)} шт).", file=sys.stderr)
    print("Теперь запусти разметку:  ! python ai_detect_tool/human_eval.py label", file=sys.stderr)


# ---------------------------------------------------------------------------
# 2. label — interactive blind labelling
# ---------------------------------------------------------------------------

def cmd_label(args: argparse.Namespace) -> None:
    if not SESSION.exists():
        raise SystemExit("Нет сессии. Сначала: human_eval.py prepare")
    data = json.loads(SESSION.read_text(encoding="utf-8"))
    items = data["items"]

    print("\n" + "=" * 70)
    print("СЛЕПАЯ РАЗМЕТКА — машинных вердиктов не видно.")
    print("Для каждого сообщения нажми:  a = AI,  h = человек,  ? = не знаю")
    print("                              s = пропустить,  q = выйти и сохранить")
    print("=" * 70)

    start = next((i for i, it in enumerate(items) if it["human"] is None), len(items))
    if start == len(items):
        print("\nВсё уже размечено. Запусти report.")
        return

    for i in range(start, len(items)):
        it = items[i]
        print(f"\n{'─'*70}\n[{i+1}/{len(items)}]\n{'─'*70}")
        print(it["text"])
        print("─" * 70)
        while True:
            ans = input("Твой вердикт (a/h/?/s/q): ").strip().lower()
            if ans in ("a", "h", "?", "s", "q"):
                break
            print("  Введи одну из: a h ? s q")
        if ans == "q":
            break
        if ans == "s":
            continue
        it["human"] = {"a": "ai", "h": "human", "?": "uncertain"}[ans]
        SESSION.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

    done = sum(1 for it in items if it["human"] is not None)
    print(f"\nСохранено. Размечено {done}/{len(items)}.")
    print("Отчёт:  ! python ai_detect_tool/human_eval.py report")


# ---------------------------------------------------------------------------
# 3. report — diff human vs heuristic vs LLM
# ---------------------------------------------------------------------------

def _agree(a: str, b: str) -> bool:
    return a in ("ai", "human") and a == b


def cmd_report(args: argparse.Namespace) -> None:
    if not SESSION.exists():
        raise SystemExit("Нет сессии.")
    data = json.loads(SESSION.read_text(encoding="utf-8"))
    items = [it for it in data["items"] if it["human"] is not None]
    if not items:
        raise SystemExit("Нет размеченных сообщений. Сначала: human_eval.py label")

    has_llm = any(it["llm"] for it in items)

    # human as ground truth → accuracy of each machine method
    def metrics(method: str):
        tp = fn = fp = tn = 0
        compared = 0
        for it in items:
            hum = it["human"]
            if hum not in ("ai", "human"):
                continue
            if method == "heuristic":
                pred = "ai" if it["heuristic"]["is_ai"] else "human"
            else:
                lv = it["llm"]
                if not lv or lv["verdict"] not in ("ai", "human"):
                    continue
                pred = lv["verdict"]
            compared += 1
            if hum == "ai" and pred == "ai":
                tp += 1
            elif hum == "ai" and pred == "human":
                fn += 1
            elif hum == "human" and pred == "ai":
                fp += 1
            else:
                tn += 1
        return tp, fn, fp, tn, compared

    print("\n" + "=" * 78)
    print(f"ОТЧЁТ — {len(items)} размечено человеком (твоя разметка = эталон)")
    print("=" * 78)

    # per-message diff table
    print(f"\n{'#':<3} {'твоя':<8} {'эврист':<14} {'LLM':<14} текст")
    print("-" * 78)
    for i, it in enumerate(items):
        hum = it["human"]
        h = it["heuristic"]
        h_str = f"{'ai' if h['is_ai'] else 'human'} ({h['score']:.2f})"
        if it["llm"] and it["llm"]["verdict"] in ("ai", "human"):
            l_str = f"{it['llm']['verdict']} ({it['llm']['confidence']:.2f})"
        elif it["llm"]:
            l_str = it["llm"]["verdict"]
        else:
            l_str = "—"
        # flag disagreements with human
        hflag = "" if _agree(hum, "ai" if h["is_ai"] else "human") else "✗"
        lflag = ""
        if it["llm"] and it["llm"]["verdict"] in ("ai", "human"):
            lflag = "" if _agree(hum, it["llm"]["verdict"]) else "✗"
        txt = it["text"].replace("\n", " ")[:34]
        print(f"{i+1:<3} {hum:<8} {h_str:<12}{hflag:<2} {l_str:<12}{lflag:<2} {txt}")

    # summary metrics
    for method, label in [("heuristic", "ЭВРИСТИКА"), ("llm", "LLM")]:
        if method == "llm" and not has_llm:
            continue
        tp, fn, fp, tn, n = metrics(method)
        if n == 0:
            continue
        acc = (tp + tn) / n
        prec = tp / (tp + fp) if (tp + fp) else 0.0
        rec = tp / (tp + fn) if (tp + fn) else 0.0
        print(f"\n── {label} vs твоя разметка ({n} сравнено) ──")
        print(f"   совпало: {tp+tn}/{n}  →  accuracy {acc:.3f}")
        print(f"   precision {prec:.3f}  recall {rec:.3f}")
        print(f"   confusion: AI→AI {tp}, AI→human {fn}, human→AI {fp}, human→human {tn}")

    # human label distribution
    da = sum(1 for it in items if it["human"] == "ai")
    dh = sum(1 for it in items if it["human"] == "human")
    du = sum(1 for it in items if it["human"] == "uncertain")
    print(f"\nТвоё распределение: AI={da}  human={dh}  не знаю={du}")
    print()


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="command")
    p = sub.add_parser("prepare")
    p.add_argument("--n", type=int, default=20)
    p.add_argument("--llm", action="store_true")
    sub.add_parser("label")
    sub.add_parser("report")
    args = ap.parse_args()
    if args.command == "prepare":
        cmd_prepare(args)
    elif args.command == "label":
        cmd_label(args)
    elif args.command == "report":
        cmd_report(args)
    else:
        ap.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
