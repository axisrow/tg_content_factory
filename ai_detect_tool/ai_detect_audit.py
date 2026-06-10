#!/usr/bin/env python3
"""Audit the AI-detection results (read-only).

Reads data/ai_detection.db (and main DB for texts), reconstructs the per-feature
sub-scores from HeuristicAnalyzer.classify() weights, and prints:
  - score histogram
  - min/mean/max of each of the 8 sub-scores (shows which features are dead)
  - top AI-scored and top human-scored messages with their texts

No writes. Standalone diagnostic for `tools/ai_detect.py`.

Usage:
    python tools/ai_detect_audit.py
    python tools/ai_detect_audit.py --top 10
"""

from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path

TOOL_DIR = Path(__file__).resolve().parent          # ai_detect_tool/
PROJECT_ROOT = TOOL_DIR.parent                       # project root
MAIN_DB = PROJECT_ROOT / "data" / "tg_search.db"     # source DB (read-only, shared)
DETECT_DB = TOOL_DIR / "ai_detection.db"             # results next to the tool


def _clamp(x: float) -> float:
    return max(0.0, min(1.0, x))


def subscores(f: dict) -> dict[str, float]:
    """Reconstruct the 8 sub-scores exactly as HeuristicAnalyzer.classify does."""
    return {
        "entropy": _clamp(1.0 - (f["char_trigram_entropy"] - 3.0) / 1.5),
        "burstiness": _clamp(1.0 - (f["burstiness"] - 0.1) / 1.2),
        "ttr": _clamp((f["type_token_ratio"] - 0.2) / 0.7),
        "starter": _clamp(1.0 - (f["sentence_starter_diversity"] - 0.1) / 0.9),
        "punct": _clamp(1.0 - (f["punctuation_regularity"] - 0.1) / 1.5),
        "fmt": _clamp(f["formatting_density"] * 20),
        "list": _clamp(f["list_marker_ratio"] * 3),
        "emoji": _clamp(1.0 - f["emoji_density"] / 2.0),
    }


WEIGHTS = {
    "entropy": 0.20, "burstiness": 0.20, "ttr": 0.10, "starter": 0.15,
    "punct": 0.10, "fmt": 0.08, "list": 0.07, "emoji": 0.10,
}


def _load_texts(ids: list[int]) -> dict[int, str]:
    if not MAIN_DB.exists() or not ids:
        return {}
    conn = sqlite3.connect(f"file:{MAIN_DB}?mode=ro", uri=True)
    out: dict[int, str] = {}
    qmarks = ",".join("?" * len(ids))
    for mid, text in conn.execute(
        f"SELECT id, text FROM messages WHERE id IN ({qmarks})", ids
    ):
        out[mid] = text or ""
    conn.close()
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--top", type=int, default=10)
    args = ap.parse_args()

    if not DETECT_DB.exists():
        raise SystemExit(f"Detection DB not found: {DETECT_DB}")

    conn = sqlite3.connect(f"file:{DETECT_DB}?mode=ro", uri=True)
    rows = conn.execute(
        "SELECT message_id, channel_id, is_ai, confidence, heuristic_score, features_json "
        "FROM ai_detection_results"
    ).fetchall()
    conn.close()

    n = len(rows)
    print(f"\n{'='*64}\nAUDIT: {n} results in {DETECT_DB.name}\n{'='*64}")

    # --- score histogram ---
    print("\n── Score histogram (bucket = 0.05) ──")
    buckets: dict[float, int] = {}
    for _, _, _, _, score, _ in rows:
        b = round(score / 0.05) * 0.05
        buckets[b] = buckets.get(b, 0) + 1
    for b in sorted(buckets):
        bar = "█" * buckets[b]
        print(f"  {b:.2f}  {buckets[b]:>4}  {bar}")

    n_ai = sum(1 for r in rows if r[2])
    print(f"\n  AI: {n_ai} ({100*n_ai/n:.1f}%)   Human: {n-n_ai} ({100*(n-n_ai)/n:.1f}%)")

    # --- sub-score spread ---
    print("\n── Sub-score spread (0=human-like, 1=AI-like) ──")
    print(f"  {'feature':<10} {'weight':>6} {'min':>6} {'mean':>6} {'max':>6}  verdict")
    agg: dict[str, list[float]] = {k: [] for k in WEIGHTS}
    for _, _, _, _, _, fj in rows:
        f = json.loads(fj)
        for k, v in subscores(f).items():
            agg[k].append(v)
    for k in WEIGHTS:
        v = agg[k]
        lo, mx, mean = min(v), max(v), sum(v) / len(v)
        if mx - lo < 0.001:
            verdict = "DEAD (constant)"
        elif mean > 0.85:
            verdict = "saturated → always AI"
        elif mean < 0.15:
            verdict = "saturated → always human"
        else:
            verdict = "varies"
        print(f"  {k:<10} {WEIGHTS[k]:>6.2f} {lo:>6.3f} {mean:>6.3f} {mx:>6.3f}  {verdict}")

    # --- top AI / top human ---
    sorted_rows = sorted(rows, key=lambda r: r[4], reverse=True)
    top_ai = sorted_rows[: args.top]
    top_human = sorted_rows[-args.top :]
    texts = _load_texts([r[0] for r in top_ai + top_human])

    def dump(label, rs):
        print(f"\n{'='*64}\n{label}\n{'='*64}")
        for mid, ch, is_ai, conf, score, _ in rs:
            t = texts.get(mid, "<text unavailable>").replace("\n", " ⏎ ")
            tag = "🤖AI" if is_ai else "👤HU"
            print(f"\n[{tag}] id={mid} score={score:.3f} conf={conf:.3f}")
            print(f"   {t[:200]}")

    dump(f"TOP {args.top} BY SCORE (most AI-like)", top_ai)
    dump(f"BOTTOM {args.top} BY SCORE (most human-like)", list(reversed(top_human)))
    print()


if __name__ == "__main__":
    main()
