#!/usr/bin/env python3
"""Evaluate the heuristic AI-detector against a labelled ground-truth set.

Reads tools/eval/ai_samples.txt (label=AI) and tools/eval/human_samples.txt
(label=human), runs HeuristicAnalyzer.classify() on each, and prints a confusion
matrix + accuracy/precision/recall. Optionally also runs the Ollama LLM judge.

Usage:
    python tools/ai_detect_eval.py                 # heuristic only
    python tools/ai_detect_eval.py --llm           # also LLM judge (needs OLLAMA_BASE)
    python tools/ai_detect_eval.py --llm --model qwen3.5:9b
"""

from __future__ import annotations

import argparse
import asyncio
from pathlib import Path

import ai_detect  # same dir

TOOLS = Path(__file__).resolve().parent
EVAL = TOOLS / "eval"


def load_samples(path: Path) -> list[str]:
    """Parse a sample file: blocks separated by a line '---', '#' lines are comments."""
    if not path.exists():
        return []
    blocks: list[str] = []
    cur: list[str] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.strip() == "---":
            text = "\n".join(cur).strip()
            if text:
                blocks.append(text)
            cur = []
        elif line.lstrip().startswith("#"):
            continue
        else:
            cur.append(line)
    text = "\n".join(cur).strip()
    if text:
        blocks.append(text)
    return blocks


def confusion(preds: list[tuple[int, int]]) -> None:
    """preds = list of (true_label, pred_label); 1=AI, 0=human."""
    tp = sum(1 for y, p in preds if y == 1 and p == 1)
    fn = sum(1 for y, p in preds if y == 1 and p == 0)
    fp = sum(1 for y, p in preds if y == 0 and p == 1)
    tn = sum(1 for y, p in preds if y == 0 and p == 0)
    n = len(preds)
    acc = (tp + tn) / n if n else 0
    prec = tp / (tp + fp) if (tp + fp) else 0.0
    rec = tp / (tp + fn) if (tp + fn) else 0.0
    f1 = 2 * prec * rec / (prec + rec) if (prec + rec) else 0.0

    print("\n  Confusion matrix (rows=true, cols=pred):")
    print("                pred:AI   pred:HUMAN")
    print(f"    true:AI       {tp:>5}      {fn:>6}")
    print(f"    true:HUMAN    {fp:>5}      {tn:>6}")
    print(f"\n  Accuracy : {acc:.3f}   ({tp+tn}/{n})")
    print(f"  Precision: {prec:.3f}   (из помеченных AI — реально AI)")
    print(f"  Recall   : {rec:.3f}   (из реальных AI — поймано)")
    print(f"  F1       : {f1:.3f}")
    print(f"  Baseline (всегда human): {sum(1 for y,_ in preds if y==0)/n:.3f}")


def eval_heuristic(ai: list[str], human: list[str]) -> None:
    print(f"\n{'='*64}\nHEURISTIC — {len(ai)} AI + {len(human)} human samples\n{'='*64}")
    preds = []
    rows = [(t, 1) for t in ai] + [(t, 0) for t in human]
    for text, label in rows:
        f = ai_detect.HeuristicAnalyzer.compute_features(text)
        is_ai, conf, score = ai_detect.HeuristicAnalyzer.classify(f)
        preds.append((label, 1 if is_ai else 0))
    confusion(preds)

    # show worst mistakes
    print("\n  Примеры ошибок:")
    shown = 0
    for text, label in rows:
        f = ai_detect.HeuristicAnalyzer.compute_features(text)
        is_ai, conf, score = ai_detect.HeuristicAnalyzer.classify(f)
        pred = 1 if is_ai else 0
        if pred != label and shown < 6:
            kind = "AI→помечен HUMAN" if label == 1 else "HUMAN→помечен AI"
            print(f"   [{kind}] score={score:.3f}: {text[:90].replace(chr(10),' ')}")
            shown += 1


def eval_llm(ai: list[str], human: list[str], model: str | None) -> None:
    print(f"\n{'='*64}\nLLM JUDGE (Ollama)\n{'='*64}")
    judge = ai_detect.LlmJudge(model=model)
    try:
        m, base, _ = judge._resolve_config()
        print(f"  model={m}  base_url={base}")
    except RuntimeError as e:
        print(f"  LLM не настроен: {e}")
        return

    rows = [(t, 1) for t in ai] + [(t, 0) for t in human]

    async def run():
        preds = []
        items = [(i, t) for i, (t, _) in enumerate(rows)]
        verdicts = await judge.judge_batch(items)
        vmap = {mid: v for mid, v in verdicts}
        for i, (text, label) in enumerate(rows):
            v = vmap.get(i)
            if v is None or v.verdict == "uncertain":
                pred = -1
            else:
                pred = 1 if v.verdict == "ai" else 0
            preds.append((label, pred))
        return preds

    try:
        preds = asyncio.run(run())
    except Exception as e:
        print(f"  LLM запрос упал: {e}")
        return

    unc = sum(1 for _, p in preds if p == -1)
    decided = [(y, p) for y, p in preds if p != -1]
    print(f"\n  uncertain/не определено: {unc}/{len(preds)}")
    if decided:
        confusion(decided)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--llm", action="store_true")
    ap.add_argument("--model")
    args = ap.parse_args()

    ai = load_samples(EVAL / "ai_samples.txt")
    human = load_samples(EVAL / "human_samples.txt")
    if not ai or not human:
        raise SystemExit("Нет образцов. Заполни tools/eval/ai_samples.txt и human_samples.txt")

    eval_heuristic(ai, human)
    if args.llm:
        eval_llm(ai, human, args.model)
    print()


if __name__ == "__main__":
    main()
