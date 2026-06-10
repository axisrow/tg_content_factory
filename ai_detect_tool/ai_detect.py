#!/usr/bin/env python3
"""AI-generated text detection for Telegram messages.

Standalone analysis tool — reads from the main project DB (read-only),
writes results to a separate ai_detection.db. No changes to the project.

Usage:
    python tools/ai_detect.py run --limit 100 --dry-run
    python tools/ai_detect.py run --channel-id 12345
    python tools/ai_detect.py stats
    python tools/ai_detect.py message 42
"""

from __future__ import annotations

import argparse
import asyncio
import json
import logging
import math
import os
import re
import sqlite3
import sys
from collections import Counter
from dataclasses import dataclass
from pathlib import Path
from typing import Sequence

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("ai_detect")

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
TOOL_DIR = Path(__file__).resolve().parent          # ai_detect_tool/
PROJECT_ROOT = TOOL_DIR.parent                       # project root
DEFAULT_MAIN_DB = PROJECT_ROOT / "data" / "tg_search.db"   # source DB (read-only, shared)
DEFAULT_DETECT_DB = TOOL_DIR / "ai_detection.db"          # results live next to the tool

# ---------------------------------------------------------------------------
# Detection thresholds
# ---------------------------------------------------------------------------
HUMAN_THRESHOLD = 0.30   # score < this → confidently human
AI_THRESHOLD = 0.70      # score > this → confidently AI
# Between HUMAN_THRESHOLD and AI_THRESHOLD → uncertain → send to LLM if available

MIN_TEXT_LENGTH = 50
BATCH_SIZE = 1000
LLM_BATCH_SIZE = 8

# ---------------------------------------------------------------------------
# 1. Separate results DB
# ---------------------------------------------------------------------------

_SCHEMA = """
CREATE TABLE IF NOT EXISTS ai_detection_results (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    message_id INTEGER NOT NULL,
    channel_id INTEGER NOT NULL,
    is_ai INTEGER NOT NULL DEFAULT 0,
    confidence REAL NOT NULL DEFAULT 0.0,
    method TEXT NOT NULL,
    heuristic_score REAL,
    features_json TEXT,
    llm_verdict TEXT,
    model_used TEXT,
    analyzed_at TEXT DEFAULT (datetime('now')),
    UNIQUE(message_id)
);
CREATE INDEX IF NOT EXISTS idx_adr_channel
    ON ai_detection_results(channel_id, is_ai);
CREATE INDEX IF NOT EXISTS idx_adr_msg
    ON ai_detection_results(message_id);
"""


class AiDetectionDb:
    """Thin wrapper around the separate ai_detection.db (synchronous sqlite3)."""

    def __init__(self, path: Path):
        self._path = path
        self._conn: sqlite3.Connection | None = None

    def open(self) -> None:
        self._conn = sqlite3.connect(str(self._path))
        self._conn.executescript(_SCHEMA)
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.execute("PRAGMA synchronous=NORMAL")
        logger.info("Results DB: %s", self._path)

    def close(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None

    def insert_batch(self, rows: Sequence[tuple]) -> None:
        if not rows or not self._conn:
            return
        self._conn.executemany(
            """INSERT OR REPLACE INTO ai_detection_results
               (message_id, channel_id, is_ai, confidence, method,
                heuristic_score, features_json, llm_verdict, model_used)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            rows,
        )
        self._conn.commit()

    def count_results(self) -> dict[str, int]:
        if not self._conn:
            return {}
        cur = self._conn.execute(
            "SELECT is_ai, COUNT(*) FROM ai_detection_results GROUP BY is_ai"
        )
        out: dict[str, int] = {}
        for is_ai, cnt in cur.fetchall():
            key = "ai" if is_ai else "human"
            out[key] = cnt
        total = self._conn.execute("SELECT COUNT(*) FROM ai_detection_results").fetchone()[0]
        out["total"] = total
        return out

    def count_by_method(self) -> dict[str, int]:
        if not self._conn:
            return {}
        cur = self._conn.execute(
            "SELECT method, COUNT(*) FROM ai_detection_results GROUP BY method"
        )
        return dict(cur.fetchall())

    def top_ai_channels(self, limit: int = 15) -> list[tuple]:
        if not self._conn:
            return []
        cur = self._conn.execute(
            """
            SELECT channel_id,
                   SUM(is_ai) AS ai_count,
                   COUNT(*) AS total,
                   ROUND(100.0 * SUM(is_ai) / COUNT(*), 1) AS pct
            FROM ai_detection_results
            GROUP BY channel_id
            ORDER BY pct DESC, ai_count DESC
            LIMIT ?
            """,
            (limit,),
        )
        return cur.fetchall()

    def get_message_result(self, message_id: int) -> dict | None:
        if not self._conn:
            return None
        cur = self._conn.execute(
            "SELECT * FROM ai_detection_results WHERE message_id = ?",
            (message_id,),
        )
        row = cur.fetchone()
        if not row:
            return None
        cols = [d[0] for d in cur.description]
        return dict(zip(cols, row))

    def analyzed_ids(self) -> set[int]:
        if not self._conn:
            return set()
        cur = self._conn.execute("SELECT message_id FROM ai_detection_results")
        return {r[0] for r in cur.fetchall()}

    def has_message(self, message_id: int) -> bool:
        if not self._conn:
            return False
        cur = self._conn.execute(
            "SELECT 1 FROM ai_detection_results WHERE message_id = ?", (message_id,)
        )
        return cur.fetchone() is not None


# ---------------------------------------------------------------------------
# 2. Heuristic Analyzer
# ---------------------------------------------------------------------------

_SENTENCE_SPLIT_RE = re.compile(r"(?<=[.!?。\!\?])\s+|\n+")
_WORD_RE = re.compile(r"\b\w+\b", re.UNICODE)
_FORMATTING_RE = re.compile(r"(\*\*.*?\*\*|__.*?__|##\s|-\s|\d+\.\s|•\s|►\s|→\s)")
_EMOJI_RE = re.compile(
    "[\U0001F600-\U0001F64F\U0001F300-\U0001F5FF\U0001F680-\U0001F6FF"
    "\U0001F1E0-\U0001F1FF\U00002702-\U000027B0\U0001F900-\U0001F9FF"
    "\U0001FA00-\U0001FA6F\U0001FA70-\U0001FAFF\U00002600-\U000026FF]"
)
_LIST_MARKER_RE = re.compile(r"^[\s]*[-•►→]\s|^\d+[.)]\s", re.MULTILINE)


def _split_sentences(text: str) -> list[str]:
    parts = _SENTENCE_SPLIT_RE.split(text)
    return [p.strip() for p in parts if p.strip()]


def _char_ngrams(text: str, n: int = 3) -> Counter:
    text = text.lower()
    return Counter(text[i : i + n] for i in range(len(text) - n + 1))


def _entropy_from_counter(counter: Counter) -> float:
    total = sum(counter.values())
    if total == 0:
        return 0.0
    return -sum((c / total) * math.log2(c / total) for c in counter.values() if c > 0)


def _coefficient_of_variation(values: Sequence[float]) -> float:
    if not values:
        return 0.0
    mean = sum(values) / len(values)
    if mean == 0:
        return 0.0
    variance = sum((v - mean) ** 2 for v in values) / len(values)
    return math.sqrt(variance) / mean


@dataclass
class Features:
    """Statistical features for a single message."""

    char_trigram_entropy: float = 0.0
    burstiness: float = 0.0           # CV of sentence lengths
    type_token_ratio: float = 0.0     # unique_words / total_words
    sentence_starter_diversity: float = 0.0  # unique first words / total sentences
    punctuation_regularity: float = 0.0     # CV of inter-punctuation intervals
    formatting_density: float = 0.0   # ratio of formatting markers to text length
    emoji_density: float = 0.0        # emojis per 100 chars
    list_marker_ratio: float = 0.0    # fraction of lines starting with list markers
    avg_sentence_length: float = 0.0
    word_count: int = 0
    sentence_count: int = 0


class HeuristicAnalyzer:
    """Compute per-message statistical features indicative of AI-generated text."""

    @staticmethod
    def compute_features(text: str) -> Features:
        f = Features()

        # -- char trigram entropy --
        trigrams = _char_ngrams(text, 3)
        f.char_trigram_entropy = _entropy_from_counter(trigrams)

        # -- sentences --
        sentences = _split_sentences(text)
        f.sentence_count = len(sentences) if sentences else 1

        sent_lengths = [len(s) for s in sentences] if sentences else [len(text)]
        f.avg_sentence_length = sum(sent_lengths) / len(sent_lengths)

        # -- burstiness (CV of sentence lengths) --
        f.burstiness = _coefficient_of_variation([float(x) for x in sent_lengths])

        # -- words --
        words = _WORD_RE.findall(text.lower())
        f.word_count = len(words)
        if words:
            f.type_token_ratio = len(set(words)) / len(words)

        # -- sentence starter diversity --
        if sentences:
            starters = []
            for s in sentences:
                ws = s.split()
                if ws:
                    starters.append(ws[0].lower())
            if starters:
                f.sentence_starter_diversity = len(set(starters)) / len(starters)

        # -- punctuation regularity --
        punct_positions = [m.start() for m in re.finditer(r"[,;:!?]", text)]
        if len(punct_positions) >= 2:
            intervals = [float(punct_positions[i + 1] - punct_positions[i]) for i in range(len(punct_positions) - 1)]
            f.punctuation_regularity = _coefficient_of_variation(intervals)

        # -- formatting density --
        fmt_matches = _FORMATTING_RE.findall(text)
        f.formatting_density = len(fmt_matches) / max(len(text), 1)

        # -- emoji density --
        emojis = _EMOJI_RE.findall(text)
        f.emoji_density = len(emojis) / max(len(text) / 100, 1)

        # -- list marker ratio --
        lines = text.split("\n")
        non_empty = [ln for ln in lines if ln.strip()]
        if non_empty:
            list_lines = sum(1 for ln in non_empty if _LIST_MARKER_RE.match(ln))
            f.list_marker_ratio = list_lines / len(non_empty)

        return f

    @staticmethod
    def classify(f: Features) -> tuple[bool, float]:
        """Return (is_ai_predicted, confidence_0_to_1) based on weighted heuristic score.

        Weights are tuned for Russian Telegram channel content.
        Lower entropy + lower burstiness + higher TTR + more formatting → more AI-like.
        """

        # Normalize features to 0-1 range where higher = more AI-like
        # char_trigram_entropy: AI text tends to be MORE predictable (lower entropy)
        # Typical range: 3.0-4.5 for real text; AI often 3.2-3.8
        entropy_ai_score = max(0, min(1, 1.0 - (f.char_trigram_entropy - 3.0) / 1.5))

        # burstiness: AI has LOWER burstiness (more uniform sentence lengths)
        # Typical human: 0.5-1.5, AI: 0.2-0.6
        burstiness_ai_score = max(0, min(1, 1.0 - (f.burstiness - 0.1) / 1.2))

        # type_token_ratio: AI often has HIGHER TTR (more diverse vocab)
        # Typical: 0.3-0.7, AI tends 0.5-0.8
        ttr_ai_score = max(0, min(1, (f.type_token_ratio - 0.2) / 0.7))

        # sentence_starter_diversity: AI has LOWER diversity (repetitive openers)
        # Typical human: 0.5-1.0, AI: 0.2-0.5
        starter_ai_score = max(0, min(1, 1.0 - (f.sentence_starter_diversity - 0.1) / 0.9))

        # punctuation_regularity: AI has LOWER CV (more regular punctuation)
        # Typical human: 0.5-2.0, AI: 0.2-0.8
        punct_ai_score = max(0, min(1, 1.0 - (f.punctuation_regularity - 0.1) / 1.5))

        # formatting_density: AI uses more formatting
        fmt_ai_score = max(0, min(1, f.formatting_density * 20))

        # list_marker_ratio: AI loves lists
        list_ai_score = max(0, min(1, f.list_marker_ratio * 3))

        # emoji_density: AI tends to use FEWER emojis (or very specific ones)
        emoji_ai_score = max(0, min(1, 1.0 - f.emoji_density / 2.0))

        # Weighted combination
        weights = {
            "entropy": 0.20,
            "burstiness": 0.20,
            "ttr": 0.10,
            "starter": 0.15,
            "punct": 0.10,
            "fmt": 0.08,
            "list": 0.07,
            "emoji": 0.10,
        }

        score = (
            weights["entropy"] * entropy_ai_score
            + weights["burstiness"] * burstiness_ai_score
            + weights["ttr"] * ttr_ai_score
            + weights["starter"] * starter_ai_score
            + weights["punct"] * punct_ai_score
            + weights["fmt"] * fmt_ai_score
            + weights["list"] * list_ai_score
            + weights["emoji"] * emoji_ai_score
        )

        # Boost confidence for extreme values
        is_ai = score > 0.5
        # Map score to confidence: distance from 0.5, scaled to [0, 1]
        confidence = abs(score - 0.5) * 2.0

        return is_ai, round(confidence, 3), round(score, 3)


# ---------------------------------------------------------------------------
# 3. LLM Judge
# ---------------------------------------------------------------------------

LLM_PROMPT_TEMPLATE = """Определи, написано ли следующее сообщение из Telegram-канала ИИ или человеком.

Критерии для анализа:
- Однородность структуры предложений (ИИ пишет однообразно)
- Отсутствие разговорных оборотов, сленга, опечаток (ИИ пишет «чисто»)
- Излишне формальный или «гладкий» тон
- Предсказуемый словарный запас, шаблонные фразы
- Отсутствие личного опыта, мнений, эмоций
- Избыточное форматирование (списки, заголовки, **жирный** текст)
- Неестественно правильная пунктуация

Ответь ТОЛЬКО валидным JSON, без markdown-обёрток:
{{"verdict": "ai" или "human", "confidence": 0.0-1.0, "reasoning": "краткое объяснение на русском"}}

Сообщение:
{text}"""

LLM_BATCH_PROMPT_TEMPLATE = """Определи для каждого сообщения из Telegram-канала, написано ли оно ИИ или человеком.

Критерии: однородность структуры, отсутствие сленга/опечаток, формальный тон,
шаблонные фразы, отсутствие личного опыта, избыточное форматирование.

Ответь ТОЛЬКО валидным JSON-массивом, без markdown-обёрток:
[{{"id": 1, "verdict": "ai" или "human", "confidence": 0.0-1.0, "reasoning": "кратко"}}]

Сообщения:
{messages}"""


@dataclass
class LlmVerdict:
    verdict: str     # "ai", "human", "uncertain"
    confidence: float
    reasoning: str


class LlmJudge:
    """LLM-based AI text detector using OpenAI-compatible API or Ollama."""

    def __init__(
        self,
        model: str | None = None,
        base_url: str | None = None,
        api_key: str | None = None,
    ):
        self._model = model or os.environ.get("AI_DETECT_MODEL", "")
        self._base_url = base_url or os.environ.get("AI_DETECT_BASE_URL", "")
        self._api_key = api_key or os.environ.get("AI_DETECT_API_KEY", os.environ.get("OPENAI_API_KEY", ""))
        self._client = None

    def _resolve_config(self) -> tuple[str, str, str]:
        """Resolve model, base_url, api_key with fallbacks."""
        model = self._model
        base_url = self._base_url
        api_key = self._api_key

        # Ollama as first backend. The base_url choice is INDEPENDENT of whether a
        # model was given explicitly: with OLLAMA_BASE set, `--model gemma3:12b` must
        # still hit Ollama (previously base_url stayed empty → AsyncOpenAI → 401).
        if not base_url:
            ollama_base = os.environ.get("OLLAMA_BASE") or os.environ.get("OLLAMA_URL", "")
            if ollama_base:
                base_url = ollama_base.rstrip("/")
                if not base_url.endswith("/v1"):
                    base_url += "/v1"
                api_key = api_key or os.environ.get("OLLAMA_API_KEY", "ollama")
                if not model:
                    model = "gemma3:12b"

        # OpenAI as second fallback (no Ollama, but an api_key is present).
        if not model and api_key:
            model = "gpt-4o-mini"
            base_url = "https://api.openai.com/v1"

        if not model:
            raise RuntimeError(
                "No LLM configured. Set AI_DETECT_MODEL + AI_DETECT_BASE_URL, "
                "or OLLAMA_BASE, or OPENAI_API_KEY."
            )

        return model, base_url, api_key

    async def _get_client(self):
        if self._client is not None:
            return self._client
        try:
            from openai import AsyncOpenAI

            model, base_url, api_key = self._resolve_config()
            self._client = AsyncOpenAI(
                api_key=api_key or "unused",
                base_url=base_url or None,
            )
            self._model = model
            return self._client
        except ImportError:
            raise RuntimeError("openai package not installed. pip install openai")

    async def judge_single(self, text: str) -> LlmVerdict:
        """Judge a single message."""
        client = await self._get_client()
        prompt = LLM_PROMPT_TEMPLATE.format(text=text[:2000])

        resp = await client.chat.completions.create(
            model=self._model,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=300,
            temperature=0.1,
        )
        raw = resp.choices[0].message.content.strip()
        return self._parse_single(raw)

    async def judge_batch(self, items: list[tuple[int, str]]) -> list[tuple[int, LlmVerdict]]:
        """Judge a batch of messages. Returns list of (original_id, verdict)."""
        if not items:
            return []
        client = await self._get_client()

        numbered = "\n".join(f"{i+1}. [id={mid}] {text[:800]}" for i, (mid, text) in enumerate(items))
        prompt = LLM_BATCH_PROMPT_TEMPLATE.format(messages=numbered)

        # Budget output tokens by batch size so the JSON array is not truncated
        # mid-object (a truncated array → JSONDecodeError → 0 verdicts → all uncertain).
        max_tokens = max(2000, len(items) * 120)
        resp = await client.chat.completions.create(
            model=self._model,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=max_tokens,
            temperature=0.1,
        )
        raw = resp.choices[0].message.content.strip()
        return self._parse_batch(raw, items)

    @staticmethod
    def _parse_single(raw: str) -> LlmVerdict:
        try:
            start = raw.find("{")
            end = raw.rfind("}") + 1
            if start >= 0 and end > start:
                data = json.loads(raw[start:end])
            else:
                data = {}
        except json.JSONDecodeError:
            data = {}

        verdict = data.get("verdict", "uncertain").lower()
        if verdict not in ("ai", "human", "uncertain"):
            verdict = "uncertain"
        return LlmVerdict(
            verdict=verdict,
            confidence=float(data.get("confidence", 0.5)),
            reasoning=data.get("reasoning", ""),
        )

    @staticmethod
    def _parse_batch(raw: str, items: list[tuple[int, str]]) -> list[tuple[int, LlmVerdict]]:
        data = LlmJudge._extract_json_array(raw)

        mids = {mid for mid, _ in items}
        results = []
        for pos, entry in enumerate(data):
            if not isinstance(entry, dict):
                continue
            eid = entry.get("id")
            # Match by real message_id first (prompt embeds [id={mid}]); fall back to
            # positional index (model often renumbers 1..N).
            if isinstance(eid, int) and eid in mids:
                mid = eid
            else:
                idx = (eid - 1) if isinstance(eid, int) else pos
                if not (0 <= idx < len(items)):
                    continue
                mid = items[idx][0]
            verdict = str(entry.get("verdict", "uncertain")).lower()
            if verdict not in ("ai", "human", "uncertain"):
                verdict = "uncertain"
            results.append((
                mid,
                LlmVerdict(
                    verdict=verdict,
                    confidence=float(entry.get("confidence", 0.5)),
                    reasoning=entry.get("reasoning", ""),
                ),
            ))
        return results

    @staticmethod
    def _extract_json_array(raw: str) -> list:
        """Parse a JSON array, tolerating ```json fences and truncated output.

        On a clean array → json.loads. On a truncated/garbled array (e.g. max_tokens
        cut it mid-object) → best-effort: pull each top-level {...} object out and
        parse individually, so the first N complete verdicts survive.
        """
        start = raw.find("[")
        end = raw.rfind("]") + 1
        if start >= 0 and end > start:
            try:
                return json.loads(raw[start:end])
            except json.JSONDecodeError:
                pass
        # best-effort: balance braces to extract individual objects
        objs = []
        depth = 0
        buf: list[str] = []
        for ch in raw:
            if ch == "{":
                if depth == 0:
                    buf = []
                depth += 1
            if depth > 0:
                buf.append(ch)
            if ch == "}":
                depth -= 1
                if depth == 0 and buf:
                    try:
                        objs.append(json.loads("".join(buf)))
                    except json.JSONDecodeError:
                        pass
                    buf = []
        return objs


# ---------------------------------------------------------------------------
# 4. Pipeline
# ---------------------------------------------------------------------------


@dataclass
class MessageRow:
    id: int
    channel_id: int
    text: str


@dataclass
class DetectionResult:
    message_id: int
    channel_id: int
    is_ai: int
    confidence: float
    method: str
    heuristic_score: float | None = None
    features_json: str | None = None
    llm_verdict: str | None = None
    model_used: str | None = None


class AiDetectionPipeline:
    """Orchestrate batch detection over the main messages table."""

    def __init__(
        self,
        main_db_path: Path,
        detect_db: AiDetectionDb,
        *,
        batch_size: int = BATCH_SIZE,
        use_llm: bool = True,
        dry_run: bool = False,
        llm_model: str | None = None,
        llm_base_url: str | None = None,
        llm_api_key: str | None = None,
    ):
        self._main_db_path = main_db_path
        self._detect_db = detect_db
        self._batch_size = batch_size
        self._use_llm = use_llm
        self._dry_run = dry_run
        self._llm_judge: LlmJudge | None = None
        self._llm_model = llm_model
        self._llm_base_url = llm_base_url
        self._llm_api_key = llm_api_key
        self._analyzer = HeuristicAnalyzer()
        self._stats = {"heuristic_only": 0, "llm_reviewed": 0, "total_processed": 0}

    def _init_llm(self) -> None:
        if self._llm_judge is None and self._use_llm:
            try:
                self._llm_judge = LlmJudge(
                    model=self._llm_model,
                    base_url=self._llm_base_url,
                    api_key=self._llm_api_key,
                )
                # Test configuration
                model, base_url, _ = self._llm_judge._resolve_config()
                logger.info("LLM judge: model=%s, base_url=%s", model, base_url)
            except RuntimeError as e:
                logger.warning("LLM judge unavailable: %s", e)
                self._use_llm = False

    def _fetch_batch(
        self,
        conn: sqlite3.Connection,
        after_id: int,
        channel_id: int | None,
        limit: int,
    ) -> list[MessageRow]:
        query = """
            SELECT m.id, m.channel_id, m.text
            FROM messages m
            WHERE m.message_kind = 'regular'
              AND m.text IS NOT NULL
              AND LENGTH(m.text) > ?
              AND m.id > ?
        """
        params: list = [MIN_TEXT_LENGTH, after_id]
        if channel_id is not None:
            query += " AND m.channel_id = ?"
            params.append(channel_id)
        query += " ORDER BY m.id ASC LIMIT ?"
        params.append(limit)

        cur = conn.execute(query, params)
        return [MessageRow(id=r[0], channel_id=r[1], text=r[2]) for r in cur.fetchall()]

    async def run(
        self,
        *,
        limit: int | None = None,
        channel_id: int | None = None,
        max_batches: int | None = None,
    ) -> None:
        self._init_llm()
        main_conn = sqlite3.connect(f"file:{self._main_db_path}?mode=ro", uri=True)
        main_conn.execute("PRAGMA journal_mode=WAL")
        analyzed_ids = self._detect_db.analyzed_ids() if not self._dry_run else set()
        logger.info("Already analyzed: %d messages", len(analyzed_ids))

        try:
            after_id = 0
            batch_num = 0
            total_done = 0

            while True:
                if max_batches and batch_num >= max_batches:
                    break

                fetch_size = self._batch_size
                if limit:
                    remaining = limit - total_done
                    fetch_size = min(self._batch_size, remaining)
                rows = self._fetch_batch(main_conn, after_id, channel_id, fetch_size)
                if not rows:
                    break

                # Filter out already analyzed
                if analyzed_ids:
                    last_fetched_id = rows[-1].id
                    rows = [r for r in rows if r.id not in analyzed_ids]
                    if not rows:
                        after_id = last_fetched_id
                        continue

                results = await self._process_batch(rows)
                after_id = rows[-1].id
                batch_num += 1
                total_done += len(results)
                self._stats["total_processed"] += len(results)

                if not self._dry_run:
                    self._save_results(results)
                    analyzed_ids.update(r.message_id for r in results)

                logger.info(
                    "Batch %d: %d msgs processed (total: %d)",
                    batch_num, len(results), total_done,
                )

                if limit and total_done >= limit:
                    break

            logger.info("Done. Total processed: %d", total_done)
            self._print_summary()
        finally:
            main_conn.close()

    async def _process_batch(self, rows: list[MessageRow]) -> list[DetectionResult]:
        results: list[DetectionResult] = []
        uncertain: list[tuple[MessageRow, DetectionResult]] = []

        for row in rows:
            features = self._analyzer.compute_features(row.text)
            is_ai, confidence, score = self._analyzer.classify(features)

            feat_json = json.dumps(
                {
                    "char_trigram_entropy": round(features.char_trigram_entropy, 3),
                    "burstiness": round(features.burstiness, 3),
                    "type_token_ratio": round(features.type_token_ratio, 3),
                    "sentence_starter_diversity": round(features.sentence_starter_diversity, 3),
                    "punctuation_regularity": round(features.punctuation_regularity, 3),
                    "formatting_density": round(features.formatting_density, 4),
                    "emoji_density": round(features.emoji_density, 3),
                    "list_marker_ratio": round(features.list_marker_ratio, 3),
                    "avg_sentence_length": round(features.avg_sentence_length, 1),
                    "word_count": features.word_count,
                    "sentence_count": features.sentence_count,
                },
                ensure_ascii=False,
            )

            det = DetectionResult(
                message_id=row.id,
                channel_id=row.channel_id,
                is_ai=1 if is_ai else 0,
                confidence=confidence,
                method="heuristic",
                heuristic_score=score,
                features_json=feat_json,
            )

            # Check if uncertain → needs LLM review
            if self._use_llm and HUMAN_THRESHOLD <= score <= AI_THRESHOLD:
                uncertain.append((row, det))
            else:
                results.append(det)
                self._stats["heuristic_only"] += 1

        # Process uncertain messages with LLM
        if uncertain and self._llm_judge:
            await self._process_uncertain(uncertain, results)

        return results

    async def _process_uncertain(
        self,
        uncertain: list[tuple[MessageRow, DetectionResult]],
        results: list[DetectionResult],
    ) -> None:
        # Batch LLM calls
        for i in range(0, len(uncertain), LLM_BATCH_SIZE):
            chunk = uncertain[i : i + LLM_BATCH_SIZE]
            items = [(row.id, row.text) for row, _ in chunk]

            try:
                verdicts = await self._llm_judge.judge_batch(items)
            except Exception as e:
                logger.warning("LLM batch failed: %s — keeping heuristic results", e)
                for _, det in chunk:
                    results.append(det)
                continue

            verdict_map = {mid: v for mid, v in verdicts}

            for row, det in chunk:
                v = verdict_map.get(row.id)
                if v:
                    det.method = "llm"
                    det.llm_verdict = v.reasoning
                    det.model_used = self._llm_judge._model
                    if v.verdict == "ai":
                        det.is_ai = 1
                        det.confidence = v.confidence
                    elif v.verdict == "human":
                        det.is_ai = 0
                        det.confidence = v.confidence
                    # "uncertain" keeps heuristic result
                results.append(det)
                self._stats["llm_reviewed"] += 1

    def _save_results(self, results: list[DetectionResult]) -> None:
        rows = [
            (
                r.message_id,
                r.channel_id,
                r.is_ai,
                r.confidence,
                r.method,
                r.heuristic_score,
                r.features_json,
                r.llm_verdict,
                r.model_used,
            )
            for r in results
        ]
        self._detect_db.insert_batch(rows)

    def _print_summary(self) -> None:
        logger.info("── Summary ──")
        logger.info("  Heuristic only: %d", self._stats["heuristic_only"])
        logger.info("  LLM reviewed:   %d", self._stats["llm_reviewed"])
        logger.info("  Total:          %d", self._stats["total_processed"])


# ---------------------------------------------------------------------------
# 5. CLI
# ---------------------------------------------------------------------------


def _cmd_run(args: argparse.Namespace) -> None:
    main_db = Path(args.db) if args.db else DEFAULT_MAIN_DB
    detect_db_path = Path(args.output) if args.output else DEFAULT_DETECT_DB

    if not main_db.exists():
        logger.error("Main DB not found: %s", main_db)
        sys.exit(1)

    detect_db = AiDetectionDb(detect_db_path)
    if not args.dry_run:
        detect_db.open()

    pipeline = AiDetectionPipeline(
        main_db_path=main_db,
        detect_db=detect_db,
        batch_size=args.batch_size,
        use_llm=not args.no_llm,
        dry_run=args.dry_run,
        llm_model=args.model,
    )

    asyncio.run(pipeline.run(
        limit=args.limit,
        channel_id=args.channel_id,
        max_batches=args.max_batches,
    ))

    if not args.dry_run:
        detect_db.close()


def _cmd_stats(args: argparse.Namespace) -> None:
    detect_db_path = Path(args.output) if args.output else DEFAULT_DETECT_DB
    if not detect_db_path.exists():
        logger.error("Detection DB not found: %s", detect_db_path)
        sys.exit(1)

    db = AiDetectionDb(detect_db_path)
    db.open()

    counts = db.count_results()
    by_method = db.count_by_method()
    top_channels = db.top_ai_channels(args.top)

    total = counts.get("total", 0)
    ai = counts.get("ai", 0)
    human = counts.get("human", 0)

    print("\n╔══════════════════════════════════════════╗")
    print("║        AI Detection Results              ║")
    print("╠══════════════════════════════════════════╣")
    print(f"║  Total analyzed:   {total:>10,}            ║")
    if total:
        print(f"║  AI predicted:     {ai:>10,} ({100*ai/total:5.1f}%)    ║")
        print(f"║  Human predicted:  {human:>10,} ({100*human/total:5.1f}%)    ║")
    print("║                                          ║")
    print("║  By method:                              ║")
    for method, cnt in by_method.items():
        print(f"║    {method:<16}: {cnt:>10,}            ║")
    print("║                                          ║")
    print(f"║  Top AI-heavy channels (top {args.top}):       ║")

    # Resolve channel titles from main DB
    main_db = Path(args.db) if args.db else DEFAULT_MAIN_DB
    channel_titles: dict[int, str] = {}
    if main_db.exists():
        try:
            conn = sqlite3.connect(f"file:{main_db}?mode=ro", uri=True)
            cur = conn.execute("SELECT channel_id, title FROM channels")
            channel_titles = {r[0]: (r[1] or f"ID {r[0]}") for r in cur.fetchall()}
            conn.close()
        except Exception:
            pass

    for ch_id, ai_cnt, ch_total, pct in top_channels:
        title = channel_titles.get(ch_id, f"ID {ch_id}")
        print(f"║    {title[:25]:<25} {float(pct):5.1f}% AI  ║")

    print("╚══════════════════════════════════════════╝\n")
    db.close()


def _cmd_message(args: argparse.Namespace) -> None:
    main_db = Path(args.db) if args.db else DEFAULT_MAIN_DB
    detect_db_path = Path(args.output) if args.output else DEFAULT_DETECT_DB

    # Read the message from main DB
    if not main_db.exists():
        logger.error("Main DB not found: %s", main_db)
        sys.exit(1)

    conn = sqlite3.connect(f"file:{main_db}?mode=ro", uri=True)
    cur = conn.execute(
        "SELECT id, channel_id, text, date FROM messages WHERE id = ?",
        (args.message_id,),
    )
    row = cur.fetchone()
    conn.close()

    if not row:
        logger.error("Message %d not found", args.message_id)
        sys.exit(1)

    msg_id, ch_id, text, date = row
    print(f"\n{'='*60}")
    print(f"Message #{msg_id}  |  channel_id={ch_id}  |  date={date}")
    print(f"{'='*60}")
    print(f"\nText:\n{text[:500]}{'...' if len(text) > 500 else ''}\n")

    # Compute features
    features = HeuristicAnalyzer.compute_features(text)
    is_ai, confidence, score = HeuristicAnalyzer.classify(features)

    print("── Heuristic Features ──")
    print(f"  char_trigram_entropy:      {features.char_trigram_entropy:.3f}")
    print(f"  burstiness (CV sent len):   {features.burstiness:.3f}")
    print(f"  type_token_ratio:           {features.type_token_ratio:.3f}")
    print(f"  sentence_starter_diversity: {features.sentence_starter_diversity:.3f}")
    print(f"  punctuation_regularity:     {features.punctuation_regularity:.3f}")
    print(f"  formatting_density:         {features.formatting_density:.4f}")
    print(f"  emoji_density:              {features.emoji_density:.3f}")
    print(f"  list_marker_ratio:          {features.list_marker_ratio:.3f}")
    print(f"  avg_sentence_length:        {features.avg_sentence_length:.1f}")
    print(f"  word_count:                 {features.word_count}")
    print(f"  sentence_count:             {features.sentence_count}")
    print(f"\n── Heuristic Score: {score:.3f} ──")
    print(f"  Prediction: {'🤖 AI' if is_ai else '👤 HUMAN'}")
    print(f"  Confidence: {confidence:.3f}")

    if HUMAN_THRESHOLD <= score <= AI_THRESHOLD:
        print(f"  ⚠ Score in uncertain range [{HUMAN_THRESHOLD}-{AI_THRESHOLD}]")

    # Check if LLM result exists
    if detect_db_path.exists():
        db = AiDetectionDb(detect_db_path)
        db.open()
        result = db.get_message_result(msg_id)
        db.close()
        if result:
            print("\n── Stored Result ──")
            print(f"  is_ai:       {result['is_ai']}")
            print(f"  confidence:   {result['confidence']}")
            print(f"  method:       {result['method']}")
            if result.get("llm_verdict"):
                print(f"  LLM verdict:  {result['llm_verdict']}")
            if result.get("model_used"):
                print(f"  model:        {result['model_used']}")

    # Optionally run LLM
    if args.llm and not (HUMAN_THRESHOLD <= score <= AI_THRESHOLD):
        print("\n  (Score is clear-cut, LLM review not needed)")
    elif args.llm and HUMAN_THRESHOLD <= score <= AI_THRESHOLD:
        print("\n── LLM Review ──")
        try:
            judge = LlmJudge()
            verdict = asyncio.run(judge.judge_single(text))
            label = {"ai": "🤖 AI", "human": "👤 HUMAN"}.get(verdict.verdict, "❓ UNCERTAIN")
            print(f"  Verdict:     {label}")
            print(f"  Confidence:  {verdict.confidence:.2f}")
            print(f"  Reasoning:   {verdict.reasoning}")
        except Exception as e:
            print(f"  LLM error: {e}")

    print()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="AI-generated text detection for Telegram messages",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    parser.add_argument("--db", help="Path to main tg_search.db")
    parser.add_argument("--output", help="Path to ai_detection.db")

    sub = parser.add_subparsers(dest="command")

    # run
    run_p = sub.add_parser("run", help="Run AI detection pipeline")
    run_p.add_argument("--limit", type=int, help="Max messages to process")
    run_p.add_argument("--channel-id", type=int, help="Analyze only one channel")
    run_p.add_argument("--batch-size", type=int, default=BATCH_SIZE)
    run_p.add_argument("--max-batches", type=int)
    run_p.add_argument("--no-llm", action="store_true", help="Skip LLM review")
    run_p.add_argument("--dry-run", action="store_true", help="Analyze without writing")
    run_p.add_argument("--model", help="LLM model override")

    # stats
    stats_p = sub.add_parser("stats", help="Show detection results summary")
    stats_p.add_argument("--top", type=int, default=15, help="Top N AI-heavy channels")

    # message
    msg_p = sub.add_parser("message", help="Detailed analysis of one message")
    msg_p.add_argument("message_id", type=int, help="messages.id to analyze")
    msg_p.add_argument("--llm", action="store_true", help="Also run LLM review if uncertain")

    args = parser.parse_args()

    if args.command == "run":
        _cmd_run(args)
    elif args.command == "stats":
        _cmd_stats(args)
    elif args.command == "message":
        _cmd_message(args)
    else:
        parser.print_help()
        sys.exit(1)


if __name__ == "__main__":
    main()
