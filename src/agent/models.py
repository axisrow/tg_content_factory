from __future__ import annotations

CLAUDE_MODELS: tuple[tuple[str, str], ...] = (
    ("claude-sonnet-4-6", "Sonnet 4.6"),
    ("claude-opus-4-6", "Opus 4.6 (1M)"),
    ("claude-opus-4-6-0", "Opus 4.6"),
    ("claude-haiku-4-5-20251001", "Haiku 4.5"),
)

CLAUDE_MODEL_IDS: frozenset[str] = frozenset(m[0] for m in CLAUDE_MODELS)
