from __future__ import annotations

CLAUDE_MODELS: tuple[tuple[str, str], ...] = (
    ("claude-sonnet-4-6", "Sonnet 4.6"),
    ("claude-opus-4-6", "Opus 4.6 (1M)"),
    ("claude-opus-4-6-0", "Opus 4.6"),
    ("claude-haiku-4-5-20251001", "Haiku 4.5"),
)

CLAUDE_MODEL_IDS: frozenset[str] = frozenset(m[0] for m in CLAUDE_MODELS)

# Only the IDs are consumed (model-validation in manager dispatch); unlike
# CLAUDE_MODELS, codex has no UI model picker, so no display-name table.
CODEX_MODEL_IDS: frozenset[str] = frozenset({"gpt-5.4"})

# Gemini model IDs the ADK backend accepts; like codex, no UI model picker, so
# no display-name table. Used for model-validation in manager dispatch.
ADK_MODEL_IDS: frozenset[str] = frozenset({"gemini-2.5-flash", "gemini-2.5-pro"})

# Valid values for the ``agent_backend_override`` setting. "auto" lets
# get_runtime_status pick; the rest force a specific backend (dev-mode only).
VALID_AGENT_BACKENDS: frozenset[str] = frozenset({"auto", "claude", "deepagents", "codex", "adk"})
