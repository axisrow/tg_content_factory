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

# Per-backend allow-lists of model IDs accepted from the UI / API. deepagents is
# absent on purpose: its model comes from saved provider settings, never from the
# request, so it always resolves to None below.
_BACKEND_MODEL_IDS: dict[str, frozenset[str]] = {
    "claude": CLAUDE_MODEL_IDS,
    "codex": CODEX_MODEL_IDS,
    "adk": ADK_MODEL_IDS,
}


def model_for_backend(backend_name: str | None, model: str | None) -> str | None:
    """Return ``model`` only if it is valid for ``backend_name``, else ``None``.

    A model ID submitted for the wrong backend (e.g. a Claude ID left over in the
    browser while codex/adk is selected) is silently dropped to ``None`` (= auto)
    so the backend picks its own default. deepagents and unknown backends never
    accept a request-supplied model and always resolve to ``None`` (#1002).
    """
    allowed = _BACKEND_MODEL_IDS.get(backend_name or "")
    return model if allowed is not None and model in allowed else None
