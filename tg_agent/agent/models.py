from __future__ import annotations

CLAUDE_MODELS: tuple[tuple[str, str], ...] = (
    ("claude-sonnet-4-6", "Sonnet 4.6"),
    ("claude-opus-4-6", "Opus 4.6 (1M)"),
    ("claude-opus-4-6-0", "Opus 4.6"),
    ("claude-haiku-4-5-20251001", "Haiku 4.5"),
)

CLAUDE_MODEL_IDS: frozenset[str] = frozenset(m[0] for m in CLAUDE_MODELS)

# Valid values for the ``agent_backend_override`` setting. "auto" lets
# get_runtime_status pick; the rest force a specific backend (dev-mode only).
VALID_AGENT_BACKENDS: frozenset[str] = frozenset({"auto", "claude", "deepagents"})
# Spellings operators and the agent's own tool docs used for a backend; stored
# verbatim they were silently ignored (the runtime only reads the names above).
_AGENT_BACKEND_ALIASES: dict[str, str] = {"claude-agent-sdk": "claude"}


def normalize_agent_backend(value: str) -> str | None:
    """The canonical ``agent_backend_override`` value for ``value``, or ``None`` if unknown."""
    name = _AGENT_BACKEND_ALIASES.get(value.strip(), value.strip())
    return name if name in VALID_AGENT_BACKENDS else None

# Per-backend allow-lists of model IDs accepted from the UI / API. deepagents is
# absent on purpose: its model comes from saved provider settings, never from the
# request, so it always resolves to None below.
_BACKEND_MODEL_IDS: dict[str, frozenset[str]] = {
    "claude": CLAUDE_MODEL_IDS,
}


def model_for_backend(backend_name: str | None, model: str | None) -> str | None:
    """Return ``model`` only if it is valid for ``backend_name``, else ``None``.

    A model ID submitted for the wrong backend is silently dropped to ``None``
    (= auto) so the backend picks its own default. deepagents and unknown
    backends never accept a request-supplied model and always resolve to
    ``None`` (#1002).
    """
    allowed = _BACKEND_MODEL_IDS.get(backend_name or "")
    return model if allowed is not None and model in allowed else None
