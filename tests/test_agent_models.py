"""Unit tests for per-backend model validation (src/agent/models.py, #1002)."""

from __future__ import annotations

import pytest

from src.agent.models import (
    ADK_MODEL_IDS,
    CLAUDE_MODEL_IDS,
    CODEX_MODEL_IDS,
    model_for_backend,
)


@pytest.mark.parametrize(
    "backend, ids",
    [
        ("claude", CLAUDE_MODEL_IDS),
        ("codex", CODEX_MODEL_IDS),
        ("adk", ADK_MODEL_IDS),
    ],
)
def test_model_for_backend_accepts_own_ids(backend, ids):
    """Each backend keeps a model ID drawn from its own allow-list."""
    for model_id in ids:
        assert model_for_backend(backend, model_id) == model_id


def test_model_for_backend_drops_claude_id_for_codex():
    """A stale Claude ID submitted while codex is active is coerced to None (#1002)."""
    claude_id = next(iter(CLAUDE_MODEL_IDS))
    assert claude_id not in CODEX_MODEL_IDS
    assert model_for_backend("codex", claude_id) is None


def test_model_for_backend_drops_claude_id_for_adk():
    """A stale Claude ID submitted while adk is active is coerced to None (#1002)."""
    claude_id = next(iter(CLAUDE_MODEL_IDS))
    assert claude_id not in ADK_MODEL_IDS
    assert model_for_backend("adk", claude_id) is None


def test_model_for_backend_deepagents_always_none():
    """deepagents never honours a request model — its model is settings-led."""
    assert model_for_backend("deepagents", next(iter(CLAUDE_MODEL_IDS))) is None
    assert model_for_backend("deepagents", None) is None


@pytest.mark.parametrize("backend", [None, "", "unknown", "auto"])
def test_model_for_backend_unknown_backend_none(backend):
    """An unknown/None backend cannot pin a model — always None."""
    assert model_for_backend(backend, next(iter(CLAUDE_MODEL_IDS))) is None


@pytest.mark.parametrize("backend", ["claude", "codex", "adk", "deepagents", None])
def test_model_for_backend_none_model_stays_none(backend):
    """A None model (Авто) is preserved for every backend."""
    assert model_for_backend(backend, None) is None


def test_model_for_backend_unknown_model_dropped():
    """A model ID in no allow-list is dropped even for a valid backend."""
    assert model_for_backend("claude", "not-a-real-model") is None
    assert model_for_backend("codex", "gpt-9.9-imaginary") is None
