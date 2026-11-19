"""Unit tests for per-backend model validation (tg_agent/agent/models.py, #1002)."""

from __future__ import annotations

import pytest

from tg_agent.agent.models import CLAUDE_MODEL_IDS, VALID_AGENT_BACKENDS, model_for_backend


def test_model_for_backend_accepts_claude_ids():
    """The claude backend keeps a model ID drawn from its own allow-list."""
    for model_id in CLAUDE_MODEL_IDS:
        assert model_for_backend("claude", model_id) == model_id


def test_model_for_backend_deepagents_always_none():
    """deepagents never honours a request model — its model is settings-led."""
    assert model_for_backend("deepagents", next(iter(CLAUDE_MODEL_IDS))) is None
    assert model_for_backend("deepagents", None) is None


@pytest.mark.parametrize("backend", [None, "", "unknown", "auto", "codex", "adk"])
def test_model_for_backend_unknown_backend_none(backend):
    """An unknown/None backend (including the removed codex/adk) cannot pin a model."""
    assert model_for_backend(backend, next(iter(CLAUDE_MODEL_IDS))) is None


@pytest.mark.parametrize("backend", ["claude", "deepagents", None])
def test_model_for_backend_none_model_stays_none(backend):
    """A None model (Авто) is preserved for every backend."""
    assert model_for_backend(backend, None) is None


def test_model_for_backend_unknown_model_dropped():
    """A model ID in no allow-list is dropped even for a valid backend."""
    assert model_for_backend("claude", "not-a-real-model") is None


def test_backend_override_accepts_only_in_process_backends():
    """The out-of-process codex/adk backends (and their mcp-server) are gone (#1868)."""
    assert VALID_AGENT_BACKENDS == {"auto", "claude", "deepagents"}
