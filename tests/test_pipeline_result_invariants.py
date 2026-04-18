"""Unit tests for pipeline result semantic primitives (issue #463).

Covers:
  - summarize_result() — all (generation, action, mixed, empty) cases.
  - increment_action_count() — accumulation, zero/negative amounts.
  - get_action_counts() — malformed input handling.
  - GenerationRun.result_kind/result_count — all fallback branches.
  - result_kind_label() — Russian labels.
"""

from __future__ import annotations

import pytest

from src.models import GenerationRun
from src.services.pipeline_result import (
    RESULT_KIND_GENERATED_ITEMS,
    RESULT_KIND_PROCESSED_MESSAGES,
    get_action_counts,
    increment_action_count,
    result_kind_label,
    summarize_result,
)

# ── summarize_result ─────────────────────────────────────────────────────────


class TestSummarizeResult:
    def test_generation_with_citations(self):
        kind, count = summarize_result(
            generated_text="hello", citations=[{"id": 1}, {"id": 2}], action_counts=None
        )
        assert kind == RESULT_KIND_GENERATED_ITEMS
        assert count == 2

    def test_generation_with_text_no_citations(self):
        kind, count = summarize_result(generated_text="hello", citations=[], action_counts=None)
        assert kind == RESULT_KIND_GENERATED_ITEMS
        assert count == 1

    def test_generation_whitespace_only_text_falls_through(self):
        kind, count = summarize_result(
            generated_text="   \n", citations=[], action_counts={"react": 2}
        )
        assert kind == RESULT_KIND_PROCESSED_MESSAGES
        assert count == 2

    def test_action_only(self):
        kind, count = summarize_result(
            generated_text=None, citations=None, action_counts={"react": 3, "forward": 2}
        )
        assert kind == RESULT_KIND_PROCESSED_MESSAGES
        assert count == 5

    def test_mixed_generation_wins(self):
        """Mixed: generation present AND actions performed.

        Per issue #463: generation semantics wins for result_kind/result_count;
        action_counts survives in metadata (ContentGenerationService responsibility).
        """
        kind, count = summarize_result(
            generated_text="draft",
            citations=[{"id": 1}, {"id": 2}],
            action_counts={"react": 7},
        )
        assert kind == RESULT_KIND_GENERATED_ITEMS
        assert count == 2

    def test_empty_everything(self):
        kind, count = summarize_result(generated_text=None, citations=None, action_counts=None)
        assert kind == RESULT_KIND_PROCESSED_MESSAGES
        assert count == 0

    def test_action_counts_with_zero_values(self):
        kind, count = summarize_result(
            generated_text=None, citations=[], action_counts={"react": 0, "forward": 0}
        )
        assert kind == RESULT_KIND_PROCESSED_MESSAGES
        assert count == 0

    def test_action_counts_negative_values_clamped(self):
        kind, count = summarize_result(
            generated_text=None, citations=[], action_counts={"react": -5, "forward": 3}
        )
        assert kind == RESULT_KIND_PROCESSED_MESSAGES
        assert count == 3


# ── increment_action_count ───────────────────────────────────────────────────


class _FakeContext:
    """Minimal context substitute mirroring NodeContext global-store API."""

    def __init__(self):
        self._globals: dict[str, object] = {}

    def get_global(self, key, default=None):
        return self._globals.get(key, default)

    def set_global(self, key, value):
        self._globals[key] = value


class TestIncrementActionCount:
    def test_single_increment(self):
        ctx = _FakeContext()
        increment_action_count(ctx, "react")
        assert ctx.get_global("action_counts") == {"react": 1}

    def test_accumulates_same_action(self):
        ctx = _FakeContext()
        increment_action_count(ctx, "react")
        increment_action_count(ctx, "react", amount=3)
        assert ctx.get_global("action_counts") == {"react": 4}

    def test_mixed_actions(self):
        ctx = _FakeContext()
        increment_action_count(ctx, "react", amount=2)
        increment_action_count(ctx, "forward", amount=1)
        increment_action_count(ctx, "delete_message", amount=5)
        assert ctx.get_global("action_counts") == {
            "react": 2,
            "forward": 1,
            "delete_message": 5,
        }

    def test_zero_amount_noop(self):
        ctx = _FakeContext()
        increment_action_count(ctx, "react", amount=0)
        assert ctx.get_global("action_counts") is None

    def test_negative_amount_noop(self):
        ctx = _FakeContext()
        increment_action_count(ctx, "react", amount=-3)
        assert ctx.get_global("action_counts") is None


class TestGetActionCounts:
    def test_empty(self):
        ctx = _FakeContext()
        assert get_action_counts(ctx) == {}

    def test_filters_non_string_keys(self):
        ctx = _FakeContext()
        ctx.set_global("action_counts", {"react": 3, 42: 1})
        assert get_action_counts(ctx) == {"react": 3}

    def test_coerces_float_values(self):
        ctx = _FakeContext()
        ctx.set_global("action_counts", {"react": 3.7, "forward": 2})
        result = get_action_counts(ctx)
        assert result == {"react": 3, "forward": 2}


# ── GenerationRun.result_kind / result_count ─────────────────────────────────


class TestGenerationRunResultKind:
    def test_metadata_kind_wins(self):
        run = GenerationRun(metadata={"result_kind": "processed_messages"}, generated_text="hi")
        assert run.result_kind == "processed_messages"

    def test_falls_back_to_generated_text(self):
        run = GenerationRun(metadata={}, generated_text="hi")
        assert run.result_kind == "generated_items"

    def test_falls_back_to_processed_when_no_text(self):
        run = GenerationRun(metadata={}, generated_text=None)
        assert run.result_kind == "processed_messages"

    def test_none_metadata_treated_as_empty(self):
        run = GenerationRun(metadata=None, generated_text="hi")
        assert run.result_kind == "generated_items"

    def test_non_string_kind_ignored(self):
        run = GenerationRun(metadata={"result_kind": 42}, generated_text="hi")
        assert run.result_kind == "generated_items"

    def test_empty_string_kind_ignored(self):
        run = GenerationRun(metadata={"result_kind": ""}, generated_text="hi")
        assert run.result_kind == "generated_items"


class TestGenerationRunResultCount:
    def test_metadata_count_int(self):
        run = GenerationRun(metadata={"result_count": 5})
        assert run.result_count == 5

    def test_metadata_count_float_coerced(self):
        run = GenerationRun(metadata={"result_count": 3.7})
        assert run.result_count == 3

    def test_falls_back_to_citations_length(self):
        run = GenerationRun(metadata={"citations": [{"id": 1}, {"id": 2}, {"id": 3}]})
        assert run.result_count == 3

    def test_falls_back_to_generated_text_truthy(self):
        run = GenerationRun(metadata={}, generated_text="hello")
        assert run.result_count == 1

    def test_empty_text_no_metadata(self):
        run = GenerationRun(metadata={}, generated_text=None)
        assert run.result_count == 0

    def test_count_zero_still_zero(self):
        run = GenerationRun(metadata={"result_count": 0}, generated_text="hi")
        assert run.result_count == 0


# ── REGRESSION: empty text but positive count ────────────────────────────────


class TestEmptyTextPositiveCountRegression:
    """Issue #463: action-only run has empty generated_text but result_count > 0.

    UI/CLI layers must never show "0 messages" for such runs.
    """

    def test_action_only_run_has_positive_count_with_empty_text(self):
        run = GenerationRun(
            metadata={
                "result_kind": "processed_messages",
                "result_count": 7,
                "action_counts": {"react": 7},
            },
            generated_text="",
        )
        assert run.result_kind == "processed_messages"
        assert run.result_count == 7
        assert (run.generated_text or "") == ""


# ── result_kind_label ────────────────────────────────────────────────────────


class TestResultKindLabel:
    def test_processed_messages_label(self):
        assert result_kind_label("processed_messages") == "Обработано"

    def test_generated_items_label(self):
        assert result_kind_label("generated_items") == "Сгенерировано"

    def test_unknown_defaults_to_generated(self):
        assert result_kind_label("unknown_kind") == "Сгенерировано"

    def test_none_defaults_to_generated(self):
        assert result_kind_label(None) == "Сгенерировано"


# Ensure pytest doesn't get confused about the suite being empty.
if __name__ == "__main__":  # pragma: no cover
    pytest.main([__file__, "-v"])
