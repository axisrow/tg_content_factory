"""Tests for src/cli/commands/pipeline.py — CLI pipeline subcommands."""

from __future__ import annotations

import pytest

from src.cli.commands.pipeline import _parse_target_refs, _preview_text, run
from src.services.pipeline_service import PipelineTargetRef, PipelineValidationError
from tests.helpers import cli_ns as _ns

# ---------------------------------------------------------------------------
# _parse_target_refs — pure logic
# ---------------------------------------------------------------------------


class TestParseTargetRefs:
    def test_valid_single(self):
        result = _parse_target_refs(["+79001234567|1234567"])
        assert len(result) == 1
        assert result[0] == PipelineTargetRef(phone="+79001234567", dialog_id=1234567)

    def test_valid_multiple(self):
        result = _parse_target_refs(["phone1|100", "phone2|200"])
        assert len(result) == 2

    def test_missing_separator(self):
        with pytest.raises(PipelineValidationError, match="PHONE\\|DIALOG_ID"):
            _parse_target_refs(["no_separator"])

    def test_non_numeric_dialog_id(self):
        with pytest.raises(PipelineValidationError, match="numeric"):
            _parse_target_refs(["phone|abc"])

    def test_empty_list(self):
        assert _parse_target_refs([]) == []


# ---------------------------------------------------------------------------
# _preview_text — pure logic
# ---------------------------------------------------------------------------


class TestPreviewText:
    def test_none(self):
        assert _preview_text(None) == "—"

    def test_empty(self):
        assert _preview_text("") == "—"

    def test_short(self):
        assert _preview_text("hello world") == "hello world"

    def test_long_truncated(self):
        result = _preview_text("x" * 100)
        assert len(result) == 60
        assert result.endswith("...")

    def test_whitespace_collapsed(self):
        result = _preview_text("  hello   world  ")
        assert result == "hello world"

    def test_exactly_at_limit(self):
        text = "x" * 60
        assert _preview_text(text) == text


# ---------------------------------------------------------------------------
# Pipeline list
# ---------------------------------------------------------------------------


class TestPipelineList:
    def test_empty(self, cli_env, capsys):

        run(_ns(pipeline_action="list"))
        assert "No pipelines found." in capsys.readouterr().out


# ---------------------------------------------------------------------------
# Pipeline toggle / delete / show — with mock service
# ---------------------------------------------------------------------------


class TestPipelineToggle:
    def test_not_found(self, cli_env, capsys):

        run(_ns(pipeline_action="toggle", id=999))
        assert "not found" in capsys.readouterr().out


class TestPipelineDelete:
    def test_delete(self, cli_env, capsys):

        run(_ns(pipeline_action="delete", id=1))
        out = capsys.readouterr().out
        # The command always prints "Deleted" (no check)
        assert "Deleted pipeline id=1" in out


class TestPipelineShow:
    def test_not_found(self, cli_env, capsys):

        run(_ns(pipeline_action="show", id=999))
        assert "not found" in capsys.readouterr().out


# ---------------------------------------------------------------------------
# Pipeline runs
# ---------------------------------------------------------------------------


class TestPipelineRuns:
    def test_not_found(self, cli_env, capsys):

        run(_ns(pipeline_action="runs", id=999, limit=10, status=None))
        assert "not found" in capsys.readouterr().out


class TestPipelineRunShow:
    def test_not_found(self, cli_env, capsys):

        run(_ns(pipeline_action="run-show", run_id=999))
        assert "not found" in capsys.readouterr().out


# ---------------------------------------------------------------------------
# Pipeline approve / reject
# ---------------------------------------------------------------------------


class TestPipelineApprove:
    def test_not_found(self, cli_env, capsys):

        run(_ns(pipeline_action="approve", run_id=999))
        assert "not found" in capsys.readouterr().out


class TestPipelineReject:
    def test_not_found(self, cli_env, capsys):

        run(_ns(pipeline_action="reject", run_id=999))
        assert "not found" in capsys.readouterr().out


# ---------------------------------------------------------------------------
# Pipeline bulk-approve / bulk-reject
# ---------------------------------------------------------------------------


class TestPipelineBulkApprove:
    def test_not_found_run(self, cli_env, capsys):

        run(_ns(pipeline_action="bulk-approve", run_ids=[998, 999]))
        out = capsys.readouterr().out
        assert "Bulk approved: 0/2" in out


class TestPipelineBulkReject:
    def test_not_found_run(self, cli_env, capsys):

        run(_ns(pipeline_action="bulk-reject", run_ids=[998, 999]))
        out = capsys.readouterr().out
        assert "Bulk rejected: 0/2" in out


# ---------------------------------------------------------------------------
# Pipeline queue
# ---------------------------------------------------------------------------


class TestPipelineQueue:
    def test_not_found(self, cli_env, capsys):

        run(_ns(pipeline_action="queue", id=999, limit=10))
        assert "not found" in capsys.readouterr().out


# ---------------------------------------------------------------------------
# Pipeline add — validation errors
# ---------------------------------------------------------------------------


class TestPipelineAddLegacyErrors:
    def test_missing_prompt_template(self, cli_env, capsys):

        run(_ns(
            pipeline_action="add",
            name="test",
            json_file=None,
            node_specs=None,
            prompt_template=None,
            source=[100],
            target=["phone|123"],
            llm_model=None,
            image_model=None,
            interval=60,
            publish_mode="manual",
            generation_backend="search",
            inactive=False,
            run_after=False,
        ))
        assert "--prompt-template is required" in capsys.readouterr().out

    def test_missing_source(self, cli_env, capsys):

        run(_ns(
            pipeline_action="add",
            name="test",
            json_file=None,
            node_specs=None,
            prompt_template="test prompt",
            source=None,
            target=["phone|123"],
            llm_model=None,
            image_model=None,
            interval=60,
            publish_mode="manual",
            generation_backend="search",
            inactive=False,
            run_after=False,
        ))
        assert "--source is required" in capsys.readouterr().out

    def test_missing_target(self, cli_env, capsys):

        run(_ns(
            pipeline_action="add",
            name="test",
            json_file=None,
            node_specs=None,
            prompt_template="test prompt",
            source=[100],
            target=None,
            llm_model=None,
            image_model=None,
            interval=60,
            publish_mode="manual",
            generation_backend="search",
            inactive=False,
            run_after=False,
        ))
        assert "--target is required" in capsys.readouterr().out


# ---------------------------------------------------------------------------
# Pipeline refinement-steps
# ---------------------------------------------------------------------------


class TestPipelineRefinementSteps:
    def test_not_found(self, cli_env, capsys):

        run(_ns(pipeline_action="refinement-steps", id=999, steps_json=None))
        assert "not found" in capsys.readouterr().out


# ---------------------------------------------------------------------------
# Pipeline dry-run-count
# ---------------------------------------------------------------------------


class TestPipelineDryRunCount:
    def test_dry_run_count(self, cli_env, capsys):

        run(_ns(
            pipeline_action="dry-run-count",
            source=[100],
            since_value=1,
            since_unit="d",
        ))
        out = capsys.readouterr().out
        assert "Messages found:" in out


# ---------------------------------------------------------------------------
# Pipeline graph
# ---------------------------------------------------------------------------


class TestPipelineGraph:
    def test_not_found(self, cli_env, capsys):

        run(_ns(pipeline_action="graph", id=999))
        assert "not found" in capsys.readouterr().out
