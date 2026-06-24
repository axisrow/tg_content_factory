"""Tests for src/cli/commands/pipeline.py — CLI pipeline subcommands."""

from __future__ import annotations

import asyncio
import json
import sqlite3
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from src.cli.commands.pipeline import _parse_target_refs, _preview_text, run
from src.database import Database
from src.services.pipeline_service import PipelineTargetRef, PipelineValidationError
from tests.helpers import cli_ns as _ns

_PIPELINE_INIT_DB_TARGET = "src.cli.commands.pipeline.runtime.init_db"


def _read_pipeline_row(db_path: str, pipeline_id: int) -> dict | None:
    """Read pipeline row using a fresh sqlite3 connection (handler closes aiosqlite)."""
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        cur = conn.execute("SELECT * FROM content_pipelines WHERE id = ?", (pipeline_id,))
        row = cur.fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def _read_run_moderation(db_path: str, run_id: int) -> str | None:
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.execute("SELECT moderation_status FROM generation_runs WHERE id = ?", (run_id,))
        row = cur.fetchone()
        return row[0] if row else None
    finally:
        conn.close()


def _read_run_status(db_path: str, run_id: int) -> tuple[str | None, str | None]:
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.execute(
            "SELECT status, generated_text FROM generation_runs WHERE id = ?", (run_id,)
        )
        row = cur.fetchone()
        return (row[0], row[1]) if row else (None, None)
    finally:
        conn.close()


def _count_pipelines(db_path: str) -> int:
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.execute("SELECT COUNT(*) FROM content_pipelines")
        return cur.fetchone()[0]
    finally:
        conn.close()


def _open_db(db_path: str) -> Database:
    db = Database(db_path)
    asyncio.run(db.initialize())
    return db


def _close_db(db: Database) -> None:
    asyncio.run(db.close())


def _seed_minimal_pipeline(db_path: str, *, name: str = "P1") -> int:
    """Seed a minimal pipeline at db_path. Opens+closes its own Database.

    Uses import_json with empty source_ids/target_refs because PipelineService.add
    unconditionally validates channel/account existence (pipeline_service.py:138),
    whereas import_json has explicit `if source_ids else []` guards (lines 515-516)
    that bypass validation when the lists are empty.

    `pipeline_json={}` marks the pipeline as DAG (PipelineGraph(nodes=[], edges=[]))
    so svc.update() takes the `is_dag` branch (pipeline_service.py:210-212) and
    skips _normalize_sources entirely — needed for test_edit_renames_pipeline
    because _pipeline_ns defaults pass source=[100_001] which would otherwise be
    re-validated and rejected on the empty test DB.
    """
    from src.services.pipeline_service import PipelineService

    db = _open_db(db_path)
    try:
        svc = PipelineService(db)
        return asyncio.run(
            svc.import_json(
                {
                    "name": name,
                    "prompt_template": "generate something",
                    "source_ids": [],
                    "target_refs": [],
                    "pipeline_json": {},
                    "generate_interval_minutes": 60,
                }
            )
        )
    finally:
        _close_db(db)


def _seed_pipeline_with_graph(db_path: str, *, name: str = "DAG1") -> int:
    """Seed a pipeline with a minimal DAG: source → llm_generate → publish.

    Uses the canonical PipelineNodeType values; node ids match the
    `generate_node_id(type, counter)` convention so the CLI's auto-id logic
    (see pipeline.run for `pipeline node add`) yields predictable next ids.
    Edges serialize with `from`/`to` keys via PipelineGraph.to_json.
    """
    from src.services.pipeline_service import PipelineService

    graph_dict = {
        "nodes": [
            {"id": "source_0", "type": "source", "name": "source", "config": {}},
            {
                "id": "llm_generate_0",
                "type": "llm_generate",
                "name": "llm_generate",
                "config": {"prompt_template": "x"},
            },
            {"id": "publish_0", "type": "publish", "name": "publish", "config": {}},
        ],
        "edges": [
            {"from": "source_0", "to": "llm_generate_0"},
            {"from": "llm_generate_0", "to": "publish_0"},
        ],
    }
    db = _open_db(db_path)
    try:
        svc = PipelineService(db)
        return asyncio.run(
            svc.import_json(
                {
                    "name": name,
                    "prompt_template": "ignored for DAG",
                    # Empty sources/targets avoid PipelineService._normalize_sources
                    # validation against an empty Channel/Account table (see the
                    # `if source_ids else []` guards at pipeline_service.py:515-516).
                    "source_ids": [],
                    "target_refs": [],
                    "pipeline_json": graph_dict,
                    "generate_interval_minutes": 60,
                }
            )
        )
    finally:
        _close_db(db)


def _seed_generation_run(db_path: str, pipeline_id: int) -> int:
    db = _open_db(db_path)
    try:
        return asyncio.run(db.repos.generation_runs.create_run(pipeline_id, "test prompt"))
    finally:
        _close_db(db)


def _list_template_id(db_path: str) -> int | None:
    """Return the first built-in pipeline template id."""
    from src.services.pipeline_service import PipelineService

    db = _open_db(db_path)
    try:
        svc = PipelineService(db)
        templates = asyncio.run(svc.list_templates())
        return templates[0].id if templates else None
    finally:
        _close_db(db)


def _empty_db_path(tmp_path, name: str) -> str:
    """Create and initialize a fresh DB file, then close the connection."""
    db_path = str(tmp_path / name)
    db = _open_db(db_path)
    _close_db(db)
    return db_path

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


# ---------------------------------------------------------------------------
# RW-DB happy-path tests (gap-filling for issue #577)
# ---------------------------------------------------------------------------


def _pipeline_ns(action, **overrides):
    """Build a pipeline-action Namespace prefilled with the most common defaults."""
    defaults = {
        "pipeline_action": action,
        # `add` defaults
        "name": "P-test",
        "json_file": None,
        "node_specs": None,
        "prompt_template": "do a thing",
        "source": [100_001],
        "target": ["+70000000001|1"],
        "llm_model": None,
        "image_model": None,
        "interval": 60,
        "publish_mode": "moderated",
        "generation_backend": "chain",
        "inactive": False,
        "run_after": False,
        "since_value": 24,
        "since_unit": "h",
        "edge": None,
        "node_configs": None,
        # `edit` defaults (id resolved per test)
        "active": None,
        # `refinement-steps`
        "steps_json": None,
        # `filter` defaults
        "filter_action": None,
        "message_kinds": None,
        "service_actions": None,
        "media_types": None,
        "sender_kinds": None,
        "keywords": None,
        "regex": None,
        "forwarded": None,
        "has_text": None,
        # `node`/`edge`
        "node_action": None,
        "edge_action": None,
    }
    defaults.update(overrides)
    return _ns(**defaults)


def _pipeline_cli_run(db_path: str, cli_init_patch, ns):
    """Open a fresh Database at db_path, run pipeline command, then close.

    cli_init_patch with fresh_database=True asks the handler to open its own
    Database internally and closes it on exit, but it still needs a Database
    object to derive the file path from. So we open one here, hand it over,
    and close it after the handler returns.
    """
    db = _open_db(db_path)
    try:
        with cli_init_patch(db, _PIPELINE_INIT_DB_TARGET, fresh_database=True):
            run(ns)
    finally:
        _close_db(db)


class TestPipelineAddRW:
    def test_add_minimal_dag(self, tmp_path, cli_init_patch, capsys):
        """Use DAG mode (--node) so the CLI takes the import_json branch and
        passes an empty source_ids list — avoids _normalize_sources validation
        on a fresh DB without seeded channels/accounts.
        """
        db_path = _empty_db_path(tmp_path, "pipeline_add.db")

        _pipeline_cli_run(
            db_path,
            cli_init_patch,
            _pipeline_ns(
                "add",
                name="happy",
                node_specs=["source", "llm_generate:prompt_template=x", "publish"],
                source=None,
                target=None,
                prompt_template=None,
            ),
        )

        assert _count_pipelines(db_path) == 1
        out = capsys.readouterr().out
        assert "Added pipeline" in out and "happy" in out


class TestPipelineEditRW:
    def test_edit_renames_pipeline(self, tmp_path, cli_init_patch, capsys):
        db_path = _empty_db_path(tmp_path, "pipeline_edit.db")
        pid = _seed_minimal_pipeline(db_path, name="OldName")

        _pipeline_cli_run(
            db_path, cli_init_patch, _pipeline_ns("edit", id=pid, name="NewName")
        )

        row = _read_pipeline_row(db_path, pid)
        assert row is not None and row["name"] == "NewName"
        assert "Updated pipeline" in capsys.readouterr().out


class TestPipelineToggleRW:
    def test_toggle_flips_active(self, tmp_path, cli_init_patch, capsys):
        db_path = _empty_db_path(tmp_path, "pipeline_toggle.db")
        pid = _seed_minimal_pipeline(db_path)
        original = _read_pipeline_row(db_path, pid)["is_active"]

        _pipeline_cli_run(db_path, cli_init_patch, _pipeline_ns("toggle", id=pid))

        after = _read_pipeline_row(db_path, pid)["is_active"]
        assert after != original
        assert "Toggled pipeline" in capsys.readouterr().out


class TestPipelineApproveRW:
    def test_approve_marks_run_approved(self, tmp_path, cli_init_patch, capsys):
        db_path = _empty_db_path(tmp_path, "pipeline_approve.db")
        pid = _seed_minimal_pipeline(db_path)
        run_id = _seed_generation_run(db_path, pid)

        _pipeline_cli_run(
            db_path, cli_init_patch, _pipeline_ns("approve", run_id=run_id)
        )

        assert _read_run_moderation(db_path, run_id) == "approved"
        assert f"Approved run id={run_id}" in capsys.readouterr().out


class TestPipelineRejectRW:
    def test_reject_marks_run_rejected(self, tmp_path, cli_init_patch, capsys):
        db_path = _empty_db_path(tmp_path, "pipeline_reject.db")
        pid = _seed_minimal_pipeline(db_path)
        run_id = _seed_generation_run(db_path, pid)

        _pipeline_cli_run(db_path, cli_init_patch, _pipeline_ns("reject", run_id=run_id))

        assert _read_run_moderation(db_path, run_id) == "rejected"
        assert f"Rejected run id={run_id}" in capsys.readouterr().out


class TestPipelineRefinementStepsRW:
    def test_set_steps_persists_json(self, tmp_path, cli_init_patch, capsys):
        db_path = _empty_db_path(tmp_path, "pipeline_refine.db")
        pid = _seed_minimal_pipeline(db_path)

        steps = [{"instruction": "polish"}, {"instruction": "translate"}]
        _pipeline_cli_run(
            db_path,
            cli_init_patch,
            _pipeline_ns("refinement-steps", id=pid, steps_json=json.dumps(steps)),
        )

        row = _read_pipeline_row(db_path, pid)
        assert row is not None
        assert json.loads(row["refinement_steps"]) == steps
        out = capsys.readouterr().out
        assert f"Set 2 refinement step(s) for pipeline id={pid}" in out


class TestPipelineImportRW:
    def test_import_creates_pipeline(self, tmp_path, cli_init_patch, capsys):
        db_path = _empty_db_path(tmp_path, "pipeline_import.db")

        payload_path = tmp_path / "pipeline.json"
        payload_path.write_text(
            json.dumps(
                {
                    "name": "FromFile",
                    "prompt_template": "from file",
                    # Empty source_ids/target_refs bypass _normalize_sources/_targets
                    # validation; the test only verifies that import wires a pipeline
                    # row into content_pipelines, not the relation tables.
                    "source_ids": [],
                    "target_refs": [],
                    "generate_interval_minutes": 60,
                }
            )
        )

        _pipeline_cli_run(
            db_path,
            cli_init_patch,
            _pipeline_ns("import", file=str(payload_path), name=None),
        )

        assert _count_pipelines(db_path) == 1
        assert "Imported pipeline" in capsys.readouterr().out


class TestPipelineFromTemplateRW:
    def test_creates_pipeline_from_template(self, tmp_path, cli_init_patch, capsys):
        db_path = _empty_db_path(tmp_path, "pipeline_from_tpl.db")
        tpl_id = _list_template_id(db_path)
        assert tpl_id is not None, "No builtin pipeline templates available"

        _pipeline_cli_run(
            db_path,
            cli_init_patch,
            _pipeline_ns(
                "from-template",
                template_id=tpl_id,
                name="FromTpl",
                source_ids="",
                target_refs="",
            ),
        )

        assert _count_pipelines(db_path) == 1
        out = capsys.readouterr().out
        assert "Created pipeline from template" in out


class TestPipelineFilterRW:
    def test_filter_set_adds_filter_node(self, tmp_path, cli_init_patch, capsys):
        db_path = _empty_db_path(tmp_path, "pipeline_filter_set.db")
        pid = _seed_pipeline_with_graph(db_path)

        _pipeline_cli_run(
            db_path,
            cli_init_patch,
            _pipeline_ns(
                "filter", id=pid, filter_action="set", keywords=["hello"]
            ),
        )

        row = _read_pipeline_row(db_path, pid)
        graph = json.loads(row["pipeline_json"])
        types = [node["type"] for node in graph["nodes"]]
        assert "filter" in types
        assert "Updated filter" in capsys.readouterr().out

    def test_filter_clear_removes_filter_node(self, tmp_path, cli_init_patch, capsys):
        db_path = _empty_db_path(tmp_path, "pipeline_filter_clear.db")
        pid = _seed_pipeline_with_graph(db_path)

        _pipeline_cli_run(
            db_path,
            cli_init_patch,
            _pipeline_ns("filter", id=pid, filter_action="set", keywords=["a"]),
        )

        _pipeline_cli_run(
            db_path,
            cli_init_patch,
            _pipeline_ns("filter", id=pid, filter_action="clear"),
        )

        row = _read_pipeline_row(db_path, pid)
        graph = json.loads(row["pipeline_json"])
        types = [node["type"] for node in graph["nodes"]]
        assert "filter" not in types
        out = capsys.readouterr().out
        assert "Cleared filter" in out


def _edge_pairs(graph: dict) -> set[tuple[str, str]]:
    """Extract (from, to) tuples from a serialized pipeline_json graph.

    PipelineGraph.to_json writes edges with `from`/`to` keys (see models.py:275),
    so reading the persisted JSON must use the same keys.
    """
    return {(e["from"], e["to"]) for e in graph["edges"]}


class TestPipelineNodeRW:
    def test_node_add_appends(self, tmp_path, cli_init_patch, capsys):
        db_path = _empty_db_path(tmp_path, "pipeline_node_add.db")
        pid = _seed_pipeline_with_graph(db_path)

        _pipeline_cli_run(
            db_path,
            cli_init_patch,
            _pipeline_ns(
                "node",
                pipeline_id=pid,
                node_action="add",
                node_spec="image_generate",
            ),
        )

        row = _read_pipeline_row(db_path, pid)
        graph = json.loads(row["pipeline_json"])
        types = [node["type"] for node in graph["nodes"]]
        assert "image_generate" in types
        assert "Added node" in capsys.readouterr().out

    def test_node_replace_swaps(self, tmp_path, cli_init_patch, capsys):
        db_path = _empty_db_path(tmp_path, "pipeline_node_replace.db")
        pid = _seed_pipeline_with_graph(db_path)

        _pipeline_cli_run(
            db_path,
            cli_init_patch,
            _pipeline_ns(
                "node",
                pipeline_id=pid,
                node_action="replace",
                node_id="llm_generate_0",
                node_spec="llm_generate:prompt_template=replaced",
            ),
        )

        row = _read_pipeline_row(db_path, pid)
        graph = json.loads(row["pipeline_json"])
        gen_node = next(n for n in graph["nodes"] if n["id"] == "llm_generate_0")
        assert gen_node["config"].get("prompt_template") == "replaced"
        assert "Replaced node" in capsys.readouterr().out

    def test_node_remove_drops(self, tmp_path, cli_init_patch, capsys):
        db_path = _empty_db_path(tmp_path, "pipeline_node_remove.db")
        pid = _seed_pipeline_with_graph(db_path)

        # Pre-add a deletable extra node so the minimum src→gen→publish chain stays intact
        _pipeline_cli_run(
            db_path,
            cli_init_patch,
            _pipeline_ns(
                "node",
                pipeline_id=pid,
                node_action="add",
                node_spec="image_generate",
            ),
        )
        capsys.readouterr()

        row = _read_pipeline_row(db_path, pid)
        graph = json.loads(row["pipeline_json"])
        added = next(n for n in graph["nodes"] if n["type"] == "image_generate")
        added_id = added["id"]

        _pipeline_cli_run(
            db_path,
            cli_init_patch,
            _pipeline_ns(
                "node",
                pipeline_id=pid,
                node_action="remove",
                node_id=added_id,
            ),
        )

        row2 = _read_pipeline_row(db_path, pid)
        graph2 = json.loads(row2["pipeline_json"])
        ids = [n["id"] for n in graph2["nodes"]]
        assert added_id not in ids
        assert "Removed node" in capsys.readouterr().out


class TestPipelineEdgeRW:
    def test_edge_add_and_remove(self, tmp_path, cli_init_patch, capsys):
        db_path = _empty_db_path(tmp_path, "pipeline_edge.db")
        pid = _seed_pipeline_with_graph(db_path)

        # Add an extra node first so we have a fresh edge target
        _pipeline_cli_run(
            db_path,
            cli_init_patch,
            _pipeline_ns(
                "node",
                pipeline_id=pid,
                node_action="add",
                node_spec="image_generate",
            ),
        )
        capsys.readouterr()

        row = _read_pipeline_row(db_path, pid)
        graph = json.loads(row["pipeline_json"])
        new_node = next(n for n in graph["nodes"] if n["type"] == "image_generate")

        # edge add: llm_generate_0 → image_generate_0
        _pipeline_cli_run(
            db_path,
            cli_init_patch,
            _pipeline_ns(
                "edge",
                pipeline_id=pid,
                edge_action="add",
                from_node="llm_generate_0",
                to_node=new_node["id"],
            ),
        )

        row2 = _read_pipeline_row(db_path, pid)
        graph2 = json.loads(row2["pipeline_json"])
        assert ("llm_generate_0", new_node["id"]) in _edge_pairs(graph2)
        assert "Added edge" in capsys.readouterr().out

        # edge remove
        _pipeline_cli_run(
            db_path,
            cli_init_patch,
            _pipeline_ns(
                "edge",
                pipeline_id=pid,
                edge_action="remove",
                from_node="llm_generate_0",
                to_node=new_node["id"],
            ),
        )

        row3 = _read_pipeline_row(db_path, pid)
        graph3 = json.loads(row3["pipeline_json"])
        assert ("llm_generate_0", new_node["id"]) not in _edge_pairs(graph3)
        assert "Removed edge" in capsys.readouterr().out


class TestPipelineExportRW:
    def test_export_writes_new_file(self, tmp_path, cli_init_patch, capsys):
        db_path = _empty_db_path(tmp_path, "pipeline_export.db")
        pid = _seed_minimal_pipeline(db_path, name="Exp")
        out_file = tmp_path / "exported.json"

        _pipeline_cli_run(
            db_path,
            cli_init_patch,
            _pipeline_ns("export", id=pid, output=str(out_file), force=False),
        )

        assert out_file.exists()
        assert json.loads(out_file.read_text(encoding="utf-8"))["name"] == "Exp"
        assert f"Exported pipeline id={pid}" in capsys.readouterr().out

    def test_export_refuses_overwrite_without_force(self, tmp_path, cli_init_patch, capsys):
        db_path = _empty_db_path(tmp_path, "pipeline_export_noforce.db")
        pid = _seed_minimal_pipeline(db_path, name="Exp")
        out_file = tmp_path / "exists.json"
        out_file.write_text("ORIGINAL", encoding="utf-8")

        _pipeline_cli_run(
            db_path,
            cli_init_patch,
            _pipeline_ns("export", id=pid, output=str(out_file), force=False),
        )

        # File left untouched; user is told how to override.
        assert out_file.read_text(encoding="utf-8") == "ORIGINAL"
        assert "already exists" in capsys.readouterr().out

    def test_export_force_overwrites(self, tmp_path, cli_init_patch, capsys):
        db_path = _empty_db_path(tmp_path, "pipeline_export_force.db")
        pid = _seed_minimal_pipeline(db_path, name="Exp")
        out_file = tmp_path / "exists.json"
        out_file.write_text("ORIGINAL", encoding="utf-8")

        _pipeline_cli_run(
            db_path,
            cli_init_patch,
            _pipeline_ns("export", id=pid, output=str(out_file), force=True),
        )

        assert json.loads(out_file.read_text(encoding="utf-8"))["name"] == "Exp"


# ---------------------------------------------------------------------------
# pipeline generate-stream — mirrors web GET /pipelines/{id}/generate-stream
# ---------------------------------------------------------------------------


def _max_run_id(db_path: str) -> int | None:
    conn = sqlite3.connect(db_path)
    try:
        cur = conn.execute("SELECT MAX(id) FROM generation_runs")
        row = cur.fetchone()
        return row[0] if row and row[0] is not None else None
    finally:
        conn.close()


def _gen_stream_ns(**overrides):
    defaults = {
        "pipeline_action": "generate-stream",
        "model": None,
        "max_tokens": 256,
        "temperature": 0.0,
        "limit": 8,
    }
    defaults.update(overrides)
    return _ns(**defaults)


class _FakeGenerationService:
    """Async-generator double for GenerationService used by generate-stream."""

    def __init__(self, *args, **kwargs):
        pass

    async def generate_stream(self, *args, **kwargs):
        yield {"delta": "Hello", "generated_text": "Hello", "citations": []}
        yield {
            "delta": " world",
            "generated_text": "Hello world",
            "citations": [{"message_id": 1}],
        }


class TestPipelineGenerateStream:
    def test_not_found(self, cli_env, capsys):
        run(_gen_stream_ns(id=99999))
        assert "not found" in capsys.readouterr().out

    def test_streams_jsonlines_and_persists_run(self, tmp_path, cli_init_patch, capsys):
        db_path = _empty_db_path(tmp_path, "pipeline_gen_stream.db")
        pid = _seed_pipeline_with_graph(db_path, name="StreamDAG")

        provider = MagicMock()
        provider.load_db_providers = AsyncMock()
        provider.has_providers = MagicMock(return_value=True)
        provider.get_provider_callable = MagicMock(return_value=AsyncMock())

        db = _open_db(db_path)
        try:
            with cli_init_patch(db, _PIPELINE_INIT_DB_TARGET, fresh_database=True), patch(
                "src.services.provider_service.RuntimeProviderRegistry",
                return_value=provider,
            ), patch(
                "src.services.generation_service.GenerationService",
                _FakeGenerationService,
            ):
                run(_gen_stream_ns(id=pid))
        finally:
            _close_db(db)

        out = capsys.readouterr().out
        lines = [json.loads(ln) for ln in out.strip().splitlines() if ln.strip()]
        # Two delta updates + a final done event.
        assert lines[0]["delta"] == "Hello"
        assert lines[1]["text"] == "Hello world"
        assert lines[-1]["event"] == "done"
        run_id = lines[-1]["run_id"]

        status, generated = _read_run_status(db_path, run_id)
        assert status == "completed"
        assert generated == "Hello world"

    def test_midstream_break_marks_run_failed_not_completed(self, tmp_path, cli_init_patch, capsys):
        """A graceful mid-stream break (partial/stream_error final update) must
        persist the run as 'failed', not 'completed' (issue #1034, cycle-review).

        The real generate_stream no longer raises on a provider drop — it yields a
        final update flagged stream_error. The CLI handler must honor that flag and
        flip the run to failed, otherwise truncated text is saved as a completed run.
        The fake mirrors that exact output shape.
        """

        class _BreakingGenerationService:
            def __init__(self, *args, **kwargs):
                pass

            async def generate_stream(self, *args, **kwargs):
                yield {"delta": "partial", "generated_text": "partial", "citations": []}
                yield {
                    "delta": "",
                    "generated_text": "partial",
                    "citations": [],
                    "partial": True,
                    "stream_error": "provider dropped mid-stream",
                }

        db_path = _empty_db_path(tmp_path, "pipeline_gen_stream_break.db")
        pid = _seed_pipeline_with_graph(db_path, name="BreakDAG")

        provider = MagicMock()
        provider.load_db_providers = AsyncMock()
        provider.has_providers = MagicMock(return_value=True)
        provider.get_provider_callable = MagicMock(return_value=AsyncMock())

        db = _open_db(db_path)
        try:
            with cli_init_patch(db, _PIPELINE_INIT_DB_TARGET, fresh_database=True), patch(
                "src.services.provider_service.RuntimeProviderRegistry",
                return_value=provider,
            ), patch(
                "src.services.generation_service.GenerationService",
                _BreakingGenerationService,
            ):
                run(_gen_stream_ns(id=pid))
        finally:
            _close_db(db)

        out = capsys.readouterr().out
        lines = [json.loads(ln) for ln in out.strip().splitlines() if ln.strip()]
        assert lines[-1]["event"] == "error"
        assert "done" not in {ln.get("event") for ln in lines}

        run_id = _max_run_id(db_path)
        assert run_id is not None
        status, _ = _read_run_status(db_path, run_id)
        assert status == "failed"

    def test_cancellation_marks_run_failed(self, tmp_path, cli_init_patch, capsys):
        """Ctrl+C raises CancelledError (a BaseException, not Exception). The run
        must be flipped to "failed" and the exception re-raised, not left dangling
        in "running" forever. (#737)"""

        class _CancellingGenerationService:
            def __init__(self, *args, **kwargs):
                pass

            async def generate_stream(self, *args, **kwargs):
                yield {"delta": "Hi", "generated_text": "Hi", "citations": []}
                raise asyncio.CancelledError()

        db_path = _empty_db_path(tmp_path, "pipeline_gen_stream_cancel.db")
        pid = _seed_pipeline_with_graph(db_path, name="CancelDAG")

        provider = MagicMock()
        provider.load_db_providers = AsyncMock()
        provider.has_providers = MagicMock(return_value=True)
        provider.get_provider_callable = MagicMock(return_value=AsyncMock())

        db = _open_db(db_path)
        try:
            with cli_init_patch(db, _PIPELINE_INIT_DB_TARGET, fresh_database=True), patch(
                "src.services.provider_service.RuntimeProviderRegistry",
                return_value=provider,
            ), patch(
                "src.services.generation_service.GenerationService",
                _CancellingGenerationService,
            ):
                with pytest.raises(asyncio.CancelledError):
                    run(_gen_stream_ns(id=pid))
        finally:
            _close_db(db)

        run_id = _max_run_id(db_path)
        assert run_id is not None
        status, _ = _read_run_status(db_path, run_id)
        assert status == "failed"

    def test_no_llm_provider_aborts(self, tmp_path, cli_init_patch, capsys):
        db_path = _empty_db_path(tmp_path, "pipeline_gen_stream_noprov.db")
        pid = _seed_pipeline_with_graph(db_path, name="NoProv")

        provider = MagicMock()
        provider.load_db_providers = AsyncMock()
        provider.has_providers = MagicMock(return_value=False)

        db = _open_db(db_path)
        try:
            with cli_init_patch(db, _PIPELINE_INIT_DB_TARGET, fresh_database=True), patch(
                "src.services.provider_service.RuntimeProviderRegistry",
                return_value=provider,
            ):
                run(_gen_stream_ns(id=pid))
        finally:
            _close_db(db)

        assert "LLM provider is not configured" in capsys.readouterr().out
        # No run should have been created.
        assert _max_run_id(db_path) is None
