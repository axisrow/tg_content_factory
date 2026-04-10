"""Tests for built-in pipeline templates."""
from __future__ import annotations

import pytest

from src.database.bundles import PipelineBundle
from src.models import Account, Channel, PipelineNodeType
from src.services.pipeline_service import PipelineService, PipelineTargetRef
from src.services.pipeline_templates_builtin import get_builtin_templates


def test_list_templates_not_empty():
    templates = get_builtin_templates()
    assert len(templates) > 0


def test_all_templates_have_required_fields():
    templates = get_builtin_templates()
    for t in templates:
        assert t.name, f"Template {t} has no name"
        assert t.description, f"Template {t.name} has no description"
        assert t.category, f"Template {t.name} has no category"
        assert t.is_builtin is True, f"Template {t.name} is not marked builtin"
        assert t.template_json is not None, f"Template {t.name} has no graph"


def test_all_template_graphs_have_nodes_and_edges():
    templates = get_builtin_templates()
    for t in templates:
        graph = t.template_json
        assert len(graph.nodes) > 0, f"Template {t.name} has no nodes"
        # Nodes must have unique IDs
        node_ids = [n.id for n in graph.nodes]
        assert len(node_ids) == len(set(node_ids)), f"Template {t.name} has duplicate node IDs"


def test_all_edges_reference_valid_nodes():
    templates = get_builtin_templates()
    for t in templates:
        graph = t.template_json
        node_ids = {n.id for n in graph.nodes}
        for edge in graph.edges:
            assert edge.from_node in node_ids, (
                f"Template {t.name}: edge from_node {edge.from_node} not in nodes"
            )
            assert edge.to_node in node_ids, (
                f"Template {t.name}: edge to_node {edge.to_node} not in nodes"
            )


def test_templates_have_expected_categories():
    templates = get_builtin_templates()
    categories = {t.category for t in templates}
    assert "content" in categories


def test_nodes_have_valid_types():
    templates = get_builtin_templates()
    valid_types = {t.value for t in PipelineNodeType}
    for t in templates:
        for node in t.template_json.nodes:
            assert node.type.value in valid_types, (
                f"Template {t.name}: node {node.id} has invalid type {node.type}"
            )


def test_content_generation_template_exists():
    templates = get_builtin_templates()
    content_templates = [t for t in templates if t.category == "content"]
    assert len(content_templates) >= 2  # text-only and text+image


# ------------------------------------------------------------------
# Template wiring tests (source/target injection)
# ------------------------------------------------------------------


@pytest.fixture
async def _seeded_db(db):
    """DB with accounts, channels, and builtin templates seeded."""
    await db.add_account(Account(phone="+100", session_string="sess"))
    await db.add_channel(Channel(channel_id=1001, title="Source A"))
    await db.add_channel(Channel(channel_id=1002, title="Source B"))
    await db.repos.dialog_cache.replace_dialogs(
        "+100",
        [{"channel_id": 77, "title": "Target A", "username": "targeta", "channel_type": "channel"}],
    )
    # Seed builtin templates
    templates = get_builtin_templates()
    await db.repos.pipeline_templates.ensure_builtins(templates)
    return db


async def _create_from_template(db, template_name, source_ids, target_refs=None):
    """Helper: create a pipeline from a builtin template by name."""
    svc = PipelineService(PipelineBundle.from_database(db))
    tpl = await db.repos.pipeline_templates.get_by_name(template_name)
    assert tpl is not None, f"Template '{template_name}' not found"
    return await svc.create_from_template(
        tpl.id,
        name=f"Test {template_name}",
        source_ids=source_ids,
        target_refs=target_refs or [],
    )


@pytest.mark.asyncio
async def test_forward_template_source_wiring(_seeded_db):
    db = _seeded_db
    pipeline_id = await _create_from_template(
        db, "Пересылка сообщений", [1001, 1002], [PipelineTargetRef(phone="+100", dialog_id=77)]
    )
    svc = PipelineService(PipelineBundle.from_database(db))
    pipeline = await svc.get(pipeline_id)
    graph = pipeline.pipeline_json

    source_node = next(n for n in graph.nodes if n.type == PipelineNodeType.SOURCE)
    assert source_node.config["channel_ids"] == [1001, 1002]

    forward_node = next(n for n in graph.nodes if n.type == PipelineNodeType.FORWARD)
    assert len(forward_node.config.get("targets", [])) == 1
    assert forward_node.config["targets"][0]["phone"] == "+100"
    assert forward_node.config["targets"][0]["dialog_id"] == 77


@pytest.mark.asyncio
async def test_react_template_source_wiring(_seeded_db):
    db = _seeded_db
    pipeline_id = await _create_from_template(db, "Реакции на сообщения", [1001])
    svc = PipelineService(PipelineBundle.from_database(db))
    pipeline = await svc.get(pipeline_id)

    source_node = next(n for n in pipeline.pipeline_json.nodes if n.type == PipelineNodeType.SOURCE)
    assert source_node.config["channel_ids"] == [1001]

    react_node = next(n for n in pipeline.pipeline_json.nodes if n.type == PipelineNodeType.REACT)
    assert react_node is not None


@pytest.mark.asyncio
async def test_cleanup_template_source_wiring(_seeded_db):
    db = _seeded_db
    pipeline_id = await _create_from_template(db, "Удаление join/leave сообщений", [1001])
    svc = PipelineService(PipelineBundle.from_database(db))
    pipeline = await svc.get(pipeline_id)
    graph = pipeline.pipeline_json

    source_node = next(n for n in graph.nodes if n.type == PipelineNodeType.SOURCE)
    assert source_node.config["channel_ids"] == [1001]

    filter_node = next(n for n in graph.nodes if n.type == PipelineNodeType.FILTER)
    assert "service_types" in filter_node.config

    delete_node = next(n for n in graph.nodes if n.type == PipelineNodeType.DELETE_MESSAGE)
    assert delete_node is not None


@pytest.mark.asyncio
async def test_template_without_targets_no_injection(_seeded_db):
    """Templates without source_ids should leave source node config empty."""
    db = _seeded_db
    svc = PipelineService(PipelineBundle.from_database(db))
    templates = get_builtin_templates()
    await db.repos.pipeline_templates.ensure_builtins(templates)
    tpl = await db.repos.pipeline_templates.get_by_name("Удаление join/leave сообщений")
    assert tpl is not None

    pipeline_id = await svc.create_from_template(
        tpl.id, name="No sources", source_ids=[], target_refs=[]
    )
    pipeline = await svc.get(pipeline_id)
    source_node = next(n for n in pipeline.pipeline_json.nodes if n.type == PipelineNodeType.SOURCE)
    assert source_node.config.get("channel_ids") == []
