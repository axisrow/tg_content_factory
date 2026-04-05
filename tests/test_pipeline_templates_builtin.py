"""Tests for built-in pipeline templates."""
from __future__ import annotations

from src.models import PipelineNodeType
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
