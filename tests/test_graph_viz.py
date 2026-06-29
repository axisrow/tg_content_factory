"""Tests for graph_viz ASCII rendering and embedding service helpers."""
from __future__ import annotations

from collections import defaultdict
from unittest.mock import AsyncMock, MagicMock

import pytest

from src.cli.graph_viz import _config_summary, render_ascii
from src.models import PipelineEdge, PipelineGraph, PipelineNode, PipelineNodeType

# --- graph_viz tests ---


def _legacy_render_ascii_manual_counter(graph: PipelineGraph) -> str:
    if not graph.nodes:
        return "(empty graph)"

    node_by_id = {n.id: n for n in graph.nodes}
    outgoing: dict[str, list[str]] = defaultdict(list)
    for edge in graph.edges:
        outgoing[edge.from_node].append(edge.to_node)

    in_degree: dict[str, int] = {n.id: 0 for n in graph.nodes}
    for edge in graph.edges:
        in_degree[edge.to_node] = in_degree.get(edge.to_node, 0) + 1
    queue = [nid for nid, d in in_degree.items() if d == 0]
    order: list[str] = []
    while queue:
        queue.sort()
        nid = queue.pop(0)
        order.append(nid)
        for child in outgoing.get(nid, []):
            in_degree[child] -= 1
            if in_degree[child] == 0:
                queue.append(child)

    remaining = [n.id for n in graph.nodes if n.id not in set(order)]
    order.extend(remaining)

    lines: list[str] = []
    rendered: set[str] = set()
    for idx, nid in enumerate(list(order)):
        if nid in rendered:
            continue
        node = node_by_id.get(nid)
        if node is None:
            continue
        config_summary = _config_summary(node.config)
        lines.append(f"[{node.type.value}] id={node.id}{config_summary}")

        children = outgoing.get(nid, [])
        if idx < len(order) - 1 or children:
            if len(children) <= 1:
                next_nid = children[0] if children else None
                if next_nid and next_nid in node_by_id:
                    lines.append("   |")
                    lines.append("   v")
            else:
                prefix = "   "
                for ci, child_id in enumerate(children):
                    child = node_by_id.get(child_id)
                    if child is None:
                        continue
                    c_summary = _config_summary(child.config)
                    label = f"[{child.type.value}] id={child.id}{c_summary}"
                    if ci == 0:
                        lines.append(f"{prefix}|")
                        lines.append(f"{prefix}+---> {label}")
                    else:
                        lines.append(f"{prefix}|")
                        lines.append(f"{prefix}+---> {label}")
                rendered.update(children)

    return "\n".join(lines)


def _viz_node(
    node_id: str,
    node_type: PipelineNodeType = PipelineNodeType.LLM_GENERATE,
    config: dict | None = None,
) -> PipelineNode:
    return PipelineNode(id=node_id, name=node_id, type=node_type, config=config or {})


def test_render_empty_graph():
    graph = PipelineGraph(nodes=[], edges=[])
    assert render_ascii(graph) == "(empty graph)"


def test_render_single_node():
    node = PipelineNode(id="n1", name="gen", type=PipelineNodeType.LLM_GENERATE, config={})
    graph = PipelineGraph(nodes=[node], edges=[])
    result = render_ascii(graph)
    assert "n1" in result
    assert "llm_generate" in result


def test_render_linear_chain():
    n1 = PipelineNode(id="n1", name="gen", type=PipelineNodeType.LLM_GENERATE, config={})
    n2 = PipelineNode(id="n2", name="pub", type=PipelineNodeType.PUBLISH, config={})
    edge = PipelineEdge(from_node="n1", to_node="n2")
    graph = PipelineGraph(nodes=[n1, n2], edges=[edge])
    result = render_ascii(graph)
    assert "n1" in result
    assert "n2" in result
    assert "|" in result
    assert "v" in result


def test_render_fan_out():
    n1 = PipelineNode(id="n1", name="gen", type=PipelineNodeType.LLM_GENERATE, config={})
    n2 = PipelineNode(id="n2", name="pub1", type=PipelineNodeType.PUBLISH, config={})
    n3 = PipelineNode(id="n3", name="pub2", type=PipelineNodeType.PUBLISH, config={})
    graph = PipelineGraph(
        nodes=[n1, n2, n3],
        edges=[
            PipelineEdge(from_node="n1", to_node="n2"),
            PipelineEdge(from_node="n1", to_node="n3"),
        ],
    )
    result = render_ascii(graph)
    assert "n1" in result
    assert "+--->" in result


def test_render_ascii_matches_legacy_manual_counter_cases():
    cases = [
        PipelineGraph(nodes=[], edges=[]),
        PipelineGraph(
            nodes=[
                _viz_node("src", PipelineNodeType.SOURCE),
                _viz_node("gen"),
                _viz_node("pub", PipelineNodeType.PUBLISH),
            ],
            edges=[
                PipelineEdge(from_node="src", to_node="gen"),
                PipelineEdge(from_node="gen", to_node="pub"),
            ],
        ),
        PipelineGraph(
            nodes=[
                _viz_node("src", PipelineNodeType.SOURCE, {"topic": "news"}),
                _viz_node("pub1", PipelineNodeType.PUBLISH),
                _viz_node("pub2", PipelineNodeType.PUBLISH),
            ],
            edges=[
                PipelineEdge(from_node="src", to_node="pub1"),
                PipelineEdge(from_node="src", to_node="pub2"),
            ],
        ),
        PipelineGraph(
            nodes=[_viz_node("a"), _viz_node("b"), _viz_node("orphan", PipelineNodeType.NOTIFY)],
            edges=[
                PipelineEdge(from_node="a", to_node="b"),
                PipelineEdge(from_node="a", to_node="b"),
                PipelineEdge(from_node="missing", to_node="a"),
            ],
        ),
    ]

    for graph in cases:
        assert render_ascii(graph) == _legacy_render_ascii_manual_counter(graph)


def test_config_summary_empty():
    assert _config_summary({}) == ""


def test_config_summary_short():
    result = _config_summary({"key": "value"})
    assert "key=value" in result


def test_config_summary_truncated():
    result = _config_summary({"key": "a" * 50})
    assert "..." in result


def test_config_summary_many_keys():
    config = {f"k{i}": str(i) for i in range(5)}
    result = _config_summary(config)
    assert "..." in result


# --- EmbeddingRuntimeConfig tests ---


def test_embedding_config_model_ref_with_colon():
    from src.services.embedding_service import EmbeddingRuntimeConfig

    cfg = EmbeddingRuntimeConfig(
        provider="openai", model="openai:gpt-4", api_key="k", base_url="", batch_size=32,
    )
    assert cfg.model_ref == "openai:gpt-4"


def test_embedding_config_model_ref_without_colon():
    from src.services.embedding_service import EmbeddingRuntimeConfig

    cfg = EmbeddingRuntimeConfig(
        provider="openai", model="text-embedding-3-small", api_key="k", base_url="", batch_size=32,
    )
    assert cfg.model_ref == "openai:text-embedding-3-small"


def test_embedding_service_init_with_db():
    from src.services.embedding_service import EmbeddingService

    db = MagicMock()
    svc = EmbeddingService(db)
    assert svc._embeddings is None


def test_embedding_service_init_with_search_bundle():
    from src.services.embedding_service import EmbeddingService

    bundle = MagicMock()
    svc = EmbeddingService(bundle)
    assert svc._search is bundle


async def test_embedding_service_get_embeddings_no_vec_or_numpy():
    from src.services.embedding_service import EmbeddingService

    bundle = MagicMock()
    bundle.vec_available = False
    bundle.numpy_available = False
    bundle.get_setting = AsyncMock(return_value=None)
    svc = EmbeddingService(bundle)
    with pytest.raises(RuntimeError, match="unavailable"):
        await svc._get_embeddings()
