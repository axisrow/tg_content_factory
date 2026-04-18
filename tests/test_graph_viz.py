"""Tests for graph_viz ASCII rendering and embedding service helpers."""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from src.cli.graph_viz import _config_summary, render_ascii
from src.models import PipelineEdge, PipelineGraph, PipelineNode, PipelineNodeType


# --- graph_viz tests ---


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
