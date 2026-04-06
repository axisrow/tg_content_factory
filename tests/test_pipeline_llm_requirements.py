"""Unit tests for pipeline_needs_llm helper."""
from __future__ import annotations

from src.models import (
    ContentPipeline,
    PipelineEdge,
    PipelineGenerationBackend,
    PipelineGraph,
    PipelineNode,
    PipelineNodeType,
)
from src.services.pipeline_llm_requirements import pipeline_needs_llm


def _base_pipeline(**overrides) -> ContentPipeline:
    defaults = dict(
        id=1,
        name="test",
        prompt_template="hello",
        generation_backend=PipelineGenerationBackend.CHAIN,
    )
    defaults.update(overrides)
    return ContentPipeline(**defaults)


def test_legacy_chain_needs_llm() -> None:
    pipeline = _base_pipeline(pipeline_json=None)
    assert pipeline_needs_llm(pipeline) is True


def test_agent_backend_always_needs_llm() -> None:
    pipeline = _base_pipeline(generation_backend=PipelineGenerationBackend.AGENT)
    assert pipeline_needs_llm(pipeline) is True


def test_deep_agents_backend_always_needs_llm() -> None:
    pipeline = _base_pipeline(generation_backend=PipelineGenerationBackend.DEEP_AGENTS)
    assert pipeline_needs_llm(pipeline) is True


def test_dag_with_llm_generate_node_needs_llm() -> None:
    graph = PipelineGraph(
        nodes=[
            PipelineNode(id="src", type=PipelineNodeType.SOURCE, name="src"),
            PipelineNode(id="llm", type=PipelineNodeType.LLM_GENERATE, name="gen"),
            PipelineNode(id="pub", type=PipelineNodeType.PUBLISH, name="pub"),
        ],
        edges=[
            PipelineEdge(from_node="src", to_node="llm"),
            PipelineEdge(from_node="llm", to_node="pub"),
        ],
    )
    pipeline = _base_pipeline(pipeline_json=graph)
    assert pipeline_needs_llm(pipeline) is True


def test_dag_with_llm_refine_node_needs_llm() -> None:
    graph = PipelineGraph(
        nodes=[
            PipelineNode(id="src", type=PipelineNodeType.SOURCE, name="src"),
            PipelineNode(id="refine", type=PipelineNodeType.LLM_REFINE, name="refine"),
        ],
    )
    pipeline = _base_pipeline(pipeline_json=graph)
    assert pipeline_needs_llm(pipeline) is True


def test_dag_pure_forward_does_not_need_llm() -> None:
    """SOURCE → FORWARD → PUBLISH pipeline should not require LLM."""
    graph = PipelineGraph(
        nodes=[
            PipelineNode(id="src", type=PipelineNodeType.SOURCE, name="src"),
            PipelineNode(id="fwd", type=PipelineNodeType.FORWARD, name="fwd"),
            PipelineNode(id="pub", type=PipelineNodeType.PUBLISH, name="pub"),
        ],
        edges=[
            PipelineEdge(from_node="src", to_node="fwd"),
            PipelineEdge(from_node="fwd", to_node="pub"),
        ],
    )
    pipeline = _base_pipeline(pipeline_json=graph)
    assert pipeline_needs_llm(pipeline) is False


def test_dag_source_publish_only_does_not_need_llm() -> None:
    graph = PipelineGraph(
        nodes=[
            PipelineNode(id="src", type=PipelineNodeType.SOURCE, name="src"),
            PipelineNode(id="pub", type=PipelineNodeType.PUBLISH, name="pub"),
        ],
        edges=[PipelineEdge(from_node="src", to_node="pub")],
    )
    pipeline = _base_pipeline(pipeline_json=graph)
    assert pipeline_needs_llm(pipeline) is False


def test_dag_with_notify_only_does_not_need_llm() -> None:
    graph = PipelineGraph(
        nodes=[
            PipelineNode(id="src", type=PipelineNodeType.SOURCE, name="src"),
            PipelineNode(id="notify", type=PipelineNodeType.NOTIFY, name="notify"),
        ],
    )
    pipeline = _base_pipeline(pipeline_json=graph)
    assert pipeline_needs_llm(pipeline) is False


def test_agent_backend_overrides_non_llm_dag() -> None:
    """AGENT backend needs LLM even if the DAG itself contains no LLM nodes."""
    graph = PipelineGraph(
        nodes=[PipelineNode(id="src", type=PipelineNodeType.SOURCE, name="src")],
    )
    pipeline = _base_pipeline(
        generation_backend=PipelineGenerationBackend.AGENT,
        pipeline_json=graph,
    )
    assert pipeline_needs_llm(pipeline) is True


def test_empty_dag_does_not_need_llm() -> None:
    """Empty DAG shouldn't crash — treat as no LLM nodes."""
    pipeline = _base_pipeline(pipeline_json=PipelineGraph(nodes=[], edges=[]))
    assert pipeline_needs_llm(pipeline) is False
