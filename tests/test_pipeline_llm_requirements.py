"""Unit tests for pipeline_needs_llm, pipeline_is_dag, pipeline_needs_publish_mode, get_react_emoji_config, get_dag_source_channel_ids helpers."""
from __future__ import annotations

from src.models import (
    ContentPipeline,
    PipelineEdge,
    PipelineGenerationBackend,
    PipelineGraph,
    PipelineNode,
    PipelineNodeType,
)
from src.services.pipeline_llm_requirements import (
    get_dag_source_channel_ids,
    get_react_emoji_config,
    pipeline_is_dag,
    pipeline_needs_llm,
    pipeline_needs_publish_mode,
)


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


# ── pipeline_is_dag ─────────────────────────────────────────────────


def test_pipeline_is_dag_with_graph() -> None:
    graph = PipelineGraph(
        nodes=[PipelineNode(id="src", type=PipelineNodeType.SOURCE, name="src")],
    )
    pipeline = _base_pipeline(pipeline_json=graph)
    assert pipeline_is_dag(pipeline) is True


def test_pipeline_is_dag_legacy() -> None:
    pipeline = _base_pipeline(pipeline_json=None)
    assert pipeline_is_dag(pipeline) is False


# ── pipeline_needs_publish_mode ─────────────────────────────────────


def test_needs_publish_mode_legacy_chain() -> None:
    pipeline = _base_pipeline(pipeline_json=None)
    assert pipeline_needs_publish_mode(pipeline) is True


def test_needs_publish_mode_dag_with_publish() -> None:
    graph = PipelineGraph(
        nodes=[
            PipelineNode(id="src", type=PipelineNodeType.SOURCE, name="src"),
            PipelineNode(id="pub", type=PipelineNodeType.PUBLISH, name="pub"),
        ],
    )
    pipeline = _base_pipeline(pipeline_json=graph)
    assert pipeline_needs_publish_mode(pipeline) is True


def test_needs_publish_mode_dag_with_llm_generate() -> None:
    graph = PipelineGraph(
        nodes=[
            PipelineNode(id="src", type=PipelineNodeType.SOURCE, name="src"),
            PipelineNode(id="llm", type=PipelineNodeType.LLM_GENERATE, name="llm"),
        ],
    )
    pipeline = _base_pipeline(pipeline_json=graph)
    assert pipeline_needs_publish_mode(pipeline) is True


def test_needs_publish_mode_dag_without_publish() -> None:
    graph = PipelineGraph(
        nodes=[
            PipelineNode(id="src", type=PipelineNodeType.SOURCE, name="src"),
            PipelineNode(id="fwd", type=PipelineNodeType.FORWARD, name="fwd"),
        ],
    )
    pipeline = _base_pipeline(pipeline_json=graph)
    assert pipeline_needs_publish_mode(pipeline) is False


# ── get_react_emoji_config ──────────────────────────────────────────


def test_react_emoji_config_legacy_returns_none() -> None:
    pipeline = _base_pipeline(pipeline_json=None)
    assert get_react_emoji_config(pipeline) is None


def test_react_emoji_config_no_react_node() -> None:
    graph = PipelineGraph(
        nodes=[PipelineNode(id="src", type=PipelineNodeType.SOURCE, name="src")],
    )
    pipeline = _base_pipeline(pipeline_json=graph)
    assert get_react_emoji_config(pipeline) is None


def test_react_emoji_config_single_emoji() -> None:
    graph = PipelineGraph(
        nodes=[
            PipelineNode(
                id="react",
                type=PipelineNodeType.REACT,
                name="react",
                config={"emoji": "👍"},
            ),
        ],
    )
    pipeline = _base_pipeline(pipeline_json=graph)
    assert get_react_emoji_config(pipeline) == "👍"


def test_react_emoji_config_random_emojis() -> None:
    graph = PipelineGraph(
        nodes=[
            PipelineNode(
                id="react",
                type=PipelineNodeType.REACT,
                name="react",
                config={"random_emojis": ["👍", "❤️", "🔥"], "emoji": "👍"},
            ),
        ],
    )
    pipeline = _base_pipeline(pipeline_json=graph)
    assert get_react_emoji_config(pipeline) == "👍,❤️,🔥"


def test_react_emoji_config_default_emoji() -> None:
    """When no emoji config, defaults to thumbs up."""
    graph = PipelineGraph(
        nodes=[
            PipelineNode(
                id="react",
                type=PipelineNodeType.REACT,
                name="react",
                config={},
            ),
        ],
    )
    pipeline = _base_pipeline(pipeline_json=graph)
    assert get_react_emoji_config(pipeline) == "👍"


# ── get_dag_source_channel_ids ──────────────────────────────────────


def test_dag_source_channel_ids_legacy_returns_none() -> None:
    pipeline = _base_pipeline(pipeline_json=None)
    assert get_dag_source_channel_ids(pipeline) is None


def test_dag_source_channel_ids_with_source_node() -> None:
    graph = PipelineGraph(
        nodes=[
            PipelineNode(
                id="src",
                type=PipelineNodeType.SOURCE,
                name="src",
                config={"channel_ids": ["100", "200"]},
            ),
        ],
    )
    pipeline = _base_pipeline(pipeline_json=graph)
    result = get_dag_source_channel_ids(pipeline)
    assert result == [100, 200]


def test_dag_source_channel_ids_no_source_node() -> None:
    graph = PipelineGraph(
        nodes=[
            PipelineNode(id="fwd", type=PipelineNodeType.FORWARD, name="fwd"),
        ],
    )
    pipeline = _base_pipeline(pipeline_json=graph)
    result = get_dag_source_channel_ids(pipeline)
    assert result == []


def test_dag_source_channel_ids_empty_config() -> None:
    graph = PipelineGraph(
        nodes=[
            PipelineNode(
                id="src",
                type=PipelineNodeType.SOURCE,
                name="src",
                config={},
            ),
        ],
    )
    pipeline = _base_pipeline(pipeline_json=graph)
    result = get_dag_source_channel_ids(pipeline)
    assert result == []


# ── agent_loop node type needs LLM ─────────────────────────────────


def test_dag_with_agent_loop_needs_llm() -> None:
    graph = PipelineGraph(
        nodes=[
            PipelineNode(id="src", type=PipelineNodeType.SOURCE, name="src"),
            PipelineNode(id="agent", type=PipelineNodeType.AGENT_LOOP, name="agent"),
        ],
    )
    pipeline = _base_pipeline(pipeline_json=graph)
    assert pipeline_needs_llm(pipeline) is True
