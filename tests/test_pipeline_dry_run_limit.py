"""Tests for dry-run fetch-limit handling (audit #837/10)."""

from __future__ import annotations

from src.models import (
    ContentPipeline,
    PipelineGraph,
    PipelineNode,
    PipelineNodeType,
    PipelinePublishMode,
)
from src.web.pipelines.handlers import _apply_fetch_limit


def _pipeline(graph: PipelineGraph | None = None) -> ContentPipeline:
    return ContentPipeline(
        name="p",
        prompt_template="x",
        llm_model="m",
        publish_mode=PipelinePublishMode.MODERATED,
        pipeline_json=graph,
    )


def test_apply_fetch_limit_caps_candidates():
    graph = PipelineGraph(
        nodes=[
            PipelineNode(
                id="f", type=PipelineNodeType.FETCH_MESSAGES, name="fetch", config={"limit": 2}
            )
        ],
        edges=[],
    )
    assert _apply_fetch_limit(_pipeline(graph), [1, 2, 3, 4, 5]) == [1, 2]


def test_apply_fetch_limit_without_limit_returns_all():
    graph = PipelineGraph(
        nodes=[PipelineNode(id="f", type=PipelineNodeType.FETCH_MESSAGES, name="fetch", config={})],
        edges=[],
    )
    assert _apply_fetch_limit(_pipeline(graph), [1, 2, 3]) == [1, 2, 3]


def test_apply_fetch_limit_no_graph_returns_all():
    assert _apply_fetch_limit(_pipeline(None), [1, 2, 3]) == [1, 2, 3]
