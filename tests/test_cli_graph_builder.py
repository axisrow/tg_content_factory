"""Unit tests for the pipeline graph builder (src/cli/graph_builder.py) and ASCII viz."""
from __future__ import annotations

import pytest

from src.cli.graph_viz import render_ascii
from src.models import (
    PipelineEdge,
    PipelineGraph,
    PipelineNode,
    PipelineNodeType,
)

# ---------------------------------------------------------------------------
# GraphBuilder tests
# ---------------------------------------------------------------------------

def _builder():
    from src.cli.graph_builder import GraphBuilder
    return GraphBuilder()


def _spec(raw: str):
    from src.cli.node_dsl import parse_node_spec
    return parse_node_spec(raw)


class TestSimpleLinearChain:
    def test_two_nodes(self):
        b = _builder()
        b.add_node_spec(_spec("delay"))
        b.add_node_spec(_spec("react:emoji=heart"))
        g = b.build()
        assert len(g.nodes) == 2
        assert len(g.edges) == 1
        assert g.edges[0].from_node == g.nodes[0].id
        assert g.edges[0].to_node == g.nodes[1].id


class TestAutoSourceAndFetch:
    def test_auto_source_and_fetch_messages(self):
        b = _builder()
        b.set_sources([1001])
        b.add_node_spec(_spec("llm_generate:model=claude"))
        g = b.build()
        # Expect: source_0, fetch_messages_0, llm_generate_0
        types = [n.type for n in g.nodes]
        assert PipelineNodeType.SOURCE in types
        assert PipelineNodeType.FETCH_MESSAGES in types
        assert PipelineNodeType.LLM_GENERATE in types
        assert len(g.nodes) == 3

    def test_source_injects_channel_ids(self):
        b = _builder()
        b.set_sources([1001, 1002])
        b.add_node_spec(_spec("react:emoji=heart"))
        g = b.build()
        source = next(n for n in g.nodes if n.type == PipelineNodeType.SOURCE)
        assert source.config["channel_ids"] == [1001, 1002]

    def test_user_specified_source_gets_channel_ids_injected(self):
        b = _builder()
        b.add_node_spec(_spec("source"))
        b.set_sources([1001])
        b.add_node_spec(_spec("react:emoji=heart"))
        g = b.build()
        source = next(n for n in g.nodes if n.type == PipelineNodeType.SOURCE)
        assert source.config["channel_ids"] == [1001]


class TestAutoPublish:
    def test_auto_publish_from_targets(self):
        b = _builder()
        b.set_sources([1001])
        b.set_targets([{"phone": "+100", "dialog_id": 77}])
        b.add_node_spec(_spec("react:emoji=heart"))
        g = b.build()
        types = [n.type for n in g.nodes]
        assert PipelineNodeType.PUBLISH in types

    def test_target_injection_into_existing_publish(self):
        b = _builder()
        b.set_sources([1001])
        b.add_node_spec(_spec("publish"))
        b.set_targets([{"phone": "+100", "dialog_id": 77}])
        g = b.build()
        pub = next(n for n in g.nodes if n.type == PipelineNodeType.PUBLISH)
        assert pub.config["targets"] == [{"phone": "+100", "dialog_id": 77}]

    def test_target_injection_into_forward(self):
        b = _builder()
        b.set_sources([1001])
        b.add_node_spec(_spec("forward"))
        b.set_targets([{"phone": "+100", "dialog_id": 77}])
        g = b.build()
        fwd = next(n for n in g.nodes if n.type == PipelineNodeType.FORWARD)
        assert fwd.config["targets"] == [{"phone": "+100", "dialog_id": 77}]


class TestExplicitEdges:
    def test_explicit_edge_on_top_of_linear(self):
        b = _builder()
        b.add_node_spec(_spec("delay:id=a"))
        b.add_node_spec(_spec("delay:id=b"))
        b.add_node_spec(_spec("delay:id=c"))
        b.add_explicit_edge("a", "c")
        g = b.build()
        # Linear: a->b, b->c. Plus explicit a->c
        edge_set = {(e.from_node, e.to_node) for e in g.edges}
        assert ("a", "b") in edge_set
        assert ("b", "c") in edge_set
        assert ("a", "c") in edge_set


class TestNodeConfigOverride:
    def test_override_merges_into_config(self):
        b = _builder()
        b.add_node_spec(_spec("react:id=r1,emoji=heart"))
        b.set_node_config_override("r1", {"emoji": "fire", "random_emojis": ["fire", "100"]})
        g = b.build()
        node = next(n for n in g.nodes if n.id == "r1")
        assert node.config["emoji"] == "fire"
        assert node.config["random_emojis"] == ["fire", "100"]


class TestIdAssignment:
    def test_explicit_id_preserved(self):
        b = _builder()
        b.add_node_spec(_spec("delay:id=my_delay"))
        g = b.build()
        assert g.nodes[0].id == "my_delay"

    def test_auto_id_generated(self):
        b = _builder()
        b.add_node_spec(_spec("delay"))
        g = b.build()
        assert g.nodes[0].id == "delay_0"

    def test_duplicate_id_raises(self):
        from src.cli.graph_builder import GraphBuilderError
        b = _builder()
        b.add_node_spec(_spec("delay:id=dup"))
        b.add_node_spec(_spec("react:id=dup"))
        with pytest.raises(GraphBuilderError, match="[Dd]uplicate"):
            b.build()


class TestAutoPosition:
    def test_positions_spread_horizontally(self):
        b = _builder()
        b.add_node_spec(_spec("delay"))
        b.add_node_spec(_spec("react:emoji=heart"))
        g = b.build()
        xs = [n.position["x"] for n in g.nodes]
        assert xs[0] < xs[1]


class TestMinimalGraph:
    def test_source_only_creates_full_chain(self):
        """set_sources only → source → fetch → publish."""
        b = _builder()
        b.set_sources([1001])
        b.set_targets([{"phone": "+100", "dialog_id": 77}])
        g = b.build()
        types = [n.type for n in g.nodes]
        assert types == [PipelineNodeType.SOURCE, PipelineNodeType.FETCH_MESSAGES, PipelineNodeType.PUBLISH]


class TestEmptyBuilder:
    def test_empty_raises(self):
        from src.cli.graph_builder import GraphBuilderError
        b = _builder()
        with pytest.raises(GraphBuilderError):
            b.build()


# ---------------------------------------------------------------------------
# ASCII viz tests
# ---------------------------------------------------------------------------

class TestRenderAscii:
    def test_linear_graph(self):
        g = PipelineGraph(
            nodes=[
                PipelineNode(id="s", type=PipelineNodeType.SOURCE, name="src"),
                PipelineNode(id="f", type=PipelineNodeType.FETCH_MESSAGES, name="fetch"),
            ],
            edges=[PipelineEdge(from_node="s", to_node="f")],
        )
        out = render_ascii(g)
        assert "source" in out
        assert "fetch_messages" in out
        assert "|" in out

    def test_branching_graph(self):
        g = PipelineGraph(
            nodes=[
                PipelineNode(id="c", type=PipelineNodeType.CONDITION, name="cond"),
                PipelineNode(id="p", type=PipelineNodeType.PUBLISH, name="pub"),
                PipelineNode(id="n", type=PipelineNodeType.NOTIFY, name="notif"),
            ],
            edges=[
                PipelineEdge(from_node="c", to_node="p"),
                PipelineEdge(from_node="c", to_node="n"),
            ],
        )
        out = render_ascii(g)
        assert "condition" in out
        assert "publish" in out
        assert "notify" in out

    def test_empty_graph(self):
        g = PipelineGraph()
        out = render_ascii(g)
        assert "empty" in out
