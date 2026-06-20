"""Pipeline graph builder with auto-wiring.

Assembles a :class:`~src.models.PipelineGraph` from a list of
:class:`~src.cli.node_dsl.NodeSpec` objects plus optional source/target/edge
hints, applying auto-wiring rules:

1. ``set_sources()`` auto-creates a *source* node (or injects into existing).
2. A *source* followed by user nodes triggers auto-insertion of *fetch_messages*.
3. ``--node`` order defines linear edges.
4. ``--edge FROM->TO`` adds extra edges on top.
5. ``set_targets()`` injects targets into the first *publish*/*forward* node
   or auto-creates a *publish* node at the end.
6. ``--node-config NODE_ID JSON`` merges into the node config.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from src.cli.node_dsl import NodeSpec, generate_node_id
from src.models import (
    PipelineEdge,
    PipelineGraph,
    PipelineNode,
    PipelineNodeType,
)


class GraphBuilderError(ValueError):
    """Raised when graph construction fails validation."""


_AUTO_INSERT_TYPES = {PipelineNodeType.FETCH_MESSAGES}


@dataclass
class _BuildState:
    """Mutable accumulator threaded through the ``GraphBuilder.build`` phases.

    Holds the nodes/edges under construction plus the auto-wiring bookkeeping
    (id counters, the resolved source/fetch ids, the ordered user-node ids).
    Split out of ``build`` so each phase is a small method instead of one
    rank-F function (#922).
    """

    nodes: list[PipelineNode] = field(default_factory=list)
    edges: list[PipelineEdge] = field(default_factory=list)
    type_counters: dict[str, int] = field(default_factory=dict)
    source_node_id: str | None = None
    fetch_node_id: str | None = None
    user_node_ids: list[str] = field(default_factory=list)

    def next_id(self, spec: NodeSpec) -> str:
        if spec.id is not None:
            return spec.id
        key = spec.type.value
        idx = self.type_counters.get(key, 0)
        self.type_counters[key] = idx + 1
        return generate_node_id(spec.type, idx)


class GraphBuilder:
    """Incremental builder that produces a :class:`PipelineGraph`."""

    def __init__(self) -> None:
        self._node_specs: list[NodeSpec] = []
        self._source_channel_ids: list[int] = []
        self._target_refs: list[dict[str, Any]] = []
        self._explicit_edges: list[tuple[str, str]] = []
        self._node_config_overrides: dict[str, dict[str, Any]] = {}

    # -- Public API ----------------------------------------------------------

    def add_node_spec(self, spec: NodeSpec) -> None:
        self._node_specs.append(spec)

    def set_sources(self, channel_ids: list[int]) -> None:
        self._source_channel_ids = sorted(set(int(cid) for cid in channel_ids))

    def set_targets(self, target_refs: list[dict[str, Any]]) -> None:
        self._target_refs = list(target_refs)

    def add_explicit_edge(self, from_id: str, to_id: str) -> None:
        self._explicit_edges.append((from_id, to_id))

    def set_node_config_override(self, node_id: str, config: dict[str, Any]) -> None:
        self._node_config_overrides[node_id] = config

    def build(self) -> PipelineGraph:
        """Build and return a :class:`PipelineGraph`."""
        if not self._node_specs and not self._source_channel_ids:
            raise GraphBuilderError("Cannot build an empty graph. Add --node specs or --source.")

        state = _BuildState()
        self._build_source_and_user_nodes(state)
        self._auto_insert_fetch(state)
        self._auto_publish_and_targets(state)
        self._build_edges(state)
        self._apply_overrides_and_validate(state)
        return PipelineGraph(nodes=state.nodes, edges=state.edges)

    def _build_source_and_user_nodes(self, state: _BuildState) -> None:
        # -- 1. Resolve source --------------------------------------------------
        has_user_source = any(s.type == PipelineNodeType.SOURCE for s in self._node_specs)

        if self._source_channel_ids and not has_user_source:
            # Auto-create source node (channel_ids injection into user source happens at step 2)
            source_id = "source_0"
            state.type_counters["source"] = 1
            state.source_node_id = source_id
            state.nodes.append(PipelineNode(
                id=source_id,
                type=PipelineNodeType.SOURCE,
                name="source",
                config={"channel_ids": self._source_channel_ids},
                position={"x": 0.0, "y": 0.0},
            ))

        # -- 2. Process user node specs ------------------------------------------
        for spec in self._node_specs:
            nid = state.next_id(spec)
            config = dict(spec.config)

            # Inject channel_ids into user source node
            if spec.type == PipelineNodeType.SOURCE:
                cids = config.get("channel_ids")
                if cids is not None and not isinstance(cids, list):
                    try:
                        config["channel_ids"] = [int(cids)]
                    except (ValueError, TypeError):
                        raise GraphBuilderError(f"channel_ids must be an integer, got: {cids!r}") from None
                if self._source_channel_ids:
                    config["channel_ids"] = self._source_channel_ids

            node = PipelineNode(
                id=nid,
                type=spec.type,
                name=spec.type.value,
                config=config,
            )
            state.nodes.append(node)
            state.user_node_ids.append(nid)

            if spec.type == PipelineNodeType.SOURCE and state.source_node_id is None:
                state.source_node_id = nid

    def _auto_insert_fetch(self, state: _BuildState) -> None:
        # -- 3. Auto-insert fetch_messages after source --------------------------
        need_fetch = (
            state.source_node_id is not None
            and not any(
                s.type in _AUTO_INSERT_TYPES for s in self._node_specs
            )
            and (len(state.user_node_ids) > 0 or self._target_refs)
        )
        if not need_fetch:
            return
        fetch_id = "fetch_messages_0"
        state.type_counters["fetch_messages"] = 1
        state.fetch_node_id = fetch_id
        # Insert after source
        source_idx = next(
            (i for i, n in enumerate(state.nodes) if n.id == state.source_node_id), -1
        )
        fetch_node = PipelineNode(
            id=fetch_id,
            type=PipelineNodeType.FETCH_MESSAGES,
            name="fetch_messages",
            config={},
            position={"x": float(source_idx + 1) * 200, "y": 0.0},
        )
        state.nodes.insert(source_idx + 1, fetch_node)

    def _auto_publish_and_targets(self, state: _BuildState) -> None:
        # -- 4. Auto-publish from targets ----------------------------------------
        has_publish_or_forward = any(
            n.type in (PipelineNodeType.PUBLISH, PipelineNodeType.FORWARD)
            for n in state.nodes
        )
        if self._target_refs and not has_publish_or_forward:
            pub_id = "publish_0"
            state.type_counters["publish"] = 1
            pub_node = PipelineNode(
                id=pub_id,
                type=PipelineNodeType.PUBLISH,
                name="publish",
                config={"targets": self._target_refs},
            )
            state.nodes.append(pub_node)
            state.user_node_ids.append(pub_id)

        # -- 5. Inject targets into first publish/forward ------------------------
        if self._target_refs:
            for node in state.nodes:
                if node.type in (PipelineNodeType.PUBLISH, PipelineNodeType.FORWARD):
                    if not node.config.get("targets"):
                        node.config["targets"] = self._target_refs
                    break

    def _build_edges(self, state: _BuildState) -> None:
        # -- 6. Build linear edges -----------------------------------------------
        # Chain: source -> [fetch] -> user_0 -> user_1 -> ...
        chain: list[str] = []
        if state.source_node_id:
            chain.append(state.source_node_id)
        if state.fetch_node_id:
            chain.append(state.fetch_node_id)
        chain.extend(nid for nid in state.user_node_ids if nid not in chain)

        for i in range(len(chain) - 1):
            state.edges.append(PipelineEdge(from_node=chain[i], to_node=chain[i + 1]))

        # -- 7. Explicit edges ---------------------------------------------------
        node_ids = {n.id for n in state.nodes}
        existing = {(e.from_node, e.to_node) for e in state.edges}
        for from_id, to_id in self._explicit_edges:
            if from_id not in node_ids or to_id not in node_ids:
                raise GraphBuilderError(
                    f"Edge references non-existent node: {from_id} -> {to_id}"
                )
            if (from_id, to_id) not in existing:
                state.edges.append(PipelineEdge(from_node=from_id, to_node=to_id))

    def _apply_overrides_and_validate(self, state: _BuildState) -> None:
        # -- 8. Apply node config overrides --------------------------------------
        node_by_id = {n.id: n for n in state.nodes}
        for nid, override in self._node_config_overrides.items():
            node = node_by_id.get(nid)
            if node is not None:
                node.config.update(override)

        # -- 9. Assign positions --------------------------------------------------
        _assign_positions(state.nodes)

        # -- 10. Validate: no duplicate IDs --------------------------------------
        seen_ids: set[str] = set()
        for n in state.nodes:
            if n.id in seen_ids:
                raise GraphBuilderError(f"Duplicate node ID: {n.id}")
            seen_ids.add(n.id)


def _assign_positions(nodes: list[PipelineNode]) -> None:
    """Assign auto-layout x positions if they are still 0,0."""
    for i, node in enumerate(nodes):
        if node.position.get("x") == 0.0 and node.position.get("y") == 0.0:
            node.position = {"x": float(i) * 200.0, "y": 0.0}
