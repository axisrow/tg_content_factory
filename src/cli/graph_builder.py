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

        nodes: list[PipelineNode] = []
        edges: list[PipelineEdge] = []
        type_counters: dict[str, int] = {}

        def _next_id(spec: NodeSpec) -> str:
            if spec.id is not None:
                return spec.id
            key = spec.type.value
            idx = type_counters.get(key, 0)
            type_counters[key] = idx + 1
            return generate_node_id(spec.type, idx)

        # -- 1. Resolve source --------------------------------------------------
        has_user_source = any(s.type == PipelineNodeType.SOURCE for s in self._node_specs)
        source_node_id: str | None = None

        if self._source_channel_ids and not has_user_source:
            # Auto-create source node (channel_ids injection into user source happens at step 2)
            source_id = "source_0"
            type_counters["source"] = 1
            source_node_id = source_id
            nodes.append(PipelineNode(
                id=source_id,
                type=PipelineNodeType.SOURCE,
                name="source",
                config={"channel_ids": self._source_channel_ids},
                position={"x": 0.0, "y": 0.0},
            ))

        # -- 2. Process user node specs ------------------------------------------
        user_node_ids: list[str] = []
        for spec in self._node_specs:
            nid = _next_id(spec)
            config = dict(spec.config)

            # Inject channel_ids into user source node
            if spec.type == PipelineNodeType.SOURCE and self._source_channel_ids:
                config["channel_ids"] = self._source_channel_ids

            node = PipelineNode(
                id=nid,
                type=spec.type,
                name=spec.type.value,
                config=config,
            )
            nodes.append(node)
            user_node_ids.append(nid)

            if spec.type == PipelineNodeType.SOURCE and source_node_id is None:
                source_node_id = nid

        # -- 3. Auto-insert fetch_messages after source --------------------------
        need_fetch = (
            source_node_id is not None
            and not any(
                s.type in _AUTO_INSERT_TYPES for s in self._node_specs
            )
            and (len(user_node_ids) > 0 or self._target_refs)
        )
        fetch_node_id: str | None = None
        if need_fetch:
            fetch_id = "fetch_messages_0"
            type_counters["fetch_messages"] = 1
            fetch_node_id = fetch_id
            # Insert after source
            source_idx = next(
                (i for i, n in enumerate(nodes) if n.id == source_node_id), -1
            )
            fetch_node = PipelineNode(
                id=fetch_id,
                type=PipelineNodeType.FETCH_MESSAGES,
                name="fetch_messages",
                config={},
                position={"x": float(source_idx + 1) * 200, "y": 0.0},
            )
            nodes.insert(source_idx + 1, fetch_node)

        # -- 4. Auto-publish from targets ----------------------------------------
        has_publish_or_forward = any(
            n.type in (PipelineNodeType.PUBLISH, PipelineNodeType.FORWARD)
            for n in nodes
        )
        if self._target_refs and not has_publish_or_forward:
            pub_id = "publish_0"
            type_counters["publish"] = 1
            pub_node = PipelineNode(
                id=pub_id,
                type=PipelineNodeType.PUBLISH,
                name="publish",
                config={"targets": self._target_refs},
            )
            nodes.append(pub_node)
            user_node_ids.append(pub_id)

        # -- 5. Inject targets into first publish/forward ------------------------
        if self._target_refs:
            for node in nodes:
                if node.type in (PipelineNodeType.PUBLISH, PipelineNodeType.FORWARD):
                    if "targets" not in node.config:
                        node.config["targets"] = self._target_refs
                    break

        # -- 6. Build linear edges -----------------------------------------------
        # Chain: source -> [fetch] -> user_0 -> user_1 -> ...
        chain: list[str] = []
        if source_node_id:
            chain.append(source_node_id)
        if fetch_node_id:
            chain.append(fetch_node_id)
        chain.extend(user_node_ids)

        for i in range(len(chain) - 1):
            edges.append(PipelineEdge(from_node=chain[i], to_node=chain[i + 1]))

        # -- 7. Explicit edges ---------------------------------------------------
        existing = {(e.from_node, e.to_node) for e in edges}
        for from_id, to_id in self._explicit_edges:
            if (from_id, to_id) not in existing:
                edges.append(PipelineEdge(from_node=from_id, to_node=to_id))

        # -- 8. Apply node config overrides --------------------------------------
        node_by_id = {n.id: n for n in nodes}
        for nid, override in self._node_config_overrides.items():
            node = node_by_id.get(nid)
            if node is not None:
                node.config.update(override)

        # -- 9. Assign positions --------------------------------------------------
        _assign_positions(nodes)

        # -- 10. Validate: no duplicate IDs --------------------------------------
        seen_ids: set[str] = set()
        for n in nodes:
            if n.id in seen_ids:
                raise GraphBuilderError(f"Duplicate node ID: {n.id}")
            seen_ids.add(n.id)

        return PipelineGraph(nodes=nodes, edges=edges)


def _assign_positions(nodes: list[PipelineNode]) -> None:
    """Assign auto-layout x positions if they are still 0,0."""
    for i, node in enumerate(nodes):
        if node.position.get("x") == 0.0 and node.position.get("y") == 0.0:
            node.position = {"x": float(i) * 200.0, "y": 0.0}
