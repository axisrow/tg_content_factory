"""ASCII visualisation for :class:`~src.models.PipelineGraph`.

Renders a pipeline DAG as a vertical text diagram suitable for terminal output.
"""
from __future__ import annotations

from collections import defaultdict
from typing import Any

from src.models import PipelineGraph


def render_ascii(graph: PipelineGraph) -> str:
    """Render *graph* as an ASCII diagram.

    Linear chains are printed top-to-bottom with ``|`` / ``v`` connectors.
    Nodes with multiple outgoing edges produce a fan-out.
    """
    if not graph.nodes:
        return "(empty graph)"

    # Build adjacency info
    node_by_id = {n.id: n for n in graph.nodes}
    outgoing: dict[str, list[str]] = defaultdict(list)
    for edge in graph.edges:
        outgoing[edge.from_node].append(edge.to_node)

    # Topological order (Kahn's algorithm)
    in_degree: dict[str, int] = {n.id: 0 for n in graph.nodes}
    for edge in graph.edges:
        in_degree[edge.to_node] = in_degree.get(edge.to_node, 0) + 1
    queue = [nid for nid, d in in_degree.items() if d == 0]
    order: list[str] = []
    while queue:
        queue.sort()  # deterministic
        nid = queue.pop(0)
        order.append(nid)
        for child in outgoing.get(nid, []):
            in_degree[child] -= 1
            if in_degree[child] == 0:
                queue.append(child)

    # Fallback: include any nodes not in topological order (cycles / orphans)
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
                # Simple linear connector
                next_nid = children[0] if children else None
                if next_nid and next_nid in node_by_id:
                    lines.append("   |")
                    lines.append("   v")
            else:
                # Fan-out
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


def _config_summary(config: dict[str, Any]) -> str:
    """Short summary of node config for display."""
    if not config:
        return ""
    parts: list[str] = []
    for key, value in list(config.items())[:3]:
        sv = str(value)
        if len(sv) > 30:
            sv = sv[:27] + "..."
        parts.append(f"{key}={sv}")
    summary = ", ".join(parts)
    if len(config) > 3:
        summary += ", ..."
    return f" ({summary})" if summary else ""
