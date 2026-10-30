"""Pipeline CLI graph operations."""

from __future__ import annotations

from tg_agent.cli.pipeline import requests, responses
from tg_agent.cli.pipeline.responses import _format_filter_config
from tg_agent.models import PipelineNode, PipelineNodeType
from tg_agent.services.pipeline_service import (
    PipelineService,
    PipelineValidationError,
)


def _str_to_bool(value: str | None) -> bool | None:
    if value is None:
        return None
    return value == "true"


def _find_filter_node(graph) -> PipelineNode | None:
    return next((node for node in graph.nodes if node.type == PipelineNodeType.FILTER), None)


def _build_message_filter_config(args: requests.PipelineFilterSetCommand) -> dict:
    return {
        "type": "message_filter",
        "message_kinds": list(getattr(args, "message_kinds", None) or []),
        "service_actions": list(getattr(args, "service_actions", None) or []),
        "media_types": list(getattr(args, "media_types", None) or []),
        "sender_kinds": list(getattr(args, "sender_kinds", None) or []),
        "keywords": list(getattr(args, "keywords", None) or []),
        "regex": getattr(args, "regex", None) or "",
        "forwarded": _str_to_bool(getattr(args, "forwarded", None)),
        "has_text": _str_to_bool(getattr(args, "has_text", None)),
    }


async def _safe_add_edge(svc: PipelineService, pipeline_id: int, from_node: str, to_node: str) -> bool:
    """Add a rewiring edge, skipping it if it would create a cycle (#1077).

    The filter-node splice/unsplice helpers reconnect edges around a node; that
    rewiring must never silently build a cyclic graph, but a cycle-creating edge
    here also must not crash the whole ``pipeline filter`` command with an
    uncaught ``PipelineValidationError``. Skip the offending edge instead — the
    rest of the rewire stays intact and the graph remains acyclic."""
    try:
        return await svc.add_edge(pipeline_id, from_node, to_node)
    except PipelineValidationError:
        return False


async def _upsert_filter_node(svc: PipelineService, pipeline_id: int, config: dict) -> bool:
    graph = await svc.get_graph(pipeline_id)
    if graph is None:
        return False

    existing = _find_filter_node(graph)
    if existing is not None:
        replacement = existing.model_copy(update={"config": config})
        return await svc.replace_node(pipeline_id, existing.id, replacement)

    # Sources only declare channel ids; fetch/retrieve/search triggers write messages.
    # Never guess from node names or list order, especially for imported DAGs.
    loaders = [
        node
        for node in graph.nodes
        if node.type
        in (
            PipelineNodeType.FETCH_MESSAGES,
            PipelineNodeType.RETRIEVE_CONTEXT,
            PipelineNodeType.SEARCH_QUERY_TRIGGER,
        )
    ]
    if len(loaders) != 1:
        raise PipelineValidationError(
            "Automatic filter placement requires exactly one message-writing node "
            "(fetch_messages, retrieve_context, search_query_trigger); "
            "add and connect a filter explicitly for this graph."
        )
    upstream_id = loaders[0].id
    downstream_ids = [edge.to_node for edge in graph.edges if edge.from_node == upstream_id]
    used_ids = {node.id for node in graph.nodes}
    index = 1
    while f"filter_{index}" in used_ids:
        index += 1
    filter_id = f"filter_{index}"
    new_node = PipelineNode(
        id=filter_id,
        type=PipelineNodeType.FILTER,
        name="Фильтр",
        config=config,
        position={"x": 330.0, "y": 0.0},
    )
    if not await svc.add_node(pipeline_id, new_node):
        return False

    for downstream_id in downstream_ids:
        await svc.remove_edge(pipeline_id, upstream_id, downstream_id)
    await _safe_add_edge(svc, pipeline_id, upstream_id, filter_id)
    for downstream_id in downstream_ids:
        await _safe_add_edge(svc, pipeline_id, filter_id, downstream_id)
    return True


async def _clear_filter_node(svc: PipelineService, pipeline_id: int) -> bool:
    graph = await svc.get_graph(pipeline_id)
    if graph is None:
        return False
    existing = _find_filter_node(graph)
    if existing is None:
        return True
    incoming = [edge.from_node for edge in graph.edges if edge.to_node == existing.id]
    outgoing = [edge.to_node for edge in graph.edges if edge.from_node == existing.id]
    ok = await svc.remove_node(pipeline_id, existing.id)
    if not ok:
        return False
    for source in incoming:
        for target in outgoing:
            await _safe_add_edge(svc, pipeline_id, source, target)
    return True


async def _pipeline_filter(
    args: requests.PipelineFilterSetCommand | requests.PipelineFilterShowCommand | requests.PipelineFilterClearCommand,
    db,
    config,
    svc: PipelineService,
) -> None:
    pipeline = await svc.get(args.id)
    if pipeline is None:
        print(f"Pipeline id={args.id} not found")
        return
    graph = await svc.get_graph(args.id)
    if graph is None:
        print(f"Pipeline id={args.id} has no graph (legacy pipeline)")
        return

    if args.filter_action == "set":
        config = _build_message_filter_config(args)
        try:
            ok = await _upsert_filter_node(svc, args.id, config)
        except PipelineValidationError as exc:
            print(f"Error: {exc}")
            return
        if not ok:
            print(f"Failed to update filter for pipeline id={args.id}")
            return
        print(f"Updated filter for pipeline id={args.id}")
        for line in _format_filter_config(config):
            print(line)

    elif args.filter_action == "show":
        filter_node = _find_filter_node(graph)
        if filter_node is None:
            print(f"Pipeline id={args.id} has no filter")
            return
        for line in _format_filter_config(filter_node.config):
            print(line)

    elif args.filter_action == "clear":
        ok = await _clear_filter_node(svc, args.id)
        if not ok:
            print(f"Failed to clear filter for pipeline id={args.id}")
            return
        print(f"Cleared filter for pipeline id={args.id}")


async def _pipeline_node(
    args: requests.PipelineNodeAddCommand | requests.PipelineNodeReplaceCommand | requests.PipelineNodeRemoveCommand,
    db,
    config,
    svc: PipelineService,
) -> None:
    from tg_agent.cli.node_dsl import NodeSpecError, parse_node_spec

    if args.node_action == "add":
        try:
            spec = parse_node_spec(args.node_spec)
        except NodeSpecError as exc:
            print(f"Invalid node spec: {exc}")
            return
        from tg_agent.cli.node_dsl import generate_node_id

        if spec.id:
            node_id = spec.id
        else:
            graph = await svc.get_graph(args.pipeline_id)
            existing_type_prefix = spec.type.value
            max_idx = -1
            if graph:
                for n in graph.nodes:
                    if n.id.startswith(existing_type_prefix + "_"):
                        try:
                            idx = int(n.id.split("_")[-1])
                            max_idx = max(max_idx, idx)
                        except (ValueError, IndexError):
                            pass
            node_id = generate_node_id(spec.type, max_idx + 1)
        node = PipelineNode(id=node_id, type=spec.type, name=spec.type.value, config=spec.config)
        ok = await svc.add_node(args.pipeline_id, node)
        if ok:
            print(f"Added node '{node_id}' to pipeline id={args.pipeline_id}")
        else:
            print(f"Pipeline id={args.pipeline_id} not found or has no graph")

    elif args.node_action == "replace":
        try:
            spec = parse_node_spec(args.node_spec)
        except NodeSpecError as exc:
            print(f"Invalid node spec: {exc}")
            return
        new_id = spec.id or args.node_id
        node = PipelineNode(id=new_id, type=spec.type, name=spec.type.value, config=spec.config)
        ok = await svc.replace_node(args.pipeline_id, args.node_id, node)
        if ok:
            print(f"Replaced node '{args.node_id}' in pipeline id={args.pipeline_id}")
        else:
            print(f"Node '{args.node_id}' not found")

    elif args.node_action == "remove":
        ok = await svc.remove_node(args.pipeline_id, args.node_id)
        if ok:
            print(f"Removed node '{args.node_id}' from pipeline id={args.pipeline_id}")
        else:
            print(f"Node '{args.node_id}' not found")


async def _pipeline_edge(
    args: requests.PipelineEdgeAddCommand | requests.PipelineEdgeRemoveCommand, db, config, svc: PipelineService
) -> None:
    if args.edge_action == "add":
        try:
            ok = await svc.add_edge(args.pipeline_id, args.from_node, args.to_node)
        except PipelineValidationError as exc:
            # Fail-closed (#1077): a cycle-creating edge is rejected.
            print(f"Error: {exc}")
            return
        if ok:
            print(f"Added edge {args.from_node} -> {args.to_node}")
        else:
            print(f"Pipeline id={args.pipeline_id} not found or has no graph")

    elif args.edge_action == "remove":
        ok = await svc.remove_edge(args.pipeline_id, args.from_node, args.to_node)
        if ok:
            print(f"Removed edge {args.from_node} -> {args.to_node}")
        else:
            print(f"Edge {args.from_node} -> {args.to_node} not found")


async def _pipeline_graph(args: requests.PipelineGraphCommand, db, config, svc: PipelineService) -> None:
    graph = await svc.get_graph(args.id)
    if graph is None:
        pipeline = await svc.get(args.id)
        if pipeline is None:
            print(f"Pipeline id={args.id} not found")
        else:
            print(f"Pipeline id={args.id} has no graph (legacy pipeline)")
        return
    return responses.render_pipeline_graph(graph)


HANDLERS = {
    "filter": _pipeline_filter,
    "node": _pipeline_node,
    "edge": _pipeline_edge,
    "graph": _pipeline_graph,
}
