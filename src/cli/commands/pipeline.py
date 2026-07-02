from __future__ import annotations

import argparse
import asyncio
import os
from typing import TYPE_CHECKING, Callable, cast

import typer

from src.cli import runtime
from src.cli.commands.common import (
    GenerationBackend,
    PublishMode,
    SinceUnit,
    TriBool,
    apply_startup,
    run_async,
)

if TYPE_CHECKING:
    from src.telegram.client_pool import ClientPool
from src.models import PipelineNode, PipelineNodeType
from src.search.engine import SearchEngine
from src.services.content_generation_service import ContentGenerationService
from src.services.pipeline_filters import normalize_filter_config
from src.services.pipeline_llm_requirements import pipeline_needs_llm
from src.services.pipeline_refs import parse_pipeline_target_refs
from src.services.pipeline_result import result_kind_label
from src.services.pipeline_service import (
    PipelineScopeError,
    PipelineService,
    PipelineTargetRef,
    PipelineValidationError,
    to_since_hours,
)
from src.services.publish_service import PublishService


def _parse_target_refs(values: list[str]) -> list[PipelineTargetRef]:
    return parse_pipeline_target_refs(
        values,
        missing_separator_message="Target must be in PHONE|DIALOG_ID format.",
        invalid_dialog_id_message="Target dialog id must be numeric.",
    )


def _preview_text(value: str | None, limit: int = 60) -> str:
    if not value:
        return "—"
    compact = " ".join(value.split())
    if len(compact) <= limit:
        return compact
    return f"{compact[: limit - 3]}..."


def _str_to_bool(value: str | None) -> bool | None:
    if value is None:
        return None
    return value == "true"


def _find_filter_node(graph) -> PipelineNode | None:
    return next((node for node in graph.nodes if node.type == PipelineNodeType.FILTER), None)


def _build_message_filter_config(args: argparse.Namespace) -> dict:
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


def _format_filter_config(config: dict) -> list[str]:
    normalized = normalize_filter_config(config)
    return [
        f"message_kinds={','.join(normalized.get('message_kinds', [])) or '—'}",
        f"service_actions={','.join(normalized.get('service_actions', [])) or '—'}",
        f"media_types={','.join(normalized.get('media_types', [])) or '—'}",
        f"sender_kinds={','.join(normalized.get('sender_kinds', [])) or '—'}",
        f"keywords={','.join(normalized.get('keywords', [])) or '—'}",
        f"regex={normalized.get('regex') or '—'}",
        f"forwarded={normalized.get('forwarded') if normalized.get('forwarded') is not None else '—'}",
        f"has_text={normalized.get('has_text') if normalized.get('has_text') is not None else '—'}",
    ]


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

    new_node = PipelineNode(
        id="filter_1",
        type=PipelineNodeType.FILTER,
        name="Фильтр",
        config=config,
        position={"x": 330.0, "y": 0.0},
    )
    if not await svc.add_node(pipeline_id, new_node):
        return False

    graph = await svc.get_graph(pipeline_id)
    if graph is None:
        return False

    upstream_id = None
    for candidate in ("fetch_1", "source_1"):
        if any(node.id == candidate for node in graph.nodes):
            upstream_id = candidate
            break
    if upstream_id is None and graph.nodes:
        upstream_id = graph.nodes[0].id

    downstream_ids = [edge.to_node for edge in graph.edges if edge.from_node == upstream_id] if upstream_id else []
    if upstream_id:
        for downstream_id in downstream_ids:
            await svc.remove_edge(pipeline_id, upstream_id, downstream_id)
        await _safe_add_edge(svc, pipeline_id, upstream_id, "filter_1")
    for downstream_id in downstream_ids:
        if downstream_id != "filter_1":
            await _safe_add_edge(svc, pipeline_id, "filter_1", downstream_id)
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


async def _pipeline_list(args: argparse.Namespace, db, config, svc: PipelineService) -> None:
    items = await svc.get_with_relations()
    if not items:
        print("No pipelines found.")
        return
    print(
        f"{'ID':<5} {'Name':<24} {'Backend':<8} "
        f"{'Mode':<10} {'Active':<8} {'Src/Tgt':<10}"
    )
    print("-" * 76)
    for item in items:
        pipeline = item["pipeline"]
        counts = f"{len(item['sources'])}/{len(item['targets'])}"
        print(
            f"{pipeline.id:<5} {pipeline.name[:24]:<24} "
            f"{pipeline.generation_backend.value:<8} "
            f"{pipeline.publish_mode.value:<10} "
            f"{str(pipeline.is_active):<8} {counts:<10}"
        )



async def _pipeline_show(args: argparse.Namespace, db, config, svc: PipelineService) -> None:
    detail = await svc.get_detail(args.id)
    if detail is None:
        print(f"Pipeline id={args.id} not found")
        return
    pipeline = detail["pipeline"]
    print(f"id={pipeline.id}")
    print(f"name={pipeline.name}")
    print(f"backend={pipeline.generation_backend.value}")
    print(f"publish_mode={pipeline.publish_mode.value}")
    print(f"interval={pipeline.generate_interval_minutes}m")
    print(f"active={pipeline.is_active}")
    print(f"llm_model={pipeline.llm_model or '—'}")
    print(f"image_model={pipeline.image_model or '—'}")
    if pipeline.pipeline_json is not None:
        filter_node = _find_filter_node(pipeline.pipeline_json)
        if filter_node is not None:
            print("filter:")
            for line in _format_filter_config(filter_node.config):
                print(f" - {line}")
    print("sources:")
    for title in detail["source_titles"]:
        print(f" - {title}")
    print("targets:")
    for target in detail["targets"]:
        print(f" - {target.phone}:{target.dialog_id} ({target.title or '—'})")



async def _pipeline_add_from_json(
    args: argparse.Namespace,
    svc: PipelineService,
    json_file: str,
) -> int | None:
    import json as _json

    with open(json_file) as _f:
        graph_data = _json.load(_f)
    data = {
        "name": args.name,
        "prompt_template": args.prompt_template or ".",
        "llm_model": args.llm_model,
        "source_ids": args.source or [],
        "target_refs": (
            [{"phone": r.phone, "dialog_id": r.dialog_id} for r in _parse_target_refs(args.target)]
            if args.target
            else []
        ),
        "generate_interval_minutes": args.interval,
        "pipeline_json": graph_data,
    }
    return await svc.import_json(data)


def _build_pipeline_add_node_specs(node_specs_raw: list[str]) -> tuple[list, bool]:
    from src.cli.node_dsl import NodeSpecError, parse_node_spec

    specs = []
    for raw in node_specs_raw:
        try:
            specs.append(parse_node_spec(raw))
        except NodeSpecError as exc:
            print(f"Invalid node spec '{raw}': {exc}")
            return specs, False
    return specs, True


def _apply_pipeline_add_edges(args: argparse.Namespace, builder) -> bool:
    if getattr(args, "edge", None):
        for edge_str in args.edge:
            from_id, _, to_id = edge_str.partition("->")
            if not to_id:
                print(f"Invalid edge format '{edge_str}'; use FROM->TO")
                return False
            builder.add_explicit_edge(from_id.strip(), to_id.strip())
    return True


def _apply_pipeline_add_node_configs(args: argparse.Namespace, builder) -> bool:
    if getattr(args, "node_configs", None):
        import json as _json

        for nc_str in args.node_configs:
            node_id, _, json_str = nc_str.partition("=")
            try:
                config = _json.loads(json_str)
            except _json.JSONDecodeError as exc:
                print(f"Invalid JSON in --node-config for {node_id}: {exc}")
                return False
            builder.set_node_config_override(node_id.strip(), config)
    return True


def _build_pipeline_add_graph(args: argparse.Namespace, node_specs_raw: list[str]):
    from src.cli.graph_builder import GraphBuilder, GraphBuilderError

    specs, ok = _build_pipeline_add_node_specs(node_specs_raw)
    if not ok:
        return None

    builder = GraphBuilder()
    for spec in specs:
        builder.add_node_spec(spec)

    if args.source:
        builder.set_sources(args.source)

    if args.target:
        target_refs_list = [{"phone": t.phone, "dialog_id": t.dialog_id} for t in _parse_target_refs(args.target)]
        builder.set_targets(target_refs_list)

    if not _apply_pipeline_add_edges(args, builder):
        return None
    if not _apply_pipeline_add_node_configs(args, builder):
        return None

    try:
        return builder.build()
    except GraphBuilderError as exc:
        print(f"Graph build error: {exc}")
        return None


async def _pipeline_add_from_nodes(
    args: argparse.Namespace,
    svc: PipelineService,
    node_specs_raw: list[str],
) -> tuple[int | None, bool]:
    import json as _json

    graph = _build_pipeline_add_graph(args, node_specs_raw)
    if graph is None:
        return None, False

    pipeline_id = await svc.import_json(
        {
            "name": args.name,
            "prompt_template": args.prompt_template or ".",
            "llm_model": args.llm_model,
            "image_model": args.image_model,
            "generation_backend": args.generation_backend,
            "publish_mode": args.publish_mode,
            "generate_interval_minutes": args.interval,
            "pipeline_json": _json.loads(graph.to_json()),
            "source_ids": args.source or [],
            "target_refs": (
                [f"{t.phone}|{t.dialog_id}" for t in _parse_target_refs(args.target)]
                if args.target
                else []
            ),
        },
    )

    # Activate if --inactive was not passed
    if not args.inactive and pipeline_id:
        await svc._bundle.set_active(pipeline_id, True)
    return pipeline_id, True


async def _pipeline_add_legacy(
    args: argparse.Namespace,
    svc: PipelineService,
) -> tuple[int | None, bool]:
    # Legacy mode: --prompt-template + --source + --target required
    if not args.prompt_template:
        print("Error: --prompt-template is required when --json-file/--node is not used")
        return None, False
    if not args.source:
        print("Error: --source is required")
        return None, False
    if not args.target:
        print("Error: --target is required")
        return None, False
    pipeline_id = await svc.add(
        name=args.name,
        prompt_template=args.prompt_template,
        source_channel_ids=args.source,
        target_refs=_parse_target_refs(args.target),
        llm_model=args.llm_model,
        image_model=args.image_model,
        publish_mode=args.publish_mode,
        generation_backend=args.generation_backend,
        generate_interval_minutes=args.interval,
        is_active=not args.inactive,
        ab_num_variants=getattr(args, "ab_variants", 1) or 1,
        ab_auto_select=getattr(args, "ab_auto_select", False),
    )
    return pipeline_id, True


async def _pipeline_add_pipeline(args: argparse.Namespace, svc: PipelineService) -> tuple[int | None, bool]:
    json_file = getattr(args, "json_file", None)
    node_specs_raw = getattr(args, "node_specs", None)
    if json_file:
        return await _pipeline_add_from_json(args, svc, json_file), True
    if node_specs_raw:
        # DAG mode: build graph from --node specs
        return await _pipeline_add_from_nodes(args, svc, node_specs_raw)
    return await _pipeline_add_legacy(args, svc)


async def _pipeline_enqueue_run_after(args: argparse.Namespace, db, pipeline_id: int | None) -> None:
    from src.services.task_enqueuer import TaskEnqueuer

    since_h = to_since_hours(args.since_value, args.since_unit)
    enqueuer_factory = cast(Callable[[object], TaskEnqueuer], TaskEnqueuer)
    enqueuer = enqueuer_factory(db)
    await enqueuer.enqueue_pipeline_run(cast(int, pipeline_id), since_hours=since_h)
    print(f"Enqueued pipeline run (since={args.since_value}{args.since_unit})")


async def _pipeline_add(args: argparse.Namespace, db, config, svc: PipelineService) -> None:
    try:
        pipeline_id, should_print = await _pipeline_add_pipeline(args, svc)
    except PipelineValidationError as exc:
        print(f"Error: {exc}")
        return

    if not should_print:
        return
    print(f"Added pipeline id={pipeline_id}: {args.name}")
    if getattr(args, "run_after", False):
        await _pipeline_enqueue_run_after(args, db, pipeline_id)



async def _pipeline_edit(args: argparse.Namespace, db, config, svc: PipelineService) -> None:
    existing = await svc.get(args.id)
    if existing is None:
        print(f"Pipeline id={args.id} not found")
        return
    current_sources = [source.channel_id for source in await svc.get_sources(args.id)]
    current_targets = [
        PipelineTargetRef(phone=target.phone, dialog_id=target.dialog_id)
        for target in await svc.get_targets(args.id)
    ]
    try:
        ok = await svc.update(
            args.id,
            name=args.name or existing.name,
            prompt_template=args.prompt_template or existing.prompt_template,
            source_channel_ids=args.source if args.source else current_sources,
            target_refs=(
                _parse_target_refs(args.target) if args.target else current_targets
            ),
            llm_model=(
                args.llm_model if args.llm_model is not None else existing.llm_model
            ),
            image_model=(
                args.image_model
                if args.image_model is not None
                else existing.image_model
            ),
            publish_mode=args.publish_mode or existing.publish_mode,
            generation_backend=args.generation_backend or existing.generation_backend,
            generate_interval_minutes=(
                args.interval
                if args.interval is not None
                else existing.generate_interval_minutes
            ),
            is_active=existing.is_active if args.active is None else args.active,
            ab_num_variants=(
                args.ab_variants
                if getattr(args, "ab_variants", None) is not None
                else existing.ab_num_variants
            ),
            ab_auto_select=(
                args.ab_auto_select
                if getattr(args, "ab_auto_select", None) is not None
                else existing.ab_auto_select
            ),
        )
    except PipelineValidationError as exc:
        print(f"Error: {exc}")
        return
    if not ok:
        print(f"Pipeline id={args.id} not found")
        return
    print(f"Updated pipeline id={args.id}")



async def _pipeline_toggle(args: argparse.Namespace, db, config, svc: PipelineService) -> None:
    ok = await svc.toggle(args.id)
    if not ok:
        print(f"Pipeline id={args.id} not found")
        return
    print(f"Toggled pipeline id={args.id}")



async def _pipeline_delete(args: argparse.Namespace, db, config, svc: PipelineService) -> None:
    await svc.delete(args.id)
    print(f"Deleted pipeline id={args.id}")



async def _pipeline_run(args: argparse.Namespace, db, config, svc: PipelineService) -> ClientPool | None:
    pipeline = await svc.get(args.id)
    if pipeline is None:
        print(f"Pipeline id={args.id} not found")
        return None
    engine = SearchEngine(db)

    from src.services.provider_service import build_provider_service
    from src.services.quality_scoring_service import QualityScoringService

    # build_provider_service snapshots os.environ once and loads DB
    # providers; the registry no longer reads env itself (#1050).
    provider_service = await build_provider_service(db, config)
    if pipeline_needs_llm(pipeline) and not provider_service.has_providers():
        print(
            "LLM provider is not configured. Add one in /settings or set an API key "
            "env var (e.g. OPENAI_API_KEY). Non-LLM pipelines run without a provider."
        )
        return None

    _, pool = await runtime.init_pool(config, db)
    client_pool = pool
    gen_svc = ContentGenerationService(
        db,
        engine,
        config=config,
        client_pool=client_pool,
        quality_service=QualityScoringService(db, provider_service=provider_service),
        provider_service=provider_service,
    )
    try:
        run_obj = await gen_svc.generate(
            pipeline=pipeline,
            model=pipeline.llm_model,
            max_tokens=args.max_tokens,
            temperature=args.temperature,
        )
        print(f"Created generation run id={run_obj.id}")
        print(f"Generation completed for run id={run_obj.id}")
        print(
            f"result_kind={run_obj.result_kind} "
            f"result_count={run_obj.result_count}"
        )
        if args.preview and run_obj.generated_text:
            print("--- DRAFT PREVIEW ---")
            print(run_obj.generated_text)
        if args.publish:
            print(
                "Publish requested — publishing via targets is not implemented in CLI; "
                "Use the web UI or implement account targets."
            )
    except Exception as exc:
        print(f"Generation failed: {exc}")

    return pool


async def _pipeline_generate(args: argparse.Namespace, db, config, svc: PipelineService) -> ClientPool | None:
    pipeline = await svc.get(args.id)
    if pipeline is None:
        print(f"Pipeline id={args.id} not found")
        return None
    # Per-run A/B overrides (issue #1068): --ab-variants / --auto-select
    # override the pipeline's stored A/B config for this single run only.
    ab_overrides: dict[str, object] = {}
    if getattr(args, "ab_variants", None) is not None:
        ab_overrides["ab_num_variants"] = args.ab_variants
    if getattr(args, "auto_select", False):
        ab_overrides["ab_auto_select"] = True
    if ab_overrides:
        pipeline = pipeline.model_copy(update=ab_overrides)
    engine = SearchEngine(db)
    from src.services.provider_service import build_provider_service
    from src.services.quality_scoring_service import QualityScoringService

    # build_provider_service snapshots os.environ once and loads DB
    # providers; the registry no longer reads env itself (#1050).
    provider_svc = await build_provider_service(db, config)
    if pipeline_needs_llm(pipeline) and not provider_svc.has_providers():
        print(
            "LLM provider is not configured. Add one in /settings or set an API key "
            "env var (e.g. OPENAI_API_KEY). Non-LLM pipelines run without a provider."
        )
        return None
    _, pool = await runtime.init_pool(config, db)
    client_pool = pool
    agent_manager = None
    if getattr(pipeline.generation_backend, "value", pipeline.generation_backend) == "deep_agents":
        from src.agent.manager import AgentManager

        agent_manager = AgentManager(db, config, client_pool=client_pool)
    gen_svc = ContentGenerationService(
        db,
        engine,
        config=config,
        client_pool=client_pool,
        agent_manager=agent_manager,
        quality_service=QualityScoringService(db, provider_service=provider_svc),
        provider_service=provider_svc,
    )
    try:
        run = await gen_svc.generate(
            pipeline=pipeline,
            model=args.model,
            max_tokens=args.max_tokens,
            temperature=args.temperature,
        )
        print(f"Created generation run id={run.id}")
        if run.variants:
            sel = run.selected_variant
            print(f"--- A/B VARIANTS ({len(run.variants)}) ---")
            for idx, variant_text in enumerate(run.variants):
                marker = " *selected*" if sel == idx else ""
                print(f"[{idx}]{marker} {variant_text[:120]}")
        if run.generated_text:
            print("--- DRAFT PREVIEW ---")
            print(run.generated_text)
    except Exception as exc:
        print(f"Generation failed: {exc}")

    return pool


async def _pipeline_generate_stream(args: argparse.Namespace, db, config, svc: PipelineService) -> None:
    pipeline = await svc.get(args.id)
    if pipeline is None:
        print(f"Pipeline id={args.id} not found")
        return

    from src.services.generation_service import GenerationService
    from src.services.provider_service import build_provider_service
    from src.utils.json import safe_json_dumps

    # build_provider_service snapshots os.environ once and loads DB
    # providers; the registry no longer reads env itself (#1050).
    provider_service = await build_provider_service(db, config)
    if pipeline_needs_llm(pipeline) and not provider_service.has_providers():
        print(
            "LLM provider is not configured. Add one in /settings or set an API key "
            "env var (e.g. OPENAI_API_KEY)."
        )
        return
    if not pipeline_needs_llm(pipeline):
        print("Pipeline has no LLM nodes; nothing to stream.")
        return

    provider_callable = provider_service.get_provider_callable(pipeline.llm_model)
    engine = SearchEngine(db)
    gen = GenerationService(engine, provider_callable=provider_callable)
    # Fail-closed (#1077): never widen a failed source-scope lookup to
    # an all-channels retrieval. Abort before creating a run.
    try:
        scope = await svc.get_retrieval_scope(pipeline)
    except PipelineScopeError as exc:
        print(safe_json_dumps({"event": "error", "error": str(exc)}), flush=True)
        return

    run_id = await db.repos.generation_runs.create_run(
        pipeline.id, pipeline.prompt_template
    )
    await db.repos.generation_runs.set_status(run_id, "running")
    last = None
    try:
        async for update in gen.generate_stream(
            query=scope.query,
            prompt_template=pipeline.prompt_template,
            model=(args.model or pipeline.llm_model),
            max_tokens=args.max_tokens,
            temperature=args.temperature,
            limit=args.limit,
            channel_id=scope.channel_id,
        ):
            last = update
            print(
                safe_json_dumps(
                    {
                        "delta": update.get("delta"),
                        "text": update.get("generated_text"),
                        "citations": update.get("citations"),
                    }
                ),
                flush=True,
            )
        # A mid-stream provider failure ends the generator gracefully
        # (partial text printed) but flags stream_error — it is NOT a
        # successful run. Persist it as failed with the error recorded
        # instead of saving truncated text as completed (issue #1034,
        # cycle-review).
        if last and last.get("stream_error"):
            await db.repos.generation_runs.set_status(
                run_id, "failed", metadata={"stream_error": last["stream_error"]}
            )
            print(
                safe_json_dumps({"event": "error", "error": last["stream_error"]}),
                flush=True,
            )
            return
        final_text = last.get("generated_text") if last else ""
        metadata = {"citations": last.get("citations", []) if last else []}
        await db.repos.generation_runs.save_result(run_id, final_text, metadata)
        await db.repos.generation_runs.set_status(run_id, "completed")
        print(safe_json_dumps({"event": "done", "run_id": run_id}), flush=True)
    except Exception as exc:
        await db.repos.generation_runs.set_status(run_id, "failed")
        print(safe_json_dumps({"event": "error", "error": str(exc)}), flush=True)
    except BaseException:
        # Ctrl+C raises CancelledError (a BaseException, not Exception in
        # py3.11), which would otherwise leave the run stuck in "running".
        # Mirror the web handler: mark failed, then re-raise to abort. (#737)
        await db.repos.generation_runs.set_status(run_id, "failed")
        raise



async def _pipeline_runs(args: argparse.Namespace, db, config, svc: PipelineService) -> None:
    pipeline = await svc.get(args.id)
    if pipeline is None:
        print(f"Pipeline id={args.id} not found")
        return
    runs = await db.repos.generation_runs.list_by_pipeline(
        args.id, limit=args.limit
    )
    if args.status:
        runs = [r for r in runs if r.status == args.status]
    if not runs:
        print("No generation runs found.")
        return
    print(f"{'ID':<8} {'Status':<12} {'ModStatus':<12} {'Result':<22} {'Created':<20}")
    print("-" * 82)
    for r in runs:
        created = r.created_at.isoformat() if r.created_at else "—"
        result_summary = f"{r.result_kind}:{r.result_count}"
        print(f"{r.id:<8} {r.status:<12} {r.moderation_status:<12} {result_summary:<22} {created:<20}")



async def _pipeline_run_show(args: argparse.Namespace, db, config, svc: PipelineService) -> None:
    run = await db.repos.generation_runs.get(args.run_id)
    if run is None:
        print(f"Run id={args.run_id} not found")
        return
    print(f"id={run.id}")
    print(f"pipeline_id={run.pipeline_id}")
    print(f"status={run.status}")
    print(f"moderation_status={run.moderation_status}")
    print(f"result_kind={run.result_kind}")
    print(f"result_count={run.result_count}")
    print(f"created_at={run.created_at}")
    if run.generated_text:
        print("--- GENERATED TEXT ---")
        print(run.generated_text[:500])
        if len(run.generated_text) > 500:
            print(f"... ({len(run.generated_text) - 500} more chars)")
    else:
        print(f"result_label={result_kind_label(run.result_kind)}")
    if run.image_url:
        print(f"image_url={run.image_url}")
    if run.published_at:
        print(f"published_at={run.published_at}")
    metadata = run.metadata if isinstance(run.metadata, dict) else {}
    node_errors = metadata.get("node_errors")
    if isinstance(node_errors, list) and node_errors:
        print(f"Ошибки нод: {len(node_errors)}")
        for err in node_errors:
            if not isinstance(err, dict):
                continue
            node_id = err.get("node_id", "?")
            code = err.get("code", "unknown")
            detail = err.get("detail", "")
            line = f"  - [{node_id}] {code}: {detail}"
            if err.get("retry_after") is not None:
                line += f" retry_after={err['retry_after']}"
            print(line)



async def _pipeline_variants(args: argparse.Namespace, db, config, svc: PipelineService) -> None:
    from src.services.ab_testing_service import ABTestingService

    ab_service = ABTestingService(db, config=config)
    ab_result = await ab_service.get_variants(args.run_id)
    if ab_result is None:
        print(f"Run id={args.run_id} not found")
        return
    print(f"Run id={ab_result.run_id}: {len(ab_result.variants)} variant(s)")
    for variant in ab_result.variants:
        marker = " *selected*" if ab_result.selected_index == variant.index else ""
        print(f"[{variant.index}]{marker} {variant.text[:200]}")



async def _pipeline_select_variant(args: argparse.Namespace, db, config, svc: PipelineService) -> None:
    from src.services.ab_testing_service import ABTestingService

    ab_service = ABTestingService(db, config=config)
    try:
        await ab_service.select_variant(args.run_id, args.index)
    except ValueError as exc:
        print(f"Error: {exc}")
        return
    print(f"Selected variant {args.index} for run id={args.run_id}")



async def _pipeline_auto_select(args: argparse.Namespace, db, config, svc: PipelineService) -> None:
    from src.services.ab_testing_service import ABTestingService
    from src.services.provider_service import build_provider_service
    from src.services.quality_scoring_service import QualityScoringService

    provider_svc = await build_provider_service(db, config)
    quality_service = QualityScoringService(db, provider_service=provider_svc)
    ab_service = ABTestingService(db, provider_service=provider_svc, config=config)
    best_index = await ab_service.auto_select_best(
        args.run_id, scoring_service=quality_service
    )
    print(f"Auto-selected variant {best_index} for run id={args.run_id}")



async def _pipeline_queue(args: argparse.Namespace, db, config, svc: PipelineService) -> None:
    pipeline = await svc.get(args.id)
    if pipeline is None:
        print(f"Pipeline id={args.id} not found")
        return
    runs = await db.repos.generation_runs.list_pending_moderation(
        pipeline_id=args.id,
        limit=args.limit,
    )
    if not runs:
        print(f"No pending moderation runs for pipeline id={args.id}")
        return
    print(f"{'Run ID':<8} {'Status':<12} {'Created':<19} Preview")
    print("-" * 80)
    for run in runs:
        created_at = (
            run.created_at.strftime("%Y-%m-%d %H:%M:%S") if run.created_at else "—"
        )
        print(
            f"{run.id or 0:<8} {run.moderation_status:<12} "
            f"{created_at:<19} {_preview_text(run.generated_text)}"
        )



async def _pipeline_moderation_list(args: argparse.Namespace, db, config, svc: PipelineService) -> None:
    runs = await db.repos.generation_runs.list_pending_moderation(
        pipeline_id=args.pipeline_id,
        limit=args.limit,
    )
    if not runs:
        print("No pending moderation runs.")
        return
    print(f"{'Run ID':<8} {'Pipeline':<8} {'Status':<12} {'Created':<19} Preview")
    print("-" * 90)
    for run in runs:
        created_at = (
            run.created_at.strftime("%Y-%m-%d %H:%M:%S") if run.created_at else "—"
        )
        print(
            f"{run.id or 0:<8} {run.pipeline_id or 0:<8} {run.moderation_status:<12} "
            f"{created_at:<19} {_preview_text(run.generated_text)}"
        )



async def _pipeline_moderation_view(args: argparse.Namespace, db, config, svc: PipelineService) -> None:
    run = await db.repos.generation_runs.get(args.run_id)
    if run is None:
        print(f"Run id={args.run_id} not found")
        return
    print(f"Run id={run.id} (pipeline_id={run.pipeline_id})")
    print(f"Status: {run.status}")
    print(f"Moderation: {run.moderation_status}")
    print(f"Created: {run.created_at}")
    print("")
    print("Generated text:")
    print(run.generated_text or "(empty)")



async def _pipeline_approve(args: argparse.Namespace, db, config, svc: PipelineService) -> None:
    run = await db.repos.generation_runs.get(args.run_id)
    if run is None:
        print(f"Run id={args.run_id} not found")
        return
    await db.repos.generation_runs.set_moderation_status(args.run_id, "approved")
    print(f"Approved run id={args.run_id}")



async def _pipeline_reject(args: argparse.Namespace, db, config, svc: PipelineService) -> None:
    run = await db.repos.generation_runs.get(args.run_id)
    if run is None:
        print(f"Run id={args.run_id} not found")
        return
    await db.repos.generation_runs.set_moderation_status(args.run_id, "rejected")
    print(f"Rejected run id={args.run_id}")



async def _pipeline_bulk_approve(args: argparse.Namespace, db, config, svc: PipelineService) -> None:
    # Atomic batch (#1041): resolve which ids exist (preserving the
    # skip-missing behaviour), then flip them all in one transaction
    # so a mid-batch failure rolls back instead of half-applying.
    existing_approve_ids: list[int] = []
    for run_id in args.run_ids:
        run = await db.repos.generation_runs.get(run_id)
        if run is None:
            print(f"  Run id={run_id} not found, skipping.")
            continue
        existing_approve_ids.append(run_id)
    await db.repos.generation_runs.set_moderation_status_bulk(
        existing_approve_ids, "approved"
    )
    print(f"Bulk approved: {len(existing_approve_ids)}/{len(args.run_ids)}")



async def _pipeline_bulk_reject(args: argparse.Namespace, db, config, svc: PipelineService) -> None:
    # Atomic batch (#1041): see bulk-approve above for the rationale.
    existing_reject_ids: list[int] = []
    for run_id in args.run_ids:
        run = await db.repos.generation_runs.get(run_id)
        if run is None:
            print(f"  Run id={run_id} not found, skipping.")
            continue
        existing_reject_ids.append(run_id)
    await db.repos.generation_runs.set_moderation_status_bulk(
        existing_reject_ids, "rejected"
    )
    print(f"Bulk rejected: {len(existing_reject_ids)}/{len(args.run_ids)}")



async def _pipeline_publish(args: argparse.Namespace, db, config, svc: PipelineService) -> ClientPool | None:
    run = await db.repos.generation_runs.get(args.run_id)
    if run is None:
        print(f"Run id={args.run_id} not found")
        return None
    if run.pipeline_id is None:
        print(f"Run id={args.run_id} has no pipeline")
        return None

    pipeline = await svc.get(run.pipeline_id)
    if pipeline is None:
        print(f"Pipeline id={run.pipeline_id} not found")
        return None

    _, pool = await runtime.init_pool(config, db)
    if not pool.clients:
        print("ERROR: Нет доступных аккаунтов Telegram.")
        return pool

    publish_service = PublishService(db, pool)
    results = await publish_service.publish_run(run, pipeline)
    if not results or not all(result.success for result in results):
        print(f"Failed to publish run id={args.run_id}")
        for result in results:
            if not result.success:
                print(f"  - {result.error or 'Unknown publish error'}")
        return pool

    print(
        f"Published run id={args.run_id} to {len(results)} target(s)"
    )
    for r in results:
        if r.message_id is not None:
            phone_part = f" phone={r.phone}" if r.phone else ""
            dialog_part = f" dialog_id={r.dialog_id}" if r.dialog_id is not None else ""
            print(f"  published_message_id={r.message_id}{phone_part}{dialog_part}")
    return pool


async def _pipeline_refinement_steps(args: argparse.Namespace, db, config, svc: PipelineService) -> None:
    pipeline = await svc.get(args.id)
    if pipeline is None:
        print(f"Pipeline id={args.id} not found")
        return
    if args.steps_json:
        import json

        try:
            steps = json.loads(args.steps_json)
        except json.JSONDecodeError as exc:
            print(f"Invalid JSON: {exc}")
            return
        if not isinstance(steps, list):
            print("Refinement steps must be a JSON array.")
            return
        await db.repos.content_pipelines.set_refinement_steps(args.id, steps)
        print(f"Set {len(steps)} refinement step(s) for pipeline id={args.id}.")
    else:
        steps = pipeline.refinement_steps or []
        if not steps:
            print(f"Pipeline id={args.id} has no refinement steps.")
        else:
            import json

            print(json.dumps(steps, ensure_ascii=False, indent=2))



async def _pipeline_export(args: argparse.Namespace, db, config, svc: PipelineService) -> None:
    import json

    data = await svc.export_json(args.id)
    if data is None:
        print(f"Pipeline id={args.id} not found")
        return
    output = json.dumps(data, ensure_ascii=False, indent=2)
    if args.output:
        if await asyncio.to_thread(os.path.exists, args.output) and not args.force:
            print(
                f"File {args.output} already exists. Use --force to overwrite."
            )
            return
        with open(args.output, "w", encoding="utf-8") as fh:
            fh.write(output)
        print(f"Exported pipeline id={args.id} to {args.output}")
    else:
        print(output)



async def _pipeline_import(args: argparse.Namespace, db, config, svc: PipelineService) -> None:
    import json

    with open(args.file, encoding="utf-8") as fh:
        data = json.load(fh)
    try:
        pipeline_id = await svc.import_json(data, name_override=getattr(args, "name", None))
        print(f"Imported pipeline (id={pipeline_id})")
    except PipelineValidationError as exc:
        print(f"Validation error: {exc}")



async def _pipeline_templates(args: argparse.Namespace, db, config, svc: PipelineService) -> None:
    templates = await svc.list_templates(category=getattr(args, "category", None))
    if not templates:
        print("No templates found.")
        return
    print(f"{'ID':<5} {'Category':<14} {'Name':<32} Description")
    print("-" * 80)
    for tpl in templates:
        builtin = " [builtin]" if tpl.is_builtin else ""
        print(f"{tpl.id or '—':<5} {tpl.category:<14} {tpl.name:<32} {tpl.description[:40]}{builtin}")



async def _pipeline_from_template(args: argparse.Namespace, db, config, svc: PipelineService) -> None:
    try:
        template_id = args.template_id
        name = args.name
        source_ids = (
            [int(x.strip()) for x in args.source_ids.split(",") if x.strip()]
            if args.source_ids else []
        )
        target_refs = _parse_target_refs(args.target_refs.split(",") if args.target_refs else [])
        pipeline_id = await svc.create_from_template(
            template_id,
            name=name,
            source_ids=source_ids,
            target_refs=target_refs,
        )
        print(f"Created pipeline from template (id={pipeline_id})")
    except PipelineValidationError as exc:
        print(f"Validation error: {exc}")



async def _pipeline_dry_run_count(args: argparse.Namespace, db, config, svc: PipelineService) -> None:
    since_h = to_since_hours(args.since_value, args.since_unit)
    msgs = await db.repos.messages.get_recent_for_channels(args.source, since_h)
    print(
        f"Messages found: {len(msgs)} "
        f"(sources={args.source}, since={args.since_value}{args.since_unit})"
    )



async def _pipeline_ai_edit(args: argparse.Namespace, db, config, svc: PipelineService) -> None:
    instruction = args.instruction
    result = await svc.edit_via_llm(args.id, instruction, db, config=config)
    import json

    if result["ok"]:
        print("Pipeline JSON updated successfully.")
        if getattr(args, "show", False):
            print(json.dumps(result["pipeline_json"], ensure_ascii=False, indent=2))
    else:
        print(f"Error: {result['error']}")



async def _pipeline_filter(args: argparse.Namespace, db, config, svc: PipelineService) -> None:
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
        ok = await _upsert_filter_node(svc, args.id, config)
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



async def _pipeline_node(args: argparse.Namespace, db, config, svc: PipelineService) -> None:
    from src.cli.node_dsl import NodeSpecError, parse_node_spec

    if args.node_action == "add":
        try:
            spec = parse_node_spec(args.node_spec)
        except NodeSpecError as exc:
            print(f"Invalid node spec: {exc}")
            return
        from src.cli.node_dsl import generate_node_id

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



async def _pipeline_edge(args: argparse.Namespace, db, config, svc: PipelineService) -> None:
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



async def _pipeline_graph(args: argparse.Namespace, db, config, svc: PipelineService) -> None:
    graph = await svc.get_graph(args.id)
    if graph is None:
        pipeline = await svc.get(args.id)
        if pipeline is None:
            print(f"Pipeline id={args.id} not found")
        else:
            print(f"Pipeline id={args.id} has no graph (legacy pipeline)")
        return
    from src.cli.graph_viz import render_ascii

    print(render_ascii(graph))



_PIPELINE_HANDLERS = {
    "list": _pipeline_list,
    "show": _pipeline_show,
    "add": _pipeline_add,
    "edit": _pipeline_edit,
    "toggle": _pipeline_toggle,
    "delete": _pipeline_delete,
    "run": _pipeline_run,
    "generate": _pipeline_generate,
    "generate-stream": _pipeline_generate_stream,
    "runs": _pipeline_runs,
    "run-show": _pipeline_run_show,
    "variants": _pipeline_variants,
    "select-variant": _pipeline_select_variant,
    "auto-select": _pipeline_auto_select,
    "queue": _pipeline_queue,
    "moderation-list": _pipeline_moderation_list,
    "moderation-view": _pipeline_moderation_view,
    "approve": _pipeline_approve,
    "reject": _pipeline_reject,
    "bulk-approve": _pipeline_bulk_approve,
    "bulk-reject": _pipeline_bulk_reject,
    "publish": _pipeline_publish,
    "refinement-steps": _pipeline_refinement_steps,
    "export": _pipeline_export,
    "import": _pipeline_import,
    "templates": _pipeline_templates,
    "from-template": _pipeline_from_template,
    "dry-run-count": _pipeline_dry_run_count,
    "ai-edit": _pipeline_ai_edit,
    "filter": _pipeline_filter,
    "node": _pipeline_node,
    "edge": _pipeline_edge,
    "graph": _pipeline_graph,
}


async def _dispatch(args: argparse.Namespace) -> None:
    """Shared async body for every ``pipeline`` action (incl. nested groups).

    Opens the db, lazily opens the client pool only for actions that need it
    (run / generate / publish), dispatches on ``args.pipeline_action`` (and the
    nested ``filter_action`` / ``node_action`` / ``edge_action`` for the depth-2
    groups), and always disconnects any opened pool + closes the db in finally.
    Called by the argparse ``run`` wrapper (its own ``asyncio.run``) and, via
    ``run_async``, by the Typer command bodies — byte-identical logic, including
    the ``generate-stream`` JSON-Lines streaming + graceful-partial semantics.
    """
    config, db = await runtime.init_db(args.config)
    pool = None
    try:
        svc = PipelineService(db)
        handler = _PIPELINE_HANDLERS.get(args.pipeline_action)
        if handler is not None:
            pool = await handler(args, db, config, svc) or pool
    finally:
        if pool is not None:
            await pool.disconnect_all()
        await db.close()


def run(args: argparse.Namespace) -> None:
    """Primary CLI entrypoint for content pipelines — owns the single asyncio.run."""
    asyncio.run(_dispatch(args))


# --------------------------------------------------------------------------- #
# pipeline → list / show / add / dry-run-count / edit / delete / toggle / run /
#   generate / generate-stream / runs / run-show / variants / select-variant /
#   auto-select / queue / moderation-list / moderation-view / publish / approve /
#   reject / bulk-approve / bulk-reject / refinement-steps / export / import /
#   templates / from-template / ai-edit / graph
#   + NESTED depth-2: filter (set/show/clear), node (add/replace/remove),
#     edge (add/remove)
#
# Every pipeline leaf builds the argparse Namespace ``_dispatch``
# reads and runs it via ``run_async`` — so the Typer path executes the exact
# same logic, including the ``generate-stream`` JSON-Lines streaming and the
# pool lifecycle. The argparse ``append`` (variadic) options are expressed as
# repeated Typer options (``--source 1 --source 2``); see the known-drift note
# on ``_pipeline_argv``.
# --------------------------------------------------------------------------- #

pipeline_app = typer.Typer(no_args_is_help=True, help="Content pipelines")

# Three nested depth-2 groups mounted via add_typer; the frozen
# ``pipeline filter|node|edge <action>`` paths are the fragile Wave-4 invariant.
pipeline_filter_app = typer.Typer(no_args_is_help=True, help="Manage a pipeline's message filter")
pipeline_app.add_typer(pipeline_filter_app, name="filter")
pipeline_node_app = typer.Typer(no_args_is_help=True, help="Manage pipeline graph nodes")
pipeline_app.add_typer(pipeline_node_app, name="node")
pipeline_edge_app = typer.Typer(no_args_is_help=True, help="Manage pipeline graph edges")
pipeline_app.add_typer(pipeline_edge_app, name="edge")


def _run_pipeline(ctx: typer.Context, pipeline_action: str, **ns_kwargs) -> None:
    """Build the Namespace a pipeline action dispatches on, then run it."""
    apply_startup(ctx)
    ns = argparse.Namespace(
        config=ctx.obj.config, pipeline_action=pipeline_action, **ns_kwargs
    )
    run_async(_dispatch(ns))


@pipeline_app.command("list")
def pipeline_list(ctx: typer.Context) -> None:
    """List pipelines."""
    _run_pipeline(ctx, "list")


@pipeline_app.command("show")
def pipeline_show(ctx: typer.Context, id: int = typer.Argument(..., help="Pipeline id")) -> None:
    """Show pipeline details."""
    _run_pipeline(ctx, "show", id=id)


@pipeline_app.command("add")
def pipeline_add(
    ctx: typer.Context,
    name: str = typer.Argument(..., help="Pipeline name"),
    prompt_template: str | None = typer.Option(
        None, "--prompt-template", help="Prompt template (required unless --json-file/--node is used)"
    ),
    json_file: str | None = typer.Option(None, "--json-file"),
    source: list[int] = typer.Option([], "--source", help="Source channel id (repeat for multiple)"),
    target: list[str] = typer.Option([], "--target", help="Target PHONE|DIALOG_ID (repeat for multiple)"),
    llm_model: str | None = typer.Option(None, "--llm-model"),
    image_model: str | None = typer.Option(None, "--image-model"),
    publish_mode: PublishMode = typer.Option(PublishMode.moderated, "--publish-mode"),
    generation_backend: GenerationBackend = typer.Option(GenerationBackend.chain, "--generation-backend"),
    interval: int = typer.Option(60, "--interval"),
    inactive: bool = typer.Option(False, "--inactive"),
    ab_variants: int = typer.Option(1, "--ab-variants"),
    ab_auto_select: bool = typer.Option(False, "--ab-auto-select"),
    node_specs: list[str] = typer.Option([], "--node", help="Node spec (repeat for multiple)"),
    edge: list[str] = typer.Option([], "--edge", help="Explicit edge FROM->TO (repeat)"),
    node_configs: list[str] = typer.Option([], "--node-config", help="Node config NODE=JSON (repeat)"),
    run_after: bool = typer.Option(False, "--run-after"),
    since_value: int = typer.Option(24, "--since-value"),
    since_unit: SinceUnit = typer.Option(SinceUnit.h, "--since-unit"),
) -> None:
    """Add a pipeline."""
    _run_pipeline(
        ctx, "add", name=name, prompt_template=prompt_template, json_file=json_file,
        source=list(source) or None, target=list(target) or None, llm_model=llm_model,
        image_model=image_model, publish_mode=publish_mode.value,
        generation_backend=generation_backend.value, interval=interval, inactive=inactive,
        ab_variants=ab_variants, ab_auto_select=ab_auto_select,
        node_specs=list(node_specs) or None, edge=list(edge) or None,
        node_configs=list(node_configs) or None, run_after=run_after,
        since_value=since_value, since_unit=since_unit.value,
    )


@pipeline_app.command("dry-run-count")
def pipeline_dry_run_count(
    ctx: typer.Context,
    source: list[int] = typer.Option(..., "--source", help="Source channel id (repeat for multiple)"),
    since_value: int = typer.Option(24, "--since-value"),
    since_unit: SinceUnit = typer.Option(SinceUnit.h, "--since-unit"),
) -> None:
    """Count messages for given sources."""
    _run_pipeline(ctx, "dry-run-count", source=list(source), since_value=since_value, since_unit=since_unit.value)


@pipeline_app.command("edit")
def pipeline_edit(
    ctx: typer.Context,
    id: int = typer.Argument(..., help="Pipeline id"),
    name: str | None = typer.Option(None, "--name"),
    prompt_template: str | None = typer.Option(None, "--prompt-template"),
    source: list[int] = typer.Option([], "--source"),
    target: list[str] = typer.Option([], "--target"),
    llm_model: str | None = typer.Option(None, "--llm-model"),
    image_model: str | None = typer.Option(None, "--image-model"),
    publish_mode: PublishMode | None = typer.Option(None, "--publish-mode"),
    generation_backend: GenerationBackend | None = typer.Option(None, "--generation-backend"),
    interval: int | None = typer.Option(None, "--interval"),
    active: bool | None = typer.Option(
        None, "--active/--inactive", help="Set active (--active) or inactive (--inactive)"
    ),
    ab_variants: int | None = typer.Option(None, "--ab-variants"),
    ab_auto_select: bool | None = typer.Option(None, "--ab-auto-select/--no-ab-auto-select"),
) -> None:
    """Edit a pipeline."""
    _run_pipeline(
        ctx, "edit", id=id, name=name, prompt_template=prompt_template,
        source=list(source) or None, target=list(target) or None, llm_model=llm_model,
        image_model=image_model,
        publish_mode=publish_mode.value if publish_mode else None,
        generation_backend=generation_backend.value if generation_backend else None,
        interval=interval, active=active, ab_variants=ab_variants, ab_auto_select=ab_auto_select,
    )


@pipeline_app.command("delete")
def pipeline_delete(ctx: typer.Context, id: int = typer.Argument(..., help="Pipeline id")) -> None:
    """Delete a pipeline."""
    _run_pipeline(ctx, "delete", id=id)


@pipeline_app.command("toggle")
def pipeline_toggle(ctx: typer.Context, id: int = typer.Argument(..., help="Pipeline id")) -> None:
    """Toggle pipeline active state."""
    _run_pipeline(ctx, "toggle", id=id)


@pipeline_app.command("run")
def pipeline_run(
    ctx: typer.Context,
    id: int = typer.Argument(..., help="Pipeline id"),
    preview: bool = typer.Option(False, "--preview"),
    publish: bool = typer.Option(False, "--publish"),
    limit: int = typer.Option(8, "--limit"),
    max_tokens: int = typer.Option(256, "--max-tokens"),
    temperature: float = typer.Option(0.0, "--temperature"),
) -> None:
    """Run pipeline generation (preview/publish)."""
    _run_pipeline(
        ctx, "run", id=id, preview=preview, publish=publish, limit=limit,
        max_tokens=max_tokens, temperature=temperature,
    )


@pipeline_app.command("generate")
def pipeline_generate(
    ctx: typer.Context,
    id: int = typer.Argument(..., help="Pipeline id"),
    max_tokens: int = typer.Option(512, "--max-tokens"),
    temperature: float = typer.Option(0.7, "--temperature"),
    model: str | None = typer.Option(None, "--model"),
    preview: bool = typer.Option(False, "--preview"),
    ab_variants: int | None = typer.Option(None, "--ab-variants"),
    auto_select: bool = typer.Option(False, "--auto-select"),
) -> None:
    """Generate content for a pipeline."""
    _run_pipeline(
        ctx, "generate", id=id, max_tokens=max_tokens, temperature=temperature, model=model,
        preview=preview, ab_variants=ab_variants, auto_select=auto_select,
    )


@pipeline_app.command("generate-stream")
def pipeline_generate_stream(
    ctx: typer.Context,
    id: int = typer.Argument(..., help="Pipeline id"),
    model: str | None = typer.Option(None, "--model"),
    max_tokens: int = typer.Option(256, "--max-tokens"),
    temperature: float = typer.Option(0.0, "--temperature"),
    limit: int = typer.Option(8, "--limit"),
) -> None:
    """Generate content for a pipeline, streaming JSON-Lines updates."""
    _run_pipeline(
        ctx, "generate-stream", id=id, model=model, max_tokens=max_tokens,
        temperature=temperature, limit=limit,
    )


@pipeline_app.command("runs")
def pipeline_runs(
    ctx: typer.Context,
    id: int = typer.Argument(..., help="Pipeline id"),
    limit: int = typer.Option(20, "--limit"),
    status: str | None = typer.Option(None, "--status"),
) -> None:
    """List generation runs."""
    _run_pipeline(ctx, "runs", id=id, limit=limit, status=status)


@pipeline_app.command("run-show")
def pipeline_run_show(ctx: typer.Context, run_id: int = typer.Argument(..., help="Run id")) -> None:
    """Show generation run details."""
    _run_pipeline(ctx, "run-show", run_id=run_id)


@pipeline_app.command("variants")
def pipeline_variants(ctx: typer.Context, run_id: int = typer.Argument(..., help="Run id")) -> None:
    """List A/B variants."""
    _run_pipeline(ctx, "variants", run_id=run_id)


@pipeline_app.command("select-variant")
def pipeline_select_variant(
    ctx: typer.Context,
    run_id: int = typer.Argument(..., help="Run id"),
    index: int = typer.Argument(..., help="Variant index"),
) -> None:
    """Select an A/B variant."""
    _run_pipeline(ctx, "select-variant", run_id=run_id, index=index)


@pipeline_app.command("auto-select")
def pipeline_auto_select(ctx: typer.Context, run_id: int = typer.Argument(..., help="Run id")) -> None:
    """Auto-select the best A/B variant."""
    _run_pipeline(ctx, "auto-select", run_id=run_id)


@pipeline_app.command("queue")
def pipeline_queue(
    ctx: typer.Context,
    id: int = typer.Argument(..., help="Pipeline id"),
    limit: int = typer.Option(20, "--limit"),
) -> None:
    """Show pending moderation queue for a pipeline."""
    _run_pipeline(ctx, "queue", id=id, limit=limit)


@pipeline_app.command("moderation-list")
def pipeline_moderation_list(
    ctx: typer.Context,
    pipeline_id: int | None = typer.Option(None, "--pipeline-id"),
    limit: int = typer.Option(20, "--limit"),
) -> None:
    """List pending moderation runs."""
    _run_pipeline(ctx, "moderation-list", pipeline_id=pipeline_id, limit=limit)


@pipeline_app.command("moderation-view")
def pipeline_moderation_view(ctx: typer.Context, run_id: int = typer.Argument(..., help="Run id")) -> None:
    """Show a moderation run's details."""
    _run_pipeline(ctx, "moderation-view", run_id=run_id)


@pipeline_app.command("publish")
def pipeline_publish(ctx: typer.Context, run_id: int = typer.Argument(..., help="Run id")) -> None:
    """Publish a generation run."""
    _run_pipeline(ctx, "publish", run_id=run_id)


@pipeline_app.command("approve")
def pipeline_approve(ctx: typer.Context, run_id: int = typer.Argument(..., help="Run id")) -> None:
    """Approve a generation run."""
    _run_pipeline(ctx, "approve", run_id=run_id)


@pipeline_app.command("reject")
def pipeline_reject(ctx: typer.Context, run_id: int = typer.Argument(..., help="Run id")) -> None:
    """Reject a generation run."""
    _run_pipeline(ctx, "reject", run_id=run_id)


@pipeline_app.command("bulk-approve")
def pipeline_bulk_approve(
    ctx: typer.Context,
    run_ids: list[int] = typer.Argument(..., help="Run ids"),
) -> None:
    """Approve multiple generation runs."""
    _run_pipeline(ctx, "bulk-approve", run_ids=list(run_ids))


@pipeline_app.command("bulk-reject")
def pipeline_bulk_reject(
    ctx: typer.Context,
    run_ids: list[int] = typer.Argument(..., help="Run ids"),
) -> None:
    """Reject multiple generation runs."""
    _run_pipeline(ctx, "bulk-reject", run_ids=list(run_ids))


@pipeline_app.command("refinement-steps")
def pipeline_refinement_steps(
    ctx: typer.Context,
    id: int = typer.Argument(..., help="Pipeline id"),
    steps_json: str | None = typer.Option(None, "--set", help="Set refinement steps (JSON array)"),
) -> None:
    """View or set refinement steps."""
    _run_pipeline(ctx, "refinement-steps", id=id, steps_json=steps_json)


@pipeline_app.command("export")
def pipeline_export(
    ctx: typer.Context,
    id: int = typer.Argument(..., help="Pipeline id"),
    output: str | None = typer.Option(None, "--output", "-o"),
    force: bool = typer.Option(False, "--force", "-f"),
) -> None:
    """Export a pipeline as JSON."""
    _run_pipeline(ctx, "export", id=id, output=output, force=force)


@pipeline_app.command("import")
def pipeline_import(
    ctx: typer.Context,
    file: str = typer.Argument(..., help="Path to JSON file"),
    name: str | None = typer.Option(None, "--name"),
) -> None:
    """Import a pipeline from a JSON file."""
    _run_pipeline(ctx, "import", file=file, name=name)


@pipeline_app.command("templates")
def pipeline_templates(
    ctx: typer.Context,
    category: str | None = typer.Option(None, "--category"),
) -> None:
    """List available pipeline templates."""
    _run_pipeline(ctx, "templates", category=category)


@pipeline_app.command("from-template")
def pipeline_from_template(
    ctx: typer.Context,
    template_id: int = typer.Argument(..., help="Template id"),
    name: str = typer.Argument(..., help="Pipeline name"),
    source_ids: str = typer.Option("", "--source-ids"),
    target_refs: str = typer.Option("", "--target-refs"),
) -> None:
    """Create a pipeline from a template."""
    _run_pipeline(
        ctx, "from-template", template_id=template_id, name=name,
        source_ids=source_ids, target_refs=target_refs,
    )


@pipeline_app.command("ai-edit")
def pipeline_ai_edit(
    ctx: typer.Context,
    id: int = typer.Argument(..., help="Pipeline id"),
    instruction: str = typer.Argument(..., help="Instruction for the LLM"),
    show: bool = typer.Option(False, "--show"),
) -> None:
    """Edit a pipeline's JSON via an LLM instruction."""
    _run_pipeline(ctx, "ai-edit", id=id, instruction=instruction, show=show)


@pipeline_app.command("graph")
def pipeline_graph(ctx: typer.Context, id: int = typer.Argument(..., help="Pipeline id")) -> None:
    """Show a pipeline's graph (ASCII)."""
    _run_pipeline(ctx, "graph", id=id)


# ---- nested: pipeline filter <action> ------------------------------------- #


@pipeline_filter_app.command("set")
def pipeline_filter_set(
    ctx: typer.Context,
    id: int = typer.Argument(..., help="Pipeline id"),
    message_kinds: list[str] = typer.Option([], "--message-kind"),
    service_actions: list[str] = typer.Option([], "--service-action"),
    media_types: list[str] = typer.Option([], "--media-type"),
    sender_kinds: list[str] = typer.Option([], "--sender-kind"),
    keywords: list[str] = typer.Option([], "--keyword"),
    regex: str | None = typer.Option(None, "--regex"),
    forwarded: TriBool | None = typer.Option(None, "--forwarded"),
    has_text: TriBool | None = typer.Option(None, "--has-text"),
) -> None:
    """Set a pipeline's message filter."""
    _run_pipeline(
        ctx, "filter", filter_action="set", id=id,
        message_kinds=list(message_kinds) or None, service_actions=list(service_actions) or None,
        media_types=list(media_types) or None, sender_kinds=list(sender_kinds) or None,
        keywords=list(keywords) or None, regex=regex,
        forwarded=forwarded.value if forwarded else None,
        has_text=has_text.value if has_text else None,
    )


@pipeline_filter_app.command("show")
def pipeline_filter_show(ctx: typer.Context, id: int = typer.Argument(..., help="Pipeline id")) -> None:
    """Show a pipeline's message filter."""
    _run_pipeline(ctx, "filter", filter_action="show", id=id)


@pipeline_filter_app.command("clear")
def pipeline_filter_clear(ctx: typer.Context, id: int = typer.Argument(..., help="Pipeline id")) -> None:
    """Clear a pipeline's message filter."""
    _run_pipeline(ctx, "filter", filter_action="clear", id=id)


# ---- nested: pipeline node <action> --------------------------------------- #


@pipeline_node_app.command("add")
def pipeline_node_add(
    ctx: typer.Context,
    pipeline_id: int = typer.Argument(..., help="Pipeline id"),
    node_spec: str = typer.Argument(..., help="Node spec: type:key=value,..."),
) -> None:
    """Add a node to a pipeline graph."""
    _run_pipeline(ctx, "node", node_action="add", pipeline_id=pipeline_id, node_spec=node_spec)


@pipeline_node_app.command("replace")
def pipeline_node_replace(
    ctx: typer.Context,
    pipeline_id: int = typer.Argument(..., help="Pipeline id"),
    node_id: str = typer.Argument(..., help="Node ID to replace"),
    node_spec: str = typer.Argument(..., help="New node spec: type:key=value,..."),
) -> None:
    """Replace a node in a pipeline graph."""
    _run_pipeline(ctx, "node", node_action="replace", pipeline_id=pipeline_id, node_id=node_id, node_spec=node_spec)


@pipeline_node_app.command("remove")
def pipeline_node_remove(
    ctx: typer.Context,
    pipeline_id: int = typer.Argument(..., help="Pipeline id"),
    node_id: str = typer.Argument(..., help="Node ID to remove"),
) -> None:
    """Remove a node from a pipeline graph."""
    _run_pipeline(ctx, "node", node_action="remove", pipeline_id=pipeline_id, node_id=node_id)


# ---- nested: pipeline edge <action> --------------------------------------- #


@pipeline_edge_app.command("add")
def pipeline_edge_add(
    ctx: typer.Context,
    pipeline_id: int = typer.Argument(..., help="Pipeline id"),
    from_node: str = typer.Argument(..., help="Source node ID"),
    to_node: str = typer.Argument(..., help="Target node ID"),
) -> None:
    """Add an edge to a pipeline graph."""
    _run_pipeline(ctx, "edge", edge_action="add", pipeline_id=pipeline_id, from_node=from_node, to_node=to_node)


@pipeline_edge_app.command("remove")
def pipeline_edge_remove(
    ctx: typer.Context,
    pipeline_id: int = typer.Argument(..., help="Pipeline id"),
    from_node: str = typer.Argument(..., help="Source node ID"),
    to_node: str = typer.Argument(..., help="Target node ID"),
) -> None:
    """Remove an edge from a pipeline graph."""
    _run_pipeline(ctx, "edge", edge_action="remove", pipeline_id=pipeline_id, from_node=from_node, to_node=to_node)
