from __future__ import annotations

import argparse
import asyncio

from src.cli import runtime
from src.models import PipelineNode, PipelineNodeType
from src.search.engine import SearchEngine
from src.services.content_generation_service import ContentGenerationService
from src.services.pipeline_filters import normalize_filter_config
from src.services.pipeline_llm_requirements import pipeline_needs_llm
from src.services.pipeline_result import result_kind_label
from src.services.pipeline_service import (
    PipelineService,
    PipelineTargetRef,
    PipelineValidationError,
    to_since_hours,
)
from src.services.publish_service import PublishService


def _parse_target_refs(values: list[str]) -> list[PipelineTargetRef]:
    refs: list[PipelineTargetRef] = []
    for value in values:
        phone, separator, raw_dialog_id = value.partition("|")
        if not separator:
            raise PipelineValidationError("Target must be in PHONE|DIALOG_ID format.")
        try:
            dialog_id = int(raw_dialog_id)
        except ValueError as exc:
            raise PipelineValidationError("Target dialog id must be numeric.") from exc
        refs.append(PipelineTargetRef(phone=phone, dialog_id=dialog_id))
    return refs


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
    for downstream_id in downstream_ids:
        await svc.remove_edge(pipeline_id, upstream_id, downstream_id)
    if upstream_id:
        await svc.add_edge(pipeline_id, upstream_id, "filter_1")
    for downstream_id in downstream_ids:
        if downstream_id != "filter_1":
            await svc.add_edge(pipeline_id, "filter_1", downstream_id)
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
            await svc.add_edge(pipeline_id, source, target)
    return True


def run(args: argparse.Namespace) -> None:
    async def _run() -> None:
        config, db = await runtime.init_db(args.config)
        pool = None
        try:
            svc = PipelineService(db)

            if args.pipeline_action == "list":
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

            elif args.pipeline_action == "show":
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

            elif args.pipeline_action == "add":
                json_file = getattr(args, "json_file", None)
                node_specs_raw = getattr(args, "node_specs", None)
                try:
                    if json_file:
                        import json as _json

                        with open(json_file) as _f:
                            graph_data = _json.load(_f)
                        data = {
                            "name": args.name,
                            "prompt_template": args.prompt_template or ".",
                            "llm_model": args.llm_model,
                            "source_ids": args.source or [],
                            "target_refs": (
                                [{"phone": r.phone, "dialog_id": r.dialog_id}
                                 for r in _parse_target_refs(args.target)]
                                if args.target else []
                            ),
                            "generate_interval_minutes": args.interval,
                            "pipeline_json": graph_data,
                        }
                        pipeline_id = await svc.import_json(data)
                    elif node_specs_raw:
                        # DAG mode: build graph from --node specs
                        import json as _json

                        from src.cli.graph_builder import GraphBuilder, GraphBuilderError
                        from src.cli.node_dsl import NodeSpecError, parse_node_spec

                        specs = []
                        for raw in node_specs_raw:
                            try:
                                specs.append(parse_node_spec(raw))
                            except NodeSpecError as exc:
                                print(f"Invalid node spec '{raw}': {exc}")
                                return

                        builder = GraphBuilder()
                        for spec in specs:
                            builder.add_node_spec(spec)

                        if args.source:
                            builder.set_sources(args.source)

                        target_refs_list = []
                        if args.target:
                            target_refs_list = [
                                {"phone": t.phone, "dialog_id": t.dialog_id}
                                for t in _parse_target_refs(args.target)
                            ]
                            builder.set_targets(target_refs_list)

                        if getattr(args, "edge", None):
                            for edge_str in args.edge:
                                from_id, _, to_id = edge_str.partition("->")
                                if not to_id:
                                    print(f"Invalid edge format '{edge_str}'; use FROM->TO")
                                    return
                                builder.add_explicit_edge(from_id.strip(), to_id.strip())

                        if getattr(args, "node_configs", None):
                            for nc_str in args.node_configs:
                                node_id, _, json_str = nc_str.partition("=")
                                try:
                                    config = _json.loads(json_str)
                                except _json.JSONDecodeError as exc:
                                    print(f"Invalid JSON in --node-config for {node_id}: {exc}")
                                    return
                                builder.set_node_config_override(node_id.strip(), config)

                        try:
                            graph = builder.build()
                        except GraphBuilderError as exc:
                            print(f"Graph build error: {exc}")
                            return

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
                    else:
                        # Legacy mode: --prompt-template + --source + --target required
                        if not args.prompt_template:
                            print("Error: --prompt-template is required when --json-file/--node is not used")
                            return
                        if not args.source:
                            print("Error: --source is required")
                            return
                        if not args.target:
                            print("Error: --target is required")
                            return
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
                        )
                except PipelineValidationError as exc:
                    print(f"Error: {exc}")
                    return
                print(f"Added pipeline id={pipeline_id}: {args.name}")
                if getattr(args, "run_after", False):
                    from src.services.task_enqueuer import TaskEnqueuer

                    since_h = to_since_hours(args.since_value, args.since_unit)
                    enqueuer = TaskEnqueuer(db)
                    await enqueuer.enqueue_pipeline_run(pipeline_id, since_hours=since_h)
                    print(f"Enqueued pipeline run (since={args.since_value}{args.since_unit})")

            elif args.pipeline_action == "edit":
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
                    )
                except PipelineValidationError as exc:
                    print(f"Error: {exc}")
                    return
                if not ok:
                    print(f"Pipeline id={args.id} not found")
                    return
                print(f"Updated pipeline id={args.id}")

            elif args.pipeline_action == "toggle":
                ok = await svc.toggle(args.id)
                if not ok:
                    print(f"Pipeline id={args.id} not found")
                    return
                print(f"Toggled pipeline id={args.id}")

            elif args.pipeline_action == "delete":
                await svc.delete(args.id)
                print(f"Deleted pipeline id={args.id}")

            elif args.pipeline_action == "run":
                pipeline = await svc.get(args.id)
                if pipeline is None:
                    print(f"Pipeline id={args.id} not found")
                    return
                engine = SearchEngine(db)

                from src.services.provider_service import AgentProviderService
                from src.services.quality_scoring_service import QualityScoringService

                provider_service = AgentProviderService(db, config)
                await provider_service.load_db_providers()
                if pipeline_needs_llm(pipeline) and not provider_service.has_providers():
                    print(
                        "LLM provider is not configured. Add one in /settings or set an API key "
                        "env var (e.g. OPENAI_API_KEY). Non-LLM pipelines run without a provider."
                    )
                    return

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

            elif args.pipeline_action == "generate":
                pipeline = await svc.get(args.id)
                if pipeline is None:
                    print(f"Pipeline id={args.id} not found")
                    return
                engine = SearchEngine(db)
                agent_manager = None
                if getattr(pipeline.generation_backend, "value", pipeline.generation_backend) == "deep_agents":
                    from src.agent.manager import AgentManager

                    agent_manager = AgentManager(db, config)
                from src.services.provider_service import AgentProviderService
                from src.services.quality_scoring_service import QualityScoringService

                provider_svc = AgentProviderService(db, config)
                await provider_svc.load_db_providers()
                if pipeline_needs_llm(pipeline) and not provider_svc.has_providers():
                    print(
                        "LLM provider is not configured. Add one in /settings or set an API key "
                        "env var (e.g. OPENAI_API_KEY). Non-LLM pipelines run without a provider."
                    )
                    return
                _, pool = await runtime.init_pool(config, db)
                client_pool = pool
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
                    if run.generated_text:
                        print("--- DRAFT PREVIEW ---")
                        print(run.generated_text)
                except Exception as exc:
                    print(f"Generation failed: {exc}")

            elif args.pipeline_action == "runs":
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

            elif args.pipeline_action == "run-show":
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

            elif args.pipeline_action == "queue":
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

            elif args.pipeline_action == "approve":
                run = await db.repos.generation_runs.get(args.run_id)
                if run is None:
                    print(f"Run id={args.run_id} not found")
                    return
                await db.repos.generation_runs.set_moderation_status(args.run_id, "approved")
                print(f"Approved run id={args.run_id}")

            elif args.pipeline_action == "reject":
                run = await db.repos.generation_runs.get(args.run_id)
                if run is None:
                    print(f"Run id={args.run_id} not found")
                    return
                await db.repos.generation_runs.set_moderation_status(args.run_id, "rejected")
                print(f"Rejected run id={args.run_id}")

            elif args.pipeline_action == "bulk-approve":
                approved = 0
                for run_id in args.run_ids:
                    run = await db.repos.generation_runs.get(run_id)
                    if run is None:
                        print(f"  Run id={run_id} not found, skipping.")
                        continue
                    await db.repos.generation_runs.set_moderation_status(run_id, "approved")
                    approved += 1
                print(f"Bulk approved: {approved}/{len(args.run_ids)}")

            elif args.pipeline_action == "bulk-reject":
                rejected = 0
                for run_id in args.run_ids:
                    run = await db.repos.generation_runs.get(run_id)
                    if run is None:
                        print(f"  Run id={run_id} not found, skipping.")
                        continue
                    await db.repos.generation_runs.set_moderation_status(run_id, "rejected")
                    rejected += 1
                print(f"Bulk rejected: {rejected}/{len(args.run_ids)}")

            elif args.pipeline_action == "publish":
                run = await db.repos.generation_runs.get(args.run_id)
                if run is None:
                    print(f"Run id={args.run_id} not found")
                    return
                if run.pipeline_id is None:
                    print(f"Run id={args.run_id} has no pipeline")
                    return

                pipeline = await svc.get(run.pipeline_id)
                if pipeline is None:
                    print(f"Pipeline id={run.pipeline_id} not found")
                    return

                _, pool = await runtime.init_pool(config, db)
                if not pool.clients:
                    print("ERROR: Нет доступных аккаунтов Telegram.")
                    return

                publish_service = PublishService(db, pool)
                results = await publish_service.publish_run(run, pipeline)
                if not results or not all(result.success for result in results):
                    print(f"Failed to publish run id={args.run_id}")
                    for result in results:
                        if not result.success:
                            print(f"  - {result.error or 'Unknown publish error'}")
                    return

                print(
                    f"Published run id={args.run_id} to {len(results)} target(s)"
                )
            elif args.pipeline_action == "refinement-steps":
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

            elif args.pipeline_action == "export":
                import json

                data = await svc.export_json(args.id)
                if data is None:
                    print(f"Pipeline id={args.id} not found")
                    return
                output = json.dumps(data, ensure_ascii=False, indent=2)
                if args.output:
                    with open(args.output, "w", encoding="utf-8") as fh:
                        fh.write(output)
                    print(f"Exported pipeline id={args.id} to {args.output}")
                else:
                    print(output)

            elif args.pipeline_action == "import":
                import json

                with open(args.file, encoding="utf-8") as fh:
                    data = json.load(fh)
                try:
                    pipeline_id = await svc.import_json(data, name_override=getattr(args, "name", None))
                    print(f"Imported pipeline (id={pipeline_id})")
                except PipelineValidationError as exc:
                    print(f"Validation error: {exc}")

            elif args.pipeline_action == "templates":
                import json

                templates = await svc.list_templates(category=getattr(args, "category", None))
                if not templates:
                    print("No templates found.")
                    return
                print(f"{'ID':<5} {'Category':<14} {'Name':<32} Description")
                print("-" * 80)
                for tpl in templates:
                    builtin = " [builtin]" if tpl.is_builtin else ""
                    print(f"{tpl.id or '—':<5} {tpl.category:<14} {tpl.name:<32} {tpl.description[:40]}{builtin}")

            elif args.pipeline_action == "from-template":
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

            elif args.pipeline_action == "dry-run-count":
                since_h = to_since_hours(args.since_value, args.since_unit)
                msgs = await db.repos.messages.get_recent_for_channels(args.source, since_h)
                print(
                    f"Messages found: {len(msgs)} "
                    f"(sources={args.source}, since={args.since_value}{args.since_unit})"
                )

            elif args.pipeline_action == "ai-edit":
                instruction = args.instruction
                result = await svc.edit_via_llm(args.id, instruction, db, config=config)
                import json

                if result["ok"]:
                    print("Pipeline JSON updated successfully.")
                    if getattr(args, "show", False):
                        print(json.dumps(result["pipeline_json"], ensure_ascii=False, indent=2))
                else:
                    print(f"Error: {result['error']}")

            elif args.pipeline_action == "filter":
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

            elif args.pipeline_action == "node":
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

            elif args.pipeline_action == "edge":
                if args.edge_action == "add":
                    ok = await svc.add_edge(args.pipeline_id, args.from_node, args.to_node)
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

            elif args.pipeline_action == "graph":
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

        finally:
            if pool is not None:
                await pool.disconnect_all()
            await db.close()

    asyncio.run(_run())
