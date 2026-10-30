"""Pipeline CLI crud operations."""

from __future__ import annotations

import asyncio
import os
from typing import Callable, cast

from tg_agent.cli.pipeline import requests, responses
from tg_agent.services.pipeline_refs import parse_pipeline_target_refs
from tg_agent.services.pipeline_service import (
    PipelineService,
    PipelineTargetRef,
    PipelineValidationError,
    to_since_hours,
)


def _parse_target_refs(values: list[str]) -> list[PipelineTargetRef]:
    return parse_pipeline_target_refs(
        values,
        missing_separator_message="Target must be in PHONE|DIALOG_ID format.",
        invalid_dialog_id_message="Target dialog id must be numeric.",
    )


async def _pipeline_list(args: requests.PipelineListCommand, db, config, svc: PipelineService) -> None:
    items = await svc.get_with_relations()
    return responses.render_pipeline_list(items)


async def _pipeline_show(args: requests.PipelineShowCommand, db, config, svc: PipelineService) -> None:
    detail = await svc.get_detail(args.id)
    return responses.render_pipeline_show(args, detail)


async def _pipeline_add_from_json(
    args: requests.PipelineAddCommand,
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
        # Same flags the --node and legacy paths pass (#1594): import_json
        # defaults to MODERATED/CHAIN/None when the payload omits them.
        "image_model": args.image_model,
        "generation_backend": args.generation_backend,
        "publish_mode": args.publish_mode,
        "source_ids": args.source or [],
        "target_refs": (
            [{"phone": r.phone, "dialog_id": r.dialog_id} for r in _parse_target_refs(args.target)]
            if args.target
            else []
        ),
        "generate_interval_minutes": args.interval,
        # Activate unless --inactive was passed; import_json reads is_active from
        # the payload, so the pipeline is created active in one write (#1246).
        "is_active": not args.inactive,
        "pipeline_json": graph_data,
    }
    return await svc.import_json(data)


def _build_pipeline_add_node_specs(node_specs_raw: list[str]) -> tuple[list, bool]:
    from tg_agent.cli.node_dsl import NodeSpecError, parse_node_spec

    specs = []
    for raw in node_specs_raw:
        try:
            specs.append(parse_node_spec(raw))
        except NodeSpecError as exc:
            print(f"Invalid node spec '{raw}': {exc}")
            return specs, False
    return specs, True


def _apply_pipeline_add_edges(args: requests.PipelineAddCommand, builder) -> bool:
    if getattr(args, "edge", None):
        for edge_str in args.edge:
            from_id, _, to_id = edge_str.partition("->")
            if not to_id:
                print(f"Invalid edge format '{edge_str}'; use FROM->TO")
                return False
            builder.add_explicit_edge(from_id.strip(), to_id.strip())
    return True


def _apply_pipeline_add_node_configs(args: requests.PipelineAddCommand, builder) -> bool:
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


def _build_pipeline_add_graph(args: requests.PipelineAddCommand, node_specs_raw: list[str]):
    from tg_agent.cli.graph_builder import GraphBuilder, GraphBuilderError

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
    args: requests.PipelineAddCommand,
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
            # A/B flags must persist on the DAG path like the legacy add (#1620);
            # import_json reads them from the payload.
            "ab_num_variants": getattr(args, "ab_variants", 1) or 1,
            "ab_auto_select": bool(getattr(args, "ab_auto_select", False)),
            "target_refs": (
                [f"{t.phone}|{t.dialog_id}" for t in _parse_target_refs(args.target)] if args.target else []
            ),
        },
    )

    # Activate if --inactive was not passed
    if not args.inactive and pipeline_id:
        await svc._bundle.set_active(pipeline_id, True)
    return pipeline_id, True


async def _pipeline_add_legacy(
    args: requests.PipelineAddCommand,
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


async def _pipeline_add_pipeline(args: requests.PipelineAddCommand, svc: PipelineService) -> tuple[int | None, bool]:
    json_file = getattr(args, "json_file", None)
    node_specs_raw = getattr(args, "node_specs", None)
    if json_file:
        return await _pipeline_add_from_json(args, svc, json_file), True
    if node_specs_raw:
        # DAG mode: build graph from --node specs
        return await _pipeline_add_from_nodes(args, svc, node_specs_raw)
    return await _pipeline_add_legacy(args, svc)


async def _pipeline_enqueue_run_after(args: requests.PipelineAddCommand, db, pipeline_id: int | None) -> None:
    from tg_agent.services.task_enqueuer import TaskEnqueuer

    since_h = to_since_hours(args.since_value, args.since_unit)
    enqueuer_factory = cast(Callable[[object], TaskEnqueuer], TaskEnqueuer)
    enqueuer = enqueuer_factory(db)
    await enqueuer.enqueue_pipeline_run(cast(int, pipeline_id), since_hours=since_h)
    print(f"Enqueued pipeline run (since={args.since_value}{args.since_unit})")


async def _pipeline_add(args: requests.PipelineAddCommand, db, config, svc: PipelineService) -> None:
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


async def _pipeline_edit(args: requests.PipelineEditCommand, db, config, svc: PipelineService) -> None:
    existing = await svc.get(args.id)
    if existing is None:
        print(f"Pipeline id={args.id} not found")
        return
    current_sources = [source.channel_id for source in await svc.get_sources(args.id)]
    current_targets = [
        PipelineTargetRef(phone=target.phone, dialog_id=target.dialog_id) for target in await svc.get_targets(args.id)
    ]
    try:
        ok = await svc.update(
            args.id,
            name=args.name or existing.name,
            prompt_template=args.prompt_template or existing.prompt_template,
            source_channel_ids=args.source if args.source else current_sources,
            dag_source_channel_ids=args.source if args.source else None,
            dag_target_refs=_parse_target_refs(args.target) if args.target else None,
            target_refs=(_parse_target_refs(args.target) if args.target else current_targets),
            llm_model=(args.llm_model if args.llm_model is not None else existing.llm_model),
            image_model=(args.image_model if args.image_model is not None else existing.image_model),
            publish_mode=args.publish_mode or existing.publish_mode,
            generation_backend=args.generation_backend or existing.generation_backend,
            generate_interval_minutes=(
                args.interval if args.interval is not None else existing.generate_interval_minutes
            ),
            is_active=existing.is_active if args.active is None else args.active,
            ab_num_variants=(
                args.ab_variants if getattr(args, "ab_variants", None) is not None else existing.ab_num_variants
            ),
            ab_auto_select=(
                args.ab_auto_select if getattr(args, "ab_auto_select", None) is not None else existing.ab_auto_select
            ),
        )
    except PipelineValidationError as exc:
        print(f"Error: {exc}")
        return
    if not ok:
        print(f"Pipeline id={args.id} not found")
        return
    print(f"Updated pipeline id={args.id}")


async def _pipeline_toggle(args: requests.PipelineToggleCommand, db, config, svc: PipelineService) -> None:
    ok = await svc.toggle(args.id)
    if not ok:
        print(f"Pipeline id={args.id} not found")
        return
    print(f"Toggled pipeline id={args.id}")


async def _pipeline_delete(args: requests.PipelineDeleteCommand, db, config, svc: PipelineService) -> None:
    await svc.delete(args.id)
    print(f"Deleted pipeline id={args.id}")


async def _pipeline_refinement_steps(
    args: requests.PipelineRefinementStepsCommand, db, config, svc: PipelineService
) -> None:
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


async def _pipeline_export(args: requests.PipelineExportCommand, db, config, svc: PipelineService) -> None:
    import json

    data = await svc.export_json(args.id)
    if data is None:
        print(f"Pipeline id={args.id} not found")
        return
    output = json.dumps(data, ensure_ascii=False, indent=2)
    if args.output:
        if await asyncio.to_thread(os.path.exists, args.output) and not args.force:
            print(f"File {args.output} already exists. Use --force to overwrite.")
            return
        with open(args.output, "w", encoding="utf-8") as fh:
            fh.write(output)
        print(f"Exported pipeline id={args.id} to {args.output}")
    else:
        print(output)


async def _pipeline_import(args: requests.PipelineImportCommand, db, config, svc: PipelineService) -> None:
    import json

    with open(args.file, encoding="utf-8") as fh:
        data = json.load(fh)
    try:
        pipeline_id = await svc.import_json(data, name_override=getattr(args, "name", None))
        print(f"Imported pipeline (id={pipeline_id})")
    except PipelineValidationError as exc:
        print(f"Validation error: {exc}")


async def _pipeline_templates(args: requests.PipelineTemplatesCommand, db, config, svc: PipelineService) -> None:
    templates = await svc.list_templates(category=getattr(args, "category", None))
    return responses.render_pipeline_templates(templates)


async def _pipeline_from_template(args: requests.PipelineFromTemplateCommand, db, config, svc: PipelineService) -> None:
    try:
        template_id = args.template_id
        name = args.name
        source_ids = [int(x.strip()) for x in args.source_ids.split(",") if x.strip()] if args.source_ids else []
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


async def _pipeline_ai_edit(args: requests.PipelineAiEditCommand, db, config, svc: PipelineService) -> None:
    instruction = args.instruction
    result = await svc.edit_via_llm(args.id, instruction, db, config=config)
    import json

    if result["ok"]:
        print("Pipeline JSON updated successfully.")
        if getattr(args, "show", False):
            print(json.dumps(result["pipeline_json"], ensure_ascii=False, indent=2))
    else:
        print(f"Error: {result['error']}")


HANDLERS = {
    "list": _pipeline_list,
    "show": _pipeline_show,
    "add": _pipeline_add,
    "edit": _pipeline_edit,
    "toggle": _pipeline_toggle,
    "delete": _pipeline_delete,
    "refinement-steps": _pipeline_refinement_steps,
    "export": _pipeline_export,
    "import": _pipeline_import,
    "templates": _pipeline_templates,
    "from-template": _pipeline_from_template,
    "ai-edit": _pipeline_ai_edit,
}
