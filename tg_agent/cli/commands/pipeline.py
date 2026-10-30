from __future__ import annotations

import argparse
import asyncio
from dataclasses import replace
from functools import partial

import typer

from tg_agent.cli import runtime
from tg_agent.cli.commands.common import (
    GenerationBackend,
    PublishMode,
    SinceUnit,
    TriBool,
    apply_startup,
    run_async,
)
from tg_agent.cli.pipeline import crud, generation, graph, moderation, requests
from tg_agent.cli.pipeline.context import PipelineServices
from tg_agent.cli.pipeline.crud import _apply_pipeline_add_edges as _apply_pipeline_add_edges
from tg_agent.cli.pipeline.crud import _apply_pipeline_add_node_configs as _apply_pipeline_add_node_configs
from tg_agent.cli.pipeline.crud import _build_pipeline_add_graph as _build_pipeline_add_graph
from tg_agent.cli.pipeline.crud import _build_pipeline_add_node_specs as _build_pipeline_add_node_specs
from tg_agent.cli.pipeline.crud import _parse_target_refs as _parse_target_refs
from tg_agent.cli.pipeline.crud import _pipeline_add as _pipeline_add
from tg_agent.cli.pipeline.crud import _pipeline_add_from_json as _pipeline_add_from_json
from tg_agent.cli.pipeline.crud import _pipeline_add_from_nodes as _pipeline_add_from_nodes
from tg_agent.cli.pipeline.crud import _pipeline_add_legacy as _pipeline_add_legacy
from tg_agent.cli.pipeline.crud import _pipeline_add_pipeline as _pipeline_add_pipeline
from tg_agent.cli.pipeline.crud import _pipeline_ai_edit as _pipeline_ai_edit
from tg_agent.cli.pipeline.crud import _pipeline_delete as _pipeline_delete
from tg_agent.cli.pipeline.crud import _pipeline_edit as _pipeline_edit
from tg_agent.cli.pipeline.crud import _pipeline_enqueue_run_after as _pipeline_enqueue_run_after
from tg_agent.cli.pipeline.crud import _pipeline_export as _pipeline_export
from tg_agent.cli.pipeline.crud import _pipeline_from_template as _pipeline_from_template
from tg_agent.cli.pipeline.crud import _pipeline_import as _pipeline_import
from tg_agent.cli.pipeline.crud import _pipeline_list as _pipeline_list
from tg_agent.cli.pipeline.crud import _pipeline_refinement_steps as _pipeline_refinement_steps
from tg_agent.cli.pipeline.crud import _pipeline_show as _pipeline_show
from tg_agent.cli.pipeline.crud import _pipeline_templates as _pipeline_templates
from tg_agent.cli.pipeline.crud import _pipeline_toggle as _pipeline_toggle
from tg_agent.cli.pipeline.generation import _generation_pool as _generation_pool
from tg_agent.cli.pipeline.generation import _pipeline_auto_select as _pipeline_auto_select
from tg_agent.cli.pipeline.generation import _pipeline_dry_run_count as _pipeline_dry_run_count
from tg_agent.cli.pipeline.generation import _pipeline_generate as _pipeline_generate
from tg_agent.cli.pipeline.generation import _pipeline_generate_stream as _pipeline_generate_stream
from tg_agent.cli.pipeline.generation import _pipeline_run as _pipeline_run
from tg_agent.cli.pipeline.generation import _pipeline_run_show as _pipeline_run_show
from tg_agent.cli.pipeline.generation import _pipeline_runs as _pipeline_runs
from tg_agent.cli.pipeline.generation import _pipeline_select_variant as _pipeline_select_variant
from tg_agent.cli.pipeline.generation import _pipeline_variants as _pipeline_variants
from tg_agent.cli.pipeline.graph import _build_message_filter_config as _build_message_filter_config
from tg_agent.cli.pipeline.graph import _clear_filter_node as _clear_filter_node
from tg_agent.cli.pipeline.graph import _find_filter_node as _find_filter_node
from tg_agent.cli.pipeline.graph import _pipeline_edge as _pipeline_edge
from tg_agent.cli.pipeline.graph import _pipeline_filter as _pipeline_filter
from tg_agent.cli.pipeline.graph import _pipeline_graph as _pipeline_graph
from tg_agent.cli.pipeline.graph import _pipeline_node as _pipeline_node
from tg_agent.cli.pipeline.graph import _safe_add_edge as _safe_add_edge
from tg_agent.cli.pipeline.graph import _str_to_bool as _str_to_bool
from tg_agent.cli.pipeline.graph import _upsert_filter_node as _upsert_filter_node
from tg_agent.cli.pipeline.moderation import _bulk_moderate as _bulk_moderate
from tg_agent.cli.pipeline.moderation import _pipeline_approve as _pipeline_approve
from tg_agent.cli.pipeline.moderation import _pipeline_bulk_approve as _pipeline_bulk_approve
from tg_agent.cli.pipeline.moderation import _pipeline_bulk_reject as _pipeline_bulk_reject
from tg_agent.cli.pipeline.moderation import _pipeline_moderation_list as _pipeline_moderation_list
from tg_agent.cli.pipeline.moderation import _pipeline_moderation_view as _pipeline_moderation_view
from tg_agent.cli.pipeline.moderation import _pipeline_publish as _pipeline_publish
from tg_agent.cli.pipeline.moderation import _pipeline_queue as _pipeline_queue
from tg_agent.cli.pipeline.moderation import _pipeline_reject as _pipeline_reject
from tg_agent.cli.pipeline.moderation import _pipeline_resolve_unconfirmed as _pipeline_resolve_unconfirmed
from tg_agent.cli.pipeline.responses import _format_filter_config as _format_filter_config
from tg_agent.cli.pipeline.responses import _preview_text as _preview_text
from tg_agent.search.engine import SearchEngine
from tg_agent.services.content_generation_service import ContentGenerationService
from tg_agent.services.pipeline_service import (
    PipelineService,
)
from tg_agent.services.publish_service import PublishService

_PIPELINE_HANDLERS = {**crud.HANDLERS, **generation.HANDLERS, **moderation.HANDLERS, **graph.HANDLERS}
_SERVICE_ACTIONS = frozenset(["generate", "generate-stream", "publish", "run"])


async def _dispatch(args: requests.PipelineCommand) -> None:
    """Own one DB/pool lifecycle for the typed command execution."""
    config, db = await runtime.init_db(args.config)
    pool = None
    try:
        svc = PipelineService(db)
        handler = _PIPELINE_HANDLERS.get(args.pipeline_action)
        if handler is not None:
            if args.pipeline_action in _SERVICE_ACTIONS:
                services = PipelineServices(ContentGenerationService, PublishService, SearchEngine)
                handler = partial(handler, services=services)
            pool = await handler(args, db, config, svc) or pool
    finally:
        if pool is not None:
            await pool.disconnect_all()
        await db.close()


def run(args: argparse.Namespace) -> None:
    """Compatibility entrypoint; adapt Namespace once at the CLI boundary."""
    asyncio.run(_dispatch(requests.request_from_namespace(args)))


pipeline_app = typer.Typer(no_args_is_help=True, help="Content pipelines")

# Three nested depth-2 groups mounted via add_typer; the frozen
# ``pipeline filter|node|edge <action>`` paths are the fragile Wave-4 invariant.
pipeline_filter_app = typer.Typer(no_args_is_help=True, help="Manage a pipeline's message filter")
pipeline_app.add_typer(pipeline_filter_app, name="filter")
pipeline_node_app = typer.Typer(no_args_is_help=True, help="Manage pipeline graph nodes")
pipeline_app.add_typer(pipeline_node_app, name="node")
pipeline_edge_app = typer.Typer(no_args_is_help=True, help="Manage pipeline graph edges")
pipeline_app.add_typer(pipeline_edge_app, name="edge")


def _run_pipeline(ctx: typer.Context, request: requests.PipelineCommand) -> None:
    """Apply CLI startup policy and execute a typed pipeline request."""
    apply_startup(ctx)
    run_async(_dispatch(replace(request, config=ctx.obj.config)))


@pipeline_app.command("list")
def pipeline_list(ctx: typer.Context) -> None:
    """List pipelines."""
    _run_pipeline(ctx, requests.PipelineListCommand())


@pipeline_app.command("show")
def pipeline_show(ctx: typer.Context, id: int = typer.Argument(..., help="Pipeline id")) -> None:
    """Show pipeline details."""
    _run_pipeline(ctx, requests.PipelineShowCommand(id=id))


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
        ctx,
        requests.PipelineAddCommand(
            name=name,
            prompt_template=prompt_template,
            json_file=json_file,
            source=list(source) or None,
            target=list(target) or None,
            llm_model=llm_model,
            image_model=image_model,
            publish_mode=publish_mode.value,
            generation_backend=generation_backend.value,
            interval=interval,
            inactive=inactive,
            ab_variants=ab_variants,
            ab_auto_select=ab_auto_select,
            node_specs=list(node_specs) or None,
            edge=list(edge) or None,
            node_configs=list(node_configs) or None,
            run_after=run_after,
            since_value=since_value,
            since_unit=since_unit.value,
        ),
    )


@pipeline_app.command("dry-run-count")
def pipeline_dry_run_count(
    ctx: typer.Context,
    source: list[int] = typer.Option(..., "--source", help="Source channel id (repeat for multiple)"),
    since_value: int = typer.Option(24, "--since-value"),
    since_unit: SinceUnit = typer.Option(SinceUnit.h, "--since-unit"),
) -> None:
    """Count messages for given sources."""
    _run_pipeline(
        ctx,
        requests.PipelineDryRunCountCommand(source=list(source), since_value=since_value, since_unit=since_unit.value),
    )


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
        ctx,
        requests.PipelineEditCommand(
            id=id,
            name=name,
            prompt_template=prompt_template,
            source=list(source) or None,
            target=list(target) or None,
            llm_model=llm_model,
            image_model=image_model,
            publish_mode=publish_mode.value if publish_mode else None,
            generation_backend=generation_backend.value if generation_backend else None,
            interval=interval,
            active=active,
            ab_variants=ab_variants,
            ab_auto_select=ab_auto_select,
        ),
    )


@pipeline_app.command("delete")
def pipeline_delete(ctx: typer.Context, id: int = typer.Argument(..., help="Pipeline id")) -> None:
    """Delete a pipeline."""
    _run_pipeline(ctx, requests.PipelineDeleteCommand(id=id))


@pipeline_app.command("toggle")
def pipeline_toggle(ctx: typer.Context, id: int = typer.Argument(..., help="Pipeline id")) -> None:
    """Toggle pipeline active state."""
    _run_pipeline(ctx, requests.PipelineToggleCommand(id=id))


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
        ctx,
        requests.PipelineRunCommand(
            id=id, preview=preview, publish=publish, limit=limit, max_tokens=max_tokens, temperature=temperature
        ),
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
        ctx,
        requests.PipelineGenerateCommand(
            id=id,
            max_tokens=max_tokens,
            temperature=temperature,
            model=model,
            preview=preview,
            ab_variants=ab_variants,
            auto_select=auto_select,
        ),
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
        ctx,
        requests.PipelineGenerateStreamCommand(
            id=id, model=model, max_tokens=max_tokens, temperature=temperature, limit=limit
        ),
    )


@pipeline_app.command("runs")
def pipeline_runs(
    ctx: typer.Context,
    id: int = typer.Argument(..., help="Pipeline id"),
    limit: int = typer.Option(20, "--limit"),
    status: str | None = typer.Option(None, "--status"),
) -> None:
    """List generation runs."""
    _run_pipeline(ctx, requests.PipelineRunsCommand(id=id, limit=limit, status=status))


@pipeline_app.command("run-show")
def pipeline_run_show(ctx: typer.Context, run_id: int = typer.Argument(..., help="Run id")) -> None:
    """Show generation run details."""
    _run_pipeline(ctx, requests.PipelineRunShowCommand(run_id=run_id))


@pipeline_app.command("variants")
def pipeline_variants(ctx: typer.Context, run_id: int = typer.Argument(..., help="Run id")) -> None:
    """List A/B variants."""
    _run_pipeline(ctx, requests.PipelineVariantsCommand(run_id=run_id))


@pipeline_app.command("select-variant")
def pipeline_select_variant(
    ctx: typer.Context,
    run_id: int = typer.Argument(..., help="Run id"),
    index: int = typer.Argument(..., help="Variant index"),
) -> None:
    """Select an A/B variant."""
    _run_pipeline(ctx, requests.PipelineSelectVariantCommand(run_id=run_id, index=index))


@pipeline_app.command("auto-select")
def pipeline_auto_select(ctx: typer.Context, run_id: int = typer.Argument(..., help="Run id")) -> None:
    """Auto-select the best A/B variant."""
    _run_pipeline(ctx, requests.PipelineAutoSelectCommand(run_id=run_id))


@pipeline_app.command("queue")
def pipeline_queue(
    ctx: typer.Context,
    id: int = typer.Argument(..., help="Pipeline id"),
    limit: int = typer.Option(20, "--limit"),
) -> None:
    """Show pending moderation queue for a pipeline."""
    _run_pipeline(ctx, requests.PipelineQueueCommand(id=id, limit=limit))


@pipeline_app.command("moderation-list")
def pipeline_moderation_list(
    ctx: typer.Context,
    pipeline_id: int | None = typer.Option(None, "--pipeline-id"),
    limit: int = typer.Option(20, "--limit"),
) -> None:
    """List pending moderation runs."""
    _run_pipeline(ctx, requests.PipelineModerationListCommand(pipeline_id=pipeline_id, limit=limit))


@pipeline_app.command("moderation-view")
def pipeline_moderation_view(ctx: typer.Context, run_id: int = typer.Argument(..., help="Run id")) -> None:
    """Show a moderation run's details."""
    _run_pipeline(ctx, requests.PipelineModerationViewCommand(run_id=run_id))


@pipeline_app.command("publish")
def pipeline_publish(ctx: typer.Context, run_id: int = typer.Argument(..., help="Run id")) -> None:
    """Publish a generation run."""
    _run_pipeline(ctx, requests.PipelinePublishCommand(run_id=run_id))


@pipeline_app.command("approve")
def pipeline_approve(ctx: typer.Context, run_id: int = typer.Argument(..., help="Run id")) -> None:
    """Approve a generation run."""
    _run_pipeline(ctx, requests.PipelineApproveCommand(run_id=run_id))


@pipeline_app.command("reject")
def pipeline_reject(ctx: typer.Context, run_id: int = typer.Argument(..., help="Run id")) -> None:
    """Reject a generation run."""
    _run_pipeline(ctx, requests.PipelineRejectCommand(run_id=run_id))


@pipeline_app.command("resolve-unconfirmed")
def pipeline_resolve_unconfirmed(
    ctx: typer.Context,
    run_id: int = typer.Argument(..., help="Run id"),
    target: str = typer.Argument(..., help="Target key PHONE:DIALOG_ID"),
    action: str = typer.Option("delivered", "--action", help="Resolution: delivered or failed"),
) -> None:
    """Resolve a manually checked unconfirmed delivery target."""
    if action not in {"delivered", "failed"}:
        raise typer.BadParameter("must be delivered or failed", param_hint="--action")
    _run_pipeline(ctx, requests.PipelineResolveUnconfirmedCommand(run_id=run_id, target=target, action=action))


@pipeline_app.command("bulk-approve")
def pipeline_bulk_approve(
    ctx: typer.Context,
    run_ids: list[int] = typer.Argument(..., help="Run ids"),
) -> None:
    """Approve multiple generation runs."""
    _run_pipeline(ctx, requests.PipelineBulkApproveCommand(run_ids=list(run_ids)))


@pipeline_app.command("bulk-reject")
def pipeline_bulk_reject(
    ctx: typer.Context,
    run_ids: list[int] = typer.Argument(..., help="Run ids"),
) -> None:
    """Reject multiple generation runs."""
    _run_pipeline(ctx, requests.PipelineBulkRejectCommand(run_ids=list(run_ids)))


@pipeline_app.command("refinement-steps")
def pipeline_refinement_steps(
    ctx: typer.Context,
    id: int = typer.Argument(..., help="Pipeline id"),
    steps_json: str | None = typer.Option(None, "--set", help="Set refinement steps (JSON array)"),
) -> None:
    """View or set refinement steps."""
    _run_pipeline(ctx, requests.PipelineRefinementStepsCommand(id=id, steps_json=steps_json))


@pipeline_app.command("export")
def pipeline_export(
    ctx: typer.Context,
    id: int = typer.Argument(..., help="Pipeline id"),
    output: str | None = typer.Option(None, "--output", "-o"),
    force: bool = typer.Option(False, "--force", "-f"),
) -> None:
    """Export a pipeline as JSON."""
    _run_pipeline(ctx, requests.PipelineExportCommand(id=id, output=output, force=force))


@pipeline_app.command("import")
def pipeline_import(
    ctx: typer.Context,
    file: str = typer.Argument(..., help="Path to JSON file"),
    name: str | None = typer.Option(None, "--name"),
) -> None:
    """Import a pipeline from a JSON file."""
    _run_pipeline(ctx, requests.PipelineImportCommand(file=file, name=name))


@pipeline_app.command("templates")
def pipeline_templates(
    ctx: typer.Context,
    category: str | None = typer.Option(None, "--category"),
) -> None:
    """List available pipeline templates."""
    _run_pipeline(ctx, requests.PipelineTemplatesCommand(category=category))


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
        ctx,
        requests.PipelineFromTemplateCommand(
            template_id=template_id, name=name, source_ids=source_ids, target_refs=target_refs
        ),
    )


@pipeline_app.command("ai-edit")
def pipeline_ai_edit(
    ctx: typer.Context,
    id: int = typer.Argument(..., help="Pipeline id"),
    instruction: str = typer.Argument(..., help="Instruction for the LLM"),
    show: bool = typer.Option(False, "--show"),
) -> None:
    """Edit a pipeline's JSON via an LLM instruction."""
    _run_pipeline(ctx, requests.PipelineAiEditCommand(id=id, instruction=instruction, show=show))


@pipeline_app.command("graph")
def pipeline_graph(ctx: typer.Context, id: int = typer.Argument(..., help="Pipeline id")) -> None:
    """Show a pipeline's graph (ASCII)."""
    _run_pipeline(ctx, requests.PipelineGraphCommand(id=id))


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
        ctx,
        requests.PipelineFilterSetCommand(
            id=id,
            message_kinds=list(message_kinds) or None,
            service_actions=list(service_actions) or None,
            media_types=list(media_types) or None,
            sender_kinds=list(sender_kinds) or None,
            keywords=list(keywords) or None,
            regex=regex,
            forwarded=forwarded.value if forwarded else None,
            has_text=has_text.value if has_text else None,
        ),
    )


@pipeline_filter_app.command("show")
def pipeline_filter_show(ctx: typer.Context, id: int = typer.Argument(..., help="Pipeline id")) -> None:
    """Show a pipeline's message filter."""
    _run_pipeline(ctx, requests.PipelineFilterShowCommand(id=id))


@pipeline_filter_app.command("clear")
def pipeline_filter_clear(ctx: typer.Context, id: int = typer.Argument(..., help="Pipeline id")) -> None:
    """Clear a pipeline's message filter."""
    _run_pipeline(ctx, requests.PipelineFilterClearCommand(id=id))


# ---- nested: pipeline node <action> --------------------------------------- #


@pipeline_node_app.command("add")
def pipeline_node_add(
    ctx: typer.Context,
    pipeline_id: int = typer.Argument(..., help="Pipeline id"),
    node_spec: str = typer.Argument(..., help="Node spec: type:key=value,..."),
) -> None:
    """Add a node to a pipeline graph."""
    _run_pipeline(ctx, requests.PipelineNodeAddCommand(pipeline_id=pipeline_id, node_spec=node_spec))


@pipeline_node_app.command("replace")
def pipeline_node_replace(
    ctx: typer.Context,
    pipeline_id: int = typer.Argument(..., help="Pipeline id"),
    node_id: str = typer.Argument(..., help="Node ID to replace"),
    node_spec: str = typer.Argument(..., help="New node spec: type:key=value,..."),
) -> None:
    """Replace a node in a pipeline graph."""
    _run_pipeline(
        ctx, requests.PipelineNodeReplaceCommand(pipeline_id=pipeline_id, node_id=node_id, node_spec=node_spec)
    )


@pipeline_node_app.command("remove")
def pipeline_node_remove(
    ctx: typer.Context,
    pipeline_id: int = typer.Argument(..., help="Pipeline id"),
    node_id: str = typer.Argument(..., help="Node ID to remove"),
) -> None:
    """Remove a node from a pipeline graph."""
    _run_pipeline(ctx, requests.PipelineNodeRemoveCommand(pipeline_id=pipeline_id, node_id=node_id))


# ---- nested: pipeline edge <action> --------------------------------------- #


@pipeline_edge_app.command("add")
def pipeline_edge_add(
    ctx: typer.Context,
    pipeline_id: int = typer.Argument(..., help="Pipeline id"),
    from_node: str = typer.Argument(..., help="Source node ID"),
    to_node: str = typer.Argument(..., help="Target node ID"),
) -> None:
    """Add an edge to a pipeline graph."""
    _run_pipeline(ctx, requests.PipelineEdgeAddCommand(pipeline_id=pipeline_id, from_node=from_node, to_node=to_node))


@pipeline_edge_app.command("remove")
def pipeline_edge_remove(
    ctx: typer.Context,
    pipeline_id: int = typer.Argument(..., help="Pipeline id"),
    from_node: str = typer.Argument(..., help="Source node ID"),
    to_node: str = typer.Argument(..., help="Target node ID"),
) -> None:
    """Remove an edge from a pipeline graph."""
    _run_pipeline(
        ctx, requests.PipelineEdgeRemoveCommand(pipeline_id=pipeline_id, from_node=from_node, to_node=to_node)
    )
