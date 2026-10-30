"""Formatting and streaming output for pipeline CLI results."""

from __future__ import annotations

from typing import Any

from tg_agent.cli.pipeline import requests
from tg_agent.services.pipeline_filters import normalize_filter_config
from tg_agent.services.pipeline_result import result_kind_label


def _preview_text(value: str | None, limit: int = 60) -> str:
    if not value:
        return "—"
    compact = " ".join(value.split())
    if len(compact) <= limit:
        return compact
    return f"{compact[: limit - 3]}..."


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


def render_pipeline_show(args: requests.PipelineShowCommand, detail: Any) -> None:
    """Render the retrieved result without service or database operations."""
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
        from tg_agent.cli.pipeline.graph import _find_filter_node

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


def render_pipeline_templates(templates: Any) -> None:
    """Render the retrieved result without service or database operations."""
    if not templates:
        print("No templates found.")
        return
    print(f"{'ID':<5} {'Category':<14} {'Name':<32} Description")
    print("-" * 80)
    for tpl in templates:
        builtin = " [builtin]" if tpl.is_builtin else ""
        print(f"{tpl.id or '—':<5} {tpl.category:<14} {tpl.name:<32} {tpl.description[:40]}{builtin}")


def render_pipeline_runs(args: requests.PipelineRunsCommand, runs: Any) -> None:
    """Render the retrieved result without service or database operations."""
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


def render_pipeline_list(items: Any) -> None:
    """Render the retrieved result without service or database operations."""
    if not items:
        print("No pipelines found.")
        return
    print(f"{'ID':<5} {'Name':<24} {'Backend':<8} {'Mode':<10} {'Active':<8} {'Src/Tgt':<10}")
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


def render_pipeline_variants(ab_result: Any, args: requests.PipelineVariantsCommand) -> None:
    """Render the retrieved result without service or database operations."""
    if ab_result is None:
        print(f"Run id={args.run_id} not found")
        return
    print(f"Run id={ab_result.run_id}: {len(ab_result.variants)} variant(s)")
    for variant in ab_result.variants:
        marker = " *selected*" if ab_result.selected_index == variant.index else ""
        print(f"[{variant.index}]{marker} {variant.text[:200]}")


def render_pipeline_moderation_list(runs: Any) -> None:
    """Render the retrieved result without service or database operations."""
    if not runs:
        print("No pending moderation runs.")
        return
    print(f"{'Run ID':<8} {'Pipeline':<8} {'Status':<12} {'Created':<19} Preview")
    print("-" * 90)
    for run in runs:
        created_at = run.created_at.strftime("%Y-%m-%d %H:%M:%S") if run.created_at else "—"
        print(
            f"{run.id or 0:<8} {run.pipeline_id or 0:<8} {run.moderation_status:<12} "
            f"{created_at:<19} {_preview_text(run.generated_text)}"
        )


def render_pipeline_queue(args: requests.PipelineQueueCommand, runs: Any) -> None:
    """Render the retrieved result without service or database operations."""
    if not runs:
        print(f"No pending moderation runs for pipeline id={args.id}")
        return
    print(f"{'Run ID':<8} {'Status':<12} {'Created':<19} Preview")
    print("-" * 80)
    for run in runs:
        created_at = run.created_at.strftime("%Y-%m-%d %H:%M:%S") if run.created_at else "—"
        print(f"{run.id or 0:<8} {run.moderation_status:<12} {created_at:<19} {_preview_text(run.generated_text)}")


def render_pipeline_moderation_view(args: requests.PipelineModerationViewCommand, run: Any) -> None:
    """Render the retrieved result without service or database operations."""
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


def render_pipeline_graph(graph: Any) -> None:
    """Render the retrieved result without service or database operations."""
    from tg_agent.cli.graph_viz import render_ascii

    print(render_ascii(graph))


def render_pipeline_run_show(args: requests.PipelineRunShowCommand, run: Any) -> None:
    """Render the retrieved result without service or database operations."""
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
    unconfirmed_targets = metadata.get("unconfirmed_targets") or []
    if unconfirmed_targets:
        print(f"unconfirmed_targets={','.join(unconfirmed_targets)}")
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


def emit_json_line(payload: dict[str, Any]) -> None:
    """Keep streaming records on stdout, flushing each complete JSON line."""
    from tg_agent.utils.json import safe_json_dumps

    print(safe_json_dumps(payload), flush=True)


def render_pipeline_run_result(args: requests.PipelineRunCommand, run_obj: Any) -> None:
    print(f"Created generation run id={run_obj.id}")
    print(f"Generation completed for run id={run_obj.id}")
    print(f"result_kind={run_obj.result_kind} result_count={run_obj.result_count}")
    if args.preview and run_obj.generated_text:
        print("--- DRAFT PREVIEW ---")
        print(run_obj.generated_text)
    if args.publish:
        print(
            "Publish requested — publishing via targets is not implemented in CLI; "
            "Use the web UI or implement account targets."
        )


def render_pipeline_generate_result(run: Any) -> None:
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
