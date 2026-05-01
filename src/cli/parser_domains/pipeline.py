from __future__ import annotations

import argparse


def register(subparsers: argparse._SubParsersAction) -> argparse.ArgumentParser | None:
    pipeline_parser = subparsers.add_parser("pipeline", help="Content pipeline management")
    pipeline_sub = pipeline_parser.add_subparsers(dest="pipeline_action")
    pipeline_sub.add_parser("list", help="List pipelines")

    pipeline_show = pipeline_sub.add_parser("show", help="Show pipeline details")
    pipeline_show.add_argument("id", type=int, help="Pipeline id")

    pipeline_add = pipeline_sub.add_parser("add", help="Add pipeline")
    pipeline_add.add_argument("name", help="Pipeline name")
    pipeline_add.add_argument(
        "--prompt-template",
        default=None,
        help="Prompt template (required unless --json-file/--node is used)",
    )
    pipeline_add.add_argument(
        "--json-file",
        default=None,
        dest="json_file",
        help="Path to pipeline DAG graph JSON file (enables DAG mode; --prompt-template optional)",
    )
    pipeline_add.add_argument(
        "--source",
        type=int,
        action="append",
        default=None,
        help="Source channel_id; repeat for multiple channels",
    )
    pipeline_add.add_argument(
        "--target",
        action="append",
        default=None,
        help="Target in PHONE|DIALOG_ID format; repeat for multiple targets",
    )
    pipeline_add.add_argument("--llm-model", default=None, help="Optional LLM model")
    pipeline_add.add_argument("--image-model", default=None, help="Optional image model")
    pipeline_add.add_argument(
        "--publish-mode",
        choices=["auto", "moderated"],
        default="moderated",
        help="Publish mode",
    )
    pipeline_add.add_argument(
        "--generation-backend",
        choices=["chain", "agent", "deep_agents"],
        default="chain",
        help="Generation backend",
    )
    pipeline_add.add_argument(
        "--interval",
        type=int,
        default=60,
        help="Generate interval in minutes",
    )
    pipeline_add.add_argument("--inactive", action="store_true", help="Create pipeline disabled")
    pipeline_add.add_argument(
        "--node",
        action="append",
        default=None,
        dest="node_specs",
        help="Node spec in type:key=value format; repeat for multiple nodes",
    )
    pipeline_add.add_argument(
        "--edge",
        action="append",
        default=None,
        help="Explicit edge FROM_ID->TO_ID; repeat for multiple edges",
    )
    pipeline_add.add_argument(
        "--node-config",
        action="append",
        default=None,
        dest="node_configs",
        help="JSON config override for a node: NODE_ID='{\"key\":\"value\"}'",
    )
    pipeline_add.add_argument(
        "--run-after",
        action="store_true",
        dest="run_after",
        help="Enqueue a pipeline run immediately after creation",
    )
    pipeline_add.add_argument(
        "--since-value",
        type=int,
        default=24,
        dest="since_value",
        help="Lookback depth for --run-after (default 24)",
    )
    pipeline_add.add_argument(
        "--since-unit",
        choices=["m", "h", "d"],
        default="h",
        dest="since_unit",
        help="Lookback unit for --run-after: m/h/d (default h)",
    )

    pipeline_drc = pipeline_sub.add_parser(
        "dry-run-count",
        help="Count messages available for given source channels",
    )
    pipeline_drc.add_argument(
        "--source",
        type=int,
        action="append",
        required=True,
        help="Source channel_id; repeat for multiple channels",
    )
    pipeline_drc.add_argument("--since-value", type=int, default=24, dest="since_value")
    pipeline_drc.add_argument(
        "--since-unit", choices=["m", "h", "d"], default="h", dest="since_unit"
    )

    pipeline_edit = pipeline_sub.add_parser("edit", help="Edit pipeline")
    pipeline_edit.add_argument("id", type=int, help="Pipeline id")
    pipeline_edit.add_argument("--name", default=None, help="New pipeline name")
    pipeline_edit.add_argument("--prompt-template", default=None, help="New prompt template")
    pipeline_edit.add_argument(
        "--source",
        type=int,
        action="append",
        default=None,
        help="Replace sources with these channel_id values",
    )
    pipeline_edit.add_argument(
        "--target",
        action="append",
        default=None,
        help="Replace targets with PHONE|DIALOG_ID values",
    )
    pipeline_edit.add_argument("--llm-model", default=None, help="Optional LLM model")
    pipeline_edit.add_argument("--image-model", default=None, help="Optional image model")
    pipeline_edit.add_argument("--publish-mode", choices=["auto", "moderated"], default=None)
    pipeline_edit.add_argument("--generation-backend", choices=["chain", "agent", "deep_agents"], default=None)
    pipeline_edit.add_argument(
        "--interval",
        type=int,
        default=None,
        help="Generate interval in minutes",
    )
    pipeline_edit.add_argument(
        "--active",
        dest="active",
        action="store_const",
        const=True,
        default=None,
        help="Enable pipeline",
    )
    pipeline_edit.add_argument(
        "--inactive",
        dest="active",
        action="store_const",
        const=False,
        help="Disable pipeline",
    )

    pipeline_delete = pipeline_sub.add_parser("delete", help="Delete pipeline")
    pipeline_delete.add_argument("id", type=int, help="Pipeline id")

    pipeline_toggle = pipeline_sub.add_parser("toggle", help="Toggle pipeline active state")
    pipeline_toggle.add_argument("id", type=int, help="Pipeline id")

    pipeline_run = pipeline_sub.add_parser("run", help="Run pipeline generation (preview/publish)")
    pipeline_run.add_argument("id", type=int, help="Pipeline id")
    pipeline_run.add_argument(
        "--preview", action="store_true", default=False, help="Only preview generated draft"
    )
    pipeline_run.add_argument(
        "--publish",
        action="store_true",
        default=False,
        help="Publish generated draft to targets (requires accounts and confirmation)",
    )
    pipeline_run.add_argument(
        "--limit", type=int, default=8, help="Number of context messages to fetch"
    )
    pipeline_run.add_argument(
        "--max-tokens", type=int, default=256, help="Max tokens for LLM generation"
    )
    pipeline_run.add_argument(
        "--temperature", type=float, default=0.0, help="Sampling temperature for generation"
    )

    pipeline_generate = pipeline_sub.add_parser(
        "generate", help="Generate content for a pipeline (uses ContentGenerationService)"
    )
    pipeline_generate.add_argument("id", type=int, help="Pipeline id")
    pipeline_generate.add_argument(
        "--max-tokens", type=int, default=512, help="Max tokens for LLM generation"
    )
    pipeline_generate.add_argument(
        "--temperature", type=float, default=0.7, help="Sampling temperature for generation"
    )
    pipeline_generate.add_argument(
        "--model", default=None, help="Override LLM model"
    )
    pipeline_generate.add_argument(
        "--preview", action="store_true", default=False, help="Print generated text to stdout"
    )

    pipeline_runs = pipeline_sub.add_parser("runs", help="List generation runs for a pipeline")
    pipeline_runs.add_argument("id", type=int, help="Pipeline id")
    pipeline_runs.add_argument("--limit", type=int, default=20, help="Max runs to show")
    pipeline_runs.add_argument("--status", default=None, help="Filter by status")

    pipeline_run_show = pipeline_sub.add_parser("run-show", help="Show generation run details")
    pipeline_run_show.add_argument("run_id", type=int, help="Run id")

    pipeline_queue = pipeline_sub.add_parser("queue", help="Show moderation queue")
    pipeline_queue.add_argument("id", type=int, help="Pipeline id")
    pipeline_queue.add_argument("--limit", type=int, default=20, help="Max runs to show")

    moderation_list = pipeline_sub.add_parser("moderation-list", help="Show pending moderation runs")
    moderation_list.add_argument("--pipeline-id", type=int, default=None, help="Filter by pipeline id")
    moderation_list.add_argument("--limit", type=int, default=20, help="Max runs to show")

    moderation_view = pipeline_sub.add_parser("moderation-view", help="Show moderation run details")
    moderation_view.add_argument("run_id", type=int, help="Run id")

    pipeline_publish = pipeline_sub.add_parser("publish", help="Publish a generation run")
    pipeline_publish.add_argument("run_id", type=int, help="Run id to publish")

    pipeline_approve = pipeline_sub.add_parser("approve", help="Approve a generation run")
    pipeline_approve.add_argument("run_id", type=int, help="Run id to approve")

    pipeline_reject = pipeline_sub.add_parser("reject", help="Reject a generation run")
    pipeline_reject.add_argument("run_id", type=int, help="Run id to reject")

    pipeline_bulk_approve = pipeline_sub.add_parser("bulk-approve", help="Approve multiple runs")
    pipeline_bulk_approve.add_argument("run_ids", nargs="+", type=int, help="Run IDs to approve")

    pipeline_bulk_reject = pipeline_sub.add_parser("bulk-reject", help="Reject multiple runs")
    pipeline_bulk_reject.add_argument("run_ids", nargs="+", type=int, help="Run IDs to reject")

    pipeline_refine = pipeline_sub.add_parser("refinement-steps", help="View/set refinement steps")
    pipeline_refine.add_argument("id", type=int, help="Pipeline id")
    pipeline_refine.add_argument("--set", default=None, dest="steps_json",
                                 help="Set refinement steps (JSON array)")

    # JSON import/export
    pipeline_export = pipeline_sub.add_parser("export", help="Export pipeline as JSON")
    pipeline_export.add_argument("id", type=int, help="Pipeline id")
    pipeline_export.add_argument("--output", "-o", default=None, help="Output file path (default: stdout)")

    pipeline_import = pipeline_sub.add_parser("import", help="Import pipeline from JSON file")
    pipeline_import.add_argument("file", help="Path to JSON file")
    pipeline_import.add_argument("--name", default=None, help="Override pipeline name")

    # Templates
    pipeline_templates = pipeline_sub.add_parser("templates", help="List available pipeline templates")
    pipeline_templates.add_argument("--category", default=None, help="Filter by category")

    pipeline_from_tpl = pipeline_sub.add_parser("from-template", help="Create pipeline from template")
    pipeline_from_tpl.add_argument("template_id", type=int, help="Template id from 'pipeline templates'")
    pipeline_from_tpl.add_argument("name", help="Pipeline name")
    pipeline_from_tpl.add_argument("--source-ids", default="", dest="source_ids", help="Comma-separated channel IDs")
    pipeline_from_tpl.add_argument(
        "--target-refs", default="", dest="target_refs", help="Comma-separated phone|dialog_id targets"
    )

    # AI edit
    pipeline_ai_edit = pipeline_sub.add_parser("ai-edit", help="Edit pipeline JSON via LLM instruction")
    pipeline_ai_edit.add_argument("id", type=int, help="Pipeline id")
    pipeline_ai_edit.add_argument("instruction", help="Instruction for the LLM (e.g. 'Add an image generation node')")
    pipeline_ai_edit.add_argument("--show", action="store_true", help="Print updated JSON after edit")

    pipeline_filter = pipeline_sub.add_parser("filter", help="Manage semantic filters on pipeline DAGs")
    filter_sub = pipeline_filter.add_subparsers(dest="filter_action")

    filter_set = filter_sub.add_parser("set", help="Create or replace the pipeline filter node")
    filter_set.add_argument("id", type=int, help="Pipeline id")
    filter_set.add_argument("--message-kind", action="append", default=None, dest="message_kinds")
    filter_set.add_argument("--service-action", action="append", default=None, dest="service_actions")
    filter_set.add_argument("--media-type", action="append", default=None, dest="media_types")
    filter_set.add_argument("--sender-kind", action="append", default=None, dest="sender_kinds")
    filter_set.add_argument("--keyword", action="append", default=None, dest="keywords")
    filter_set.add_argument("--regex", default=None, dest="regex")
    filter_set.add_argument("--forwarded", choices=["true", "false"], default=None)
    filter_set.add_argument("--has-text", choices=["true", "false"], default=None, dest="has_text")

    filter_show = filter_sub.add_parser("show", help="Show the resolved filter config")
    filter_show.add_argument("id", type=int, help="Pipeline id")

    filter_clear = filter_sub.add_parser("clear", help="Remove the pipeline filter node")
    filter_clear.add_argument("id", type=int, help="Pipeline id")

    # Node CRUD
    pipeline_node = pipeline_sub.add_parser("node", help="Node CRUD operations on pipeline graph")
    node_sub = pipeline_node.add_subparsers(dest="node_action")

    node_add = node_sub.add_parser("add", help="Add node to pipeline graph")
    node_add.add_argument("pipeline_id", type=int, help="Pipeline id")
    node_add.add_argument("node_spec", help="Node spec: type:key=value,...")

    node_replace = node_sub.add_parser("replace", help="Replace node in pipeline graph")
    node_replace.add_argument("pipeline_id", type=int, help="Pipeline id")
    node_replace.add_argument("node_id", help="Node ID to replace")
    node_replace.add_argument("node_spec", help="New node spec: type:key=value,...")

    node_remove = node_sub.add_parser("remove", help="Remove node from pipeline graph")
    node_remove.add_argument("pipeline_id", type=int, help="Pipeline id")
    node_remove.add_argument("node_id", help="Node ID to remove")

    # Edge CRUD
    pipeline_edge = pipeline_sub.add_parser("edge", help="Edge CRUD operations on pipeline graph")
    edge_sub = pipeline_edge.add_subparsers(dest="edge_action")

    edge_add = edge_sub.add_parser("add", help="Add edge to pipeline graph")
    edge_add.add_argument("pipeline_id", type=int, help="Pipeline id")
    edge_add.add_argument("from_node", help="Source node ID")
    edge_add.add_argument("to_node", help="Target node ID")

    edge_rm = edge_sub.add_parser("remove", help="Remove edge from pipeline graph")
    edge_rm.add_argument("pipeline_id", type=int, help="Pipeline id")
    edge_rm.add_argument("from_node", help="Source node ID")
    edge_rm.add_argument("to_node", help="Target node ID")

    # Graph visualization
    pipeline_graph = pipeline_sub.add_parser("graph", help="Show pipeline graph (ASCII)")
    pipeline_graph.add_argument("id", type=int, help="Pipeline id")
