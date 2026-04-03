from __future__ import annotations

import argparse

from src import __version__


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="TG Post Search")
    parser.add_argument("--version", action="version", version=f"%(prog)s {__version__}")
    parser.add_argument("--config", default="config.yaml", help="Path to config file")
    sub = parser.add_subparsers(dest="command")

    serve_parser = sub.add_parser("serve", help="Start web server")
    serve_parser.add_argument("--web-pass", help="Web panel password (overrides config)")

    sub.add_parser("stop", help="Stop web server started by this app")

    restart_parser = sub.add_parser("restart", help="Restart web server")
    restart_parser.add_argument("--web-pass", help="Web panel password (overrides config)")

    collect_parser = sub.add_parser("collect", help="Run one-shot collection")
    collect_parser.add_argument(
        "--channel-id",
        type=int,
        default=None,
        help="Collect single channel by channel_id (full mode)",
    )
    collect_sub = collect_parser.add_subparsers(dest="collect_action")
    collect_sample = collect_sub.add_parser(
        "sample",
        help="Preview last N messages without saving to DB",
    )
    collect_sample.add_argument("channel_id", type=int, help="Channel ID (numeric)")
    collect_sample.add_argument(
        "--limit",
        type=int,
        default=10,
        help="Number of messages to preview (default: 10)",
    )

    search_parser = sub.add_parser("search", help="Search messages")
    search_parser.add_argument("query", nargs="?", default="", help="Search query")
    search_parser.add_argument("--limit", type=int, default=20, help="Max results")
    search_parser.add_argument(
        "--mode",
        choices=["local", "semantic", "hybrid", "telegram", "my_chats", "channel"],
        default="local",
        help="Search mode: local, semantic, hybrid, telegram, my_chats, channel",
    )
    search_parser.add_argument(
        "--channel-id",
        type=int,
        default=None,
        help="Channel ID for --mode=channel",
    )
    search_parser.add_argument("--min-length", type=int, default=None, help="Min message length")
    search_parser.add_argument("--max-length", type=int, default=None, help="Max message length")
    search_parser.add_argument(
        "--fts", action="store_true", default=False, help="Use FTS5 boolean syntax"
    )
    search_parser.add_argument(
        "--index-now",
        action="store_true",
        default=False,
        help="Run semantic embeddings indexing/backfill before exiting",
    )
    search_parser.add_argument(
        "--reset-index",
        action="store_true",
        default=False,
        help="Drop semantic vector index before --index-now",
    )

    # ── messages ──
    msg_parser = sub.add_parser("messages", help="Read messages from DB or live Telegram")
    msg_sub = msg_parser.add_subparsers(dest="messages_action")
    msg_read = msg_sub.add_parser("read", help="Read messages from a channel/dialog")
    msg_read.add_argument("identifier", help="Channel pk, channel_id, @username, or dialog ID")
    msg_read.add_argument("--limit", type=int, default=50, help="Max messages (default: 50)")
    msg_read.add_argument("--live", action="store_true", help="Read from Telegram instead of DB")
    msg_read.add_argument("--phone", default=None, help="Account phone (for --live)")
    msg_read.add_argument("--query", default="", help="Text filter (DB only)")
    msg_read.add_argument("--date-from", dest="date_from", default=None, help="Start date YYYY-MM-DD (DB only)")
    msg_read.add_argument("--date-to", dest="date_to", default=None, help="End date YYYY-MM-DD (DB only)")
    msg_read.add_argument("--topic-id", type=int, default=None, dest="topic_id", help="Forum topic ID")
    msg_read.add_argument("--offset-id", type=int, default=None, dest="offset_id",
                          help="Read messages before this message ID (--live)")
    msg_read.add_argument(
        "--format", choices=["text", "json", "csv"], default="text", dest="output_format",
        help="Output format (default: text)",
    )

    ch_parser = sub.add_parser("channel", help="Channel management")
    ch_sub = ch_parser.add_subparsers(dest="channel_action")

    ch_sub.add_parser("list", help="List channels with message counts")
    ch_add = ch_sub.add_parser("add", help="Add channel by identifier")
    ch_add.add_argument("identifier", help="Username, link, or numeric ID")

    ch_del = ch_sub.add_parser("delete", help="Delete channel")
    ch_del.add_argument("identifier", help="Channel pk, channel_id, or @username")

    ch_toggle = ch_sub.add_parser("toggle", help="Toggle channel active state")
    ch_toggle.add_argument("identifier", help="Channel pk, channel_id, or @username")

    ch_collect = ch_sub.add_parser("collect", help="Collect single channel (full)")
    ch_collect.add_argument("identifier", help="Channel pk, channel_id, or @username")

    ch_stats = ch_sub.add_parser("stats", help="Collect channel statistics")
    ch_stats.add_argument(
        "identifier",
        nargs="?",
        default=None,
        help="Channel pk, channel_id, or @username",
    )
    ch_stats.add_argument(
        "--all",
        action="store_true",
        help="Collect stats for all active channels",
    )

    ch_sub.add_parser("refresh-types", help="Fill missing channel_type for existing channels")

    ch_meta = ch_sub.add_parser("refresh-meta", help="Refresh about/linked_chat_id for channels")
    ch_meta.add_argument(
        "identifier",
        nargs="?",
        default=None,
        help="Channel pk, channel_id, or @username (omit for all)",
    )
    ch_meta.add_argument(
        "--all",
        action="store_true",
        help="Refresh metadata for all active channels",
    )

    ch_import = ch_sub.add_parser("import", help="Bulk import from file or text")
    ch_import.add_argument("source", help="Path to .txt/.csv file, or comma-separated identifiers")

    ch_add_bulk = ch_sub.add_parser("add-bulk", help="Add channels from account dialogs")
    ch_add_bulk.add_argument("--phone", required=True, help="Account phone")
    ch_add_bulk.add_argument(
        "--dialog-ids", required=True, dest="dialog_ids",
        help="Comma-separated dialog IDs to add as channels",
    )

    # ── channel tag ──
    ch_tag_parser = ch_sub.add_parser("tag", help="Manage channel tags")
    ch_tag_sub = ch_tag_parser.add_subparsers(dest="tag_action")
    ch_tag_sub.add_parser("list", help="List all tags")
    ch_tag_add = ch_tag_sub.add_parser("add", help="Create a tag")
    ch_tag_add.add_argument("name", help="Tag name")
    ch_tag_del = ch_tag_sub.add_parser("delete", help="Delete a tag")
    ch_tag_del.add_argument("name", help="Tag name")
    ch_tag_set = ch_tag_sub.add_parser("set", help="Set tags for a channel")
    ch_tag_set.add_argument("pk", type=int, help="Channel primary key")
    ch_tag_set.add_argument("tags", help="Comma-separated tag names")
    ch_tag_get = ch_tag_sub.add_parser("get", help="Get tags for a channel")
    ch_tag_get.add_argument("pk", type=int, help="Channel primary key")

    flt_parser = sub.add_parser("filter", help="Channel content filter")
    flt_sub = flt_parser.add_subparsers(dest="filter_action")
    flt_sub.add_parser("analyze", help="Analyze channels and show report")
    flt_sub.add_parser("apply", help="Analyze and mark filtered channels")
    flt_sub.add_parser("reset", help="Reset all channel filters")
    flt_sub.add_parser("precheck", help="Apply pre-filter by subscriber ratio (no Telegram needed)")
    flt_toggle = flt_sub.add_parser("toggle", help="Toggle filter for a single channel")
    flt_toggle.add_argument("pk", type=int, help="Channel primary key")
    flt_purge = flt_sub.add_parser("purge", help="Purge messages from filtered channels")
    flt_purge.add_argument("--pks", default=None, help="Comma-separated PKs (default: all)")
    flt_purge_msgs = flt_sub.add_parser(
        "purge-messages",
        help="Delete messages for a specific channel from DB",
    )
    flt_purge_msgs.add_argument("--channel-id", type=int, required=True, dest="channel_id",
                                help="Channel ID whose messages to delete")
    flt_purge_msgs.add_argument("--yes", "-y", action="store_true", help="Skip confirmation prompt")

    flt_hard = flt_sub.add_parser(
        "hard-delete",
        help="Hard-delete filtered channels from DB (dev/testing)",
    )
    flt_hard.add_argument("--pks", default=None, help="Comma-separated PKs (default: all)")
    flt_hard.add_argument("--yes", action="store_true", help="Skip confirmation prompt")

    sq_parser = sub.add_parser("search-query", help="Search query management")
    sq_sub = sq_parser.add_subparsers(dest="search_query_action")
    sq_sub.add_parser("list", help="List search queries")

    sq_add = sq_sub.add_parser("add", help="Add search query")
    sq_add.add_argument("query", help="FTS5 search query text")
    sq_add.add_argument("--interval", type=int, default=60, help="Run interval in minutes")
    sq_add.add_argument("--regex", action="store_true", help="Use regex matching")
    sq_add.add_argument("--fts", action="store_true", help="Use FTS5 boolean syntax (no quoting)")
    sq_add.add_argument("--notify", action="store_true", help="Notify on collect")
    sq_add.add_argument("--no-track-stats", dest="track_stats", action="store_false", default=True)
    sq_add.add_argument(
        "--exclude-patterns", default="", help="Exclude patterns, one per line (use \\n)"
    )
    sq_add.add_argument("--max-length", type=int, default=None, help="Max message text length")

    sq_edit = sq_sub.add_parser("edit", help="Edit search query")
    sq_edit.add_argument("id", type=int, help="Search query id")
    sq_edit.add_argument("--query", help="New query text")
    sq_edit.add_argument("--interval", type=int, help="New interval in minutes")
    sq_edit.add_argument("--regex", action="store_true", default=None)
    sq_edit.add_argument("--no-regex", dest="regex", action="store_false")
    sq_edit.add_argument("--fts", action="store_true", default=None)
    sq_edit.add_argument("--no-fts", dest="fts", action="store_false")
    sq_edit.add_argument("--notify", action="store_true", default=None)
    sq_edit.add_argument("--no-notify", dest="notify", action="store_false")
    sq_edit.add_argument("--track-stats", action="store_true", default=None)
    sq_edit.add_argument("--no-track-stats", dest="track_stats", action="store_false")
    sq_edit.add_argument("--exclude-patterns", default=None, help="Exclude patterns (use \\n)")
    sq_edit.add_argument("--max-length", type=int, default=None, help="Max message text length")
    sq_edit.add_argument("--no-max-length", dest="max_length", action="store_const", const=-1)

    sq_del = sq_sub.add_parser("delete", help="Delete search query")
    sq_del.add_argument("id", type=int, help="Search query id")

    sq_toggle = sq_sub.add_parser("toggle", help="Toggle search query active state")
    sq_toggle.add_argument("id", type=int, help="Search query id")

    sq_run = sq_sub.add_parser("run", help="Run a search query once and show matches")
    sq_run.add_argument("id", type=int, help="Search query id")

    sq_stats = sq_sub.add_parser("stats", help="Show daily stats for a search query")
    sq_stats.add_argument("id", type=int, help="Search query id")
    sq_stats.add_argument("--days", type=int, default=30, help="Number of days")

    pipeline_parser = sub.add_parser("pipeline", help="Content pipeline management")
    pipeline_sub = pipeline_parser.add_subparsers(dest="pipeline_action")
    pipeline_sub.add_parser("list", help="List pipelines")

    pipeline_show = pipeline_sub.add_parser("show", help="Show pipeline details")
    pipeline_show.add_argument("id", type=int, help="Pipeline id")

    pipeline_add = pipeline_sub.add_parser("add", help="Add pipeline")
    pipeline_add.add_argument("name", help="Pipeline name")
    pipeline_add.add_argument("--prompt-template", required=True, help="Prompt template")
    pipeline_add.add_argument(
        "--source",
        type=int,
        action="append",
        required=True,
        help="Source channel_id; repeat for multiple channels",
    )
    pipeline_add.add_argument(
        "--target",
        action="append",
        required=True,
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

    # ── image ──
    image_parser = sub.add_parser("image", help="Image generation")
    image_sub = image_parser.add_subparsers(dest="image_action")

    image_gen = image_sub.add_parser("generate", help="Generate an image from prompt")
    image_gen.add_argument("prompt", help="Text prompt for image generation")
    image_gen.add_argument("--model", default=None, help="Model string (e.g. replicate:flux-schnell)")

    image_models = image_sub.add_parser("models", help="Search available models")
    image_models.add_argument("--provider", required=True, help="Provider name (replicate, together, openai)")
    image_models.add_argument("--query", default="", help="Search query")

    image_sub.add_parser("providers", help="List configured image providers")

    acc_parser = sub.add_parser("account", help="Account management")
    acc_sub = acc_parser.add_subparsers(dest="account_action")
    acc_sub.add_parser("list", help="List accounts")

    acc_info = acc_sub.add_parser("info", help="Show profile info for connected accounts")
    acc_info.add_argument("--phone", default=None, help="Filter by phone number")

    acc_toggle = acc_sub.add_parser("toggle", help="Toggle account active state")
    acc_toggle.add_argument("id", type=int, help="Account id")

    acc_del = acc_sub.add_parser("delete", help="Delete account")
    acc_del.add_argument("id", type=int, help="Account id")

    acc_add = acc_sub.add_parser("add", help="Add Telegram account (interactive auth)")
    acc_add.add_argument("--api-id", type=int, default=None, dest="api_id",
                         help="Telegram API ID (uses stored if omitted)")
    acc_add.add_argument("--api-hash", default=None, dest="api_hash",
                         help="Telegram API hash (uses stored if omitted)")
    acc_add.add_argument("--phone", required=True, help="Phone number with country code")

    acc_sub.add_parser("flood-status", help="Show flood wait timers for all accounts")

    acc_flood_clear = acc_sub.add_parser("flood-clear", help="Clear flood wait for an account")
    acc_flood_clear.add_argument("--phone", required=True, help="Account phone number")

    sched_parser = sub.add_parser("scheduler", help="Scheduler control")
    sched_sub = sched_parser.add_subparsers(dest="scheduler_action")
    sched_sub.add_parser("start", help="Start scheduler (foreground)")
    sched_sub.add_parser("trigger", help="Trigger one-shot collection")
    sched_sub.add_parser("status", help="Show scheduler configuration and status")
    sched_sub.add_parser("stop", help="Disable scheduler autostart")
    sched_job_toggle = sched_sub.add_parser("job-toggle", help="Toggle scheduler job enabled/disabled")
    sched_job_toggle.add_argument("job_id", help="Job identifier (e.g. collect_all, sq_1)")
    sched_interval = sched_sub.add_parser("set-interval", help="Set scheduler job interval")
    sched_interval.add_argument("job_id", help="Job identifier")
    sched_interval.add_argument("minutes", type=int, help="Interval in minutes (1-1440)")
    sched_task_cancel = sched_sub.add_parser("task-cancel", help="Cancel a collection task")
    sched_task_cancel.add_argument("task_id", type=int, help="Task ID to cancel")
    sched_sub.add_parser("clear-pending", help="Clear all pending collection tasks")

    dialogs_parser = sub.add_parser(
        "dialogs", aliases=["my-telegram"], help="Telegram dialogs management",
    )
    dialogs_sub = dialogs_parser.add_subparsers(dest="dialogs_action")
    dialogs_list = dialogs_sub.add_parser("list", help="List all dialogs for an account")
    dialogs_list.add_argument(
        "--phone", default=None, help="Account phone (default: first connected)"
    )  # noqa: E501
    dialogs_refresh = dialogs_sub.add_parser("refresh", help="Refresh dialog cache from Telegram")
    dialogs_refresh.add_argument(
        "--phone", default=None, help="Account phone (default: first connected)"
    )

    dialogs_leave = dialogs_sub.add_parser("leave", help="Leave dialogs by ID")
    dialogs_leave.add_argument(
        "dialog_ids",
        nargs="+",
        help="Dialog IDs to leave (space- or comma-separated)",
    )
    dialogs_leave.add_argument(
        "--phone", default=None, help="Account phone (default: first connected)"
    )
    dialogs_leave.add_argument("--yes", "-y", action="store_true", help="Skip confirmation prompt")

    my_tg_topics = dialogs_sub.add_parser("topics", help="List forum topics for a channel")
    my_tg_topics.add_argument(
        "--channel-id",
        type=int,
        required=True,
        dest="channel_id",
        help="Channel ID to fetch forum topics for",
    )
    my_tg_topics.add_argument(
        "--phone",
        default=None,
        help="Account phone (default: any available)",
    )

    my_tg_clear = dialogs_sub.add_parser("cache-clear", help="Clear in-memory and DB dialog cache")
    my_tg_clear.add_argument("--phone", default=None, help="Account phone (default: all accounts)")
    dialogs_sub.add_parser("cache-status", help="Show dialog cache status (entries, age)")

    my_tg_send = dialogs_sub.add_parser("send", help="Send a direct message to a user or chat")
    my_tg_send.add_argument("recipient", help="Recipient: @username, phone number, or numeric ID")
    my_tg_send.add_argument("text", help="Message text to send")
    my_tg_send.add_argument("--phone", default=None, help="Account phone (default: first connected)")
    my_tg_send.add_argument("--yes", "-y", action="store_true", help="Skip confirmation prompt")

    my_tg_fwd = dialogs_sub.add_parser("forward", help="Forward messages between chats")
    my_tg_fwd.add_argument("from_chat", help="Source chat ID or @username")
    my_tg_fwd.add_argument("to_chat", help="Destination chat ID or @username")
    my_tg_fwd.add_argument("message_ids", nargs="+", help="Message IDs to forward (space or comma-separated)")
    my_tg_fwd.add_argument("--phone", default=None, help="Account phone (default: first connected)")
    my_tg_fwd.add_argument("--yes", "-y", action="store_true", help="Skip confirmation prompt")

    my_tg_edit = dialogs_sub.add_parser("edit-message", help="Edit a sent message")
    my_tg_edit.add_argument("chat_id", help="Chat ID or @username")
    my_tg_edit.add_argument("message_id", type=int, help="Message ID to edit")
    my_tg_edit.add_argument("text", help="New message text")
    my_tg_edit.add_argument("--phone", default=None, help="Account phone (default: first connected)")
    my_tg_edit.add_argument("--yes", "-y", action="store_true", help="Skip confirmation prompt")

    my_tg_del_msg = dialogs_sub.add_parser("delete-message", help="Delete messages from a chat")
    my_tg_del_msg.add_argument("chat_id", help="Chat ID or @username")
    my_tg_del_msg.add_argument("message_ids", nargs="+", help="Message IDs to delete (space or comma-separated)")
    my_tg_del_msg.add_argument("--phone", default=None, help="Account phone (default: first connected)")
    my_tg_del_msg.add_argument("--yes", "-y", action="store_true", help="Skip confirmation prompt")

    my_tg_create = dialogs_sub.add_parser("create-channel", help="Create a new Telegram broadcast channel")
    my_tg_create.add_argument("--phone", default=None, help="Account phone (default: first connected)")
    my_tg_create.add_argument("--title", required=True, help="Channel title")
    my_tg_create.add_argument("--about", default="", help="Channel description")
    my_tg_create.add_argument("--username", default="", help="Public username (leave empty for private)")

    my_tg_pin = dialogs_sub.add_parser("pin-message", help="Pin a message in a chat")
    my_tg_pin.add_argument("chat_id", help="Chat ID or @username")
    my_tg_pin.add_argument("message_id", type=int, help="Message ID to pin")
    my_tg_pin.add_argument("--phone", default=None, help="Account phone (default: first connected)")
    my_tg_pin.add_argument("--notify", action="store_true", help="Notify members about pinned message")
    my_tg_pin.add_argument("--yes", "-y", action="store_true", help="Skip confirmation prompt")

    my_tg_unpin = dialogs_sub.add_parser("unpin-message", help="Unpin a message in a chat")
    my_tg_unpin.add_argument("chat_id", help="Chat ID or @username")
    my_tg_unpin.add_argument("--message-id", type=int, default=None, dest="message_id",
                             help="Message ID to unpin (omit to unpin all)")
    my_tg_unpin.add_argument("--phone", default=None, help="Account phone (default: first connected)")
    my_tg_unpin.add_argument("--yes", "-y", action="store_true", help="Skip confirmation prompt")

    my_tg_dl = dialogs_sub.add_parser("download-media", help="Download media from a message")
    my_tg_dl.add_argument("chat_id", help="Chat ID or @username")
    my_tg_dl.add_argument("message_id", type=int, help="Message ID containing media")
    my_tg_dl.add_argument("--phone", default=None, help="Account phone (default: first connected)")
    my_tg_dl.add_argument("--output-dir", default=".", dest="output_dir",
                          help="Directory to save file (default: current dir)")

    my_tg_participants = dialogs_sub.add_parser("participants", help="List participants of a channel/group")
    my_tg_participants.add_argument("chat_id", help="Chat ID or @username")
    my_tg_participants.add_argument("--phone", default=None, help="Account phone (default: first connected)")
    my_tg_participants.add_argument("--limit", type=int, default=200, help="Max participants to fetch (default: 200)")
    my_tg_participants.add_argument("--search", default="", help="Search query to filter participants")

    my_tg_edit_admin = dialogs_sub.add_parser("edit-admin", help="Promote or demote a user as admin")
    my_tg_edit_admin.add_argument("chat_id", help="Chat ID or @username")
    my_tg_edit_admin.add_argument("user_id", help="User ID or @username to change admin rights for")
    my_tg_edit_admin.add_argument("--phone", default=None, help="Account phone (default: first connected)")
    my_tg_edit_admin.add_argument("--title", default=None, help="Custom admin title")
    my_tg_edit_admin.add_argument("--is-admin", dest="is_admin", action="store_true",
                                  default=True, help="Promote to admin (default)")
    my_tg_edit_admin.add_argument("--no-admin", dest="is_admin", action="store_false", help="Demote from admin")
    my_tg_edit_admin.add_argument("--yes", "-y", action="store_true", help="Skip confirmation prompt")

    my_tg_edit_perms = dialogs_sub.add_parser("edit-permissions", help="Restrict or unrestrict a user in a group")
    my_tg_edit_perms.add_argument("chat_id", help="Chat ID or @username")
    my_tg_edit_perms.add_argument("user_id", help="User ID or @username")
    my_tg_edit_perms.add_argument("--phone", default=None, help="Account phone (default: first connected)")
    my_tg_edit_perms.add_argument("--until-date", default=None, dest="until_date",
                                  help="Restriction end date (ISO format, e.g. 2025-12-31)")
    my_tg_edit_perms.add_argument("--send-messages", dest="send_messages", default=None,
                                  help="Allow sending messages (true/false)")
    my_tg_edit_perms.add_argument("--send-media", dest="send_media", default=None,
                                  help="Allow sending media (true/false)")
    my_tg_edit_perms.add_argument("--yes", "-y", action="store_true", help="Skip confirmation prompt")

    my_tg_kick = dialogs_sub.add_parser("kick", help="Kick a participant from a chat")
    my_tg_kick.add_argument("chat_id", help="Chat ID or @username")
    my_tg_kick.add_argument("user_id", help="User ID or @username to kick")
    my_tg_kick.add_argument("--phone", default=None, help="Account phone (default: first connected)")
    my_tg_kick.add_argument("--yes", "-y", action="store_true", help="Skip confirmation prompt")

    my_tg_bstats = dialogs_sub.add_parser("broadcast-stats", help="Get broadcast statistics for a channel")
    my_tg_bstats.add_argument("chat_id", help="Channel ID or @username")
    my_tg_bstats.add_argument("--phone", default=None, help="Account phone (default: first connected)")

    my_tg_archive = dialogs_sub.add_parser("archive", help="Archive a dialog (move to archive folder)")
    my_tg_archive.add_argument("chat_id", help="Chat ID or @username")
    my_tg_archive.add_argument("--phone", default=None, help="Account phone (default: first connected)")

    my_tg_unarchive = dialogs_sub.add_parser("unarchive", help="Unarchive a dialog (move to main folder)")
    my_tg_unarchive.add_argument("chat_id", help="Chat ID or @username")
    my_tg_unarchive.add_argument("--phone", default=None, help="Account phone (default: first connected)")

    my_tg_mark_read = dialogs_sub.add_parser("mark-read", help="Mark messages as read in a chat")
    my_tg_mark_read.add_argument("chat_id", help="Chat ID or @username")
    my_tg_mark_read.add_argument("--phone", default=None, help="Account phone (default: first connected)")
    my_tg_mark_read.add_argument("--max-id", type=int, default=None, dest="max_id",
                                 help="Mark messages up to this ID as read (default: all)")

    notif_parser = sub.add_parser("notification", help="Personal notification bot management")
    notif_sub = notif_parser.add_subparsers(dest="notification_action")
    notif_sub.add_parser("setup", help="Create personal notification bot via BotFather")
    notif_sub.add_parser("status", help="Show notification bot status")
    notif_sub.add_parser("delete", help="Delete notification bot via BotFather")
    notif_test = notif_sub.add_parser("test", help="Send a test notification message")
    notif_test.add_argument("--message", default="Тестовое уведомление", help="Message text")
    notif_sub.add_parser("dry-run", help="Preview notification matches without sending")
    notif_set_acc = notif_sub.add_parser("set-account", help="Set account for notification bot")
    notif_set_acc.add_argument("--phone", required=True, help="Account phone number")

    agent_parser = sub.add_parser("agent", help="Agent chat management")
    agent_sub = agent_parser.add_subparsers(dest="agent_action")

    agent_sub.add_parser("threads", help="List agent threads")

    agent_create = agent_sub.add_parser("thread-create", help="Create new thread")
    agent_create.add_argument("--title", default=None, help="Thread title")

    agent_delete = agent_sub.add_parser("thread-delete", help="Delete thread")
    agent_delete.add_argument("thread_id", type=int, help="Thread ID")

    agent_chat = agent_sub.add_parser("chat", help="Interactive TUI chat or one-shot message (with -p)")
    agent_chat.add_argument("-p", "--prompt", default=None, dest="prompt", help="Message text (non-interactive mode)")
    agent_chat.add_argument("--thread-id", type=int, default=None, dest="thread_id")
    agent_chat.add_argument("--model", default=None, help="Model name")

    agent_rename = agent_sub.add_parser("thread-rename", help="Rename thread")
    agent_rename.add_argument("thread_id", type=int, help="Thread ID")
    agent_rename.add_argument("title", help="New title")

    agent_msgs = agent_sub.add_parser("messages", help="Show thread messages")
    agent_msgs.add_argument("thread_id", type=int, help="Thread ID")
    agent_msgs.add_argument("--limit", type=int, default=None, help="Last N messages")

    agent_ctx = agent_sub.add_parser("context", help="Inject channel context into thread")
    agent_ctx.add_argument("thread_id", type=int, help="Thread ID")
    agent_ctx.add_argument("--channel-id", type=int, required=True, dest="channel_id")
    agent_ctx.add_argument("--limit", type=int, default=100000, help="Max messages")
    agent_ctx.add_argument("--topic-id", type=int, default=None, dest="topic_id")

    agent_sub.add_parser("test-escaping", help="Test agent with special characters")
    agent_sub.add_parser("test-tools", help="Test that agent tool calls produce tool_start/tool_end events")

    photo_parser = sub.add_parser("photo-loader", help="Photo upload automation")
    photo_sub = photo_parser.add_subparsers(dest="photo_loader_action")

    photo_dialogs = photo_sub.add_parser("dialogs", help="List dialogs for an account")
    photo_dialogs.add_argument("--phone", required=True, help="Account phone")

    photo_refresh = photo_sub.add_parser("refresh", help="Refresh dialog cache for photo loader")
    photo_refresh.add_argument("--phone", required=True, help="Account phone")

    photo_send = photo_sub.add_parser("send", help="Send photos now")
    photo_send.add_argument("--phone", required=True, help="Account phone")
    photo_send.add_argument("--target", required=True, help="Dialog id")
    photo_send.add_argument("--files", nargs="+", required=True, help="Photo file paths")
    photo_send.add_argument("--mode", choices=["album", "separate"], default="album")
    photo_send.add_argument("--caption", default=None, help="Caption")

    photo_schedule = photo_sub.add_parser("schedule-send", help="Schedule photo send via Telegram")
    photo_schedule.add_argument("--phone", required=True, help="Account phone")
    photo_schedule.add_argument("--target", required=True, help="Dialog id")
    photo_schedule.add_argument("--files", nargs="+", required=True, help="Photo file paths")
    photo_schedule.add_argument("--mode", choices=["album", "separate"], default="album")
    photo_schedule.add_argument("--at", required=True, help="ISO datetime")
    photo_schedule.add_argument("--caption", default=None, help="Caption")

    photo_batch = photo_sub.add_parser("batch-create", help="Create delayed batch from manifest")
    photo_batch.add_argument("--phone", required=True, help="Account phone")
    photo_batch.add_argument("--target", required=True, help="Dialog id")
    photo_batch.add_argument("--manifest", required=True, help="JSON/YAML manifest path")
    photo_batch.add_argument("--caption", default=None, help="Default caption")

    photo_sub.add_parser("batch-list", help="List photo batches")

    photo_cancel = photo_sub.add_parser("batch-cancel", help="Cancel a photo batch item")
    photo_cancel.add_argument("id", type=int, help="Photo item id")

    photo_auto_create = photo_sub.add_parser("auto-create", help="Create auto-upload job")
    photo_auto_create.add_argument("--phone", required=True, help="Account phone")
    photo_auto_create.add_argument("--target", required=True, help="Dialog id")
    photo_auto_create.add_argument("--folder", required=True, help="Folder path")
    photo_auto_create.add_argument(
        "--interval",
        type=int,
        required=True,
        help="Interval in minutes",
    )
    photo_auto_create.add_argument("--mode", choices=["album", "separate"], default="album")
    photo_auto_create.add_argument("--caption", default=None, help="Caption")

    photo_sub.add_parser("auto-list", help="List auto-upload jobs")

    photo_auto_update = photo_sub.add_parser("auto-update", help="Update auto-upload job")
    photo_auto_update.add_argument("id", type=int, help="Job id")
    photo_auto_update.add_argument("--folder", default=None, help="Folder path")
    photo_auto_update.add_argument("--interval", type=int, default=None, help="Interval in minutes")
    photo_auto_update.add_argument("--mode", choices=["album", "separate"], default=None)
    photo_auto_update.add_argument("--caption", default=None, help="Caption")
    photo_auto_update.add_argument("--active", action="store_true", help="Enable job")
    photo_auto_update.add_argument("--paused", action="store_true", help="Pause job")

    photo_auto_toggle = photo_sub.add_parser("auto-toggle", help="Toggle auto-upload job")
    photo_auto_toggle.add_argument("id", type=int, help="Job id")

    photo_auto_delete = photo_sub.add_parser("auto-delete", help="Delete auto-upload job")
    photo_auto_delete.add_argument("id", type=int, help="Job id")

    photo_sub.add_parser("run-due", help="Run due photo items and auto jobs now")

    test_parser = sub.add_parser("test", help="Run diagnostic tests")
    test_sub = test_parser.add_subparsers(dest="test_action")
    test_sub.add_parser("all", help="Run all test sections (read + write + telegram)")
    test_sub.add_parser("read", help="Read-only DB checks")
    test_sub.add_parser("write", help="Write DB checks on a temporary DB copy")
    test_sub.add_parser("telegram", help="Live Telegram API tests on a temporary DB copy")
    test_sub.add_parser(
        "benchmark",
        help="Benchmark serial pytest run against the safe mixed parallel test workflow",
    )

    analytics_parser = sub.add_parser("analytics", help="Message analytics")
    analytics_sub = analytics_parser.add_subparsers(dest="analytics_action")

    analytics_top = analytics_sub.add_parser("top", help="Top messages by reactions")
    analytics_top.add_argument(
        "--limit", type=int, default=20, help="Number of results (default: 20)"
    )
    analytics_top.add_argument(
        "--date-from", dest="date_from", default=None, help="Start date (YYYY-MM-DD)"
    )
    analytics_top.add_argument(
        "--date-to", dest="date_to", default=None, help="End date (YYYY-MM-DD)"
    )

    analytics_ct = analytics_sub.add_parser("content-types", help="Engagement by content type")
    analytics_ct.add_argument(
        "--date-from", dest="date_from", default=None, help="Start date (YYYY-MM-DD)"
    )
    analytics_ct.add_argument(
        "--date-to", dest="date_to", default=None, help="End date (YYYY-MM-DD)"
    )

    analytics_hourly = analytics_sub.add_parser("hourly", help="Hourly activity patterns")
    analytics_hourly.add_argument(
        "--date-from", dest="date_from", default=None, help="Start date (YYYY-MM-DD)"
    )
    analytics_hourly.add_argument(
        "--date-to", dest="date_to", default=None, help="End date (YYYY-MM-DD)"
    )

    analytics_sub.add_parser("summary", help="Content generation summary")

    analytics_daily = analytics_sub.add_parser("daily", help="Daily generation stats")
    analytics_daily.add_argument("--days", type=int, default=30, help="Number of days (default: 30)")
    analytics_daily.add_argument("--pipeline-id", dest="pipeline_id", type=int, default=None)

    analytics_pipeline = analytics_sub.add_parser("pipeline-stats", help="Per-pipeline statistics")
    analytics_pipeline.add_argument("--pipeline-id", dest="pipeline_id", type=int, default=None)

    analytics_trending = analytics_sub.add_parser("trending-topics", help="Trending topics/keywords")
    analytics_trending.add_argument("--days", type=int, default=7, help="Number of days (default: 7)")
    analytics_trending.add_argument("--limit", type=int, default=20)

    analytics_channels = analytics_sub.add_parser("trending-channels", help="Top channels by activity")
    analytics_channels.add_argument("--days", type=int, default=7, help="Number of days (default: 7)")
    analytics_channels.add_argument("--limit", type=int, default=20)

    analytics_velocity = analytics_sub.add_parser("velocity", help="Message volume per day")
    analytics_velocity.add_argument("--days", type=int, default=30, help="Number of days (default: 30)")

    analytics_sub.add_parser("peak-hours", help="Peak activity hours")

    analytics_calendar = analytics_sub.add_parser("calendar", help="Upcoming scheduled publications")
    analytics_calendar.add_argument("--limit", type=int, default=20)
    analytics_calendar.add_argument("--pipeline-id", dest="pipeline_id", type=int, default=None)

    analytics_emojis = analytics_sub.add_parser("trending-emojis", help="Trending emojis in messages")
    analytics_emojis.add_argument("--days", type=int, default=7, help="Number of days (default: 7)")
    analytics_emojis.add_argument("--limit", type=int, default=20)

    # ── provider ──
    provider_parser = sub.add_parser("provider", help="LLM provider management")
    provider_sub = provider_parser.add_subparsers(dest="provider_action")
    provider_sub.add_parser("list", help="List configured providers with models and status")
    provider_add = provider_sub.add_parser("add", help="Add or update a provider")
    provider_add.add_argument("name", help="Provider name (e.g. openai, groq, anthropic)")
    provider_add.add_argument("--api-key", required=True, dest="api_key", help="API key")
    provider_add.add_argument("--base-url", default=None, dest="base_url", help="Custom base URL")
    provider_del = provider_sub.add_parser("delete", help="Delete a provider")
    provider_del.add_argument("name", help="Provider name")
    provider_probe = provider_sub.add_parser("probe", help="Test provider connection")
    provider_probe.add_argument("name", help="Provider name")
    provider_refresh = provider_sub.add_parser("refresh", help="Refresh provider models")
    provider_refresh.add_argument("name", nargs="?", default=None, help="Provider name (default: all)")
    provider_sub.add_parser("test-all", help="Test all configured providers")

    # ── export ──
    export_parser = sub.add_parser("export", help="Export collected messages")
    export_sub = export_parser.add_subparsers(dest="export_action")
    for fmt_name in ("json", "csv", "rss"):
        exp = export_sub.add_parser(fmt_name, help=f"Export as {fmt_name.upper()}")
        exp.add_argument("--channel-id", type=int, default=None, dest="channel_id",
                         help="Filter by channel ID")
        exp.add_argument("--limit", type=int, default=200, help="Max messages (default: 200)")
        exp.add_argument("--output", "-o", default=None, help="Output file (default: stdout)")

    translate_parser = sub.add_parser("translate", help="Language detection and translation")
    translate_sub = translate_parser.add_subparsers(dest="translate_action")
    translate_sub.add_parser("stats", help="Show language distribution")
    detect_parser = translate_sub.add_parser("detect", help="Backfill language detection")
    detect_parser.add_argument("--batch-size", type=int, default=5000)
    run_parser = translate_sub.add_parser("run", help="Run translation batch")
    run_parser.add_argument("--target", default="en", help="Target language code")
    run_parser.add_argument("--source-filter", default="", help="Comma-separated source languages")
    run_parser.add_argument("--limit", type=int, default=100, help="Max messages to translate")

    translate_msg = translate_sub.add_parser("message", help="Translate a single message")
    translate_msg.add_argument("message_id", type=int, help="Message DB id")
    translate_msg.add_argument("--target", default="en", help="Target language code")

    settings_parser = sub.add_parser("settings", help="System settings management")
    settings_sub = settings_parser.add_subparsers(dest="settings_action")
    settings_get = settings_sub.add_parser("get", help="Show settings")
    settings_get.add_argument("--key", default=None, help="Specific setting key (default: show all)")
    settings_set = settings_sub.add_parser("set", help="Set a setting value")
    settings_set.add_argument("key", help="Setting key")
    settings_set.add_argument("value", help="Setting value")
    settings_sub.add_parser("info", help="Show system diagnostics")

    settings_agent = settings_sub.add_parser("agent", help="Configure agent backend and defaults")
    settings_agent.add_argument("--backend", default=None, help="Agent backend (claude-agent-sdk, deepagents)")
    settings_agent.add_argument("--prompt-template", default=None, dest="prompt_template",
                                help="Default prompt template")

    settings_filter = settings_sub.add_parser("filter-criteria", help="Configure filter thresholds")
    settings_filter.add_argument("--min-uniqueness", type=float, default=None, dest="min_uniqueness")
    settings_filter.add_argument("--min-sub-ratio", type=float, default=None, dest="min_sub_ratio")
    settings_filter.add_argument("--max-cross-dupe", type=float, default=None, dest="max_cross_dupe")
    settings_filter.add_argument("--min-cyrillic", type=float, default=None, dest="min_cyrillic")

    settings_semantic = settings_sub.add_parser("semantic", help="Configure semantic search")
    settings_semantic.add_argument("--provider", default=None, help="Embedding provider")
    settings_semantic.add_argument("--model", default=None, help="Embedding model")
    settings_semantic.add_argument("--api-key", default=None, dest="api_key", help="Embedding API key")

    # ── debug ──
    debug_parser = sub.add_parser("debug", help="Diagnostic tools")
    debug_sub = debug_parser.add_subparsers(dest="debug_action")
    debug_logs = debug_sub.add_parser("logs", help="Show recent log entries")
    debug_logs.add_argument("--limit", type=int, default=50, help="Number of log lines (default: 50)")
    debug_sub.add_parser("memory", help="Show memory usage statistics")
    debug_sub.add_parser("timing", help="Show operation timing stats")

    return parser
