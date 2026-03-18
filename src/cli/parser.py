from __future__ import annotations

import argparse


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="TG Post Search")
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

    ch_import = ch_sub.add_parser("import", help="Bulk import from file or text")
    ch_import.add_argument("source", help="Path to .txt/.csv file, or comma-separated identifiers")

    flt_parser = sub.add_parser("filter", help="Channel content filter")
    flt_sub = flt_parser.add_subparsers(dest="filter_action")
    flt_sub.add_parser("analyze", help="Analyze channels and show report")
    flt_sub.add_parser("apply", help="Analyze and mark filtered channels")
    flt_sub.add_parser("reset", help="Reset all channel filters")
    flt_sub.add_parser("precheck", help="Apply pre-filter by subscriber ratio (no Telegram needed)")
    flt_purge = flt_sub.add_parser("purge", help="Purge messages from filtered channels")
    flt_purge.add_argument("--pks", default=None, help="Comma-separated PKs (default: all)")
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
        choices=["chain", "agent"],
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
    pipeline_edit.add_argument("--generation-backend", choices=["chain", "agent"], default=None)
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

    pipeline_queue = pipeline_sub.add_parser("queue", help="Show moderation queue")
    pipeline_queue.add_argument("id", type=int, help="Pipeline id")
    pipeline_queue.add_argument("--limit", type=int, default=20, help="Max runs to show")

    pipeline_publish = pipeline_sub.add_parser("publish", help="Publish a generation run")
    pipeline_publish.add_argument("run_id", type=int, help="Run id to publish")

    pipeline_approve = pipeline_sub.add_parser("approve", help="Approve a generation run")
    pipeline_approve.add_argument("run_id", type=int, help="Run id to approve")

    pipeline_reject = pipeline_sub.add_parser("reject", help="Reject a generation run")
    pipeline_reject.add_argument("run_id", type=int, help="Run id to reject")

    acc_parser = sub.add_parser("account", help="Account management")
    acc_sub = acc_parser.add_subparsers(dest="account_action")
    acc_sub.add_parser("list", help="List accounts")

    acc_info = acc_sub.add_parser("info", help="Show profile info for connected accounts")
    acc_info.add_argument("--phone", default=None, help="Filter by phone number")

    acc_toggle = acc_sub.add_parser("toggle", help="Toggle account active state")
    acc_toggle.add_argument("id", type=int, help="Account id")

    acc_del = acc_sub.add_parser("delete", help="Delete account")
    acc_del.add_argument("id", type=int, help="Account id")

    acc_sub.add_parser("flood-status", help="Show flood wait timers for all accounts")

    acc_flood_clear = acc_sub.add_parser("flood-clear", help="Clear flood wait for an account")
    acc_flood_clear.add_argument("--phone", required=True, help="Account phone number")

    sched_parser = sub.add_parser("scheduler", help="Scheduler control")
    sched_sub = sched_parser.add_subparsers(dest="scheduler_action")
    sched_sub.add_parser("start", help="Start scheduler (foreground)")
    sched_sub.add_parser("trigger", help="Trigger one-shot collection")

    my_tg_parser = sub.add_parser("my-telegram", help="View account dialogs")
    my_tg_sub = my_tg_parser.add_subparsers(dest="my_telegram_action")
    my_tg_list = my_tg_sub.add_parser("list", help="List all dialogs for an account")
    my_tg_list.add_argument(
        "--phone", default=None, help="Account phone (default: first connected)"
    )  # noqa: E501
    my_tg_leave = my_tg_sub.add_parser("leave", help="Leave dialogs by ID")
    my_tg_leave.add_argument(
        "dialog_ids",
        nargs="+",
        help="Dialog IDs to leave (space- or comma-separated)",
    )
    my_tg_leave.add_argument(
        "--phone", default=None, help="Account phone (default: first connected)"
    )
    my_tg_leave.add_argument("--yes", "-y", action="store_true", help="Skip confirmation prompt")

    my_tg_topics = my_tg_sub.add_parser("topics", help="List forum topics for a channel")
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

    my_tg_clear = my_tg_sub.add_parser("cache-clear", help="Clear in-memory and DB dialog cache")
    my_tg_clear.add_argument("--phone", default=None, help="Account phone (default: all accounts)")
    my_tg_sub.add_parser("cache-status", help="Show dialog cache status (entries, age)")

    notif_parser = sub.add_parser("notification", help="Personal notification bot management")
    notif_sub = notif_parser.add_subparsers(dest="notification_action")
    notif_sub.add_parser("setup", help="Create personal notification bot via BotFather")
    notif_sub.add_parser("status", help="Show notification bot status")
    notif_sub.add_parser("delete", help="Delete notification bot via BotFather")

    agent_parser = sub.add_parser("agent", help="Agent chat management")
    agent_sub = agent_parser.add_subparsers(dest="agent_action")

    agent_sub.add_parser("threads", help="List agent threads")

    agent_create = agent_sub.add_parser("thread-create", help="Create new thread")
    agent_create.add_argument("--title", default=None, help="Thread title")

    agent_delete = agent_sub.add_parser("thread-delete", help="Delete thread")
    agent_delete.add_argument("thread_id", type=int, help="Thread ID")

    agent_chat = agent_sub.add_parser("chat", help="Send message to agent")
    agent_chat.add_argument("message", help="Message text")
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

    photo_parser = sub.add_parser("photo-loader", help="Photo upload automation")
    photo_sub = photo_parser.add_subparsers(dest="photo_loader_action")

    photo_dialogs = photo_sub.add_parser("dialogs", help="List dialogs for an account")
    photo_dialogs.add_argument("--phone", required=True, help="Account phone")

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

    return parser
