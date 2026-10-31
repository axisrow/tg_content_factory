# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Positioning (automation-first, set 2026-09-30; was "dependency for coding agents", 2026-09)

tg-agent is an **automation-first Telegram engine**: the contract consumers are programs —
coding agents (Claude Code, OpenCode, Codex) get full-spectrum Telegram access through the
`tg-agent` CLI plus the skill in `skills/tg-agent/` (plugin manifests in `.claude-plugin/`).
No human-facing surface is part of the contract: humans are not consumers of this product —
nobody looks at screens; every consumer is a program.

- **Native path — CLI first.** New capabilities are designed, implemented and tested as CLI
  commands (incl. real-TG manifest coverage) before any other surface. If a change affects the
  agent-facing surface, update `skills/tg-agent/SKILL.md` / `skills/tg-agent/reference.md` in
  the same PR.
- **Deprecated (automation-first contract, 2026-09-30):** web dashboard, TUI, embedded agent
  chat (`agent chat`, SDK backends), MCP server. They keep running but receive **no investment**:
  no features and no proactive bug fixes — fixes only on an explicit owner request. Removal is a
  standing candidate, decided separately.
- **Not legacy:** the worker runtime (collection queue, scheduler, dispatchers, snapshots) —
- **Not legacy:** the worker runtime (collection queue, scheduler, dispatchers, snapshots) —
  it is the engine behind scheduled collection and queued sends.
- Keep `.claude-plugin/plugin.json` `version` in sync with `pyproject.toml` on release.

## Commands

```bash
# Install dev dependencies
pip install -e ".[dev]"

# Install the monorepo shared package — the factory imports tg_shared at
# runtime (#1421 wave A), so without this step the CLI/daemon dies with
# ModuleNotFoundError: tg_shared (pytest alone is covered by the root
# conftest.py sys.path shim). A declared pyproject dependency lands when
# tg-shared is published (#1424 wave).
pip install -e packages/tg_shared

# Optional legacy surfaces live in their own monorepo packages (#1720): the
# interactive agent-chat TUI (`agent chat` without a prompt) needs tg-tui.
# The core never imports them (import-linter contracts).
pip install -e packages/tg_tui

# Dependency-touching PRs must regenerate the lockfile: the committed uv.lock
# is the full-closure graph the CI dependency-review gate diffs against
# (#1706); a stale lock reds static-checks. Installs stay on pip.
uv lock

# Verify that the project is installed (required by isolated tooling such as
# mutmut; the distribution name is tg-agent, not the repository name).
python -m pip show tg-agent

# If console-entrypoint tests fail with ModuleNotFoundError: No module named
# 'src', the editable install predates the #1582 src/ -> tg_agent/ rename.
# tests/test_local_install_consistency.py fails with the diagnosis; repair:
pip install -e ".[dev]"

# Managed daemon (automation-first default) — stop any previous daemon and
# run the worker runtime in this process. No web panel, no WEB_PASS; the
# PID file makes `stop`/`restart` and the CLI worker hand-off work.
python -m tg_agent.main restart

# Same worker runtime without the stop-first step (e.g. under tmux/systemd).
python -m tg_agent.main worker

# Legacy web panel (deprecated) — uvicorn + embedded worker by default.
# For split deployments (Docker/k8s) pass --no-worker and run `worker`.
python -m tg_agent.main serve [--web-pass PASS] [--no-worker]

# Lint
ruff check tg_agent/ tests/ conftest.py

# Auto-fix lint issues
ruff check --fix tg_agent/ tests/ conftest.py

# Run parallel-safe tests (-n auto is capped at 4 workers by root conftest.py;
# override via TGCF_PYTEST_XDIST_WORKERS; single-test runs with :: force 1 worker).
# Package tests (tg_shared #1421, tg_tui #1720) live outside tests/ — include their roots.
pytest tests/ packages/tg_shared/tests packages/tg_tui/tests -v -m "not aiosqlite_serial" -n auto

# Run aiosqlite-backed tests sequentially within each file, with independent files in parallel
pytest tests/ -q -m aiosqlite_serial -n auto --dist=loadfile

# Run a single test
pytest tests/test_web.py::test_health_endpoint -v

# Benchmark serial vs safe mixed-mode suite execution
python -m tg_agent.main test benchmark

# Mutation testing (run only after the editable install above).
# NOTE: the flat-`src` layout that broke stock PyPI mutmut is gone (the import
# package was renamed to tg_agent, #1448); the strip_src_prefix fork is still
# the installed tool until the upstream PR (boxed/mutmut,
# fix/strip-src-prefix-knob) lands. Setup + sandbox details: [tool.mutmut] in
# pyproject.toml. `rm -rf mutants` before a rerun (mutmut never prunes).
mutmut run
```

Full CLI reference:

```bash
python -m tg_agent.main [--config CONFIG] serve [--web-pass PASS]
python -m tg_agent.main [--config CONFIG] worker
python -m tg_agent.main [--config CONFIG] collect [--channel-id ID] | collect sample
python -m tg_agent.main [--config CONFIG] search "query" [--limit N] [--mode MODE]

python -m tg_agent.main messages read <identifier> [--limit N] [--live] [--phone PHONE] [--query TEXT] [--date-from DATE] [--date-to DATE] [--topic-id ID] [--offset-id ID] [--format text|json|csv]
python -m tg_agent.main channel list|add|delete|toggle|collect|stats|refresh-types|refresh-meta|import|add-bulk|list-for-import|tag (tag list|add|delete|set|get)
python -m tg_agent.main filter analyze|apply|reset|precheck|toggle|purge|purge-messages|hard-delete
python -m tg_agent.main account list|info|toggle|set-primary|delete|send-code|verify-code|add|flood-status|flood-clear
python -m tg_agent.main global enable|disable|status   # shared account store (~/.tg-agent, TG_AGENT_GLOBAL_DIR); per-project .tg-agent-global marker (#1683)
python -m tg_agent.main key set|clear|status   # machine-level SESSION_ENCRYPTION_KEY in the OS secret store (opt-in; env/config wins) (#1699)
python -m tg_agent.main scheduler start|trigger|status|stop|job-toggle|set-interval|task-cancel|clear-pending|queue-pause|queue-resume
python -m tg_agent.main notification setup|status|delete|test|dry-run|set-account
python -m tg_agent.main test all|read|write|telegram|benchmark

python -m tg_agent.main stop|restart   # restart = worker daemon, no web panel; --force = provenance escape hatch
python -m tg_agent.main daemon status  # read-only daemon provenance check (up/down, PID, store, executable; #1714)
python -m tg_agent.main search-query list|get|add|edit|delete|toggle|run|stats
python -m tg_agent.main pipeline list|show|add|dry-run-count|edit|delete|toggle|run|generate|generate-stream|runs|run-show|queue|moderation-list|moderation-view|publish|approve|reject|bulk-approve|bulk-reject|refinement-steps|export|import|templates|from-template|ai-edit|filter|node|edge|graph
python -m tg_agent.main photo-loader dialogs|refresh|send|schedule-send|batch-create|batch-list|items|batch-cancel|auto-create|auto-list|auto-update|auto-toggle|auto-delete|run-due
python -m tg_agent.main dialogs list|refresh|resolve|leave|join|topics|cache-clear|cache-status|send|forward|edit-message|delete-message|create-channel|create-group|pin-message|react|unpin-message|download-media|transcribe|participants|edit-admin|edit-permissions|kick|broadcast-stats|archive|unarchive|mark-read|queue|queue status|queue cancel|queue clear-pending
python -m tg_agent.main dm drafts list|create|approve|reject|send
python -m tg_agent.main agent threads|thread-create|thread-delete|chat|thread-rename|thread-stop|messages|context|test-escaping|test-tools
python -m tg_agent.main analytics top|content-types|hourly|summary|daily|pipeline-stats|trending-topics|trending-channels|velocity|peak-hours|calendar|trending-emojis|channel|channel-rating|channel-rate|usefulness
python -m tg_agent.main provider list|add|delete|probe|refresh|test-all
python -m tg_agent.main export json|csv|rss
python -m tg_agent.main translate stats|detect|run|message
python -m tg_agent.main settings get|set|info|server-time|agent|filter-criteria|reactions|semantic
python -m tg_agent.main style init|list|show --channel <id>   # local-only per-channel style specs (~/.tg-agent/designs, never committed)
python -m tg_agent.main telegraph login --short-name "..."   # one-time: createAccount, token → settings (enc:v2, masked)
python -m tg_agent.main telegraph create --title "..." --file article.md [--author-name N] [--author-url U] [--format text|json]   # telegra.ph page from markdown (#1703)
python -m tg_agent.main debug logs|memory|timing
python -m tg_agent.main image generate|models|providers|generated
python -m tg_agent.main mcp-server [--no-pool]   # LEGACY: stdio MCP bridge over the agent tool registry (frozen)
```

CLI command for Telegram dialogs management is `dialogs`.

## Architecture

Three layers: **CLI (native surface; web/TUI/agent-chat/MCP are legacy)** → **Telegram + Search + Scheduler + Agent/Pipeline** → **SQLite**

- **Runtime split (web ↔ worker)**: the worker runtime is assembled in `tg_agent/runtime/bootstrap.py` into `RuntimeContainer`; legacy web adds presentation state through the `AppContainer` subclass in `tg_agent/web/`, with compatible entrypoints in `web/bootstrap.py`. The rest of the core must not import `tg_agent.web` either (import-linter `runtime_no_web_imports`, #1720; only `cli/commands/serve.py` may): runtime data paths live in `tg_agent/paths.py`, request timing in `tg_agent/utils/timing.py` (`web/paths.py`/`web/timing.py` re-export them). The **default daemon is worker-only** (automation-first): `restart`/`worker` run the standalone worker (`tg_agent/runtime/worker.py` — functionally identical to the embedded one) with the managed PID file and no web surface. `serve` is the legacy panel entry: it hosts uvicorn and spawns an `EmbeddedWorker` (`tg_agent/web/embedded_worker.py`) next to the web container; `--no-worker` runs only the web side (Docker/k8s split deployments).
  - Web container (`runtime_mode="web"`) uses snapshot shims (`SnapshotClientPool`, `SnapshotCollector`, `SnapshotSchedulerManager` in `tg_agent/web/runtime_shims.py`). It does NOT open Telegram connections; UI actions enqueue work into `collection_tasks` / `telegram_commands` / task tables and read `runtime_snapshots` to render status.
  - Worker container (`runtime_mode="worker"`, either embedded or standalone via `tg_agent/runtime/worker.py`) owns the live `ClientPool`, `CollectionQueue`, `UnifiedDispatcher`, `TelegramCommandDispatcher`, and `SchedulerManager`, and publishes `runtime_snapshots` (heartbeat, accounts_status, scheduler_status, …) that the web side reads.
  - In web-mode `collection_queue = None` and `CollectionService` falls back to writing a PENDING row — the worker picks those up at startup via `CollectionQueue.requeue_startup_tasks()`.
- CLI (`tg_agent/main.py` → `tg_agent/cli/commands/`) is the primary entry point and the product contract for coding agents (see Positioning); Web (`tg_agent/web/`) is a legacy parallel entry to the same logic — keep it working, do not extend
- Telegram layer: `ClientPool` manages multi-account connections, `Collector` fetches messages, `Notifier` sends alerts
- Search layer: `SearchEngine` (local DB), `AISearchEngine` (LLM-powered)
- Scheduler: APScheduler wrapper (`tg_agent/scheduler/manager.py`) triggers periodic collection
- DB: single SQLite file via aiosqlite; schema in `tg_agent/database/schema.py`, migrations in `tg_agent/database/migrations.py`, connection in `tg_agent/database/connection.py`
- Filters: `ChannelAnalyzer` (`tg_agent/filters/analyzer.py`) scores channels by uniqueness, subscriber ratio, cross-channel spam, language; thresholds in `tg_agent/filters/criteria.py`
- Collection service (`tg_agent/services/collection_service.py`): orchestration layer between web/CLI and Collector/Queue — handles enqueue logic, stats collection
- Parsers (`tg_agent/parsers.py`): identifier extraction for channel import — t.me links, @usernames, negative IDs; file parsing (txt/csv/xlsx)
- Notification bot: personal bot created via BotFather through a connected account (`tg_agent/telegram/notifier.py`)
- **Agent system**: four backends selected in `AgentManager.get_runtime_status()` (`tg_agent/agent/manager.py`) — auto-selection prefers `deepagents` when usable DB provider configs exist, then `claude-agent-sdk` (needs `ANTHROPIC_API_KEY` / `CLAUDE_CODE_OAUTH_TOKEN`), then `deepagents` again; the Codex SDK backend (`tg_agent/agent/codex_backend.py`) and the Google ADK backend (`tg_agent/agent/adk_backend.py`, Gemini models, needs `GOOGLE_API_KEY` / `GEMINI_API_KEY`) are intentionally NOT in the auto-fallback chain (each spawns a blocking out-of-process `mcp-server` subprocess) and are opt-in only via the dev-mode `agent_backend_override` setting. Both Codex and ADK reach the project tools over the same stdio MCP server (`python -m tg_agent.main mcp-server`) the in-process path serves
- **Agent system is legacy** (2026-09, see Positioning): in-process agent chat (`agent chat`, TUI, web `/agent`), SDK backends and MCP — frozen: keep functional, no new features. The interactive TUI is the optional package `packages/tg_tui` (`tg_tui.app.AgentTuiApp`, #1720 wave 2); `agent chat` without a prompt imports it lazily and exits 1 with an install hint when it is missing; the core must not import `tg_tui` (import-linter `core_no_tui_imports`)
- **Provider system**: `ProviderService` auto-registers LLM providers from env vars (`OPENAI_API_KEY`, `COHERE_API_KEY`, `OLLAMA_BASE`, etc.); the text/LLM adapters in `tg_agent/services/provider_adapters.py` are lightweight HTTP wrappers (no heavy SDK deps). The image adapters in the same file use official SDKs where they are cheap and clearer (see Image generation below) — the principle is "no *heavy* SDK", not "no SDK at all"
- **Content pipelines**: `PipelineService` + `ContentGenerationService` orchestrate generate → image → draft → notify → publish flow; tracked via `generation_runs` DB table
- **Image generation**: `ImageGenerationService` routes to provider-specific adapters (Together/HuggingFace/OpenAI/Replicate/Codex) via `provider:model_id` convention; auto-registers from env vars; adapters defined in `tg_agent/services/provider_adapters.py`. Since #958 the OpenAI/Together/Replicate adapters drive official SDKs (`openai` for both OpenAI and Together's OpenAI-compatible endpoint; `replicate` `async_run` replacing the manual poll loop); HuggingFace stays on raw aiohttp on purpose (its SDK returns a `PIL.Image`, pulling Pillow, vs saving the raw bytes); Codex uses the `openai_codex` SDK. `openai` is a direct dep (already transitive via langchain-openai); `replicate` is lightweight (httpx+pydantic)
- **UnifiedDispatcher** (`tg_agent/services/unified_dispatcher.py`): polls DB for generic tasks (CONTENT_GENERATE, CONTENT_PUBLISH, PIPELINE_RUN, PHOTO_DUE, etc.) and dispatches to handler methods; recovers interrupted tasks on startup
- **Photo publishing**: `PhotoAutoUploadService` / `PhotoPublishService` / `PhotoTaskService` — separate upload, schedule, publish tasks tracked in DB
- **Agent tools**: `tg_agent/agent/tools/` — modular tool files (channels, search, pipelines, images, etc.); `_registry.py` provides `require_confirmation()` gate for destructive ops and `normalize_phone()`; `react_agent.py` is ReAct fallback when claude-agent-sdk is unavailable; `manager.py` orchestrates backend selection
- **Agent tool permissions**: `TOOL_CATEGORIES` in `tg_agent/agent/tools/permissions.py` classifies every tool as read/write/delete; per-phone ACL overrides stored in DB setting `agent_tool_permissions` (JSON); `get_all_allowed_tools()` derives MCP-prefixed allow-list
- **S3 storage**: `tg_agent/services/s3_store.py` — optional S3-compatible backend for media (images); used by `ImageGenerationService` when S3 config is present

### Database access pattern

Repositories are accessed via `db.repos.<repo_name>.<method>()`. The `Database` facade exposes a `repos` bundle:

```python
db.repos.channels.get_all()
db.repos.generation_runs.list_pending_moderation(pipeline_id=1)
db.repos.settings.get("key")
```

Each repository has a `_to_<model>(row)` static helper that maps `aiosqlite.Row` → Pydantic model, including safe `.keys()` checks for nullable/optional columns added by migrations.

**Connection-wide write lock (issue #569).** The web app shares one `aiosqlite.Connection` (autocommit + WAL). All DML must go through one of two locked helpers on `Database` so coroutines on the same connection cannot interleave writes and commit each other's open transactions prematurely:

- `async with db.transaction() as conn` — multi-statement writes; runs `BEGIN IMMEDIATE`, holds `Database._write_lock` for the whole block, commits on clean exit and rolls back on exception.
- `await db.execute_write(sql, params)` / `await db.executemany_write(sql, seq)` — single-statement autocommit writes; acquires the same lock for the duration of execute+commit.

Reads (`SELECT`) stay lock-free. Repositories accept `database: Database | None = None` and call `self._database.transaction()` / `self._database.execute_write()`. Direct `await self._db.commit()` in repositories is a regression — use the helpers.

### Web app wiring

- `tg_agent/web/assembly.py` — `register_routes()` imports and mounts all routers; `configure_app()` binds the `AppContainer` to `app.state.*`
- `tg_agent/web/container.py` — `AppContainer` inherits shared services from `tg_agent/runtime/container.py` and adds templates/timing state; injected into FastAPI `app.state` at startup; accessed in routes via `tg_agent/web/deps.py` helpers (`deps.get_db()`, `deps.get_templates()`, etc.)

## Key Patterns

- **Entity cache**: `collect_all_channels()` calls `client.get_dialogs()` inline before iterating channels — StringSession loses entity cache between restarts, so this is required for PeerChannel lookups
- **Flood wait rotation**: `ClientPool.get_available_client()` skips accounts where `flood_wait_until` is in the future; falls back if all clients are in-use
- **Config key dropping**: `_walk_and_substitute` in config.py — if a YAML value is purely `${ENV_VAR}` and that var is empty/absent, the key is dropped entirely (not set to "")
- **Incremental collection**: `min_id = channel.last_collected_id`, `reverse=True`; after the loop, `last_collected_id` is updated to `max(seen message_ids)`
- **Batch insert**: `INSERT OR IGNORE` + `UNIQUE(channel_id, message_id)` — duplicates silently skipped
- **Cancellation**: `Collector._cancel_event` is an `asyncio.Event`, checked every 10 messages in the iter loop and at each channel boundary
- **Session tokens**: custom HMAC-SHA256 signed tokens in `tg_agent/web/session.py` — payload is `{user, exp}`, secret persisted in DB settings table, cookie max-age 30 days (`Secure` on HTTPS)
- **CollectionQueue** (`tg_agent/collection_queue.py`): `asyncio.Queue` + single worker task, task status (`pending/running/completed/failed/cancelled`) tracked in DB
- **DB migrations**: `_migrate()` in `tg_agent/database/migrations.py` uses `PRAGMA table_info` to detect missing columns and issues `ALTER TABLE ADD COLUMN` as needed
- **Keyword matching**: plain text (case-insensitive substring) and regex (`re.IGNORECASE`)
- **Channel filters**: `ChannelAnalyzer` checks `low_uniqueness`, `low_subscriber_ratio`, `cross_channel_spam`, `non_cyrillic`, `chat_noise`, `auto_feed`; filtered channels skipped during collection unless `force=True`
- **Channel creation date**: `channels.created_at` stores Telegram `entity.date` (channel creation timestamp); captured via `resolve_channel()` and `get_dialogs_for_phone()`; distinct from `added_at` (when added to the system)
- **Collection service**: `enqueue_channel_by_pk(pk, force)` respects `is_filtered` flag; `enqueue_all_channels()` uses `full=False` for incremental collection
- **Hour x Weekday Heatmap**: `db.repos.messages.get_hour_weekday_heatmap(channel_id, days)` → `[{hour, weekday, count}]`; weekday 0=Sunday per SQLite `%w`; service wrapper: `ChannelAnalyticsService.get_heatmap()`; CLI: `analytics channel`; Web: `/analytics/channels/api/heatmap`
- **Cross-Channel Citations**: `db.repos.messages.get_cross_channel_citations(channel_id, days)` → sources by `forward_from_channel_id` with JOIN to channels for title/username enrichment; stored as positive MTProto channel ID
- **Image adapter convention**: `ImageAdapter = Callable[[str, str], Awaitable[str | None]]` — signature is `(prompt, model_id) → URL/path`; model string format `provider:model_id` (e.g. `together:black-forest-labs/FLUX.1-schnell`); without prefix falls back to first registered adapter
- **Content pipeline flow**: CONTENT_GENERATE task → `ContentGenerationService.generate()` → LLM text → optional image → `generation_runs` record → if AUTO publish mode, enqueues CONTENT_PUBLISH → `PublishService.publish_run()` sends to target channel
- **Destructive tool confirmation**: agent tools that delete data require `confirm=true` argument; `require_confirmation()` in `_registry.py` returns a warning response if not confirmed
- **Decision provenance (#1307)**: automation decides only where automation decided before; operator decisions are never silently overridden. Every decision field carries an `*_origin` column (`channels.active_origin`, `channels.filtered_origin`, `accounts.active_origin`, `generation_runs.moderation_origin`), and every human write appends old/new values to the `decisions` journal. Writes go through the guarded repository methods (`set_channel_active`, `set_channel_filtered`, `set_filtered_bulk`, `reset_all_channel_filters`, `set_account_active`, `set_moderation_status`) whose SQL carries `(? = 'human' OR <origin> != 'human')`: an `origin='auto'` write on a human-owned row is suppressed (and the suppression journaled as evidence), while `origin='human'` always applies. Set `origin='human'` only for a real operator action (CLI flag/toggle, web action, approval queue); everything else stays `origin='auto'`. Never add raw SQL writes to these columns outside the guarded repo methods — `tests/test_provenance_conventions.py` fails any new unpinned site — and never purge messages of `filtered_origin='human'` channels (collector `_maybe_auto_delete` skips them).
- **HTMX progressive enhancement**: collect routes check `HX-Request` header to return HTML fragments vs redirects
- **Frontend policy — HTMX vs fetch()**:
  - **HTMX** — for operations where the server returns an HTML fragment to swap into the DOM (server-driven swaps): collect buttons, badge updates, form submissions with DOM replacement
  - **fetch()** — only for: (1) JSON endpoints without DOM replacement, (2) streaming/SSE responses (agent chat, image gen), (3) complex client-side logic before/after the request
  - PR review rule: reject fetch() where HTMX fits, and vice versa
- **Identifier parsing**: `parse_identifiers()` splits text by comma/semicolon/newline; `extract_identifiers()` regex-extracts t.me links, @usernames, negative IDs; `parse_file()` handles txt/csv/xlsx
- **aiosqlite connection cleanup**: in tests using raw `aiosqlite.connect()`, always wrap in `try/finally` with `await conn.close()` — an unclosed worker-thread blocks pytest process exit
- **SQL in triple-quoted strings**: Python does NOT concatenate adjacent string literals inside `"""..."""`; for values with quotes use parameterized `execute()` with `?`-placeholders, not inline values in `executescript()`
- **JOIN on `channels`**: always `ON x.channel_id = c.channel_id`. `channels.id` is the DB primary key and is used only in dedicated pk-lookups (`get_by_pk`, `set_channel_type`, `delete_channel`). Every sidecar table (`messages`, `channel_stats`, `generated_images`, `forward_from_channel_id`, etc.) stores the Telegram channel_id, so joining on `c.id` silently returns zero rows for any channel whose pk differs from its Telegram id. Enforced by `tests/test_sql_conventions.py`.
- **pytest-timeout**: global 120s timeout configured in `pyproject.toml` (`timeout = 120`) as a deadlock guard; dependency `pytest-timeout` in `[dev]`
- **Warnings are errors**: `filterwarnings = ["error"]` in `pyproject.toml`; CI has a dedicated step that fails if this is ever relaxed — fix warnings, don't suppress them
- **Test parallelism split**: root `conftest.py` auto-marks tests as `aiosqlite_serial` if they use the `cli_db` fixture or contain `import aiosqlite` (raw aiosqlite calls); the marked lane runs `-n auto --dist=loadfile` (sequential within each file, independent files in parallel), everything else runs with `-n auto` (`--dist=loadfile`, worker count capped at 4 by default, `TGCF_PYTEST_XDIST_WORKERS` to override; `::`-style single-test invocations and real-TG gate envs force 1 worker). Locally `-n auto` reserves one core (`cpu - 1`, no load term — the #780 load-aware throttle was removed in #1463: load average is a queue signal, not free CPU, and it collapsed `-n auto` to 1 worker on busy machines); on CI (the `CI` env var is set) every runner core is used (#944), so `-n auto` reaches the full cap
- **`db` fixture is `:memory:`**: `tests/conftest.py` provides `db` as `Database(":memory:")`. The `real_pool_harness_factory` fixture depends on `db` and passes it to the harness. Any fixture/test that creates a web app and calls `real_pool_harness_factory()` **must** accept `db` as a parameter and use it for `app.state.db` — creating a separate `Database(tmp_path / "test.db")` would give the app and the harness different DB instances, breaking account lookups.

## Conventions

- Review = the standard AO review cycle for all PRs, including pool/lease/session/security layers (`ao review trigger` on the current head → address inline findings → push; re-review after fixes). No extra second-reviewer requirement (dual Claude+Codex retired 2026-10-16); reconcile reviewer disagreement against the code and prefer the deeper analysis.
- **CLI-first**: the CLI is the product contract for coding agents; every new capability lands as a CLI command first (with tests + real-TG manifest coverage) and updates `skills/tg-agent/SKILL.md` / `reference.md` when it changes the agent-facing surface. Legacy surfaces (see Positioning) are frozen — parity with them is no longer a requirement
- Async everywhere (asyncio)
- Pydantic v2 models (`model_validate`, not `parse_obj`)
- Config via `config.yaml` with `${ENV_VAR}` substitution
- Web auth (legacy web panel): HTTP Basic Auth (password only via `WEB_PASS`, username hardcoded as "admin")
- ruff for linting: line-length=120, target py311, rules E/F/I/N/W
- Tests: pytest-asyncio with `asyncio_mode="auto"`
- Session strings stored as `enc:v2:*` when `SESSION_ENCRYPTION_KEY` is set; startup fails fast if encrypted rows exist without key
- `SESSION_ENCRYPTION_KEY` resolution (#1699): config.yaml/env → opt-in OS secret store (`tg-agent key set`; macOS Keychain / Windows Credential Manager / secret-tool, 0600-file fallback in the global store root) → warning + degraded pool. Both former raw env read sites (config loader and `Database.__init__` default) route through the shared resolvers (`resolve_session_encryption_secret` / `resolve_session_secret_from_env_or_store`); `TG_AGENT_SECRET_STORE=off` disables the store layer (tests set it for hermeticity)

## CI

- GitHub Actions: `.github/workflows/ci.yml` — runs on push to `main` and on all PRs; branch names containing `+` are rejected. Split into **parallel jobs** (#1090, #1097 §5) that fan out for speed: `check-branch-name` (PR only), `lint` (ruff), `static-checks` (gates below), `tests`. This parallel split is the CI speedup that landed. Structure is regression-guarded by `tests/test_ci_workflow_structure.py`.
- **Dependency review gate (#1704)**: a `dependency-review` job (PR only) blocks PRs introducing a dependency — **direct or transitive** — with known CVE/malware advisories (`fail-on-severity: moderate`, blocking, unlike the advisory pip-audit). Raise the threshold to `high` if it gets noisy; never make it advisory.
- **Lockfile (#1706)**: the committed `uv.lock` (uv workspace, root + `packages/*`) is the full-closure dependency graph the review gate diffs against. Installs stay on pip; any PR touching dependencies must regenerate it (`uv lock`) — a stale lock reds `static-checks` (`uv lock --check`).
- **Test gate (#1090)**: the `tests` job runs the **full** suite on both PR and main — smoke first, then parallel (`-n auto -m "not aiosqlite_serial"`), then the serial lane (`-m aiosqlite_serial -n auto --dist=loadfile`), with coverage. (pytest-testmon selective runs were evaluated and dropped: testmon only deselects single-process and crashes under xdist+coverage, so a testmon PR gate would run single-process without coverage — often *slower* than the full `-n auto` sweep on this large suite, not faster. The parallel-job split is the real win.)
- Import architecture contracts: `lint-imports --config .importlinter` — CLI/web/agent-tools entrypoints must not import `telethon` directly (raw Telethon stays behind the telegram layer)
- CI enforces `filterwarnings = ["error"]` in `pyproject.toml` (dedicated check step in `static-checks`)
- Code-health tooling in `static-checks`: `scripts/code_health.py --fail-on F` (blocking cyclomatic-complexity gate, radon+vulture). Advisory scans (`continue-on-error`): jscpd duplication, `pip-audit` (dependency CVEs, #1053/#1097 §4), bandit. **Doc-coverage (interrogate, #1072) is NOT in CI** — it stays a local script (`scripts/doc_coverage.py`); "no docstring" ≠ "dead code (uncalled, vulture)" — independent signals, different PRs.
- Other workflows: `real-provider.yml` (opt-in live provider smoke), `docs.yml` (mkdocs → GitHub Pages), `release.yml`

## Real Telegram Testing

- Default rule: tests stay fake/harness-first; real Telegram is never the default path; live tests are opt-in via gate env vars only
- Real-CLI integration suite: `tests/cli_real_tg_integration/` (subdirs by category: `safe_ro`, `safe_write`, `mutation_safe`, `mutating`, `heavy`, `manual`, `dangerous`, …) with a `command_manifest.py`; every CLI leaf command must be either covered or manifested — invariants enforced by `test_real_telegram_policy.py`
- Any live Telegram pytest must use `real_telegram_sandbox` plus a `real_tg_*` marker
- Mutating flows such as BotFather, photo send, auth, and `leave_channels` must not be converted to generic live pytest cases

### pytest markers
- `aiosqlite_serial` — auto-applied by conftest; its lane runs `-n auto --dist=loadfile`, i.e. serially within each file (raw aiosqlite or `cli_db` fixture)
- `native_backend_allowed` — explicitly exercises native Telethon allowlist flows
- `real_tg_safe` — opt-in read-only sandbox; gate: `RUN_REAL_TELEGRAM_SAFE=1`
- `real_tg_mutation_safe` — opt-in bounded operator-approved mutations; gate: `RUN_REAL_TELEGRAM_MUTATION_SAFE=1`
- `real_tg_manual` — opt-in mutating sandbox, manual only; gate: `RUN_REAL_TELEGRAM_MANUAL=1`
- `real_tg_never` — must never request a real Telegram client
- `telegram_unit` — pure unit test, no transport wiring
- `real_materializer` — uses real `SessionMaterializer` instead of stub
- `real_provider_smoke` — opt-in live LLM/image provider API smoke
- `codex_image_live` / `codex_cli_live` — opt-in live Codex SDK/CLI tests; gates: `RUN_CODEX_IMAGE_LIVE=1` / `RUN_CODEX_CLI_LIVE=1`
- `e2e` — end-to-end browser test using Playwright

### Test levels (unit / integration / smoke / e2e)
A second classification axis layered on top of the markers above, **auto-applied** at collection time by `_infer_test_level` in root `conftest.py` — no manual annotation needed. Run a level with `pytest -m <level>`. An explicit hand-written level marker on a test/module always wins over the heuristic.
- `unit` — pure logic, **no DB at all**, no HTTP/file-IO/subprocess; fast. Default when nothing else matches.
- `integration` — exercises a real in-process subsystem: **any `Database` including `:memory:`** (fixtures `db`/`cli_db`, or file-backed DB built inline), the FastAPI app over ASGI (`build_web_app`/`build_web_container`/`ASGITransport`, fixtures `client`/`base_app`/…), repositories (`tests/repositories/`), routes (`tests/routes/`), or a real subprocess/session-file path.
- `smoke` — curated fast **offline** preflight over real critical paths (`tests/test_smoke_preflight.py` + `@pytest.mark.smoke` spots); orthogonal to level (a smoke test is usually also `integration`). Live API smoke (`real_provider_smoke`/`codex_*`) also carries `smoke` but stays opt-in (skipped without its gate). CI runs `pytest -m smoke` first as a sub-second sanity gate.
- `e2e` — long/full real-surface flows: `tests/e2e/` (Playwright) plus all `real_tg_*` live CLI suites.
- **Classification is per-test, not per-file**: a file may hold both `unit` (mock-only) and `integration` (DB-touching) tests — each gets its level from its own fixtures/body. Detection order (first match wins): e2e path/marker → live-smoke marker → `tests/routes`|`tests/repositories` path → integration fixture → file-DB (`aiosqlite_serial`) signal → per-test AST source scan (`_INTEGRATION_SOURCE_TOKENS`) → else `unit`. Adding a new way to stand up a subsystem inline may need a token added to `_INTEGRATION_SOURCE_TOKENS`.
- Invariant (enforced by `tests/test_test_levels.py`): every collected test carries exactly one level; `unit ∩ integration = ∅`; nothing is left unlevelled.
