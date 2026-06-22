# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Install dev dependencies
pip install -e ".[dev]"

# Run the web server — spawns the embedded Telegram worker by default so a
# single command gives you UI + actual collection (#457 round 4). For split
# deployments (Docker/k8s) pass --no-worker and run `worker` separately.
python -m src.main serve [--web-pass PASS] [--no-worker]

# Standalone Telegram worker — only needed alongside `serve --no-worker`.
python -m src.main worker

# Lint
ruff check src/ tests/ conftest.py

# Auto-fix lint issues
ruff check --fix src/ tests/ conftest.py

# Run parallel-safe tests (-n auto is capped at 4 workers by root conftest.py;
# override via TGCF_PYTEST_XDIST_WORKERS; single-test runs with :: force 1 worker)
pytest tests/ -v -m "not aiosqlite_serial" -n auto

# Run aiosqlite-backed tests serially
pytest tests/ -v -m aiosqlite_serial

# Run a single test
pytest tests/test_web.py::test_health_endpoint -v

# Benchmark serial vs safe mixed-mode suite execution
python -m src.main test benchmark
```

Full CLI reference:

```bash
python -m src.main [--config CONFIG] serve [--web-pass PASS]
python -m src.main [--config CONFIG] worker
python -m src.main [--config CONFIG] collect [--channel-id ID] | collect sample
python -m src.main [--config CONFIG] search "query" [--limit N] [--mode MODE]

python -m src.main messages read <identifier> [--limit N] [--live] [--phone PHONE] [--query TEXT] [--date-from DATE] [--date-to DATE] [--topic-id ID] [--offset-id ID] [--format text|json|csv]
python -m src.main channel list|add|delete|toggle|collect|stats|refresh-types|refresh-meta|import|add-bulk|list-for-import|tag (tag list|add|delete|set|get)
python -m src.main filter analyze|apply|reset|precheck|toggle|purge|purge-messages|hard-delete
python -m src.main account list|info|toggle|set-primary|delete|send-code|verify-code|add|flood-status|flood-clear
python -m src.main scheduler start|trigger|status|stop|job-toggle|set-interval|task-cancel|clear-pending|queue-pause|queue-resume
python -m src.main notification setup|status|delete|test|dry-run|set-account
python -m src.main test all|read|write|telegram|benchmark

python -m src.main stop|restart
python -m src.main search-query list|get|add|edit|delete|toggle|run|stats
python -m src.main pipeline list|show|add|dry-run-count|edit|delete|toggle|run|generate|generate-stream|runs|run-show|queue|moderation-list|moderation-view|publish|approve|reject|bulk-approve|bulk-reject|refinement-steps|export|import|templates|from-template|ai-edit|filter|node|edge|graph
python -m src.main photo-loader dialogs|refresh|send|schedule-send|batch-create|batch-list|items|batch-cancel|auto-create|auto-list|auto-update|auto-toggle|auto-delete|run-due
python -m src.main dialogs list|refresh|resolve|leave|join|topics|cache-clear|cache-status|send|forward|edit-message|delete-message|create-channel|create-group|pin-message|react|unpin-message|download-media|participants|edit-admin|edit-permissions|kick|broadcast-stats|archive|unarchive|mark-read|queue|status|cancel|clear-pending
python -m src.main agent threads|thread-create|thread-delete|chat|thread-rename|thread-stop|messages|context|test-escaping|test-tools
python -m src.main analytics top|content-types|hourly|summary|daily|pipeline-stats|trending-topics|trending-channels|velocity|peak-hours|calendar|trending-emojis|channel
python -m src.main provider list|add|delete|probe|refresh|test-all
python -m src.main export json|csv|rss
python -m src.main translate stats|detect|run|message
python -m src.main settings get|set|info|server-time|agent|filter-criteria|reactions|semantic
python -m src.main debug logs|memory|timing
python -m src.main image generate|models|providers|generated
python -m src.main mcp-server [--no-pool]   # expose agent tool registry as stdio MCP server (for external agents like Codex)
```

CLI command for Telegram dialogs management is `dialogs`.

## Architecture

Three layers: **CLI/Web** → **Telegram + Search + Scheduler + Agent/Pipeline** → **SQLite**

- **Runtime split (web ↔ worker)**: since #444 the runtime consists of two `AppContainer` flavours keyed on `runtime_mode` ("web" vs "worker") in `src/web/bootstrap.py`. Since #457 round 4 they normally run in the **same process**: `serve` spawns an `EmbeddedWorker` (`src/web/embedded_worker.py`) as an asyncio task next to the web container. Pass `--no-worker` to run only the web side and start `python -m src.main worker` separately (Docker/k8s split deployments).
  - Web container (`runtime_mode="web"`) uses snapshot shims (`SnapshotClientPool`, `SnapshotCollector`, `SnapshotSchedulerManager` in `src/web/runtime_shims.py`). It does NOT open Telegram connections; UI actions enqueue work into `collection_tasks` / `telegram_commands` / task tables and read `runtime_snapshots` to render status.
  - Worker container (`runtime_mode="worker"`, either embedded or standalone via `src/runtime/worker.py`) owns the live `ClientPool`, `CollectionQueue`, `UnifiedDispatcher`, `TelegramCommandDispatcher`, and `SchedulerManager`, and publishes `runtime_snapshots` (heartbeat, accounts_status, scheduler_status, …) that the web side reads.
  - In web-mode `collection_queue = None` and `CollectionService` falls back to writing a PENDING row — the worker picks those up at startup via `CollectionQueue.requeue_startup_tasks()`.
- CLI (`src/main.py` → `src/cli/commands/`) and Web (`src/web/`) are parallel entry points to the same logic
- Telegram layer: `ClientPool` manages multi-account connections, `Collector` fetches messages, `Notifier` sends alerts
- Search layer: `SearchEngine` (local DB), `AISearchEngine` (LLM-powered)
- Scheduler: APScheduler wrapper (`src/scheduler/manager.py`) triggers periodic collection
- DB: single SQLite file via aiosqlite; schema in `src/database/schema.py`, migrations in `src/database/migrations.py`, connection in `src/database/connection.py`
- Filters: `ChannelAnalyzer` (`src/filters/analyzer.py`) scores channels by uniqueness, subscriber ratio, cross-channel spam, language; thresholds in `src/filters/criteria.py`
- Collection service (`src/services/collection_service.py`): orchestration layer between web/CLI and Collector/Queue — handles enqueue logic, stats collection
- Parsers (`src/parsers.py`): identifier extraction for channel import — t.me links, @usernames, negative IDs; file parsing (txt/csv/xlsx)
- Notification bot: personal bot created via BotFather through a connected account (`src/telegram/notifier.py`)
- **Agent system**: four backends selected in `AgentManager.get_runtime_status()` (`src/agent/manager.py`) — auto-selection prefers `deepagents` when usable DB provider configs exist, then `claude-agent-sdk` (needs `ANTHROPIC_API_KEY` / `CLAUDE_CODE_OAUTH_TOKEN`), then `deepagents` again; the Codex SDK backend (`src/agent/codex_backend.py`) and the Google ADK backend (`src/agent/adk_backend.py`, Gemini models, needs `GOOGLE_API_KEY` / `GEMINI_API_KEY`) are intentionally NOT in the auto-fallback chain (each spawns a blocking out-of-process `mcp-server` subprocess) and are opt-in only via the dev-mode `agent_backend_override` setting. Both Codex and ADK reach the project tools over the same stdio MCP server (`python -m src.main mcp-server`) the in-process path serves
- **Provider system**: `ProviderService` auto-registers LLM providers from env vars (`OPENAI_API_KEY`, `COHERE_API_KEY`, `OLLAMA_BASE`, etc.); provider adapters in `src/services/provider_adapters.py` are lightweight HTTP wrappers (no heavy SDK deps)
- **Content pipelines**: `PipelineService` + `ContentGenerationService` orchestrate generate → image → draft → notify → publish flow; tracked via `generation_runs` DB table
- **Image generation**: `ImageGenerationService` routes to provider-specific HTTP adapters (Together/HuggingFace/OpenAI/Replicate) via `provider:model_id` convention; auto-registers from env vars; adapters defined in `src/services/provider_adapters.py`
- **UnifiedDispatcher** (`src/services/unified_dispatcher.py`): polls DB for generic tasks (CONTENT_GENERATE, CONTENT_PUBLISH, PIPELINE_RUN, PHOTO_DUE, etc.) and dispatches to handler methods; recovers interrupted tasks on startup
- **Photo publishing**: `PhotoAutoUploadService` / `PhotoPublishService` / `PhotoTaskService` — separate upload, schedule, publish tasks tracked in DB
- **Agent tools**: `src/agent/tools/` — modular tool files (channels, search, pipelines, images, etc.); `_registry.py` provides `require_confirmation()` gate for destructive ops and `normalize_phone()`; `react_agent.py` is ReAct fallback when claude-agent-sdk is unavailable; `manager.py` orchestrates backend selection
- **Agent tool permissions**: `TOOL_CATEGORIES` in `src/agent/tools/permissions.py` classifies every tool as read/write/delete; per-phone ACL overrides stored in DB setting `agent_tool_permissions` (JSON); `get_all_allowed_tools()` derives MCP-prefixed allow-list
- **S3 storage**: `src/services/s3_store.py` — optional S3-compatible backend for media (images); used by `ImageGenerationService` when S3 config is present

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

- `src/web/assembly.py` — `register_routes()` imports and mounts all routers; `configure_app()` binds the `AppContainer` to `app.state.*`
- `src/web/container.py` — `AppContainer` dataclass aggregates all services; injected into FastAPI `app.state` at startup; accessed in routes via `src/web/deps.py` helpers (`deps.get_db()`, `deps.get_templates()`, etc.)

## Key Patterns

- **Entity cache**: `collect_all_channels()` calls `client.get_dialogs()` inline before iterating channels — StringSession loses entity cache between restarts, so this is required for PeerChannel lookups
- **Flood wait rotation**: `ClientPool.get_available_client()` skips accounts where `flood_wait_until` is in the future; falls back if all clients are in-use
- **Config key dropping**: `_walk_and_substitute` in config.py — if a YAML value is purely `${ENV_VAR}` and that var is empty/absent, the key is dropped entirely (not set to "")
- **Incremental collection**: `min_id = channel.last_collected_id`, `reverse=True`; after the loop, `last_collected_id` is updated to `max(seen message_ids)`
- **Batch insert**: `INSERT OR IGNORE` + `UNIQUE(channel_id, message_id)` — duplicates silently skipped
- **Cancellation**: `Collector._cancel_event` is an `asyncio.Event`, checked every 10 messages in the iter loop and at each channel boundary
- **Session tokens**: custom HMAC-SHA256 signed tokens in `src/web/session.py` — payload is `{user, exp}`, secret persisted in DB settings table, cookie max-age 30 days (`Secure` on HTTPS)
- **CollectionQueue** (`src/collection_queue.py`): `asyncio.Queue` + single worker task, task status (`pending/running/completed/failed/cancelled`) tracked in DB
- **DB migrations**: `_migrate()` in `src/database/migrations.py` uses `PRAGMA table_info` to detect missing columns and issues `ALTER TABLE ADD COLUMN` as needed
- **Keyword matching**: plain text (case-insensitive substring) and regex (`re.IGNORECASE`)
- **Channel filters**: `ChannelAnalyzer` checks `low_uniqueness`, `low_subscriber_ratio`, `cross_channel_spam`, `non_cyrillic`, `chat_noise`; filtered channels skipped during collection unless `force=True`
- **Channel creation date**: `channels.created_at` stores Telegram `entity.date` (channel creation timestamp); captured via `resolve_channel()` and `get_dialogs_for_phone()`; distinct from `added_at` (when added to the system)
- **Collection service**: `enqueue_channel_by_pk(pk, force)` respects `is_filtered` flag; `enqueue_all_channels()` uses `full=False` for incremental collection
- **Hour x Weekday Heatmap**: `db.repos.messages.get_hour_weekday_heatmap(channel_id, days)` → `[{hour, weekday, count}]`; weekday 0=Sunday per SQLite `%w`; service wrapper: `ChannelAnalyticsService.get_heatmap()`; CLI: `analytics channel`; Web: `/analytics/channels/api/heatmap`
- **Cross-Channel Citations**: `db.repos.messages.get_cross_channel_citations(channel_id, days)` → sources by `forward_from_channel_id` with JOIN to channels for title/username enrichment; stored as positive MTProto channel ID
- **Image adapter convention**: `ImageAdapter = Callable[[str, str], Awaitable[str | None]]` — signature is `(prompt, model_id) → URL/path`; model string format `provider:model_id` (e.g. `together:black-forest-labs/FLUX.1-schnell`); without prefix falls back to first registered adapter
- **Content pipeline flow**: CONTENT_GENERATE task → `ContentGenerationService.generate()` → LLM text → optional image → `generation_runs` record → if AUTO publish mode, enqueues CONTENT_PUBLISH → `PublishService.publish_run()` sends to target channel
- **Destructive tool confirmation**: agent tools that delete data require `confirm=true` argument; `require_confirmation()` in `_registry.py` returns a warning response if not confirmed
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
- **Test parallelism split**: root `conftest.py` auto-marks tests as `aiosqlite_serial` if they use the `cli_db` fixture or contain `import aiosqlite` (raw aiosqlite calls); everything else runs with `-n auto` (`--dist=loadfile`, worker count capped at 4 by default, `TGCF_PYTEST_XDIST_WORKERS` to override; `::`-style single-test invocations and real-TG gate envs force 1 worker). Locally `-n auto` is load-aware (reserves a core, subtracts the load average) so a busy laptop isn't hogged; on CI (the `CI` env var is set) that throttle is disabled and every runner core is used (#944), so `-n auto` reaches the full cap
- **`db` fixture is `:memory:`**: `tests/conftest.py` provides `db` as `Database(":memory:")`. The `real_pool_harness_factory` fixture depends on `db` and passes it to the harness. Any fixture/test that creates a web app and calls `real_pool_harness_factory()` **must** accept `db` as a parameter and use it for `app.state.db` — creating a separate `Database(tmp_path / "test.db")` would give the app and the harness different DB instances, breaking account lookups.

## Conventions

- CLI/Web parity: every web operation must have a CLI equivalent and vice versa
- Async everywhere (asyncio)
- Pydantic v2 models (`model_validate`, not `parse_obj`)
- Config via `config.yaml` with `${ENV_VAR}` substitution
- Web auth: HTTP Basic Auth (password only via `WEB_PASS`, username hardcoded as "admin")
- ruff for linting: line-length=120, target py311, rules E/F/I/N/W
- Tests: pytest-asyncio with `asyncio_mode="auto"`
- Session strings stored as `enc:v2:*` when `SESSION_ENCRYPTION_KEY` is set; startup fails fast if encrypted rows exist without key

## CI

- GitHub Actions: `.github/workflows/ci.yml` — ruff lint + pytest on push to `main`/`fix/**`/`codex/**` and all PRs; branch names containing `+` are rejected
- Import architecture contracts: `lint-imports --config .importlinter` — CLI/web/agent-tools entrypoints must not import `telethon` directly (raw Telethon stays behind the telegram layer)
- CI enforces `filterwarnings = ["error"]` in `pyproject.toml` (dedicated check step), then runs parallel tests (`-n auto -m "not aiosqlite_serial"`), then serial (`-m aiosqlite_serial`)
- Other workflows: `real-provider.yml` (opt-in live provider smoke), `docs.yml` (mkdocs → GitHub Pages), `release.yml`

## Real Telegram Testing

- Default rule: tests stay fake/harness-first; real Telegram is never the default path; live tests are opt-in via gate env vars only
- Real-CLI integration suite: `tests/cli_real_tg_integration/` (subdirs by category: `safe_ro`, `safe_write`, `mutation_safe`, `mutating`, `heavy`, `manual`, `dangerous`, …) with a `command_manifest.py`; every CLI leaf command must be either covered or manifested — invariants enforced by `test_real_telegram_policy.py`
- Any live Telegram pytest must use `real_telegram_sandbox` plus a `real_tg_*` marker
- Mutating flows such as BotFather, photo send, auth, and `leave_channels` must not be converted to generic live pytest cases

### pytest markers
- `aiosqlite_serial` — auto-applied by conftest; runs serially (raw aiosqlite or `cli_db` fixture)
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
