# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
# Install dev dependencies
pip install -e ".[dev]"

# Run the web server
python -m src.main serve [--web-pass PASS]

# Lint
ruff check src/ tests/ conftest.py

# Auto-fix lint issues
ruff check --fix src/ tests/ conftest.py

# Run parallel-safe tests (all available CPUs minus one worker)
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
python -m src.main [--config CONFIG] collect [--channel-id ID]
python -m src.main [--config CONFIG] search "query" [--limit N] [--mode MODE]

python -m src.main channel list|add|delete|toggle|collect|stats|refresh-types|import
python -m src.main filter analyze|apply|reset|precheck
python -m src.main keyword list|add|delete|toggle
python -m src.main account list|toggle|delete
python -m src.main scheduler start|trigger|search
python -m src.main notification setup|status|delete
python -m src.main test all|read|write|telegram|benchmark

python -m src.main stop|restart
python -m src.main search-query list|add|delete|toggle
python -m src.main pipeline list|add|delete|run|runs
python -m src.main photo-loader list|upload|schedule|cancel
python -m src.main my-telegram leave|export
python -m src.main agent chat
python -m src.main analytics summary|export
```

## Architecture

Three layers: **CLI/Web** → **Telegram + Search + Scheduler + Agent/Pipeline** → **SQLite**

- CLI (`src/main.py` → `src/cli/commands/`) and Web (`src/web/`) are parallel entry points to the same logic
- Telegram layer: `ClientPool` manages multi-account connections, `Collector` fetches messages, `Notifier` sends alerts
- Search layer: `SearchEngine` (local DB), `AISearchEngine` (LLM-powered)
- Scheduler: APScheduler wrapper (`src/scheduler/manager.py`) triggers periodic collection
- DB: single SQLite file via aiosqlite; schema in `src/database/schema.py`, migrations in `src/database/migrations.py`, connection in `src/database/connection.py`
- Filters: `ChannelAnalyzer` (`src/filters/analyzer.py`) scores channels by uniqueness, subscriber ratio, cross-channel spam, language; thresholds in `src/filters/criteria.py`
- Collection service (`src/services/collection_service.py`): orchestration layer between web/CLI and Collector/Queue — handles enqueue logic, stats collection
- Parsers (`src/parsers.py`): identifier extraction for channel import — t.me links, @usernames, negative IDs; file parsing (txt/csv/xlsx)
- Notification bot: personal bot created via BotFather through a connected account (`src/telegram/notifier.py`)
- **Agent system**: `AgentProviderService` selects backend — `claude-agent-sdk` when `ANTHROPIC_API_KEY` / `CLAUDE_CODE_OAUTH_TOKEN` is set, otherwise falls back to `deepagents` with provider adapters; developer override available in Settings UI
- **Provider system**: `ProviderService` auto-registers LLM providers from env vars (`OPENAI_API_KEY`, `COHERE_API_KEY`, `OLLAMA_BASE`, etc.); provider adapters in `src/services/provider_adapters.py` are lightweight HTTP wrappers (no heavy SDK deps)
- **Content pipelines**: `PipelineService` + `ContentGenerationService` orchestrate generate → draft → notify → publish flow; tracked via `generation_runs` DB table
- **Photo publishing**: `PhotoAutoUploadService` / `PhotoPublishService` / `PhotoTaskService` — separate upload, schedule, publish tasks tracked in DB
- **LangChain integration**: optional, lazy-loaded via `src/services/langchain_adapters.py`; enabled with `USE_LANGCHAIN=1`

### Database access pattern

Repositories are accessed via `db.repos.<repo_name>.<method>()`. The `Database` facade exposes a `repos` bundle:

```python
db.repos.channels.get_all()
db.repos.generation_runs.list_pending_moderation(pipeline_id=1)
db.repos.settings.get("key")
```

Each repository has a `_to_<model>(row)` static helper that maps `aiosqlite.Row` → Pydantic model, including safe `.keys()` checks for nullable/optional columns added by migrations.

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
- **Collection service**: `enqueue_channel_by_pk(pk, force)` respects `is_filtered` flag; `enqueue_all_channels()` uses `full=False` for incremental collection
- **HTMX progressive enhancement**: collect routes check `HX-Request` header to return HTML fragments vs redirects
- **Identifier parsing**: `parse_identifiers()` splits text by comma/semicolon/newline; `extract_identifiers()` regex-extracts t.me links, @usernames, negative IDs; `parse_file()` handles txt/csv/xlsx
- **aiosqlite connection cleanup**: in tests using raw `aiosqlite.connect()`, always wrap in `try/finally` with `await conn.close()` — an unclosed worker-thread blocks pytest process exit
- **SQL in triple-quoted strings**: Python does NOT concatenate adjacent string literals inside `"""..."""`; for values with quotes use parameterized `execute()` with `?`-placeholders, not inline values in `executescript()`
- **pytest-timeout**: global 30s timeout configured in `pyproject.toml` (`timeout = 30`), dependency `pytest-timeout` in `[dev]`
- **Test parallelism split**: root `conftest.py` auto-marks tests as `aiosqlite_serial` if they use the `cli_db` fixture or contain `import aiosqlite` (raw aiosqlite calls); everything else runs with `-n auto`
- **`db` fixture is `:memory:`**: `tests/conftest.py` provides `db` as `Database(":memory:")`. The `real_pool_harness_factory` fixture depends on `db` and passes it to the harness. Any fixture/test that creates a web app and calls `real_pool_harness_factory()` **must** accept `db` as a parameter and use it for `app.state.db` — creating a separate `Database(tmp_path / "test.db")` would give the app and the harness different DB instances, breaking account lookups.

## Conventions

- CLI/Web parity: every web operation must have a CLI equivalent and vice versa
- Async everywhere (asyncio)
- Pydantic v2 models (`model_validate`, not `parse_obj`)
- Config via `config.yaml` with `${ENV_VAR}` substitution
- Web auth: HTTP Basic Auth (password only via `WEB_PASS`, username hardcoded as "admin")
- ruff for linting: line-length=120, target py311, rules E/F/I/N/W
- Tests: pytest-asyncio with `asyncio_mode="auto"`
- Session strings stored as `enc:v2:*` when `SESSION_ENCRYPTION_KEY` is set; legacy `enc:v1:*` auto-migrated; startup fails fast if encrypted rows exist without key

## Real Telegram Testing

- Default rule: tests stay fake/harness-first; real Telegram is never the default path
- Policy document: `docs/testing/real-telegram.md`
- Any live Telegram pytest must use `real_telegram_sandbox` plus `@pytest.mark.real_tg_safe` or `@pytest.mark.real_tg_manual`
- Mutating flows such as BotFather, photo send, auth, and `leave_channels` must not be converted to generic live pytest cases

### pytest markers
- `aiosqlite_serial` — auto-applied by conftest; runs serially (raw aiosqlite or `cli_db` fixture)
- `native_backend_allowed` — explicitly exercises native Telethon allowlist flows
- `real_tg_safe` — opt-in read-only sandbox; gate: `RUN_REAL_TELEGRAM_SAFE=1`
- `real_tg_manual` — opt-in mutating sandbox, manual only; gate: `RUN_REAL_TELEGRAM_MANUAL=1`
- `real_tg_never` — must never request a real Telegram client
- `telegram_unit` — pure unit test, no transport wiring
- `real_materializer` — uses real `SessionMaterializer` instead of stub
