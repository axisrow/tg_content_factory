# Core

Telegram content factory: collects messages from Telegram channels, filters/scores them, searches (local + LLM), and runs AI content pipelines (generate → image → draft → publish). CLI + Web are parallel entrypoints to the same services.

## Source map

- `src/main.py` — single entrypoint; `python -m src.main <command>`. CLI commands in `src/cli/commands/`.
- `src/web/` — FastAPI app. Wiring in `assembly.py` (routes) + `container.py` (`AppContainer` → `app.state`); access via `src/web/deps.py`. Bootstrap/runtime split in `bootstrap.py`, `embedded_worker.py`.
- `src/telegram/` — `ClientPool` (multi-account), `Collector`, `Notifier`. Raw Telethon lives ONLY here (import contract).
- `src/database/` — SQLite via aiosqlite; `schema.py`, `migrations.py`, `connection.py`. Repos under `db.repos.<name>`.
- `src/services/` — orchestration: `collection_service`, `unified_dispatcher`, pipeline/content/image/photo services, `s3_store`.
- `src/agent/` — agent backends (`manager.py`) + modular tools in `src/agent/tools/`.
- `src/filters/` — `ChannelAnalyzer` (`analyzer.py`) + thresholds (`criteria.py`).
- `src/scheduler/manager.py` — APScheduler wrapper. `src/search/` — `SearchEngine` + `AISearchEngine`.

## Project-wide invariants

- **Runtime split (web ↔ worker)**: two `AppContainer` flavours keyed on `runtime_mode`. `serve` embeds the worker as an asyncio task by default; `--no-worker` + standalone `worker` for split deploys. Web container uses snapshot shims (`src/web/runtime_shims.py`), opens NO Telegram connections, enqueues work into DB tables; worker owns live `ClientPool`/queues and publishes `runtime_snapshots`. See `mem:tech_stack` for the stack.
- **DB write lock (issue #569)**: shared single aiosqlite connection (autocommit+WAL). ALL writes go through `db.transaction()` (multi-stmt, BEGIN IMMEDIATE) or `db.execute_write()`/`executemany_write()` (single-stmt). Reads stay lock-free. Direct `await conn.commit()` in a repo is a regression.
- **JOIN on channels**: always `ON x.channel_id = c.channel_id`. `channels.id` is the DB pk, used ONLY in pk-lookups; sidecar tables store the Telegram `channel_id`. Joining on `c.id` silently returns zero rows. Enforced by `tests/test_sql_conventions.py`.
- **CLI/Web parity**: every web op has a CLI equivalent and vice versa (reviewer-enforced).
- **Warnings are errors**: `filterwarnings = ["error"]`; CI fails if relaxed — fix, don't suppress.
- Conventions, commands, and task-completion steps live in `mem:conventions`, `mem:suggested_commands`, `mem:task_completion`.
