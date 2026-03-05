# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with this codebase.

## Commands

```bash
# Install dev dependencies
pip install -e ".[dev]"

# Run the web server
python -m src.main serve [--web-pass PASS]

# Lint
ruff check src/ tests/

# Run all tests
pytest tests/ -v

# Run a single test
pytest tests/test_web.py::test_health_endpoint -v
```

Full CLI reference:

```bash
python -m src.main [--config CONFIG] serve [--web-pass PASS]
python -m src.main [--config CONFIG] collect [--channel-id ID]
python -m src.main [--config CONFIG] search "query" [--limit N] [--mode MODE]

python -m src.main channel list|add|delete|toggle|collect
python -m src.main keyword list|add|delete|toggle
python -m src.main account list|toggle|delete
python -m src.main scheduler start|trigger
```

## Architecture

Three layers: **CLI/Web** → **Telegram + Search + Scheduler** → **SQLite**

- CLI (`src/main.py`) and Web (`src/web/`) are parallel entry points to the same logic
- Telegram layer: `ClientPool` manages multi-account connections, `Collector` fetches messages, `Notifier` sends alerts
- Search layer: `SearchEngine` (local DB), `AISearchEngine` (LLM-powered)
- Scheduler: APScheduler wrapper (`src/scheduler/manager.py`) triggers periodic collection
- DB: single SQLite file via aiosqlite (`src/database.py`), schema auto-created on init

## Key Patterns

- **Entity cache**: `collect_all_channels()` calls `client.get_dialogs()` inline before iterating channels — StringSession loses entity cache between restarts, so this is required for PeerChannel lookups
- **Flood wait rotation**: `ClientPool.get_available_client()` skips accounts where `flood_wait_until` is in the future; falls back if all clients are in-use
- **Config key dropping**: `_walk_and_substitute` in config.py — if a YAML value is purely `${ENV_VAR}` and that var is empty/absent, the key is dropped entirely (not set to "")
- **Incremental collection**: `min_id = channel.last_collected_id`, `reverse=True`; after the loop, `last_collected_id` is updated to `max(seen message_ids)`
- **Batch insert**: `INSERT OR IGNORE` + `UNIQUE(channel_id, message_id)` — duplicates silently skipped
- **Cancellation**: `Collector._cancel_event` is an `asyncio.Event`, checked every 10 messages in the iter loop and at each channel boundary
- **Session tokens**: custom HMAC-SHA256 signed tokens in `src/web/session.py` — payload is `{user, exp}`, secret persisted in DB settings table, cookie max-age 30 days
- **CollectionQueue** (`src/collection_queue.py`): `asyncio.Queue` + single worker task, task status (`pending/running/completed/failed/cancelled`) tracked in DB
- **DB migrations**: `_migrate()` in database.py uses `PRAGMA table_info` to detect missing columns and issues `ALTER TABLE ADD COLUMN` as needed
- **Keyword matching**: plain text (case-insensitive substring) and regex (`re.IGNORECASE`)

## Conventions

- CLI/Web parity: every web operation must have a CLI equivalent and vice versa
- Async everywhere (asyncio)
- Pydantic v2 models (`model_validate`, not `parse_obj`)
- Config via `config.yaml` with `${ENV_VAR}` substitution
- Web auth: HTTP Basic Auth (password only via `WEB_PASS`, username hardcoded as "admin")
- ruff for linting: line-length=100, target py311, rules E/F/I/N/W
- Tests: pytest-asyncio with `asyncio_mode="auto"`
- Session strings stored encrypted in DB (cryptography package)
