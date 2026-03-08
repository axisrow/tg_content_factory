# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

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

python -m src.main channel list|add|delete|toggle|collect|stats|refresh-types|import
python -m src.main filter analyze|apply|reset|precheck
python -m src.main keyword list|add|delete|toggle
python -m src.main account list|toggle|delete
python -m src.main scheduler start|trigger|search
python -m src.main notification setup|status|delete
```

## Architecture

Three layers: **CLI/Web** → **Telegram + Search + Scheduler** → **SQLite**

- CLI (`src/main.py`) and Web (`src/web/`) are parallel entry points to the same logic
- Telegram layer: `ClientPool` manages multi-account connections, `Collector` fetches messages, `Notifier` sends alerts
- Search layer: `SearchEngine` (local DB), `AISearchEngine` (LLM-powered)
- Scheduler: APScheduler wrapper (`src/scheduler/manager.py`) triggers periodic collection
- DB: single SQLite file via aiosqlite (`src/database.py`), schema auto-created on init
- Filters: `ChannelAnalyzer` (`src/filters/analyzer.py`) scores channels by uniqueness, subscriber ratio, cross-channel spam, language; thresholds in `src/filters/criteria.py`
- Collection service (`src/services/collection_service.py`): orchestration layer between web/CLI and Collector/Queue — handles enqueue logic, stats collection
- Parsers (`src/parsers.py`): identifier extraction for channel import — t.me links, @usernames, negative IDs; file parsing (txt/csv/xlsx)
- Notification bot: personal bot created via BotFather through a connected account (`src/telegram/notifier.py`)

## Key Patterns

- **Entity cache**: `collect_all_channels()` calls `client.get_dialogs()` inline before iterating channels — StringSession loses entity cache between restarts, so this is required for PeerChannel lookups
- **Flood wait rotation**: `ClientPool.get_available_client()` skips accounts where `flood_wait_until` is in the future; falls back if all clients are in-use
- **Config key dropping**: `_walk_and_substitute` in config.py — if a YAML value is purely `${ENV_VAR}` and that var is empty/absent, the key is dropped entirely (not set to "")
- **Incremental collection**: `min_id = channel.last_collected_id`, `reverse=True`; after the loop, `last_collected_id` is updated to `max(seen message_ids)`
- **Batch insert**: `INSERT OR IGNORE` + `UNIQUE(channel_id, message_id)` — duplicates silently skipped
- **Cancellation**: `Collector._cancel_event` is an `asyncio.Event`, checked every 10 messages in the iter loop and at each channel boundary
- **Session tokens**: custom HMAC-SHA256 signed tokens in `src/web/session.py` — payload is `{user, exp}`, secret persisted in DB settings table, cookie max-age 30 days (`Secure` on HTTPS)
- **CollectionQueue** (`src/collection_queue.py`): `asyncio.Queue` + single worker task, task status (`pending/running/completed/failed/cancelled`) tracked in DB
- **DB migrations**: `_migrate()` in database.py uses `PRAGMA table_info` to detect missing columns and issues `ALTER TABLE ADD COLUMN` as needed
- **Keyword matching**: plain text (case-insensitive substring) and regex (`re.IGNORECASE`)
- **Channel filters**: `ChannelAnalyzer` checks `low_uniqueness`, `low_subscriber_ratio`, `cross_channel_spam`, `non_cyrillic`, `chat_noise`; filtered channels skipped during collection unless `force=True`
- **Collection service**: `enqueue_channel_by_pk(pk, force)` respects `is_filtered` flag; `enqueue_all_channels()` uses `full=False` for incremental collection
- **HTMX progressive enhancement**: collect routes check `HX-Request` header to return HTML fragments vs redirects
- **Identifier parsing**: `parse_identifiers()` splits text by comma/semicolon/newline; `extract_identifiers()` regex-extracts t.me links, @usernames, negative IDs; `parse_file()` handles txt/csv/xlsx
- **aiosqlite connection cleanup**: in tests using raw `aiosqlite.connect()`, always wrap in `try/finally` with `await conn.close()` — an unclosed worker-thread blocks pytest process exit
- **SQL in triple-quoted strings**: Python does NOT concatenate adjacent string literals inside `"""..."""`; for values with quotes use parameterized `execute()` with `?`-placeholders, not inline values in `executescript()`
- **pytest-timeout**: global 30s timeout configured in `pyproject.toml` (`timeout = 30`), dependency `pytest-timeout` in `[dev]`

## Conventions

- CLI/Web parity: every web operation must have a CLI equivalent and vice versa
- Async everywhere (asyncio)
- Pydantic v2 models (`model_validate`, not `parse_obj`)
- Config via `config.yaml` with `${ENV_VAR}` substitution
- Web auth: HTTP Basic Auth (password only via `WEB_PASS`, username hardcoded as "admin")
- ruff for linting: line-length=100, target py311, rules E/F/I/N/W
- Tests: pytest-asyncio with `asyncio_mode="auto"`
- Session strings stored as `enc:v2:*` when `SESSION_ENCRYPTION_KEY` is set; legacy `enc:v1:*` auto-migrated; startup fails fast if encrypted rows exist without key
