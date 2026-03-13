# Copilot instructions

## Build, test, and lint commands

- Install dev dependencies: `pip install -e ".[dev]"`
- Lint: `ruff check src/ tests/ conftest.py`
- Run parallel-safe tests: `pytest tests/ -v -m "not aiosqlite_serial" -n auto`
- Run `aiosqlite`-backed tests serially: `pytest tests/ -v -m aiosqlite_serial`
- Run a single test: `pytest tests/test_web.py::test_health_endpoint -v`
- Benchmark serial vs safe mixed-mode suite execution: `python -m src.main test benchmark`
- Run the web app locally: `python -m src.main serve [--web-pass PASS]`

## High-level architecture

This project has three main layers: CLI/Web entry points, Telegram/search/scheduler services, and a single SQLite database.

- `src/main.py` is the main CLI entry point. CLI commands live under `src/cli/commands/`.
- `src/web/` is a FastAPI web UI over the same underlying logic. Keep CLI and web behavior aligned when changing features.
- The Telegram layer centers on `ClientPool` for multi-account rotation, `Collector` for message collection, and `Notifier` for Telegram alerts.
- Collection is orchestrated through `src/services/collection_service.py` and `src/collection_queue.py`: requests are queued, persisted as collection tasks, and processed by a single async worker.
- Search has multiple backends: local SQLite/FTS search, direct Telegram search, and AI-backed search/agent flows.
- Storage is a single SQLite database managed asynchronously with `aiosqlite`; schema creation and additive migrations happen in code.

## Key conventions

- Preserve CLI/web parity: features exposed in the web UI should have corresponding CLI behavior and reuse shared logic.
- This codebase is async-first. Prefer `asyncio` patterns and existing async helpers over introducing sync shortcuts.
- Use Pydantic v2 APIs such as `model_validate`, not deprecated v1 patterns.
- Config values support `${ENV_VAR}` substitution, and keys whose value is only `${ENV_VAR}` are dropped entirely if the env var is empty or missing.
- Incremental collection matters: collection resumes from `last_collected_id` instead of reloading full history unless a full run is explicitly requested.
- `ClientPool` rotates around flood waits. Reuse its account-selection logic instead of bypassing it.
- Collection tasks are persisted and processed sequentially through `CollectionQueue`; keep task status transitions and cancellation behavior intact.
- Filtered channels are normally skipped during collection unless the flow explicitly uses `force=True`.
- The collector depends on Telegram entity cache warmup before some channel operations. Do not remove cache-priming behavior such as dialog loading without verifying PeerChannel resolution still works.
- Duplicate messages are tolerated through `INSERT OR IGNORE` with unique constraints; avoid replacing that behavior with manual dedup flows.
- Web auth is password-based and session tokens are custom HMAC-signed cookies backed by a secret stored in the database.
- When `SESSION_ENCRYPTION_KEY` is configured, session strings are stored encrypted (`enc:v2:*`); startup should fail fast if encrypted rows exist but the key is missing.
- Web routes use HTMX progressive enhancement patterns. When a route already branches on `HX-Request`, preserve fragment-vs-redirect behavior.
- Identifier import flows accept `t.me` links, `@usernames`, negative IDs, and text/file parsing through existing parser helpers; reuse those helpers instead of duplicating parsing logic.
- Real Telegram tests are opt-in only. Default tests should stay fake/harness-based. Any live pytest must use `real_telegram_sandbox` plus `@pytest.mark.real_tg_safe` or `@pytest.mark.real_tg_manual`.
- Parallel pytest runs use `pytest-xdist`, and the repository-level worker hook uses `joblib.cpu_count()` to pick `max(1, cpu_count - 1)`. Tests marked `aiosqlite_serial` must stay out of xdist and run in a separate serial pass; live Telegram runs are also forced back to a single worker.
