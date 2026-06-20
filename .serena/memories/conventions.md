# Conventions

## Code style
- ruff: line-length=120, target py311, rules E/F/I/N/W.
- Async everywhere (asyncio). Pydantic v2 (`model_validate`).
- Each repository has a `_to_<model>(row)` static helper mapping `aiosqlite.Row` → Pydantic, with safe `.keys()` checks for nullable/migration-added columns.

## DB access
- Use via `db.repos.<repo>.<method>()`. Writes ONLY through `db.transaction()` / `db.execute_write()` / `db.executemany_write()` (see `mem:core` write-lock invariant). Repos take `database: Database | None`.
- Migrations: `_migrate()` uses `PRAGMA table_info` to detect missing columns + `ALTER TABLE ADD COLUMN`. JOIN rule and channel-id vs pk distinction: see `mem:core`.
- SQL in `"""..."""`: Python does NOT concat adjacent literals inside triple quotes; use parameterized `?`-placeholders, never inline quoted values in `executescript()`.

## Frontend (HTMX vs fetch)
- HTMX for server-driven DOM swaps (HTML fragments): collect buttons, badges, form submits. Routes check `HX-Request` header to choose fragment vs redirect.
- `fetch()` ONLY for: JSON-without-DOM-replacement, streaming/SSE (agent chat, image gen), complex client-side pre/post logic.
- Reviewers reject fetch() where HTMX fits and vice versa.

## Agent tools
- Destructive tools require `confirm=true`; `require_confirmation()` in `src/agent/tools/_registry.py` returns a warning if absent. Tool read/write/delete classes in `permissions.py` (`TOOL_CATEGORIES`); per-phone ACL in DB setting `agent_tool_permissions`.

## Tests
- pytest-asyncio `asyncio_mode="auto"`. `db` fixture is `Database(":memory:")` — any test creating a web app + `real_pool_harness_factory()` MUST accept `db` and use it for `app.state.db` (separate Database breaks account lookups).
- Real Telegram is opt-in only via gate env vars + `real_tg_*` markers; tests stay fake/harness-first. Conftest auto-marks `aiosqlite_serial` (uses `cli_db` or raw `import aiosqlite`).
- Always close raw `aiosqlite.connect()` in `try/finally` — an unclosed worker-thread blocks pytest exit.
