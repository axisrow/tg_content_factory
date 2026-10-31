# Repository Guidelines

## Project Structure & Module Organization
Core code lives in `tg_agent/`. Use `tg_agent/cli/` for command-line entrypoints, `packages/tg_webui/` (import `tg_webui`, optional package) for the legacy FastAPI dashboard, `tg_agent/services/` for orchestration/business logic, `tg_agent/telegram/` for Telethon integrations, and `tg_agent/database/` plus `tg_agent/database/repositories/` for SQLite access. Tests live under `tests/` (repository coverage in `tests/repositories/`); the web panel's tests, including route coverage, live in `packages/tg_webui/tests/` (`routes/`, `e2e/`). Documentation lives in `docs/`; static web assets are in `packages/tg_webui/src/tg_webui/static/` and Jinja templates in `packages/tg_webui/src/tg_webui/templates/`.

## Build, Test, and Development Commands
Install locally with `pip install -e .` from the repo root. Run `python -m tg_agent.main serve` — it spawns the embedded Telegram worker by default, so a single command gives you the web UI plus real collection, scheduler jobs, and `/agent` tools. Open `http://localhost:8080`. For Docker/k8s split deployments add `--no-worker` and run `python -m tg_agent.main worker` in a separate container. Start the Docker stack with `docker-compose up -d`.

Use these validation commands during development:

- `ruff check tg_agent/ tests/ packages/ conftest.py` — lint Python code (core and monorepo packages).
- `pytest tests/ -v -m "not aiosqlite_serial" -n auto` — run parallel-safe tests.
- `pytest tests/ -q -m aiosqlite_serial -n auto --dist=loadfile` — run DB/thread-sensitive tests sequentially within each file, with independent files in parallel.
- `pytest packages/tg_webui/tests/test_web.py::test_health_endpoint -v` — run one focused test.

## Coding Style & Naming Conventions
Target Python 3.11+ and keep code async-first. Ruff enforces imports, naming, and basic style with a `line-length` of `120`. Follow the existing naming pattern: `snake_case` for modules/functions, `PascalCase` for classes, and descriptive service names such as `content_generation_service.py`. Prefer thin CLI/web handlers that delegate to `tg_agent/services/` and `db.repos.*`.

## Testing Guidelines
Pytest is the test runner, with `pytest-asyncio`, `pytest-timeout`, and `pytest-xdist` enabled. Put new tests in `tests/` using `test_<feature>.py` names. Mark raw `aiosqlite` or `cli_db` cases so they stay in the serial lane, where `--dist=loadfile` keeps each file on one worker; keep pure unit tests parallel-safe when possible. Add route tests for web changes and service/repository tests for persistence logic.

## Commit & Pull Request Guidelines
Recent history uses Conventional Commit prefixes such as `feat(web): ...`, `fix: ...`, and `refactor(web): ...`. Keep commits scoped and imperative. PRs should explain the user-visible change, note schema/config impacts, link the issue or PR number when relevant, and include screenshots for dashboard/template changes. Before opening a PR, run Ruff plus both pytest lanes locally.

Define the acceptance criterion before implementing: the observable result as *condition → action → where the effect is visible*, plus the touched paths and explicit exclusions from the PR's promise.

- Bug fixes: a test that fails on the original bug precedes the fix; tests verify the effect, not a mock call or absence of an exception.
- Cross-subsystem changes: at least one full-scenario test; verify repeats, races, cancellation, restart, and partial failure only where they apply to the change — not on trivial edits.
- Confirm new code is actually wired up and the dependency/environment matches; a push-only path (untestable locally) gets an equivalent run before merge.
- PR body reports commands, results, and a CI link; a partial, cancelled, or skipped run is not a full pass — keep expectations separate from measurements.
- Before merging: description and criteria reconciled with the final diff; an unmet mandatory criterion blocks acceptance unless explicitly excluded from scope.

## Security & Configuration Tips
Keep secrets in `.env`, not in tracked files. Required runtime values include `TG_API_ID`, `TG_API_HASH`, and `WEB_PASS`; `SESSION_ENCRYPTION_KEY` should be set before storing live sessions. Prefer `config.yaml` with `${ENV_VAR}` substitution for local overrides. For multi-project machines the key may instead be stored once in the OS secret store (`tg-agent key set`; opt-in, env/config still wins).
