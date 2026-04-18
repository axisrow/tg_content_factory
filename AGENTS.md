# Repository Guidelines

## Project Structure & Module Organization
Core code lives in `src/`. Use `src/cli/` for command-line entrypoints, `src/web/` for the FastAPI dashboard, `src/services/` for orchestration/business logic, `src/telegram/` for Telethon integrations, and `src/database/` plus `src/database/repositories/` for SQLite access. Tests live under `tests/`, with route coverage in `tests/routes/` and repository coverage in `tests/repositories/`. Documentation lives in `docs/`; static web assets are in `src/web/static/` and Jinja templates in `src/web/templates/`.

## Build, Test, and Development Commands
Install locally with `pip install -e .` from the repo root. Run `python -m src.main serve` — it spawns the embedded Telegram worker by default, so a single command gives you the web UI plus real collection, scheduler jobs, and `/agent` tools. Open `http://localhost:8080`. For Docker/k8s split deployments add `--no-worker` and run `python -m src.main worker` in a separate container. Start the Docker stack with `docker-compose up -d`.

Use these validation commands during development:

- `ruff check src/ tests/ conftest.py` — lint Python code.
- `pytest tests/ -v -m "not aiosqlite_serial" -n auto` — run parallel-safe tests.
- `pytest tests/ -v -m aiosqlite_serial` — run DB/thread-sensitive tests serially.
- `pytest tests/test_web.py::test_health_endpoint -v` — run one focused test.

## Coding Style & Naming Conventions
Target Python 3.11+ and keep code async-first. Ruff enforces imports, naming, and basic style with a `line-length` of `120`. Follow the existing naming pattern: `snake_case` for modules/functions, `PascalCase` for classes, and descriptive service names such as `content_generation_service.py`. Prefer thin CLI/web handlers that delegate to `src/services/` and `db.repos.*`.

## Testing Guidelines
Pytest is the test runner, with `pytest-asyncio`, `pytest-timeout`, and `pytest-xdist` enabled. Put new tests in `tests/` using `test_<feature>.py` names. Mark raw `aiosqlite` or `cli_db` cases so they stay in the serial lane; keep pure unit tests parallel-safe when possible. Add route tests for web changes and service/repository tests for persistence logic.

## Commit & Pull Request Guidelines
Recent history uses Conventional Commit prefixes such as `feat(web): ...`, `fix: ...`, and `refactor(web): ...`. Keep commits scoped and imperative. PRs should explain the user-visible change, note schema/config impacts, link the issue or PR number when relevant, and include screenshots for dashboard/template changes. Before opening a PR, run Ruff plus both pytest lanes locally.

## Security & Configuration Tips
Keep secrets in `.env`, not in tracked files. Required runtime values include `TG_API_ID`, `TG_API_HASH`, and `WEB_PASS`; `SESSION_ENCRYPTION_KEY` should be set before storing live sessions. Prefer `config.yaml` with `${ENV_VAR}` substitution for local overrides.
