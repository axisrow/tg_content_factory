# Repository Guidelines

## Project Structure & Module Organization
Core code lives in `tg_agent/`. Use `tg_agent/cli/` for command-line entrypoints, `tg_agent/services/` for orchestration/business logic, `tg_agent/telegram/` for Telethon integrations, and `tg_agent/database/` plus `tg_agent/database/repositories/` for SQLite access. Tests live under `tests/` (repository coverage in `tests/repositories/`). The CLI is the only surface: the web panel, the embedded agent chat with its TUI and the MCP server were removed in #1868 (import-linter bans `tg_webui`/`tg_tui`). Documentation lives in `docs/`.

## Build, Test, and Development Commands
Install locally with `pip install -e packages/tg_shared -e .` from the repo root. Run the worker daemon with `python -m tg_agent.main restart` — it stops any previous daemon and runs collection, scheduler jobs and queued sends; `python -m tg_agent.main worker` is the same runtime without the stop-first step (e.g. under tmux/systemd). While the daemon runs it owns the Telegram connections and every live CLI command runs on them through its local relay; without a daemon the CLI connects itself. After upgrading, restart a running daemon once (`python -m tg_agent.main restart`). Start the Docker stack with `docker-compose up -d` — the image runs the worker.

Use these validation commands during development:

- `ruff check tg_agent/ tests/ packages/ conftest.py` — lint Python code (core and monorepo packages).
- `pytest tests/ -v -m "not aiosqlite_serial" -n auto` — run parallel-safe tests.
- `pytest tests/ -q -m aiosqlite_serial -n auto --dist=loadfile` — run DB/thread-sensitive tests sequentially within each file, with independent files in parallel.
- `pytest tests/test_local_install_consistency.py -v` — run one focused test.

## Coding Style & Naming Conventions
Target Python 3.11+ and keep code async-first. Ruff enforces imports, naming, and basic style with a `line-length` of `120`. Follow the existing naming pattern: `snake_case` for modules/functions, `PascalCase` for classes, and descriptive service names such as `content_generation_service.py`. Prefer thin CLI handlers that delegate to `tg_agent/services/` and `db.repos.*`.

## Testing Guidelines
Pytest is the test runner, with `pytest-asyncio`, `pytest-timeout`, and `pytest-xdist` enabled. Put new tests in `tests/` using `test_<feature>.py` names. Mark raw `aiosqlite` or `cli_db` cases so they stay in the serial lane, where `--dist=loadfile` keeps each file on one worker; keep pure unit tests parallel-safe when possible. Add CLI tests for command changes and service/repository tests for persistence logic.

## Commit & Pull Request Guidelines
Recent history uses Conventional Commit prefixes such as `feat(cli): ...`, `fix: ...`, and `refactor(telegram): ...`. Keep commits scoped and imperative. PRs should explain the user-visible change, note schema/config impacts, and link the issue or PR number when relevant. Before opening a PR, run Ruff plus both pytest lanes locally.

Define the acceptance criterion before implementing: the observable result as *condition → action → where the effect is visible*, plus the touched paths and explicit exclusions from the PR's promise.

- Bug fixes: a test that fails on the original bug precedes the fix; tests verify the effect, not a mock call or absence of an exception.
- Cross-subsystem changes: at least one full-scenario test; verify repeats, races, cancellation, restart, and partial failure only where they apply to the change — not on trivial edits.
- Confirm new code is actually wired up and the dependency/environment matches; a push-only path (untestable locally) gets an equivalent run before merge.
- PR body reports commands, results, and a CI link; a partial, cancelled, or skipped run is not a full pass — keep expectations separate from measurements.
- Before merging: description and criteria reconciled with the final diff; an unmet mandatory criterion blocks acceptance unless explicitly excluded from scope.

## Security & Configuration Tips
Keep secrets in `.env`, not in tracked files. Required runtime values are `TG_API_ID` and `TG_API_HASH`; `SESSION_ENCRYPTION_KEY` should be set before storing live sessions (a gateway-bound project needs none of its own — the shared gateway owns the sessions). Prefer `config.yaml` with `${ENV_VAR}` substitution for local overrides. For multi-project machines the key may instead be stored once in the OS secret store (`tg-agent key set`; opt-in, env/config still wins).
