# Suggested Commands

Host: Linux (standard unix shell — generic `git`/`ls`/`grep`/`find` behave normally; nothing OS-specific).

## Setup
- `pip install -e ".[dev]"` — install with dev extras.

## Run
- `python -m src.main serve [--web-pass PASS] [--no-worker]` — web UI + embedded worker (default). `--no-worker` for split deploys.
- `python -m src.main worker` — standalone worker (only alongside `serve --no-worker`).
- `python -m src.main [--config CONFIG] <command>` — full CLI; top-level groups: `serve worker collect search messages channel filter account scheduler notification test search-query pipeline photo-loader dialogs agent analytics provider export translate settings debug image mcp-server`. `dialogs` is the command for Telegram dialogs management.

## Lint (run before done)
- `ruff check src/ tests/ conftest.py`
- `ruff check --fix src/ tests/ conftest.py` — auto-fix.

## Test
- `pytest tests/ -v -m "not aiosqlite_serial" -n auto` — parallel-safe suite (xdist capped at 4; override `TGCF_PYTEST_XDIST_WORKERS`).
- `pytest tests/ -v -m aiosqlite_serial` — aiosqlite-backed tests, serial.
- `pytest tests/test_web.py::test_health_endpoint -v` — single test (`::` forces 1 worker).
- `python -m src.main test benchmark` — serial vs safe mixed-mode timing.

## Import architecture contract
- `lint-imports --config .importlinter` — CLI/web/agent-tool entrypoints must NOT import telethon directly.

See `mem:task_completion` for the exact done-checklist.
