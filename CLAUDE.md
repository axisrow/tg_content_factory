# CLAUDE.md

## Project

tg-user-search — Telegram Post Search & Monitoring

## Tech Stack

- Python 3.11+, FastAPI, Telethon, aiosqlite, APScheduler, Pydantic v2
- Frontend: Jinja2 templates + Pico CSS
- DB: SQLite (data/tg_search.db)
- Tests: pytest + pytest-asyncio

## Structure

```
src/main.py              — CLI entrypoint (serve, collect, search, channel, keyword, account, scheduler)
src/config.py            — Pydantic config with YAML + env substitution
src/database.py          — async SQLite (aiosqlite)
src/models.py            — data models
src/telegram/auth.py     — TelegramAuth (auth flows)
src/telegram/client_pool.py — ClientPool (multi-account management)
src/telegram/collector.py   — Collector (message fetching)
src/telegram/notifier.py    — Notifier (admin alerts)
src/search/engine.py     — SearchEngine (local + telegram)
src/search/ai_search.py  — AISearchEngine (LLM-powered search)
src/scheduler/manager.py — SchedulerManager (APScheduler wrapper)
src/web/app.py           — FastAPI app + BasicAuthMiddleware
src/web/routes/          — auth, accounts, channels, dashboard, search, scheduler
src/web/templates/       — Jinja2 HTML
src/web/static/          — CSS
```

## Commands

```bash
python -m src.main [--config CONFIG] serve [--web-user X --web-pass Y]
python -m src.main [--config CONFIG] collect [--channel-id ID]
python -m src.main [--config CONFIG] search "query" [--limit N] [--mode MODE]

python -m src.main channel list
python -m src.main channel add <identifier>
python -m src.main channel delete <pk>
python -m src.main channel toggle <pk>
python -m src.main channel collect <pk>

python -m src.main keyword list
python -m src.main keyword add <pattern> [--regex]
python -m src.main keyword delete <id>
python -m src.main keyword toggle <id>

python -m src.main account list
python -m src.main account toggle <id>
python -m src.main account delete <id>

python -m src.main scheduler start
python -m src.main scheduler trigger

ruff check src/ tests/
pytest tests/ -v
```

## Conventions

- CLI/Web parity: every web operation must have a CLI equivalent and vice versa
- Async everywhere (asyncio)
- Pydantic v2 models (model_validate, not parse_obj)
- Config via config.yaml with ${ENV_VAR} substitution
- Web auth: HTTP Basic Auth (password only via WEB_PASS, username hardcoded as "admin")
- ruff for linting: line-length=100, target py311, rules E/F/I/N/W
- Tests: pytest-asyncio with asyncio_mode="auto"

## Key Patterns

- `_walk_and_substitute` in config.py drops keys with empty env vars
- `ClientPool` rotates accounts on FloodWaitError
- `Collector` uses min_id for incremental collection
- `BasicAuthMiddleware` skips /health endpoint
- Session strings stored encrypted in DB (cryptography)
- Keyword matching: plain text (case-insensitive) and regex (re.IGNORECASE)
