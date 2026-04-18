# TG Agent

[![Release](https://img.shields.io/github/v/release/axisrow/tg_content_factory)](https://github.com/axisrow/tg_content_factory/releases)

A personal Telegram monitoring toolkit — collect messages, search across channels, get keyword alerts. Built as a pet project for my own use.

[Русская версия](README.ru.md)

## Features

- **All chat types** — channels, supergroups, gigagroups, forums, public and private
- **Multi-account** with automatic flood-wait rotation
- **3 search modes** — local DB (FTS5), direct Telegram API, AI/LLM-powered
- **AI Agent** — interactive chat powered by `claude-agent-sdk` with automatic `deepagents` fallback when Claude SDK is not configured; developer override is available in Settings
- All search results are cached in a local SQLite database
- **Scheduled collection** — incremental message fetching on a timer
- **Keyword monitoring** — plain text and regex, with Telegram bot notifications
- **Built-in anti-spam filters** — deduplication, low-uniqueness detection, cross-channel spam, subscriber ratio filters, non-Cyrillic content filter
- **Task queue** — background job processing with status tracking
- **Web dashboard** — FastAPI + Bootstrap 5, manage everything from a browser
- **Security** — session encryption (Fernet + PBKDF2), web panel password, HTTP Basic fallback, HMAC-signed cookies
- **Docker-ready**

## Quick Start

### Prerequisites

- Python 3.11+
- Telegram API credentials from [my.telegram.org/apps](https://my.telegram.org/apps)

### Installation

```bash
pip install tg-agent
```

Or from source:

```bash
pip install .
cp .env.example .env
```

Edit `.env`:

```
TG_API_ID=your_api_id
TG_API_HASH=your_api_hash
WEB_PASS=your_password
SESSION_ENCRYPTION_KEY=    # encrypts account session strings in DB
LLM_API_KEY=               # optional, for AI search
AGENT_MODEL=               # optional, Claude SDK model override
AGENT_FALLBACK_MODEL=      # optional, provider:model for deepagents fallback
AGENT_FALLBACK_API_KEY=    # optional, explicit API key for deepagents fallback provider
```

Start the server — one command, everything works:

```bash
python -m src.main serve
```

Open http://localhost:8080 in your browser and enter the `WEB_PASS` password.

### Split deployment (Docker / k8s)

`serve` spawns an embedded Telegram worker inside the same process by default,
so a single `python -m src.main serve` command runs the web UI and the
collection worker together. For Docker or Kubernetes where you want the web
and worker in separate containers, pass `--no-worker` and run a dedicated
worker service:

```bash
# container 1 — web UI + API only
python -m src.main serve --no-worker

# container 2 — Telegram worker (shared SQLite volume)
python -m src.main worker
```

## Docker

```bash
cp .env.example .env
# fill in your credentials
docker-compose up -d
```

## Semantic Search Roadmap Note

The current semantic and hybrid search implementation was originally built around
runtime `sqlite-vec` loading. That turned out to be too fragile as a mandatory
foundation: installing the `sqlite-vec` package alone is not enough, because the
active Python/SQLite build must also support `sqlite3.enable_load_extension(...)`.
In practice, the same `pip install` can therefore produce different operator
outcomes across machines, including "package installed but semantic search
unavailable."

The roadmap is being corrected toward a portable SQLite-first semantic backend
that works on standard Python builds without `enable_load_extension`. Until that
backend lands, treat `sqlite-vec` as a transitional dependency rather than a
guaranteed feature toggle. The public UX stays the same: semantic indexing,
semantic search, and hybrid search remain the target interface.

See [docs/semantic-search.md](docs/semantic-search.md) for the architecture
note, migration story, and rationale for de-emphasizing mandatory `sqlite-vec`.

## Configuration

### Environment Variables (.env)

| Variable | Required | Description |
|---|---|---|
| `TG_API_ID` | Yes | Telegram API ID |
| `TG_API_HASH` | Yes | Telegram API Hash |
| `WEB_PASS` | Yes | Web panel password |
| `SESSION_ENCRYPTION_KEY` | No* | Key for encrypting Telegram session strings in DB |
| `LLM_API_KEY` | No | API key for AI-powered search (deepagents) |
| `ANTHROPIC_API_KEY` | No | Anthropic API key for `claude-agent-sdk` only |
| `CLAUDE_CODE_OAUTH_TOKEN` | No | Claude Code auth token for `claude-agent-sdk` only |
| `AGENT_MODEL` | No | Override Claude SDK model for `/agent` |
| `AGENT_FALLBACK_MODEL` | No | `provider:model` for `deepagents` fallback in `/agent` |
| `AGENT_FALLBACK_API_KEY` | No | Explicit API key passed to LangChain `init_chat_model(...)` for fallback |

\* If not set, sessions are stored in plaintext. If the DB already contains encrypted sessions (`enc:v*`), startup fails until this key is provided.

### config.yaml

Supports `${ENV_VAR}` substitution. Empty env vars are dropped (defaults apply).

| Section | Description |
|---|---|
| `telegram` | API credentials (`api_id`, `api_hash`) |
| `web` | Host, port, password (default: `0.0.0.0:8080`) |
| `scheduler` | Collection interval, delays, limits, max flood wait |
| `notifications` | `admin_chat_id` for keyword match alerts |
| `database` | SQLite path (default: `data/tg_search.db`) |
| `llm` | LLM provider, model, API key for AI search (deepagents) |
| `agent` | Claude model override and `deepagents` fallback settings for `/agent` |
| `security` | Session encryption settings |

### Agent backend rules

- `/agent` uses `claude-agent-sdk` when `ANTHROPIC_API_KEY` or `CLAUDE_CODE_OAUTH_TOKEN` is configured.
- If Claude SDK is not configured, `/agent` falls back to `deepagents` when `AGENT_FALLBACK_MODEL` is set.
- `ANTHROPIC_API_KEY` and `CLAUDE_CODE_OAUTH_TOKEN` are never reused by `deepagents`.
- Developer override for forcing `claude-agent-sdk` or `deepagents` lives on the Settings page and applies only when developer mode is enabled.

## CLI

```bash
# Web server (spawns the embedded Telegram worker by default)
python -m src.main [--config CONFIG] serve [--web-pass PASS] [--no-worker]
python -m src.main [--config CONFIG] stop
python -m src.main [--config CONFIG] restart [--web-pass PASS]

# Standalone Telegram worker (only needed with `serve --no-worker` in split deployments)
python -m src.main [--config CONFIG] worker

# One-shot collection
python -m src.main [--config CONFIG] collect [--channel-id ID]

# Search
python -m src.main [--config CONFIG] search "query" [--limit N] [--mode MODE]

# Channel management
python -m src.main channel list|add|delete|toggle|collect|stats|refresh-types|import

# Content filters
python -m src.main filter analyze|apply|reset|precheck

# Keywords
python -m src.main keyword list|add|delete|toggle

# Accounts
python -m src.main account list|toggle|delete

# Scheduler
python -m src.main scheduler start|trigger|search

# Notification bot
python -m src.main notification setup|status|delete

# Diagnostics and benchmarks
python -m src.main test all|read|write|telegram|benchmark
```

### `telethon-cli`

`telethon-cli` is installed with the project and reuses the same `TG_API_ID`
and `TG_API_HASH` values from `.env`.

Optional CLI-only overrides:

- `TG_SESSION` sets a custom Telethon session path or name.
- `TG_PASSWORD` supplies the Telegram 2FA password for non-interactive runs.

Legacy `TELETHON_*` environment variable names are still accepted by
`telethon-cli` for compatibility, but this project standardizes on `TG_*`.

```bash
telethon-cli login
telethon-cli users get-me --output json
```

## Web Interface

| Page | Path | Description |
|---|---|---|
| Web login | `/login` | Sign in to the web panel with `WEB_PASS` |
| Dashboard | `/` | Stats, scheduler status, connected accounts |
| Telegram auth | `/auth/login` | Add Telegram accounts (phone + code + 2FA) |
| Accounts | `/accounts` | Manage connected accounts |
| Channels | `/channels` | Add/remove channels, keywords, import |
| Search | `/search` | Search messages (local / Telegram / AI) |
| Analytics | `/analytics` | Top posts leaderboard, engagement by content type, hourly patterns |
| Filters | `/filter` | Anti-spam filter report and controls |
| Scheduler | `/scheduler` | Start/stop/trigger collection and keyword search |
| Agent | `/agent` | AI chat assistant with access to collected messages |

## Roadmap

- Portable semantic search on stock Python installs without mandatory runtime SQLite extension loading
- LLM-powered content factory
- LLM-powered intelligent search
- LLM-based chat spam moderation
- Direct message handling
- Telegram action automation (broadcasts, etc.)

## Development

 ```bash
 # Install dev dependencies
 pip install -e ".[dev]"
 
 # Run parallel-safe tests (all available CPUs minus one worker)
 pytest tests/ -v -m "not aiosqlite_serial" -n auto

 # Run aiosqlite-backed tests serially
 pytest tests/ -v -m aiosqlite_serial

 # Run a single test
 pytest tests/test_web.py::test_health_endpoint -v

 # Benchmark serial vs safe mixed-mode suite execution
 python -m src.main test benchmark

  # Lint
  ruff check src/ tests/ conftest.py
  ```

### CI Note

- `push` workflow checks the branch head only.
- `pull_request` workflow checks the merge result against `main`.
- A branch can therefore be green on `push` and red on `pull_request` if `main`
  introduced a lint/test failure that is pulled into the PR merge ref.
- Before rerunning PR checks, fetch and sync with `origin/main` so local
  verification matches CI.
