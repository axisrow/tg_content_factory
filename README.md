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

Start the server:

```bash
python -m src.main serve
```

Open http://localhost:8080 in your browser and enter the `WEB_PASS` password.

## Docker

```bash
cp .env.example .env
# fill in your credentials
docker-compose up -d
```

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
# Web server
python -m src.main [--config CONFIG] serve [--web-pass PASS]
python -m src.main [--config CONFIG] stop
python -m src.main [--config CONFIG] restart [--web-pass PASS]

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
| Filters | `/filter` | Anti-spam filter report and controls |
| Scheduler | `/scheduler` | Start/stop/trigger collection and keyword search |
| Agent | `/agent` | AI chat assistant with access to collected messages |

## Roadmap

- LLM-powered content factory
- LLM-powered intelligent search
- LLM-based chat spam moderation
- Direct message handling
- Telegram action automation (broadcasts, etc.)

## Development

```bash
# Install dev dependencies
pip install -e ".[dev]"

# Run tests
pytest tests/ -v

# Lint
ruff check src/ tests/
```
