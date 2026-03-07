# TG Agent

A personal Telegram monitoring toolkit — collect messages, search across channels, get keyword alerts. Built as a pet project for my own use.

[Русская версия](README.ru.md)

## Features

- **All chat types** — channels, supergroups, gigagroups, forums, public and private
- **Multi-account** with automatic flood-wait rotation
- **3 search modes** — local DB (FTS5), direct Telegram API, AI/LLM-powered
- All search results are cached in a local SQLite database
- **Scheduled collection** — incremental message fetching on a timer
- **Keyword monitoring** — plain text and regex, with Telegram bot notifications
- **Built-in anti-spam filters** — deduplication, low-uniqueness detection, cross-channel spam, subscriber ratio filters, non-Cyrillic content filter
- **Task queue** — background job processing with status tracking
- **Web dashboard** — FastAPI + Pico CSS, manage everything from a browser
- **Security** — session encryption (Fernet + PBKDF2), HTTP Basic Auth, HMAC-signed cookies
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
```

Start the server:

```bash
python -m src.main serve
```

Open http://localhost:8080 in your browser.

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
| `LLM_API_KEY` | No | API key for AI-powered search |

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
| `llm` | LLM provider, model, API key, enabled flag |
| `security` | Session encryption settings |

## CLI

```bash
# Web server
python -m src.main [--config CONFIG] serve [--web-pass PASS]

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
| Dashboard | `/` | Stats, scheduler status, connected accounts |
| Auth | `/auth/login` | Add Telegram accounts (phone + code + 2FA) |
| Accounts | `/accounts` | Manage connected accounts |
| Channels | `/channels` | Add/remove channels, keywords, import |
| Search | `/search` | Search messages (local / Telegram / AI) |
| Filters | `/filter` | Anti-spam filter report and controls |
| Scheduler | `/scheduler` | Start/stop/trigger collection and keyword search |

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
