# TG Post Search

[Русская версия](README.ru.md)

Telegram Post Search & Monitoring — a tool for collecting, searching, and monitoring messages from Telegram channels with a web dashboard.

## Features

- Multi-account Telegram client pool with flood-wait fallback
- Incremental message collection from channels
- Keyword monitoring with Telegram notifications (plain text & regex)
- Full-text search: local DB, direct Telegram, AI-powered (LLM)
- Web dashboard (FastAPI + Pico CSS)
- HTTP Basic Auth
- Docker ready

## Quick Start

### Prerequisites

- Python 3.11+
- Telegram API credentials from [my.telegram.org/apps](https://my.telegram.org/apps)

### Installation

```bash
pip install .
```

Copy and fill in environment variables:

```bash
cp .env.example .env
```

Edit `.env`:

```
TG_API_ID=your_api_id
TG_API_HASH=your_api_hash
WEB_PASS=your_password
SESSION_ENCRYPTION_KEY=    # optional, recommended for account session encryption
LLM_API_KEY=               # optional, for AI search
```

Edit `config.yaml` if needed (defaults work out of the box).

Start the server:

```bash
python -m src.main serve
```

Open http://localhost:8080 in your browser.

### Docker

```bash
cp .env.example .env
# edit .env with your credentials
docker-compose up -d
```

## Configuration

### Environment Variables (.env)

| Variable | Required | Description |
|---|---|---|
| `TG_API_ID` | Yes | Telegram API ID |
| `TG_API_HASH` | Yes | Telegram API Hash |
| `WEB_PASS` | Yes | Web panel password |
| `SESSION_ENCRYPTION_KEY` | No | Explicit key for encrypting Telegram session strings in DB |
| `LLM_API_KEY` | No | API key for AI-powered search |

### config.yaml

Config supports `${ENV_VAR}` substitution. Empty env vars are dropped (defaults apply).

| Section | Description |
|---|---|
| `telegram` | API credentials (`api_id`, `api_hash`) |
| `web` | Host, port, password (default: `0.0.0.0:8080`) |
| `scheduler` | Collection interval, delays, limits, max flood wait |
| `notifications` | `admin_chat_id` for keyword match alerts |
| `database` | SQLite path (default: `data/tg_search.db`) |
| `llm` | LLM provider, model, API key, enabled flag |
| `security` | Session encryption settings (`session_encryption_key`) |

## Usage

### CLI Commands

```bash
# Start web server
python -m src.main [--config CONFIG] serve [--web-pass PASS]

# One-shot message collection
python -m src.main [--config CONFIG] collect

# Search local database
python -m src.main [--config CONFIG] search "query" [--limit N]
```

### Web Interface

| Page | Path | Description |
|---|---|---|
| Dashboard | `/` | Stats, scheduler status, connected accounts |
| Auth | `/auth/login` | Add Telegram accounts (phone + code + optional 2FA) |
| Accounts | `/accounts` | Manage connected accounts (toggle, delete) |
| Channels | `/channels` | Add/remove channels, manage keywords |
| Search | `/search` | Search messages (local / Telegram / AI modes) |
| Scheduler | `/scheduler` | Start/stop/trigger automatic collection |

### Workflow

1. Start the server (`python -m src.main serve`)
2. Open the web dashboard and add a Telegram account via Auth page
3. Add channels to monitor
4. (Optional) Configure keywords for notifications
5. Start the scheduler or trigger manual collection
6. Search collected messages

## Architecture

```
┌─────────────────────────────────────────────┐
│                   CLI (main.py)              │
│           serve / collect / search           │
└──────┬──────────────┬───────────────┬────────┘
       │              │               │
┌──────▼──────┐ ┌─────▼──────┐ ┌──────▼──────┐
│  Web Layer  │ │  Telegram   │ │   Search    │
│  FastAPI +  │ │  ClientPool │ │   Engine    │
│  Templates  │ │  Collector  │ │  + AI Search│
│  Auth MW    │ │  Notifier   │ │             │
└──────┬──────┘ └─────┬──────┘ └──────┬──────┘
       │              │               │
┌──────▼──────────────▼───────────────▼──────┐
│              Database (aiosqlite)           │
│              data/tg_search.db             │
└────────────────────────────────────────────┘
```

## Development

### Setup

```bash
pip install -e ".[dev]"
```

### Testing

```bash
pytest tests/ -v
```

### Linting

```bash
ruff check src/ tests/
```
