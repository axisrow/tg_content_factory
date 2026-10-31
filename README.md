# TG Agent

[![Release](https://img.shields.io/github/v/release/axisrow/tg_content_factory)](https://github.com/axisrow/tg_content_factory/releases)

Telegram automation for **coding agents** — Claude Code, OpenCode, Codex. Ships as a **Claude Code plugin** (skill + CLI): install the plugin and your agent can act on Telegram for you — read and send messages, manage channels and chats, download media, run LLM content pipelines. The content factory (collect → search → generate → publish) is one module among many.

[Русская версия](README.ru.md)

## How it works

- **Skill + Claude Code plugin** — this repo ships a [skill](skills/tg-agent/SKILL.md) (installed from the plugin marketplace, or copied manually) that teaches your agent the command catalog, the runtime model and safety rules. The skill drives the CLI — no MCP tool surface eating your context window.
- **`tg-agent` CLI** — the product surface: ~250 commands covering the full Telegram spectrum (`channel`, `dialogs`, `messages`, `search`, `pipeline`, `photo-loader`, `analytics`, `scheduler`, `account`, …). Every command is a self-contained one-shot: it opens a Telegram connection, does the work, exits. No daemon required.
- **`tg-agent worker`** — optional long-lived process for scheduled collection and queued sends.

## Quick Start (coding agents)

### Prerequisites

- Python 3.11+
- Telegram API credentials from [my.telegram.org/apps](https://my.telegram.org/apps)

### 1. Install the plugin (Claude Code) — or the skill alone

```
/plugin marketplace add https://github.com/axisrow/tg_content_factory
/plugin install tg-agent@tg-agent-marketplace
```

> The short form (`axisrow/tg_content_factory`) clones over SSH — without GitHub SSH keys it
> fails; use the full HTTPS URL as above. If the marketplace is not visible right after
> `marketplace add`, run `/reload-plugins` before `plugin install`.

**OpenCode / Codex / other agents** — skip the plugin: point the agent at [`skills/tg-agent/SKILL.md`](skills/tg-agent/SKILL.md) as an instruction file (AGENTS.md include, system-prompt attachment, etc.), or copy the skill folder:

```bash
cp -r skills/tg-agent ~/.claude/skills/tg-agent
```

### 2. Install the CLI

The skill drives the `tg-agent` CLI, which must be on the machine where your agent runs:

```bash
pip install tg-agent
```

Create `.env` in your working directory (auto-loaded by every command):

```
TG_API_ID=your_api_id
TG_API_HASH=your_api_hash
SESSION_ENCRYPTION_KEY=your_random_secret    # required before adding/importing accounts
```

For several projects on one machine you can skip the per-project key copies:
store it once in the OS secret store with `tg-agent key set` (macOS Keychain /
Windows Credential Manager / secret-tool). An explicit `SESSION_ENCRYPTION_KEY`
still wins over the stored secret.

### 3. Authorize an account (interactive, once)

```bash
tg-agent account add
```

You will receive a Telegram login code — enter it yourself. Verify with `tg-agent account list`.

### 4. Just ask your agent

- "Show yesterday's posts from @durov"
- "Send this draft to my Saved Messages"
- "Collect new messages from my channels and find mentions of \<keyword\>"

## Features

- **Built for coding agents** — the CLI is the contract: new capabilities land as CLI commands first (tested there), other surfaces follow only if at all
- **All chat types** — channels, supergroups, gigagroups, forums, public and private
- **Multi-account** with automatic flood-wait rotation
- **3 search modes** — local DB (FTS5), direct Telegram API, AI/LLM-powered
- **Scheduled collection** — incremental fetching; runs in the background `tg-agent worker`
- **Keyword monitoring** — plain text and regex, with Telegram bot notifications
- **Content factory** — LLM pipelines: generate → moderate → publish, image generation included
- **Built-in anti-spam filters** — deduplication, low-uniqueness detection, cross-channel spam, subscriber-ratio and non-Cyrillic filters
- **Analytics** — top posts, trends, activity heatmaps, trending topics and emojis
- **Security** — session encryption (Fernet + PBKDF2), HMAC-signed web session cookies
- **Docker-ready**

## Legacy surfaces

These still work but are **frozen**: development is paused indefinitely, and new capabilities must not be built on them. The CLI + skill is the only actively developed interface.

- **Web dashboard** (FastAPI + Bootstrap 5) — `python -m tg_agent.main serve`, then http://localhost:8080 (password from `WEB_PASS`)
- **TUI** and the **embedded agent chat** (`tg-agent agent chat`; `claude-agent-sdk` / `deepagents` backends). The interactive TUI is the optional package `packages/tg_tui` (`pip install -e packages/tg_tui`); one-shot `agent chat -p "..."` works without it
- **MCP server** (`python -m tg_agent.main mcp-server`)

### Legacy: split deployment (Docker / k8s)

`serve` spawns an embedded Telegram worker inside the same process by default.
For split deployments pass `--no-worker` and run a dedicated worker service:

```bash
# container 1 — web UI + API only
python -m tg_agent.main serve --no-worker

# container 2 — Telegram worker (shared SQLite volume)
python -m tg_agent.main worker
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
| `SESSION_ENCRYPTION_KEY` | Yes* | Key for encrypting Telegram session strings in DB; may instead live in the OS secret store (`tg-agent key set`) |
| `WEB_PASS` | —† | Web panel password (legacy web dashboard only) |
| `LLM_API_KEY` | No | API key for AI-powered search |
| `ANTHROPIC_API_KEY` | —† | `claude-agent-sdk` only (legacy embedded agent chat) |
| `CLAUDE_CODE_OAUTH_TOKEN` | —† | Claude Code auth token for `claude-agent-sdk` (legacy) |
| `AGENT_MODEL` | —† | Claude SDK model override (legacy embedded agent chat) |
| `AGENT_FALLBACK_MODEL` | —† | `provider:model` for `deepagents` fallback (legacy) |
| `AGENT_FALLBACK_API_KEY` | —† | Explicit API key for the legacy fallback provider |

\* Required to add or import accounts and decrypt stored sessions. New sessions are always encrypted (`enc:v2:`); plaintext writes are refused. Generate a secret with `python -c "import secrets; print(secrets.token_urlsafe(32))"`, keep it private, and retain it to read existing sessions. No `config.yaml` is needed: the key is read directly from `.env`.

\† Legacy-only: needed solely by the legacy web panel and embedded agent chat (see Legacy surfaces).

### config.yaml

Supports `${ENV_VAR}` substitution. Empty env vars are dropped (defaults apply).

| Section | Description |
|---|---|
| `telegram` | API credentials (`api_id`, `api_hash`) |
| `web` | Host, port, password (default: `127.0.0.1:8080`; non-loopback host requires a strong `WEB_PASS`) — legacy web panel |
| `scheduler` | Collection interval, delays, limits, max flood wait |
| `notifications` | `admin_chat_id` for keyword match alerts |
| `database` | SQLite path (default: `data/tg_search.db`) |
| `llm` | LLM provider, model, API key for AI search and content pipelines |
| `agent` | Legacy embedded agent chat settings |
| `security` | Session encryption settings |

### Legacy: embedded agent backend rules

- `/agent` uses `claude-agent-sdk` when `ANTHROPIC_API_KEY` or `CLAUDE_CODE_OAUTH_TOKEN` is configured.
- If Claude SDK is not configured, `/agent` falls back to `deepagents` when `AGENT_FALLBACK_MODEL` is set.
- `ANTHROPIC_API_KEY` and `CLAUDE_CODE_OAUTH_TOKEN` are never reused by `deepagents`.
- Developer override for forcing `claude-agent-sdk` or `deepagents` lives on the Settings page and applies only when developer mode is enabled.

## CLI reference (selected)

```bash
tg-agent restart                                 # managed daemon: worker runtime, no web panel
tg-agent worker                                  # same runtime without the stop-first step
tg-agent channel collect --channel-id ID         # one-off incremental collection (no daemon)
tg-agent search "query" --limit 20               # search collected history
tg-agent messages read @channel --format json    # read message history
tg-agent dialogs send                            # real actions in real chats
tg-agent pipeline generate                       # LLM content factory
tg-agent serve                                   # legacy web panel (deprecated)
```

Full catalog for agents — [`skills/tg-agent/reference.md`](skills/tg-agent/reference.md); every
group also has `--help`.

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

## Web Interface (legacy)

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
| Agent | `/agent` | Legacy embedded AI chat |

## Roadmap

- Portable semantic search on stock Python installs without mandatory runtime SQLite extension loading
- Agent-facing capability growth: every new feature lands in the CLI (and the skill) first
- LLM-powered content factory
- LLM-powered intelligent search
- LLM-based chat spam moderation
- Direct message handling
- Telegram action automation (broadcasts, etc.)

## Development

 ```bash
 # Install dev dependencies
 pip install -e ".[dev]"

 # Run parallel-safe tests (up to four workers locally)
 pytest tests/ -v -m "not aiosqlite_serial" -n auto

 # Keep aiosqlite-backed tests sequential within each file; run independent files in parallel
 pytest tests/ -q -m aiosqlite_serial -n auto --dist=loadfile

 # Run a single test
 pytest packages/tg_webui/tests/test_web.py::test_health_endpoint -v

 # Benchmark serial vs safe mixed-mode suite execution
 python -m tg_agent.main test benchmark

  # Lint
  ruff check tg_agent/ tests/ packages/ conftest.py
  ```

### CI Note

- `push` workflow checks the branch head only.
- `pull_request` workflow checks the merge result against `main`.
- A branch can therefore be green on `push` and red on `pull_request` if `main`
  introduced a lint/test failure that is pulled into the PR merge ref.
- Before rerunning PR checks, fetch and sync with `origin/main` so local
  verification matches CI.

