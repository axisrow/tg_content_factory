# TG Agent

[![Release](https://img.shields.io/github/v/release/axisrow/tg_content_factory)](https://github.com/axisrow/tg_content_factory/releases)

Telegram automation for **coding agents** — Claude Code, OpenCode, Codex. Ships as a **Claude Code plugin** (skill + CLI): install the plugin and your agent can act on Telegram for you — read and send messages, manage channels and chats, download media, run LLM content pipelines. The content factory (collect → search → generate → publish) is one module among many.

[Русская версия](README.ru.md)

## How it works

- **Skill + Claude Code plugin** — this repo ships a [skill](skills/tg-agent/SKILL.md) (installed from the plugin marketplace, or copied manually) that teaches your agent the command catalog, the runtime model and safety rules. The skill drives the CLI — no MCP tool surface eating your context window.
- **`tg-agent` CLI** — the product surface: ~250 commands covering the full Telegram spectrum (`channel`, `dialogs`, `messages`, `search`, `pipeline`, `photo-loader`, `analytics`, `scheduler`, `account`, …). Every command is a self-contained one-shot: it does the work and exits. No daemon required — without one, the command connects the account itself.
- **Worker daemon** (`tg-agent restart`) — optional long-lived process for scheduled collection and queued sends. While it runs it owns every account's Telegram connection (one connection per account per machine): live commands (`messages read --live`, all `dialogs` actions, `account info`, `search --mode telegram`, `pipeline run`, …) run on the daemon's connection through a local relay (127.0.0.1, token file readable by the owner only) — same output, no second login, nothing queued. Collection hand-offs go to the daemon's queue and wake it at once. In gateway mode (`tg-agent global enable`) the shared gateway is that owner.

> **Upgrading to 0.2.5:** restart a running daemon once (`tg-agent restart`), and the shared gateway if you use one (`tg-agent gateway restart --shared`) — an older daemon or gateway serves no relay, and live commands next to it fail at once with that hint. The web panel (`serve`, the optional `tg-webui` package) and the embedded agent chat (`agent …`; its interactive TUI is the optional `tg-tui` package) stay available as deprecated, optional surfaces. The stdio MCP bridge (`mcp-server`) and the Codex/ADK agent backends were removed in #1868.

## Quick Start (coding agents)

### Prerequisites

- Python 3.11.5+
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

- **Built for coding agents** — the CLI is the contract: new capabilities land as CLI commands (tested there) and in the skill
- **All chat types** — channels, supergroups, gigagroups, forums, public and private
- **Multi-account** with automatic flood-wait rotation; one Telegram connection per account per machine, shared by every CLI process through the daemon's (or gateway's) relay
- **Search** — local DB (FTS5, semantic, hybrid) and live Telegram search (global, your chats, one channel)
- **Scheduled collection** — incremental fetching; runs in the background worker daemon
- **Keyword monitoring** — plain text and regex, with Telegram bot notifications
- **Content factory** — LLM pipelines: generate → moderate → publish, image generation included
- **Built-in anti-spam filters** — deduplication, low-uniqueness detection, cross-channel spam, subscriber-ratio and non-Cyrillic filters
- **Analytics** — top posts, trends, activity heatmaps, trending topics and emojis
- **Security** — session encryption (Fernet + PBKDF2); the daemon's relay listens on 127.0.0.1 only, behind a random token in a file readable by the owner only
- **Docker-ready**

## Docker

```bash
cp .env.example .env
# fill in your credentials
docker-compose up -d
```

The image runs the Telegram worker runtime (automation-first) — collection,
scheduler, queues — with the managed PID file. The image has no web surface (the
optional web panel is not part of it), hence no published port and no HTTP healthcheck.

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
| `LLM_API_KEY` | No | Fallback API key (`llm` section) for semantic-search embeddings when `settings semantic --api-key` is not set |
| `ANTHROPIC_API_KEY` | No | `claude-agent-sdk` backend of the content-pipeline agent |
| `CLAUDE_CODE_OAUTH_TOKEN` | No | Claude Code auth token for the `claude-agent-sdk` backend (content-pipeline agent) |
| `AGENT_MODEL` | No | Model override for the `claude-agent-sdk` backend (content-pipeline agent) |
| `AGENT_FALLBACK_MODEL` | No | `provider:model` for the `deepagents` backend of the content-pipeline agent |
| `AGENT_FALLBACK_API_KEY` | No | Explicit API key for the `deepagents` fallback provider |

\* Required to add or import accounts and decrypt stored sessions. New sessions are always encrypted (`enc:v2:`); plaintext writes are refused. Generate a secret with `python -c "import secrets; print(secrets.token_urlsafe(32))"`, keep it private, and retain it to read existing sessions. No `config.yaml` is needed: the key is read directly from `.env`. A gateway-bound project (`tg-agent global enable`) needs no key of its own: the shared gateway owns the sessions.

### config.yaml

Supports `${ENV_VAR}` substitution. Empty env vars are dropped (defaults apply).

| Section | Description |
|---|---|
| `telegram` | API credentials (`api_id`, `api_hash`) |
| `scheduler` | Collection interval, delays, limits, max flood wait |
| `notifications` | `admin_chat_id` for keyword match alerts |
| `database` | SQLite path (default: `data/tg_search.db`); the daemon's relay endpoint file sits next to it (`<db>.relay.json`, 0600) |
| `llm` | LLM provider, model, API key — the fallback provider/key for semantic-search embeddings |
| `agent` | Content-pipeline agent: model, `deepagents` fallback model/key, stream timeouts |
| `security` | Session encryption settings |

### Content-pipeline agent backend rules

Pipelines with `--generation-backend deep_agents` run on the agent engine (`AgentManager`):

- Auto-selection prefers `deepagents` when usable provider configs exist in the DB (`tg-agent provider add`), then `claude-agent-sdk` when `ANTHROPIC_API_KEY` or `CLAUDE_CODE_OAUTH_TOKEN` is configured, then `deepagents` with `AGENT_FALLBACK_MODEL`.
- `ANTHROPIC_API_KEY` and `CLAUDE_CODE_OAUTH_TOKEN` are never reused by `deepagents`.
- `tg-agent settings agent --backend auto|claude|deepagents` forces a backend; the override applies only when developer mode is enabled (`tg-agent settings set agent_dev_mode_enabled 1`).

## CLI reference (selected)

```bash
tg-agent restart                                 # managed daemon: stop the previous one, run the worker
tg-agent worker                                  # same runtime without the stop-first step
tg-agent daemon status                           # read-only: is a daemon up, PID, store
tg-agent channel collect ID                      # one channel (handed to the daemon's queue while it runs)
tg-agent search "query" --limit 20               # search collected history
tg-agent messages read @channel --format json    # read message history
tg-agent dialogs send                            # real actions in real chats
tg-agent pipeline generate                       # LLM content factory
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
 pip install -e packages/tg_shared -e ".[dev]"

 # Run parallel-safe tests (up to four workers locally)
 pytest tests/ -v -m "not aiosqlite_serial" -n auto

 # Keep aiosqlite-backed tests sequential within each file; run independent files in parallel
 pytest tests/ -q -m aiosqlite_serial -n auto --dist=loadfile

 # Run a single test
 pytest tests/test_local_install_consistency.py -v

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

