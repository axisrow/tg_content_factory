---
name: tg-agent
description: Full-spectrum Telegram actions via the tg-agent CLI — channels, collection, search, chats and messages (send, edit, delete, forward, media, participants, admins), content pipelines, analytics. Use when the user asks to do anything in Telegram, manage Telegram channels/chats/content, or mentions tg-agent.
allowed-tools: Bash(tg-agent *)
---

# tg-agent — Telegram actions for coding agents

`tg-agent` is a personal Telegram automation CLI. Once installed and authorized, it lets you
act on the user's Telegram for them: read and send messages, manage channels and chats,
download media, run LLM content pipelines, and inspect analytics. Full command catalog:
[reference.md](reference.md).

## Setup check (run once per session)

```bash
tg-agent --version
```

- If missing: `pip install tg-agent`, then the user must provide Telegram API credentials
  (`TG_API_ID`, `TG_API_HASH` from my.telegram.org) in `.env` in the working directory.
  Set a private `SESSION_ENCRYPTION_KEY` there before adding/importing accounts; retain it
  to decrypt stored sessions. Generate one with `python -c "import secrets; print(secrets.token_urlsafe(32))"`.
  The key is required for account writes; sessions are never stored in plaintext.
  Multi-project machines: the key may live ONCE in the OS secret store instead of per-project
  `.env` copies — `tg-agent key set` (macOS Keychain / Windows Credential Manager / secret-tool;
  `key status` / `key clear` manage it). An explicit env/config key still wins; nothing is
  stored unless the owner runs `key set`.
- Check an account is authorized: `tg-agent account list`. If none, first authorization is
  **interactive** — the user must run `tg-agent account add` themselves (they receive a
  Telegram login code). Never try to automate the code entry.
- Fresh project shows `No accounts found`? Accounts are per-project by default. Attach the
  project to the machine-wide account gateway once with `tg-agent global enable --account
  <PHONE>` (repeatable; requires explicit grants) — shared accounts become visible without
  re-logging in. Only accounts are shared: the project database stays project-local (#1724,
  global auth never implies a global DB). Legacy plain-path `.tg-agent-global` markers are
  inert; replace one with `enable`, check with `global status`.
- `.env` from the current directory is picked up automatically.
- If Telegram connections time out (MTProto IPs blocked on this network), set
  `TG_PROXY=socks5://user:pass@host:port` (no-auth `socks5://host:port`, or
  `http://host:port`) in `.env`. Unset/empty = direct connection.

## How to run commands

```bash
tg-agent <group> <command> [args]
```

Use `tg-agent <group> --help` and `tg-agent <group> <command> --help` liberally — flags are
not repeated here. Many commands support `--format json` for parseable output.

Task → group (details in [reference.md](reference.md)):

| Task | Group |
|------|-------|
| AI news digest (structured, schema-first) | [digest.md](digest.md) — window search → schema JSON → format per request → publish on approval |
| Per-channel posting style specs (local-only, never committed) | `style init|list|show --channel <id>` — voice specs live in `~/.tg-agent/designs/`; bootstrap a missing one, read it before rendering for a channel |
| Publish an article as a telegra.ph page (long-form behind a TG post) | `telegraph login --short-name N` once (stores the account token in settings), then `telegraph create --title "..." --file article.md [--format json]` — prints the page URL to pair with `dialogs send` |
| Channel discovery by keywords (premium search) | [discovery.md](discovery.md) — keyword sweep → candidates short-list → toggle on explicit approval |
| Read message history | `messages read <id> [--query TEXT --date-from D --date-to D --topic-id ID --limit N --format json]` |
| Search collected history | `search "query" ["query2" ...] [--limit N --mode local|semantic|hybrid|telegram|my_chats|channel]` — 2+ queries = batch JSON |
| Search-discovered channel candidates (premium search cache) | `channel candidates [--json]`, full queue `channel pending-list`; decide per candidate: `channel toggle` (approve) / `channel reject` |
| Channels: list, add, collect, stats, import | `channel`, `filter` |
| Chats: read, send, edit, forward, media, participants, admins — real actions, see Safety | `dialogs` |
| Incoming-DM reply drafts: prepare → owner approves → worker sends (queue, no double-send) | `dm drafts` |
| Incoming-DM subsystem state: listener/catch-up liveness, unprocessed journal, effective settings | `dm status` |
| Dormant-DM friendly pings: monitor silent 1:1 chats, auto re-ping on a decaying schedule | `ping` |
| Content factory (LLM pipelines, images) | `pipeline`, `photo-loader` |
| Analytics, notifications, scheduling, accounts, settings | `analytics`, `notification`, `scheduler`, `account`, `settings` |
| Shared account store across projects (attach/detach/status) | `global` |
| Installed plugin tools: activation status, run a tool | `plugin` — `plugin list [--json]`, `plugin run TOOL [flags from schema] [--with-pool]`; output is JSON, details in [reference.md](reference.md) |

## Safety — this is the user's real Telegram

`dialogs *` commands perform **real actions** from the user's account to real chats.
Sending a wrong message or kicking a wrong member is not reversible by you.

- **Read-only** (`dialogs list/read/topics/participants`, `channel *`, `search`, `messages read`,
  `analytics`): run freely. Exception: `analytics channel-rate` runs an LLM judge — provider
  spend + DB write, calibration tool for `analytics usefulness` (content rating «полезное vs
  вода», deterministic and free); don't batch channel-rate without the user asking for it.
- **Visible actions** (`send`, `forward`, `edit-message`, `delete-message`, `react`,
  `pin-message`, `download-media`): only when the user explicitly asked for this action.
  Confirm target chat + content before first send in a task.
- **Destructive / hard to undo** (`delete`, `kick`, `leave`, `edit-admin`, `edit-permissions`,
  `create-channel`, `create-group`, `channel delete`, `filter purge*`/`hard-delete`): always ask
  an explicit yes/no confirmation naming the exact target before running, even if the user
  described the intent. `dialogs delete` is permanent (DeleteChannel/DeleteChat): on an owned
  channel it deletes the channel itself for all members, not just a local entry.
- **No bulk messaging** (broadcast to many chats) unless the user spelled out the exact
  recipient list and content.
- On `FloodWait` errors: stop and wait the reported seconds (or tell the user); do not retry
  in a loop.

## Background collection

One-off: `tg-agent channel collect <id>` runs a collection synchronously and
exits. Continuous/scheduled collection needs the long-lived daemon: `tg-agent restart`
stops any previous daemon and starts the worker runtime — no web panel, no WEB_PASS
required, managed via a PID file (`tg-agent stop` halts it). Check scheduler state with
`tg-agent scheduler status`. Check the daemon itself with `tg-agent daemon status`
(read-only: up/down + provenance — do this before any stop/restart; a stop from a
directory that does not own the daemon is refused, and a failed restart says so loudly).
See [reference.md](reference.md) → "Daemon lifecycle".

While the daemon runs, most `dialogs` actions (`send`, `forward`, `react`, `refresh`, …),
`channel collect`, `collect --channel-id`, `channel stats` and the scheduler mutations
(`scheduler start`, `trigger`, `task-cancel`, `clear-pending`) are handed
off to the worker automatically — the CLI queues them (or writes the scheduler setting and
pokes the worker) instead of connecting directly; the command returns "queued" with the
command id, the worker
does the Telegram work. Commands that must open their own connection and have no
queue equivalent (`messages read --live`, `dialogs read`, `dialogs download-media`,
`account info`, `search --mode telegram`, `notification …`, `channel add`/`import`/`probe`,
`pipeline publish`, …) are **refused at once** with exit 1 (`Error: A running tg-agent daemon
(PID N) owns the Telegram sessions …`) — no Telegram connection is attempted: Telegram
kills sessions seen from two connections at once. Stop the daemon (`tg-agent stop`) for
those, then `tg-agent restart`. `pipeline run`/`generate` keep working next to the daemon
but without Telegram access (Telegram-side nodes — react/forward/delete/edit — fail
without a pool, so pipelines containing them need the daemon stopped); `dialogs queue …` and
`dialogs cache-status` are DB-only and always work. Details: [reference.md](reference.md) → "Session lock" and
"scheduling & background".

## Legacy surfaces — do not use for integration

The web panel, TUI, embedded agent chat (`tg-agent agent chat`) and MCP server still work,
but their development is paused indefinitely (bug fixes only). For coding agents the CLI
above is the only supported interface.
Interactive `tg-agent agent chat` (no prompt) needs the optional `tg-tui` package
(`pip install -e packages/tg_tui`); without it the command exits 1 with an install hint.
One-shot `agent chat -p "..."` works without it.

