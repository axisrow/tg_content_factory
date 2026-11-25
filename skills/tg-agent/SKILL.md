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
| Chat seeding: compose dialogues for own chats, then publish on explicit approval | [seeding.md](seeding.md) — real chat speech is the prompt input (`seeding samples`: conversation windows + sender voices; composing method in [seeding-voice.md](seeding-voice.md)), stats bound the timing; drafts (`samples → stats → scenario → validate → add`), then `seeding approve` + `seeding schedule` per run — each publish run needs the owner's explicit consent |
| Per-channel posting style specs (local-only, never committed) | `style init|list|show --channel <id>` — voice specs live in `~/.tg-agent/designs/`; bootstrap a missing one, read it before rendering for a channel |
| Publish an article as a telegra.ph page (long-form behind a TG post) | `telegraph login --short-name N` once (stores the account token in settings), then `telegraph create --title "..." --file article.md [--format json]` — prints the page URL to pair with `dialogs send` |
| Channel discovery by keywords (premium search) | [discovery.md](discovery.md) — keyword sweep → candidates short-list → toggle on explicit approval |
| Read message history | `messages read <id> [--query TEXT --date-from D --date-to D --topic-id ID --limit N --format json]`; incremental: `messages read <id> --live --after-id N --format json` → `{"messages": […], "newest_id": N, "has_more": bool}` — loop with `--after-id <newest_id>` |
| Check whether a sent message was deleted (bot cleaners) | `messages wait --message-id N [--phone P] [--chat ID] [--timeout S]` — local store, no Telegram calls; exit 0 = deleted, 1 = alive |
| Client reports: everything we sent (what/when/account/feature + deletion status) | `messages sent [--phone P] [--chat ID] [--source SRC] [--date-from D --date-to D] [--format csv]` — local store, no Telegram calls |
| Search collected history | `search "query" ["query2" ...] [--limit N --mode local|semantic|hybrid|telegram|my_chats|channel]` — 2+ queries = batch JSON; `--date-from D --date-to D` window works on DB modes and `telegram` (client-side filter there — see [reference.md](reference.md) #1750 note) |
| Search-discovered channel candidates (premium search cache) | `channel candidates [--json]`, full queue `channel pending-list`; decide per candidate: `channel toggle` (approve) / `channel reject` |
| Channels: list, add, collect, stats, import | `channel`, `filter` |
| Chats: read, send, edit, forward, media, participants, admins — real actions, see Safety | `dialogs` |
| Comment on a channel post (lands in the linked discussion group) | `dialogs send <channel> <text> --comment-to <post_id>` — recipient must be a channel; find the post id first (`messages read <channel> --limit N` or the collected store) |
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

- **Read-only** (`dialogs list/read/topics/participants`, `dialogs draft-list`, `channel *`,
  `search`, `messages read`, `messages wait`, `messages sent`, `analytics`): run freely. Exception: `analytics channel-rate` runs
  an LLM judge — provider spend + DB write, calibration tool for `analytics usefulness`
  (content rating «полезное vs вода», deterministic and free); don't batch channel-rate
  without the user asking for it.
- **Visible actions** (`send`, `forward`, `edit-message`, `edit-message-media`,
  `delete-message`, `react`, `pin-message`, `download-media`, `draft-save`): only when the
  user explicitly asked for this action. Confirm target chat + content before first send in
  a task.
- **Destructive / hard to undo** (`delete`, `kick`, `leave`, `edit-admin`, `edit-permissions`,
  `create-channel`, `create-group`, `channel delete`, `filter purge*`/`hard-delete`): always ask
  an explicit yes/no confirmation naming the exact target before running, even if the user
  described the intent. `dialogs delete` is permanent (DeleteChannel/DeleteChat): on an owned
  channel it deletes the channel itself for all members, not just a local entry.
- **No bulk messaging** (broadcast to many chats) unless the user spelled out the exact
  recipient list and content.
- On `FloodWait` errors: stop and wait the reported seconds (or tell the user); do not retry
  in a loop.
- A tool may return a local floodgate deferral reply («Локальный floodgate откладывает
  "…": повторите через N сек/мин … НЕ повторяйте вызов немедленно (код: FLOODGATE-ERR-…»)
  before any Telegram work happens (#1851): schedule the retry after the stated delay or
  switch accounts; retrying sooner just returns the same deferral. Disable with env
  `TG_AGENT_FLOODGATE=0`. The local guards' spent budget (rate windows incl. the
  hour-long ones, open flood breakers, the outgoing ceiling) survives a daemon restart
  and carries over between CLI runs without a daemon, so restarting is not a way around a
  deferral. Next to a running daemon (or gateway) its guards gate every command.

## Background collection

One-off: `tg-agent channel collect <id>` runs a collection synchronously and
exits. Continuous/scheduled collection needs the long-lived daemon: `tg-agent restart`
stops any previous daemon and starts the worker runtime — no web panel, no WEB_PASS
required, managed via a PID file (`tg-agent stop` halts it). Check scheduler state with
`tg-agent scheduler status`. Check the daemon itself with `tg-agent daemon status`
(read-only: up/down + provenance — do this before any stop/restart; a stop from a
directory that does not own the daemon is refused, and a failed restart says so loudly).
See [reference.md](reference.md) → "Daemon lifecycle".

While the daemon runs it owns every account connection, and live commands run on that
connection through its local relay — `messages read --live`, every `dialogs` action,
`account info`, `search --mode telegram`, `channel add`/`import`/`probe`, `pipeline
run`/`generate`/`publish`, … all work next to the daemon exactly as without it (same
output, synchronous, no second login). Collection (`channel collect`, `collect`) and
`ping run-due` go to the daemon's own queues. After upgrading tg-agent, restart the daemon
once (`tg-agent restart`): an older daemon serves no relay, and live commands fail at once
with exit 1 (`Error: The running tg-agent daemon (PID N) does not serve the CLI relay …`).
Details: [reference.md](reference.md) → "Live commands next to the running daemon" and
"scheduling & background".

## Legacy surfaces — do not use for integration

The web panel, TUI and embedded agent chat (`tg-agent agent chat`) still work,
but their development is paused indefinitely (bug fixes only). For coding agents the CLI
above is the only supported interface. The MCP server (`mcp-server`) and the Codex/ADK
agent backends were removed in #1868.
Interactive `tg-agent agent chat` (no prompt) needs the optional `tg-tui` package
(`pip install tg-tui`); without it the command exits 1 with an install hint.
One-shot `agent chat -p "..."` works without it. Likewise `tg-agent serve` needs the
optional `tg-webui` package (`pip install tg-webui`) and exits 1 with an
install hint without it; the worker daemon (`restart`/`worker`) never needs it.

