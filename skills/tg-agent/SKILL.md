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
- Check an account is authorized: `tg-agent account list`. If none, first authorization is
  **interactive** — the user must run `tg-agent account add` themselves (they receive a
  Telegram login code). Never try to automate the code entry.
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
| Channel discovery by keywords (premium search) | [discovery.md](discovery.md) — keyword sweep → candidates short-list → toggle on explicit approval |
| Read message history | `messages read <id> [--query TEXT --date-from D --date-to D --topic-id ID --limit N --format json]` |
| Search collected history | `search "query" ["query2" ...] [--limit N --mode local|semantic|hybrid|telegram|my_chats|channel]` — 2+ queries = batch JSON |
| Search-discovered channel candidates (premium search cache) | `channel candidates [--json]` |
| Channels: list, add, collect, stats, import | `channel`, `filter` |
| Chats: read, send, edit, forward, media, participants, admins — real actions, see Safety | `dialogs` |
| Incoming-DM reply drafts: prepare → owner approves → worker sends (queue, no double-send) | `dm drafts` |
| Incoming-DM subsystem state: listener/catch-up liveness, unprocessed journal, effective settings | `dm status` |
| Dormant-DM friendly pings: monitor silent 1:1 chats, auto re-ping on a decaying schedule | `ping` |
| Content factory (LLM pipelines, images) | `pipeline`, `photo-loader` |
| Analytics, notifications, scheduling, accounts, settings | `analytics`, `notification`, `scheduler`, `account`, `settings` |

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
`tg-agent scheduler status`.

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
but without Telegram access (Telegram-side nodes are skipped); `dialogs queue …` and
`dialogs cache-status` are DB-only and always work. Details: [reference.md](reference.md) → "Session lock" and
"scheduling & background".

## Legacy surfaces — do not use for integration

The web panel, TUI, embedded agent chat (`tg-agent agent chat`) and MCP server still work,
but their development is paused indefinitely (bug fixes only). For coding agents the CLI
above is the only supported interface.

