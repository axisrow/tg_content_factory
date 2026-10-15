# tg-agent CLI reference

Global form: `tg-agent [--config CONFIG] <group> <command> [args]`.
Every group and command has `--help`. Where a `--format text|json|csv` flag exists, prefer
`json` when you need to parse output. Config comes from `config.yaml` + `.env` in the working
directory (auto-loaded). `TG_PROXY=socks5://user:pass@host:port` (or `http://...`) routes all
Telegram traffic through that proxy when MTProto is blocked; unset = direct.

Groups below mirror `tg-agent --help`. This file lists common commands and their notable
flags; anything not listed — check `--help`.

## Session lock — live commands vs the running daemon

Every live command that opens its own Telegram connection takes a cross-process lock on
the account session. While the worker daemon runs, most `dialogs` actions (`send`,
`forward`, `react`, `refresh`, `pin-message`, `kick`, `archive`, …) and `channel collect`
are **handed off automatically**: the CLI enqueues them for the worker instead of
connecting directly, so they keep working unchanged (`--direct` forces the in-process
path instead).

Commands without a queue equivalent (`messages read --live`,
`dialogs read`, `dialogs download-media`, `scheduler trigger`, …) wait for the daemon to
release the session (up to `TGCF_SESSION_LOCK_GRACE_SECONDS`, default 20) and then fail:

```
SessionLockError: Another tg-agent process is already connected with this session
(lock .../session-<hash>.lock held by PID 12345). Refusing to connect a second time...
```

This is intentional: a second simultaneous connection with the same auth key risks
Telegram killing the session (`AuthKeyDuplicatedError`). If you hit `SessionLockError`,
stop the daemon first (`tg-agent stop`) — or switch to a hand-off `dialogs` action, which
the running worker executes without a second connection. A refused command never
disturbs the live session; when the holder exits, the lock clears on its own.

## channels / collection

```
channel list | add | delete | toggle | collect | stats | refresh-types | refresh-meta
channel import | add-bulk | list-for-import | candidates | tag (list|add|delete|set|get)
filter analyze | apply | reset | precheck | suspects | toggle | purge | purge-messages | hard-delete
```

- `channel collect [--channel-id ID]` — incremental collection for one channel; without flags
  interactive selection. One-off; exit when done.
- `channel stats` — per-channel collection status (`last_collected_id`, counts).
- `channel candidates [--json] [--limit N]` — read-only list of channels discovered by
  premium search (`search --mode tg`): inactive rows whose cached messages carry the
  search-query tag. Pick ones worth collecting, then `channel toggle <identifier>`
  (activates with human origin) + `channel collect --channel-id ID` for the first fetch.
- `filter analyze|apply` — score channels for spam/quality (flags incl. `low_engagement` —
  dead engagement: avg views <10% of subscribers over the last 100 posts); filtered channels are skipped
  unless `force=True`.
- `filter suspects` — read-only contradiction report (#1490): channels carrying the
  «авторский контент» trust tag together with filter flags — candidates for a wrong
  automatic filter decision. Changes nothing; review and decide manually.

## reading & search

```
messages read <identifier> [--limit N] [--live] [--phone PHONE] [--query TEXT]
              [--date-from DATE] [--date-to DATE] [--topic-id ID] [--offset-id ID]
              [--format text|json|csv]
search "query" [--limit N] [--mode MODE]
search-query list | get | add | edit | delete | toggle | run | stats
```

- `messages read` accepts t.me links, @usernames or internal ids as `<identifier>`;
  `--live` reads directly from Telegram instead of the local DB.
- Commands with an optional `--phone` default to the **primary account** (DB flag);
  if the primary is not connected, the first connected account is used (#1480).
- `search --mode` picks local DB (FTS5), direct Telegram API, or AI-powered search.

## dialogs — real actions in real chats

```
dialogs list | refresh | resolve | topics
dialogs read | send | forward | edit-message | delete-message | react | pin-message | unpin-message
dialogs download-media | transcribe | mark-read | archive | unarchive
dialogs participants | edit-admin | edit-permissions | kick
dialogs create-channel | create-group | leave | join
dialogs queue | status | cancel | clear-pending
dialogs cache-clear | cache-status
dialogs archive-history
```

All of these touch the user's live Telegram account — see Safety in SKILL.md.
`dialogs send` posts a message to a chat by identifier; `dialogs queue`/`status` manage the
send queue. `dialogs read` supports history reads with limits.
`dialogs transcribe <chat> <message-id> [--engine telegram|groq|openai|whisper]` returns the
text of a voice note / video note, cached in the DB (a repeat call costs nothing). Engines:
`groq` (default, needs GROQ_API_KEY — sends audio to a third party), `telegram` (native,
needs a Premium account, drops the last speech segment), `openai` (any OpenAI-compatible
endpoint via TELEGRAM_TRANSCRIBE_OPENAI_URL), `whisper` (local faster-whisper). The printed
text is a machine reading, not a verbatim quote.
`dialogs archive-history [--phone P] [--chat-id ID]` backfills the full personal-chat
history (people, bots and Saved Messages, both directions) into the local `dm_messages`
archive — no TTL, idempotent/resumable. Each run takes a FRESH dialog snapshot first
(same engine as `dialogs refresh`, rate-limited per page), then reads history per chat;
a partial snapshot or flood marks the run incomplete — rerun the command, it continues
from archive cursors. Local DB write, no Telegram send; while the worker daemon runs it
is refused by the session lock (`tg-agent stop` first — see Session lock above).

## dm — reply drafts for incoming private messages (#1429)

```
dm drafts list [--status draft|approved|rejected|sending|sent] [--limit N]
dm drafts create <incoming-dm-id> --text TEXT [--phone P] [--chat-id ID]
dm drafts approve <id>
dm drafts reject <id>
dm drafts send <id>
```

Draft lifecycle: `draft → approved → sending → sent` (atomic DB claim makes a
double send impossible); `reject` is terminal for sending — a rejected draft is
never delivered. `dm drafts send` never sends in-process: it enqueues the
`dm.send_reply` command and the worker delivers it (safe to run while the
daemon is up; repeated `send` calls on the same approved draft dedupe into one
queued command). `create` reads phone/chat from the 24h incoming-DM journal by
id; pass `--phone`/`--chat-id` explicitly if the journal row was already
pruned. Approve after send is a no-op — a delivered draft cannot be re-queued.

## content factory (LLM pipelines)

```
pipeline list | show | add | dry-run-count | edit | delete | toggle
pipeline run | generate | generate-stream | runs | run-show
pipeline queue | moderation-list | moderation-view | publish | approve | reject
pipeline bulk-approve | bulk-reject | refinement-steps
pipeline export | import | templates | from-template | ai-edit | filter | node | edge | graph
photo-loader dialogs | refresh | send | schedule-send | batch-create | batch-list | items
photo-loader batch-cancel | auto-create | auto-list | auto-update | auto-toggle | auto-delete | run-due
```

Flow: `pipeline generate` produces text (optionally an image) → run lands in moderation →
`pipeline approve` + `publish` post to the target channel. Generated runs are tracked and
inspectable via `pipeline runs` / `run-show`.

## scheduling & background

```
scheduler start | trigger | status | stop | job-toggle | set-interval | task-cancel
scheduler clear-pending | queue-pause | queue-resume
scheduler queue-status [--format json]    # live collection queue: remaining, throughput, ETA
worker    # long-lived process: collection queue, scheduler, dispatchers — run in background
```

## analytics

```
analytics top | content-types | hourly | summary | daily | pipeline-stats
analytics trending-topics | trending-channels | velocity | peak-hours | calendar
analytics trending-emojis | channel
analytics channel-rate <channel_id> | channel-rating
```

## accounts, notifications, settings, misc

```
account list | info | toggle | set-primary | delete | add
account send-code | verify-code        # interactive first-time auth — user does this themselves
account flood-status | flood-clear
notification setup | status | delete | test | dry-run | set-account
settings get | set | info | server-time | agent | filter-criteria | reactions | semantic
export json | csv | rss
translate stats | detect | run | message
image generate | models | providers | generated
provider list | add | delete | probe | refresh | test-all
debug logs | memory | timing
test all | read | write | telegram | benchmark
stop | restart
serve [--web-pass PASS] [--no-worker]   # legacy web panel (deprecated)
worker                                  # default daemon: worker runtime, no web panel
restart                                 # stop + start the worker daemon (managed PID file)
agent threads | chat | messages | ...   # legacy embedded agent
mcp-server [--no-pool]                  # legacy MCP bridge
```

`account send-code` / `verify-code` are part of interactive phone authorization — the user
runs these and types the code; agents must not drive them unattended.
