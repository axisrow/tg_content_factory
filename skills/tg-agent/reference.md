# tg-agent CLI reference

Global form: `tg-agent [--config CONFIG] <group> <command> [args]`.
Every group and command has `--help`. Where a `--format text|json|csv` flag exists, prefer
`json` when you need to parse output. Config comes from `config.yaml` + `.env` in the working
directory (auto-loaded). `TG_PROXY=socks5://user:pass@host:port` (or `http://...`) routes all
Telegram traffic through that proxy when MTProto is blocked; unset = direct.

Groups below mirror `tg-agent --help`. This file lists common commands and their notable
flags; anything not listed — check `--help`.

## channels / collection

```
channel list | add | delete | toggle | collect | stats | refresh-types | refresh-meta
channel import | add-bulk | list-for-import | tag (list|add|delete|set|get)
filter analyze | apply | reset | precheck | toggle | purge | purge-messages | hard-delete
```

- `channel collect [--channel-id ID]` — incremental collection for one channel; without flags
  interactive selection. One-off; exit when done.
- `channel stats` — per-channel collection status (`last_collected_id`, counts).
- `filter analyze|apply` — score channels for spam/quality; filtered channels are skipped
  unless `force=True`.

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
- `search --mode` picks local DB (FTS5), direct Telegram API, or AI-powered search.

## dialogs — real actions in real chats

```
dialogs list | refresh | resolve | topics
dialogs read | send | forward | edit-message | delete-message | react | pin-message | unpin-message
dialogs download-media | mark-read | archive | unarchive
dialogs participants | edit-admin | edit-permissions | kick
dialogs create-channel | create-group | leave | join
dialogs queue | status | cancel | clear-pending
dialogs cache-clear | cache-status
dialogs archive-history
```

All of these touch the user's live Telegram account — see Safety in SKILL.md.
`dialogs send` posts a message to a chat by identifier; `dialogs queue`/`status` manage the
send queue. `dialogs read` supports history reads with limits.
`dialogs archive-history [--phone P] [--chat-id ID]` backfills the full personal-chat
history (people, bots and Saved Messages, both directions) into the local `dm_messages`
archive — no TTL, idempotent/resumable. Each run takes a FRESH dialog snapshot first
(same engine as `dialogs refresh`, rate-limited per page), then reads history per chat;
a partial snapshot or flood marks the run incomplete — rerun the command, it continues
from archive cursors. Local DB write, no Telegram send; still stop the worker first:
it opens a second MTProto connection on the same session.

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
worker    # long-lived process: collection queue, scheduler, dispatchers — run in background
```

## analytics

```
analytics top | content-types | hourly | summary | daily | pipeline-stats
analytics trending-topics | trending-channels | velocity | peak-hours | calendar
analytics trending-emojis | channel
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
serve [--web-pass PASS] [--no-worker]   # legacy web panel
agent threads | chat | messages | ...   # legacy embedded agent
mcp-server [--no-pool]                  # legacy MCP bridge
```

`account send-code` / `verify-code` are part of interactive phone authorization — the user
runs these and types the code; agents must not drive them unattended.
