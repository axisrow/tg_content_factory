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

Commands without a queue equivalent (`messages read --live`, `dialogs read`,
`dialogs download-media`, `account info`, `search --mode telegram|my_chats|channel`,
`notification …`, `photo-loader …`, `channel add|import|add-bulk|list-for-import|refresh-types|refresh-meta|probe`,
`collect sample`, `pipeline publish`, `ping create --username`, `test telegram`)
are **refused before any connection is attempted** while the daemon runs (#1521) — exit 1,
message on stderr:

```
Error: A running tg-agent daemon (PID 12345) owns the Telegram sessions, and this command
needs its own connection (it has no worker hand-off). Stop the daemon first
(`tg-agent stop`), run the command, then `tg-agent restart`.
```

This is intentional: a second simultaneous connection with the same auth key risks
Telegram killing the session (`AuthKeyDuplicatedError`). Stop the daemon first — or switch
to a hand-off `dialogs` action, which the running worker executes without a second
connection. A refused command never disturbs the live session. Two exceptions run
next to the daemon instead of being refused: `pipeline run` / `pipeline generate` generate
without Telegram access (a note goes to stderr; Telegram-side nodes — react/forward/
delete-message/edit-message — fail without a pool, so pipelines containing them must
wait for `tg-agent stop`), and the
DB-only `dialogs queue …` / `dialogs cache-status` (the latter then shows only the DB cache).

`TGCF_CLI_DIRECT=1` (or `--direct` where a command has one) skips this check. The
per-session lock below it still applies: the command waits for the daemon to release the
session (up to `TGCF_SESSION_LOCK_GRACE_SECONDS`, default 20) and then fails with
`SessionLockError: Another tg-agent process is already connected with this session (lock
.../session-<hash>.lock held by PID 12345)` — useful only while a daemon is shutting down.
When the holder exits, the lock clears on its own.

## Daemon lifecycle — check before you touch (#1714)

The managed daemon is project-scoped but its PID file can sit on any store path the
project used since launch (#1698). Before `stop`/`restart`, run:

```
daemon status          # read-only: daemon up/down, PID, PID file, project/store roots,
                       # executable (recorded + observed), started_at; exit 0 even when down
```

- `stop`/`restart` probe every candidate PID file (current store, pre-switch store,
  legacy marker store) and terminate whichever daemon is alive — the documented
  `global enable` → `restart` flow no longer orphans the previous daemon (#1698).
- A `stop`/`restart` from a directory that does not own the running daemon is REFUSED
  with provenance output (exit 1); the daemon survives. `worker`/`serve` refuse to
  double-spawn next to a daemon registered on a previous store path.
- A `restart` whose start phase fails prints a loud `RESTART FAILED … NO daemon is
  running now` banner — never assume a restart succeeded; verify with `daemon status`.
- `stop --force` is the human-operator escape hatch (#1714): it overrides the
  provenance refusal but still only signals a live managed tg-agent daemon. Agents do
  not use `--force` without explicit operator instruction.

## channels / collection

```
channel list | add | delete | toggle | collect | stats | refresh-types | refresh-meta | probe
channel import | add-bulk | list-for-import | candidates | pending-list | reject | tag (list|add|delete|set|get)
filter analyze | apply | reset | precheck | suspects | toggle | flag | purge | purge-messages | hard-delete
```

- `channel probe <id|@username> [--limit 300] [--json]` — quality card from a LIVE
  sample of the last N posts, WITHOUT adding the channel: flags use the analyzer's own
  thresholds (engagement vs subscribers, forwards share, auto-feed, uniqueness,
  language, suspicious username). DB-history metrics (cross-channel spam) are listed
  as not checked. Read-only live read.
- `channel collect <channel-id-or-username>` — incremental collection for one channel; the
  identifier argument is required (there is no interactive selection). One-off; exit when
  done. With a live daemon the channel is queued
  to the worker (no second MTProto client); `TGCF_CLI_DIRECT=1` forces the in-process path.
  A pending-approval identifier is reported as such with the approve/reject commands
  instead of the misleading "not found" (#1701).
- `channel stats` — queue stats collection: one channel by identifier; `--all` refreshes
  channels whose stats are missing **or older than** `stats_all_skip_fresh_hours` (24h),
  oldest first, capped per run at `stats_all_max_channels_per_run` (10; `--max-channels N`
  overrides). The worker collects asynchronously; numbers land in DB — read them via
  `channel list` / `analytics channel`.
- `channel candidates [--json] [--limit N]` — read-only list of channels discovered by
  premium search (`search --mode telegram`): pending-approval rows whose cached messages carry the
  search-query tag. Pick ones worth collecting, then `channel toggle <identifier>`
  (approves with human origin) + `channel collect <identifier>` for the first fetch.
- `channel list [--pending | --all]` — the regular table hides pending rows (#1313 gate).
  `--pending` shows only the approval queue; `--all` appends its rows marked in the
  Approval column (#1701).
- `channel pending-list` — the full approval queue (#1314): every pending row, including
  ones without search-tagged messages that `candidates` skips.
- `channel add <identifier>` — manual add of a channel that is already in the approval
  queue approves it (human origin, journaled): the explicit add IS the operator's
  decision (#1701). A fresh add creates the row approved, by @username or numeric ID alike.
- `channel reject <identifier>` — refuse a pending-approval candidate: deletes the pending
  row and its cached messages; the decision stays in the `decisions` journal.
- `filter analyze|apply` — score channels for spam/quality (flags incl. `low_engagement` —
  dead engagement: avg views <10% of subscribers over the last 100 posts; `forwarder` —
  forwarding channel: ≥50% of the last 100 posts are forwards, label-only; `high_volume` —
  mass poster: ≥100 messages/24h AND above the P95 of per-channel 24h volume); the uniqueness /
  subscriber-ratio / cross-dupe / cyrillic thresholds are overridable via
  `settings filter-criteria` (defaults built in, out-of-range values fall back to defaults);
  the four text metrics of a full analyze come from an incremental stats cache (#1679) —
  no full-table scans for them; cross-dupe still scans history (the slow tail on very
  large DBs) until the follow-up cache lands; `filter analyze --rebuild` recomputes the
  cache from full history (exact manual mode, first run on a large DB takes tens of minutes);
  filtered channels are skipped unless `force=True`.
  Channels approved by a human
  (`filtered_origin='human'`) are invisible to the analyzer — no metrics, no verdicts,
  no proposals; `--channel-id <human>` prints «No channels found.» (#1312).
- `filter suspects` — read-only contradiction report (#1490): channels carrying the
  «авторский контент» trust tag together with filter flags — candidates for a wrong
  automatic filter decision. Changes nothing; review and decide manually.
- `filter flag <pk> --set manual` / `--unset manual` — set/unset the operator sticky
  flag (#1499). `--set` marks the channel as a durable human verdict
  (`is_filtered=1`, `origin=human`); the flag survives reset→apply cycles. `--unset`
  returns the channel to automatic verdicts. Other flags (analyzer/system) are not
  settable by hand.
- `filter apply --whitelist` — whitelist mode (#1490, MVP): filters every channel
  WITHOUT the «авторский контент» tag (`no_author_content` flag) and unfilters tagged
  ones (trust beats flags; `keep`-tag escrow and human decisions are respected).
  Tag-only verdict — `analyze` is not run. Open design questions (default mode,
  migration, blacklist interplay) are tracked in #1490.

## reading & search

```
messages read <identifier> [--limit N] [--live] [--phone PHONE] [--query TEXT]
              [--date-from DATE] [--date-to DATE] [--topic-id ID] [--offset-id ID]
              [--format text|json|csv]
search "query" ["query2" ...] [--limit N] [--mode MODE] [--date-from DATE]
       [--date-to DATE] [--format text|json|csv]
search-query list | get | add | edit | delete | toggle | run | stats
```

- `messages read` accepts t.me links, @usernames or internal ids as `<identifier>`;
  `--live` reads directly from Telegram instead of the local DB.
- **Exit-code contract (#1486):** a failed command prints its error to **stderr**
  and exits 1 (parser usage errors exit 2); **stdout stays for results**.
  Empty-but-valid results ("No messages found.") are not failures — they print
  normally and exit 0. Branch on the exit code, not on parsing stdout text.
  This holds repo-wide for `messages`/`dialogs`/`account`/`settings`/`provider`/
  `export`/`scheduler`/`filter`/`photo-loader` (and pre-existing `dm`/`export`
  checks); remaining groups may still print-and-exit-0 — see issue #1486.
  Known exception: `dialogs react` without `--emoji`/`--clear` still prints
  its usage error to stdout and exits 2.
- `--format json` always emits valid JSON (empty result → `[]`); `--format csv`
  always includes the header row. Errors (e.g. channel not found in DB) go to
  **stderr** with exit code 1. `--live` combined with `--format json|csv` is
  rejected with exit code 1 — live output is text-only (#1598).
- Forum messages carry `topic_id` plus the cached `topic_title` (#1552): text output
  renders `topic=<id> (<title>)`, JSON/CSV expose both fields. Titles come from the
  collector-maintained cache; an unknown/deleted topic renders without a title.
- Commands with an optional `--phone` default to the **primary account** (DB flag);
  if the primary is not connected, the first connected account is used (#1480).
- `search --mode` picks local DB (FTS5), direct Telegram API, or AI-powered search.
  `--date-from`/`--date-to` constrain the window on the DB modes only
  (`local`/`semantic`/`hybrid`) and are rejected with an error on live modes;
  both take `YYYY-MM-DD` — a relaxed `YYYY-M-D` (`2026-10-5`) is normalized to
  the canonical form, a full ISO datetime keeps exact precision and is
  normalized to UTC (an explicit offset like `2026-10-20T13:00:00+03:00` is
  converted to the same instant `2026-10-20T10:00:00+00:00`; naive datetimes
  are treated as UTC), so equivalent bounds match regardless of notation, and
  anything else is a usage error with exit 2, never a silently empty/oversized
  window (#1593, #1666);
  `--format json|csv` renders the hits with `@username`/title attribution —
  the AI digest workflow ([digest.md](digest.md)) is built on these.
- **Batch mode (#1668):** 2+ positional queries — `search "нейросети" "LLM" "OpenAI" …`
  — run every keyword against the same shared filters in ONE call and print a
  single JSON object `{"queries": [<envelope>, ...]}`; each envelope carries the
  same `query/total/has_more/error/messages` contract (completeness is judged
  per keyword). Batch requires `--format json`; live modes follow the same
  rules as single-query calls (no `--date-from/--date-to`).

## dialogs — real actions in real chats

```
dialogs list | refresh | resolve | topics
dialogs read | send | forward | edit-message | delete-message | react | pin-message | unpin-message
dialogs download-media | transcribe | mark-read | archive | unarchive
dialogs participants | edit-admin | edit-permissions | kick
dialogs create-channel | create-group | leave | join | delete
dialogs queue | queue status | queue cancel | queue clear-pending
dialogs cache-clear | cache-status
dialogs archive-history
```

All of these touch the user's live Telegram account — see Safety in SKILL.md.
`dialogs send` posts a message to a chat by identifier (links render without a
Telegram web preview); `dialogs queue` (with the
`status`, `cancel`, `clear-pending` subcommands) manages the send queue.
`dialogs read` supports history reads with limits.
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
`dialogs download-media` writes to `--output-dir` (default: current directory); the queued
command and the agent tool (which carry no directory by contract) land files in the runtime
data root (`./data/downloads`, overridable via `TG_AGENT_DATA_DIR`) — never inside the
installed package (#1595).

## dm — incoming-DM journal, archive, reply drafts (#1429, #1580)

```
dm status
dm journal list [--limit N] [--offset N]
dm archive list [--phone P] [--chat-id ID] [--limit N]
dm drafts list [--status draft|approved|rejected|sending|sent] [--limit N]
dm drafts create <incoming-dm-id> --text TEXT [--phone P] [--chat-id ID]
dm drafts approve <id>
dm drafts reject <id>
dm drafts send <id>
```

`dm status` shows the DM listener/catch-up state the worker publishes in the
`dm_listener_status` snapshot (liveness, per-account update counts, unprocessed
journal rows, last catch-up runs) plus the effective catch-up settings read
from the `dm_catchup` setting; invalid settings JSON fails loudly with a
`Catch-up settings INVALID` error (exit 1) instead of silently falling back to
defaults (#1581). Without a live worker the snapshot section says so honestly;
settings are still shown.

`dm journal list` shows the incoming-DM journal (24h TTL window, fresh first)
with the ids the drafts flow needs; `dm archive list` reads the persistent
dm_messages archive (both directions, no TTL, optional phone/chat filters).

Draft lifecycle: `draft → approved → sending → sent` (atomic DB claim makes a
double send impossible); `reject` is terminal for sending — a rejected draft is
never delivered. `dm drafts send` never sends in-process: it enqueues the
`dm.send_reply` command (payload carries the account phone) and the worker
delivers it (safe to run while the daemon is up; repeated `send` calls on the
same approved draft dedupe into one queued command). `create` reads
phone/chat from the 24h incoming-DM journal by id; pass `--phone`/`--chat-id`
explicitly if the journal row was already pruned; creating a draft marks the
journal row processed. Approve after send is a no-op — a delivered draft
cannot be re-queued.

## ping — friendly pings for dormant DM chats (#1415)

```
ping create --phone P (--dialog-id ID | --username NAME) --text TEXT [--days N] [--title T]
ping list [--active-only]
ping update <id> [--text TEXT] [--days N] [--active | --inactive]
ping delete <id>
ping run-due
```

Monitors 1:1 dialogs: if the last message in the chat is yours and older than the
base threshold (`--days`, default 30), the worker sends `--text` to the contact and
repeats on a decaying schedule (+1 day, +7, +30, then every 30 days) until the
contact replies — a reply resets the cycle back to the base threshold. Checking
runs hourly as the `ping_due` scheduler job; `ping run-due` triggers it immediately
and performs real Telegram reads/sends. While the daemon is up, `run-due` hands off
to it instead (queues a PING_DUE task, deduped) — no second pool, no double send.
The CRUD commands are local DB writes and
safe to run while the daemon is up.

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

`pipeline approve | reject | bulk-approve | bulk-reject` are operator decisions
(`moderation_origin='human'`, journaled in `decisions`): automation never overrides
them — the AUTO pipeline and agent tools cannot re-approve a run the operator
rejected; the suppressed attempt is journaled instead (#1315).
`bulk-approve | bulk-reject` print a `Run id=N is publishing/published, skipping.` line
for every run already in a terminal state and leave it out of the `Bulk …: N/M` count.

## scheduling & background

```
scheduler start | trigger | status | stop | job-toggle | set-interval | task-cancel
scheduler start    # writes scheduler_autostart=1 + reconciles the running worker; exits
scheduler clear-pending | queue-pause | queue-resume
scheduler queue-status [--format json]    # live collection queue: remaining, throughput, ETA
# scheduler status: shows live worker state from runtime_snapshots (heartbeat age, is_running,
# next-run per job, queue paused/running) when a worker publishes; config-only otherwise.
worker    # long-lived process: collection queue, scheduler, dispatchers — run in background
```

`trigger`, `task-cancel <id>` and `clear-pending` are live RPC when the daemon runs:
each enqueues a telegram_command and acks with its id, and the worker acts on the
live objects (`trigger` runs the enqueue-all collection; `task-cancel` interrupts a
RUNNING collection at the collector's next check; `clear-pending` also drains the
worker's in-memory queue and flood-wait timers). Without a daemon the DB effect is
immediate; the command executes on the next worker start.

`job-toggle` (and `settings set scheduler_job_disabled:<id>`) journals the decision;
the scheduler's startup migration of the legacy pre-#835/2 disabled state never
re-enables a journaled job, so an operator-disabled `content_generate_<id>` stays
off across restarts, fresh DBs and restored dumps (#1315).

## analytics

```
analytics top | content-types | hourly | summary | daily | pipeline-stats
analytics trending-topics | trending-channels | velocity | peak-hours | calendar
analytics trending-emojis | channel
analytics channel-rate <channel_id> | channel-rating
analytics usefulness [--limit 100] [--json]
```

`analytics usefulness` — content rating per channel, «полезное vs вода»
(scenario 4): deterministic heuristics over each channel's last 200 posts —
`water_score` (stamp/filler density, Ильяхов «Пиши, сокращай»), authorship
profile `own/quality_retell/rewrite` shares (same keys as the judge's
`authorship` axis), and a tier (`useful`/`mixed`/`water`/`no_data`;
`no_data` = fewer than 30 posts in the sample). Read-only, never
calls an LLM. Tiers are calibration-grade labels for curation, not collection
blocks (no is_filtered effect).

`analytics channel-rate <channel_id>` — LLM judge (water/authorship rubric +
useful/genre). CALIBRATION ONLY: costs provider spend; its verdicts are the
ground truth that calibrates `usefulness` thresholds, never a production path.

## plugins — installed plugin tools

```
plugin list [--json]
plugin run TOOL [OPTIONS...] [--with-pool]
```

Plugins install via the `tg_agent.plugins` entry-point group and register agent tools.
`plugin list` prints one tab-separated line per plugin —
`id <TAB> version <TAB> status <TAB> tools <TAB> reason` (`-` = missing version/tools,
tools comma-joined) or `No plugins installed`; `--json` prints a JSON array of
`{id, version, status, reason, tools}` records. Statuses: `active`, `disabled`
(via `plugins_disabled` config), `incompatible` (plugin API mismatch), `failed`
(load/setup error — `reason` says why). DB + entry-points only — safe to run while
the daemon is up.

`plugin run` generates flags from the tool's JSON Schema at runtime: every schema
property becomes `--name` (underscores → dashes); `boolean` → `--name/--no-name`,
`integer`/`number`/`string` take typed values, any other shape (`object`, `array`,
multi-type) takes a JSON value (`--opts '{"limit": 5}'`). Only flags actually passed
on the command line are forwarded; `help` and `with-pool` are reserved, an unknown
flag is a usage error. The tool result prints as one JSON line on stdout; errors go
to stderr as `Error: <detail>` — exit 1 (unknown tool, invalid arguments, access
denied, no live runtime), exit 2 (flag/usage errors). Phone-bound tools need a live
Telegram connection: pass `--with-pool` (refused by the session lock while the daemon
runs — `tg-agent stop` first); without `--with-pool` they fail with
"Live Telegram runtime is unavailable".

## accounts, notifications, settings, misc

```
account list | info | toggle | set-primary | delete | add
account send-code | verify-code        # interactive first-time auth — user does this themselves
account flood-status | flood-clear
global enable | disable | status       # shared account store across projects (#1683)
# global: `enable` attaches the current project to the machine-wide account
# gateway (~/.tg-agent, TG_AGENT_GLOBAL_DIR overrides) via a .tg-agent-global
# marker (JSON binding) in the project root. Accounts are shared through the
# gateway; the project DATABASE ALWAYS STAYS PROJECT-LOCAL — global auth never
# implies a global database (#1724). Requires explicit --account PHONE grants
# (repeatable). Legacy plain-path markers no longer redirect anything (status
# flags them as inert; replace with `enable`). `disable` detaches the project
# (local files untouched); `status` prints the marker format and the effective
# database path. Restart any running worker/daemon after enable/disable.
style init | list | show --channel <id>  # local-only per-channel style specs (#1687)
# style: per-channel posting-voice specs (skeleton, nicknames, emoji policy, digest
# rules) stored OUTSIDE the repository in ~/.tg-agent/designs/<key>.md
# (TG_AGENT_GLOBAL_DIR overrides) — never committed (#1584). `init` bootstraps the
# placeholder template for a channel (username, t.me link or numeric id) and refuses
# to overwrite an existing spec without --force; `list` shows the specs; `show`
# prints the spec — read it to render content in the channel's voice, and fill a
# fresh template in together with the user.
notification setup | status | delete | test | dry-run | set-account
telegraph login --short-name N [--author-name A] [--author-url U]
telegraph create --title T --file f.md [--author-name A] [--author-url U] [--format json]
# telegraph: publishes a public telegra.ph page from markdown (#1703). `login`
# runs once per project: it creates a (disposable) Telegraph account via
# createAccount and stores its access token in settings as
# telegraph_access_token — a secret (enc:v2 via set_secret, masked in every
# settings surface; needs SESSION_ENCRYPTION_KEY). A live probe showed
# createPage rejects anonymous calls (ACCESS_TOKEN_INVALID), so the token is
# required. `create` converts a markdown subset (h1–h4, lists,
# bold/italic/links, bare URLs, `code`, quotes, hr) to Telegraph nodes — body
# from --file or stdin — prints the page URL (--format json = full API
# response). telegra.ph has no tables — lay them out as lists/paragraphs
# yourself. Pair with `dialogs send` for title+link posts.
settings get | set | info | server-time | agent | filter-criteria | reactions | semantic
# settings: secret-looking keys (*_api_key, *_token, *_secret, session_secret_key) are
# masked in get/set output; settings semantic --api-key and `set semantic_embeddings_api_key`
# store the key encrypted (enc:v2) and are refused without SESSION_ENCRYPTION_KEY (#1597).
# settings set: runtime keys reach the live worker too — collection_queue_paused
# (values true/1/yes/on → pause, else resume) enqueues collection.pause/resume;
# scheduler_autostart, *_interval_minutes, scheduler_job_disabled:* enqueue scheduler.reconcile.
export json | csv | rss
translate stats | detect | run | message
image generate | models | providers | generated
provider list | add | delete | probe | refresh | test-all
debug logs | memory | timing
test all | read | write | telegram | benchmark
stop | restart [--force]
daemon status                            # read-only: is the project daemon up, whose is it (#1714)
serve [--web-pass PASS] [--no-worker]   # legacy web panel (deprecated)
worker                                  # default daemon: worker runtime, no web panel
restart                                 # stop + start the worker daemon (managed PID file)
agent threads | chat | messages | ...   # legacy embedded agent
mcp-server [--no-pool]                  # legacy MCP bridge
```

`account send-code` / `verify-code` are part of interactive phone authorization — the user
runs these and types the code; agents must not drive them unattended.

`account toggle` is an operator decision (`active_origin='human'`, journaled): a later
re-auth (`verify-code`, QR, `import --force`) refreshes the session but keeps a
disabled account disabled and out of the pool; re-enable it with `account toggle` (#1315).
