# Channel discovery by keywords — workflow

Find NEW channels worth collecting: sweep fresh keywords through Telegram's
premium global search, short-list the candidates, and activate only the ones
the user explicitly approved. Runs **only on an explicit user command**
(«поищи каналы») — never on its own, never on a schedule.

## 1. Keywords

Extract NEW keywords from the latest digest/news: model and company names that
appeared in the news but were never searched before. Propose 3-8 keywords,
let the user add or drop some. Keep a mental note (or ask) of previously
swept keywords — re-running the same keyword mostly re-caches the same
channels.

## 2. Sweep (real Telegram actions)

```bash
tg-agent search "<keyword1>" "<keyword2>" --mode telegram --limit 30
```

One call carries the whole keyword batch (batch mode). This is the premium
global search from the user's real account — daily quota is limited, FloodWait
is possible. On a quota or FloodWait error: STOP, report how far you got, do
not hammer retries. Keep the batch small on purpose.

Each sweep caches discovered channels into the DB as **inactive**
(`is_active=0` — search is not an operator approval) and tags the cached
messages with the keyword.

## 3. Short-list

```bash
tg-agent channel candidates --json
```

Lists inactive channels that carry keyword-tagged messages: channel_id, title,
username, message count, query tags, last_seen. Pre-filter before showing:

- keep candidates matching TODAY's keywords (the `queries` field);
- prefer channels with a @username (resolvable) and a solid message count;
- drop obvious junk: coupon/job/game aggregators, single-message ghosts;
- rank by message count and title relevance to the keyword.

Show 5-15 candidates as a short-list: title, @username, msgs, queries, one
line on why it might be worth collecting. The user picks.

## 3.5 Probe before approval (no DB write)

For each short-list candidate, sample its recent posts LIVE and get a quality
card — the channel is not added to the DB:

```bash
tg-agent channel probe <channel-id-or-username> --limit 300 --json
```

The card runs the analyzer's own thresholds (engagement vs subscribers,
forwarder share, auto-feed, uniqueness, language, suspicious username) over
the last 300 posts; 100 posts is the minimum for verdicts. Put the verdict
in the short-list: channels with `forwarder`/`auto_feed`/`low_engagement`
flags go to the bottom or into the discard pile. Cross-channel spam is only
checkable after the first collection (`filter analyze --quick`).

## 4. Activate (only explicit approvals)

For each user-approved candidate:

```bash
tg-agent channel toggle <channel-id-or-username>
tg-agent channel collect <channel-id-or-username>
```

`toggle` approves the pending candidate (`approval_state='approved'`,
`active_origin='human'` — the auto-filter logic can never undo a human
approval; the decision is audited in the decisions journal). `collect` pulls
the first history — through the worker queue if the daemon is running.

Never bulk-activate without a per-candidate yes.

## 5. Follow-up (next session, after history accumulates)

New channels can only be judged AFTER their messages are collected — the
analyzer works on local data:

```bash
tg-agent filter analyze --quick
tg-agent channel tag set <pk> keep
```

- `filter apply` = reset + re-apply: sticky flags (`manual`) and `keep`-tagged
  channels survive, everything else is re-judged by metrics.
- `non_cyrillic` and `forwarder` are labels only — they do not filter a
  channel by themselves.
- Whitelist mode (`filter apply --whitelist`) is manual and conflicts with the
  blacklist cycle — the next plain `filter apply` sweeps it away.
- Filtered channels are NOT collected by `collect_all` at all (force works
  only per-channel).

## Mechanics

- Premium search quota is per-account daily; exhausting it is normal — report
  and stop.
- `search-query` commands are an FTS counter over ALREADY collected messages —
  unrelated to discovery candidates; do not use them here.
- Discovery candidates sit in the approval queue (`approval_state='pending'`)
  and are invisible to regular collection until approved with `channel toggle`.
