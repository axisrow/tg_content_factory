# Chat seeding — composer + publisher workflow (#1756, #1757)

Compose realistic multi-participant dialogues for the owner's own chats
(epic #1751). Real chat speech is the prompt input (`seeding samples`, the
composing method is [seeding-voice.md](seeding-voice.md)); the learned
statistics bound the timing; the scenario JSON is the contract; validation is
a loop; drafts are recorded through the CLI.
The composing LLM is Claude Code itself (z.ai is banned). The publisher
(approve/schedule/cancel/status, #1757) turns an approved draft into a
worker-run send — gated on explicit owner consent (chat + content).

## 1. Read the real speech, then learn the chat profile

```bash
tg-agent seeding samples --chat <chat-id> --days 14
tg-agent seeding stats <chat-id> --days 14
```

`samples` comes FIRST: the most recent conversation windows verbatim
(sender, pause, full text; media as `[photo]`) plus the voices of the top
real senders (their latest messages). Compose from these windows — few-shot
style transfer of rhythm, register, slang and reply manner, not content —
following [seeding-voice.md](seeding-voice.md). `--format json` for
parsing; `--windows/--window/--senders/--per-sender/--gap-minutes/
--min-messages/--min-speakers` tune the selection. Few qualifying windows →
widen `--days` or ask the owner to backfill (`channel collect <chat-id> --full`).

`stats` is for timing and bounds only. It returns ONE JSON object — the real-chat profile computed from collected
messages (deterministic, no LLM): `length` (percentiles + mean),
`gaps_seconds` (inter-message pauses; `max` is the longest observed silence),
`active_hour_counts` (UTC hours; missing hour = quiet), `quiet_hours`,
`emoji_share`/`top_emojis`, `style` (question/exclamation/lowercase/link
shares, `top_tokens`), `per_sender` (activity per participant — the basis
for persona count). `--days` widens/narrows the window; keep 14 unless the
chat is thin.

Chat id: the Telegram-marked group/supergroup id (`-100…`) or the bare id
the DB stores (`1997034059`) — every seeding read accepts both.

## 2. Compose the scenario JSON

Fill this schema — it mirrors the `seeding_messages` columns (#1755):

```json
{
  "chat_channel_id": -1001234567890,
  "personas": [
    {"name": "Миша", "account_phone": "+79990000001",
     "style": {"vocab": ["короче", "в общем"], "punctuation": "без точек в конце",
               "activity_hours": [10, 23], "avg_len": 60}},
    {"name": "Даша", "account_phone": "+79990000002",
     "style": {"vocab": ["прива"], "punctuation": "многоточия…",
               "activity_hours": [9, 22], "avg_len": 110}}
  ],
  "messages": [
    {"position": 1, "persona": "Миша", "text": "ты видел что они выкатили",
     "delay_seconds": 420, "reply_to_position": null},
    {"position": 2, "persona": "Даша", "text": "дааа, уже смотрю…",
     "delay_seconds": 610, "reply_to_position": 1}
  ]
}
```

Rules (the validator enforces most of them — do not wait for it to fail):

- **Source**: compose from the `samples` windows, not from the stats —
  transfer a real window's skeleton (speaker order with bursts, per-slot
  length, pause shape) onto a new evergreen topic; strip AI tells; never
  post a rewrite of a real window ([seeding-voice.md](seeding-voice.md)).
- **Personas**: 2–4, one per participating account (`account_phone` unique),
  each anchored to one real sender's voice from `samples`.
  A persona is CONSISTENT across sessions: fix vocabulary, punctuation and
  the activity window in `style` and reuse the same file for the same chat.
- **No echo**: accounts never repeat each other's (or their own) phrasings —
  no duplicate texts, ignore-insensitive.
- **Timing**: sample `delay_seconds` from the learned `gaps_seconds`
  distribution (mostly between p25 and p75, an occasional long pause up to
  `max`); never below the chat's fast tail; messages must land in active
  hours (UTC) — respect `quiet_hours` by stretching the previous delay.
- **Two accounts never post in rapid succession**: a persona change needs a
  comfortable gap (≥20s even in fast chats).
- **Lengths vary**: no three consecutive messages of the same length
  (`length` percentiles are the guide).
- **Voice**: the anchored sender's register first (short fragments, no
  closure, people talk past each other); then the chat norms — occasional
  typos, lowercase starts if the chat does that, `emoji_share`-consistent
  emoji use from `top_emojis`, only the chat's own vocabulary (`validate`
  lists the words it never used).
- **No links ever** — spam-reportable; seeds with URLs are rejected.
- Telegram caps a message at 4096 chars.

## 3. Validate (loop until clean)

```bash
tg-agent seeding validate scenario.json --chat <chat-id> [--days 14] [--start-at "2026-11-02T18:00"]
```

Exit 1 lists violations; fix the scenario and re-run. A clean run prints
`Scenario OK.` and may then list, per message, the words the chat's members
never wrote (`#5: бутстрап, градиент`; vocabulary = all collected history,
not `--days`; channel posts and the anonymous admin excluded). That list is a
hint with exit code 0, but in the 08.11 blind test exactly such words gave
the seeds away: replace each with a word the chat uses, keep one only on
purpose ([seeding-voice.md](seeding-voice.md) §6). `--start-at` (ISO;
naive = UTC) additionally checks that no message lands in `quiet_hours` —
always pass it before drafting. The scenario file must pass schema
validation (`personas` unique names/phones, `messages` positions 1..N,
`reply_to_position` points backwards).

## 4. Record drafts

```bash
tg-agent seeding thread-create --chat <chat-id> --personas personas.json
tg-agent seeding add scenario.json --thread <thread-id> --start-at "2026-11-02T18:00"
```

`thread-create` stores the persona map «name → account_phone» in
`personas_json` (#1755) — the full persona styles stay in your local
personas file; reuse the same file for the same chat across sessions
(keep it outside the repo, like style specs:
`~/.tg-agent/seeding/<chat-id>/personas.json`, next to the chat passport,
[seeding-voice.md](seeding-voice.md) §3). `add` validates the
scenario against the learned stats and records draft rows in
`seeding_messages` with `status=draft` and `planned_at` derived from the
delays — the draft schedule is visible immediately via
`seeding show <thread-id>`. Approve/schedule/cancel live in the publisher
workflow, not here; a thread stops accepting scenario writes once it
leaves `draft`.

## 5. Publisher: approve → schedule → watch (#1757)

Publishing is a separate explicit run — every publish run needs the owner's
consent (chat + content). NEVER run approve/schedule without an owner request
naming the chat. Manual-first: no scheduler automation until 2–3 clean runs.

```bash
tg-agent seeding approve <thread-id>
tg-agent seeding schedule <thread-id> [--days 14]
tg-agent seeding status <thread-id>
tg-agent seeding cancel <thread-id>
```

- `approve` is the owner's YES: thread `draft→scheduled`, messages
  `draft→approved` (atomic). It does NOT send anything.
- `schedule` runs the safety rails and puts the scenario on the schedule +
  enqueues a SEED_DUE worker task (earliest `planned_at`). The rails reject
  the WHOLE scheduling on: any link in any text (spam-reportable), a gap < 20s
  between consecutive messages of different accounts, any message planned more
  than 5 minutes in the past (an overdue prefix would fire as one burst —
  re-record the scenario with a future `--start-at`), a per-account daily
  total > 12 replicas (UTC day, all threads counted), or any message landing
  in the chat's learned quiet hours — fix the schedule (re-record the
  scenario in a fresh draft thread) and come back.
- The worker claims due replicas one by one (atomic `scheduled→sending`
  claim — a re-run never double-sends), sends them from each message's own
  account with the reply chain (`reply_to` = previous replica's
  `tg_message_id`), and records `sent` (+ id) or `failed` (+ error). The
  next SEED_DUE is chained automatically on the remaining `planned_at` —
  no cron. A worker restart mid-send fails the in-flight replica honestly
  (`sending→failed`, verify the chat manually) instead of re-sending.
- `status` shows message status counts, how many are due now, the next
  planned send and whether a SEED_DUE task is active — run it instead of
  guessing. `cancel` stops a thread (unsent messages cancelled; in-flight
  or already-sent are untouched).
- Escalate `failed` replicas to the owner: check the chat, then decide —
  cancel the thread or re-record. There is no CLI reschedule verb (yet).

## Safety

- The seeding accounts also power collection: conservative rates only
  (a thread of 5–12 messages per chat per session, not dozens).
- Never run this workflow without an explicit owner request naming the
  target chat; publishing (separate workflow) always asks consent again.
- The stats read is DB-only and safe; `validate` is file-only.
