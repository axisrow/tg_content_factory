# Chat seeding — dialogue composer workflow (#1756)

Compose realistic multi-participant dialogues for the owner's own chats
(epic #1751). The learned statistics are the prompt input; the scenario JSON
is the contract; validation is a loop; drafts are recorded through the CLI.
The composing LLM is Claude Code itself (z.ai is banned). Publishing is NOT
part of this workflow — approve/schedule/cancel live in the publisher, and
every publish run needs the owner's explicit consent (chat + content).

## 1. Learn the chat profile

```bash
tg-agent seeding stats <chat-id> --days 14
```

Returns ONE JSON object — the real-chat profile computed from collected
messages (deterministic, no LLM): `length` (percentiles + mean),
`gaps_seconds` (inter-message pauses; `max` is the longest observed silence),
`active_hour_counts` (UTC hours; missing hour = quiet), `quiet_hours`,
`emoji_share`/`top_emojis`, `style` (question/exclamation/lowercase/link
shares, `top_tokens`), `per_sender` (activity per participant — the basis
for persona count). `--days` widens/narrows the window; keep 14 unless the
chat is thin.

Chat id is the negative group/supergroup id, as in `dialogs list`.

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

- **Personas**: 2–4, one per participating account (`account_phone` unique).
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
- **Voice**: informal per chat norms — occasional typos, lowercase starts if
  the chat does that, `emoji_share`-consistent emoji use from `top_emojis`,
  vocabulary close to `top_tokens`.
- **No links ever** — spam-reportable; seeds with URLs are rejected.
- Telegram caps a message at 4096 chars.

## 3. Validate (loop until clean)

```bash
tg-agent seeding validate scenario.json --chat <chat-id> [--days 14] [--start-at "2026-11-02T18:00"]
```

Exit 1 lists violations; fix the scenario and re-run. `--start-at` (ISO;
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
(keep it outside the repo, like style specs). `add` validates the
scenario against the learned stats and records draft rows in
`seeding_messages` with `status=draft` and `planned_at` derived from the
delays — the draft schedule is visible immediately via
`seeding show <thread-id>`. Approve/schedule/cancel live in the publisher
workflow, not here; a thread stops accepting scenario writes once it
leaves `draft`.

## Safety

- The seeding accounts also power collection: conservative rates only
  (a thread of 5–12 messages per chat per session, not dozens).
- Never run this workflow without an explicit owner request naming the
  target chat; publishing (separate workflow) always asks consent again.
- The stats read is DB-only and safe; `validate` is file-only.
