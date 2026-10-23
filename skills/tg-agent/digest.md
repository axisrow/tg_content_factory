# AI news digest — structured workflow

Digest over the collected DB with a fixed data schema: the agent fills the
schema, the requester formats the final post from that JSON. The schema is the
contract; rendering follows the requester's channel voice (the operator keeps
the voice spec locally).

## 1. Collect the window

Default window: the last **7 days** (weekly digest). Pull candidates per keyword;
`--date-from` / `--date-to` work on the DB modes (`local`/`semantic`/`hybrid`).
Batch mode takes ALL keywords in ONE call:

```bash
tg-agent search "нейросети" "LLM" "OpenAI" "Anthropic" --mode local \
  --date-from 2026-10-10 --date-to 2026-10-17 --format json --limit 200
```

Starter keyword set (extend per request): `ИИ`, `нейросети`, `LLM`, `модель`,
`релиз`, `OpenAI`, `Anthropic`, `Google DeepMind`, `benchmark`. If the sweep is
thin, `--mode hybrid` or `--mode semantic` improve recall.

The response is ONE JSON object `{"queries": [<envelope>, ...]}` — one envelope
per keyword, in call order. Each envelope carries `total`, `has_more` and
`error`. Before building the digest check EVERY envelope: `error` non-null =
that keyword was NOT searched (do not pretend it was); `has_more=true` = that
keyword's window is truncated — raise `--limit` (or split the window) and
re-run, otherwise the digest is silently incomplete.

Envelopes come back in call order; messages inside each are date-descending, so a week-sized window overflows any
`--limit` fast and the OLDEST days drop out first. Split the sweep into
sub-windows of 3-4 days and merge (measured 18.10: a 7-day sweep at
`--limit 50` silently lost days 1-4 of the window).

## 2. Build the structured digest

Merge all hits into ONE JSON object — the schema every consumer can rely on:

```json
{
  "date": "2026-10-17",
  "items": [
    {
      "title": "Claude 5.5 released",
      "summary": "One or two sentences: what happened and why it matters.",
      "link": "https://www.anthropic.com/news/claude-5-5",
      "sources": [
        {"username": "@channel1", "message_id": 123},
        {"username": "@channel2", "message_id": 456}
      ],
      "date": "2026-10-17"
    }
  ]
}
```

- `date` (top level) — the window the digest covers.
- `items[].date` — the EVENT date, from the message text, not the repost time.
- `link` — the item's PRIMARY source URL (repo/blog/status page — the thing the
  news is about), extracted from the hit texts during collection. Exactly one
  per item; it becomes the item's render link.
- `sources` — provenance per hit, from the search JSON: `@username` +
  `message_id` (renders as `https://t.me/<username>/<message_id>`); channel
  title only as fallback when there is no username. t.me stays in `sources`
  as provenance only, never the render link.

## 3. Selection and editorial rules

- **Ranking is mechanical, not editorial**: order and cut stories by
  DISTINCT-CHANNEL COVERAGE — how many independent channels wrote about the
  story. Default digest = **top-20**. A story covered by 2 channels loses to
  one covered by 15, however good it is.
- **≥2 independent channels** is the inclusion floor — a single-channel post
  is a rumor; leave it out.
- **Event date ≠ repost date**: a fresh repost of an old announcement is not
  news for this window.
- **Only verified items.** If the collected texts do not confirm the facts,
  the item does not exist. No speculation, no filler.
- **No primary source = no item.** If no hit text carries the original URL
  and it cannot be resolved otherwise, the story is dropped — do not
  substitute a stack of t.me links for the missing original.
- **One story = one item**: duplicates across channels merge, sources
  accumulate. Brand-token channel counts are cluster noise (jobs posts,
  coupon spam) — count the story, not the brand.

## 4. Output and publish

- Build the JSON first, then render the user-facing text in the requester's
  channel voice (their local spec), not your own taste.
- **Item template (owner-confirmed 2026-10-22)** — title, blank line, body
  (fact → numbers → verdict), blank line, then ONE bare URL: the item's
  `link` (primary source). No markdown labels around it. A stack of `t.me`
  links instead of the original is WRONG format — t.me is fallback only. An
  item without a resolvable link does not go out: no links = no item.
- Telegram caps one message at 4096 chars: send long digests in parts,
  splitting BETWEEN items — a link must never land in a different message
  than its text.
- Show the draft before sending. Publish to a channel only after the user
  explicitly approved target + text:

```bash
tg-agent dialogs send <chat-id-or-username> "<final text>" --yes
```

A published post is corrected in place (`dialogs edit-message`), not deleted.
This is a real send from the user's account — Safety rules in SKILL.md apply
(confirm target + content before the first send in a task).
