# AI news digest — structured workflow

Digest over the collected DB with a fixed data schema: the agent fills the
schema, the requester formats the final post from that JSON. The schema is the
contract; presentation is decided per request (cards, sections, language,
length) — do not hardcode a layout here.

## 1. Collect the window

Default window: the last **7 days** (weekly digest). Pull candidates per keyword;
`--date-from` / `--date-to` work on the DB modes (`local`/`semantic`/`hybrid`):

```bash
tg-agent search "нейросети" --mode local --date-from 2026-10-10 --date-to 2026-10-17 --format json --limit 50
tg-agent search "LLM"        --mode local --date-from 2026-10-10 --date-to 2026-10-17 --format json --limit 50
# …one call per keyword
```

Starter keyword set (extend per request): `ИИ`, `нейросети`, `LLM`, `модель`,
`релиз`, `OpenAI`, `Anthropic`, `Google DeepMind`, `benchmark`. If the sweep is
thin, `--mode hybrid` or `--mode semantic` improve recall.

The JSON envelope carries `total`, `has_more` and `error`. Before building the
digest: `error` non-null = the keyword was NOT searched (do not pretend it
was); `has_more=true` = the window is truncated — raise `--limit` (or split
the window) and re-run, otherwise the digest is silently incomplete.

## 2. Build the structured digest

Merge all hits into ONE JSON object — the schema every consumer can rely on:

```json
{
  "date": "2026-10-17",
  "items": [
    {
      "title": "Claude 5.5 released",
      "summary": "One or two sentences: what happened and why it matters.",
      "sources": ["@channel1", "@channel2"],
      "date": "2026-10-17"
    }
  ]
}
```

- `date` (top level) — the window the digest covers.
- `items[].date` — the EVENT date, from the message text, not the repost time.
- `sources` — `@username` (or channel title) per hit, from the search JSON.

## 3. Editorial rules (non-negotiable)

- **≥2 independent channels** report it, or it is not news — a single-channel
  post is a rumor; leave it out.
- **Event date ≠ repost date**: a fresh repost of an old announcement is not
  news for this window.
- **Only verified items.** If the collected texts do not confirm the facts,
  the item does not exist. No speculation, no filler.
- **One story = one item**: duplicates across channels merge, sources
  accumulate.

## 4. Output and publish

- Build the JSON first, then format the user-facing text from it per their
  request. The JSON is the deliverable structure; the text is a view.
- Show the draft before sending. Publish to a channel only after the user
  explicitly approved target + text:

```bash
tg-agent dialogs send <chat-id-or-username> "<final text>" --yes
```

This is a real send from the user's account — Safety rules in SKILL.md apply
(confirm target + content before the first send in a task).
