# Seeding voice — compose from real chat speech (#1793)

Seeded dialogues read as AI when the composer writes from aggregates. `seeding stats`
measures the chat (length percentiles, gaps, top tokens), but measurements are not speech:
slang, register, message rhythm and the way people hook their replies live only in the
messages themselves. This page is the composing method: **read real windows → anchor
personas to real voices → transfer a real skeleton → strip AI tells → pick an evergreen
topic → owner review**. The workflow commands are in [seeding.md](seeding.md).

Acceptance (#1793): a composed sample passes the owner's "can't tell it's AI" blind review.

## 1. Check what data you have

```bash
tg-agent seeding samples --chat <chat-id> --days 14
```

The header line reports how many messages the window holds and how many conversation
windows qualified. Thin data makes thin voices:

- Fewer than ~20 qualifying windows, or a chat younger in the DB than the topics you need
  → ask the owner to backfill history: `tg-agent channel collect <chat-id> --full` (live
  Telegram, runs on the owner's machine; then re-run `samples` with a wider `--days`).
- Coverage sanity: message ids are sequential per supergroup, so `last_id - first_id + 1`
  vs the message count shows what share of the period you hold. ~85–90% is normal
  (service and deleted messages); much lower means gaps.
- Know your period. A sample of 1,388 messages can be just 18 days (19.10–06.11.2026 for
  the reference chat) — about 6% of the chat's history.

The chat id is accepted in both forms: bare (`1997034059`) or Telegram-marked
(`-1001997034059`).

## 2. Read the real speech

```bash
tg-agent seeding samples --chat <chat-id> --days 14                       # readable
tg-agent seeding samples --chat <chat-id> --days 30 --windows 8 --format json
```

- **Windows** — the most recent conversations, verbatim: consecutive messages with pauses
  ≤ `--gap-minutes` (10), at least `--min-messages` (6) and `--min-speakers` (3). Each line
  carries the pause from the previous message. Media without a caption shows as `[photo]`,
  `[video]`…: people react to screenshots, that is part of the rhythm. Channel posts are
  shown as triggers but do not count as speakers.
- **Voices** — the top `--senders` (4) real participants by text count, each with their
  latest `--per-sender` (10) messages: a speech portrait per persona anchor. Channel posts
  and the anonymous admin are not voices.

Read at least four windows before writing anything. `seeding stats` stays the source of
timing only: delay distribution, quiet hours, validation bounds.

## 3. Measure the chat's speech markers

Compare your draft against the chat, not against clean prose. Reference chat
(`examples/vAIbecoding.jsonl`, 1,388 messages, 19.10–06.11.2026; channel posts excluded):

| Marker | Real chat |
|---|---|
| Messages ≤ 20 chars / ≤ 40 chars | 19% / 42% (median length 50) |
| Starts lowercase (`stats.style.lowercase_start_share`) | 46% |
| Ends with a period | 10% |
| Ends with a `)` smiley / has an emoji | 8% / 4% |
| Next message by the same sender (bursts of 2–3) | 16% |
| Pause inside a conversation, p25 / median / p75 | 27s / 52s / 125s |
| Speaker switch faster than 20s | 19% |

The last row is why the ≥20s persona-switch rail costs some realism: keep it anyway, it is
a safety rail.

## 4. Personas are real voices

- One persona ↔ one real top sender from `voices`. Copy the register: capitalization,
  punctuation, typical length, bursts, fillers, how they ask and how they disagree.
- Chat-wide memes (used by several senders, e.g. «инновации», «кожаный») — yes. One
  person's signature phrase verbatim — no: members recognize it.
- Keep the persona file consistent across sessions (see [seeding.md](seeding.md) §2).

## 5. Skeleton transfer — how to compose

1. Pick a real window as the skeleton (an evergreen one, §7).
2. Keep its shape: speaker order including bursts (the same persona 2–3 times in a row),
   per-slot length (±30%), the pause shape (stretch persona switches to ≥20s).
3. Replace the content with the new topic. Fragments, one thought per message, questions
   left unanswered — all allowed.
4. Map real speakers to personas by voice similarity.

Never post a rewrite of a real window into the same chat — members recognize their own
conversation. Rewrites are a quality test only (§9).

## 6. AI tells to strip

- Fixed grammar, added commas and final periods, capitalized starts where the chat writes
  lowercase.
- Uniform medium-length messages; every message a full sentence.
- Politeness and helpfulness; explanations nobody asked for.
- Closure: «спасибо, понял!», a summary, consensus. Real threads just fade.
- Every message answering the previous one. Real people talk past each other.
- Decorative emoji beyond the chat's emoji share.
- Typos copied verbatim from a real message — move them elsewhere instead.
- An opener out of nowhere («привет всем, что думаете о…»). Real conversations are
  triggered: a channel post, a screenshot, one's own problem.

## 7. Topics

**Evergreen rules** — a seeded topic must not age within days:

- No time anchors: «сегодня», «завтра», dates, deadlines («сгорают 26-го»).
- No news: releases, model version numbers, promos, limit resets, IPOs, ban waves.
  Product names are fine (клод, кодекс, гпт), version numbers are not.
- A recurring practice question: problem → advice, disagreement or a story.
- The opener is self-contained: no reference to a screenshot or post nobody can see.

**The topic bank comes from the chat.** Label the windows with topic + date and keep
topics that recur and pass the evergreen rules. The chat's own signals override
everything: in the reference chat the admin asked for personal AI-usage experience, no
links, no account/ban/payment-verification topics («тему с аккаунтами заворачиваем»),
no flood or off-topic.

**External topics (forums, web)** are a separate list — never mixed with chat topics —
and each carries its source date. Use one only if it is fresh (≤ ~2 months) **and** has
surfaced in the chat recently. Tool-capability complaints perish fastest: «the agent
forgets after context compaction» was a forum staple in H1 2026, while the reference chat
said in October 2026 that it is solved. Web search is thin for Russian Telegram chats; a
better source of fresh topics is collecting similar chats with tg-agent and running
`seeding samples` there.

## 8. Validator sanity

- A *style* rule must pass the chat's own windows. Run it over
  `seeding samples --format json` windows; if it rejects a meaningful share of real
  speech, the rule is wrong. Example: «no persona above max(top-sender share, 50%)» would
  reject 42% of the reference chat's real windows (5+ messages, 2–4 speakers) — in real
  chats one person often carries the thread. Token-overlap echo checks between personas do
  not measure realism either: unnatural text passes them cleanly.
- Safety rails stay regardless: no links, ≥20s between accounts, quiet hours, per-account
  daily caps, no overdue `planned_at` ([seeding.md](seeding.md) §5).

## 9. Review loop

1. **Side by side** — real window | rewrite on the same skeleton, so the owner can judge
   the voice.
2. **Blind test** — shuffle real windows and composed dialogues without labels; the owner
   guesses which is AI. This is the acceptance bar of #1793.
3. Only then `seeding validate` → `thread-create` → `add`.

## Examples (reference chat, participants anonymized as `user_NNN`, `ADMIN` = anonymous admin)

Rewrite test, «where to find clients» (25.10.2026) — same speakers, order and pauses:

| pause | who | Real | Rewrite |
|---|---|---|---|
| | user_011 | Что-то я уже замотался искать себе заказы.... Ничего не могу найти | Что-то совсем глухо с заказами.... Неделю ищу, ничего |
| +13s | ADMIN | потому что их нет | их и нет |
| +326s | user_011 | Ну как бы, как говорится, почти иж каждого утюга твердят, что на ботах можно по 200+ заработать ... Где только вопрос | Ну как бы, каждый второй блогер рассказывает, что на ботах по 200+ поднимают ... Только где оин все |
| +121s | ADMIN | мне вот не твердят ) | мне никто не рассказывает ) |
| +46s | user_014 | Можно конечно. / Если найти покупателя на 200 + тыщ | Поднимают, да. / Если найдёшь кто заплатит 200 + тыщ |

Rewrite test, «full access for the agent» (26.10.2026) — the opener rewritten to be
self-contained (the original reacted to an unseen screenshot):

| pause | who | Real | Rewrite |
|---|---|---|---|
| | ADMIN | [photo] ну что тут еще сказать. | ну и зачем ей фулл аксес на такую задачу. |
| +110s | user_058 | а что мне ей запрещать, если я вижу все действия, а задача не имеет двух трактовок | а от чего ограничивать, если я всё вижу что она делает, а задача однозначная |
| +69s | user_027 | Тут вопрос в том, что когда она чёто натворит , откатиться не выйдет | Вопрос не в этом, а в том что если она чёто снесёт , назад уже не вернёшь |
| +80s | user_027 | У меня тоже фулл аксес, но однажды, не на моём компе, но работал я, он просто взял и удалил всё… | Я тоже на фулл аксесе сижу, но как то раз, комп не мой был, но работал за ним я, он взял и снёс всё подчистую… |
| +62s | user_027 | раз в год и палка стреляет | бывает раз в год, но бывает |

What the rewrites keep: no grammar fixes, short stays short, typos move (`иж` → `оин`),
quirks stay (the space before a comma, the `)` ending, lowercase starts). Pauses under 20s
between different people (+13s) must be stretched before sending.

## Known gaps

- No reply structure: the collector does not store `reply_to_msg_id`, so windows are
  time adjacency only.
- A scenario cannot reply to an existing chat message (e.g. a channel post):
  `reply_to_position` points inside the scenario only.
- Text only: seeds cannot carry screenshots, so openers must not depend on one.
- Link-only posts may show the link-preview text in `text`, which the sender never typed.
