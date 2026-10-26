"""Pre-add channel quality probe: pure heuristics over a LIVE message sample.

`channel probe` reads the last N posts straight from Telegram (no DB write)
and feeds the snapshots here. Thresholds are imported from criteria.py — the
same constants the post-collect ``filter analyze`` uses, so a probe flag and a
post-collect flag mean the same thing.

Metrics that need collected history (``cross_channel_spam`` — cross-comparison
with all collected messages; ``low_subscriber_ratio`` — message-count totals)
are out of scope: they are only checkable after the channel is added and
collected. The card carries a ``not_checked`` list so the consumer sees the
gap.
"""
from __future__ import annotations

from dataclasses import dataclass

from tg_agent.filters.criteria import (
    AUTO_FEED_MIN_MESSAGES,
    AUTO_FEED_SHORT_TEXT,
    AUTO_FEED_THRESHOLD,
    CHAT_NOISE_THRESHOLD,
    FORWARD_SHARE_MIN_MESSAGES,
    FORWARD_SHARE_THRESHOLD,
    FORWARD_SHARE_WINDOW,
    LOW_ENGAGEMENT_MIN_VIEWED_MESSAGES,
    LOW_ENGAGEMENT_THRESHOLD,
    LOW_ENGAGEMENT_WINDOW,
    LOW_UNIQUENESS_THRESHOLD,
    NON_CYRILLIC_THRESHOLD,
    SUSPICIOUS_USERNAME_RE,
    contains_cyrillic,
)

#: Below this sample size there are no statistics — the same 30-post guard the
#: analyzer's auto_feed/forwarder windows use.
MIN_SAMPLE = AUTO_FEED_MIN_MESSAGES

#: The analyzer's uniqueness SQL counts DISTINCT substr(text, 1, 100).
UNIQUENESS_PREFIX = 100
#: «Короткий пост» для chat_noise — тот же литерал, что в SQL-метрике.
CHAT_NOISE_SHORT_TEXT = 10


@dataclass
class ProbeMessage:
    """Snapshot of one live post — only the fields the heuristics need."""

    text: str | None = None
    views: int | None = None
    is_forward: bool = False
    urls: tuple[str, ...] = ()


def probe_channel_snapshots(
    messages: list[ProbeMessage],
    *,
    participants_count: int | None,
    username: str | None,
    min_subscribers: int | None = None,
    is_broadcast: bool | None = None,
) -> dict:
    """Build the flag card from a latest-first sample of live posts.

    ``min_subscribers`` mirrors the ``min_subscribers_filter`` setting (0/None
    disables the check, as in the analyzer).
    ``is_broadcast`` mirrors the analyzer's chat-only guard: chat_noise is
    computed only for chat peers (groups/megagroups/forums); for a broadcast
    channel — or an unknown peer type, judged as broadcast like the analyzer
    does — the flag stays None so a probe flag and a post-collect flag mean
    the same thing.
    """
    total = len(messages)
    card: dict = {
        "total": total,
        "min_sample": MIN_SAMPLE,
        "insufficient": total < MIN_SAMPLE,
        "flags": {},
        "not_checked": ["cross_channel_spam", "low_subscriber_ratio"],
    }

    def flag(name: str, hit: bool | None, detail: str) -> None:
        # None = «недостаточно данных, флаг не считался» — не коэртить в False.
        card["flags"][name] = {"hit": hit, "detail": detail}

    texts = [(m.text or "").strip() for m in messages]
    non_empty = [t for t in texts if t]

    # low_uniqueness: доля уникальных 100-символьных префиксов < 30%.
    if non_empty:
        uniq = len({t[:UNIQUENESS_PREFIX] for t in non_empty})
        pct = uniq / len(non_empty) * 100
        flag(
            "low_uniqueness",
            pct < LOW_UNIQUENESS_THRESHOLD,
            f"{pct:.1f}% unique prefixes ({LOW_UNIQUENESS_THRESHOLD}% threshold)",
        )
    else:
        flag("low_uniqueness", None, "no text posts in sample")

    # non_cyrillic (label-only): доля постов с кириллицей < 10%.
    if non_empty:
        cyr = sum(1 for t in non_empty if contains_cyrillic(t))
        pct = cyr / len(non_empty) * 100
        flag(
            "non_cyrillic",
            pct < NON_CYRILLIC_THRESHOLD,
            f"{pct:.1f}% posts with Cyrillic ({NON_CYRILLIC_THRESHOLD}% threshold, label-only)",
        )
    else:
        flag("non_cyrillic", None, "no text posts in sample")

    # chat_noise: доля постов ≤10 символов > 70% — ЧАТОВЫЙ флаг, как в
    # анализаторе (_append_chat_noise_flag считает его только для чатов);
    # у broadcast-канала флаг не считается (None), а не «informational».
    if is_broadcast is False and non_empty:
        short = sum(1 for t in non_empty if len(t) <= CHAT_NOISE_SHORT_TEXT)
        pct = short / len(non_empty) * 100
        flag(
            "chat_noise",
            pct > CHAT_NOISE_THRESHOLD,
            f"{pct:.1f}% posts ≤{CHAT_NOISE_SHORT_TEXT} chars ({CHAT_NOISE_THRESHOLD}% threshold)",
        )
    elif is_broadcast is False:
        flag("chat_noise", None, "no text posts in sample")
    else:
        flag("chat_noise", None, "chat-only flag (broadcast or unknown peer type)")

    # auto_feed: ≥85% постов <200 символов со ссылкой (total ≥ 30).
    if total >= AUTO_FEED_MIN_MESSAGES:
        feedish = sum(
            1
            for m in messages
            if len((m.text or "")) < AUTO_FEED_SHORT_TEXT
            and (bool(m.urls) or "http" in (m.text or "").lower())
        )
        pct = feedish / total * 100
        flag(
            "auto_feed",
            pct >= AUTO_FEED_THRESHOLD,
            f"{pct:.1f}% short posts with a link ({AUTO_FEED_THRESHOLD}% threshold)",
        )
    else:
        flag("auto_feed", None, f"sample below {AUTO_FEED_MIN_MESSAGES} posts")

    # forwarder (label-only): доля форвардов в окне последних 100 постов ≥ 50%.
    fwd_window = messages[:FORWARD_SHARE_WINDOW]
    if len(fwd_window) >= FORWARD_SHARE_MIN_MESSAGES:
        fw = sum(1 for m in fwd_window if m.is_forward)
        pct = fw / len(fwd_window) * 100
        flag(
            "forwarder",
            pct >= FORWARD_SHARE_THRESHOLD,
            f"{pct:.1f}% forwards in last {len(fwd_window)} posts "
            f"({FORWARD_SHARE_THRESHOLD}% threshold, label-only)",
        )
    else:
        flag("forwarder", None, f"sample below {FORWARD_SHARE_MIN_MESSAGES} posts")

    # low_engagement: средние просмотры окна-100 ниже 10% подписчиков.
    eng_window = messages[:LOW_ENGAGEMENT_WINDOW]
    viewed = [m.views for m in eng_window if m.views is not None]
    if participants_count is None:
        flag("low_engagement", None, "subscriber count unavailable")
    elif len(viewed) < LOW_ENGAGEMENT_MIN_VIEWED_MESSAGES or sum(viewed) <= 0:
        flag(
            "low_engagement",
            None,
            f"only {len(viewed)} viewed posts in window "
            f"(need {LOW_ENGAGEMENT_MIN_VIEWED_MESSAGES} with views)",
        )
    else:
        ratio = (sum(viewed) / len(viewed)) / participants_count * 100
        flag(
            "low_engagement",
            ratio < LOW_ENGAGEMENT_THRESHOLD,
            f"avg views = {ratio:.1f}% of subscribers ({LOW_ENGAGEMENT_THRESHOLD}% threshold)",
        )

    # low_subscribers: вручную заданный минимум подписчиков (0/None — выкл).
    if min_subscribers:
        if participants_count is None:
            flag("low_subscribers", None, "subscriber count unavailable")
        else:
            flag(
                "low_subscribers",
                participants_count < min_subscribers,
                f"{participants_count} subscribers (min {min_subscribers})",
            )
    else:
        flag("low_subscribers", None, "min_subscribers_filter is not set")

    # suspicious_username: формы спам-генератора (criteria.py).
    if username:
        hit = bool(SUSPICIOUS_USERNAME_RE.match(username))
        flag(
            "suspicious_username",
            hit,
            f"@{username} {'matches' if hit else 'clean'}",
        )
    else:
        flag("suspicious_username", None, "no username")

    return card
