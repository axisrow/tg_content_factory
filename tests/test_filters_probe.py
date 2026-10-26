"""Unit tests for the pre-add channel probe (pure function, no DB)."""
from __future__ import annotations

from tg_agent.filters.criteria import (
    AUTO_FEED_THRESHOLD,
    FORWARD_SHARE_THRESHOLD,
    LOW_ENGAGEMENT_THRESHOLD,
)
from tg_agent.filters.probe import ProbeMessage, probe_channel_snapshots


def mk(
    text: str | None = "пост про ИИ и нейросети с достаточной длиной",
    views: int | None = None,
    is_forward=False,
    urls=(),
):
    return ProbeMessage(text=text, views=views, is_forward=is_forward, urls=urls)


def probe(msgs, participants=None, username=None, min_subscribers=None, is_broadcast=None):
    return probe_channel_snapshots(
        msgs,
        participants_count=participants,
        username=username,
        min_subscribers=min_subscribers,
        is_broadcast=is_broadcast,
    )


def sample(n=35, text="уникальный осмысленный русский текст поста номер", views: int | None = 1000, **kw):
    return [mk(text=f"{text} {i}", views=views, **kw) for i in range(n)]


def test_insufficient_guard_below_30():
    card = probe(sample(10))
    assert card["insufficient"] is True
    assert card["flags"]["auto_feed"]["hit"] is None
    assert card["flags"]["forwarder"]["hit"] is None


def test_sufficient_sample_has_no_guard():
    card = probe(sample(35), participants=10000)
    assert card["insufficient"] is False
    assert card["flags"]["auto_feed"]["hit"] is False


def test_low_uniqueness_flags_duplicates():
    msgs = [mk(text="одинаковый шаблонный текст фида") for _ in range(35)]
    assert probe(msgs)["flags"]["low_uniqueness"]["hit"] is True
    assert probe(sample(35))["flags"]["low_uniqueness"]["hit"] is False


def test_non_cyrillic_label():
    english = [mk(text="short english post about models") for _ in range(35)]
    assert probe(english)["flags"]["non_cyrillic"]["hit"] is True
    assert probe(sample(35))["flags"]["non_cyrillic"]["hit"] is False


def test_auto_feed_flags_short_linked_posts():
    linked = [mk(text=f"новость {i} https://example.com/{i}") for i in range(30)]
    card = probe(linked)
    assert card["flags"]["auto_feed"]["hit"] is True
    long_posts = [mk(text=f"длинный авторский разбор темы номер {i} " + "е" * 220) for i in range(30)]
    assert probe(long_posts)["flags"]["auto_feed"]["hit"] is False
    # masked link in entities counts too
    masked = [mk(text=f"Read more {i}", urls=("https://x.com/a",)) for i in range(30)]
    assert probe(masked)["flags"]["auto_feed"]["hit"] is True


def test_forwarder_window():
    forwards = [mk(is_forward=(i % 2 == 0)) for i in range(30)]
    assert probe(forwards)["flags"]["forwarder"]["hit"] is True
    assert probe(sample(30))["flags"]["forwarder"]["hit"] is False


def test_low_engagement_formula_and_window():
    # 5% of subscribers → FLAG; 50% → clean; views from the head-100 window only.
    poor = sample(40, views=500)  # 500/10000 = 5%
    assert probe(poor, participants=10_000)["flags"]["low_engagement"]["hit"] is True
    healthy = sample(40, views=5_000)  # 50%
    assert probe(healthy, participants=10_000)["flags"]["low_engagement"]["hit"] is False
    exact = sample(40, views=1_000)  # exactly 10% → not below threshold
    assert probe(exact, participants=10_000)["flags"]["low_engagement"]["hit"] is False


def test_low_engagement_guards():
    few_views = sample(40, views=None)[:35] + [mk(views=100)] * 5
    card = probe(few_views, participants=10_000)
    assert card["flags"]["low_engagement"]["hit"] is None
    card = probe(sample(40), participants=None)
    assert card["flags"]["low_engagement"]["hit"] is None


def test_low_subscribers_respects_setting():
    card = probe(sample(35), participants=500, min_subscribers=1000)
    assert card["flags"]["low_subscribers"]["hit"] is True
    card = probe(sample(35), participants=5000, min_subscribers=1000)
    assert card["flags"]["low_subscribers"]["hit"] is False
    card = probe(sample(35), participants=1, min_subscribers=None)
    assert card["flags"]["low_subscribers"]["hit"] is None


def test_suspicious_username():
    assert probe(sample(35), username="S0IMD1EDUAW")["flags"]["suspicious_username"]["hit"] is True
    assert probe(sample(35), username="my_ai_channel")["flags"]["suspicious_username"]["hit"] is False
    assert probe(sample(35), username=None)["flags"]["suspicious_username"]["hit"] is None


def test_chat_noise_is_chat_only_like_the_analyzer():
    # The analyzer (_append_chat_noise_flag) computes chat_noise for chat
    # peers only; the probe must agree — a broadcast (or unknown-type)
    # channel never gets the flag, not even "informational".
    noisy = [mk(text="ok")] * 35  # 35 short posts → 100% > 70%
    chat = probe(noisy, participants=100, is_broadcast=False)
    assert chat["flags"]["chat_noise"]["hit"] is True
    broadcast = probe(noisy, participants=100, is_broadcast=True)
    assert broadcast["flags"]["chat_noise"]["hit"] is None
    unknown = probe(noisy, participants=100, is_broadcast=None)
    assert unknown["flags"]["chat_noise"]["hit"] is None
    # chat with no text posts → insufficient data, not a verdict
    assert probe([mk(text=None)] * 35, participants=100, is_broadcast=False)["flags"]["chat_noise"]["hit"] is None


def test_card_reports_db_only_metrics():
    card = probe(sample(35))
    assert "cross_channel_spam" in card["not_checked"]
    assert "low_subscriber_ratio" in card["not_checked"]


def test_thresholds_comes_from_criteria():
    from tg_agent.filters import criteria
    from tg_agent.filters import probe as probe_mod

    assert probe_mod.MIN_SAMPLE == criteria.AUTO_FEED_MIN_MESSAGES
    assert criteria.LOW_ENGAGEMENT_THRESHOLD == LOW_ENGAGEMENT_THRESHOLD
    assert criteria.FORWARD_SHARE_THRESHOLD == FORWARD_SHARE_THRESHOLD
    assert criteria.AUTO_FEED_THRESHOLD == AUTO_FEED_THRESHOLD
