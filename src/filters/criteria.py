from __future__ import annotations

import re

CYRILLIC_RE = re.compile(r"[а-яА-ЯёЁ]")

LOW_UNIQUENESS_THRESHOLD = 30.0
LOW_SUBSCRIBER_RATIO_THRESHOLD = 1.0
CROSS_DUPE_THRESHOLD = 50.0
NON_CYRILLIC_THRESHOLD = 10.0
CHAT_NOISE_THRESHOLD = 70.0

VALID_FLAGS = frozenset({
    "low_uniqueness",
    "low_subscriber_ratio",
    "cross_channel_spam",
    "non_cyrillic",
    "chat_noise",
})


def contains_cyrillic(text: str) -> bool:
    return bool(CYRILLIC_RE.search(text))
