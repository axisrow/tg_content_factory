from __future__ import annotations

import re

CYRILLIC_RE = re.compile(r"[а-яА-ЯёЁ]")

LOW_UNIQUENESS_THRESHOLD = 30.0
LOW_SUBSCRIBER_RATIO_THRESHOLD = 1.0  # broadcast-каналы
LOW_SUBSCRIBER_RATIO_CHAT_THRESHOLD = 0.02  # supergroup / group / gigagroup
CROSS_DUPE_THRESHOLD = 50.0
NON_CYRILLIC_THRESHOLD = 10.0
CHAT_NOISE_THRESHOLD = 70.0

PRECHECK_CROSS_DUPE_SAMPLE = 10  # сколько постов сэмплировать
PRECHECK_CROSS_DUPE_RATIO = 0.8  # порог совпадений (80%)
PRECHECK_CROSS_DUPE_MIN_SAMPLE = 5  # минимум текстовых сообщений для вывода

VALID_FLAGS = frozenset(
    {
        "low_uniqueness",
        "low_subscriber_ratio",
        "low_subscriber_manual",
        "manual",
        "cross_channel_spam",
        "non_cyrillic",
        "chat_noise",
        "username_changed",
    }
)


def contains_cyrillic(text: str) -> bool:
    return bool(CYRILLIC_RE.search(text))
