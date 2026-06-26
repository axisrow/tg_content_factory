from __future__ import annotations

import re

CYRILLIC_RE = re.compile(r"[а-яА-ЯёЁ]")

# Случайно сгенерированные username: только заглавные латинские + цифры,
# длина ≥ 10 и обязательно хотя бы одна цифра. Ловит шаблоны вроде
# "S0IMD1EDUAW", "EXF74CHE3RZ1"; безопасно пропускает "BITCOIN24", "NASDAQNEWS".
SUSPICIOUS_USERNAME_RE = re.compile(r"^(?=.*\d)[A-Z0-9]{10,}$")

LOW_UNIQUENESS_THRESHOLD = 30.0
LOW_SUBSCRIBER_RATIO_THRESHOLD = 1.0  # broadcast-каналы
LOW_SUBSCRIBER_RATIO_CHAT_THRESHOLD = 0.02  # supergroup / group / gigagroup
CROSS_DUPE_THRESHOLD = 50.0
NON_CYRILLIC_THRESHOLD = 10.0
CHAT_NOISE_THRESHOLD = 70.0

PRECHECK_CROSS_DUPE_SAMPLE = 10  # сколько постов сэмплировать
PRECHECK_CROSS_DUPE_RATIO = 0.8  # порог совпадений (80%)
PRECHECK_CROSS_DUPE_MIN_SAMPLE = 5  # минимум текстовых сообщений для вывода

# Quick-режим (#1138): сколько последних сообщений на канал семплировать для
# text-метрик вместо полного скана. N=300 откалиброван эмпирически на боевой БД
# (бинарный вердикт совпадает с полной историей на 99.65% каналов); полный скан
# 25.6М строк = ~6 мин, семпл N=300 = ~5-10с.
DEFAULT_QUICK_SAMPLE_SIZE = 300

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
        "title_changed",
        "suspicious_username",
    }
)


def contains_cyrillic(text: str) -> bool:
    return bool(CYRILLIC_RE.search(text))
