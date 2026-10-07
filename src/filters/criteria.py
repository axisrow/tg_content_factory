from __future__ import annotations

import re

CYRILLIC_RE = re.compile(r"[а-яА-ЯёЁ]")

# Случайно сгенерированные username. Две формы спам-генератора:
#  - сплошной КАПС+цифры, длина ≥10: "S0IMD1EDUAW", "EXF74CHE3RZ1";
#  - CamelCase-слова + хвост ≥3 цифр, ≥2 заглавных и ≥2 строчных
#    ("WordWord123456"): ферма 02.10.26 — 18/18 поймано, 0/1429 ложных
#    на живой БД.
# Безопасные случаи остаются за бортом: "BITCOIN24" (9 символов),
# "NASDAQNEWS" (нет цифр), "PublicChannel42" (1 заглавная, 2 цифры),
# "JohnDoe1990" (7 букв, годовой хвост). Lowercase-only фермы
# ("haberkanal724"-стиль) сознательно не ловим — слишком много легитимных.
SUSPICIOUS_USERNAME_RE = re.compile(
    r"^(?=.*\d)[A-Z0-9]{10,}$"  # сплошной КАПС + цифры
    r"|^(?=.*[A-Z].*[A-Z])(?=.*[a-z].*[a-z])[A-Za-z]{8,}\d{3,}$"  # CamelCase+хвост
)

LOW_UNIQUENESS_THRESHOLD = 30.0
LOW_SUBSCRIBER_RATIO_THRESHOLD = 1.0  # broadcast-каналы
LOW_SUBSCRIBER_RATIO_CHAT_THRESHOLD = 0.02  # supergroup / group / gigagroup
CROSS_DUPE_THRESHOLD = 50.0
NON_CYRILLIC_THRESHOLD = 10.0
CHAT_NOISE_THRESHOLD = 70.0

# «Автогенерённая лента» (auto_feed): канал публикует короткие посты по одному
# шаблону со ссылкой наружу (слаг-фиды, автопостинг). Откалибровано на живой БД
# 04.10.26: шаблонный слаг-фид = 88.8% коротких постов со ссылкой, writeup-канал =
# 61.4%, авторские каналы ≤12% — порог 85% разделяет с запасом. «Короткий» —
# <200 символов, литерал в _SQL_AUTO_FEED (repositories/filters.py), SQL-слой не
# импортирует пакет filters. Минимум сообщений — ниже нет статистики.
#
# Ограничение (ревью #1491): ссылка детектится только в видимом тексте поста
# (LIKE '%http%' / '%t.me%'). Замаскированные гиперссылки (MessageEntityTextUrl)
# при парсинге отбрасываются — в messages.text остаётся лишь надпись («Read
# more»), такие ленты не флагаются. Системный фикс — persist link metadata при
# сборе, отдельная задача.
AUTO_FEED_MIN_MESSAGES = 30
AUTO_FEED_THRESHOLD = 85.0

# Флаги-метки (решение владельца 06.10.26): флаг ОСТАЁТСЯ в filter_flags как сигнал
# для LLM-кураторства, но не ставит is_filtered — нерусскоязычные ИИ-каналы
# (TestingCatalog, AI_News_CN и т.п.) собираются, язык виден куратору.
LABEL_ONLY_FLAGS = frozenset({"non_cyrillic"})

# Тег-эскроу: канал с тегом KEEP_TAG исключается из авто-фильтрации целиком —
# apply_filters сбрасывает всем is_filtered, но keep-каналу возвращает is_filtered=0
# и объединённые (старые ∪ свежие) флаги как метки. Нужен, когда блокирует не языковой
# флаг (напр. low_subscriber_ratio у TestingCatalog), а канал решено собирать.
KEEP_TAG = "keep"

# Whitelist-метка доверия (#1490, решение владельца 04.10.26): тег живёт в штатной
# тег-системе каналов (tags/channel_tags, CLI `channel tag`), а не в VALID_FLAGS.
# Канал с этим тегом И фильтр-флагами — подозрение на ошибку фильтра; отчёт:
# `filter suspects` (read-only, поведение сбора не меняет).
AUTHOR_CONTENT_TAG = "авторский контент"

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
        "auto_feed",
    }
)


def contains_cyrillic(text: str) -> bool:
    return bool(CYRILLIC_RE.search(text))
