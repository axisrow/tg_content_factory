"""Общие SQLite UDF, регистрируемые на коннектах.

Нейтральный дом для UDF, нужных нескольким репозиториям (вставка сообщений и
фильтратор регистрируют одну и ту же функцию): репозиторий-репозиторий не
импортирует приватные символы соседа, оба берут отсюда.
"""

from __future__ import annotations

import re

_CYRILLIC_RE = re.compile(r"[а-яА-ЯёЁ]")


def has_cyrillic_udf(text: str | None) -> int:
    """1, если в тексте есть кириллица, иначе 0 (None/пустая строка → 0)."""
    if not text:
        return 0
    return 1 if _CYRILLIC_RE.search(text) else 0
