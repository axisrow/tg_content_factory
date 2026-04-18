from __future__ import annotations

from typing import Any

RESULT_KIND_GENERATED_ITEMS = "generated_items"
RESULT_KIND_PROCESSED_MESSAGES = "processed_messages"
_ACTION_COUNTS_KEY = "action_counts"


def increment_action_count(context: Any, action: str, amount: int = 1) -> None:
    if amount <= 0:
        return
    current = context.get_global(_ACTION_COUNTS_KEY, {}) or {}
    counts = dict(current)
    counts[action] = int(counts.get(action, 0)) + amount
    context.set_global(_ACTION_COUNTS_KEY, counts)


def get_action_counts(context: Any) -> dict[str, int]:
    raw = context.get_global(_ACTION_COUNTS_KEY, {}) or {}
    return {
        str(key): int(value)
        for key, value in raw.items()
        if isinstance(key, str) and isinstance(value, int | float)
    }


def summarize_result(
    *,
    generated_text: str | None,
    citations: list[Any] | None,
    action_counts: dict[str, int] | None,
) -> tuple[str, int]:
    citation_count = len(citations or [])
    text_present = bool((generated_text or "").strip())
    if citation_count > 0 or text_present:
        return RESULT_KIND_GENERATED_ITEMS, citation_count or 1
    processed_count = sum(max(0, int(value)) for value in (action_counts or {}).values())
    return RESULT_KIND_PROCESSED_MESSAGES, processed_count


def result_kind_label(result_kind: str | None) -> str:
    if result_kind == RESULT_KIND_PROCESSED_MESSAGES:
        return "Обработано"
    return "Сгенерировано"
