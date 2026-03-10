from __future__ import annotations

import json
from itertools import groupby

from src.models import Message


def format_context(
    messages: list[Message],
    channel_title: str,
    topic_id: int | None,
    topics_map: dict[int, str],
) -> str:
    """Format messages as JSONL with optional topic grouping."""
    header = f"[КОНТЕКСТ: {channel_title}"
    if topic_id is not None:
        topic_label = topics_map.get(topic_id)
        if topic_label:
            header += f', тема "{topic_label}"'
        else:
            header += f", тема #{topic_id}"
    header += f", {len(messages)} сообщений]"
    lines: list[str] = [header]

    def _msg_line(m: Message) -> str:
        return json.dumps(
            {
                "id": m.message_id,
                "date": m.date.strftime("%Y-%m-%d"),
                "author": m.sender_name
                or (f"id={m.sender_id}" if m.sender_id else "unknown"),
                "text": (m.text or "").replace("\n", " ")[:200],
            },
            ensure_ascii=False,
        )

    has_topics = bool(topics_map) or any(m.topic_id for m in messages)

    if topic_id is not None or not has_topics:
        # Single topic or plain channel — flat JSONL, no grouping
        for m in messages:
            lines.append(_msg_line(m))
    else:
        # Forum with topics — group by topic
        sorted_msgs = sorted(messages, key=lambda m: (m.topic_id or 0))
        for tid, group in groupby(sorted_msgs, key=lambda m: m.topic_id):
            if tid and tid in topics_map:
                lines.append(f"\n## Тема: {topics_map[tid]}")
            elif tid:
                lines.append(f"\n## Тема #{tid}")
            else:
                lines.append("\n## Без темы")
            for m in group:
                lines.append(_msg_line(m))

    return "\n".join(lines)
