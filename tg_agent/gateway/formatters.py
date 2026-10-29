"""Presentation of normalized live messages without native client objects."""

from tg_agent.telegram.reactions import format_reactions_json


def live_message_lines(data, *, reverse=False, budget=50_000):
    messages = reversed(data["messages"]) if reverse else data["messages"]
    lines = []
    size = 0
    for row in messages:
        sender = " ".join(filter(None, [row.get("sender_first_name"), row.get("sender_last_name")]))
        sender = sender or row.get("sender_name") or row.get("sender_username") or ""
        reactions = format_reactions_json(row.get("reactions_json"))
        suffix = f" reactions: {reactions}" if reactions else ""
        lines.append(f"[{str(row.get('date') or '—')[:19]}] #{row['message_id']} {sender}{suffix}")
        text = row.get("text") or (f"[media: {row['media_type']}]" if row.get("media_type") else "")
        if text:
            lines.append("  " + text[:500])
        users = data["reaction_users"].get(str(row["message_id"]))
        if users:
            lines.append("  reaction users: " + users)
        lines.append("")
        size += sum(len(line) for line in lines[-4:])
        if size >= budget:
            lines.append("[message text budget reached]")
            break
    return lines
