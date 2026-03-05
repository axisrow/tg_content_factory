from __future__ import annotations


def resolve_channel(channels: list, identifier: str):
    try:
        num = int(identifier)
        ch = next((c for c in channels if c.id == num), None)
        if ch:
            return ch
        return next((c for c in channels if c.channel_id == num), None)
    except ValueError:
        pass

    uname = identifier.lstrip("@").lower()
    return next((c for c in channels if (c.username or "").lower() == uname), None)
