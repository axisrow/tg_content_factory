"""Shared Collector sentinels and exceptions (#1137)."""

from __future__ import annotations

from datetime import datetime

from src.models import Channel

# Sentinel returned by Collector._acquire_collection_client to tell the
# collection loop to retry (transient flood wait or dialog-prefetch flood) —
# the moral equivalent of the inline `continue` it replaced.
_ACQUIRE_RETRY = object()


class _StreamOutcome:
    """Mutable out-params for ``Collector._stream_channel_messages``.

    The streaming loop can be aborted mid-way by a FloodWait/idle-timeout raised
    from inside the Telethon iterator. These flags are written **in place** (not
    returned) so the caller's ``finally`` still sees them even when the streamer
    raised before returning — exactly the visibility the old ``nonlocal`` block
    had. ``messages_batch`` is shared the same way (the caller passes its list
    and the streamer mutates it in place via ``append``/``clear``).
    """

    __slots__ = ("retire_client", "stop_due_to_persistence_error")

    def __init__(self) -> None:
        self.retire_client = False
        self.stop_due_to_persistence_error = False


def _format_channel_log_name(channel: Channel) -> str:
    username = (channel.username or "").strip().lstrip("@")
    if username:
        return f"@{username}"

    title = (channel.title or "").strip()
    return title or "no username"


class NoActiveStatsClientsError(RuntimeError):
    """Raised when there are no active connected clients for stats collection."""


class NoActiveCollectionClientsError(RuntimeError):
    """Raised when there are no active connected clients for message collection."""


class AllStatsClientsFloodedError(RuntimeError):
    """Raised when all active connected clients are in flood-wait."""

    def __init__(self, retry_after_sec: int, next_available_at: datetime):
        super().__init__(
            "All active clients are flood-waited until "
            f"{next_available_at.isoformat()} (retry in {retry_after_sec}s)"
        )
        self.retry_after_sec = retry_after_sec
        self.next_available_at = next_available_at


class AllCollectionClientsFloodedError(RuntimeError):
    """Raised when all active connected clients are in flood-wait."""

    def __init__(self, retry_after_sec: int, next_available_at: datetime):
        super().__init__(
            "All active clients are flood-waited until "
            f"{next_available_at.isoformat()} (retry in {retry_after_sec}s)"
        )
        self.retry_after_sec = retry_after_sec
        self.next_available_at = next_available_at
