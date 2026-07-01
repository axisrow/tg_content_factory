"""Channel ingest and refresh command handlers (#1047).

Domains: ``channels.*`` (add / collect_stats / refresh_types / refresh_meta /
import_batch) and ``agent.forum_topics_refresh``.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

from src.services.channel_onboarding import (
    channel_from_resolved_info,
    enqueue_stats_for_new_channels,
    fetch_channel_meta,
    get_existing_channel,
)

if TYPE_CHECKING:
    from src.services.dispatcher._base import _DispatcherProtocol

    _Base = _DispatcherProtocol
else:
    _Base = object


class ChannelsCommandsMixin(_Base):
    """``channels.*`` and ``agent.forum_topics_refresh`` command handlers."""

    async def _handle_agent_forum_topics_refresh(self, payload: dict[str, Any]) -> dict[str, Any]:
        channel_id = int(payload["channel_id"])
        topics = await self._pool.get_forum_topics(channel_id)
        if topics:
            await self._db.upsert_forum_topics(channel_id, topics)
            await self._db.set_channel_type(channel_id, "forum")
        return {"channel_id": channel_id, "count": len(topics)}

    async def _handle_channels_add_identifier(self, payload: dict[str, Any]) -> dict[str, Any]:
        identifier = str(payload["identifier"]).strip()
        info = await self._pool.resolve_channel(identifier)
        if not info:
            raise RuntimeError(f"resolve failed: {identifier!r} not found")
        existing = await get_existing_channel(self._db, int(info["channel_id"]))
        meta = await fetch_channel_meta(
            self._pool, int(info["channel_id"]), info.get("channel_type")
        )
        channel = channel_from_resolved_info(info, meta)
        await self._db.add_channel(channel)
        stats_task_id = None
        if existing is None and channel.is_active:
            stats_task_id = await enqueue_stats_for_new_channels(
                self._db.create_stats_task,
                [channel.channel_id],
                context="channels.add_identifier",
            )
        return {"channel_id": info["channel_id"], "stats_task_id": stats_task_id}

    async def _handle_channels_collect_stats(self, payload: dict[str, Any]) -> dict[str, Any]:
        if self._collector is None:
            raise RuntimeError("collector_unavailable")
        channel_pk = int(payload["channel_pk"])
        channel = await self._db.get_channel_by_pk(channel_pk)
        if channel is None:
            raise RuntimeError("channel_not_found")
        result = await self._collector.collect_channel_stats(channel)
        return {"channel_id": channel.channel_id, "collected": bool(result)}

    async def _handle_channels_refresh_types(self, payload: dict[str, Any]) -> dict[str, Any]:
        channels = await self._db.get_channels(active_only=True)
        updated = 0
        failed = 0
        deactivated = 0
        quarantined = 0
        for ch in channels:
            channel_pk = ch.id
            assert channel_pk is not None
            identifier = ch.username or str(ch.channel_id)
            try:
                # numeric_fallback so a stale @username doesn't deactivate a live
                # channel — gone is retried by numeric id first (#858 review).
                info = await self._pool.resolve_channel(
                    identifier, signal_gone=True, numeric_fallback=str(ch.channel_id)
                )
            except Exception:
                info = None
            # Uncertain (cache-miss vs deleted, owner unknown/unavailable) → quarantine
            # for human review. This runs in the background worker with no interactive
            # user, so flagging for review is the only safe move (#875 redesign).
            if info and info.get("review"):
                await self._db.repos.channels.set_channel_review(channel_pk, info.get("reason", "uncertain"))
                quarantined += 1
                continue
            # Definitive not-found → deactivate; transient None → skip and leave
            # active (audit #835/8; old `if info is False` was unreachable).
            if info and info.get("gone"):
                await self._db.set_channel_active(channel_pk, False)
                await self._db.set_channel_type(ch.channel_id, "unavailable")
                deactivated += 1
                continue
            if not info or info.get("channel_type") is None:
                failed += 1
                continue
            # Resolved live: clear any stale quarantine flag (channel recovered).
            if getattr(ch, "needs_review", False):
                await self._db.repos.channels.clear_channel_review(channel_pk)
            await self._db.set_channel_type(ch.channel_id, info["channel_type"])
            updated += 1
        return {
            "updated": updated,
            "failed": failed,
            "deactivated": deactivated,
            "quarantined": quarantined,
        }

    async def _handle_channels_refresh_meta(self, payload: dict[str, Any]) -> dict[str, Any]:
        channels = await self._db.get_channels(active_only=True)
        updated = 0
        failed = 0
        for ch in channels:
            try:
                meta = await self._pool.fetch_channel_meta(ch.channel_id, ch.channel_type)
            except Exception:
                meta = None
            if not meta:
                failed += 1
                continue
            await self._db.update_channel_full_meta(
                ch.channel_id,
                about=meta["about"],
                linked_chat_id=meta["linked_chat_id"],
                has_comments=meta["has_comments"],
            )
            updated += 1
        return {"updated": updated, "failed": failed}

    async def _handle_channels_import_batch(self, payload: dict[str, Any]) -> dict[str, Any]:
        identifiers = [str(item).strip() for item in payload.get("identifiers", []) if str(item).strip()]
        existing = await self._db.get_channels()
        existing_ids = {channel.channel_id for channel in existing}
        added = 0
        skipped = 0
        failed = 0
        stats_channel_ids: list[int] = []
        details: list[dict[str, Any]] = []
        for ident in identifiers:
            try:
                info = await self._pool.resolve_channel(ident)
            except Exception:
                info = None
            if not info:
                failed += 1
                details.append({"identifier": ident, "status": "failed"})
                continue
            if info["channel_id"] in existing_ids:
                skipped += 1
                details.append({"identifier": ident, "status": "skipped"})
                continue
            meta = await fetch_channel_meta(
                self._pool, int(info["channel_id"]), info.get("channel_type")
            )
            channel = channel_from_resolved_info(info, meta)
            await self._db.add_channel(channel)
            existing_ids.add(info["channel_id"])
            if channel.is_active:
                stats_channel_ids.append(channel.channel_id)
            added += 1
            details.append({"identifier": ident, "status": "added"})
        stats_task_id = await enqueue_stats_for_new_channels(
            self._db.create_stats_task,
            stats_channel_ids,
            context="channels.import_batch",
        )
        return {
            "added": added,
            "skipped": skipped,
            "failed": failed,
            "details": details,
            "stats_task_id": stats_task_id,
        }
