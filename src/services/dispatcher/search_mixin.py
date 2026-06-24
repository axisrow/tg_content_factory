"""Live Telegram search command handler (#1047).

Domain: ``search.telegram`` — proxies premium / my_chats / in-channel search to
the worker's real ClientPool (#643), since the web container has no live pool.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from src.services.dispatcher._base import _DispatcherProtocol

    _Base = _DispatcherProtocol
else:
    _Base = object


class SearchCommandsMixin(_Base):
    """``search.telegram`` command handler."""

    async def _handle_search_telegram(self, payload: dict[str, Any]) -> dict[str, Any]:
        """Run a live Telegram-backed search on the worker's real pool.

        The web container has no live ClientPool (runtime_mode="web"), so it
        proxies premium/my_chats/in-channel search here and reads the serialized
        SearchResult back from ``result_payload`` (#643).
        """
        if self._search_engine is None:
            raise RuntimeError("Search engine unavailable in worker")
        query = str(payload.get("query", ""))
        mode = str(payload.get("mode", "telegram"))
        limit = int(payload.get("limit", 50))
        if mode == "my_chats":
            result = await self._search_engine.search_my_chats(query, limit=limit)
        elif mode == "channel":
            channel_id = payload.get("channel_id")
            result = await self._search_engine.search_in_channel(
                int(channel_id) if channel_id is not None else None, query, limit=limit
            )
        else:
            result = await self._search_engine.search_telegram(query, limit=limit)
        return {"result": result.model_dump(mode="json")}
