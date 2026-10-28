from __future__ import annotations

import asyncio
from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from typing import TypeVar

from tg_agent.database.bundles import SearchBundle
from tg_agent.models import SearchParams, SearchResult
from tg_agent.search.numpy_semantic import NumpySemanticIndex
from tg_agent.services.embedding_service import EmbeddingService

_T = TypeVar("_T")


def _as_aware(value: datetime) -> datetime:
    """Naive datetimes are UTC by project convention — compare chronologically."""
    return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)


def _parse_date_bound(raw: str) -> datetime:
    """Parse an ISO date bound ("YYYY-MM-DD" or full timestamp) into a datetime."""
    return _as_aware(datetime.fromisoformat(raw))


class LocalSearch:
    def __init__(self, search: SearchBundle, embedding_service: EmbeddingService | None = None):
        self._search = search
        self._embedding_service = embedding_service
        self._numpy_index: NumpySemanticIndex | None = None
        self._numpy_index_loaded: bool = False
        self._numpy_index_include_filtered: bool = False
        self._numpy_generation = 0
        self._numpy_build_lock = asyncio.Lock()
        self._numpy_compute_lock = asyncio.Lock()

    def invalidate_numpy_index(self) -> None:
        """Reset the cached numpy index so it is rebuilt on the next search."""
        self._numpy_index = None
        self._numpy_index_loaded = False
        self._numpy_generation += 1

    async def _compute(self, function: Callable[..., _T], *args, **kwargs) -> _T:
        """Bound CPU work, retaining the slot until a cancelled thread exits."""
        async with self._numpy_compute_lock:
            task = asyncio.create_task(asyncio.to_thread(function, *args, **kwargs))
            cancelled = False
            while not task.done():
                try:
                    await asyncio.shield(task)
                except asyncio.CancelledError:
                    cancelled = True
                except Exception:
                    if cancelled:
                        raise asyncio.CancelledError from None
                    raise
            if cancelled:
                # Retrieve a possible exception before propagating cancellation.
                if not task.cancelled():
                    task.exception()
                raise asyncio.CancelledError
            return task.result()

    async def search(self, params: SearchParams) -> SearchResult:
        page = await self._search.search_messages(params)
        return SearchResult(
            messages=page.messages, total=page.total, has_more=page.has_more, query=params.query
        )

    async def _ensure_numpy_index(self, include_filtered: bool = False) -> NumpySemanticIndex:
        """Lazily load and build the numpy in-memory index from the JSON table.

        The filtered-channel scope is part of the index: a scope change rebuilds
        it, otherwise include_filtered=True would silently search the
        filtered-out subset.
        """
        async with self._numpy_build_lock:
            while not self._numpy_index_loaded or self._numpy_index_include_filtered != include_filtered:
                generation = self._numpy_generation
                index = NumpySemanticIndex()
                async for batch in self._search.messages.iter_embedding_json_batches(
                    include_filtered=include_filtered
                ):
                    await self._compute(index.add_json_batch, batch)
                    if generation != self._numpy_generation:
                        break
                if generation != self._numpy_generation:
                    continue
                await self._compute(index.finish_loading)
                if generation != self._numpy_generation:
                    continue
                self._numpy_index = index
                self._numpy_index_loaded = True
                self._numpy_index_include_filtered = include_filtered
            assert self._numpy_index is not None
            return self._numpy_index

    async def search_semantic(
        self,
        query: str,
        channel_id: int | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
        limit: int = 50,
        offset: int = 0,
        min_length: int | None = None,
        max_length: int | None = None,
        include_filtered: bool = False,
    ) -> SearchResult:
        if self._embedding_service is None:
            raise RuntimeError("Semantic search is unavailable.")
        query_embedding = await self._embedding_service.embed_query(query)

        if self._search.vec_available:
            messages, total = await self._search.messages.search_semantic_messages(
                query_embedding,
                channel_id=channel_id,
                date_from=date_from,
                date_to=date_to,
                limit=limit,
                offset=offset,
                min_length=min_length,
                max_length=max_length,
                include_filtered=include_filtered,
            )
            return SearchResult(messages=messages, total=total, query=query)

        # Fallback: numpy cosine similarity
        if not self._search.numpy_available:
            raise RuntimeError(
                "Semantic search is unavailable: sqlite-vec is not loaded and numpy is not installed."
            )
        index = await self._ensure_numpy_index(include_filtered=include_filtered)
        if index.size == 0:
            return SearchResult(messages=[], total=0, query=query)
        # Filtered channels are already excluded (or included) at index load
        # time — the load scope IS the include_filtered contract (#1541).
        # Channel filter must constrain the candidate pool BEFORE the top-K
        # selection: otherwise noise from other channels crowds the wanted
        # messages out of the candidate window and the post-filter finds
        # nothing (issue #1541). Date bounds are post-filters — their bug was
        # the string comparison, fixed below.
        allowed_ids: set[int] | None = None
        if channel_id is not None:
            allowed_ids = await self._channel_candidate_ids(channel_id)
            if not allowed_ids:
                return SearchResult(messages=[], total=0, query=query)
        parsed_from = _parse_date_bound(date_from) if date_from else None
        parsed_to = _parse_date_bound(date_to) if date_to else None
        if parsed_to is not None and len(date_to or "") == 10:  # date-only "to" is inclusive
            parsed_to += timedelta(days=1)
        # Over-fetch to allow for post-filtering
        fetch_k = min(index.size, max((limit + offset) * 4, 200))
        neighbors = await self._compute(index.search, query_embedding, k=fetch_k, allowed_ids=allowed_ids)
        top_ids = [mid for mid, _score in neighbors]
        filtered: list = []
        for msg in await self._search.messages.get_by_ids(top_ids):
            if channel_id is not None and msg.channel_id != channel_id:
                continue
            if parsed_from is not None and msg.date is not None and _as_aware(msg.date) < parsed_from:
                continue
            if parsed_to is not None and msg.date is not None and _as_aware(msg.date) >= parsed_to:
                continue
            text_len = len(msg.text) if msg.text else 0
            if min_length is not None and text_len < min_length:
                continue
            if max_length is not None and text_len > max_length:
                continue
            filtered.append(msg)
        total = len(filtered)
        messages = filtered[offset : offset + limit]
        return SearchResult(messages=messages, total=total, query=query)

    async def _channel_candidate_ids(self, channel_id: int) -> set[int]:
        cur = await self._search.messages._db.execute(
            "SELECT id FROM messages WHERE channel_id = ?", (channel_id,)
        )
        return {int(row["id"]) for row in await cur.fetchall()}

    async def search_hybrid(
        self,
        query: str,
        channel_id: int | None = None,
        date_from: str | None = None,
        date_to: str | None = None,
        limit: int = 50,
        offset: int = 0,
        is_fts: bool = False,
        min_length: int | None = None,
        max_length: int | None = None,
        include_filtered: bool = False,
    ) -> SearchResult:
        if self._embedding_service is None:
            raise RuntimeError("Hybrid search is unavailable.")
        query_embedding = await self._embedding_service.embed_query(query)
        messages, total = await self._search.messages.search_hybrid_messages(
            query=query,
            query_embedding=query_embedding,
            channel_id=channel_id,
            date_from=date_from,
            date_to=date_to,
            limit=limit,
            offset=offset,
            is_fts=is_fts,
            min_length=min_length,
            max_length=max_length,
            include_filtered=include_filtered,
        )
        return SearchResult(messages=messages, total=total, query=query)
