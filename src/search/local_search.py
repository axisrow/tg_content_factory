from __future__ import annotations

from src.database.bundles import SearchBundle
from src.models import SearchResult
from src.search.numpy_semantic import NumpySemanticIndex
from src.services.embedding_service import EmbeddingService


class LocalSearch:
    def __init__(self, search: SearchBundle, embedding_service: EmbeddingService | None = None):
        self._search = search
        self._embedding_service = embedding_service
        self._numpy_index: NumpySemanticIndex | None = None
        self._numpy_index_loaded: bool = False

    def invalidate_numpy_index(self) -> None:
        """Reset the cached numpy index so it is rebuilt on the next search."""
        self._numpy_index = None
        self._numpy_index_loaded = False

    async def search(
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
    ) -> SearchResult:
        messages, total = await self._search.search_messages(
            query=query,
            channel_id=channel_id,
            date_from=date_from,
            date_to=date_to,
            limit=limit,
            offset=offset,
            is_fts=is_fts,
            min_length=min_length,
            max_length=max_length,
        )
        return SearchResult(messages=messages, total=total, query=query)

    async def _ensure_numpy_index(self) -> NumpySemanticIndex:
        """Lazily load and build the numpy in-memory index from the JSON table."""
        if not self._numpy_index_loaded:
            index = NumpySemanticIndex()
            embeddings = await self._search.messages.load_all_embeddings_json()
            index.load(embeddings)
            self._numpy_index = index
            self._numpy_index_loaded = True
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
            )
            return SearchResult(messages=messages, total=total, query=query)

        # Fallback: numpy cosine similarity
        if not self._search.numpy_available:
            raise RuntimeError(
                "Semantic search is unavailable: sqlite-vec is not loaded and numpy is not installed."
            )
        index = await self._ensure_numpy_index()
        if index.size == 0:
            return SearchResult(messages=[], total=0, query=query)
        top_ids = [mid for mid, _score in index.search(query_embedding, k=limit + offset)]
        paginated_ids = top_ids[offset : offset + limit]
        messages = []
        for mid in paginated_ids:
            msg = await self._search.messages.get_by_id(mid)
            if msg is not None:
                messages.append(msg)
        return SearchResult(messages=messages, total=len(top_ids), query=query)

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
        )
        return SearchResult(messages=messages, total=total, query=query)
