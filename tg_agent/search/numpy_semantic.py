from __future__ import annotations

import json
import logging
from typing import TYPE_CHECKING

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    import numpy as np
    from sklearn.neighbors import NearestNeighbors


class NumpySemanticIndex:
    """In-memory cosine-similarity index backed by scikit-learn.

    Fallback for environments where sqlite-vec is not available.
    Embeddings are stored as numpy arrays keyed by message_id while nearest
    neighbor search is delegated to sklearn.
    """

    def __init__(self) -> None:
        self._ids: list[int] = []
        self._matrix: np.ndarray | None = None  # shape (N, dims), lazy import
        self._index: NearestNeighbors | None = None  # lazy import
        self._chunks: list[np.ndarray] = []

    def add_json_batch(self, rows: list[tuple[int, str]]) -> None:
        """Decode only one DB page at a time, retaining compact float32 chunks."""
        import numpy as np

        ids, vectors = [], []
        for message_id, raw in rows:
            try:
                vector = json.loads(raw)
            except (ValueError, TypeError):
                continue
            ids.append(message_id)
            vectors.append(vector)
        if vectors:
            chunk = np.asarray(vectors, dtype=np.float32)
            self._chunks.append(chunk)
            self._ids.extend(ids)

    def finish_loading(self) -> None:
        """Publish a contiguous matrix after all compact chunks have arrived."""
        if not self._chunks:
            return
        import numpy as np
        from sklearn.neighbors import NearestNeighbors

        self._matrix = self._chunks[0] if len(self._chunks) == 1 else np.concatenate(self._chunks)
        self._chunks.clear()
        self._index = NearestNeighbors(metric="cosine")
        self._index.fit(self._matrix)

    def load(self, embeddings: list[tuple[int, list[float]]]) -> None:
        """Load pre-computed embeddings.  ``embeddings`` is a list of (message_id, vector)."""
        self._chunks.clear()
        if not embeddings:
            self._ids = []
            self._matrix = None
            self._index = None
            return
        try:
            import numpy as np
            from sklearn.neighbors import NearestNeighbors
        except ImportError as exc:
            raise RuntimeError(
                "numpy and scikit-learn are required for the portable semantic fallback. "
                "Install them with: pip install numpy scikit-learn"
            ) from exc

        self._ids = [mid for mid, _ in embeddings]
        self._matrix = np.array([vec for _, vec in embeddings], dtype=np.float32)
        self._index = NearestNeighbors(metric="cosine")
        self._index.fit(self._matrix)

    def search(
        self, query_vec: list[float], k: int = 50, allowed_ids: set[int] | None = None
    ) -> list[tuple[int, float]]:
        """Return (message_id, score) sorted descending by cosine similarity.

        ``allowed_ids`` restricts the neighbor search to that subset of message
        ids BEFORE top-k selection — a channel filter applied only after the
        global top-k would let noise from other channels crowd the wanted
        messages out of the candidate window entirely (issue #1541).
        """
        if self._matrix is None or self._index is None or not self._ids:
            return []
        try:
            import numpy as np
        except ImportError:
            return []

        if allowed_ids is not None:
            positions = [i for i, mid in enumerate(self._ids) if mid in allowed_ids]
            if not positions:
                return []
            from sklearn.neighbors import NearestNeighbors

            sub_index = NearestNeighbors(metric="cosine")
            sub_index.fit(self._matrix[positions])
            k = min(k, len(positions))
            q = np.array([query_vec], dtype=np.float32)
            distances, indices = sub_index.kneighbors(q, n_neighbors=k)
            return [
                (self._ids[positions[int(idx)]], float(1.0 - distance))
                for distance, idx in zip(distances[0], indices[0], strict=False)
            ]

        k = min(k, len(self._ids))
        if k <= 0:
            return []

        q = np.array([query_vec], dtype=np.float32)
        distances, indices = self._index.kneighbors(q, n_neighbors=k)
        return [
            (self._ids[int(idx)], float(1.0 - distance))
            for distance, idx in zip(distances[0], indices[0], strict=False)
        ]

    @property
    def size(self) -> int:
        return len(self._ids)
