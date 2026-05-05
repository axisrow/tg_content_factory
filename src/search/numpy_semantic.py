from __future__ import annotations

import logging
from typing import TYPE_CHECKING

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    pass


class NumpySemanticIndex:
    """In-memory cosine-similarity index backed by scikit-learn.

    Fallback for environments where sqlite-vec is not available.
    Embeddings are stored as numpy arrays keyed by message_id while nearest
    neighbor search is delegated to sklearn.
    """

    def __init__(self) -> None:
        self._ids: list[int] = []
        self._matrix = None  # numpy ndarray, shape (N, dims), lazy import
        self._index = None  # sklearn.neighbors.NearestNeighbors, lazy import

    def load(self, embeddings: list[tuple[int, list[float]]]) -> None:
        """Load pre-computed embeddings.  ``embeddings`` is a list of (message_id, vector)."""
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

    def search(self, query_vec: list[float], k: int = 50) -> list[tuple[int, float]]:
        """Return (message_id, score) sorted descending by cosine similarity."""
        if self._matrix is None or self._index is None or not self._ids:
            return []
        try:
            import numpy as np
        except ImportError:
            return []

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
