from __future__ import annotations

import logging
from typing import TYPE_CHECKING

logger = logging.getLogger(__name__)

if TYPE_CHECKING:
    pass


class NumpySemanticIndex:
    """In-memory cosine-similarity index using numpy.

    Fallback for environments where sqlite-vec is not available.
    Embeddings are stored as numpy arrays keyed by message_id.
    """

    def __init__(self) -> None:
        self._ids: list[int] = []
        self._matrix = None  # numpy ndarray, shape (N, dims), lazy import

    def load(self, embeddings: list[tuple[int, list[float]]]) -> None:
        """Load pre-computed embeddings.  ``embeddings`` is a list of (message_id, vector)."""
        if not embeddings:
            self._ids = []
            self._matrix = None
            return
        try:
            import numpy as np
        except ImportError as exc:
            raise RuntimeError(
                "numpy is required for the portable semantic fallback. "
                "Install it with: pip install numpy"
            ) from exc

        self._ids = [mid for mid, _ in embeddings]
        self._matrix = np.array([vec for _, vec in embeddings], dtype=np.float32)
        # pre-normalise rows so cosine similarity = dot product
        norms = np.linalg.norm(self._matrix, axis=1, keepdims=True)
        norms = np.where(norms == 0, 1.0, norms)
        self._matrix = self._matrix / norms

    def search(self, query_vec: list[float], k: int = 50) -> list[tuple[int, float]]:
        """Return (message_id, score) sorted descending by cosine similarity."""
        if self._matrix is None or not self._ids:
            return []
        try:
            import numpy as np
        except ImportError:
            return []

        q = np.array(query_vec, dtype=np.float32)
        norm = np.linalg.norm(q)
        if norm > 0:
            q = q / norm

        scores = self._matrix @ q  # shape (N,)
        k = min(k, len(self._ids))
        top_indices = np.argpartition(scores, -k)[-k:]
        top_indices = top_indices[np.argsort(scores[top_indices])[::-1]]
        return [(self._ids[i], float(scores[i])) for i in top_indices]

    @property
    def size(self) -> int:
        return len(self._ids)
