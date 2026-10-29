"""Private, project-scoped temporary files addressed only by opaque UUIDs."""

import hashlib
import os
import time
from pathlib import Path
from uuid import UUID


class ArtifactStore:
    def __init__(self, root: Path):
        self.root = root / "artifacts"

    def path(self, project: str, artifact: UUID) -> Path:
        scope = hashlib.sha256(project.encode()).hexdigest()
        return self.root / scope / str(artifact)

    def create(self, project: str, artifact: UUID):
        path = self.path(project, artifact)
        path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
        descriptor = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
        return os.fdopen(descriptor, "wb")

    def resolve(self, project: str, artifact: UUID) -> Path:
        path = self.path(project, artifact)
        if path.stat().st_mtime < time.time() - 86400:
            path.unlink(missing_ok=True)
            raise FileNotFoundError("Artifact expired")
        return path
