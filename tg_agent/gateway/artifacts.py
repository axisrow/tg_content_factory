"""Private, project-scoped temporary files addressed only by opaque UUIDs."""

import hashlib
import json
import os
import shutil
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

    def describe(self, project: str, artifact: UUID) -> dict:
        self.resolve(project, artifact)
        try:
            return json.loads(self.path(project, artifact).with_suffix(".json").read_text(encoding="utf-8"))
        except FileNotFoundError:
            return {"filename": "download", "content_type": "application/octet-stream"}

    def finish(self, project: str, artifact: UUID, partial: Path, *, filename: str, content_type: str, digest: str):
        from tg_agent.gateway.project import write_private_json

        target = self.path(project, artifact)
        metadata = target.with_suffix(".json")
        filename = Path(filename.replace("\\", "/")).name.replace("\x00", "") or "upload"
        if filename in {".", ".."}:
            filename = "upload"
        try:
            os.link(partial, target)
        except FileExistsError:
            existing = self.describe(project, artifact)
            if existing.get("sha256") != digest or existing.get("filename") != filename:
                raise ValueError("Artifact ID already contains a different file") from None
        else:
            write_private_json(metadata, {"filename": filename, "content_type": content_type, "sha256": digest})
        finally:
            partial.unlink(missing_ok=True)

    def cleanup(self, *, now=None):
        cutoff = (time.time() if now is None else now) - 86400
        if not self.root.exists():
            return
        for scope in self.root.iterdir():
            if not scope.is_dir() or scope.is_symlink():
                continue
            for path in scope.iterdir():
                if path.is_dir() and not path.is_symlink() and path.stat().st_mtime < cutoff:
                    shutil.rmtree(path)
                if path.is_file() and path.stat().st_mtime < cutoff:
                    path.unlink(missing_ok=True)
