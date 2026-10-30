"""Versioned project enrollment; the marker contains no session or access token."""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

logger = logging.getLogger(__name__)


class ProjectBinding(BaseModel):
    model_config = ConfigDict(extra="forbid")
    version: int = Field(default=2, ge=2, le=2)
    project_id: UUID
    store_root: str
    token_file: str
    database_path: str | None = None


def read_binding(path: Path) -> ProjectBinding | None:
    try:
        raw = path.read_text(encoding="utf-8").strip()
    except FileNotFoundError:
        return None
    if not raw.startswith("{"):
        return None
    try:
        json.loads(raw)
    except json.JSONDecodeError:
        logger.warning(
            "Gateway marker %s is not valid JSON — ignored; re-run "
            "`tg-agent global enable --account PHONE` to rewrite it.",
            path,
        )
        return None
    # Valid JSON that fails the ProjectBinding schema (e.g. unknown version)
    # still refuses loudly: only *malformed* JSON is treated as absent (#1733).
    binding = ProjectBinding.model_validate_json(raw)
    if not Path(binding.store_root).is_absolute() or not Path(binding.token_file).is_absolute():
        raise ValueError("Gateway marker requires absolute store and token paths")
    return binding


def write_private_json(path: Path, value: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    temporary = path.with_name(path.name + f".{os.getpid()}.tmp")
    descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(value, handle)
            handle.flush()
            os.fsync(handle.fileno())
        temporary.replace(path)
    finally:
        temporary.unlink(missing_ok=True)
