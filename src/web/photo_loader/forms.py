from __future__ import annotations

import uuid
from collections.abc import Mapping
from datetime import datetime
from pathlib import Path
from typing import Any

from fastapi import UploadFile
from pydantic import BaseModel, ConfigDict

from src.models import PhotoSendMode
from src.services.photo_task_service import PhotoTarget
from src.utils.datetime import parse_required_schedule_datetime

UPLOAD_ROOT = Path("data/photo_uploads")
FormMapping = Mapping[str, Any]


class _FrozenForm(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True, frozen=True)


class PhotoRefreshForm(_FrozenForm):
    phone: str


class PhotoSendForm(_FrozenForm):
    phone: str
    target_dialog_id: str
    target_title: str
    target_type: str
    send_mode: str
    caption: str
    photos: list[UploadFile]


class PhotoScheduleForm(_FrozenForm):
    phone: str
    target_dialog_id: str
    target_title: str
    target_type: str
    send_mode: str
    caption: str
    schedule_at: str
    photos: list[UploadFile]


class PhotoBatchForm(_FrozenForm):
    phone: str
    target_dialog_id: str
    target_title: str
    target_type: str
    caption: str
    manifest_text: str


class PhotoAutoCreateForm(_FrozenForm):
    phone: str
    target_dialog_id: str
    target_title: str
    target_type: str
    folder_path: str
    send_mode: str
    caption: str
    interval_minutes: int | None


class PhotoPhoneForm(_FrozenForm):
    phone: str


class PhotoAutoUpdateForm(_FrozenForm):
    phone: str
    values: dict[str, object]


async def persist_uploads(files: list[UploadFile], folder_name: str) -> list[str]:
    target_dir = UPLOAD_ROOT / folder_name
    target_dir.mkdir(parents=True, exist_ok=True)
    stored: list[str] = []
    for upload in files:
        if not upload.filename:
            continue
        data = await upload.read()
        if not data:
            continue
        safe_name = f"{uuid.uuid4().hex}_{Path(upload.filename).name}"
        path = target_dir / safe_name
        path.write_bytes(data)
        stored.append(str(path))
    return stored


def parse_target(form: FormMapping, dialogs: list[dict]) -> PhotoTarget:
    dialog_id = int(str(form.get("target_dialog_id", "0")))
    title = str(form.get("target_title", "")).strip() or None
    target_type = str(form.get("target_type", "")).strip() or None
    if not title or not target_type:
        for dialog in dialogs:
            if int(dialog["channel_id"]) == dialog_id:
                title = dialog.get("title")
                target_type = dialog.get("channel_type")
                break
    return PhotoTarget(dialog_id=dialog_id, title=title, target_type=target_type)


def parse_schedule_at(value: str) -> datetime:
    return parse_required_schedule_datetime(value)


def parse_auto_update_form(form: FormMapping) -> PhotoAutoUpdateForm:
    values: dict[str, object] = {}
    if form.get("folder"):
        values["folder_path"] = form["folder"]
    if form.get("mode"):
        values["send_mode"] = PhotoSendMode(form["mode"])
    if form.get("caption") is not None:
        values["caption"] = form["caption"]
    interval = form.get("interval_minutes")
    if interval and str(interval).isdigit():
        values["interval_minutes"] = int(interval)
    if form.get("is_active"):
        values["is_active"] = form["is_active"] in ("1", "true", "on")
    return PhotoAutoUpdateForm(phone=str(form.get("phone", "")), values=values)
