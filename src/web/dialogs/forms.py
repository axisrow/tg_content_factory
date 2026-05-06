from __future__ import annotations

from types import UnionType
from typing import Any, TypeVar, get_args, get_origin

from fastapi import Request
from pydantic import BaseModel, ConfigDict, Field, TypeAdapter
from starlette.datastructures import FormData

T = TypeVar("T", bound=BaseModel)


class _FrozenForm(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True, frozen=True, str_strip_whitespace=True)


class RefreshDialogsForm(_FrozenForm):
    phone: str = ""


class CacheClearForm(_FrozenForm):
    phone: str = ""


class LeaveDialogsForm(_FrozenForm):
    phone: str = ""
    channel_ids: list[str] = Field(default_factory=list)

    @property
    def dialogs(self) -> list[tuple[int, str]]:
        parsed: list[tuple[int, str]] = []
        for item in self.channel_ids:
            parts = item.split(":", 1)
            if len(parts) == 2 and parts[0].lstrip("-").isdigit():
                parsed.append((int(parts[0]), parts[1]))
        return parsed


class SendMessageForm(_FrozenForm):
    phone: str = ""
    recipient: str = ""
    text: str = ""


class EditMessageForm(_FrozenForm):
    phone: str = ""
    chat_id: str = ""
    message_id: str = ""
    text: str = ""


class MessageIdsForm(_FrozenForm):
    phone: str = ""
    chat_id: str = ""
    message_ids: str = ""

    @property
    def ids(self) -> list[int]:
        return [int(item.strip()) for item in self.message_ids.split(",") if item.strip().isdigit()]


class ForwardMessagesForm(_FrozenForm):
    phone: str = ""
    from_chat: str = ""
    to_chat: str = ""
    message_ids: str = ""

    @property
    def ids(self) -> list[int]:
        return [int(item.strip()) for item in self.message_ids.split(",") if item.strip().isdigit()]


class PinMessageForm(_FrozenForm):
    phone: str = ""
    chat_id: str = ""
    message_id: str = ""
    notify: bool = False


class UnpinMessageForm(_FrozenForm):
    phone: str = ""
    chat_id: str = ""
    message_id: str = ""

    @property
    def parsed_message_id(self) -> int | None:
        return int(self.message_id) if self.message_id and self.message_id.isdigit() else None


class DownloadMediaForm(_FrozenForm):
    phone: str = ""
    chat_id: str = ""
    message_id: str = ""


class EditAdminForm(_FrozenForm):
    phone: str = ""
    chat_id: str = ""
    user_id: str = ""
    title: str | None = None
    is_admin: bool = False


class EditPermissionsForm(_FrozenForm):
    phone: str = ""
    chat_id: str = ""
    user_id: str = ""
    until_date: str | None = None
    send_messages: bool | None = None
    send_media: bool | None = None


class ChatActionForm(_FrozenForm):
    phone: str = ""
    chat_id: str = ""


class KickParticipantForm(ChatActionForm):
    user_id: str = ""


class MarkReadForm(ChatActionForm):
    max_id: str | None = None

    @property
    def parsed_max_id(self) -> int | None:
        return int(self.max_id) if self.max_id and self.max_id.isdigit() else None


class CreateChannelForm(_FrozenForm):
    phone: str = ""
    title: str = ""
    about: str = ""
    username: str = ""


def _is_optional(annotation: object) -> bool:
    origin = get_origin(annotation)
    if origin is UnionType or str(origin) == "typing.Union":
        return type(None) in get_args(annotation)
    return False


def _is_bool_annotation(annotation: object) -> bool:
    if annotation is bool:
        return True
    return bool in get_args(annotation)


def _is_list_annotation(annotation: object) -> bool:
    return get_origin(annotation) is list


def _checked(form: FormData, name: str) -> str:
    return "true" if form.get(name) in ("1", "true", "on") else "false"


def _form_payload(form: FormData, model: type[T]) -> tuple[dict[str, Any], bool]:
    payload: dict[str, Any] = {}
    uses_mixed_fields = False
    for name, field in model.model_fields.items():
        annotation = field.annotation
        if _is_list_annotation(annotation):
            payload[name] = form.getlist(name)
            uses_mixed_fields = True
            continue
        if _is_bool_annotation(annotation):
            if name not in form and _is_optional(annotation):
                continue
            else:
                payload[name] = _checked(form, name)
            continue
        if name in form:
            value = form.get(name)
            if value == "" and _is_optional(annotation):
                continue
            payload[name] = value
        elif _is_optional(annotation):
            continue
        else:
            payload[name] = field.default if field.default is not None else ""
    return payload, uses_mixed_fields


async def parse_dialog_form(request: Request, model: type[T]) -> T:
    form = await request.form()
    payload, uses_mixed_fields = _form_payload(form, model)
    if uses_mixed_fields:
        return TypeAdapter(model).validate_python(payload)
    return model.model_validate_strings(payload)
