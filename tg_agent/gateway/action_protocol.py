"""Explicit business-action registry with schemas derived from the existing typed facade."""

import inspect
from typing import Literal, get_type_hints
from uuid import UUID

from pydantic import BaseModel, ConfigDict, create_model

ACTION_NAMES = (
    "send_reaction", "ensure_client", "send_message", "edit_message", "delete_messages", "forward_messages",
    "pin_message", "unpin_message", "mark_read", "set_dialog_folder", "get_participants", "read_history",
    "get_broadcast_stats", "edit_admin", "edit_permissions", "kick_participant", "download_media",
    "download_media_sized", "transcribe_voice", "leave_dialogs", "delete_dialogs", "create_channel", "join_dialog",
)
READ_ACTIONS = frozenset({"ensure_client", "get_participants", "read_history", "get_broadcast_stats",
                          "download_media", "download_media_sized"})
LOCAL_ARGUMENTS = frozenset({"phone", "native", "allow_any", "resolve_entity", "resolve_entities", "db"})
PATH_ARGUMENTS = frozenset({"output_path", "output_dir", "root_dir", "base_dir", "download_dir", "path"})


class PeerReference(BaseModel):
    model_config = ConfigDict(extra="forbid")
    account_id: UUID
    peer_type: Literal["channel", "chat", "user"]
    peer_id: int


def build_action_models(base):
    from tg_agent.services.telegram_actions import TelegramActionService

    models = {}
    for name in ACTION_NAMES:
        method = getattr(TelegramActionService, name)
        annotations = get_type_hints(method)
        fields = {}
        for argument, parameter in inspect.signature(method).parameters.items():
            if argument in {"self", *LOCAL_ARGUMENTS, *PATH_ARGUMENTS}:
                continue
            annotation = annotations.get(argument, str)
            if argument in {"chat_id", "recipient", "from_chat", "to_chat", "user_id"}:
                annotation = int | str | PeerReference
            default = ... if parameter.default is inspect.Parameter.empty else parameter.default
            fields[argument] = (annotation, default)
        title = "".join(part.title() for part in name.split("_"))
        arguments = create_model(title + "Arguments", __config__=ConfigDict(extra="forbid"), **fields)
        models[name] = create_model("Gateway" + title, __base__=base,
                                    kind=(Literal[name], name), arguments=(arguments, ...))
    return models
