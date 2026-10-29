"""Versioned, explicit wire types; native Telethon objects never cross this API."""

from datetime import datetime
from typing import Annotated, Literal, Protocol, Union
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, SecretStr

from tg_agent.gateway.action_protocol import READ_ACTIONS, PeerReference, build_action_models


class Enrollment(BaseModel):
    model_config = ConfigDict(extra="forbid")
    project_id: UUID
    root: str
    token: SecretStr


class AuthRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    action: Literal["send_code", "resend_code", "verify_code", "qr_start", "qr_status", "qr_password", "import"]
    phone: str = ""
    code: SecretStr = SecretStr("")
    phone_code_hash: str = ""
    password_2fa: SecretStr = SecretStr("")
    qr_id: str = ""
    session_string: SecretStr = SecretStr("")


class ImportAccount(BaseModel):
    model_config = ConfigDict(extra="forbid")
    phone: str
    session: SecretStr
    source_secret: SecretStr | None = None
    replace: bool = False


class ImportNotificationBot(BaseModel):
    model_config = ConfigDict(extra="forbid")
    project_id: UUID
    tg_user_id: int
    tg_username: str | None = None
    bot_id: int | None = None
    bot_username: str
    bot_token: SecretStr


class GrantAccount(BaseModel):
    model_config = ConfigDict(extra="forbid")
    project_id: UUID
    phone: str
    primary: bool = False


class UpdateGrant(BaseModel):
    model_config = ConfigDict(extra="forbid")
    active: bool | None = None
    primary: bool = False


class AdministerAccount(BaseModel):
    model_config = ConfigDict(extra="forbid")
    phone: str
    active: bool | None = None
    delete: bool = False


class Payload(BaseModel):
    model_config = ConfigDict(extra="forbid")
    account_id: UUID


class History(Payload):
    kind: Literal["history"]
    peer: int | str
    peer_type: Literal["channel", "chat", "user", "auto"] = "channel"
    after_id: int = Field(default=0, ge=0)
    limit: int = Field(default=100, ge=1, le=100)


class Send(Payload):
    kind: Literal["send"]
    peer: int | str
    peer_type: Literal["channel", "chat", "user", "auto"] = "auto"
    text: str = Field(min_length=1, max_length=4096)
    reply_to: int | None = None
    automatic: bool = False
    link_preview: bool = False


class Acknowledgement(BaseModel):
    model_config = ConfigDict(extra="forbid")
    event_id: int = Field(ge=0)


class Edit(Payload):
    kind: Literal["edit"]
    peer: int | str
    message_id: int = Field(gt=0)
    text: str = Field(min_length=1, max_length=4096)


class Delete(Payload):
    kind: Literal["delete"]
    peer: int | str
    message_ids: list[int] = Field(min_length=1, max_length=100)


class Publish(Payload):
    kind: Literal["publish"]
    peer: int | str
    peer_type: Literal["channel", "chat", "user", "auto"] = "auto"
    artifact_ids: list[UUID] = Field(min_length=1, max_length=10)
    caption: str | None = Field(default=None, max_length=1024)
    schedule: datetime | None = None
    reply_to: int | None = None
    force_document: bool = False


class BusinessAction(Payload):
    automatic: bool = False


class Profile(Payload):
    kind: Literal["profile"]
    include_avatar: bool = False


class InspectPeer(Payload):
    kind: Literal["inspect_peer"]
    peer: int | str
    peer_type: Literal["channel", "chat", "user", "auto"] = "channel"
    statistics: bool = False
    topics: bool = False


class Dialogs(Payload):
    kind: Literal["dialogs"]
    include_dm: bool = False
    refresh: bool = False
    mode: Literal["cached", "full"] = "full"


class Search(Payload):
    kind: Literal["search"]
    mode: Literal["telegram", "my_chats", "channel", "quota"]
    query: str = ""
    channel_id: int | None = None
    limit: int = Field(default=50, ge=1, le=1000)


class DmHistory(Payload):
    kind: Literal["dm_history"]
    dialog_id: int
    after_id: int = Field(default=0, ge=0)
    limit: int = Field(default=100, ge=1, le=100)


class Notification(Payload):
    kind: Literal["notification"]
    action: Literal["setup", "delete", "status", "send"]
    text: str = Field(default="", max_length=4096)
    chat_id: int | None = None
    bot_name_prefix: str = "LeadHunter"
    bot_username_prefix: str = "leadhunter_"


class MessagePage(Payload):
    kind: Literal["message_page"]
    peer: int | str
    peer_type: Literal["channel", "chat", "user", "auto"] = "auto"
    limit: int = Field(default=100, ge=1, le=100)
    offset_id: int = Field(default=0, ge=0)
    topic_id: int | None = None
    include_reaction_users: bool = False
    reaction_users_limit: int = Field(default=20, ge=1, le=100)


class ScheduledDelete(Payload):
    kind: Literal["scheduled_delete"]
    peer: int
    peer_type: Literal["channel", "chat", "user", "auto"] = "auto"
    message_ids: list[int] = Field(min_length=1, max_length=100)


class VoiceSource(Payload):
    kind: Literal["voice_source"]
    peer: int | str | PeerReference
    message_id: int = Field(gt=0)
    metadata_only: bool = False


class ReplyOwner(BaseModel):
    model_config = ConfigDict(extra="forbid")
    account_id: UUID
    dialog_id: int
    release: bool = False


ACTION_MODELS = build_action_models(BusinessAction)
Operation = Annotated[Union[tuple([History, Send, Edit, Delete, Publish, Profile, InspectPeer,
                                  Dialogs, Search, DmHistory, Notification, MessagePage, ScheduledDelete, VoiceSource,
                                  *ACTION_MODELS.values()])],
                      Field(discriminator="kind")]


def is_read_operation(operation) -> bool:
    if isinstance(operation, Notification):
        return operation.action == "status"
    return operation.kind in READ_ACTIONS | {
        "history", "profile", "inspect_peer", "dialogs", "search", "dm_history", "message_page", "voice_source",
    }


class Submission(BaseModel):
    model_config = ConfigDict(extra="forbid")
    request_id: UUID
    operation: Operation


class TelegramAccess(Protocol):
    async def execute(self, project: str, operation: Operation) -> dict: ...
    async def close(self) -> None: ...
