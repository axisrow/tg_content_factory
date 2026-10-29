"""Versioned, explicit wire types; native Telethon objects never cross this API."""

from typing import Annotated, Literal, Protocol
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class Payload(BaseModel):
    model_config = ConfigDict(extra="forbid")
    account_id: UUID


class History(Payload):
    kind: Literal["history"]
    peer: int | str
    after_id: int = Field(default=0, ge=0)
    limit: int = Field(default=100, ge=1, le=100)


class Send(Payload):
    kind: Literal["send"]
    peer: int | str
    text: str = Field(min_length=1, max_length=4096)
    reply_to: int | None = None
    automatic: bool = False


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
    artifact_ids: list[UUID] = Field(min_length=1, max_length=10)
    caption: str | None = Field(default=None, max_length=1024)


Operation = Annotated[History | Send | Edit | Delete | Publish, Field(discriminator="kind")]


class Submission(BaseModel):
    model_config = ConfigDict(extra="forbid")
    request_id: UUID
    operation: Operation


class TelegramAccess(Protocol):
    async def execute(self, project: str, operation: Operation) -> dict: ...
    async def close(self) -> None: ...
