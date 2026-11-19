"""Control-plane request bodies of the shared gateway's relay operations (#1868).

Telegram requests themselves travel as raw MTProto over the relay; only the
administrative and login operations carry these validated bodies.
"""

from typing import Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, SecretStr


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


class ReplyOwner(BaseModel):
    model_config = ConfigDict(extra="forbid")
    account_id: UUID
    dialog_id: int
    release: bool = False
