"""The shared gateway, served over the session-owner relay (#1868).

One gateway process owns the shared accounts' Telegram connections. Projects
reach them through the relay (``tg_agent.telegram.relay``): their Telegram
requests run on the gateway's connections (``invoke``), and the control plane —
catalog, grants, logins, DM events, administration — consists of relay
operations. There is no HTTP server and no operation queue: requests run
concurrently on the connection, gated by the gateway's own flood guards.
"""

from __future__ import annotations

import asyncio
import hmac
import logging
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ValidationError

from tg_agent.gateway.auth import GatewayLogins
from tg_agent.gateway.protocol import (
    AdministerAccount,
    AuthRequest,
    Enrollment,
    GrantAccount,
    ImportAccount,
    ReplyOwner,
    UpdateGrant,
)
from tg_agent.gateway.store import EventCursorExpiredError, GatewayConflictError, GatewayStore
from tg_agent.telegram.relay import RELAY_PROTOCOL, OpHandler, RelayAccountUnavailableError, RelayPeer

logger = logging.getLogger(__name__)


class GatewayStatusError(RuntimeError):
    """A refused control operation; ``status`` mirrors the HTTP code it used to be."""

    def __init__(self, status: int, message: str):
        super().__init__(message)
        self.status = status


@dataclass(frozen=True)
class Grant:
    """Who opened the relay: a project (by token) or the gateway administrator."""

    project: str | None

    @property
    def admin(self) -> bool:
        return self.project is None


def _model(cls: type[BaseModel], message: dict) -> Any:
    try:
        return cls.model_validate(message.get("body") or {})
    except ValidationError as exc:
        raise GatewayStatusError(422, f"invalid {cls.__name__}: {exc.error_count()} error(s)") from None


class GatewayHub:
    """Relay owner and operation table of one gateway process."""

    def __init__(self, store: GatewayStore, access: Any, *, admin_token: str, request_shutdown=None):
        self.store = store
        self.access = access
        self._admin_token = admin_token.strip()
        self.logins = GatewayLogins(store, access)
        self.request_shutdown = request_shutdown
        self._event_listeners: dict[RelayPeer, str] = {}  # subscribed connection -> its project
        if hasattr(access, "on_event"):
            access.on_event = self.notify_events
        project_ops = {
            "auth": self._auth,
            "accounts": self._accounts,
            "account.update": self._update_account,
            "account.flood_clear": self._clear_flood,
            "account.detach": self._detach,
            "account.export_session": self._export_session,
            "status": self._status,
            "events": self._events,
            "events.recovery": self._event_recovery,
            "events.ack": self._acknowledge,
            "events.subscribe": self._subscribe_events,
            "reply_owner": self._reply_owner,
            "reply_owner.check": self._can_auto_reply,
            "bots.export": self._export_bots,
        }
        admin_ops = {
            "admin.projects.register": self._register_project,
            "admin.shutdown": self._shutdown,
            "admin.accounts.import": self._import_account,
            "admin.accounts.list": self._all_accounts,
            "admin.accounts.update": self._administer_account,
            "admin.accounts.export": self._export_account,
            "admin.grants": self._grant,
        }
        self.ops: dict[str, OpHandler] = {
            name: self._for_project(handler) for name, handler in project_ops.items()
        }
        self.ops.update({name: self._for_admin(handler) for name, handler in admin_ops.items()})

    # ------------------------------------------------------------------ owner

    async def authorize(self, token: str) -> Grant | None:
        if self._admin_token and hmac.compare_digest(token.encode(), self._admin_token.encode()):
            return Grant(None)
        project = await self.store.authenticate(token)
        return Grant(project) if project else None

    async def phones(self, grant: Grant) -> list[str]:
        if grant.project is None:
            return []
        return [row["phone"] for row in await self.store.accounts(grant.project) if row.get("active")]

    async def client(self, grant: Grant, phone: str) -> Any:
        if grant.project is None:
            raise RelayAccountUnavailableError(f"{phone} is not granted to this project")
        rows = [row for row in await self.store.accounts(grant.project) if row.get("active")]
        row = next((row for row in rows if row["phone"] == phone), None)
        if row is None:
            raise RelayAccountUnavailableError(f"{phone} is not granted to this project")
        session = await self.access.connection(grant.project, str(row["id"]))
        return session.raw_client

    # -------------------------------------------------------------- plumbing

    @staticmethod
    def _for_project(handler):
        async def run(grant: Grant, message: dict, peer: RelayPeer) -> Any:
            if grant.admin:
                raise GatewayStatusError(403, "project token required")
            return await handler(grant.project, message, peer)

        return run

    @staticmethod
    def _for_admin(handler):
        async def run(grant: Grant, message: dict, peer: RelayPeer) -> Any:
            if not grant.admin:
                raise GatewayStatusError(403, "gateway administrator token required")
            return await handler(message)

        return run

    async def _restart_accounts(self) -> None:
        start = getattr(self.access, "start", None)
        if start is not None:
            await start()

    async def _drop_account(self, account: str) -> None:
        remove = getattr(self.access, "remove_account", None)
        if remove is not None:
            await remove(account)

    async def notify_events(self, account: str) -> None:
        """A DM event of ``account`` was journaled: projects granted that account pull it now.

        Other subscribers hear nothing — no empty round trip, and no hint that a
        DM arrived on an account they cannot see.
        """
        granted: dict[str, bool] = {}
        targets: list[RelayPeer] = []
        for peer, project in tuple(self._event_listeners.items()):
            if peer.closed:
                self._event_listeners.pop(peer, None)
                continue
            if project not in granted:
                granted[project] = any(str(row["id"]) == account for row in await self.store.accounts(project))
            if granted[project]:
                targets.append(peer)
        # All at once: a subscriber that stopped reading must not delay the others.
        outcomes = await asyncio.gather(*(peer.push("events") for peer in targets), return_exceptions=True)
        for peer, outcome in zip(targets, outcomes, strict=True):
            if isinstance(outcome, (ConnectionError, RuntimeError)):
                self._event_listeners.pop(peer, None)
            elif isinstance(outcome, BaseException):
                raise outcome

    async def _project_account(self, project: str, account_id: str) -> str:
        if account_id not in {str(row["id"]) for row in await self.store.accounts(project)}:
            raise GatewayStatusError(403, "Account is not available to this project")
        return account_id

    # ------------------------------------------------------------ project ops

    async def _auth(self, project: str, message: dict, _peer) -> dict:
        try:
            return await self.logins.execute(project, _model(AuthRequest, message))
        except (ValueError, RuntimeError) as exc:
            # Never reflect codes, passwords or sessions in diagnostics.
            raise GatewayStatusError(409, type(exc).__name__) from None

    async def _accounts(self, project: str, _message, _peer) -> list[dict]:
        return await self.store.catalog(project)

    async def _update_account(self, project: str, message: dict, _peer) -> bool:
        body = _model(UpdateGrant, message)
        try:
            await self.store.update_grant(project, str(message.get("account_id")), active=body.active,
                                          primary=body.primary)
        except KeyError:
            raise GatewayStatusError(404, "Project account not found") from None
        return True

    async def _clear_flood(self, project: str, message: dict, _peer) -> list[str]:
        """Operator flood-clear (#1845): the account-wide timer and every family."""
        account = await self._project_account(project, str(message.get("account_id")))
        return await self.store.clear_account_flood(account)

    async def _detach(self, project: str, message: dict, _peer) -> bool:
        await self.store.revoke(project, str(message.get("account_id")))
        return True

    async def _export_session(self, project: str, message: dict, _peer) -> str:
        try:
            return await self.store.session(project, str(message.get("account_id")))
        except PermissionError:
            raise GatewayStatusError(403, "Account is not available to this project") from None

    async def _status(self, project: str, _message, _peer) -> dict:
        await self._restart_accounts()
        rows = await self.store.accounts(project)
        # Per-family read floods (#1845): the project routes reads past a
        # family-flooded account instead of paying a deferred round trip.
        floods = await self.store.family_floods()
        rows = [{**row, "family_floods": floods.get(row["id"], {})} for row in rows]
        account_status = getattr(self.access, "account_status", None)
        if account_status is not None:
            rows = [{**row, **account_status(row["id"])} for row in rows]
        return {"protocol": RELAY_PROTOCOL, "available": True, "accounts": rows}

    async def _events(self, project: str, message: dict, _peer) -> list[dict]:
        await self.store.expire_events(now=time.time())
        try:
            return await self.store.events(project, int(message.get("after") or 0))
        except EventCursorExpiredError:
            raise GatewayStatusError(410, "resync_required") from None
        except ValueError:
            raise GatewayStatusError(422, "Invalid event cursor") from None

    async def _event_recovery(self, _project: str, _message, _peer) -> int:
        await self.store.expire_events(now=time.time())
        return await self.store.event_floor()

    async def _acknowledge(self, project: str, message: dict, _peer) -> bool:
        await self.store.acknowledge(project, int(message.get("event_id") or 0))
        return True

    async def _subscribe_events(self, project: str, _message, peer: RelayPeer) -> bool:
        self._event_listeners[peer] = project
        return True

    async def _reply_owner(self, project: str, message: dict, _peer) -> bool:
        body = _model(ReplyOwner, message)
        try:
            if body.release:
                await self.store.release_reply_owner(project, str(body.account_id), body.dialog_id)
            else:
                await self.store.assign_reply_owner(project, str(body.account_id), body.dialog_id)
        except GatewayConflictError:
            raise GatewayStatusError(409, "Another project owns automatic replies in this dialog") from None
        except PermissionError:
            raise GatewayStatusError(403, "Account is not granted to this project") from None
        return not body.release

    async def _can_auto_reply(self, project: str, message: dict, _peer) -> bool:
        return await self.store.can_auto_reply(project, str(message.get("account_id")),
                                               int(message.get("dialog_id") or 0))

    async def _export_bots(self, project: str, _message, _peer) -> list[dict]:
        """Notification bots the gateway kept for this project (pre-#1868 enrollments)."""
        from tg_agent.gateway.notifications import list_legacy_bots

        return [bot.model_dump(mode="json") for bot in await list_legacy_bots(self.store, project)]

    # -------------------------------------------------------------- admin ops

    async def _register_project(self, message: dict) -> str:
        body = _model(Enrollment, message)
        try:
            await self.store.register_project(str(body.project_id), Path(body.root), body.token.get_secret_value())
        except GatewayConflictError as exc:
            raise GatewayStatusError(409, str(exc)) from exc
        return str(body.project_id)

    async def _shutdown(self, _message) -> bool:
        if self.request_shutdown is None:
            raise GatewayStatusError(409, "This gateway is not managed by the process runtime")
        asyncio.get_running_loop().call_later(0.1, self.request_shutdown)
        return True

    async def _import_account(self, message: dict) -> str:
        body = _model(ImportAccount, message)
        try:
            source_secret = body.source_secret.get_secret_value() if body.source_secret is not None else None
            account = await self.store.import_account(body.phone, body.session.get_secret_value(),
                                                      source_secret=source_secret, replace=body.replace)
        except ValueError:
            raise GatewayStatusError(409, "Shared account import failed; check source key/session conflict") from None
        if body.replace:
            await self._drop_account(account)
        await self._restart_accounts()
        return account

    async def _all_accounts(self, _message) -> list[dict]:
        return await self.store.catalog()

    async def _administer_account(self, message: dict) -> bool:
        body = _model(AdministerAccount, message)
        try:
            account = await self.store.administer_account(body.phone, active=body.active, delete=body.delete)
        except KeyError:
            raise GatewayStatusError(404, "Shared account not found") from None
        if body.delete or body.active is False:
            await self._drop_account(account)
        elif body.active:
            await self._restart_accounts()
        return True

    async def _export_account(self, message: dict) -> str:
        try:
            return await self.store.export_session(str(message.get("phone") or ""))
        except KeyError:
            raise GatewayStatusError(404, "Shared session not found") from None

    async def _grant(self, message: dict) -> str:
        body = _model(GrantAccount, message)
        try:
            account = await self.store.account_for_phone(body.phone)
        except KeyError:
            raise GatewayStatusError(404, "Shared account not found") from None
        await self.store.grant(str(body.project_id), account, primary=body.primary)
        await self._restart_accounts()
        return account

    async def close(self) -> None:
        await self.logins.close()
