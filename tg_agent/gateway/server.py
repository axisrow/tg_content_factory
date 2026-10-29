"""Project-authenticated gateway API and fair, durable operation dispatcher."""

from __future__ import annotations

import asyncio
import hashlib
import logging
import secrets
import time
from contextlib import asynccontextmanager
from pathlib import Path
from urllib.parse import unquote
from uuid import UUID, uuid4

from fastapi import Depends, FastAPI, HTTPException, Query, Request
from fastapi.responses import FileResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import TypeAdapter
from telethon.errors import FloodWaitError
from telethon_floodgate import HandledFloodWaitError

from tg_agent.gateway.artifacts import ArtifactStore
from tg_agent.gateway.auth import GatewayLogins
from tg_agent.gateway.protocol import (
    Acknowledgement,
    AdministerAccount,
    AuthRequest,
    Enrollment,
    GrantAccount,
    ImportAccount,
    ImportNotificationBot,
    Operation,
    ReplyOwner,
    Send,
    Submission,
    TelegramAccess,
    UpdateGrant,
    is_read_operation,
)
from tg_agent.gateway.store import EventCursorExpiredError, GatewayConflictError, GatewayStore

logger = logging.getLogger(__name__)
operation_adapter = TypeAdapter(Operation)


class GatewayDeferredError(Exception):
    def __init__(self, until: float):
        self.until = until


class Dispatcher:
    """One request/page per turn; projects alternate while both have pending work."""

    def __init__(self, store: GatewayStore, access: TelegramAccess):
        self.store = store
        self.access = access
        self.wake = asyncio.Event()
        self.last_project: str | None = None
        self.current: tuple[str, str] | None = None
        self.operation_task: asyncio.Task | None = None

    async def run(self):
        while True:
            try:
                await self._run()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.error("Gateway dispatcher recovering: error=%s", type(exc).__name__)
                await asyncio.sleep(1)
                await self.store.recover()

    async def _run(self):
        while True:
            self.wake.clear()
            pending = await self.store.pending()
            if not pending:
                try:
                    await asyncio.wait_for(self.wake.wait(), 1)
                except asyncio.TimeoutError:
                    pass
                continue
            projects = list(dict.fromkeys(row["project_id"] for row in pending))
            if self.last_project in projects:
                index = (projects.index(self.last_project) + 1) % len(projects)
            else:
                index = 0
            project = projects[index]
            row = next(row for row in pending if row["project_id"] == project)
            request = row["request_id"]
            if not await self.store.transition(project, request, "pending", "running"):
                continue
            self.last_project = project
            self.current = project, request
            operation = operation_adapter.validate_python(row["payload"])
            self.operation_task = asyncio.create_task(asyncio.wait_for(self.perform(project, operation), timeout=120))
            try:
                result = await self.operation_task
            except GatewayDeferredError as exc:
                await self.store.transition(project, request, "running", "deferred", {"retry_at": exc.until})
            except HandledFloodWaitError as exc:
                await self.store.transition(project, request, "running", "deferred", {
                    "retry_at": exc.info.next_available_at_utc.timestamp(),
                })
            except FloodWaitError as exc:
                until = time.time() + max(1, exc.seconds)
                await self.store.flood(str(operation.account_id), until)
                state = "deferred" if is_read_operation(operation) else "uncertain"
                await self.store.transition(project, request, "running", state, {"retry_at": until})
            except Exception as exc:
                from tg_agent.services.telegram_actions import TelegramActionClientUnavailableError

                if isinstance(exc, TelegramActionClientUnavailableError):
                    await self.store.transition(project, request, "running", "deferred", {"retry_at": time.time() + 5})
                    continue
                # Never persist raw exception text: upstream errors can carry message text or secrets.
                state = "failed" if is_read_operation(operation) or isinstance(exc, PermissionError) else "uncertain"
                await self.store.transition(project, request, "running", state, {"error": type(exc).__name__})
                logger.warning("Gateway operation failed: kind=%s error=%s", operation.kind, type(exc).__name__)
            except asyncio.CancelledError:
                state = "cancelled" if is_read_operation(operation) else "uncertain"
                await self.store.transition(project, request, "running", state)
                if asyncio.current_task().cancelling():
                    raise
            else:
                await self.store.transition(project, request, "running", "succeeded", result)
            finally:
                self.current = None
                self.operation_task = None

    async def perform(self, project: str, operation: Operation) -> dict:
        account = str(operation.account_id)
        accounts = await self.store.accounts(project)
        summary = next((row for row in accounts if row["id"] == account and row["active"]), None)
        if summary is None:
            raise PermissionError("Account access denied")
        if getattr(operation, "automatic", False) and operation.kind in {"send", "send_message"}:
            peer = operation.peer if isinstance(operation, Send) else operation.arguments.recipient
            if not isinstance(peer, int) or not await self.store.can_auto_reply(
                project, account, peer,
            ):
                raise PermissionError("This project does not own automatic replies for this dialog")
        if summary["flood_until"] > time.time():
            # Reads can retry after the global deadline; writes have not started.
            raise GatewayDeferredError(summary["flood_until"])
        return await self.access.execute(project, operation)

    async def cancel(self, project: str, request: str) -> dict:
        await self.store.operation(project, request)
        if await self.store.transition(project, request, "pending", "cancelled"):
            return await self.store.operation(project, request)
        if self.current == (project, request) and self.operation_task is not None:
            self.operation_task.cancel()
        return await self.store.operation(project, request)


def create_app(store: GatewayStore, access: TelegramAccess) -> FastAPI:
    dispatcher = Dispatcher(store, access)
    artifacts = ArtifactStore(store.root)
    logins = GatewayLogins(store, access)

    def paged_receipt(receipt):
        result = receipt.get("result")
        if not isinstance(result, dict):
            return receipt
        pages = {key: len(value) for key, value in result.items()
                 if key in {"messages", "dialogs", "participants"} and isinstance(value, list) and len(value) > 100}
        if not pages:
            return receipt
        return {**receipt, "result": {key: value[:100] if key in pages else value for key, value in result.items()},
                "result_pages": pages}

    @asynccontextmanager
    async def lifespan(app):
        await store.recover()
        start = getattr(access, "start", None)
        if start is not None:
            await start()
        task = asyncio.create_task(dispatcher.run(), name="gateway-dispatcher")
        app.state.dispatcher_task = task
        async def clean_artifacts():
            while True:
                try:
                    await asyncio.to_thread(artifacts.cleanup)
                    await store.expire_events(now=time.time())
                    await store.prune_read_receipts(now=time.time())
                except OSError as exc:
                    logger.warning("Gateway artifact cleanup delayed: %s", type(exc).__name__)
                await asyncio.sleep(60)

        cleanup = asyncio.create_task(clean_artifacts(), name="gateway-artifact-cleanup")
        try:
            yield
        finally:
            task.cancel()
            cleanup.cancel()
            await asyncio.gather(task, cleanup, return_exceptions=True)
            await logins.close()
            await access.close()

    app = FastAPI(lifespan=lifespan)
    bearer = HTTPBearer(auto_error=False)

    async def project(credentials: HTTPAuthorizationCredentials | None = Depends(bearer)) -> str:
        identity = await store.authenticate(credentials.credentials) if credentials else None
        if identity is None:
            raise HTTPException(401, "Invalid project token")
        return identity

    async def administrator(credentials: HTTPAuthorizationCredentials | None = Depends(bearer)) -> None:
        try:
            expected = await asyncio.to_thread((store.root / "gateway-admin.token").read_text, encoding="utf-8")
        except FileNotFoundError:
            raise HTTPException(401, "Gateway administration is not enrolled") from None
        if credentials is None or not secrets.compare_digest(credentials.credentials, expected.strip()):
            raise HTTPException(403, "Gateway administrator token required")

    @app.post("/v1/auth")
    async def authenticate(body: AuthRequest, identity: str = Depends(project)):
        try:
            return await logins.execute(identity, body)
        except (ValueError, RuntimeError) as exc:
            # Do not persist or reflect codes, passwords, or sessions in diagnostics.
            raise HTTPException(409, type(exc).__name__) from None

    @app.post("/v1/admin/projects", dependencies=[Depends(administrator)])
    async def enroll(body: Enrollment):
        try:
            await store.register_project(str(body.project_id), Path(body.root), body.token.get_secret_value())
        except GatewayConflictError as exc:
            raise HTTPException(409, str(exc)) from exc
        return {"project_id": str(body.project_id)}

    @app.post("/v1/admin/shutdown", dependencies=[Depends(administrator)])
    async def shutdown():
        callback = getattr(app.state, "request_shutdown", None)
        if callback is None:
            raise HTTPException(409, "This gateway is not managed by the process runtime")
        asyncio.get_running_loop().call_later(0.1, callback)
        return {"stopping": True}

    @app.post("/v1/admin/accounts/import", dependencies=[Depends(administrator)])
    async def import_account(body: ImportAccount):
        try:
            source_secret = body.source_secret.get_secret_value() if body.source_secret is not None else None
            account = await store.import_account(body.phone, body.session.get_secret_value(),
                                                 source_secret=source_secret, replace=body.replace)
            if body.replace:
                remove = getattr(access, "remove_account", None)
                if remove is not None:
                    await remove(account)
                start = getattr(access, "start", None)
                if start is not None:
                    await start()
        except ValueError as exc:
            raise HTTPException(409, "Shared account import failed; check source key/session conflict") from exc
        return {"account_id": account}

    @app.get("/v1/admin/accounts", dependencies=[Depends(administrator)])
    async def all_accounts():
        return {"accounts": await store.catalog()}

    @app.post("/v1/admin/notification-bots/import", dependencies=[Depends(administrator)])
    async def import_notification_bot(body: ImportNotificationBot):
        from tg_agent.gateway.notifications import BotStorage
        from tg_agent.models import NotificationBot

        storage = BotStorage(store, str(body.project_id))
        if await storage.get_bot(body.tg_user_id) is not None:
            return {"imported": False}
        values = body.model_dump(exclude={"project_id", "bot_token"})
        await storage.save_bot(NotificationBot(**values, bot_token=body.bot_token.get_secret_value()))
        return {"imported": True}

    @app.post("/v1/admin/accounts/update", dependencies=[Depends(administrator)])
    async def administer_account(body: AdministerAccount):
        try:
            account = await store.administer_account(body.phone, active=body.active, delete=body.delete)
        except KeyError as exc:
            raise HTTPException(404, "Shared account not found") from exc
        if body.delete or body.active is False:
            remove = getattr(access, "remove_account", None)
            if remove is not None:
                await remove(account)
        elif body.active:
            start = getattr(access, "start", None)
            if start is not None:
                await start()
        return {"updated": True}

    @app.get("/v1/admin/accounts/export", dependencies=[Depends(administrator)])
    async def export_account(phone: str):
        try:
            return {"session_string": await store.export_session(phone)}
        except KeyError as exc:
            raise HTTPException(404, "Shared session not found") from exc

    @app.post("/v1/admin/grants", dependencies=[Depends(administrator)])
    async def grant_account(body: GrantAccount):
        try:
            account = await store.account_for_phone(body.phone)
        except KeyError as exc:
            raise HTTPException(404, "Shared account not found") from exc
        await store.grant(str(body.project_id), account, primary=body.primary)
        start = getattr(access, "start", None)
        if start is not None:
            await start()
        return {"account_id": account}

    @app.get("/v1/accounts")
    async def catalog(identity: str = Depends(project)):
        return {"accounts": await store.catalog(identity)}

    @app.patch("/v1/accounts/{account_id}")
    async def update_account(account_id: UUID, body: UpdateGrant, identity: str = Depends(project)):
        try:
            await store.update_grant(identity, str(account_id), active=body.active, primary=body.primary)
        except KeyError as exc:
            raise HTTPException(404, "Project account not found") from exc
        return {"updated": True}

    @app.delete("/v1/accounts/{account_id}")
    async def detach_account(account_id: UUID, identity: str = Depends(project)):
        await store.revoke(identity, str(account_id))
        return {"detached": True}

    @app.post("/v1/accounts/{account_id}/export-session")
    async def project_session_export(account_id: UUID, identity: str = Depends(project)):
        try:
            return {"session_string": await store.session(identity, str(account_id))}
        except PermissionError:
            raise HTTPException(403, "Account is not available to this project") from None

    @app.get("/v1/status")
    async def status(identity: str = Depends(project)):
        start = getattr(access, "start", None)
        if start is not None:
            await start()
        rows = await store.accounts(identity)
        account_status = getattr(access, "account_status", None)
        if account_status is not None:
            rows = [{**row, **account_status(row["id"])} for row in rows]
        task = getattr(app.state, "dispatcher_task", None)
        return {"protocol": 1, "capabilities_version": 2,
                "http_available": True, "dispatcher_running": task is not None and not task.done(),
                "accounts": rows}

    @app.post("/v1/operations", status_code=202)
    async def submit(body: Submission, identity: str = Depends(project)):
        if str(body.operation.account_id) not in {row["id"] for row in await store.accounts(identity)}:
            raise HTTPException(403, "Account access denied")
        try:
            receipt = await store.submit(identity, str(body.request_id), body.operation.model_dump(mode="json"))
        except GatewayConflictError as exc:
            raise HTTPException(409, str(exc)) from exc
        if receipt["state"] == "deferred" and receipt["result"]["retry_at"] <= time.time():
            await store.transition(identity, str(body.request_id), "deferred", "pending")
            receipt = await store.operation(identity, str(body.request_id))
        if receipt["state"] == "cancelled" and is_read_operation(body.operation):
            await store.transition(identity, str(body.request_id), "cancelled", "pending")
            receipt = await store.operation(identity, str(body.request_id))
        dispatcher.wake.set()
        return paged_receipt(receipt)

    @app.get("/v1/operations/{request_id}")
    async def operation(request_id: str, identity: str = Depends(project)):
        try:
            return paged_receipt(await store.operation(identity, request_id))
        except KeyError as exc:
            raise HTTPException(404, "Operation not found") from exc

    @app.get("/v1/operations/{request_id}/results/{field}")
    async def result_page(request_id: str, field: str, offset: int = Query(0, ge=0),
                          limit: int = Query(100, ge=1, le=100), identity: str = Depends(project)):
        try:
            receipt = await store.operation(identity, request_id)
        except KeyError:
            raise HTTPException(404, "Operation not found") from None
        value = (receipt.get("result") or {}).get(field)
        if field not in {"messages", "dialogs", "participants"} or not isinstance(value, list):
            raise HTTPException(404, "Result page not found")
        return {"items": value[offset:offset + limit], "offset": offset, "total": len(value)}

    @app.post("/v1/operations/{request_id}/cancel")
    async def cancel(request_id: str, identity: str = Depends(project)):
        try:
            return await dispatcher.cancel(identity, request_id)
        except KeyError as exc:
            raise HTTPException(404, "Operation not found") from exc

    @app.get("/v1/events")
    async def events(after: int = 0, identity: str = Depends(project)):
        await store.expire_events(now=time.time())
        try:
            return {"events": await store.events(identity, after)}
        except EventCursorExpiredError as exc:
            raise HTTPException(410, "resync_required") from exc
        except ValueError as exc:
            raise HTTPException(422, "Invalid event cursor") from exc

    @app.get("/v1/events/recovery")
    async def event_recovery(identity: str = Depends(project)):
        await store.expire_events(now=time.time())
        return {"floor": await store.event_floor()}

    @app.post("/v1/reply-owner")
    async def reply_owner(body: ReplyOwner, identity: str = Depends(project)):
        try:
            if body.release:
                await store.release_reply_owner(identity, str(body.account_id), body.dialog_id)
            else:
                await store.assign_reply_owner(identity, str(body.account_id), body.dialog_id)
        except GatewayConflictError:
            raise HTTPException(409, "Another project owns automatic replies in this dialog") from None
        except PermissionError:
            raise HTTPException(403, "Account is not granted to this project") from None
        return {"assigned": not body.release}

    @app.post("/v1/events/ack")
    async def acknowledge(body: Acknowledgement, identity: str = Depends(project)):
        await store.acknowledge(identity, body.event_id)
        return {"acknowledged": body.event_id}

    @app.post("/v1/artifacts", status_code=201)
    async def upload(request: Request, identity: str = Depends(project)):
        return await receive_upload(request, identity, uuid4())

    @app.put("/v1/artifacts/{artifact_id}", status_code=201)
    async def upload_identified(artifact_id: UUID, request: Request, identity: str = Depends(project)):
        return await receive_upload(request, identity, artifact_id)

    async def receive_upload(request, identity, artifact):
        # Only complete files become visible, including after a lost HTTP response.
        temporary = uuid4()
        handle = await asyncio.to_thread(artifacts.create, identity, temporary)
        partial = artifacts.path(identity, temporary)
        size = 0
        digest = hashlib.sha256()
        completed = False
        try:
            async for chunk in request.stream():
                size += len(chunk)
                if size > 128 * 1024 * 1024:
                    raise HTTPException(413, "Artifact exceeds 128 MiB")
                await asyncio.to_thread(handle.write, chunk)
                digest.update(chunk)
            await asyncio.to_thread(handle.close)
            try:
                await asyncio.to_thread(artifacts.finish, identity, artifact, partial,
                    filename=unquote(request.headers.get("X-Filename", "upload")),
                    content_type=request.headers.get("Content-Type", "application/octet-stream"),
                    digest=digest.hexdigest())
            except ValueError as exc:
                raise HTTPException(409, str(exc)) from None
            completed = True
        finally:
            await asyncio.to_thread(handle.close)
            if not completed:
                await asyncio.to_thread(partial.unlink, missing_ok=True)
        return {"artifact_id": str(artifact), "size": size}

    @app.get("/v1/artifacts/{artifact_id}")
    async def download(artifact_id: UUID, identity: str = Depends(project)):
        try:
            path = await asyncio.to_thread(artifacts.resolve, identity, artifact_id)
        except FileNotFoundError as exc:
            raise HTTPException(404, "Artifact not found") from exc
        info = await asyncio.to_thread(artifacts.describe, identity, artifact_id)
        return FileResponse(path, media_type=info["content_type"], filename=info["filename"])

    return app
