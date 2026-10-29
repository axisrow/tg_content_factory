"""Project-authenticated gateway API and fair, durable operation dispatcher."""

from __future__ import annotations

import asyncio
import logging
import time
from contextlib import asynccontextmanager
from uuid import UUID, uuid4

from fastapi import Depends, FastAPI, HTTPException, Request
from fastapi.responses import FileResponse
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer
from pydantic import TypeAdapter
from telethon_floodgate import HandledFloodWaitError

from tg_agent.gateway.artifacts import ArtifactStore
from tg_agent.gateway.protocol import Acknowledgement, History, Operation, Send, Submission, TelegramAccess
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
            except asyncio.CancelledError:
                state = "cancelled" if isinstance(operation, History) else "uncertain"
                await self.store.transition(project, request, "running", state)
                if asyncio.current_task().cancelling():
                    raise
            except Exception as exc:
                # Never persist raw exception text: upstream errors can carry message text or secrets.
                state = "failed" if isinstance(operation, History) or isinstance(exc, PermissionError) else "uncertain"
                await self.store.transition(project, request, "running", state, {"error": type(exc).__name__})
                logger.warning("Gateway operation failed: kind=%s error=%s", operation.kind, type(exc).__name__)
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
        if isinstance(operation, Send) and operation.automatic:
            if not isinstance(operation.peer, int) or not await self.store.can_auto_reply(
                project, account, operation.peer,
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

    @asynccontextmanager
    async def lifespan(app):
        await store.recover()
        start = getattr(access, "start", None)
        if start is not None:
            await start()
        task = asyncio.create_task(dispatcher.run(), name="gateway-dispatcher")
        try:
            yield
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            await access.close()

    app = FastAPI(lifespan=lifespan)
    bearer = HTTPBearer(auto_error=False)

    async def project(credentials: HTTPAuthorizationCredentials | None = Depends(bearer)) -> str:
        identity = await store.authenticate(credentials.credentials) if credentials else None
        if identity is None:
            raise HTTPException(401, "Invalid project token")
        return identity

    @app.get("/v1/status")
    async def status(identity: str = Depends(project)):
        return {"protocol": 1, "accounts": await store.accounts(identity)}

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
        dispatcher.wake.set()
        return receipt

    @app.get("/v1/operations/{request_id}")
    async def operation(request_id: str, identity: str = Depends(project)):
        try:
            return await store.operation(identity, request_id)
        except KeyError as exc:
            raise HTTPException(404, "Operation not found") from exc

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

    @app.post("/v1/events/ack")
    async def acknowledge(body: Acknowledgement, identity: str = Depends(project)):
        await store.acknowledge(identity, body.event_id)
        return {"acknowledged": body.event_id}

    @app.post("/v1/artifacts", status_code=201)
    async def upload(request: Request, identity: str = Depends(project)):
        artifact = uuid4()
        handle = await asyncio.to_thread(artifacts.create, identity, artifact)
        size = 0
        completed = False
        try:
            async for chunk in request.stream():
                size += len(chunk)
                if size > 128 * 1024 * 1024:
                    raise HTTPException(413, "Artifact exceeds 128 MiB")
                await asyncio.to_thread(handle.write, chunk)
            completed = True
        finally:
            await asyncio.to_thread(handle.close)
            if not completed:
                await asyncio.to_thread(artifacts.path(identity, artifact).unlink, missing_ok=True)
        return {"artifact_id": str(artifact), "size": size}

    @app.get("/v1/artifacts/{artifact_id}")
    async def download(artifact_id: UUID, identity: str = Depends(project)):
        try:
            path = await asyncio.to_thread(artifacts.resolve, identity, artifact_id)
        except FileNotFoundError as exc:
            raise HTTPException(404, "Artifact not found") from exc
        return FileResponse(path, media_type="application/octet-stream")

    return app
