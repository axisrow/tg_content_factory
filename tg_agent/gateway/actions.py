"""Run the existing business-action facade on a project-scoped shared client."""

import asyncio
import shutil
from dataclasses import asdict, is_dataclass
from uuid import uuid4

from fastapi.encoders import jsonable_encoder

from tg_agent.gateway.action_protocol import PeerReference
from tg_agent.gateway.artifacts import ArtifactStore


class ScopedPool:
    def __init__(self, access, account):
        self.pool = access._pool
        self.account = account
        self._auth = access.auth
        self.clients = {account: self.pool.clients[account]}

    async def get_available_client(self, **kwargs):
        return await self.get_client_by_phone(self.account)

    async def get_premium_client(self):
        account = await self.pool._get_account_for_phone(self.account)
        return await self.get_available_client() if account and account.is_premium else None

    async def get_premium_unavailability_reason(self):
        return "This project's selected account does not have Telegram Premium"

    async def report_flood(self, phone, seconds):
        if phone != self.account:
            raise PermissionError("Cross-account flood update denied")
        await self.pool.report_flood(phone, seconds)

    def is_dialogs_fetched(self, phone):
        return self.pool.is_dialogs_fetched(phone)

    def mark_dialogs_fetched(self, phone):
        self.pool.mark_dialogs_fetched(phone)

    async def run_live_username_resolve(self, client, phone, username, **kwargs):
        if phone != self.account:
            raise PermissionError("Cross-account resolution denied")
        return await self.pool.run_live_username_resolve(client, phone, username, **kwargs)

    async def get_client_by_phone(self, phone, **kwargs):
        if phone != self.account:
            raise PermissionError("Cross-account client access denied")
        return await self.pool.get_client_by_phone(phone, **kwargs)

    async def get_native_client_by_phone(self, phone, **kwargs):
        return await self.get_client_by_phone(phone, **kwargs)

    async def release_client(self, phone, **kwargs):
        await self.pool.release_client(phone, **kwargs)

    async def resolve_dialog_entity(self, session, phone, dialog, target_type=None):
        return await self.pool.resolve_dialog_entity(session, phone, dialog, target_type)

    async def resolve_entity_with_warm(self, session, phone, identifier, **kwargs):
        return await self.pool.resolve_entity_with_warm(session, phone, identifier, **kwargs)

    async def leave_channels(self, phone, dialogs):
        if phone != self.account:
            raise PermissionError("Cross-account client access denied")
        return await self.pool.leave_channels(phone, dialogs)

    async def delete_dialogs(self, phone, dialogs):
        if phone != self.account:
            raise PermissionError("Cross-account client access denied")
        return await self.pool.delete_dialogs(phone, dialogs)


async def execute_action(access, project, operation):
    from telethon.tl.types import PeerChannel, PeerChat, PeerUser

    from tg_agent.services.telegram_actions import TelegramActionService

    account = str(operation.account_id)
    await access.connection(project, account)
    arguments = dict(operation.arguments)
    if operation.kind == "transcribe_voice":
        from tg_agent.gateway.transcription import read_voice

        if arguments["engine"] != "telegram":
            raise ValueError("External speech providers run in the project")
        return await read_voice(access, project, account, arguments["chat_id"], arguments["message_id"], native=True)
    for name, value in arguments.items():
        if isinstance(value, PeerReference):
            if str(value.account_id) != account:
                raise PermissionError("Peer reference belongs to another account")
            constructor = {"channel": PeerChannel, "chat": PeerChat, "user": PeerUser}[value.peer_type]
            arguments[name] = constructor(value.peer_id)
    artifact = None
    if operation.kind in {"download_media", "download_media_sized"}:
        artifact = uuid4()
        storage = ArtifactStore(access.store.root)
        directory = storage.path(project, artifact).with_name(str(artifact) + ".download")
        await asyncio.to_thread(directory.mkdir, parents=True, mode=0o700)
        # TelegramActionService accepts a local destination only inside this private scope.
        import inspect

        method = getattr(TelegramActionService, operation.kind)
        parameter = "output_dir" if "output_dir" in inspect.signature(method).parameters else "output_path"
        arguments[parameter] = str(directory)
    if operation.kind == "transcribe_voice" and arguments.get("engine", "telegram") != "telegram":
        raise ValueError("External speech engines run in the project process")
    service = TelegramActionService(ScopedPool(access, account))
    result = await getattr(service, operation.kind)(phone=account, **arguments)
    if artifact is not None:
        from pathlib import Path

        data = jsonable_encoder(asdict(result))
        path = data.pop("path", None)
        if path is not None:
            original = Path(path)
            data["filename"] = original.name
            await asyncio.to_thread(original.replace, storage.path(project, artifact))
            data["artifact_id"] = str(artifact)
        else:
            data["artifact_id"] = None
        await asyncio.to_thread(shutil.rmtree, directory)
        return data
    if operation.kind == "get_participants":
        return {"phone": account, "participants": [
            {field: getattr(user, field, None) for field in ("id", "username", "first_name", "last_name", "bot")}
            for user in result.participants]}
    if operation.kind == "get_broadcast_stats":
        fields = {}
        for name in ("followers", "views_per_post", "shares_per_post", "reactions_per_post", "forwards_per_post"):
            value = getattr(result.stats, name, None)
            if value is not None:
                fields[name] = {"current": getattr(value, "current", None),
                                "previous": getattr(value, "previous", None)}
        period = getattr(result.stats, "period", None)
        if period is not None:
            fields["period"] = {name: (value.isoformat() if value is not None else None)
                                for name in ("min_date", "max_date") if (value := getattr(period, name, None))}
        notifications = getattr(result.stats, "enabled_notifications", None)
        if notifications is not None:
            fields["enabled_notifications"] = {"part": getattr(notifications, "part", None),
                                               "total": getattr(notifications, "total", None)}
        return {"phone": account, "stats": fields}
    return jsonable_encoder(asdict(result) if is_dataclass(result) else result)
