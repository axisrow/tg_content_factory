"""Project-side implementation of the established TelegramActionService contract."""

import asyncio
from dataclasses import is_dataclass
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from typing import get_type_hints
from uuid import UUID

from tg_agent.gateway.action_protocol import LOCAL_ARGUMENTS, PATH_ARGUMENTS
from tg_agent.gateway.context import automatic_intent, next_intent_key
from tg_agent.gateway.protocol import ACTION_MODELS
from tg_agent.models import DialogMessage


class RemoteActionService:
    def __init__(self, pool):
        self.pool = pool

    def __getattr__(self, name):
        if name not in ACTION_MODELS:
            raise AttributeError(name)

        async def invoke(**arguments):
            return await self.invoke(name, arguments)
        return invoke

    async def invoke(self, name, arguments):
        if name == "transcribe_voice":
            from tg_agent.gateway.transcription import transcribe_remote

            return await transcribe_remote(self, arguments)
        from tg_agent.services.telegram_actions import TelegramActionService

        phone = arguments.get("phone")
        account = self.pool.select(phone)
        if phone is not None and account["phone"] != phone:
            raise PermissionError("Requested account is not connected to this project")
        fields = {key: value for key, value in arguments.items() if key not in LOCAL_ARGUMENTS | PATH_ARGUMENTS}
        operation = ACTION_MODELS[name](account_id=UUID(account["id"]), arguments=fields, automatic=automatic_intent())
        result = await self.pool.request(operation, next_intent_key())
        if not isinstance(result, dict):
            return result
        result["phone"] = account["phone"]
        if name == "read_history":
            result["messages"] = [DialogMessage.model_validate(row) for row in result["messages"]]
        elif name == "get_participants":
            result["participants"] = [SimpleNamespace(**row) for row in result["participants"]]
        elif name == "get_broadcast_stats":
            period = result["stats"].get("period")
            if period:
                result["stats"]["period"] = {key: datetime.fromisoformat(value) if value else None
                                               for key, value in period.items()}
            result["stats"] = SimpleNamespace(**{
                key: SimpleNamespace(**value) if isinstance(value, dict) and key != "enabled_notifications" else value
                for key, value in result["stats"].items()})
        elif name == "forward_messages":
            result["message_ids"] = tuple(result["message_ids"])
        elif name in {"download_media", "download_media_sized"}:
            await self._download(result, arguments)
        return_type = get_type_hints(getattr(TelegramActionService, name))["return"]
        return return_type(**result) if is_dataclass(return_type) else result

    async def _download(self, result, arguments):
        artifact = result.pop("artifact_id", None)
        filename = result.pop("filename", "download")
        if artifact is None:
            return
        directory = Path(arguments["output_dir"])
        if result.get("subdir"):
            directory = directory / result["subdir"]
        await asyncio.to_thread(directory.mkdir, parents=True, exist_ok=True)
        target = directory / Path(filename).name
        async with self.pool.client.http.stream("GET", f"/v1/artifacts/{artifact}") as response:
            response.raise_for_status()
            handle = await asyncio.to_thread(target.open, "wb")
            try:
                async for chunk in response.aiter_bytes():
                    await asyncio.to_thread(handle.write, chunk)
            finally:
                await asyncio.to_thread(handle.close)
        result["path"] = str(target)
        if "rel_path" in result:
            result["rel_path"] = str(target.relative_to(arguments["output_dir"]))
