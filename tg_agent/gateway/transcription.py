"""Telegram recording access in the gateway; provider calls and cache in projects."""

import asyncio
from types import SimpleNamespace
from uuid import UUID, uuid4

from tg_agent.gateway.action_protocol import PeerReference
from tg_agent.gateway.artifacts import ArtifactStore
from tg_agent.gateway.context import next_intent_key
from tg_agent.gateway.protocol import ACTION_MODELS, VoiceSource
from tg_agent.telegram import transcription as speech


async def read_voice(access, project, account, peer, message_id, *, native=False, metadata_only=False):
    from telethon import utils
    from telethon.tl.types import PeerChannel, PeerChat, PeerUser

    from tg_agent.gateway.actions import ScopedPool
    from tg_agent.services.telegram_actions import TelegramActionService

    await access.connection(project, account)
    if isinstance(peer, PeerReference):
        if str(peer.account_id) != account:
            raise PermissionError("Recording belongs to another account")
        peer = {"channel": PeerChannel, "chat": PeerChat, "user": PeerUser}[peer.peer_type](peer.peer_id)
    service = TelegramActionService(ScopedPool(access, account))
    async with service._client(phone=account, native=True) as (client, phone):
        entity = await service._resolve_entity(client, phone=phone, identifier=peer)
        message = await service._fetch_message_with_flood_wait(
            client, phone, entity, message_id, operation_prefix="gateway_voice_source")
        if not speech.is_transcribable(message):
            return {"status": "error", "error": "Message has no voice or video note"}
        marked = await client.get_peer_id(entity)
        peer_id, peer_class = utils.resolve_id(marked)
        reference = PeerReference(account_id=UUID(account), peer_id=peer_id,
            peer_type={PeerChannel: "channel", PeerChat: "chat", PeerUser: "user"}[peer_class])
        metadata = {"chat_id": marked, "duration": speech.voice_duration(message),
                    "peer_reference": reference.model_dump(mode="json")}
        if metadata_only:
            return {"status": "ok", **metadata}
        if native:
            result = await speech.transcribe(client, entity, message, "telegram")
            return {**result, **metadata}
        declared = getattr(getattr(message, "file", None), "size", None)
        if declared is not None and declared > 128 * 1024 * 1024:
            return {"status": "error", "error": "Recording exceeds 128 MiB"}
        data = await client.download_media(message, file=bytes)
        if not data or len(data) > 128 * 1024 * 1024:
            return {"status": "error", "error": "Recording empty or exceeds 128 MiB"}
        storage, artifact = ArtifactStore(access.store.root), uuid4()
        handle = await asyncio.to_thread(storage.create, project, artifact)
        try:
            await asyncio.to_thread(handle.write, data)
        finally:
            await asyncio.to_thread(handle.close)
        file = message.file
        return {"status": "ok", **metadata, "artifact_id": str(artifact), "size": len(data),
                "extension": getattr(file, "ext", ".ogg"), "mime_type": getattr(file, "mime_type", "audio/ogg")}


async def transcribe_remote(service, arguments):
    engine = arguments["engine"]
    if engine not in speech.ENGINES:
        return {"status": "error", "error": "Unknown transcription engine"}
    error = speech.engine_config_error(engine)
    if error:
        return {"status": "error", "error": error}
    pool, db = service.pool, arguments["db"]
    account = pool.select(arguments.get("phone"))
    if arguments.get("phone") and account["phone"] != arguments["phone"]:
        raise PermissionError("Recording account is unavailable")
    key = next_intent_key()
    message_id = arguments["message_id"]
    metadata = await pool.request(VoiceSource(kind="voice_source", account_id=UUID(account["id"]),
        peer=arguments["chat_id"], message_id=message_id, metadata_only=True), key + ":metadata")
    if metadata["status"] != "ok":
        return metadata
    chat_id = metadata["chat_id"]
    reference = PeerReference.model_validate(metadata["peer_reference"])
    async with speech._transcribe_lock((chat_id, message_id, engine)):
        cached = await speech.get_cached_transcript(db, chat_id, message_id, source=engine)
        if cached is not None:
            return speech._cached_result(cached)
        if engine == "telegram":
            operation = ACTION_MODELS["transcribe_voice"](account_id=UUID(account["id"]),
                arguments={"chat_id": reference, "message_id": message_id, "engine": "telegram"})
            result = await pool.request(operation, key + ":native")
        else:
            data = await pool.request(VoiceSource(kind="voice_source", account_id=UUID(account["id"]),
                peer=reference, message_id=message_id), key + ":download")
            if data["status"] != "ok":
                return data
            response = await pool.client.http.get(f"/v1/artifacts/{data['artifact_id']}")
            response.raise_for_status()

            class LocalAudio:
                async def download_media(self, msg, *, file):
                    return response.content

            message = SimpleNamespace(id=message_id, file=SimpleNamespace(size=data["size"],
                duration=data["duration"], ext=data["extension"], mime_type=data["mime_type"]))
            result = await speech.transcribe(LocalAudio(), None, message, engine)
            result["duration"] = data["duration"]
        result.pop("chat_id", None)
        result.pop("peer_reference", None)
        if result.get("status") == "ok":
            await speech.save_transcript(db, chat_id, message_id, engine, result["text"],
                                         duration=result.get("duration"), lang=result.get("lang"))
        return {**result, "source": engine, "cached": False}
