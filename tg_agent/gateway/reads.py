"""Normalized Telegram reads backed by the existing gateway-owned ClientPool."""

import base64
from types import SimpleNamespace

from fastapi.encoders import jsonable_encoder
from telethon.tl.types import PeerChannel, PeerChat, PeerUser

from tg_agent.gateway.actions import ScopedPool
from tg_agent.gateway.protocol import Dialogs, DmHistory, InspectPeer, MessagePage, Profile, Search
from tg_agent.models import ChannelStats
from tg_agent.search.telegram_search import TelegramSearch
from tg_agent.telegram.collector_message_parse import build_message_from_telethon


class SearchProjection:
    """Return project content over the API; the gateway never persists it."""

    def __init__(self):
        self.channels = {}
        self._search = SimpleNamespace(channels=self)

    async def get_channel_by_channel_id(self, channel):
        return None

    async def cache_messages_and_channels(self, channels, messages):
        self.channels.update(channels)
        return messages

    async def cache_search_results(self, channels, messages, phone, query):
        return await self.cache_messages_and_channels(channels, messages)


async def execute_read(access, project, operation):
    account = str(operation.account_id)
    session = await access.connection(project, account)
    handlers = {MessagePage: _read_message_page, DmHistory: _read_dm_history,
                Profile: _read_profile, Dialogs: _read_dialogs, Search: _read_search, InspectPeer: _inspect_peer}
    handler = handlers.get(type(operation))
    if handler is None:
        raise ValueError("Unsupported read")
    return await handler(access, account, session, operation)


async def _read_message_page(access, account, session, operation):
    pool = access._pool
    from tg_agent.telegram.reactions import fetch_message_reaction_users, format_reaction_users_result

    if isinstance(operation.peer, int):
        entity = await pool.resolve_dialog_entity(session, account, operation.peer,
            target_type={"channel": "channel", "chat": "group", "user": "dm"}.get(operation.peer_type))
    else:
        entity = await pool.resolve_entity_with_warm(session, account, operation.peer,
                                                    operation="gateway_message_page_resolve")
    options = {"limit": operation.limit, "offset_id": operation.offset_id}
    if operation.topic_id is not None:
        options["reply_to"] = operation.topic_id
    messages, reactors, forwards, urls = [], {}, [], {}
    async for item in session.stream_messages(entity, **options):
        channel = next(getattr(entity, field) for field in ("id", "channel_id", "chat_id", "user_id")
                       if getattr(entity, field, None) is not None)
        converted = build_message_from_telethon(item, abs(channel))
        messages.append(converted.model_dump(mode="json"))
        if getattr(item, "fwd_from", None):
            forwards.append(item.id)
        urls[item.id] = [entity.url for entity in getattr(item, "entities", None) or []
                         if getattr(entity, "url", None)]
        if operation.include_reaction_users and getattr(item, "reactions", None):
            result = await fetch_message_reaction_users(session.raw_client, entity, item.id,
                                                        limit=operation.reaction_users_limit)
            reactors[item.id] = format_reaction_users_result(result)
    return {"messages": messages, "reaction_users": reactors, "forward_ids": forwards, "urls": urls}


async def _read_dm_history(access, account, session, operation):
    pool = access._pool
    peer = await pool.resolve_entity_with_warm(session, account, PeerUser(operation.dialog_id),
                                               operation="gateway_dm_catchup")
    messages = []
    async for item in session.stream_messages(peer, min_id=operation.after_id, reverse=True, limit=operation.limit):
        messages.append({"dialog_id": operation.dialog_id, "message_id": item.id, "out": bool(item.out),
                         "text": item.raw_text, "date": item.date.isoformat() if item.date else None})
    return {"messages": messages}


async def _read_profile(access, account, session, operation):
    pool = access._pool
    me = await session.fetch_me()
    await pool._db.update_account_premium(account, bool(getattr(me, "premium", False)))
    avatar = None
    if operation.include_avatar:
        image = await session.fetch_profile_photo(me, file=bytes)
        if isinstance(image, bytes):
            avatar = "data:image/jpeg;base64," + base64.b64encode(image).decode()
    return {"user_id": me.id, "first_name": getattr(me, "first_name", "") or "",
            "last_name": getattr(me, "last_name", "") or "",
            "username": getattr(me, "username", None), "is_premium": bool(getattr(me, "premium", False)),
            "avatar_base64": avatar}


async def _read_dialogs(access, account, session, operation):
    pool = access._pool
    dialogs = await pool.get_dialogs_for_phone(account, include_dm=operation.include_dm, refresh=operation.refresh,
                                               mode=operation.mode)
    return {"dialogs": jsonable_encoder(dialogs)}


async def _read_search(access, account, session, operation):
    projection = SearchProjection()
    search = TelegramSearch(ScopedPool(access, account), projection)
    if operation.mode == "quota":
        return {"quota": jsonable_encoder(await search.check_search_quota(operation.query))}
    if operation.mode == "telegram":
        result = await search.search_telegram(operation.query, operation.limit)
    elif operation.mode == "channel":
        result = await search.search_in_channel(operation.channel_id, operation.query, operation.limit)
    else:
        result = await search.search_my_chats(operation.query, operation.limit)
    return {**result.model_dump(mode="json"),
            "channels": [channel.model_dump(mode="json") for channel in projection.channels.values()]}


async def _inspect_peer(access, account, session, operation):
    pool = access._pool
    peer = operation.peer
    if isinstance(peer, int) and operation.peer_type != "auto":
        peer = {"channel": PeerChannel, "chat": PeerChat, "user": PeerUser}[operation.peer_type](abs(peer))
    entity = await pool.resolve_entity_with_warm(session, account, peer, operation="gateway_inspect_peer")
    kind, deactivated = pool._classify_entity(entity)
    if hasattr(entity, "first_name"):
        kind = "bot" if getattr(entity, "bot", False) else "user"
    if kind in {"user", "bot"}:
        return {"channel_id": entity.id, "title": " ".join(filter(None, [getattr(entity, "first_name", None),
                    getattr(entity, "last_name", None)])), "username": getattr(entity, "username", None),
                "channel_type": kind, "deactivate": deactivated}
    full = await (session.fetch_full_chat(entity.id) if kind == "group" else session.fetch_full_channel(entity))
    details = full.full_chat
    participants = getattr(details, "participants", None)
    subscriber_count = (len(participants.participants) if getattr(participants, "participants", None) is not None
                        else getattr(details, "participants_count", None))
    result = {"channel_id": entity.id, "title": getattr(entity, "title", None),
              "username": getattr(entity, "username", None), "channel_type": kind, "deactivate": deactivated,
              "about": getattr(details, "about", None), "linked_chat_id": getattr(details, "linked_chat_id", None),
              "has_comments": bool(getattr(details, "linked_chat_id", None)), "subscriber_count": subscriber_count,
              "created_at": entity.date.isoformat() if getattr(entity, "date", None) else None}
    if operation.statistics:
        metrics = {"views": [], "reactions": [], "forwards": []}
        async for message in session.stream_messages(entity, limit=50):
            for key in ("views", "forwards"):
                value = getattr(message, key, None)
                if value is not None:
                    metrics[key].append(value)
            reactions = getattr(message, "reactions", None)
            if reactions is not None:
                metrics["reactions"].append(sum(item.count for item in reactions.results))
        stats = ChannelStats(channel_id=entity.id, subscriber_count=subscriber_count,
                             **{f"avg_{key}": sum(values) / len(values) if values else None
                                for key, values in metrics.items()})
        result["statistics"] = stats.model_dump(mode="json")
    if operation.topics:
        topics = await session.fetch_forum_topics(entity)
        result["topics"] = [{"id": item.id, "title": item.title} for item in topics.topics
                            if getattr(item, "title", None) is not None]
    return result
