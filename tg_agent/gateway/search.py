"""Shared Telegram reads with persistence and filtering owned by the project."""

from uuid import UUID, uuid4

from tg_agent.gateway.protocol import Search
from tg_agent.models import Channel, SearchResult


class GatewaySearch:
    def __init__(self, pool, persistence):
        self.pool = pool
        self.persistence = persistence

    async def _search(self, mode, query, limit=50, channel_id=None):
        account = self.pool.select()
        if mode in {"telegram", "quota"}:
            premium = [row for row in self.pool.clients.values() if row.get("is_premium")]
            if premium:
                account = self.pool.select(premium[0]["phone"])
        data = await self.pool.request(Search(kind="search", account_id=UUID(account["id"]), mode=mode,
                                              query=query, channel_id=channel_id, limit=limit),
                                       f"search:{uuid4()}")
        if mode == "quota":
            return data["quota"]
        channels = {row["channel_id"]: Channel.model_validate(row) for row in data.pop("channels", [])}
        result = SearchResult.model_validate(data)
        if mode == "telegram":
            result.messages = await self.persistence.cache_search_results(
                channels, result.messages, account["phone"], query)
        else:
            result.messages = await self.persistence.cache_messages_and_channels(channels, result.messages)
        return result

    async def check_search_quota(self, query=""):
        return await self._search("quota", query)

    async def search_telegram(self, query, limit=50):
        return await self._search("telegram", query, limit)

    async def search_my_chats(self, query, limit=50):
        return await self._search("my_chats", query, limit)

    async def search_in_channel(self, channel_id, query, limit=50):
        return await self._search("channel", query, limit, channel_id)
